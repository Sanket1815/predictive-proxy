package proxy

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/nadsanket7/go-predictive-proxy/internal/cache"
	"go.uber.org/zap"
)

type BackendConfig struct {
	Endpoint        string
	Region          string
	AccessKeyID     string
	SecretAccessKey string
	Bucket          string
	MaxConns        int
}

// flight is a backend chunk fetch in progress. Callers asking for the same
// chunk while it is in flight wait for it instead of issuing a duplicate GET.
type flight struct {
	done    chan struct{}
	data    []byte
	err     error
	waiters int
}

type Backend struct {
	cfg    BackendConfig
	client *s3.Client
	log    *zap.Logger

	mu       sync.Mutex
	inflight map[cache.ChunkKey]*flight

	sizes sync.Map // objectKey -> int64
}

func NewBackend(cfg BackendConfig, log *zap.Logger) (*Backend, error) {
	if cfg.MaxConns <= 0 {
		cfg.MaxConns = 512
	}

	transport := &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   5 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:          cfg.MaxConns,
		MaxIdleConnsPerHost:   cfg.MaxConns,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   5 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		DisableCompression:    true,
	}

	opts := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithRegion(cfg.Region),
		awsconfig.WithHTTPClient(&http.Client{Transport: transport}),
	}
	// Without static keys, fall back to the SDK default chain
	// (env vars, ~/.aws/credentials, SSO, instance role).
	if cfg.AccessKeyID != "" && cfg.SecretAccessKey != "" {
		opts = append(opts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.AccessKeyID, cfg.SecretAccessKey, ""),
		))
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(context.Background(), opts...)
	if err != nil {
		return nil, fmt.Errorf("backend: load aws config: %w", err)
	}

	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		if cfg.Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
			if !strings.Contains(cfg.Endpoint, "amazonaws.com") {
				o.UsePathStyle = true
			}
		}
	})

	return &Backend{
		cfg:      cfg,
		client:   client,
		log:      log,
		inflight: make(map[cache.ChunkKey]*flight),
	}, nil
}

// FetchChunk satisfies engine.BackendFetcher.
func (b *Backend) FetchChunk(ctx context.Context, objectKey string, chunkIndex uint64, buf *[]byte) (int, error) {
	n, _, err := b.FetchChunkShared(ctx, objectKey, chunkIndex, buf)
	return n, err
}

// FetchChunkShared fetches a chunk into buf, joining an identical fetch that is
// already in flight (e.g. a prefetch the client caught up with). joined reports
// whether this call piggy-backed on another caller's request.
func (b *Backend) FetchChunkShared(ctx context.Context, objectKey string, chunkIndex uint64, buf *[]byte) (n int, joined bool, err error) {
	key := cache.ChunkKey{ObjectKey: objectKey, ChunkIndex: chunkIndex}
	var f *flight
	for {
		b.mu.Lock()
		existing, ok := b.inflight[key]
		if !ok {
			f = &flight{done: make(chan struct{})}
			b.inflight[key] = f
			b.mu.Unlock()
			break
		}
		existing.waiters++
		b.mu.Unlock()

		select {
		case <-existing.done:
		case <-ctx.Done():
			return 0, true, ctx.Err()
		}
		if existing.err == nil {
			return copy(*buf, existing.data), true, nil
		}
		// The leader failed (possibly its own request was cancelled); retry
		// as leader unless we were cancelled too.
		if ctx.Err() != nil {
			return 0, true, ctx.Err()
		}
	}

	n, err = b.fetchChunk(ctx, objectKey, chunkIndex, buf)

	// Unpublish before copying out: once removed, no new waiters can attach,
	// so the copy is only paid when someone is actually waiting.
	b.mu.Lock()
	delete(b.inflight, key)
	if err == nil && f.waiters > 0 {
		f.data = make([]byte, n)
		copy(f.data, (*buf)[:n])
	}
	f.err = err
	b.mu.Unlock()
	close(f.done)
	return n, false, err
}

func (b *Backend) fetchChunk(ctx context.Context, objectKey string, chunkIndex uint64, buf *[]byte) (int, error) {
	rangeStart := int64(chunkIndex) * cache.ChunkSize
	rangeEnd := rangeStart + cache.ChunkSize - 1
	rangeHdr := fmt.Sprintf("bytes=%d-%d", rangeStart, rangeEnd)

	out, err := b.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(b.cfg.Bucket),
		Key:    aws.String(objectKey),
		Range:  aws.String(rangeHdr),
	})
	if err != nil {
		return 0, fmt.Errorf("s3 GetObject range %q for %q: %w", rangeHdr, objectKey, err)
	}
	defer out.Body.Close()

	n, err := io.ReadFull(out.Body, *buf)
	if err != nil && err != io.ErrUnexpectedEOF {
		return 0, fmt.Errorf("s3 read body %q chunk %d: %w", objectKey, chunkIndex, err)
	}
	return n, nil
}

// ObjectSize returns the object's total length, cached after the first HEAD.
func (b *Backend) ObjectSize(ctx context.Context, objectKey string) (int64, error) {
	if v, ok := b.sizes.Load(objectKey); ok {
		return v.(int64), nil
	}
	out, err := b.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(b.cfg.Bucket),
		Key:    aws.String(objectKey),
	})
	if err != nil {
		return 0, fmt.Errorf("s3 HeadObject %q: %w", objectKey, err)
	}
	size := aws.ToInt64(out.ContentLength)
	b.sizes.Store(objectKey, size)
	return size, nil
}

func (b *Backend) GetObjectStream(ctx context.Context, objectKey string) (io.ReadCloser, string, error) {
	out, err := b.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(b.cfg.Bucket),
		Key:    aws.String(objectKey),
	})
	if err != nil {
		return nil, "", fmt.Errorf("s3 GetObject %q: %w", objectKey, err)
	}
	ct := ""
	if out.ContentType != nil {
		ct = *out.ContentType
	}
	return out.Body, ct, nil
}
