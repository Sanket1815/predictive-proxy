package proxy

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/nadsanket7/go-predictive-proxy/internal/cache"
	"go.uber.org/zap"
)

// BackendConfig holds the connection parameters for the S3/Wasabi origin.
type BackendConfig struct {
	Endpoint        string // e.g. "https://s3.wasabisys.com" — empty for real AWS
	Region          string
	AccessKeyID     string
	SecretAccessKey string
	Bucket          string
	MaxConns        int // maximum idle + active connections in the transport pool
}

// Backend wraps an AWS SDK v2 S3 client with a tuned HTTP transport and
// implements engine.BackendFetcher for chunk-granular range requests.
type Backend struct {
	cfg    BackendConfig
	client *s3.Client
	log    *zap.Logger
}

// NewBackend creates a Backend with a custom http.Transport sized for
// high-concurrency range request workloads against Wasabi/S3.
//
// Key transport settings:
//   - DisableCompression: gzip would corrupt byte offsets in range responses.
//   - MaxIdleConnsPerHost = MaxConns: prevents connection exhaustion on a single
//     origin host; HTTP/2 multiplexing is not used here because large body
//     transfers benefit more from independent TCP streams.
//   - Short dial/TLS timeouts with a long idle timeout match the Wasabi SLA.
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

	awsCfg, err := awsconfig.LoadDefaultConfig(context.Background(),
		awsconfig.WithRegion(cfg.Region),
		awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.AccessKeyID, cfg.SecretAccessKey, ""),
		),
		awsconfig.WithHTTPClient(&http.Client{Transport: transport}),
	)
	if err != nil {
		return nil, fmt.Errorf("backend: load aws config: %w", err)
	}

	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		if cfg.Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
			// Path-style addressing is mandatory for Wasabi and MinIO.
			o.UsePathStyle = true
		}
	})

	return &Backend{cfg: cfg, client: client, log: log}, nil
}

// FetchChunk downloads a single 4 MiB aligned chunk from S3/Wasabi into *buf.
// The chunk byte range is [chunkIndex*ChunkSize, chunkIndex*ChunkSize+ChunkSize).
// For the final chunk of an object, the S3 response may be shorter than ChunkSize;
// io.ReadFull returns io.ErrUnexpectedEOF in that case, which is treated as success.
//
// FetchChunk satisfies the engine.BackendFetcher interface.
func (b *Backend) FetchChunk(ctx context.Context, objectKey string, chunkIndex uint64, buf *[]byte) (int, error) {
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
		// ErrUnexpectedEOF is expected for the terminal chunk of an object.
		return 0, fmt.Errorf("s3 read body %q chunk %d: %w", objectKey, chunkIndex, err)
	}
	return n, nil
}

// GetObjectStream opens a streaming GET for the full object and returns the
// response body and content-type. Callers are responsible for closing the body.
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
