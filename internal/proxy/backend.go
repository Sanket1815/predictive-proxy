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

type BackendConfig struct {
	Endpoint        string
	Region          string
	AccessKeyID     string
	SecretAccessKey string
	Bucket          string
	MaxConns        int
}

type Backend struct {
	cfg    BackendConfig
	client *s3.Client
	log    *zap.Logger
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
			o.UsePathStyle = true
		}
	})

	return &Backend{cfg: cfg, client: client, log: log}, nil
}

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
		return 0, fmt.Errorf("s3 read body %q chunk %d: %w", objectKey, chunkIndex, err)
	}
	return n, nil
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
