package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sts"
)

type s3Flags struct {
	bucket, region, endpoint, prefix string
}

func (f *s3Flags) register(fs *flag.FlagSet) {
	fs.StringVar(&f.bucket, "bucket", "predictive-proxy", "S3 bucket holding the dataset")
	fs.StringVar(&f.region, "region", "us-east-2", "bucket region")
	fs.StringVar(&f.endpoint, "endpoint", "", "S3-compatible endpoint override (Wasabi/MinIO); empty for AWS")
	fs.StringVar(&f.prefix, "prefix", "edu/", "key prefix of the portal dataset")
}

func (f *s3Flags) config(ctx context.Context) (aws.Config, error) {
	transport := &http.Transport{
		DialContext:         (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		MaxIdleConns:        256,
		MaxIdleConnsPerHost: 256,
		IdleConnTimeout:     90 * time.Second,
		TLSHandshakeTimeout: 5 * time.Second,
		DisableCompression:  true,
	}
	return awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(f.region),
		awsconfig.WithHTTPClient(&http.Client{Transport: transport}),
	)
}

func (f *s3Flags) client(ctx context.Context) (*s3.Client, error) {
	cfg, err := f.config(ctx)
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}
	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		if f.endpoint != "" {
			o.BaseEndpoint = aws.String(f.endpoint)
			o.UsePathStyle = !strings.Contains(f.endpoint, "amazonaws.com")
		}
	}), nil
}

func runCheck(args []string) error {
	fs := flag.NewFlagSet("check", flag.ExitOnError)
	var sf s3Flags
	sf.register(fs)
	fs.Parse(args)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cfg, err := sf.config(ctx)
	if err != nil {
		return err
	}
	id, err := sts.NewFromConfig(cfg).GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
	if err != nil {
		return fmt.Errorf("credentials not usable (check ~/.aws/credentials or AWS_PROFILE): %w", err)
	}
	fmt.Println("identity:", aws.ToString(id.Arn))

	client, err := sf.client(ctx)
	if err != nil {
		return err
	}
	if _, err := client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(sf.bucket)}); err != nil {
		return fmt.Errorf("bucket %q not reachable in %s: %w", sf.bucket, sf.region, err)
	}
	fmt.Printf("bucket:   %s (%s) reachable\n", sf.bucket, sf.region)

	objs, err := listObjects(ctx, client, sf.bucket, sf.prefix)
	if err != nil {
		return err
	}
	byKind := map[string][2]int64{}
	for _, o := range objs {
		v := byKind[o.kind()]
		byKind[o.kind()] = [2]int64{v[0] + 1, v[1] + o.size}
	}
	var total int64
	for k, v := range byKind {
		fmt.Printf("  %-8s %5d files  %8.2f GB\n", k, v[0], float64(v[1])/1e9)
		total += v[1]
	}
	fmt.Printf("dataset:  %d objects under %q, %.2f GB\n", len(objs), sf.prefix, float64(total)/1e9)
	return nil
}

type object struct {
	key  string
	size int64
}

// kind classifies a portal file by extension; it drives the access pattern.
func (o object) kind() string {
	switch k := strings.ToLower(o.key); {
	case strings.HasSuffix(k, ".mp4"), strings.HasSuffix(k, ".webm"), strings.HasSuffix(k, ".mov"):
		return "video"
	case strings.HasSuffix(k, ".pdf"):
		return "pdf"
	case strings.HasSuffix(k, ".pptx"), strings.HasSuffix(k, ".ppt"), strings.HasSuffix(k, ".key"):
		return "slides"
	case strings.HasSuffix(k, ".zip"), strings.HasSuffix(k, ".csv"), strings.HasSuffix(k, ".parquet"), strings.HasSuffix(k, ".tar.gz"):
		return "dataset"
	default:
		return "other"
	}
}

func listObjects(ctx context.Context, client *s3.Client, bucket, prefix string) ([]object, error) {
	var objs []object
	p := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	})
	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("list s3://%s/%s: %w", bucket, prefix, err)
		}
		for _, o := range page.Contents {
			if aws.ToInt64(o.Size) > 0 {
				objs = append(objs, object{key: aws.ToString(o.Key), size: aws.ToInt64(o.Size)})
			}
		}
	}
	return objs, nil
}
