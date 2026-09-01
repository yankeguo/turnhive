// Package storage implements S3-backed object storage for turnhive
// artifacts, such as session history and skill tarballs.
//
// Every object key is prefixed with the configured prefix (default
// "turnhive"), so multiple projects can share one bucket:
//
//	{prefix}/{key}
//
// The same configuration works against AWS S3 and S3-compatible services
// (MinIO, RustFS) through the custom endpoint and path-style options of
// config.S3Config.
package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"github.com/yankeguo/turnhive/config"
)

// ErrNotExist is returned by GetObject when the requested object does not
// exist in the bucket.
var ErrNotExist = errors.New("storage: object not exist")

// defaultRegion is used when the configuration leaves Region empty, which
// is common for S3-compatible services that ignore the region.
const defaultRegion = "us-east-1"

// Store reads and writes turnhive artifacts in an S3 bucket.
type Store struct {
	client  *s3.Client
	presign *s3.PresignClient
	bucket  string
	prefix  string
}

// New creates a Store from the S3 section of the turnhive configuration.
// Static credentials are used when AccessKeyID is set; otherwise the AWS
// default credential chain (environment, shared config, instance metadata)
// is used.
func New(cfg config.S3Config) (*Store, error) {
	region := cfg.Region
	if region == "" {
		region = defaultRegion
	}

	var awsCfg aws.Config
	if cfg.AccessKeyID != "" {
		// Static credentials: build a minimal config without touching the
		// default config file or environment.
		awsCfg = aws.Config{
			Region:      region,
			Credentials: credentials.NewStaticCredentialsProvider(cfg.AccessKeyID, cfg.SecretAccessKey, ""),
		}
	} else {
		// Fall back to the default credential chain.
		var err error
		if awsCfg, err = awsconfig.LoadDefaultConfig(context.Background(), awsconfig.WithRegion(region)); err != nil {
			return nil, fmt.Errorf("load default AWS config: %w", err)
		}
	}

	if cfg.Endpoint != "" {
		scheme := "https"
		if !cfg.Secure() {
			scheme = "http"
		}
		endpoint := scheme + "://" + cfg.Endpoint
		awsCfg.BaseEndpoint = aws.String(endpoint)
	}

	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.UsePathStyle = cfg.ForcePathStyle
	})
	return &Store{
		client:  client,
		presign: s3.NewPresignClient(client),
		bucket:  cfg.Bucket,
		prefix:  cfg.Prefix,
	}, nil
}

// key prefixes an object key with the configured prefix.
func (s *Store) key(k string) string {
	if s.prefix == "" {
		return k
	}
	return s.prefix + "/" + k
}

// PutObject stores body under key, overwriting any existing object.
func (s *Store) PutObject(ctx context.Context, key string, body []byte) error {
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.key(key)),
		Body:   bytes.NewReader(body),
	})
	if err != nil {
		return fmt.Errorf("put object %q: %w", key, err)
	}
	return nil
}

// GetObject reads the object stored under key. It returns ErrNotExist when
// the object does not exist.
func (s *Store) GetObject(ctx context.Context, key string) ([]byte, error) {
	output, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.key(key)),
	})
	if err != nil {
		var apiErr smithy.APIError
		if errors.As(err, &apiErr) && apiErr.ErrorCode() == "NoSuchKey" {
			return nil, ErrNotExist
		}
		return nil, fmt.Errorf("get object %q: %w", key, err)
	}
	defer output.Body.Close()

	body, err := io.ReadAll(output.Body)
	if err != nil {
		return nil, fmt.Errorf("read object %q: %w", key, err)
	}
	return body, nil
}

// PresignGet returns a presigned URL that grants read access to the object
// stored under key for ttl.
func (s *Store) PresignGet(ctx context.Context, key string, ttl time.Duration) (string, error) {
	req, err := s.presign.PresignGetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.key(key)),
	}, s3.WithPresignExpires(ttl))
	if err != nil {
		return "", fmt.Errorf("presign get object %q: %w", key, err)
	}
	return req.URL, nil
}

// PresignPut returns a presigned URL that grants write access to the
// object stored under key for ttl; upload with HTTP PUT and no extra
// signed headers (e.g. ironhive's file upload endpoint).
func (s *Store) PresignPut(ctx context.Context, key string, ttl time.Duration) (string, error) {
	req, err := s.presign.PresignPutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.key(key)),
	}, s3.WithPresignExpires(ttl))
	if err != nil {
		return "", fmt.Errorf("presign put object %q: %w", key, err)
	}
	return req.URL, nil
}
