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
	"strings"
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

// maxGetObjectSize caps how much of an object GetObject buffers in
// memory; larger objects must be fetched through a presigned URL.
const maxGetObjectSize = 64 << 20 // 64 MiB

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
		// The endpoint must be a bare host[:port]; the scheme comes from
		// use_ssl. A scheme here would produce URLs like "https://http://...".
		if strings.Contains(cfg.Endpoint, "://") {
			return nil, fmt.Errorf("s3 endpoint %q must not include a scheme; use host:port and set use_ssl instead", cfg.Endpoint)
		}
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

	// Read one byte past the cap so an oversized object is detected
	// instead of silently truncated.
	body, err := io.ReadAll(io.LimitReader(output.Body, maxGetObjectSize+1))
	if err != nil {
		return nil, fmt.Errorf("read object %q: %w", key, err)
	}
	if len(body) > maxGetObjectSize {
		return nil, fmt.Errorf("read object %q: exceeds the %d byte limit; use a presigned URL instead", key, maxGetObjectSize)
	}
	return body, nil
}

// DeleteObject removes the object stored under key. Deleting a missing
// object is not an error (S3 delete is idempotent).
func (s *Store) DeleteObject(ctx context.Context, key string) error {
	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.key(key)),
	})
	if err != nil {
		return fmt.Errorf("delete object %q: %w", key, err)
	}
	return nil
}

// ObjectInfo describes one object found by ListObjects.
type ObjectInfo struct {
	// Key is the object key relative to the store's configured prefix.
	Key string
	// Size is the object size in bytes.
	Size int64
	// LastModified is the object's last-modified time as reported by the
	// service.
	LastModified time.Time
}

// ListObjects returns every object whose key starts with prefix (relative
// to the store's configured prefix), following pagination.
func (s *Store) ListObjects(ctx context.Context, prefix string) ([]ObjectInfo, error) {
	var out []ObjectInfo
	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String(s.key(prefix)),
	}
	for {
		page, err := s.client.ListObjectsV2(ctx, input)
		if err != nil {
			return nil, fmt.Errorf("list objects %q: %w", prefix, err)
		}
		for _, obj := range page.Contents {
			info := ObjectInfo{Key: aws.ToString(obj.Key), Size: aws.ToInt64(obj.Size)}
			if obj.LastModified != nil {
				info.LastModified = *obj.LastModified
			}
			// Strip the store prefix so callers see the same relative keys
			// they pass to Get/Put.
			info.Key = strings.TrimPrefix(info.Key, s.key(""))
			out = append(out, info)
		}
		if !aws.ToBool(page.IsTruncated) {
			break
		}
		input.ContinuationToken = page.NextContinuationToken
	}
	return out, nil
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
