// Package config loads and validates the turnhive configuration file.
package config

import (
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

// DefaultPrefix is the default object key prefix used when storing objects
// in a shared S3 bucket.
const DefaultPrefix = "turnhive"

// Config is the root of the turnhive configuration file.
type Config struct {
	S3 S3Config `yaml:"s3"`
}

// S3Config holds S3-compatible object storage settings. The fields follow
// common S3 vendor conventions so the same file works against AWS S3,
// MinIO, RustFS, and other S3-compatible services.
type S3Config struct {
	// Endpoint is the S3 service address, e.g. "s3.us-east-1.amazonaws.com"
	// or "minio.example.com". Leave empty for AWS default endpoints.
	Endpoint string `yaml:"endpoint"`
	// Region is the bucket region, e.g. "us-east-1".
	Region string `yaml:"region"`
	// Bucket is the name of the bucket to use.
	Bucket string `yaml:"bucket"`
	// AccessKeyID and SecretAccessKey are the static credentials.
	AccessKeyID     string `yaml:"access_key_id"`
	SecretAccessKey string `yaml:"secret_access_key"`
	// UseSSL controls whether HTTPS is used to reach the endpoint.
	// Defaults to true; set false for plain-HTTP local services.
	UseSSL *bool `yaml:"use_ssl"`
	// ForcePathStyle uses path-style addressing (endpoint/bucket/key)
	// instead of virtual-hosted style. Required by MinIO and many
	// on-premise S3-compatible services.
	ForcePathStyle bool `yaml:"force_path_style"`
	// Prefix is prepended to every object key so multiple projects can
	// share one bucket. Defaults to "turnhive".
	Prefix string `yaml:"prefix"`
}

// Secure reports whether HTTPS should be used; it defaults to true.
func (c S3Config) Secure() bool {
	return c.UseSSL == nil || *c.UseSSL
}

// Load reads and validates the configuration file at path.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}

	cfg := &Config{}
	if err = yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}

	cfg.setDefaults()
	if err = cfg.validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}
	return cfg, nil
}

func (c *Config) setDefaults() {
	if c.S3.Prefix == "" {
		c.S3.Prefix = DefaultPrefix
	}
}

func (c *Config) validate() error {
	if c.S3.Bucket == "" {
		return fmt.Errorf("s3.bucket is required")
	}
	c.S3.Prefix = strings.Trim(c.S3.Prefix, "/")
	if strings.Contains(c.S3.Prefix, "//") {
		return fmt.Errorf("s3.prefix must not contain consecutive slashes")
	}
	return nil
}
