// Package config loads and validates the turnhive configuration file.
package config

import (
	"fmt"
	"net"
	"net/url"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// DefaultEtcdPrefix is the default etcd key prefix for node and session
// records.
const DefaultEtcdPrefix = "turnhive"

// DefaultEtcdDialTimeout is the default timeout for establishing a
// connection to an etcd endpoint.
const DefaultEtcdDialTimeout = 5 * time.Second

// DefaultEtcdLeaseTTL is the default TTL of the lease a node registers
// itself and its sessions under.
const DefaultEtcdLeaseTTL = 10 * time.Second

// DefaultListen is the default address the HTTP server listens on.
const DefaultListen = ":8080"

// Duration is a time.Duration that unmarshals from YAML strings like "5s".
type Duration time.Duration

// UnmarshalYAML implements yaml.Unmarshaler.
func (d *Duration) UnmarshalYAML(value *yaml.Node) error {
	v, err := time.ParseDuration(value.Value)
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", value.Value, err)
	}
	*d = Duration(v)
	return nil
}

// Config is the root of the turnhive configuration file.
type Config struct {
	// Listen is the address the HTTP server binds to, e.g. ":8080".
	// Defaults to ":8080".
	Listen string     `yaml:"listen"`
	Node   NodeConfig `yaml:"node"`
	S3     S3Config   `yaml:"s3"`
	Etcd   EtcdConfig `yaml:"etcd"`
}

// NodeConfig holds the identity of this cluster node, used for service
// discovery and session routing in etcd.
type NodeConfig struct {
	// ID uniquely identifies this node in the cluster. Defaults to
	// "<hostname>-<pid>".
	ID string `yaml:"id"`
	// Advertise is the base URL other nodes use to forward session
	// requests to this node, e.g. "http://10.0.0.1:8080". Defaults to
	// "http://127.0.0.1:<listen port>".
	Advertise string `yaml:"advertise"`
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
}

// Secure reports whether HTTPS should be used; it defaults to true.
func (c S3Config) Secure() bool {
	return c.UseSSL == nil || *c.UseSSL
}

// EtcdConfig holds etcd client connection settings following the
// recommendations of the official etcd clientv3 package.
type EtcdConfig struct {
	// Endpoints is the list of etcd server addresses,
	// e.g. ["https://127.0.0.1:2379"]. At least one is required.
	Endpoints []string `yaml:"endpoints"`
	// DialTimeout is the timeout for establishing a connection to an
	// endpoint. Defaults to 5s.
	DialTimeout Duration `yaml:"dial_timeout"`
	// Username and Password enable etcd authentication. Leave both empty
	// for clusters without auth.
	Username string `yaml:"username"`
	Password string `yaml:"password"`
	// TLS holds client certificate settings. Leave empty for plaintext
	// endpoints.
	TLS TLSConfig `yaml:"tls"`
	// Prefix is prepended to every etcd key so multiple clusters can
	// share one etcd. Defaults to "turnhive".
	Prefix string `yaml:"prefix"`
	// LeaseTTL is the TTL of the lease a node registers itself and its
	// sessions under. A node that loses its keepalive disappears from
	// the cluster after this duration. Defaults to 10s.
	LeaseTTL Duration `yaml:"lease_ttl"`
}

// TLSConfig holds the file paths needed to build a TLS transport.
type TLSConfig struct {
	// CertFile and KeyFile are the client certificate pair; they must be
	// set together.
	CertFile string `yaml:"cert_file"`
	KeyFile  string `yaml:"key_file"`
	// CAFile is the trusted CA bundle used to verify the server.
	CAFile string `yaml:"ca_file"`
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
	if c.Listen == "" {
		c.Listen = DefaultListen
	}
	if c.Node.ID == "" {
		hostname, err := os.Hostname()
		if err != nil {
			hostname = "unknown"
		}
		c.Node.ID = fmt.Sprintf("%s-%d", hostname, os.Getpid())
	}
	if c.Node.Advertise == "" {
		_, port, err := net.SplitHostPort(c.Listen)
		if err != nil || port == "" {
			port = strings.TrimPrefix(DefaultListen, ":")
		}
		c.Node.Advertise = "http://127.0.0.1:" + port
	}
	if c.Etcd.DialTimeout == 0 {
		c.Etcd.DialTimeout = Duration(DefaultEtcdDialTimeout)
	}
	if c.Etcd.Prefix == "" {
		c.Etcd.Prefix = DefaultEtcdPrefix
	}
	if c.Etcd.LeaseTTL == 0 {
		c.Etcd.LeaseTTL = Duration(DefaultEtcdLeaseTTL)
	}
}

func (c *Config) validate() error {
	if _, _, err := net.SplitHostPort(c.Listen); err != nil {
		return fmt.Errorf("listen must be in host:port form: %v", err)
	}
	if u, err := url.Parse(c.Node.Advertise); err != nil ||
		(u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return fmt.Errorf("node.advertise must be an http(s)://host:port URL")
	}
	if c.S3.Bucket == "" {
		return fmt.Errorf("s3.bucket is required")
	}
	if len(c.Etcd.Endpoints) == 0 {
		return fmt.Errorf("etcd.endpoints is required")
	}
	c.Etcd.Prefix = strings.Trim(c.Etcd.Prefix, "/")
	if strings.Contains(c.Etcd.Prefix, "//") {
		return fmt.Errorf("etcd.prefix must not contain consecutive slashes")
	}
	if c.Etcd.LeaseTTL <= 0 {
		return fmt.Errorf("etcd.lease_ttl must be positive")
	}
	if (c.Etcd.TLS.CertFile == "") != (c.Etcd.TLS.KeyFile == "") {
		return fmt.Errorf("etcd.tls.cert_file and etcd.tls.key_file must be set together")
	}
	return nil
}
