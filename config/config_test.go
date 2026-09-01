package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// minimalValidYAML is the smallest configuration that passes validation;
// every rejection test below breaks exactly one rule from this baseline.
const minimalValidYAML = `
s3:
  bucket: my-bucket
etcd:
  endpoints:
    - "http://127.0.0.1:2379"
ironhive:
  url: "http://127.0.0.1:30173"
`

// writeConfig writes doc to a temp file and returns its path.
func writeConfig(t *testing.T, doc string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yml")
	if err := os.WriteFile(path, []byte(doc), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}

func TestLoadDefaults(t *testing.T) {
	cfg, err := Load(writeConfig(t, minimalValidYAML))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	checks := []struct {
		name string
		got  any
		want any
	}{
		{"listen", cfg.Listen, DefaultListen},
		{"node.advertise", cfg.Node.Advertise, "http://127.0.0.1:8080"},
		{"s3.prefix", cfg.S3.Prefix, DefaultPrefix},
		{"s3.secure", cfg.S3.Secure(), true},
		{"etcd.dial_timeout", time.Duration(cfg.Etcd.DialTimeout), DefaultEtcdDialTimeout},
		{"etcd.prefix", cfg.Etcd.Prefix, DefaultPrefix},
		{"etcd.lease_ttl", time.Duration(cfg.Etcd.LeaseTTL), DefaultEtcdLeaseTTL},
		{"ironhive.lease", time.Duration(cfg.Ironhive.Lease), DefaultIronhiveLease},
		{"session.idle_timeout", time.Duration(cfg.Session.IdleTimeout), DefaultSessionIdleTimeout},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s = %v, want %v", c.name, c.got, c.want)
		}
	}
	if cfg.Node.ID == "" {
		t.Error("node.id default is empty")
	}
}

func TestLoadPrefixTrimmed(t *testing.T) {
	doc := `
s3:
  bucket: my-bucket
  prefix: /turnhive/
etcd:
  endpoints:
    - "http://127.0.0.1:2379"
  prefix: /cluster/
ironhive:
  url: "http://127.0.0.1:30173"
`
	cfg, err := Load(writeConfig(t, doc))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.S3.Prefix != "turnhive" {
		t.Errorf("s3.prefix = %q, want %q", cfg.S3.Prefix, "turnhive")
	}
	if cfg.Etcd.Prefix != "cluster" {
		t.Errorf("etcd.prefix = %q, want %q", cfg.Etcd.Prefix, "cluster")
	}
}

func TestLoadRejects(t *testing.T) {
	cases := []struct {
		name    string
		doc     string
		wantErr string
	}{
		{
			name:    "listen without port",
			doc:     "listen: \"8080\"\n" + minimalValidYAML,
			wantErr: "listen must be in host:port form",
		},
		{
			name:    "advertise without scheme",
			doc:     "node:\n  advertise: \"10.0.0.1:8080\"\n" + minimalValidYAML,
			wantErr: "node.advertise must be an http(s)://host:port URL",
		},
		{
			name:    "missing bucket",
			doc:     "etcd:\n  endpoints:\n    - \"http://127.0.0.1:2379\"\nironhive:\n  url: \"http://127.0.0.1:30173\"\n",
			wantErr: "s3.bucket is required",
		},
		{
			name: "s3 prefix with consecutive slashes",
			doc: strings.Replace(minimalValidYAML, "bucket: my-bucket",
				"bucket: my-bucket\n  prefix: a//b", 1),
			wantErr: "s3.prefix must not contain consecutive slashes",
		},
		{
			name:    "missing etcd endpoints",
			doc:     "s3:\n  bucket: my-bucket\nironhive:\n  url: \"http://127.0.0.1:30173\"\n",
			wantErr: "etcd.endpoints is required",
		},
		{
			name: "etcd prefix with consecutive slashes",
			doc: strings.Replace(minimalValidYAML, "\"http://127.0.0.1:2379\"",
				"\"http://127.0.0.1:2379\"\n  prefix: a//b", 1),
			wantErr: "etcd.prefix must not contain consecutive slashes",
		},
		{
			name: "lease_ttl below one second",
			doc: strings.Replace(minimalValidYAML, "\"http://127.0.0.1:2379\"",
				"\"http://127.0.0.1:2379\"\n  lease_ttl: 500ms", 1),
			wantErr: "etcd.lease_ttl must be at least 1s",
		},
		{
			name: "tls cert without key",
			doc: strings.Replace(minimalValidYAML, "\"http://127.0.0.1:2379\"",
				"\"http://127.0.0.1:2379\"\n  tls:\n    cert_file: /tmp/cert.pem", 1),
			wantErr: "etcd.tls.cert_file and etcd.tls.key_file must be set together",
		},
		{
			name:    "missing ironhive url",
			doc:     "s3:\n  bucket: my-bucket\netcd:\n  endpoints:\n    - \"http://127.0.0.1:2379\"\n",
			wantErr: "ironhive.url is required",
		},
		{
			name: "ironhive url with non-http scheme",
			doc: strings.Replace(minimalValidYAML, "\"http://127.0.0.1:30173\"",
				"\"ftp://127.0.0.1:30173\"", 1),
			wantErr: "ironhive.url must be an http(s):// URL",
		},
		{
			name: "negative ironhive lease",
			doc: strings.Replace(minimalValidYAML, "\"http://127.0.0.1:30173\"",
				"\"http://127.0.0.1:30173\"\n  lease: -1s", 1),
			wantErr: "ironhive.lease must be positive",
		},
		{
			name:    "negative idle timeout",
			doc:     minimalValidYAML + "session:\n  idle_timeout: -1s\n",
			wantErr: "session.idle_timeout must be positive",
		},
		{
			name:    "unknown field rejected in strict mode",
			doc:     minimalValidYAML + "session:\n  idle_timetout: 30m\n",
			wantErr: "field idle_timetout not found",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := Load(writeConfig(t, c.doc))
			if err == nil {
				t.Fatalf("Load: expected error containing %q, got nil", c.wantErr)
			}
			if !strings.Contains(err.Error(), c.wantErr) {
				t.Errorf("Load error = %q, want it to contain %q", err, c.wantErr)
			}
		})
	}
}
