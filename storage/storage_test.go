package storage

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yankeguo/turnhive/config"
)

func testConfig(endpoint string) config.S3Config {
	insecure := false
	return config.S3Config{
		Endpoint:        strings.TrimPrefix(endpoint, "http://"),
		Region:          "us-east-1",
		Bucket:          "test-bucket",
		AccessKeyID:     "test-access-key",
		SecretAccessKey: "test-secret-key",
		UseSSL:          &insecure,
		ForcePathStyle:  true,
		Prefix:          "turnhive",
	}
}

func TestKey(t *testing.T) {
	s := &Store{prefix: "turnhive"}
	if got := s.key("sessions/1/history.json"); got != "turnhive/sessions/1/history.json" {
		t.Errorf("unexpected prefixed key: %q", got)
	}

	s = &Store{prefix: ""}
	if got := s.key("sessions/1/history.json"); got != "sessions/1/history.json" {
		t.Errorf("unexpected unprefixed key: %q", got)
	}
}

func TestPresignGet(t *testing.T) {
	s, err := New(testConfig("127.0.0.1:9000"))
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	raw, err := s.PresignGet(context.Background(), "skills/demo.tar.gz", time.Hour)
	if err != nil {
		t.Fatalf("PresignGet: %v", err)
	}

	u, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("parse presigned URL: %v", err)
	}
	if u.Host != "127.0.0.1:9000" {
		t.Errorf("unexpected host: %q", u.Host)
	}
	if u.Path != "/test-bucket/turnhive/skills/demo.tar.gz" {
		t.Errorf("unexpected path: %q", u.Path)
	}
	q := u.Query()
	for _, param := range []string{"X-Amz-Algorithm", "X-Amz-Credential", "X-Amz-Signature", "X-Amz-Expires"} {
		if q.Get(param) == "" {
			t.Errorf("missing signature parameter %s in %q", param, raw)
		}
	}
	if q.Get("X-Amz-Expires") != "3600" {
		t.Errorf("unexpected expiry: %q", q.Get("X-Amz-Expires"))
	}
	if !strings.Contains(q.Get("X-Amz-Credential"), "test-access-key") {
		t.Errorf("credential does not carry the access key: %q", q.Get("X-Amz-Credential"))
	}
}

// fakeS3 is a minimal in-memory S3-compatible server handling path-style
// PUT and GET.
type fakeS3 struct {
	mu      sync.Mutex
	objects map[string][]byte
}

func newFakeS3() *fakeS3 {
	return &fakeS3{objects: map[string][]byte{}}
}

func (f *fakeS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Path-style addressing: /{bucket}/{key}.
	key := strings.TrimPrefix(r.URL.Path, "/test-bucket/")

	switch r.Method {
	case http.MethodPut:
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		f.mu.Lock()
		f.objects[key] = body
		f.mu.Unlock()
		w.Header().Set("ETag", `"etag"`)
		w.WriteHeader(http.StatusOK)
	case http.MethodGet:
		f.mu.Lock()
		body, ok := f.objects[key]
		f.mu.Unlock()
		if !ok {
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusNotFound)
			_, _ = io.WriteString(w, `<?xml version="1.0" encoding="UTF-8"?>`+
				`<Error><Code>NoSuchKey</Code><Message>The specified key does not exist.</Message></Error>`)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func TestPutGetObject(t *testing.T) {
	server := httptest.NewServer(newFakeS3())
	defer server.Close()

	s, err := New(testConfig(server.URL))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx := context.Background()

	if err = s.PutObject(ctx, "sessions/1/history.json", []byte(`{"hello":"world"}`)); err != nil {
		t.Fatalf("PutObject: %v", err)
	}

	body, err := s.GetObject(ctx, "sessions/1/history.json")
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	if string(body) != `{"hello":"world"}` {
		t.Errorf("unexpected body: %q", body)
	}

	// Overwrite.
	if err = s.PutObject(ctx, "sessions/1/history.json", []byte("updated")); err != nil {
		t.Fatalf("PutObject overwrite: %v", err)
	}
	if body, err = s.GetObject(ctx, "sessions/1/history.json"); err != nil || string(body) != "updated" {
		t.Errorf("overwrite: body=%q err=%v", body, err)
	}

	if _, err = s.GetObject(ctx, "missing"); !errors.Is(err, ErrNotExist) {
		t.Errorf("missing object: expected ErrNotExist, got %v", err)
	}
}
