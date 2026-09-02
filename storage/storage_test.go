package storage

import (
	"context"
	"errors"
	"fmt"
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
// PUT, GET, DELETE and bucket-level ListObjectsV2.
type fakeS3 struct {
	mu      sync.Mutex
	objects map[string][]byte
}

func newFakeS3() *fakeS3 {
	return &fakeS3{objects: map[string][]byte{}}
}

func (f *fakeS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Path-style addressing: /{bucket}/{key}; "/{bucket}" or "/{bucket}/"
	// is a bucket-level operation.
	if r.URL.Path == "/test-bucket" || r.URL.Path == "/test-bucket/" {
		if r.URL.Query().Get("list-type") == "2" {
			f.serveList(w, r)
			return
		}
		http.Error(w, "unsupported bucket operation", http.StatusBadRequest)
		return
	}
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
	case http.MethodDelete:
		f.mu.Lock()
		delete(f.objects, key)
		f.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// serveList answers ListObjectsV2 with every key under the requested
// prefix, unsorted (tests do not depend on order).
func (f *fakeS3) serveList(w http.ResponseWriter, r *http.Request) {
	prefix := r.URL.Query().Get("prefix")
	f.mu.Lock()
	var b strings.Builder
	for key, body := range f.objects {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		fmt.Fprintf(&b, "<Contents><Key>%s</Key><Size>%d</Size><LastModified>2026-09-02T00:00:00Z</LastModified></Contents>",
			key, len(body))
	}
	f.mu.Unlock()
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(w, `<?xml version="1.0" encoding="UTF-8"?>`+
		`<ListBucketResult><IsTruncated>false</IsTruncated>`+b.String()+`</ListBucketResult>`)
}

func TestNewEndpointWithScheme(t *testing.T) {
	cfg := testConfig("127.0.0.1:9000")
	cfg.Endpoint = "http://127.0.0.1:9000"
	if _, err := New(cfg); err == nil {
		t.Fatal("New: expected error for endpoint with scheme, got nil")
	} else if !strings.Contains(err.Error(), "must not include a scheme") {
		t.Errorf("error = %q, want it to reject the scheme", err)
	}
}

func TestGetObjectSizeLimit(t *testing.T) {
	fake := newFakeS3()
	fake.objects["turnhive/sessions/1/big.bin"] = make([]byte, maxGetObjectSize+1)
	server := httptest.NewServer(fake)
	defer server.Close()

	s, err := New(testConfig(server.URL))
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	_, err = s.GetObject(context.Background(), "sessions/1/big.bin")
	if err == nil {
		t.Fatal("GetObject: expected error for oversized object, got nil")
	}
	if !strings.Contains(err.Error(), "exceeds") {
		t.Errorf("error = %q, want it to mention the size limit", err)
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

func TestListAndDeleteObject(t *testing.T) {
	server := httptest.NewServer(newFakeS3())
	defer server.Close()

	s, err := New(testConfig(server.URL))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx := context.Background()

	for _, key := range []string{
		"sessions/s1/persisted/a.txt",
		"sessions/s1/persisted/dir/b.txt",
		"sessions/s2/persisted/c.txt",
	} {
		if err = s.PutObject(ctx, key, []byte("data-"+key)); err != nil {
			t.Fatalf("PutObject %s: %v", key, err)
		}
	}

	// List returns keys relative to the store prefix, filtered by prefix.
	objs, err := s.ListObjects(ctx, "sessions/s1/persisted/")
	if err != nil {
		t.Fatalf("ListObjects: %v", err)
	}
	if len(objs) != 2 {
		t.Fatalf("expected 2 objects, got %+v", objs)
	}
	for _, o := range objs {
		if strings.HasPrefix(o.Key, "turnhive/") {
			t.Errorf("key must be relative to the store prefix: %q", o.Key)
		}
		if o.Size != int64(len("data-"+o.Key)) {
			t.Errorf("unexpected size for %q: %d", o.Key, o.Size)
		}
		if o.LastModified.IsZero() {
			t.Errorf("LastModified not parsed for %q", o.Key)
		}
	}

	// Delete removes one object; deleting a missing object is not an error.
	if err = s.DeleteObject(ctx, "sessions/s1/persisted/a.txt"); err != nil {
		t.Fatalf("DeleteObject: %v", err)
	}
	if _, err = s.GetObject(ctx, "sessions/s1/persisted/a.txt"); !errors.Is(err, ErrNotExist) {
		t.Fatalf("expected deleted object gone, got %v", err)
	}
	if err = s.DeleteObject(ctx, "sessions/s1/persisted/a.txt"); err != nil {
		t.Fatalf("delete of a missing object must not fail: %v", err)
	}
}
