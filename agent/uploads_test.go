package agent

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

// fakeUploadStore serves presigned GET URLs from an in-memory object
// map; each presign spins up a throwaway httptest server (leaked until
// the test process exits, which is fine for tests).
type fakeUploadStore struct {
	objects map[string][]byte
}

func (f *fakeUploadStore) PresignGet(_ context.Context, key string, _ time.Duration) (string, error) {
	body, ok := f.objects[key]
	if !ok {
		return "", fmt.Errorf("no such key: %s", key)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(body)
	}))
	return srv.URL, nil
}

func TestInjectUploads(t *testing.T) {
	sb, f := newFakeIronhive(t)

	store := &fakeUploadStore{objects: map[string][]byte{
		"uploads/sess-1/abc-data.csv": []byte("a,b,c"),
	}}
	uploads := []UploadRecord{
		{Name: "data.csv", ObjectKey: "uploads/sess-1/abc-data.csv", Size: 5, At: time.Now().UTC()},
	}
	if err := InjectUploads(context.Background(), sb, store, uploads, time.Minute); err != nil {
		t.Fatalf("InjectUploads: %v", err)
	}
	got, err := os.ReadFile(f.local("/" + UploadsRoot + "/data.csv"))
	if err != nil {
		t.Fatalf("read injected file: %v", err)
	}
	if string(got) != "a,b,c" {
		t.Fatalf("injected content = %q", got)
	}

	// A missing object fails the injection.
	err = InjectUploads(context.Background(), sb, store, []UploadRecord{{Name: "x.txt", ObjectKey: "nope"}}, time.Minute)
	if err == nil || !strings.Contains(err.Error(), "inject file x.txt") {
		t.Fatalf("missing object: err = %v", err)
	}
}

func TestUploadFileNamePattern(t *testing.T) {
	for _, good := range []string{"a", "data.csv", "report.final.pdf", "A1-2_3.txt"} {
		if !UploadFileNamePattern.MatchString(good) {
			t.Fatalf("%q must match", good)
		}
	}
	for _, bad := range []string{"", "../x", "a/b", "/abs", ".hidden", "_q", " spaces "} {
		if UploadFileNamePattern.MatchString(bad) {
			t.Fatalf("%q must not match", bad)
		}
	}
}
