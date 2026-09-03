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

// fakeFileStore serves presigned GET URLs from an in-memory object
// map; each presign spins up a throwaway httptest server (leaked until
// the test process exits, which is fine for tests).
type fakeFileStore struct {
	objects map[string][]byte
}

func (f *fakeFileStore) PresignGet(_ context.Context, key string, _ time.Duration) (string, error) {
	body, ok := f.objects[key]
	if !ok {
		return "", fmt.Errorf("no such key: %s", key)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(body)
	}))
	return srv.URL, nil
}

func TestInjectFiles(t *testing.T) {
	sb, f := newFakeIronhive(t)

	store := &fakeFileStore{objects: map[string][]byte{
		"uploads/sess-1/abc-data.csv": []byte("a,b,c"),
	}}
	files := []FileRecord{
		{Name: "data.csv", ObjectKey: "uploads/sess-1/abc-data.csv", Size: 5, At: time.Now().UTC()},
	}
	if err := InjectFiles(context.Background(), sb, store, files, time.Minute); err != nil {
		t.Fatalf("InjectFiles: %v", err)
	}
	got, err := os.ReadFile(f.local("/" + FilesRoot + "/data.csv"))
	if err != nil {
		t.Fatalf("read injected file: %v", err)
	}
	if string(got) != "a,b,c" {
		t.Fatalf("injected content = %q", got)
	}

	// A missing object fails the injection.
	err = InjectFiles(context.Background(), sb, store, []FileRecord{{Name: "x.txt", ObjectKey: "nope"}}, time.Minute)
	if err == nil || !strings.Contains(err.Error(), "inject file x.txt") {
		t.Fatalf("missing object: err = %v", err)
	}
}

func TestFileNamePattern(t *testing.T) {
	for _, good := range []string{"a", "data.csv", "report.final.pdf", "A1-2_3.txt"} {
		if !FileNamePattern.MatchString(good) {
			t.Fatalf("%q must match", good)
		}
	}
	for _, bad := range []string{"", "../x", "a/b", "/abs", ".hidden", "_q", " spaces "} {
		if FileNamePattern.MatchString(bad) {
			t.Fatalf("%q must not match", bad)
		}
	}
}
