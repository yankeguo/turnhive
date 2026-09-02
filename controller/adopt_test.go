package controller

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/yankeguo/turnhive/storage"
)

// fakeAdoptionStore is an in-memory adoptionStore.
type fakeAdoptionStore struct {
	objects map[string][]byte
}

func newFakeAdoptionStore() *fakeAdoptionStore {
	return &fakeAdoptionStore{objects: map[string][]byte{}}
}

func (f *fakeAdoptionStore) GetObject(_ context.Context, key string) ([]byte, error) {
	body, ok := f.objects[key]
	if !ok {
		return nil, storage.ErrNotExist
	}
	return body, nil
}

func (f *fakeAdoptionStore) PutObject(_ context.Context, key string, body []byte) error {
	f.objects[key] = body
	return nil
}

func (f *fakeAdoptionStore) ListObjects(_ context.Context, prefix string) ([]storage.ObjectInfo, error) {
	var out []storage.ObjectInfo
	for key, body := range f.objects {
		if strings.HasPrefix(key, prefix) {
			out = append(out, storage.ObjectInfo{Key: key, Size: int64(len(body)), LastModified: time.Unix(100, 0)})
		}
	}
	return out, nil
}

func TestWriteAndLoadSessionSpec(t *testing.T) {
	store := newFakeAdoptionStore()
	ctx := context.Background()

	// Missing spec: not found, no error.
	if _, ok, err := loadSessionSpec(ctx, store, "sess-x"); err != nil || ok {
		t.Fatalf("missing spec: ok=%v err=%v", ok, err)
	}

	spec := validSessionRequest()
	spec.MCPServers = []MCPServerSpec{{Name: "fs", URL: "http://mcp.example.com", Transport: "sse"}}
	if err := writeSessionSpec(ctx, store, "sess-1", spec); err != nil {
		t.Fatalf("writeSessionSpec: %v", err)
	}
	got, ok, err := loadSessionSpec(ctx, store, "sess-1")
	if err != nil || !ok {
		t.Fatalf("loadSessionSpec: ok=%v err=%v", ok, err)
	}
	if got.Model.URL != spec.Model.URL || got.Ironhive.Pool != "default" ||
		len(got.MCPServers) != 1 || got.MCPServers[0].Transport != "sse" {
		t.Fatalf("spec did not round-trip: %+v", got)
	}

	// A spec corrupted outside turnhive fails loudly at adoption time.
	store.objects[specObjectKey("sess-bad")] = []byte(`{"model":{"url":""}}`)
	if _, _, err = loadSessionSpec(ctx, store, "sess-bad"); err == nil || !strings.Contains(err.Error(), "invalid") {
		t.Fatalf("expected invalid spec rejected, got %v", err)
	}
}

func TestListPersistedObjects(t *testing.T) {
	store := newFakeAdoptionStore()
	store.objects["sessions/s1/persisted/out/report.md"] = []byte("12345")
	store.objects["sessions/s1/persisted/chart.png"] = []byte("1234567")
	store.objects["sessions/s2/persisted/other.txt"] = []byte("x")

	persisted, err := listPersistedObjects(context.Background(), store, "s1")
	if err != nil {
		t.Fatalf("listPersistedObjects: %v", err)
	}
	if len(persisted) != 2 {
		t.Fatalf("expected 2 objects, got %+v", persisted)
	}
	obj := persisted["out/report.md"]
	if obj.ObjectKey != "sessions/s1/persisted/out/report.md" || obj.Size != 5 || obj.At != time.Unix(100, 0) {
		t.Fatalf("unexpected manifest entry: %+v", obj)
	}
}

func TestLoadAdoptableSession(t *testing.T) {
	store := newFakeAdoptionStore()
	ctx := context.Background()
	if err := writeSessionSpec(ctx, store, "sess-1", validSessionRequest()); err != nil {
		t.Fatalf("writeSessionSpec: %v", err)
	}
	store.objects["sessions/sess-1/persisted/a.txt"] = []byte("aa")

	sess, found, err := loadAdoptableSession(ctx, store, "sess-1")
	if err != nil || !found {
		t.Fatalf("loadAdoptableSession: found=%v err=%v", found, err)
	}
	if sess.ID != "sess-1" || sess.Spec.Ironhive.Pool != "default" {
		t.Fatalf("unexpected session: %+v", sess)
	}
	if got := sess.Persisted(); len(got) != 1 || got[0].Path != "a.txt" {
		t.Fatalf("unexpected persisted manifest: %+v", got)
	}

	// Unknown session: not found, no error.
	if _, found, err = loadAdoptableSession(ctx, store, "sess-nope"); err != nil || found {
		t.Fatalf("unknown session: found=%v err=%v", found, err)
	}
}
