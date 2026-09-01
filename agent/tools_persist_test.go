package agent

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"
)

// fakePersistStore records PutObject calls and serves PresignGet
// downloads from its own httptest server (the sandbox fetches objects
// from there, like a real S3 presigned URL).
type fakePersistStore struct {
	objects map[string][]byte
	srv     *httptest.Server
}

func newFakePersistStore(t *testing.T) *fakePersistStore {
	t.Helper()
	f := &fakePersistStore{objects: map[string][]byte{}}
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Query().Get("key")
		switch r.Method {
		case http.MethodPut: // presigned upload
			body, _ := io.ReadAll(r.Body)
			f.objects[key] = body
			w.WriteHeader(http.StatusOK)
		case http.MethodGet: // presigned download
			body, ok := f.objects[key]
			if !ok {
				http.Error(w, "not found", http.StatusNotFound)
				return
			}
			_, _ = w.Write(body)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	}))
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakePersistStore) PresignPut(_ context.Context, key string, _ time.Duration) (string, error) {
	return f.srv.URL + "/put?key=" + url.QueryEscape(key), nil
}

func (f *fakePersistStore) PresignGet(_ context.Context, key string, _ time.Duration) (string, error) {
	return f.srv.URL + "/get?key=" + url.QueryEscape(key), nil
}

func TestSandboxPersist(t *testing.T) {
	sb, _ := newFakeIronhive(t)
	st := newSandboxTools(sb)
	tools := st.list(false)

	if _, err := callTool(t, tools, "write", `{"file_path": "report/final.txt", "content": "quarterly results"}`); err != nil {
		t.Fatalf("write: %v", err)
	}

	store := newFakePersistStore(t)
	var recorded []PersistedObject
	tool := sandboxPersist{
		t:           st,
		store:       store,
		sessionID:   "sess-test",
		onPersisted: func(o PersistedObject) { recorded = append(recorded, o) },
	}

	out, err := tool.Execute(context.Background(), "c1", json.RawMessage(`{"file_path": "report/final.txt"}`))
	if err != nil {
		t.Fatalf("persist: %v", err)
	}
	const wantKey = "sessions/sess-test/persisted/report/final.txt"
	if !strings.Contains(out, wantKey) {
		t.Fatalf("output missing object key: %q", out)
	}
	if string(store.objects[wantKey]) != "quarterly results" {
		t.Fatalf("uploaded content mismatch: %q", store.objects[wantKey])
	}
	if len(recorded) != 1 || recorded[0].ObjectKey != wantKey || recorded[0].Path != "report/final.txt" || recorded[0].Size != int64(len("quarterly results")) || recorded[0].At.IsZero() {
		t.Fatalf("unexpected recorded object: %+v", recorded)
	}

	// Missing file is an error, nothing recorded.
	if _, err := tool.Execute(context.Background(), "c2", json.RawMessage(`{"file_path": "ghost.txt"}`)); err == nil {
		t.Fatalf("expected error for missing file")
	}
	if len(recorded) != 1 {
		t.Fatalf("hook must not fire on failure: %+v", recorded)
	}
}

func TestLoopPersistRegistered(t *testing.T) {
	sb, _ := newFakeIronhive(t)
	fs := &fakeStream{}
	fs.textReply("ok")

	// With a PersistStore the persist tool is advertised...
	l := newTestLoop(LoopConfig{ModelName: "m", Sandbox: sb, PersistStore: newFakePersistStore(t), SessionID: "sess-x"}, fs)
	if err := l.RunTurn(context.Background(), "hi", &fakeReporter{}); err != nil {
		t.Fatalf("RunTurn: %v", err)
	}
	found := false
	for _, td := range fs.lastRequest().Tools {
		if td.Name == "persist" {
			found = true
		}
	}
	if !found {
		t.Fatal("persist tool not advertised with PersistStore set")
	}

	// ...and without one it is not.
	fs2 := &fakeStream{}
	fs2.textReply("ok")
	l2 := newTestLoop(LoopConfig{ModelName: "m", Sandbox: sb}, fs2)
	if err := l2.RunTurn(context.Background(), "hi", &fakeReporter{}); err != nil {
		t.Fatalf("RunTurn: %v", err)
	}
	for _, td := range fs2.lastRequest().Tools {
		if td.Name == "persist" {
			t.Fatal("persist tool must not be advertised without PersistStore")
		}
	}
}

func TestRestorePersisted(t *testing.T) {
	sb, f := newFakeIronhive(t)
	store := newFakePersistStore(t)
	store.objects["sessions/sess-x/persisted/report/final.txt"] = []byte("final results")
	store.objects["sessions/sess-x/persisted/tmp/cache.json"] = []byte(`{"step":3}`)
	objects := []PersistedObject{
		{Path: "report/final.txt", ObjectKey: "sessions/sess-x/persisted/report/final.txt"},
		{Path: "tmp/cache.json", ObjectKey: "sessions/sess-x/persisted/tmp/cache.json"},
	}

	if err := RestorePersisted(context.Background(), sb, store, objects, time.Minute); err != nil {
		t.Fatalf("RestorePersisted: %v", err)
	}
	for path, want := range map[string]string{
		"report/final.txt": "final results",
		"tmp/cache.json":   `{"step":3}`,
	} {
		got, err := readFileString(f.local(path))
		if err != nil || got != want {
			t.Errorf("restored %s = %q, %v; want %q", path, got, err, want)
		}
	}

	// A missing object aborts the restore.
	if err := RestorePersisted(context.Background(), sb, store, []PersistedObject{{Path: "x", ObjectKey: "ghost"}}, time.Minute); err == nil {
		t.Fatal("expected error for missing object")
	}
}

func readFileString(path string) (string, error) {
	b, err := os.ReadFile(path)
	return string(b), err
}
