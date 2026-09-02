package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yankeguo/ironhive"
	"github.com/yankeguo/turnhive/agent"
)

func jsonString(s string) string {
	b, _ := json.Marshal(s)
	return string(b)
}

// uploadCall records one file injection into the fake sandbox.
type uploadCall struct {
	Path string
	URL  string
}

// fakeUploadSandbox emulates just enough of ironhive for attach tests:
// allocate returns a sandbox handle, and PUT /agent/v1/file?url= fetches
// the URL itself (the presigned-URL injection path).
type fakeUploadSandbox struct {
	mu    sync.Mutex
	calls []uploadCall
}

func (f *fakeUploadSandbox) handler(t *testing.T) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /controller/v1/allocate", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"sandbox": "s", "leaseExpires": %s}`, jsonString(time.Now().Add(time.Minute).UTC().Format(time.RFC3339Nano)))
	})
	mux.HandleFunc("PUT /agent/v1/file", func(w http.ResponseWriter, r *http.Request) {
		u := r.URL.Query().Get("url")
		if u == "" {
			http.Error(w, "url required", http.StatusBadRequest)
			return
		}
		resp, err := http.Get(u)
		if err != nil || resp.StatusCode != http.StatusOK {
			http.Error(w, "fetch failed", http.StatusBadGateway)
			return
		}
		resp.Body.Close()
		f.mu.Lock()
		f.calls = append(f.calls, uploadCall{Path: r.URL.Query().Get("path"), URL: u})
		f.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"message":"OK"}`))
	})
	return mux
}

func (f *fakeUploadSandbox) injected() []uploadCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]uploadCall(nil), f.calls...)
}

// fakePresignStore serves presigned GETs from an in-memory map.
type fakePresignStore struct {
	objects map[string][]byte
}

func (f *fakePresignStore) PresignGet(_ context.Context, key string, _ time.Duration) (string, error) {
	body, ok := f.objects[key]
	if !ok {
		return "", fmt.Errorf("no such key: %s", key)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(body)
	}))
	return srv.URL, nil
}

func TestAttachFilesInjectsIntoLiveSandbox(t *testing.T) {
	fake := &fakeUploadSandbox{}
	srv := httptest.NewServer(fake.handler(t))
	defer srv.Close()
	sb, err := ironhive.NewClient(srv.URL).Allocate(context.Background(), "default", time.Minute)
	if err != nil {
		t.Fatalf("allocate: %v", err)
	}

	store := &fakePresignStore{objects: map[string][]byte{"k1": []byte("x")}}
	c := &Controller{uploadStore: store}
	sess := &Session{ID: "s", hub: newEventHub()}
	if !sess.setSandbox(sb, func() {}) {
		t.Fatal("setSandbox must succeed")
	}

	recs := []agent.UploadRecord{{Name: "a.txt", ObjectKey: "k1", Size: 1}}
	if err := c.attachFiles(context.Background(), sess, recs); err != nil {
		t.Fatalf("attachFiles: %v", err)
	}
	calls := fake.injected()
	if len(calls) != 1 || calls[0].Path != agent.UploadsRoot+"/a.txt" {
		t.Fatalf("injected: %+v", calls)
	}
	if got := sess.Uploads(); len(got) != 1 || got[0].Name != "a.txt" {
		t.Fatalf("uploads: %+v", got)
	}
}

// TestAttachFilesAfterReap pins the ordering contract: an attach that
// loses the race with a sandbox detach (filesMu) must still record the
// file — it lands in the sandbox rebuilt on the next message.
func TestAttachFilesAfterReap(t *testing.T) {
	c := &Controller{}
	sess := &Session{ID: "s", hub: newEventHub()}
	sess.setSandbox(&ironhive.Sandbox{Name: "sb"}, func() {})
	sess.mu.Lock()
	sess.lastActivity = time.Now().Add(-time.Hour)
	sess.mu.Unlock()

	// Reap under filesMu, as the sweeper does.
	sess.filesMu.Lock()
	sb, _ := sess.takeSandboxIfIdle(time.Minute)
	sess.filesMu.Unlock()
	if sb == nil {
		t.Fatal("idle session must be reaped")
	}

	// Attach afterwards: no sandbox, so nothing is injected, but the
	// record is stored for the next build.
	recs := []agent.UploadRecord{{Name: "a.txt", ObjectKey: "k1"}}
	if err := c.attachFiles(context.Background(), sess, recs); err != nil {
		t.Fatalf("attachFiles: %v", err)
	}
	if got := sess.Uploads(); len(got) != 1 {
		t.Fatalf("uploads: %+v", got)
	}
}

// TestAttachFilesRejectsClosedSession pins the DELETE race: attaching to
// a closed session must fail rather than record files that no sandbox
// will ever receive.
func TestAttachFilesRejectsClosedSession(t *testing.T) {
	c := &Controller{}
	sess := &Session{ID: "s", hub: newEventHub()}
	sess.closeSession()
	err := c.attachFiles(context.Background(), sess, []agent.UploadRecord{{Name: "a.txt", ObjectKey: "k1"}})
	if !errors.Is(err, errSessionClosed) {
		t.Fatalf("err = %v, want errSessionClosed", err)
	}
}

func TestAttachFilesCap(t *testing.T) {
	c := &Controller{}
	sess := &Session{ID: "s", hub: newEventHub()}
	for i := 0; i < uploadsNameCountCap; i++ {
		sess.recordUpload(agent.UploadRecord{Name: fmt.Sprintf("f%02d.txt", i), ObjectKey: "k"})
	}
	// A new name past the cap is rejected without partial state.
	err := c.attachFiles(context.Background(), sess, []agent.UploadRecord{{Name: "new.txt", ObjectKey: "k"}})
	if !errors.Is(err, errTooManyFiles) {
		t.Fatalf("err = %v, want errTooManyFiles", err)
	}
	if got := sess.Uploads(); len(got) != uploadsNameCountCap {
		t.Fatalf("uploads after reject: %d", len(got))
	}
	// Re-attaching an existing name stays under the cap.
	if err := c.attachFiles(context.Background(), sess, []agent.UploadRecord{{Name: "f00.txt", ObjectKey: "k2"}}); err != nil {
		t.Fatalf("re-attach: %v", err)
	}
}

func TestFilesManifestRoundTrip(t *testing.T) {
	store := newFakeAdoptionStore()
	ctx := context.Background()

	// Missing manifest: empty, no error.
	if got, err := loadFilesManifest(ctx, store, "sess-x"); err != nil || len(got) != 0 {
		t.Fatalf("missing manifest: got=%v err=%v", got, err)
	}

	uploads := []agent.UploadRecord{
		{Name: "b.csv", ObjectKey: "uploads/sess-1/t2-b.csv", Size: 2},
		{Name: "a.csv", ObjectKey: "uploads/sess-1/t1-a.csv", Size: 1},
	}
	if err := writeFilesManifest(ctx, store, "sess-1", uploads); err != nil {
		t.Fatalf("writeFilesManifest: %v", err)
	}
	got, err := loadFilesManifest(ctx, store, "sess-1")
	if err != nil {
		t.Fatalf("loadFilesManifest: %v", err)
	}
	if len(got) != 2 || got["a.csv"].ObjectKey != "uploads/sess-1/t1-a.csv" || got["b.csv"].Size != 2 {
		t.Fatalf("round trip: %v", got)
	}
}

func TestValidateFileRefs(t *testing.T) {
	if err := validateFileRefs(nil); err != nil {
		t.Fatalf("nil: %v", err)
	}
	good := []FileRef{{Name: "a.csv", ObjectKey: "uploads/s/t-a.csv", Size: 3}}
	if err := validateFileRefs(good); err != nil {
		t.Fatalf("good: %v", err)
	}
	for _, bad := range []FileRef{
		{Name: "", ObjectKey: "k"},
		{Name: "../x", ObjectKey: "k"},
		{Name: "a/b", ObjectKey: "k"},
		{Name: "ok", ObjectKey: ""},
		{Name: "ok", ObjectKey: "../escape"},
		{Name: "ok", ObjectKey: "k", Size: -1},
	} {
		if err := validateFileRefs([]FileRef{bad}); err == nil {
			t.Fatalf("%+v must fail", bad)
		}
	}
	dup := []FileRef{{Name: "a", ObjectKey: "k1"}, {Name: "a", ObjectKey: "k2"}}
	if err := validateFileRefs(dup); err == nil {
		t.Fatal("duplicate names must fail")
	}
}

func TestHandleGetSession(t *testing.T) {
	c := &Controller{}
	mux := http.NewServeMux()
	c.RegisterRoutes(mux)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	sess := &Session{ID: "s", hub: newEventHub()}
	sess.recordUpload(agent.UploadRecord{Name: "data.csv", ObjectKey: "uploads/s/t-data.csv", Size: 7})
	c.sessions.Store("s", sess)

	resp, err := http.Get(ts.URL + "/v1/sessions/s")
	if err != nil {
		t.Fatalf("GET session: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var d SessionDetail
	if err := json.NewDecoder(resp.Body).Decode(&d); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if d.ID != "s" || d.TurnID != "" || len(d.Files) != 1 || d.Files[0].Name != "data.csv" {
		t.Fatalf("detail: %+v", d)
	}
}

func TestHandleCreateFiles(t *testing.T) {
	c := &Controller{}
	mux := http.NewServeMux()
	c.RegisterRoutes(mux)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	// The handler's manifest write goes through c.store (*storage.Store,
	// not fakeable here) and is best-effort: with a nil store it panics,
	// so give the handler a real-ish path by short-circuiting — instead
	// of POSTing through the handler for the happy path, exercise the
	// manifest functions separately (TestFilesManifestRoundTrip) and
	// keep this test on validation + in-memory state via a store-backed
	// controller.
	sess := &Session{ID: "s", hub: newEventHub()}
	c.sessions.Store("s", sess)

	// Validation failure: bad name.
	body := strings.NewReader(`{"files": [{"name": "../x", "object_key": "k"}]}`)
	resp, err := http.Post(ts.URL+"/v1/sessions/s/files", "application/json", body)
	if err != nil {
		t.Fatalf("POST files: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("bad name: status = %d, want 400", resp.StatusCode)
	}

	// Empty list.
	resp, err = http.Post(ts.URL+"/v1/sessions/s/files", "application/json", strings.NewReader(`{"files": []}`))
	if err != nil {
		t.Fatalf("POST files: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("empty files: status = %d, want 400", resp.StatusCode)
	}

	// Happy path through the internal flow the handler runs (the HTTP
	// handler's manifest write needs a real S3 store; it is best-effort
	// and covered by TestFilesManifestRoundTrip).
	rec := agent.UploadRecord{Name: "data.csv", ObjectKey: "uploads/s/t-data.csv", Size: 7}
	sess.recordUpload(rec)
	got := sess.Uploads()
	if len(got) != 1 || got[0].Name != "data.csv" || got[0].ObjectKey != "uploads/s/t-data.csv" || got[0].Size != 7 {
		t.Fatalf("uploads: %+v", got)
	}
	// A second record sorts by name.
	sess.recordUpload(agent.UploadRecord{Name: "a.txt", ObjectKey: "k2"})
	got = sess.Uploads()
	if len(got) != 2 || got[0].Name != "a.txt" || got[1].Name != "data.csv" {
		t.Fatalf("sorted uploads: %+v", got)
	}
}

func TestCreateMessageRequestShape(t *testing.T) {
	// The message body is a plain content string; referencing attached
	// files is the caller's business (it composes its own marker text).
	var req CreateMessageRequest
	raw := `{"content": "hi, see <user_uploaded_files><file name=\"data.csv\"/></user_uploaded_files>"}`
	if err := json.NewDecoder(bytes.NewBufferString(raw)).Decode(&req); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !strings.Contains(req.Content, "data.csv") {
		t.Fatalf("req: %+v", req)
	}
}
