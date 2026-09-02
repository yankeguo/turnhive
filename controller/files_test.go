package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/yankeguo/turnhive/agent"
)

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
