package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yankeguo/ironhive"
)

// fakeIronhive emulates the ironhive controller + sandbox agent over
// httptest, backed by a real temporary directory: in-sandbox absolute
// path "/x/y" maps to <root>/x/y; relative paths land in <root> (the
// sandbox working directory).
type fakeIronhive struct {
	root string
	srv  *httptest.Server

	mu       sync.Mutex
	tarCalls []fakeTarCall
	// lastShell records the form params of the most recent shell call.
	lastShellCwd       string
	lastShellEnv       []string
	lastShellStrictEnv string
}

// fakeTarCall records one PUT /agent/v1/tar request.
type fakeTarCall struct {
	Path string
	URL  string
}

// local maps an in-sandbox absolute path to the backing directory.
func (f *fakeIronhive) local(p string) string {
	return filepath.Join(f.root, strings.TrimPrefix(p, "/"))
}

// writeError mirrors the agent's {"message": ...} error envelope.
func (f *fakeIronhive) writeError(w http.ResponseWriter, msg string, code int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	fmt.Fprintf(w, `{"message": %s}`, jsonString(msg))
}

func jsonString(s string) string {
	b, _ := json.Marshal(s)
	return string(b)
}

// newFakeIronhive starts the emulated controller and allocates a sandbox
// through the real SDK.
func newFakeIronhive(t *testing.T) (*ironhive.Sandbox, *fakeIronhive) {
	t.Helper()
	f := &fakeIronhive{root: t.TempDir()}
	if err := os.MkdirAll(f.local("/workspace"), 0o755); err != nil {
		t.Fatal(err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /controller/v1/allocate", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"sandbox": "s", "leaseExpires": %s}`, jsonString(time.Now().Add(time.Minute).UTC().Format(time.RFC3339Nano)))
	})
	mux.HandleFunc("/agent/v1/file", f.handleFile)
	mux.HandleFunc("/agent/v1/file/upload", f.handleFileUpload)
	mux.HandleFunc("/agent/v1/dir", f.handleDir)
	mux.HandleFunc("/agent/v1/shell", f.handleShell)
	mux.HandleFunc("/agent/v1/tar", f.handleTar)
	f.srv = httptest.NewServer(mux)
	t.Cleanup(f.srv.Close)

	sb, err := ironhive.NewClient(f.srv.URL).Allocate(context.Background(), "default", time.Minute)
	if err != nil {
		t.Fatalf("allocate: %v", err)
	}
	return sb, f
}

// handleFile serves GET (download) and PUT (upload) of /agent/v1/file.
func (f *fakeIronhive) handleFile(w http.ResponseWriter, r *http.Request) {
	p := r.URL.Query().Get("path")
	if p == "" {
		f.writeError(w, "path is required", http.StatusBadRequest)
		return
	}
	local := f.local(p)
	switch r.Method {
	case http.MethodGet:
		st, err := os.Stat(local)
		if err != nil {
			f.writeError(w, "not found: "+p, http.StatusNotFound)
			return
		}
		if st.IsDir() {
			f.writeError(w, "is a directory: "+p, http.StatusBadRequest)
			return
		}
		http.ServeFile(w, r, local)
	case http.MethodPut:
		// Content comes from the request body, or from a URL the sandbox
		// downloads itself (the presigned-URL injection path).
		var src io.ReadCloser
		if u := r.URL.Query().Get("url"); u != "" {
			resp, err := http.Get(u)
			if err != nil || resp.StatusCode != http.StatusOK {
				f.writeError(w, "fetch url failed: "+u, http.StatusBadGateway)
				return
			}
			src = resp.Body
		} else {
			src = r.Body
		}
		if err := os.MkdirAll(filepath.Dir(local), 0o755); err != nil {
			f.writeError(w, err.Error(), http.StatusInternalServerError)
			return
		}
		tmp := local + ".tmp"
		out, err := os.Create(tmp)
		if err != nil {
			f.writeError(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if _, err := copyAndClose(out, src); err != nil {
			f.writeError(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if err := os.Rename(tmp, local); err != nil {
			f.writeError(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"message": "ok"}`)
	default:
		f.writeError(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func copyAndClose(out *os.File, src io.ReadCloser) (int64, error) {
	defer out.Close()
	defer src.Close()
	n, err := out.ReadFrom(src)
	return n, err
}

// handleFileUpload serves POST /agent/v1/file/upload: the local file at
// path is streamed to the form's url with the given method (default
// POST), mirroring the real agent's upload endpoint.
func (f *fakeIronhive) handleFileUpload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		f.writeError(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if err := r.ParseForm(); err != nil {
		f.writeError(w, err.Error(), http.StatusBadRequest)
		return
	}
	p := r.Form.Get("path")
	target := r.Form.Get("url")
	method := r.Form.Get("method")
	if method == "" {
		method = http.MethodPost
	}
	if p == "" || target == "" {
		f.writeError(w, "path and url are required", http.StatusBadRequest)
		return
	}
	data, err := os.ReadFile(f.local(p))
	if err != nil {
		f.writeError(w, "not found: "+p, http.StatusNotFound)
		return
	}
	req, err := http.NewRequestWithContext(r.Context(), method, target, strings.NewReader(string(data)))
	if err != nil {
		f.writeError(w, err.Error(), http.StatusInternalServerError)
		return
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		f.writeError(w, "upload: "+err.Error(), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		f.writeError(w, "upload: upstream "+resp.Status, http.StatusBadGateway)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprint(w, `{"message": "OK"}`)
}

// handleDir serves GET /agent/v1/dir listings.
func (f *fakeIronhive) handleDir(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		f.writeError(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	p := r.URL.Query().Get("path")
	entries, err := os.ReadDir(f.local(p))
	if err != nil {
		f.writeError(w, "not a directory: "+p, http.StatusBadRequest)
		return
	}
	out := []ironhive.DirEntry{}
	for _, e := range entries {
		info, err := e.Info()
		if err != nil {
			continue
		}
		out = append(out, ironhive.DirEntry{
			Name:  e.Name(),
			Dir:   e.IsDir(),
			Size:  info.Size(),
			Mode:  "0644",
			Mtime: info.ModTime(),
		})
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(out)
}

// handleShell runs the command with bash in the mapped cwd and streams
// canned SSE events: one stdout/stderr event per line, then exit, then
// the cwd/env state events (mirroring the real agent so the harness can
// thread shell state across calls).
func (f *fakeIronhive) handleShell(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		f.writeError(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if err := r.ParseForm(); err != nil {
		f.writeError(w, err.Error(), http.StatusBadRequest)
		return
	}
	command := r.Form.Get("command")
	// cwd arrives as an in-sandbox path; empty means the sandbox working
	// directory ("/" in this emulation).
	sandboxCwd := r.Form.Get("cwd")
	if sandboxCwd == "" {
		sandboxCwd = "/"
	}
	f.mu.Lock()
	f.lastShellCwd = r.Form.Get("cwd")
	f.lastShellEnv = r.Form["env"]
	f.lastShellStrictEnv = r.Form.Get("strict_env")
	f.mu.Unlock()

	cmd := exec.CommandContext(r.Context(), "bash", "-c", command)
	cmd.Dir = f.local(sandboxCwd)
	var stdout, stderr strings.Builder
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	w.Header().Set("Content-Type", "text/event-stream")
	flusher, _ := w.(http.Flusher)
	emit := func(eventType, data string) {
		fmt.Fprintf(w, "event: %s\ndata: %s\n\n", eventType, jsonString(data))
		if flusher != nil {
			flusher.Flush()
		}
	}

	if err := cmd.Start(); err != nil {
		f.writeError(w, err.Error(), http.StatusInternalServerError)
		return
	}
	// The real agent reports the pid as the first event, right after
	// spawn.
	fmt.Fprintf(w, "event: pid\ndata: %d\n\n", cmd.Process.Pid)
	if flusher != nil {
		flusher.Flush()
	}

	runErr := cmd.Wait()
	exitCode := 0
	if runErr != nil {
		if ee, ok := runErr.(*exec.ExitError); ok {
			exitCode = ee.ExitCode()
		} else {
			f.writeError(w, runErr.Error(), http.StatusInternalServerError)
			return
		}
	}

	for _, line := range strings.Split(strings.TrimRight(stdout.String(), "\n"), "\n") {
		if line != "" {
			emit("stdout", line)
		}
	}
	for _, line := range strings.Split(strings.TrimRight(stderr.String(), "\n"), "\n") {
		if line != "" {
			emit("stderr", line)
		}
	}
	emit("exit", fmt.Sprintf("%d", exitCode))
	emit("cwd", sandboxCwd)
	// env event data is a JSON object of the full environment; the fake
	// reports a fixed minimal one.
	fmt.Fprintf(w, "event: env\ndata: %s\n\n", `{"PATH":"/usr/bin:/bin"}`)
}

// lastShellParams returns the recorded shell form params.
func (f *fakeIronhive) lastShellParams() (cwd string, env []string, strictEnv string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lastShellCwd, f.lastShellEnv, f.lastShellStrictEnv
}

// handleTar records PUT /agent/v1/tar requests (skills installation).
func (f *fakeIronhive) handleTar(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut {
		f.writeError(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	q := r.URL.Query()
	f.mu.Lock()
	f.tarCalls = append(f.tarCalls, fakeTarCall{Path: q.Get("path"), URL: q.Get("url")})
	f.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprint(w, `{"message": "ok"}`)
}

// lastTarCall returns the recorded tar calls.
func (f *fakeIronhive) recordedTarCalls() []fakeTarCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]fakeTarCall(nil), f.tarCalls...)
}
