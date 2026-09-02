package controller

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestProxyRejectsInvalidAddress(t *testing.T) {
	c := &Controller{}
	// Garbage from etcd (a misbehaving or outdated node) must not take
	// the process down.
	for _, addr := range []string{"://bad", "not-a-url", ""} {
		if _, err := c.proxy(addr); err == nil {
			t.Fatalf("proxy(%q) must fail", addr)
		}
	}
	p, err := c.proxy("http://127.0.0.1:8080")
	if err != nil || p == nil {
		t.Fatalf("proxy(valid) = %v, %v", p, err)
	}
	// The valid proxy is cached.
	if p2, err := c.proxy("http://127.0.0.1:8080"); err != nil || p2 != p {
		t.Fatal("proxy must be cached per address")
	}
}

func TestWriteSSESyncHasNoFrameID(t *testing.T) {
	var buf bytes.Buffer
	writeSSESync(&buf, "turn-1", 7, []syncMessage{{Role: "user", Content: "hi"}}, nil)
	out := buf.String()
	// A control frame must not occupy an event sequence number; the seq
	// lives in the payload only.
	if strings.HasPrefix(out, "id:") || strings.Contains(out, "\nid:") {
		t.Fatalf("sync frame must not carry an SSE id: %q", out)
	}
	if !strings.HasPrefix(out, "event: sync\n") || !strings.Contains(out, `"seq":7`) {
		t.Fatalf("unexpected sync frame: %q", out)
	}
}

func TestHandleCancelTurn(t *testing.T) {
	c := &Controller{}
	mux := http.NewServeMux()
	c.RegisterRoutes(mux)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	sess := &Session{ID: "s", hub: newEventHub()}
	c.sessions.Store("s", sess)

	// Idle session: cancelling is a 409.
	resp, err := http.Post(ts.URL+"/v1/sessions/s/cancel", "", nil)
	if err != nil {
		t.Fatalf("POST cancel (idle): %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("idle cancel: status = %d, want 409", resp.StatusCode)
	}

	// A running turn, whose cancellation finishes the turn on its own
	// goroutine (as the turn goroutine's defer would) — never
	// synchronously, which would deadlock on the session lock inside
	// CancelTurn.
	sess.startTurn("turn-x", func() {
		go func() {
			time.Sleep(5 * time.Millisecond)
			sess.finishTurn()
		}()
	})
	resp, err = http.Post(ts.URL+"/v1/sessions/s/cancel", "", nil)
	if err != nil {
		t.Fatalf("POST cancel: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("cancel: status = %d, body %s", resp.StatusCode, body)
	}
	if !strings.Contains(string(body), `"turn-x"`) {
		t.Fatalf("cancel body should carry the turn id: %s", body)
	}
	// The handler returned only after finishTurn, so the session is idle
	// and a resend would not hit session_busy.
	if sess.TurnID() != "" {
		t.Fatalf("session must be idle after cancel, running %q", sess.TurnID())
	}
	if !sess.startTurn("turn-y", func() {}) {
		t.Fatal("resend after cancel must not hit session_busy")
	}
}
