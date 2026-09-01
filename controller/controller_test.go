package controller

import (
	"bytes"
	"strings"
	"testing"
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
