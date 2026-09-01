package controller

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/yankeguo/turnhive/agent"
)

// sseKeepalive is the interval between SSE comment lines that keep the
// connection alive through proxies.
const sseKeepalive = 15 * time.Second

// sseReporter implements agent.Reporter over a text/event-stream
// response. Headers are written lazily on the first event, so a turn
// that fails before producing anything (e.g. agent.ErrBusy) can still
// be answered with a plain JSON error.
type sseReporter struct {
	w http.ResponseWriter
	f http.Flusher

	mu          sync.Mutex
	wroteHeader bool
	stop        chan struct{}
	stopped     sync.WaitGroup
}

func newSSEReporter(w http.ResponseWriter) *sseReporter {
	rep := &sseReporter{w: w, stop: make(chan struct{})}
	if f, ok := w.(http.Flusher); ok {
		rep.f = f
	}
	rep.stopped.Add(1)
	go rep.keepalive()
	return rep
}

// keepalive emits comment lines until Close, so idle periods (long tool
// calls, external tool waits) do not trip proxy timeouts.
func (r *sseReporter) keepalive() {
	defer r.stopped.Done()
	ticker := time.NewTicker(sseKeepalive)
	defer ticker.Stop()
	for {
		select {
		case <-r.stop:
			return
		case <-ticker.C:
			r.mu.Lock()
			if r.wroteHeader {
				_, _ = fmt.Fprint(r.w, ": keepalive\n\n")
				r.flush()
			}
			r.mu.Unlock()
		}
	}
}

// Close stops the keepalive goroutine. No events may be sent afterwards.
func (r *sseReporter) Close() {
	close(r.stop)
	r.stopped.Wait()
}

// writeHeader emits the SSE response headers on first use.
func (r *sseReporter) writeHeader() {
	if r.wroteHeader {
		return
	}
	h := r.w.Header()
	h.Set("Content-Type", "text/event-stream; charset=utf-8")
	h.Set("Cache-Control", "no-cache")
	h.Set("Connection", "keep-alive")
	r.w.WriteHeader(http.StatusOK)
	r.wroteHeader = true
}

func (r *sseReporter) flush() {
	if r.f != nil {
		r.f.Flush()
	}
}

// event writes one SSE event with a JSON payload.
func (r *sseReporter) event(name string, payload any) {
	data, err := json.Marshal(payload)
	if err != nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.writeHeader()
	_, _ = fmt.Fprintf(r.w, "event: %s\ndata: %s\n\n", name, data)
	r.flush()
}

func (r *sseReporter) Delta(text string) {
	r.event("delta", map[string]string{"text": text})
}

func (r *sseReporter) ReasoningDelta(text string) {
	r.event("reasoning_delta", map[string]string{"text": text})
}

func (r *sseReporter) ToolCall(ev agent.ToolCallEvent) {
	r.event("tool_call", ev)
}

func (r *sseReporter) Done(text string) {
	r.event("done", map[string]string{"text": text})
}

func (r *sseReporter) Error(msg string) {
	r.event("error", map[string]string{"message": msg})
}
