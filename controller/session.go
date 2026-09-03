package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/yankeguo/ironhive"
	"github.com/yankeguo/turnhive/agent"
)

// Session is a session owned by this node.
type Session struct {
	ID   string
	Spec CreateSessionRequest
	// sandbox is the ironhive sandbox allocated for this session; access
	// it through the mu-guarded helpers below.
	sandbox *ironhive.Sandbox
	// hub sequences, buffers and fans out the session's events (see
	// hub.go).
	hub *eventHub

	mu sync.Mutex
	// loop runs the agent turns of this session; it is rebuilt together
	// with the sandbox.
	loop *agent.Loop
	// stopRenew cancels the sandbox lease renewal loop of this session.
	stopRenew context.CancelFunc
	// closed is set when the session is torn down (DELETE, shutdown); a
	// sandbox rebuilt concurrently must not attach to it afterwards.
	closed bool
	// turnID is the currently running turn ("" when idle); turns run
	// detached from the HTTP request that started them.
	turnID string
	// turnCancel cancels the running turn (cancel endpoint, DELETE
	// session, node shutdown).
	turnCancel context.CancelFunc
	// turnCancelCause records why the running turn is being ended; the
	// cancel endpoint sets it to errTurnCancelled so the terminal
	// turn_finished event carries the cancelled status instead of error.
	// Reset on every startTurn.
	turnCancelCause error
	// turnDone is closed by finishTurn once the current turn has been
	// marked finished; the cancel endpoint waits on it so a client can
	// resend immediately after cancelling.
	turnDone chan struct{}
	// lastActivity is the last time the session saw turn activity
	// (message accepted, turn finished). The idle reaper releases the
	// sandbox after idle_timeout without it; the session lives on.
	lastActivity time.Time
	// pending holds tool results that arrived before the agent loop
	// started waiting for them.
	pending map[string]ToolResultRequest
	// waiters holds the channels of tool calls the agent loop is
	// currently blocked on.
	waiters map[string]chan ToolResultRequest
	// persisted records every file the persist tool stored, keyed by
	// in-sandbox path (re-persisting a path replaces the entry).
	persisted map[string]agent.PersistedObject
	// filesMu serializes user-file attachment against sandbox detaches
	// (idle reap, cold eviction, teardown): an attach either injects
	// into the sandbox it saw, or — the sandbox went away — leaves the
	// record for the next build, but never accepts a file that reaches
	// no sandbox.
	filesMu sync.Mutex
	// files records every user-provided file (an object key in the
	// shared bucket), keyed by in-sandbox name; it is mirrored into
	// files.json so adoption restores it.
	files map[string]agent.FileRecord
	// bgExited holds background-process exits waiting to be reported as
	// a synthesized user turn once the session is idle.
	bgExited []agent.BgProcessExit
}

// touch marks turn activity, pushing out the idle reaper.
func (s *Session) touch() {
	s.mu.Lock()
	s.lastActivity = time.Now()
	s.mu.Unlock()
}

// liveSandbox returns the session's current sandbox, or false when the
// session is closed or its sandbox was reaped. Callers that must not
// lose work to a concurrent detach hold filesMu across the check and
// the follow-up action (see attachFiles).
func (s *Session) liveSandbox() (*ironhive.Sandbox, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.sandbox == nil {
		return nil, false
	}
	return s.sandbox, true
}

// setSandbox installs a freshly built sandbox and its lease-renewal
// cancel func. It returns false when the session was closed while the
// sandbox was being built; the caller must then stop the renewal and
// release the sandbox itself.
func (s *Session) setSandbox(sb *ironhive.Sandbox, stopRenew context.CancelFunc) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return false
	}
	s.sandbox = sb
	s.stopRenew = stopRenew
	return true
}

// takeSandbox detaches the session's sandbox and its renew cancel func,
// returning them for release (session creation rollback).
func (s *Session) takeSandbox() (*ironhive.Sandbox, context.CancelFunc) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sb, stop := s.sandbox, s.stopRenew
	s.sandbox = nil
	s.stopRenew = nil
	return sb, stop
}

// takeSandboxIfIdle detaches the session's sandbox and renew cancel func
// for release when no turn is running and the session has been inactive
// for at least d; it returns nil otherwise. Checking and detaching in a
// single critical section closes the race with startTurn/ensureSandbox:
// a turn that won the lock first can never lose its sandbox to the
// reaper.
func (s *Session) takeSandboxIfIdle(d time.Duration) (*ironhive.Sandbox, context.CancelFunc) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.turnID != "" || time.Since(s.lastActivity) < d {
		return nil, nil
	}
	sb, stop := s.sandbox, s.stopRenew
	s.sandbox = nil
	s.stopRenew = nil
	return sb, stop
}

// closeSession marks the session torn down (DELETE, shutdown) and
// detaches its sandbox and renew cancel func for release. After
// closeSession, setSandbox refuses to attach a new sandbox, so a
// concurrently rebuilding ensureSandbox cannot leak one.
func (s *Session) closeSession() (*ironhive.Sandbox, context.CancelFunc) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	sb, stop := s.sandbox, s.stopRenew
	s.sandbox = nil
	s.stopRenew = nil
	return sb, stop
}

// takeIfCold marks the session closed and detaches its sandbox for
// release when no turn is running and the session has been inactive for
// at least d; it reports false otherwise. Checking, closing and
// detaching in a single critical section closes the race with
// startTurn/ensureSandbox, like takeSandboxIfIdle. Unlike the idle reap,
// eviction retires the whole session (to cold storage) — a later request
// re-adopts it from S3.
func (s *Session) takeIfCold(d time.Duration) (*ironhive.Sandbox, context.CancelFunc, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.turnID != "" || time.Since(s.lastActivity) < d {
		return nil, nil, false
	}
	s.closed = true
	sb, stop := s.sandbox, s.stopRenew
	s.sandbox = nil
	s.stopRenew = nil
	return sb, stop, true
}

// getLoop returns the session's agent loop.
func (s *Session) getLoop() *agent.Loop {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.loop
}

// setLoop installs the agent loop built alongside a fresh sandbox.
func (s *Session) setLoop(l *agent.Loop) {
	s.mu.Lock()
	s.loop = l
	s.mu.Unlock()
}

// recordPersisted records a persisted file as session state (the
// agent.OnPersisted hook).
func (s *Session) recordPersisted(obj agent.PersistedObject) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.persisted == nil {
		s.persisted = make(map[string]agent.PersistedObject)
	}
	s.persisted[obj.Path] = obj
}

// Persisted returns the session's persisted objects, sorted by path.
func (s *Session) Persisted() []agent.PersistedObject {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]agent.PersistedObject, 0, len(s.persisted))
	for _, obj := range s.persisted {
		out = append(out, obj)
	}
	slices.SortFunc(out, func(a, b agent.PersistedObject) int { return strings.Compare(a.Path, b.Path) })
	return out
}

// recordFile records a user-provided file as session state, keyed by
// in-sandbox name (re-attaching a name replaces the entry). Callers hold
// filesMu (attachFiles) so a concurrent sandbox detach cannot strand the
// record: it either got injected into the live sandbox or, having seen
// none, will be injected by the next sandbox build.
func (s *Session) recordFile(u agent.FileRecord) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.files == nil {
		s.files = make(map[string]agent.FileRecord)
	}
	s.files[u.Name] = u
}

// isClosed reports whether the session has been torn down.
func (s *Session) isClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

// Files returns the session's user-provided files, sorted by name.
func (s *Session) Files() []agent.FileRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]agent.FileRecord, 0, len(s.files))
	for _, u := range s.files {
		out = append(out, u)
	}
	slices.SortFunc(out, func(a, b agent.FileRecord) int { return strings.Compare(a.Name, b.Name) })
	return out
}

// recordBackgroundExit queues a background-process exit for reporting
// as a synthesized user turn (the agent.OnBackgroundExit hook). Exits
// arriving after the session is closed are ignored.
func (s *Session) recordBackgroundExit(info agent.BgProcessExit) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.bgExited = append(s.bgExited, info)
}

// takeBackgroundExits pops every queued background-process exit. A
// closed session drops its queue instead of reporting it.
func (s *Session) takeBackgroundExits() []agent.BgProcessExit {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || len(s.bgExited) == 0 {
		return nil
	}
	exits := s.bgExited
	s.bgExited = nil
	return exits
}

// requeueBackgroundExits puts exits back at the head of the queue,
// preserving order, when their notification turn could not start
// because the session was busy.
func (s *Session) requeueBackgroundExits(exits []agent.BgProcessExit) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.bgExited = append(exits, s.bgExited...)
}

// startTurn marks a new turn as running, returning false when one is
// already running (the session allows one turn at a time).
func (s *Session) startTurn(turnID string, cancel context.CancelFunc) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.turnID != "" {
		return false
	}
	s.turnID = turnID
	s.turnCancel = cancel
	s.turnCancelCause = nil
	s.turnDone = make(chan struct{})
	return true
}

// finishTurn clears the running-turn mark when a turn ends and drops
// tool results nobody claimed: late results of this turn (or forgeries
// with fabricated call ids) must not leak into the next turn. The
// turnDone channel is closed so anything waiting on it (the cancel
// endpoint) is released as soon as the session is idle again.
func (s *Session) finishTurn() {
	s.mu.Lock()
	s.turnID = ""
	s.turnCancel = nil
	s.pending = nil
	if s.turnDone != nil {
		close(s.turnDone)
		s.turnDone = nil
	}
	s.mu.Unlock()
}

// cancelTurn aborts the running turn, if any (DELETE session, node
// shutdown); the cause stays nil, so the turn still ends with the error
// status rather than cancelled.
func (s *Session) cancelTurn() {
	s.mu.Lock()
	cancel := s.turnCancel
	s.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

// CancelTurn marks the running turn as a user-initiated interruption
// (errTurnCancelled) and cancels it, returning its id plus the channel
// that closes once finishTurn has marked the session idle; it returns ""
// when no turn is running. Marking and cancelling inside the lock makes
// it race-free: a turn that already finished is never reported as
// cancelled.
func (s *Session) CancelTurn() (string, <-chan struct{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.turnCancel == nil {
		return "", nil
	}
	id := s.turnID
	done := s.turnDone
	s.turnCancelCause = errTurnCancelled
	s.turnCancel()
	return id, done
}

// TurnID returns the id of the currently running turn ("" when idle).
func (s *Session) TurnID() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.turnID
}

// turnCause returns why the current turn is being ended (nil while
// running or when it failed rather than was cancelled).
func (s *Session) turnCause() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.turnCancelCause
}

// pendingToolResultsCap bounds tool results reported before the agent
// loop waits for them (or never claimed because the waiter timed out).
// Without the cap a client could grow the map forever with fabricated
// call ids.
const pendingToolResultsCap = 256

// AddToolResult delivers an externally reported tool result, either to a
// waiting agent loop or into the pending buffer. It returns an error
// when the pending buffer is full of unclaimed results and the call id
// is unknown.
func (s *Session) AddToolResult(r ToolResultRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ch, ok := s.waiters[r.CallID]; ok {
		delete(s.waiters, r.CallID)
		ch <- r
		return nil
	}
	if _, ok := s.pending[r.CallID]; !ok && len(s.pending) >= pendingToolResultsCap {
		return fmt.Errorf("too many pending tool results")
	}
	if s.pending == nil {
		s.pending = make(map[string]ToolResultRequest)
	}
	s.pending[r.CallID] = r
	return nil
}

// WaitToolResult implements agent.ToolResultWaiter: it blocks until the
// result of callID is reported via POST /v1/sessions/{id}/tool_results
// or ctx is done.
func (s *Session) WaitToolResult(ctx context.Context, callID string) (json.RawMessage, string, error) {
	s.mu.Lock()
	if r, ok := s.pending[callID]; ok {
		delete(s.pending, callID)
		s.mu.Unlock()
		return r.Result, r.Error, nil
	}
	ch := make(chan ToolResultRequest, 1)
	if s.waiters == nil {
		s.waiters = make(map[string]chan ToolResultRequest)
	}
	s.waiters[callID] = ch
	s.mu.Unlock()

	select {
	case r := <-ch:
		return r.Result, r.Error, nil
	case <-ctx.Done():
		s.mu.Lock()
		delete(s.waiters, callID)
		s.mu.Unlock()
		return nil, "", ctx.Err()
	}
}
