package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yankeguo/ironhive"
	"github.com/yankeguo/turnhive/agent"
)

// validSessionRequest returns a minimal request that passes Validate.
func validSessionRequest() CreateSessionRequest {
	return CreateSessionRequest{
		Model: ModelSpec{
			URL:      "http://llm/v1/chat/completions",
			Protocol: ProtocolOpenAICompletions,
			Name:     "m",
		},
		Prompt:   PromptSpec{System: "sys"},
		Ironhive: IronhiveSpec{Pool: "default"},
	}
}

func TestCreateSessionRequestValidateModelParams(t *testing.T) {
	// max_context and the known flag pass.
	req := validSessionRequest()
	req.Model.MaxContext = 131072
	req.Model.Features = []string{ModelFeatureSupportImage}
	if err := req.Validate(); err != nil {
		t.Fatalf("valid request rejected: %v", err)
	}

	// Negative max_context is rejected.
	req = validSessionRequest()
	req.Model.MaxContext = -1
	if err := req.Validate(); err == nil || !strings.Contains(err.Error(), "max_context") {
		t.Fatalf("expected max_context error, got %v", err)
	}

	// Unknown features are rejected, naming the offending entry.
	req = validSessionRequest()
	req.Model.Features = []string{ModelFeatureSupportImage, "support_video"}
	if err := req.Validate(); err == nil || !strings.Contains(err.Error(), `unknown feature "support_video"`) {
		t.Fatalf("expected unknown feature error, got %v", err)
	}

	// Features default to empty and pass.
	req = validSessionRequest()
	if err := req.Validate(); err != nil {
		t.Fatalf("default request rejected: %v", err)
	}
}

func TestCreateSessionRequestValidateMCPServers(t *testing.T) {
	// A full valid entry passes, transport empty (auto) or explicit.
	req := validSessionRequest()
	req.MCPServers = []MCPServerSpec{
		{Name: "fs-1", URL: "http://mcp.example.com/sse"},
		{Name: "wiki", URL: "https://mcp.example.com/mcp", Transport: "streamable", Headers: map[string]string{"Authorization": "Bearer x"}},
	}
	if err := req.Validate(); err != nil {
		t.Fatalf("valid mcp_servers rejected: %v", err)
	}

	// Names violating the namespacing pattern are rejected.
	for _, name := range []string{"", "has space", "dot.name", strings.Repeat("a", 33)} {
		req = validSessionRequest()
		req.MCPServers = []MCPServerSpec{{Name: name, URL: "http://mcp.example.com"}}
		if err := req.Validate(); err == nil || !strings.Contains(err.Error(), "mcp_servers[0].name") {
			t.Fatalf("expected name error for %q, got %v", name, err)
		}
	}

	// Duplicate names are rejected.
	req = validSessionRequest()
	req.MCPServers = []MCPServerSpec{
		{Name: "fs", URL: "http://a.example.com"},
		{Name: "fs", URL: "http://b.example.com"},
	}
	if err := req.Validate(); err == nil || !strings.Contains(err.Error(), "duplicates") {
		t.Fatalf("expected duplicate name error, got %v", err)
	}

	// Unknown transports are rejected.
	req = validSessionRequest()
	req.MCPServers = []MCPServerSpec{{Name: "fs", URL: "http://mcp.example.com", Transport: "stdio"}}
	if err := req.Validate(); err == nil || !strings.Contains(err.Error(), "transport") {
		t.Fatalf("expected transport error, got %v", err)
	}
}

func TestSessionBackgroundExitQueue(t *testing.T) {
	sess := &Session{}
	sess.recordBackgroundExit(agent.BgProcessExit{Pid: 1, Command: "a"})
	sess.recordBackgroundExit(agent.BgProcessExit{Pid: 2, Command: "b"})

	// take pops everything in order and empties the queue.
	exits := sess.takeBackgroundExits()
	if len(exits) != 2 || exits[0].Pid != 1 || exits[1].Pid != 2 {
		t.Fatalf("unexpected exits: %+v", exits)
	}
	if got := sess.takeBackgroundExits(); got != nil {
		t.Fatalf("queue must be empty after take, got %+v", got)
	}

	// requeue restores the popped exits at the head, preserving order.
	sess.recordBackgroundExit(agent.BgProcessExit{Pid: 3, Command: "c"})
	sess.requeueBackgroundExits(exits)
	got := sess.takeBackgroundExits()
	if len(got) != 3 || got[0].Pid != 1 || got[1].Pid != 2 || got[2].Pid != 3 {
		t.Fatalf("requeue must preserve order: %+v", got)
	}

	// A closed session drops its queue and ignores new exits.
	sess.recordBackgroundExit(agent.BgProcessExit{Pid: 4, Command: "d"})
	sess.closeSession()
	if got := sess.takeBackgroundExits(); got != nil {
		t.Fatalf("closed session must drop its queue, got %+v", got)
	}
	sess.recordBackgroundExit(agent.BgProcessExit{Pid: 5, Command: "e"})
	if got := sess.takeBackgroundExits(); got != nil {
		t.Fatalf("closed session must ignore new exits, got %+v", got)
	}
}

func TestDrainBackgroundExitsBusyRequeues(t *testing.T) {
	c := &Controller{}
	sess := &Session{ID: "s", hub: newEventHub()}
	// Hold the busy mark so the notification turn cannot start.
	if !sess.startTurn("turn-x", func() {}) {
		t.Fatal("startTurn on a fresh session must succeed")
	}
	sess.recordBackgroundExit(agent.BgProcessExit{Pid: 1, Command: "a"})

	c.drainBackgroundExits(sess)

	exits := sess.takeBackgroundExits()
	if len(exits) != 1 || exits[0].Pid != 1 {
		t.Fatalf("busy drain must requeue the exits: %+v", exits)
	}
}

func TestBuildBgExitMessage(t *testing.T) {
	msg := buildBgExitMessage([]agent.BgProcessExit{
		{Pid: 42, Command: "make build", ExitCode: 2, StdoutFile: ".agents/shell-logs/c1.stdout", StderrFile: ".agents/shell-logs/c1.stderr"},
		{Pid: 43, Command: "sleep 9", ExitCode: -1, StdoutFile: ".agents/shell-logs/c2.stdout", StderrFile: ".agents/shell-logs/c2.stderr"},
	})
	for _, want := range []string{
		"<background_processes_exited>",
		"</background_processes_exited>",
		"do NOT ask the user questions",
		"pid: 42", "command: make build", "exit_code: 2",
		"stdout: .agents/shell-logs/c1.stdout", "stderr: .agents/shell-logs/c1.stderr",
		"pid: 43", "exit_code: unknown",
	} {
		if !strings.Contains(msg, want) {
			t.Fatalf("message missing %q:\n%s", want, msg)
		}
	}
}

func TestSessionRecordPersisted(t *testing.T) {
	sess := &Session{}
	sess.recordPersisted(agent.PersistedObject{Path: "b.txt", ObjectKey: "sessions/s/persisted/b.txt", Size: 1})
	sess.recordPersisted(agent.PersistedObject{Path: "a.txt", ObjectKey: "sessions/s/persisted/a.txt", Size: 2})
	// Re-persisting a path replaces the entry.
	sess.recordPersisted(agent.PersistedObject{Path: "b.txt", ObjectKey: "sessions/s/persisted/b.txt", Size: 3})

	got := sess.Persisted()
	if len(got) != 2 {
		t.Fatalf("expected 2 objects after dedup, got %+v", got)
	}
	// Sorted by path; b.txt carries the latest size.
	if got[0].Path != "a.txt" || got[1].Path != "b.txt" || got[1].Size != 3 {
		t.Fatalf("unexpected persisted objects: %+v", got)
	}
}

func TestTakeSandboxIfIdle(t *testing.T) {
	sess := &Session{ID: "s"}
	if !sess.setSandbox(&ironhive.Sandbox{Name: "sb"}, func() {}) {
		t.Fatal("setSandbox on a fresh session must succeed")
	}
	sess.touch()

	// Recently active: nothing is detached.
	if sb, stop := sess.takeSandboxIfIdle(time.Minute); sb != nil || stop != nil {
		t.Fatal("active session must not be reaped")
	}

	// Idle but a turn is running: nothing is detached.
	sess.mu.Lock()
	sess.lastActivity = time.Now().Add(-time.Hour)
	sess.mu.Unlock()
	if !sess.startTurn("turn-1", func() {}) {
		t.Fatal("startTurn must succeed")
	}
	if sb, _ := sess.takeSandboxIfIdle(time.Minute); sb != nil {
		t.Fatal("sandbox of a running turn must not be reaped")
	}
	sess.finishTurn()

	// Idle and no turn: the sandbox and its renew cancel are detached.
	sb, stop := sess.takeSandboxIfIdle(time.Minute)
	if sb == nil || stop == nil {
		t.Fatal("idle session with no turn must be reaped")
	}
	if sess.hasSandbox() {
		t.Fatal("sandbox must be detached after reap")
	}
	// Nothing left to reap.
	if sb, _ := sess.takeSandboxIfIdle(0); sb != nil {
		t.Fatal("second reap must find no sandbox")
	}
}

// TestTakeSandboxIfIdleRace hammers startTurn against the reaper's
// check-and-detach critical section; run with -race. The sandbox must
// never be detached out from under a running turn.
func TestTakeSandboxIfIdleRace(t *testing.T) {
	sess := &Session{ID: "s"}
	stop := func() {}
	sess.setSandbox(&ironhive.Sandbox{Name: "sb"}, stop)
	sess.mu.Lock()
	sess.lastActivity = time.Now().Add(-time.Hour)
	sess.mu.Unlock()

	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 500 {
				if sess.startTurn("turn-x", func() {}) {
					sess.finishTurn()
				}
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range 500 {
			sb, stopFn := sess.takeSandboxIfIdle(time.Minute)
			if sb == nil {
				continue
			}
			if sess.hasSandbox() {
				t.Error("sandbox still attached after reap")
			}
			sess.setSandbox(sb, stopFn)
		}
	}()
	wg.Wait()
}

func TestSetSandboxAfterClose(t *testing.T) {
	sess := &Session{ID: "s"}
	if !sess.setSandbox(&ironhive.Sandbox{Name: "sb1"}, func() {}) {
		t.Fatal("setSandbox on a fresh session must succeed")
	}
	sb, stop := sess.closeSession()
	if sb == nil || stop == nil {
		t.Fatal("closeSession must detach the sandbox and renew cancel")
	}
	// A sandbox rebuilt concurrently with DELETE/Close must be refused.
	if sess.setSandbox(&ironhive.Sandbox{Name: "sb2"}, func() {}) {
		t.Fatal("setSandbox must refuse a closed session")
	}
	if sess.hasSandbox() {
		t.Fatal("closed session must not hold a sandbox")
	}
	if sb, stop := sess.closeSession(); sb != nil || stop != nil {
		t.Fatal("second closeSession must find nothing to detach")
	}
}

func TestToolResultBeforeWait(t *testing.T) {
	sess := &Session{}
	if err := sess.AddToolResult(ToolResultRequest{CallID: "c1", Result: json.RawMessage(`{"ok":true}`)}); err != nil {
		t.Fatalf("AddToolResult: %v", err)
	}
	res, errStr, err := sess.WaitToolResult(context.Background(), "c1")
	if err != nil || errStr != "" || string(res) != `{"ok":true}` {
		t.Fatalf("WaitToolResult = %s, %q, %v", res, errStr, err)
	}
	// The pending entry is consumed, not replayed.
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, _, err := sess.WaitToolResult(ctx, "c1"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("consumed result must not be replayed, got %v", err)
	}
}

func TestToolResultAfterWait(t *testing.T) {
	sess := &Session{}
	type outcome struct {
		res    json.RawMessage
		errStr string
		err    error
	}
	done := make(chan outcome, 1)
	go func() {
		res, errStr, err := sess.WaitToolResult(context.Background(), "c2")
		done <- outcome{res, errStr, err}
	}()
	// Wait until the waiter is registered before reporting the result.
	for range 100 {
		sess.mu.Lock()
		n := len(sess.waiters)
		sess.mu.Unlock()
		if n == 1 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if err := sess.AddToolResult(ToolResultRequest{CallID: "c2", Error: "boom"}); err != nil {
		t.Fatalf("AddToolResult: %v", err)
	}
	select {
	case o := <-done:
		if o.err != nil || o.errStr != "boom" || len(o.res) != 0 {
			t.Fatalf("WaitToolResult = %s, %q, %v", o.res, o.errStr, o.err)
		}
	case <-time.After(time.Second):
		t.Fatal("waiter was not released by the reported result")
	}
}

func TestFinishTurnClearsPending(t *testing.T) {
	sess := &Session{}
	if !sess.startTurn("turn-1", func() {}) {
		t.Fatal("startTurn must succeed")
	}
	// A result reported for a call nobody waits on (late or forged).
	if err := sess.AddToolResult(ToolResultRequest{CallID: "stale", Result: json.RawMessage(`1`)}); err != nil {
		t.Fatalf("AddToolResult: %v", err)
	}
	sess.finishTurn()
	// The stale result must not leak into the next turn.
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, _, err := sess.WaitToolResult(ctx, "stale"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("stale pending result survived finishTurn: %v", err)
	}
}

func TestAddToolResultPendingCap(t *testing.T) {
	sess := &Session{}
	for i := range pendingToolResultsCap {
		if err := sess.AddToolResult(ToolResultRequest{CallID: fmt.Sprintf("c%d", i), Result: json.RawMessage(`1`)}); err != nil {
			t.Fatalf("AddToolResult %d: %v", i, err)
		}
	}
	// A new unknown call id over the cap is rejected.
	if err := sess.AddToolResult(ToolResultRequest{CallID: "overflow", Result: json.RawMessage(`1`)}); err == nil {
		t.Fatal("expected pending-cap error")
	}
	// Re-reporting a known call id still succeeds (replace in place).
	if err := sess.AddToolResult(ToolResultRequest{CallID: "c0", Result: json.RawMessage(`2`)}); err != nil {
		t.Fatalf("re-report of a known call id: %v", err)
	}
}

func TestTakeIfCold(t *testing.T) {
	// Fresh session with activity: not cold.
	sess := &Session{ID: "s"}
	sess.setSandbox(&ironhive.Sandbox{Name: "sb"}, func() {})
	sess.touch()
	if _, _, cold := sess.takeIfCold(time.Hour); cold {
		t.Fatal("an active session must not be cold")
	}

	// A running turn blocks eviction.
	sess.startTurn("turn-1", func() {})
	sess.mu.Lock()
	sess.lastActivity = time.Now().Add(-2 * time.Hour)
	sess.mu.Unlock()
	if _, _, cold := sess.takeIfCold(time.Hour); cold {
		t.Fatal("a session with a running turn must not be cold")
	}
	sess.finishTurn()

	// Idle past the bound: evicted, closed, sandbox detached.
	sb, stop, cold := sess.takeIfCold(time.Hour)
	if !cold || sb == nil || stop == nil {
		t.Fatalf("expected eviction with sandbox and renew func, got %v %v %v", sb, stop, cold)
	}
	if sess.hasSandbox() {
		t.Fatal("sandbox must be detached after eviction")
	}
	// A second eviction attempt reports false (already closed).
	if _, _, cold = sess.takeIfCold(time.Hour); cold {
		t.Fatal("an already-evicted session must report false")
	}
	// setSandbox refuses the closed session.
	if sess.setSandbox(&ironhive.Sandbox{Name: "sb2"}, func() {}) {
		t.Fatal("setSandbox must refuse an evicted session")
	}
}

func TestCancelTurn(t *testing.T) {
	sess := &Session{}

	// No turn running: CancelTurn reports no origin.
	if id, _ := sess.CancelTurn(); id != "" {
		t.Fatalf("idle session: expected empty turn id, got %q", id)
	}

	// A running turn is cancelled; the returned channel closes only when
	// finishTurn marks the session idle.
	cancelCalled := false
	if !sess.startTurn("turn-1", func() { cancelCalled = true }) {
		t.Fatal("startTurn must succeed")
	}
	cancelCalled = false
	id, done := sess.CancelTurn()
	if id != "turn-1" || !cancelCalled {
		t.Fatalf("CancelTurn: id=%q cancelCalled=%v, want turn-1/true", id, cancelCalled)
	}
	select {
	case <-done:
		t.Fatal("done must not be closed before finishTurn")
	default:
	}
	sess.finishTurn()
	select {
	case <-done:
	case <-time.After(20 * time.Millisecond):
		t.Fatal("done must close after finishTurn")
	}
	// The session is idle again: a resend would succeed.
	if !sess.startTurn("turn-2", func() {}) {
		t.Fatal("startTurn after a cancelled turn must succeed")
	}
	if id, _ := sess.CancelTurn(); id != "turn-2" {
		t.Fatalf("second turn id = %q, want turn-2", id)
	}
	sess.finishTurn()
	// A finished turn is not reported as cancelled.
	if id, _ := sess.CancelTurn(); id != "" {
		t.Fatalf("after finishTurn: expected empty turn id, got %q", id)
	}
}

func TestCancelTurnMarksCause(t *testing.T) {
	sess := &Session{}
	sess.startTurn("turn-1", func() {})
	if sess.turnCause() != nil {
		t.Fatalf("cause = %v before cancel, want nil", sess.turnCause())
	}
	sess.CancelTurn()
	if sess.turnCause() != errTurnCancelled {
		t.Fatalf("cause = %v, want errTurnCancelled", sess.turnCause())
	}
	sess.finishTurn()

	// A new turn resets the cause; cancelTurn (DELETE/shutdown) leaves it
	// nil, so the turn ends with an error event, not turn_cancelled.
	sess.startTurn("turn-2", func() {})
	sess.cancelTurn()
	if sess.turnCause() != nil {
		t.Fatalf("cause = %v after cancelTurn, want nil", sess.turnCause())
	}
	sess.finishTurn()
}
