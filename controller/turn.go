package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/yankeguo/turnhive/agent"
)

// newTurnID generates a turn id, following the session id scheme.
func newTurnID() string {
	return "turn-" + strings.ToLower(ulid.Make().String())
}

// turnTimeout bounds a single agent turn.
const turnTimeout = time.Hour

// errTurnCancelled is the context cause of a turn interrupted through the
// cancel endpoint; it distinguishes a user-initiated interruption from a
// failure (timeout, stream error) when the terminal event is published.
var errTurnCancelled = errors.New("turn cancelled")

// CreateMessageRequest is the JSON body of POST /v1/sessions/{id}/messages.
type CreateMessageRequest struct {
	// Content is the user input for this turn. Referencing attached
	// files (POST .../files) is the caller's business: it composes
	// whatever marker text it likes — the files are always at
	// .agents/uploads/<name> inside the sandbox.
	Content string `json:"content"`
}

// handleCreateMessage accepts one user input and runs its turn
// asynchronously: the response is the new turn id, and all turn events
// flow over the session event stream (GET .../events). A session runs
// one turn at a time; a concurrent message is rejected with 409
// session_busy.
func (c *Controller) handleCreateMessage(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !c.routeSession(w, r, id) {
		return
	}
	var req CreateMessageRequest
	r.Body = http.MaxBytesReader(w, r.Body, maxJSONBody)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON body: " + err.Error()})
		return
	}
	if req.Content == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "content is required"})
		return
	}
	v, ok := c.sessions.Load(id)
	if !ok {
		// Lost a race with DELETE after routeSession.
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "session not found"})
		return
	}
	sess := v.(*Session)

	turnID, started := c.runTurn(sess, req.Content)
	if !started {
		writeJSON(w, http.StatusConflict, map[string]string{"error": "session_busy"})
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]string{"turn_id": turnID})
}

// runTurn starts a detached turn for content (a client message or a
// cluster-synthesized one, e.g. the background-process exit
// notification) and reports the new turn id; a session runs one turn at
// a time, so it returns "" when a turn is already running. The turn
// runs detached from any HTTP request: a client disconnect does not
// abort it (DELETE session or node shutdown does), and all of its
// events flow over the session event stream.
func (c *Controller) runTurn(sess *Session, content string) (string, bool) {
	turnID := newTurnID()
	ctx, cancel := context.WithTimeout(context.Background(), turnTimeout)
	if !sess.startTurn(turnID, cancel) {
		cancel()
		return "", false
	}
	sess.touch()
	sess.hub.publish(turnID, "turn_started", map[string]string{"turn_id": turnID})
	go func() {
		// A finished turn is the drain trigger for queued background
		// exits: finishTurn clears the busy mark first, so the drain can
		// start their notification turn immediately.
		defer func() {
			sess.finishTurn()
			c.drainBackgroundExits(sess)
		}()
		defer cancel()
		defer sess.touch()
		rep := &hubReporter{hub: sess.hub, turnID: turnID, cause: sess.turnCause}
		// The sandbox may have been reaped for idleness; rebuild it from
		// the session spec and persisted files first.
		if err := c.ensureSandbox(ctx, sess); err != nil {
			log.Printf("session %s turn %s: %v", sess.ID, turnID, err)
			rep.Error("failed to prepare sandbox")
			return
		}
		if err := sess.getLoop().RunTurn(ctx, content, rep); err != nil {
			if errors.Is(err, agent.ErrTurnBusy) {
				// Unreachable given startTurn; publish so subscribers are
				// not stuck watching an open turn.
				rep.Error("session busy")
			}
			log.Printf("session %s turn %s: %v", sess.ID, turnID, err)
		}
	}()
	return turnID, true
}

// drainBackgroundExits reports queued background-process exits as one
// synthesized user message in a new turn (aligned with the agentdesk
// runner's bg-notifier): the agent reacts to the exits immediately
// instead of learning about them from the user's next message. Exits
// accumulated while a turn is running coalesce into a single message;
// when the session is busy the queue goes back and the running turn's
// completion drains it again.
func (c *Controller) drainBackgroundExits(sess *Session) {
	exits := sess.takeBackgroundExits()
	if len(exits) == 0 {
		return
	}
	if _, started := c.runTurn(sess, buildBgExitMessage(exits)); !started {
		sess.requeueBackgroundExits(exits)
	}
}

// buildBgExitMessage renders the synthesized user message reporting
// background-process exits. The note steers the agent: no interactive
// user is present, so it must not ask questions unless strictly
// necessary. Format follows the agentdesk runner: bare open/close tags,
// metadata as `key: value` lines.
func buildBgExitMessage(exits []agent.BgProcessExit) string {
	var b strings.Builder
	b.WriteString("<background_processes_exited>\n")
	b.WriteString("note: Automated notification: these background processes have exited. React autonomously " +
		"(inspect the output files, continue any follow-up work); do NOT ask the user questions " +
		"unless strictly necessary.\n")
	for _, e := range exits {
		exitCode := strconv.Itoa(e.ExitCode)
		if e.ExitCode < 0 {
			exitCode = "unknown"
		}
		b.WriteString("\n<process>\n")
		fmt.Fprintf(&b, "pid: %d\ncommand: %s\nexit_code: %s\nstdout: %s\nstderr: %s\n",
			e.Pid, e.Command, exitCode, e.StdoutFile, e.StderrFile)
		b.WriteString("</process>\n")
	}
	b.WriteString("</background_processes_exited>")
	return b.String()
}

// handleCancelTurn interrupts the session's running turn: the turn's
// context is cancelled so the agent loop aborts (the partial reply is
// persisted and the turn closes with a turn_finished event carrying the
// cancelled status), and the handler returns only once finishTurn has
// cleared the busy mark — so the client can resend its message
// immediately to continue (resend is the recovery; the cancelled turn
// is not replayed or resumable).
func (c *Controller) handleCancelTurn(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	// No adoption: a cold session has no running turn, so cancelling one
	// must not recover it from storage first.
	if !c.routeSessionMode(w, r, id, false) {
		return
	}
	v, ok := c.sessions.Load(id)
	if !ok {
		// Lost a race with DELETE after routeSession.
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "session not found"})
		return
	}
	sess := v.(*Session)

	turnID, done := sess.CancelTurn()
	if turnID == "" {
		writeJSON(w, http.StatusConflict, map[string]string{"error": "no_turn_running"})
		return
	}
	if done != nil {
		select {
		case <-done:
		case <-r.Context().Done():
			// Client gave up waiting: the cancel already took effect,
			// but finishTurn has not cleared the busy mark yet — a 202
			// here would invite an immediate resend that collides with
			// session_busy. Report the gateway timeout instead; the
			// client tracks completion via the SSE turn_finished event.
			writeJSON(w, http.StatusGatewayTimeout, map[string]string{
				"error": "cancel pending; the turn is finishing",
			})
			return
		}
	}
	writeJSON(w, http.StatusAccepted, map[string]string{"turn_id": turnID, "status": "cancelled"})
}

// handleCreateToolResult accepts an externally reported tool result.
// The request is routed to the session's owner node first (leaving the
// body untouched for forwarding), then validated and queued on the
// session for the agent runtime to consume.
func (c *Controller) handleCreateToolResult(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !c.routeSession(w, r, id) {
		return
	}
	var req ToolResultRequest
	r.Body = http.MaxBytesReader(w, r.Body, maxJSONBody)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON body: " + err.Error()})
		return
	}
	if err := req.Validate(); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	v, ok := c.sessions.Load(id)
	if !ok {
		// Lost a race with DELETE after routeSession.
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "session not found"})
		return
	}
	if err := v.(*Session).AddToolResult(req); err != nil {
		writeJSON(w, http.StatusTooManyRequests, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]string{"status": "accepted"})
}
