// Package agent implements the turnhive agent turn loop: it streams chat
// completions from an OpenAI-compatible endpoint, dispatches tool calls to
// sandbox tools (executed inside an ironhive sandbox) and external tools
// (executed by the session client), installs skills into the sandbox, and
// persists the LLM-facing message history.
//
// A turn proceeds as a step loop: the assistant's reply either ends the
// turn (plain text) or requests tool calls, whose results are fed back as
// transient messages that are never persisted to history.
package agent

// ToolCallEvent reports the lifecycle of one tool call to a Reporter.
type ToolCallEvent struct {
	// ID is the tool call ID assigned by the model.
	ID string `json:"id"`
	// Name is the tool name.
	Name string `json:"name"`
	// Status is "running", "done" or "error".
	Status string `json:"status"`
}

// Status values of ToolCallEvent.Status.
const (
	ToolCallRunning = "running"
	ToolCallDone    = "done"
	ToolCallError   = "error"
)

// Reporter receives the streamed progress of a turn. Implementations must
// be safe for the single goroutine that runs the turn.
type Reporter interface {
	// Delta forwards one content delta of the assistant's reply.
	Delta(text string)
	// ReasoningDelta forwards one reasoning content delta.
	ReasoningDelta(text string)
	// ToolCall reports a tool call starting or finishing.
	ToolCall(ev ToolCallEvent)
	// Done reports a successful turn with the final assistant text.
	Done(text string)
	// Error reports a failed turn.
	Error(msg string)
}
