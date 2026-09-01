package agent

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/yankeguo/turnhive/llm"
)

// defaultExternalToolTimeout bounds one external tool call when the Loop
// configuration leaves ExternalToolTimeout zero.
const defaultExternalToolTimeout = 10 * time.Minute

// ExternalToolSpec describes an external tool: defined by the session
// client, executed externally. Tool calls are reported over the session
// stream; results come back via POST /v1/sessions/{id}/tool_results.
type ExternalToolSpec struct {
	Name        string
	Description string
	// Parameters is the JSON Schema describing the tool's arguments,
	// following the OpenAI function calling convention.
	Parameters map[string]any
}

// ToolResultWaiter blocks until the client reports the result of an
// external tool call (via POST /v1/sessions/{id}/tool_results).
type ToolResultWaiter interface {
	// WaitToolResult waits for the result of the call identified by
	// callID. Exactly one of result and errText is meaningful: result is
	// the tool's output as arbitrary JSON, errText reports that the tool
	// call failed.
	WaitToolResult(ctx context.Context, callID string) (result json.RawMessage, errText string, err error)
}

// externalTool is a Tool that delegates execution to the session client
// through a ToolResultWaiter.
type externalTool struct {
	spec    ExternalToolSpec
	waiter  ToolResultWaiter
	timeout time.Duration
}

// ExternalTools wraps external tool specs as Tools. Execution waits on
// waiter, bounded by timeout per call. External tools report nothing
// themselves; the loop reports their lifecycle.
func ExternalTools(specs []ExternalToolSpec, waiter ToolResultWaiter, timeout time.Duration) []Tool {
	if timeout <= 0 {
		timeout = defaultExternalToolTimeout
	}
	tools := make([]Tool, 0, len(specs))
	for _, spec := range specs {
		tools = append(tools, externalTool{spec: spec, waiter: waiter, timeout: timeout})
	}
	return tools
}

// Spec returns the tool definition as supplied by the session client.
func (t externalTool) Spec() llm.ToolDef {
	return llm.ToolDef{
		Name:        t.spec.Name,
		Description: t.spec.Description,
		Parameters:  t.spec.Parameters,
	}
}

// Execute waits for the client to report the call's result. A reported
// error text becomes the returned error (fed back to the model as the
// tool result); the raw JSON result is returned as-is.
func (t externalTool) Execute(ctx context.Context, callID string, _ json.RawMessage) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, t.timeout)
	defer cancel()
	result, errText, err := t.waiter.WaitToolResult(ctx, callID)
	if err != nil {
		return "", err
	}
	if errText != "" {
		return "", errors.New(errText)
	}
	return string(result), nil
}
