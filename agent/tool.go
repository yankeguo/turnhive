package agent

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/yankeguo/turnhive/llm"
)

// Tool is one callable tool exposed to the model.
type Tool interface {
	// Spec returns the OpenAI function calling definition of the tool.
	Spec() llm.ToolDef
	// Execute runs one tool call. args is the raw JSON arguments document
	// produced by the model; callID identifies the call for tools that
	// report progress or wait on external results.
	Execute(ctx context.Context, callID string, args json.RawMessage) (string, error)
}

// dispatchTool finds the tool named name among tools and executes it.
// Successful output is truncated with Truncate before it goes back to the
// model; errors are returned untruncated (the caller feeds them back as
// the tool result text).
func dispatchTool(ctx context.Context, tools []Tool, callID, name string, args json.RawMessage) (string, error) {
	for _, t := range tools {
		if t.Spec().Name == name {
			out, err := t.Execute(ctx, callID, args)
			if err != nil {
				return "", err
			}
			return Truncate(out), nil
		}
	}
	return "", fmt.Errorf("unknown tool: %s", name)
}

// toolSpecs returns the tool definitions of tools, in order.
func toolSpecs(tools []Tool) []llm.ToolDef {
	defs := make([]llm.ToolDef, 0, len(tools))
	for _, t := range tools {
		defs = append(defs, t.Spec())
	}
	return defs
}

// jsonSchema is a small helper for building the JSON Schema of a tool's
// parameters in the OpenAI function calling shape.
func jsonSchema(properties map[string]any, required ...string) map[string]any {
	return map[string]any{
		"type":       "object",
		"properties": properties,
		"required":   required,
	}
}

// stringProp is a JSON Schema string property with a description.
func stringProp(description string) map[string]any {
	return map[string]any{"type": "string", "description": description}
}
