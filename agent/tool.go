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

// ImageTool is a Tool whose execution can additionally yield images for
// the model (e.g. load_media). The loop injects the returned image data
// URIs into the conversation as a user message right after the tool
// message. Only implement it for models that accept image inputs.
type ImageTool interface {
	Tool
	// ExecuteImage runs one call like Execute, additionally returning
	// image data URIs to inject into the conversation.
	ExecuteImage(ctx context.Context, callID string, args json.RawMessage) (text string, images []string, err error)
}

// selfTruncatingTool marks a Tool that bounds its own output (read);
// dispatchTool skips the generic spill/truncation for such tools.
type selfTruncatingTool interface {
	selfTruncatingOutput()
}

// dispatchTool finds the tool named name among tools and executes it.
// Successful output goes through TruncateSpill — oversized output is
// spilled to a sandbox file and only a strict head preview plus the file
// path goes back to the model — unless the tool bounds its own output.
// Images returned by an ImageTool pass through untouched. Errors are
// returned untruncated (the caller feeds them back as the tool result
// text).
func dispatchTool(ctx context.Context, tools []Tool, spiller OutputSpiller, callID, name string, args json.RawMessage) (text string, images []string, err error) {
	for _, t := range tools {
		if t.Spec().Name != name {
			continue
		}
		if it, ok := t.(ImageTool); ok {
			text, images, err = it.ExecuteImage(ctx, callID, args)
		} else {
			text, err = t.Execute(ctx, callID, args)
		}
		if err != nil {
			return "", nil, err
		}
		if _, ok := t.(selfTruncatingTool); ok {
			return text, images, nil
		}
		return TruncateSpill(ctx, text, name, spiller), images, nil
	}
	return "", nil, fmt.Errorf("unknown tool: %s", name)
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

// boolProp is a JSON Schema boolean property with a description.
func boolProp(description string) map[string]any {
	return map[string]any{"type": "boolean", "description": description}
}
