// Package llm implements a std-lib-only client for OpenAI-compatible
// streaming chat completion endpoints (vLLM, OpenAI, DeepSeek, Qwen, ...).
//
// Stream POSTs a chat completion request with stream=true and consumes the
// SSE response, invoking a callback for every content or reasoning delta
// while accumulating the full assistant message (including tool calls) and
// token usage. Endpoints that ignore stream=true and answer with a plain
// JSON chat.completion are handled transparently.
package llm

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"net/http"
	"sort"
	"strings"
)

// maxErrorBody caps how much of a non-2xx response body is included in the
// returned error.
const maxErrorBody = 4096

// Message is one chat message. Assistant messages may carry ToolCalls;
// tool result messages use Role "tool" with ToolCallID set.
type Message struct {
	Role       string
	Content    string
	ToolCalls  []ToolCall
	ToolCallID string
}

// messageWire is the JSON wire form of Message. Content is always emitted
// as a string ("" when empty); tool call arguments are strings holding the
// JSON document, per the OpenAI function calling convention.
type messageWire struct {
	Role       string         `json:"role"`
	Content    string         `json:"content"`
	ToolCalls  []toolCallWire `json:"tool_calls,omitempty"`
	ToolCallID string         `json:"tool_call_id,omitempty"`
}

type toolCallWire struct {
	ID       string `json:"id"`
	Type     string `json:"type"`
	Function struct {
		Name      string `json:"name"`
		Arguments string `json:"arguments"`
	} `json:"function"`
}

// MarshalJSON encodes m in the OpenAI wire format.
func (m Message) MarshalJSON() ([]byte, error) {
	wire := messageWire{
		Role:       m.Role,
		Content:    m.Content,
		ToolCallID: m.ToolCallID,
	}
	for _, tc := range m.ToolCalls {
		var w toolCallWire
		w.ID = tc.ID
		w.Type = "function"
		w.Function.Name = tc.Name
		w.Function.Arguments = string(tc.Arguments)
		wire.ToolCalls = append(wire.ToolCalls, w)
	}
	return json.Marshal(wire)
}

// UnmarshalJSON decodes the OpenAI wire format into m.
func (m *Message) UnmarshalJSON(data []byte) error {
	var wire messageWire
	if err := json.Unmarshal(data, &wire); err != nil {
		return err
	}
	m.Role = wire.Role
	m.Content = wire.Content
	m.ToolCallID = wire.ToolCallID
	m.ToolCalls = nil
	for _, w := range wire.ToolCalls {
		m.ToolCalls = append(m.ToolCalls, ToolCall{
			ID:        w.ID,
			Name:      w.Function.Name,
			Arguments: json.RawMessage(w.Function.Arguments),
		})
	}
	return nil
}

// ToolCall is a function call requested by the assistant.
type ToolCall struct {
	ID        string
	Name      string
	Arguments json.RawMessage
}

// ToolDef describes a callable tool, OpenAI function calling shape.
type ToolDef struct {
	Name        string
	Description string
	Parameters  map[string]any // JSON Schema
}

// toolDefWire is the JSON wire form of ToolDef.
type toolDefWire struct {
	Type     string `json:"type"`
	Function struct {
		Name        string         `json:"name"`
		Description string         `json:"description"`
		Parameters  map[string]any `json:"parameters"`
	} `json:"function"`
}

// Usage carries token accounting when the endpoint reports it.
type Usage struct {
	PromptTokens     int
	CompletionTokens int
	TotalTokens      int
}

// Request is one streaming chat completion request.
type Request struct {
	URL      string            // full endpoint URL
	Headers  map[string]string // auth + extras, applied verbatim
	Model    string
	Messages []Message
	Tools    []ToolDef // omitted from body when empty
}

// EventType distinguishes streamed content from reasoning content.
type EventType int

const (
	// EventDelta is a normal content delta.
	EventDelta EventType = iota + 1
	// EventReasoning is a reasoning_content delta.
	EventReasoning
)

// Event is one streamed delta.
type Event struct {
	Type EventType
	Text string
}

// requestBody is the JSON body sent to the endpoint.
type requestBody struct {
	Model         string        `json:"model"`
	Messages      []Message     `json:"messages"`
	Tools         []toolDefWire `json:"tools,omitempty"`
	Stream        bool          `json:"stream"`
	StreamOptions struct {
		IncludeUsage bool `json:"include_usage"`
	} `json:"stream_options"`
}

// Stream POSTs the request and consumes the SSE stream, invoking onEvent
// for each content/reasoning delta (onEvent may be nil). It returns the
// fully accumulated assistant message (content + tool calls) and usage.
//
// Deadlines and cancellation come from ctx; the client sets no timeout of
// its own. If the endpoint ignores stream=true and answers with a plain
// JSON chat.completion, that response is decoded and returned instead.
func Stream(ctx context.Context, req Request, onEvent func(Event)) (Message, Usage, error) {
	body, err := marshalRequestBody(req)
	if err != nil {
		return Message{}, Usage{}, fmt.Errorf("marshal request body: %w", err)
	}

	hreq, err := http.NewRequestWithContext(ctx, http.MethodPost, req.URL, bytes.NewReader(body))
	if err != nil {
		return Message{}, Usage{}, fmt.Errorf("build request: %w", err)
	}
	hreq.Header.Set("Content-Type", "application/json")
	for k, v := range req.Headers {
		hreq.Header.Set(k, v)
	}

	resp, err := http.DefaultClient.Do(hreq)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return Message{}, Usage{}, ctxErr
		}
		return Message{}, Usage{}, fmt.Errorf("post %s: %w", req.URL, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBody))
		return Message{}, Usage{}, fmt.Errorf("post %s: status %d: %s", req.URL, resp.StatusCode, strings.TrimSpace(string(snippet)))
	}

	mediaType, _, _ := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	if mediaType != "text/event-stream" {
		return decodeCompletion(resp.Body)
	}

	return consumeStream(ctx, resp.Body, onEvent)
}

// marshalRequestBody builds the request body for req.
func marshalRequestBody(req Request) ([]byte, error) {
	var body requestBody
	body.Model = req.Model
	body.Messages = req.Messages
	body.Stream = true
	body.StreamOptions.IncludeUsage = true
	for _, t := range req.Tools {
		var w toolDefWire
		w.Type = "function"
		w.Function.Name = t.Name
		w.Function.Description = t.Description
		w.Function.Parameters = t.Parameters
		body.Tools = append(body.Tools, w)
	}
	return json.Marshal(body)
}

// streamChunk is a chat.completion.chunk as streamed over SSE.
type streamChunk struct {
	Choices []struct {
		Delta struct {
			Content          string `json:"content"`
			ReasoningContent string `json:"reasoning_content"`
			Reasoning        string `json:"reasoning"`
			ToolCalls        []struct {
				Index    int    `json:"index"`
				ID       string `json:"id"`
				Type     string `json:"type"`
				Function struct {
					Name      string `json:"name"`
					Arguments string `json:"arguments"`
				} `json:"function"`
			} `json:"tool_calls"`
		} `json:"delta"`
		FinishReason string `json:"finish_reason"`
	} `json:"choices"`
	Usage *struct {
		PromptTokens     int `json:"prompt_tokens"`
		CompletionTokens int `json:"completion_tokens"`
		TotalTokens      int `json:"total_tokens"`
	} `json:"usage"`
}

// toolCallAccumulator accumulates one streamed tool call, merging id and
// name and appending argument fragments.
type toolCallAccumulator struct {
	id        string
	name      strings.Builder
	arguments strings.Builder
}

// consumeStream reads the SSE stream from r until data: [DONE] or EOF.
func consumeStream(ctx context.Context, r io.Reader, onEvent func(Event)) (Message, Usage, error) {
	var (
		content   strings.Builder
		toolCalls = map[int]*toolCallAccumulator{}
		usage     Usage
	)

	reader := bufio.NewReader(r)
	for {
		line, err := reader.ReadString('\n')
		if err != nil && err != io.EOF {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return Message{}, Usage{}, ctxErr
			}
			return Message{}, Usage{}, fmt.Errorf("read event stream: %w", err)
		}

		line = strings.TrimRight(line, "\r\n")
		if data, ok := strings.CutPrefix(line, "data:"); ok {
			data = strings.TrimPrefix(data, " ")
			if data == "[DONE]" {
				break
			}
			var chunk streamChunk
			if jerr := json.Unmarshal([]byte(data), &chunk); jerr != nil {
				return Message{}, Usage{}, fmt.Errorf("malformed SSE data %q: %w", truncate(data, 256), jerr)
			}
			if chunk.Usage != nil {
				usage = Usage{
					PromptTokens:     chunk.Usage.PromptTokens,
					CompletionTokens: chunk.Usage.CompletionTokens,
					TotalTokens:      chunk.Usage.TotalTokens,
				}
			}
			if len(chunk.Choices) > 0 {
				delta := chunk.Choices[0].Delta
				if delta.Content != "" {
					content.WriteString(delta.Content)
					emit(onEvent, Event{Type: EventDelta, Text: delta.Content})
				}
				reasoning := delta.ReasoningContent
				if reasoning == "" {
					reasoning = delta.Reasoning
				}
				if reasoning != "" {
					emit(onEvent, Event{Type: EventReasoning, Text: reasoning})
				}
				for _, tc := range delta.ToolCalls {
					acc := toolCalls[tc.Index]
					if acc == nil {
						acc = &toolCallAccumulator{}
						toolCalls[tc.Index] = acc
					}
					if tc.ID != "" {
						acc.id = tc.ID
					}
					acc.name.WriteString(tc.Function.Name)
					acc.arguments.WriteString(tc.Function.Arguments)
				}
			}
		}
		// Blank lines, comments ("..."), and other SSE fields are ignored.

		if err == io.EOF {
			break
		}
	}

	msg := Message{Role: "assistant", Content: content.String()}
	if len(toolCalls) > 0 {
		indexes := make([]int, 0, len(toolCalls))
		for idx := range toolCalls {
			indexes = append(indexes, idx)
		}
		sort.Ints(indexes)
		for _, idx := range indexes {
			acc := toolCalls[idx]
			msg.ToolCalls = append(msg.ToolCalls, ToolCall{
				ID:        acc.id,
				Name:      acc.name.String(),
				Arguments: json.RawMessage(acc.arguments.String()),
			})
		}
	}
	return msg, usage, nil
}

// completion is a non-streaming chat.completion response.
type completion struct {
	Choices []struct {
		Message json.RawMessage `json:"message"`
	} `json:"choices"`
	Usage *struct {
		PromptTokens     int `json:"prompt_tokens"`
		CompletionTokens int `json:"completion_tokens"`
		TotalTokens      int `json:"total_tokens"`
	} `json:"usage"`
}

// decodeCompletion decodes a non-streaming chat.completion from r,
// used when the endpoint ignored stream=true.
func decodeCompletion(r io.Reader) (Message, Usage, error) {
	var c completion
	if err := json.NewDecoder(r).Decode(&c); err != nil {
		return Message{}, Usage{}, fmt.Errorf("decode chat.completion: %w", err)
	}
	if len(c.Choices) == 0 {
		return Message{}, Usage{}, fmt.Errorf("decode chat.completion: no choices")
	}
	var msg Message
	if err := json.Unmarshal(c.Choices[0].Message, &msg); err != nil {
		return Message{}, Usage{}, fmt.Errorf("decode chat.completion message: %w", err)
	}
	if msg.Role == "" {
		msg.Role = "assistant"
	}
	var usage Usage
	if c.Usage != nil {
		usage = Usage{
			PromptTokens:     c.Usage.PromptTokens,
			CompletionTokens: c.Usage.CompletionTokens,
			TotalTokens:      c.Usage.TotalTokens,
		}
	}
	return msg, usage, nil
}

// emit invokes onEvent when it is not nil.
func emit(onEvent func(Event), ev Event) {
	if onEvent != nil {
		onEvent(ev)
	}
}

// truncate shortens s to at most n bytes for inclusion in error messages.
func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
