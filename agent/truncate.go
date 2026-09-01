package agent

import (
	"context"
	"fmt"
	"strings"
	"unicode/utf8"
)

// Default truncation limits for tool output: when output exceeds the
// limits, the head is kept as a preview and a truncation notice with a
// hint is appended, guiding the model to inspect the full content with
// more targeted calls. These generous defaults bound the read tool,
// which truncates itself; every other tool output goes through
// TruncateSpill with the stricter StrictMaxLines/StrictMaxBytes.
const (
	// DefaultMaxLines is the default maximum number of lines kept.
	DefaultMaxLines = 2000
	// DefaultMaxBytes is the default maximum number of UTF-8 bytes kept.
	DefaultMaxBytes = 50 * 1024
	// DefaultHint is appended to truncated output when no hint is given.
	DefaultHint = "Output was truncated to fit within context limits. Use grep to search specific content, or read with offset/limit for specific sections."
)

// Strict truncation limits for non-read tool outputs (shell, external
// tools). Outputs beyond them are spilled to a sandbox file via
// TruncateSpill; only a small head preview goes back to the model.
const (
	// StrictMaxLines is the maximum number of lines kept inline.
	StrictMaxLines = 500
	// StrictMaxBytes is the maximum number of UTF-8 bytes kept inline.
	StrictMaxBytes = 16 * 1024
)

// strictHint is appended to strictly truncated output when no spill file
// is available.
const strictHint = "Output was truncated to fit within context limits. Use grep via the shell tool to search specific content."

// OutputSpiller persists an oversized tool output where the model can
// inspect it later (a file inside the sandbox) and returns the path.
type OutputSpiller interface {
	// SpillOutput writes content and returns its path.
	SpillOutput(ctx context.Context, toolName, content string) (path string, err error)
}

// spillHint points the model at a spilled output file.
func spillHint(path string) string {
	return "The full output was saved to: " + path + "\n" +
		"Inspect it with the read tool, or search and filter it with shell commands (grep, sed, tail)."
}

// TruncateSpill bounds text to the strict limits for non-read tool
// outputs. Oversized text is spilled to a sandbox file through spiller
// and the returned text carries a head preview plus the file path, so no
// information is lost. Without a spiller — or when spilling fails — it
// degrades to plain strict truncation.
func TruncateSpill(ctx context.Context, text, toolName string, spiller OutputSpiller) string {
	hint := strictHint
	if overLimits(text, StrictMaxLines, StrictMaxBytes) && spiller != nil {
		if path, err := spiller.SpillOutput(ctx, toolName, text); err == nil {
			hint = spillHint(path)
		}
	}
	return Truncate(text, WithMaxLines(StrictMaxLines), WithMaxBytes(StrictMaxBytes), WithHint(hint))
}

// overLimits reports whether text exceeds either limit.
func overLimits(text string, maxLines, maxBytes int) bool {
	return strings.Count(text, "\n")+1 > maxLines || len(text) > maxBytes
}

// truncateOptions holds the resolved limits of one Truncate call.
type truncateOptions struct {
	maxLines int
	maxBytes int
	hint     string
}

// TruncateOption customizes one Truncate call.
type TruncateOption func(*truncateOptions)

// WithMaxLines sets the maximum number of lines kept.
func WithMaxLines(n int) TruncateOption {
	return func(o *truncateOptions) { o.maxLines = n }
}

// WithMaxBytes sets the maximum number of UTF-8 bytes kept.
func WithMaxBytes(n int) TruncateOption {
	return func(o *truncateOptions) { o.maxBytes = n }
}

// WithHint sets the hint appended after the truncation notice.
func WithHint(h string) TruncateOption {
	return func(o *truncateOptions) { o.hint = h }
}

// Truncate shortens tool output to fit within context limits. Text within
// both limits is returned unchanged. Otherwise lines are accumulated from
// the head until the line or byte limit is hit, and a notice of the form
// "...N lines (M bytes) truncated..." plus the hint is appended. A single
// first line that alone exceeds the byte budget is hard-cut without
// breaking multi-byte UTF-8 characters.
func Truncate(text string, opts ...TruncateOption) string {
	o := truncateOptions{
		maxLines: DefaultMaxLines,
		maxBytes: DefaultMaxBytes,
		hint:     DefaultHint,
	}
	for _, opt := range opts {
		opt(&o)
	}

	lines := strings.Split(text, "\n")
	originalLines := len(lines)
	originalBytes := len(text) // Go strings count UTF-8 bytes.

	// Quick check: within both limits, return as-is.
	if originalLines <= o.maxLines && originalBytes <= o.maxBytes {
		return text
	}

	// Accumulate lines until a limit is hit.
	var kept []string
	accumulatedBytes := 0
	for i, line := range lines {
		if i >= o.maxLines {
			break
		}
		lineBytes := len(line)
		if len(kept) > 0 {
			lineBytes++ // +1 for the '\n' joining it to the previous line
		}
		if accumulatedBytes+lineBytes > o.maxBytes {
			if len(kept) > 0 {
				break
			}
			// The very first line alone exceeds the byte budget —
			// hard-truncate it instead of letting one oversized line
			// bypass the limit entirely.
			cut := cutToBytes(line, o.maxBytes)
			kept = append(kept, cut)
			accumulatedBytes += len(cut)
			break
		}
		kept = append(kept, line)
		accumulatedBytes += lineBytes
	}

	removedLines := originalLines - len(kept)
	removedBytes := originalBytes - accumulatedBytes

	preview := strings.Join(kept, "\n")
	return fmt.Sprintf("%s\n\n...%d lines (%d bytes) truncated...\n\n%s", preview, removedLines, removedBytes, o.hint)
}

// cutToBytes hard-truncates s to at most budget bytes without breaking
// multi-byte UTF-8 characters.
func cutToBytes(s string, budget int) string {
	if budget <= 0 {
		return ""
	}
	n := 0
	for _, r := range s {
		w := utf8.RuneLen(r)
		if n+w > budget {
			break
		}
		n += w
	}
	// s[:n] is safe: n only advances across whole runes from the start.
	return s[:n]
}
