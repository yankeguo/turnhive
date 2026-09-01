package agent

import (
	"strings"

	"github.com/yankeguo/turnhive/llm"
)

// Context window management, ported from the agentdesk runner
// (src/agent/context.ts, src/session/history.ts), which in turn follows
// OpenCode's strategy:
//
//   - TruncateToFit runs before a turn: drop oldest WHOLE turns until the
//     estimated history size fits the window (never the last user
//     message).
//   - CompactMessages runs after a turn whose usage crossed the overflow
//     threshold: keep the last keepRecentTurns turns verbatim, condense
//     everything older into a structured <context-summary> user message.
const (
	// charsPerToken is the rough character-per-token heuristic shared by
	// all estimates in this file.
	charsPerToken = 4
	// overflowThreshold triggers compaction when a turn's usage reaches
	// this fraction of the context window.
	overflowThreshold = 0.8
	// keepRecentTurns is the number of recent turns preserved verbatim
	// during compaction.
	keepRecentTurns = 2
	// maxSummaryTokens bounds the generated compaction summary.
	maxSummaryTokens = 4000
	// replyReserve is subtracted from the context window so the model
	// keeps room to answer. The agentdesk runner reserves half its max
	// output; turnhive has no max-output setting, so a fixed reserve is
	// used instead.
	replyReserve = 8000
)

// Token budgets of the summary sections.
const (
	summaryRequestTokens  = 500
	summaryResponseTokens = 800
	keyResponseCount      = 3
	minResponseChars      = 50
)

// EstimateTokens approximates a text's token count (chars/4 heuristic;
// the real count depends on the tokenizer).
func EstimateTokens(text string) int {
	if text == "" {
		return 0
	}
	return (len(text) + charsPerToken - 1) / charsPerToken
}

// estimateMessageTokens approximates the token count of a message list.
// Tool call arguments occupy context too, so they are estimated from
// their serialized size.
func estimateMessageTokens(msgs []llm.Message) int {
	total := 0
	for _, m := range msgs {
		total += EstimateTokens(m.Content)
		for _, tc := range m.ToolCalls {
			total += EstimateTokens(tc.Name) + EstimateTokens(string(tc.Arguments))
		}
	}
	return total
}

// IsOverflow reports whether a turn's usage has reached the overflow
// threshold of the context window. Cache-token accounting of the
// agentdesk runner is not ported: turnhive's llm.Usage has no cache
// fields (OpenAI-style accounting where prompt tokens already include
// them).
func IsOverflow(u llm.Usage, maxContext int) bool {
	if maxContext <= 0 {
		return false
	}
	return u.PromptTokens+u.CompletionTokens >= int(float64(maxContext)*overflowThreshold)
}

// TruncateToFit drops the oldest whole turns of history until the
// estimated size fits (maxContext - reserve). It never drops the last
// user message. changed reports whether anything was dropped.
func TruncateToFit(history []llm.Message, maxContext, reserve int) (out []llm.Message, changed bool) {
	budget := maxContext - reserve
	if budget > 0 && estimateMessageTokens(history) <= budget {
		return history, false
	}

	out = history
	fits := func() bool {
		return budget > 0 && estimateMessageTokens(out) <= budget
	}
	for len(out) > 0 && !fits() {
		head := out[0]
		if head.Role != "user" || len(out) <= 2 {
			// Only whole user+assistant turns are dropped; the last
			// turn (or an unpaired head) always stays — the user's
			// most recent message is never deleted.
			break
		}
		out = out[1:]
		if len(out) > 0 && out[0].Role == "assistant" {
			out = out[1:]
		}
		changed = true
	}
	return out, changed
}

// summarySections is the structured template the summary carries as a
// reference for the model (mirroring the agentdesk runner).
const summarySections = `## Goal
- [single-sentence task summary]

## Constraints & Preferences
- [user constraints, preferences, specs, or "(none)"]

## Progress
### Done
- [completed work or "(none)"]

### In Progress
- [current work or "(none)"]

### Blocked
- [blockers or "(none)"]

## Key Decisions
- [decision and why, or "(none)"]

## Next Steps
- [ordered next actions or "(none)"]

## Critical Context
- [important technical facts, errors, open questions, or "(none)"]

## Relevant Files
- [file or directory path: why it matters, or "(none)"]`

const summaryRules = `Rules:
- Keep every section, even when empty.
- Use terse bullets, not prose paragraphs.
- Preserve exact file paths, commands, error strings, and identifiers when known.
- Do not mention the summary process or that context was compacted.`

// CompactMessages condenses all but the last keepRecentTurns turns of
// history into a structured <context-summary> user message, followed by
// the recent turns verbatim. Histories with too few turns are returned
// unchanged.
func CompactMessages(history []llm.Message) []llm.Message {
	cutoff := compactCutoff(history, keepRecentTurns)
	if cutoff <= 0 {
		return history
	}

	summary := generateSummary(history[:cutoff], maxSummaryTokens)
	compacted := make([]llm.Message, 0, len(history)-cutoff+1)
	compacted = append(compacted, llm.Message{
		Role:    "user",
		Content: "<context-summary>\n" + summary + "\n</context-summary>",
	})
	compacted = append(compacted, history[cutoff:]...)
	return compacted
}

// compactCutoff returns the index where the last keepTurns turns start,
// or 0 when there are not enough turns to compact. A turn starts at a
// user message; anything before the first recent turn is summarized, so
// nothing is silently dropped.
func compactCutoff(history []llm.Message, keepTurns int) int {
	userIdx := []int{}
	for i, m := range history {
		if m.Role == "user" {
			userIdx = append(userIdx, i)
		}
	}
	if len(userIdx) <= keepTurns {
		return 0
	}
	return userIdx[len(userIdx)-keepTurns]
}

// generateSummary builds a deterministic, text-only summary of the old
// messages (no LLM call, mirroring the agentdesk runner): the original
// request, the key recent assistant responses, the structure template
// and continuation instructions.
func generateSummary(old []llm.Message, maxTokens int) string {
	var lines []string

	for _, m := range old {
		if m.Role == "user" && m.Content != "" {
			lines = append(lines, "## Original Request", truncateText(m.Content, summaryRequestTokens), "")
			break
		}
	}

	var assistants []llm.Message
	for _, m := range old {
		if m.Role == "assistant" {
			assistants = append(assistants, m)
		}
	}
	var keyResponses []string
	if len(assistants) > keyResponseCount {
		assistants = assistants[len(assistants)-keyResponseCount:]
	}
	for _, m := range assistants {
		if len(m.Content) > minResponseChars {
			keyResponses = append(keyResponses, truncateText(m.Content, summaryResponseTokens))
		}
	}
	if len(keyResponses) > 0 {
		lines = append(lines, "## Key Progress")
		for _, r := range keyResponses {
			lines = append(lines, r, "")
		}
	}

	lines = append(lines, "## Structure Reference", summarySections, "", summaryRules, "")
	lines = append(lines, "## Instructions",
		"The above is a summary of earlier conversation. Continue from where we left off.",
		"If you need more context about specific files or code, use the appropriate tools to read them again.")

	return truncateText(strings.Join(lines, "\n"), maxTokens)
}

// truncateText shortens text to roughly maxTokens, keeping the head. The
// cut never breaks a multi-byte UTF-8 character.
func truncateText(text string, maxTokens int) string {
	if EstimateTokens(text) <= maxTokens {
		return text
	}
	maxChars := maxTokens * charsPerToken
	if len(text) <= maxChars {
		return text
	}
	return cutToBytes(text, maxChars) + "\n\n[...content truncated for summary...]"
}
