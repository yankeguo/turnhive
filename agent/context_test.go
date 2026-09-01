package agent

import (
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/yankeguo/turnhive/llm"
)

func TestEstimateTokens(t *testing.T) {
	if got := EstimateTokens(""); got != 0 {
		t.Fatalf("empty: %d", got)
	}
	if got := EstimateTokens(strings.Repeat("a", 100)); got != 25 {
		t.Fatalf("100 chars: %d, want 25", got)
	}
	if got := EstimateTokens("abc"); got != 1 {
		t.Fatalf("3 chars rounds up: %d", got)
	}
}

func TestIsOverflow(t *testing.T) {
	if IsOverflow(llm.Usage{}, 0) {
		t.Fatal("disabled when maxContext is 0")
	}
	if IsOverflow(llm.Usage{PromptTokens: 7000, CompletionTokens: 999}, 10000) {
		t.Fatal("7999 < 8000 must not overflow")
	}
	if !IsOverflow(llm.Usage{PromptTokens: 7000, CompletionTokens: 1000}, 10000) {
		t.Fatal("8000 = 0.8 * 10000 must overflow")
	}
}

// pairs builds n {user, assistant} history pairs.
func pairs(n int, tag string) []llm.Message {
	var msgs []llm.Message
	for i := 0; i < n; i++ {
		msgs = append(msgs,
			llm.Message{Role: "user", Content: tag + " user message with a fair amount of text"},
			llm.Message{Role: "assistant", Content: tag + " assistant reply with a fair amount of text content"})
	}
	return msgs
}

func TestTruncateToFitFits(t *testing.T) {
	h := pairs(3, "small")
	out, changed := TruncateToFit(h, 100000, 1000)
	if changed || len(out) != len(h) {
		t.Fatalf("fitting history must pass through: changed=%v len=%d", changed, len(out))
	}
}

func TestTruncateToFitDropsOldestTurns(t *testing.T) {
	h := pairs(5, "x")
	full := estimateMessageTokens(h)
	// Budget fits the last two pairs with a little slack.
	reserve := full - estimateMessageTokens(h[len(h)-4:]) - 10
	out, changed := TruncateToFit(h, full, reserve)
	if !changed {
		t.Fatal("expected trimming")
	}
	if len(out) != 4 || out[0].Role != "user" {
		t.Fatalf("expected last 2 pairs kept, got %d messages starting with %s", len(out), out[0].Role)
	}
	if estimateMessageTokens(out) > full-reserve {
		t.Fatalf("trimmed history still over budget")
	}
}

func TestTruncateToFitNeverDropsLastUser(t *testing.T) {
	h := pairs(3, "x")
	// Absurd budget: nothing fits; the last turn must survive (the
	// user's most recent message is never dropped).
	out, changed := TruncateToFit(h, 100, 8000)
	if !changed {
		t.Fatal("expected trimming")
	}
	if len(out) != 2 || out[0].Role != "user" || out[1].Role != "assistant" {
		t.Fatalf("expected the last turn kept, got %+v", out)
	}
}

func TestCompactMessagesTooFewTurns(t *testing.T) {
	h := pairs(2, "x")
	if got := CompactMessages(h); len(got) != len(h) {
		t.Fatalf("too few turns must pass through, got %d messages", len(got))
	}
}

func TestCompactMessages(t *testing.T) {
	h := pairs(5, "turn")
	got := CompactMessages(h)

	// summary user message + last 2 turns (4 messages)
	if len(got) != 5 {
		t.Fatalf("expected 5 messages, got %d", len(got))
	}
	if got[0].Role != "user" || !strings.HasPrefix(got[0].Content, "<context-summary>") {
		t.Fatalf("first message must be the context summary: %+v", got[0])
	}
	if !strings.Contains(got[0].Content, "## Original Request") || !strings.Contains(got[0].Content, "turn user message") {
		t.Fatalf("summary must carry the original request: %q", got[0].Content[:200])
	}
	if !strings.Contains(got[0].Content, "## Key Progress") {
		t.Fatalf("summary must carry key progress")
	}
	// The last two turns are verbatim.
	for i, m := range got[1:] {
		if m.Content != h[len(h)-4+i].Content {
			t.Fatalf("recent turn %d altered: %q vs %q", i, m.Content, h[len(h)-4+i].Content)
		}
	}
	// The summary itself is bounded.
	if EstimateTokens(got[0].Content) > maxSummaryTokens+200 {
		t.Fatalf("summary exceeds budget: %d tokens", EstimateTokens(got[0].Content))
	}
}

func TestTruncateTextMultibyteBoundary(t *testing.T) {
	// 3-byte runes; the byte-budget cut must keep whole characters (no
	// mojibake).
	text := strings.Repeat("你", 100) // 300 bytes, ~75 tokens
	got := truncateText(text, 10)    // 40-byte budget keeps 13 runes (39 bytes)
	if !strings.HasPrefix(got, strings.Repeat("你", 13)) {
		t.Fatalf("expected 13 whole runes kept, got %q", got[:60])
	}
	if !utf8.ValidString(got) {
		t.Fatalf("truncation broke UTF-8: %q", got[:60])
	}
	if !strings.HasSuffix(got, "[...content truncated for summary...]") {
		t.Fatalf("expected truncation notice, got %q", got)
	}
}
