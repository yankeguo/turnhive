package agent

import (
	"context"
	"errors"
	"strings"
	"testing"
	"unicode/utf8"
)

func TestTruncateUnderLimitPassthrough(t *testing.T) {
	text := "hello\nworld\n"
	if got := Truncate(text); got != text {
		t.Fatalf("expected passthrough, got %q", got)
	}
}

func TestTruncateLineLimit(t *testing.T) {
	text := "line1\nline2\nline3\nline4\nline5"
	got := Truncate(text, WithMaxLines(3))
	if !strings.HasPrefix(got, "line1\nline2\nline3\n") {
		t.Fatalf("expected first 3 lines kept, got %q", got)
	}
	if strings.Contains(got, "line4") {
		t.Fatalf("expected line4 dropped, got %q", got)
	}
	if !strings.Contains(got, "...2 lines (12 bytes) truncated...") {
		t.Fatalf("expected truncation notice, got %q", got)
	}
	if !strings.HasSuffix(got, DefaultHint) {
		t.Fatalf("expected default hint suffix, got %q", got)
	}
}

func TestTruncateByteLimit(t *testing.T) {
	// 10 lines of 10 bytes each; budget fits exactly two lines
	// (10 + 1 + 10 = 21 bytes).
	var lines []string
	for range 10 {
		lines = append(lines, "0123456789")
	}
	text := strings.Join(lines, "\n")
	got := Truncate(text, WithMaxBytes(25))
	if !strings.HasPrefix(got, "0123456789\n0123456789\n") {
		t.Fatalf("expected 2 lines kept, got %q", got)
	}
	// total = 10*10 + 9 newlines = 109 bytes, kept = 21, removed = 88.
	if !strings.Contains(got, "...8 lines (88 bytes) truncated...") {
		t.Fatalf("expected truncation notice, got %q", got)
	}
}

func TestTruncateSingleOversizedLine(t *testing.T) {
	// One 24-byte line of 3-byte runes; budget 10 bytes keeps exactly
	// 3 runes (9 bytes) without breaking UTF-8.
	text := strings.Repeat("你", 8)
	got := Truncate(text, WithMaxBytes(10))
	if !strings.HasPrefix(got, "你你你") {
		t.Fatalf("expected 3 runes kept, got %q", got)
	}
	preview, _, _ := strings.Cut(got, "\n\n")
	if !utf8.ValidString(preview) {
		t.Fatalf("preview is not valid UTF-8: %q", preview)
	}
	if len(preview) > 10 {
		t.Fatalf("preview exceeds byte budget: %d bytes", len(preview))
	}
	if !strings.Contains(got, "...0 lines (15 bytes) truncated...") {
		t.Fatalf("expected truncation notice, got %q", got)
	}
}

func TestTruncateCustomHint(t *testing.T) {
	text := "a\nb\nc"
	got := Truncate(text, WithMaxLines(1), WithHint("custom hint"))
	if !strings.HasSuffix(got, "custom hint") {
		t.Fatalf("expected custom hint, got %q", got)
	}
}

// fakeSpiller records spills and returns a fixed path.
type fakeSpiller struct {
	contents []string
	path     string
	err      error
}

func (f *fakeSpiller) SpillOutput(_ context.Context, _, content string) (string, error) {
	if f.err != nil {
		return "", f.err
	}
	f.contents = append(f.contents, content)
	return f.path, nil
}

func TestTruncateSpillUnderLimitPassthrough(t *testing.T) {
	spiller := &fakeSpiller{path: ".agents/tool-results/shell-0001.txt"}
	text := "small output"
	got := TruncateSpill(context.Background(), text, "shell", spiller)
	if got != text {
		t.Fatalf("expected passthrough, got %q", got)
	}
	if len(spiller.contents) != 0 {
		t.Fatalf("spiller must not be called for small output")
	}
}

func TestTruncateSpillOversized(t *testing.T) {
	spiller := &fakeSpiller{path: ".agents/tool-results/shell-0001.txt"}
	var lines []string
	for range StrictMaxLines + 100 {
		lines = append(lines, "some output line")
	}
	text := strings.Join(lines, "\n")

	got := TruncateSpill(context.Background(), text, "shell", spiller)
	if len(spiller.contents) != 1 || spiller.contents[0] != text {
		t.Fatalf("expected full output spilled once, got %d spills", len(spiller.contents))
	}
	if !strings.Contains(got, "truncated...") || !strings.Contains(got, "The full output was saved to: .agents/tool-results/shell-0001.txt") {
		t.Fatalf("expected spill hint, got (tail) %q", got[len(got)-200:])
	}
	if len(got) > StrictMaxBytes+4096 {
		t.Fatalf("output not bounded: %d bytes", len(got))
	}
}

func TestTruncateSpillFallback(t *testing.T) {
	// No spiller, and a failing spiller, both degrade to plain strict
	// truncation with the generic hint.
	text := strings.Repeat("x\n", StrictMaxLines+10)
	for _, spiller := range []OutputSpiller{nil, &fakeSpiller{err: errors.New("disk full")}} {
		got := TruncateSpill(context.Background(), text, "shell", spiller)
		if !strings.Contains(got, "truncated...") || !strings.HasSuffix(got, strictHint) {
			t.Fatalf("expected plain truncation with strict hint, got (tail) %q", got[len(got)-200:])
		}
		if strings.Contains(got, "saved to") {
			t.Fatalf("fallback must not mention a spill file: %q", got[len(got)-200:])
		}
	}
}
