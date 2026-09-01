package agent

import (
	"strings"
	"testing"
)

func TestParseHistoryJSONLSkipsBadLines(t *testing.T) {
	body := `{"Role":"user","Content":"one"}
not-json
{"Role":"assistant","Content":"two"}

{"Role":`
	msgs, err := parseHistoryJSONL([]byte(body))
	if err != nil {
		t.Fatalf("bad lines must not fail the load: %v", err)
	}
	if len(msgs) != 2 || msgs[0].Content != "one" || msgs[1].Content != "two" {
		t.Fatalf("expected the valid lines kept, got %+v", msgs)
	}
}

func TestParseHistoryJSONLLongLine(t *testing.T) {
	// A single line beyond the old 16MB scanner cap still loads.
	long := strings.Repeat("a", 20*1024*1024)
	body := `{"Role":"user","Content":"` + long + `"}` + "\n"
	msgs, err := parseHistoryJSONL([]byte(body))
	if err != nil {
		t.Fatalf("long line must load: %v", err)
	}
	if len(msgs) != 1 || len(msgs[0].Content) != len(long) {
		t.Fatalf("expected the long message, got %d messages", len(msgs))
	}
}
