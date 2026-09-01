package agent

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"
)

// tinyPNG is a 1x1 transparent PNG.
var tinyPNG []byte

func init() {
	b, err := base64.StdEncoding.DecodeString("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8BQDwAEhQGAhKmMIQAAAABJRU5ErkJggg==")
	if err != nil {
		panic(err)
	}
	tinyPNG = b
}

// mediaTools returns the sandbox tools with load_media registered.
func mediaTools(t *testing.T) (*sandboxTools, []Tool) {
	t.Helper()
	sb, _ := newFakeIronhive(t)
	st := newSandboxTools(sb)
	return st, st.list(true)
}

func TestLoadMedia(t *testing.T) {
	st, tools := mediaTools(t)
	if err := st.sb.PutFile(context.Background(), "dot.png", bytes.NewReader(tinyPNG), nil); err != nil {
		t.Fatalf("put image: %v", err)
	}

	text, images, err := dispatchTool(context.Background(), tools, st, "c1", "load_media", json.RawMessage(`{"file_path": "dot.png"}`))
	if err != nil {
		t.Fatalf("load_media: %v", err)
	}
	if !strings.Contains(text, "Image loaded: dot.png (image/png") {
		t.Fatalf("unexpected output %q", text)
	}
	if len(images) != 1 || !strings.HasPrefix(images[0], "data:image/png;base64,") {
		t.Fatalf("unexpected images %v", images)
	}
	got, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(images[0], "data:image/png;base64,"))
	if err != nil || !bytes.Equal(got, tinyPNG) {
		t.Fatalf("data URI does not round-trip: %v", err)
	}
}

func TestLoadMediaSoftFailures(t *testing.T) {
	st, tools := mediaTools(t)

	// Unsupported extension: plain text, no image, no error.
	if err := st.sb.PutFile(context.Background(), "notes.txt", strings.NewReader("hi"), nil); err != nil {
		t.Fatalf("put file: %v", err)
	}
	text, images, err := dispatchTool(context.Background(), tools, st, "c1", "load_media", json.RawMessage(`{"file_path": "notes.txt"}`))
	if err != nil || len(images) != 0 || !strings.Contains(text, "Unsupported image type: .txt") {
		t.Fatalf("expected unsupported-type message, got %q, %v, %v", text, images, err)
	}

	// Oversized file: rejected without an image.
	if err := st.sb.PutFile(context.Background(), "huge.png", bytes.NewReader(make([]byte, maxImageBytes+1)), nil); err != nil {
		t.Fatalf("put huge file: %v", err)
	}
	text, images, err = dispatchTool(context.Background(), tools, st, "c2", "load_media", json.RawMessage(`{"file_path": "huge.png"}`))
	if err != nil || len(images) != 0 || !strings.Contains(text, "Image too large") {
		t.Fatalf("expected too-large message, got %q, %v, %v", text, images, err)
	}

	// Missing file is a hard error.
	if _, _, err = dispatchTool(context.Background(), tools, st, "c3", "load_media", json.RawMessage(`{"file_path": "missing.png"}`)); err == nil {
		t.Fatalf("expected error for missing file")
	}
}

func TestLoadMediaNotRegistered(t *testing.T) {
	sb, _ := newFakeIronhive(t)
	st := newSandboxTools(sb)
	_, _, err := dispatchTool(context.Background(), st.list(false), st, "c1", "load_media", json.RawMessage(`{"file_path": "x.png"}`))
	if err == nil || !strings.Contains(err.Error(), "unknown tool: load_media") {
		t.Fatalf("expected unknown tool without support_image, got %v", err)
	}
}
