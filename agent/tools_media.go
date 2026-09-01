package agent

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"path"
	"strings"

	"github.com/yankeguo/turnhive/llm"
)

// maxImageBytes caps an image load_media reads into memory and base64's
// into the conversation (mirroring the agentdesk runner).
const maxImageBytes = 20 * 1024 * 1024 // 20MB

// imageMIMETypes maps lowercase file extensions to MIME types.
var imageMIMETypes = map[string]string{
	"png":  "image/png",
	"jpg":  "image/jpeg",
	"jpeg": "image/jpeg",
	"gif":  "image/gif",
	"webp": "image/webp",
}

// sandboxLoadMedia implements the load_media tool: an image file from
// the sandbox is base64-encoded and injected into the conversation for
// visual analysis. It is only registered when the session's model
// declares support_image.
type sandboxLoadMedia struct{ t *sandboxTools }

func (sandboxLoadMedia) Spec() llm.ToolDef {
	return llm.ToolDef{
		Name: "load_media",
		Description: `Load a local image file into the conversation context for visual analysis.

Usage:
- Provide a path to an image file (png, jpg, jpeg, gif, webp), relative to the sandbox working directory.
- The image is read, base64-encoded, and injected into the conversation so you can see and analyze it.
- Use this when the user asks you to look at, describe, or analyze an image.`,
		Parameters: jsonSchema(map[string]any{
			"file_path": stringProp("Path to the image file (e.g. \"screenshot.png\"). Relative to the sandbox working directory"),
		}, "file_path"),
	}
}

// Execute satisfies Tool by discarding the images; dispatchTool prefers
// ExecuteImage.
func (s sandboxLoadMedia) Execute(ctx context.Context, callID string, args json.RawMessage) (string, error) {
	text, _, err := s.ExecuteImage(ctx, callID, args)
	return text, err
}

// ExecuteImage reads the image and returns its data URI for injection.
// Soft failures (too large, unsupported type) come back as plain text
// without an image, so the model can adapt; only I/O failures are
// errors.
func (s sandboxLoadMedia) ExecuteImage(ctx context.Context, _ string, args json.RawMessage) (string, []string, error) {
	var a filePathArgs
	if err := json.Unmarshal(args, &a); err != nil {
		return "", nil, fmt.Errorf("invalid arguments: %w", err)
	}
	p, err := s.t.resolveForRead(a.FilePath)
	if err != nil {
		return "", nil, err
	}

	ext := strings.ToLower(strings.TrimPrefix(path.Ext(p), "."))
	mime, ok := imageMIMETypes[ext]
	if !ok {
		return fmt.Sprintf("Unsupported image type: .%s (supported: png, jpg, jpeg, gif, webp)", ext), nil, nil
	}

	r, err := s.t.sb.GetFile(ctx, p)
	if err != nil {
		return "", nil, err
	}
	defer r.Close()
	// Read at most maxImageBytes+1 so an oversized file is rejected
	// without buffering it whole.
	data, err := io.ReadAll(io.LimitReader(r, maxImageBytes+1))
	if err != nil {
		return "", nil, fmt.Errorf("read %s: %w", p, err)
	}
	if len(data) > maxImageBytes {
		return fmt.Sprintf("Image too large: %s (limit %dMB). Resize or compress it first.", a.FilePath, maxImageBytes/1024/1024), nil, nil
	}

	dataURI := "data:" + mime + ";base64," + base64.StdEncoding.EncodeToString(data)
	return fmt.Sprintf("Image loaded: %s (%s, ~%dKB). The image is now in the conversation context.",
		path.Base(p), mime, len(data)/1024), []string{dataURI}, nil
}
