package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/yankeguo/ironhive"
	"github.com/yankeguo/turnhive/llm"
)

// maxPersistBytes caps a file the persist tool uploads (mirroring the
// agentdesk runner's upload limit).
const maxPersistBytes = 100 * 1024 * 1024 // 100MB

// PersistStore stores and retrieves persisted files. *storage.Store
// satisfies it.
type PersistStore interface {
	PutObject(ctx context.Context, key string, body []byte) error
	GetObject(ctx context.Context, key string) ([]byte, error)
}

// PersistedObject records one file a session persisted to object
// storage. It is session state, not a tool-call footnote: sandboxes are
// disposable, so these objects are what survives the session.
type PersistedObject struct {
	// Path is the in-sandbox path the model persisted.
	Path string `json:"path"`
	// ObjectKey is the S3 object key, relative to the store's configured
	// prefix (same convention as SkillSpec.ObjectKey).
	ObjectKey string `json:"object_key"`
	// Size is the file size in bytes.
	Size int64 `json:"size"`
	// At is when the file was persisted (UTC).
	At time.Time `json:"at"`
}

// RestorePersisted copies persisted objects back into a (fresh) sandbox
// at their original paths. It is used when a session's sandbox is
// rebuilt after being released: the session survives sandboxes, so its
// persisted files must follow into the new one.
func RestorePersisted(ctx context.Context, sb *ironhive.Sandbox, store PersistStore, objects []PersistedObject) error {
	for _, obj := range objects {
		data, err := store.GetObject(ctx, obj.ObjectKey)
		if err != nil {
			return fmt.Errorf("restore %s: %w", obj.Path, err)
		}
		if err := sb.PutFile(ctx, obj.Path, bytes.NewReader(data), nil); err != nil {
			return fmt.Errorf("restore %s: %w", obj.Path, err)
		}
	}
	return nil
}

// sandboxPersist implements the persist tool: a file from the sandbox is
// uploaded to object storage and recorded on the session via the
// OnPersisted hook.
type sandboxPersist struct {
	t           *sandboxTools
	store       PersistStore
	sessionID   string
	onPersisted func(PersistedObject)
}

func (sandboxPersist) Spec() llm.ToolDef {
	return llm.ToolDef{
		Name: "persist",
		Description: `Persist a file from the sandbox to durable object storage.

Usage:
- The sandbox is disposable: everything in it is destroyed when the session ends, and a rebuilt sandbox is restored from persisted files.
- Call this tool for any file that must be kept: final deliverables (reports, artifacts, exports) as well as key intermediate results that later work builds on.
- The file is uploaded to object storage and its object key is recorded on the session.
- Returns the object key of the persisted file.`,
		Parameters: jsonSchema(map[string]any{
			"file_path": stringProp("Path to the file to persist. Relative to the sandbox working directory"),
		}, "file_path"),
	}
}

func (s sandboxPersist) Execute(ctx context.Context, _ string, args json.RawMessage) (string, error) {
	var a filePathArgs
	if err := json.Unmarshal(args, &a); err != nil {
		return "", fmt.Errorf("invalid arguments: %w", err)
	}
	p, err := s.t.resolveForRead(a.FilePath)
	if err != nil {
		return "", err
	}

	r, err := s.t.sb.GetFile(ctx, p)
	if err != nil {
		return "", err
	}
	defer r.Close()
	data, err := io.ReadAll(io.LimitReader(r, maxPersistBytes+1))
	if err != nil {
		return "", fmt.Errorf("read %s: %w", p, err)
	}
	if len(data) > maxPersistBytes {
		return "", fmt.Errorf("file too large to persist: %s (limit %dMB)", a.FilePath, maxPersistBytes/1024/1024)
	}

	// Object keys mirror the in-sandbox path under the session's
	// persisted/ tree, so re-persisting a path overwrites in place.
	key := "sessions/" + s.sessionID + "/persisted/" + strings.TrimPrefix(p, "/")
	if err := s.store.PutObject(ctx, key, data); err != nil {
		return "", fmt.Errorf("persist %s: %w", a.FilePath, err)
	}

	if s.onPersisted != nil {
		s.onPersisted(PersistedObject{
			Path:      a.FilePath,
			ObjectKey: key,
			Size:      int64(len(data)),
			At:        time.Now().UTC(),
		})
	}
	return fmt.Sprintf("File persisted: %s -> %s (%d bytes). It is recorded on the session and survives the sandbox.",
		a.FilePath, key, len(data)), nil
}
