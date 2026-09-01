package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"

	"github.com/yankeguo/ironhive"
	"github.com/yankeguo/turnhive/llm"
)

// maxPersistBytes caps a file the persist tool uploads (mirroring the
// agentdesk runner's upload limit).
const maxPersistBytes = 100 * 1024 * 1024 // 100MB

// persistURLTTL is the validity of the presigned URLs the persist tool
// and the sandbox restore path use (the sandbox transfers directly).
const persistURLTTL = 15 * time.Minute

// PersistStore presigns uploads and downloads of persisted files.
// *storage.Store satisfies it.
type PersistStore interface {
	PresignPut(ctx context.Context, key string, ttl time.Duration) (string, error)
	PresignGet(ctx context.Context, key string, ttl time.Duration) (string, error)
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
//
// Files are injected via presigned URLs — the sandbox downloads each
// object itself (PUT /agent/v1/file with the url parameter, same
// mechanism as skill tarballs) instead of turnhive relaying the bytes.
func RestorePersisted(ctx context.Context, sb *ironhive.Sandbox, store PersistStore, objects []PersistedObject, urlTTL time.Duration) error {
	for _, obj := range objects {
		presigned, err := store.PresignGet(ctx, obj.ObjectKey, urlTTL)
		if err != nil {
			return fmt.Errorf("restore %s: %w", obj.Path, err)
		}
		resp, err := sb.AgentDo(ctx, http.MethodPut, "/agent/v1/file", url.Values{
			"path": {obj.Path},
			"url":  {presigned},
		}, nil)
		if err != nil {
			return fmt.Errorf("restore %s: %w", obj.Path, err)
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			snippet, _ := io.ReadAll(io.LimitReader(resp.Body, maxSkillErrorBody))
			resp.Body.Close()
			return fmt.Errorf("restore %s: status %d: %s", obj.Path, resp.StatusCode, strings.TrimSpace(string(snippet)))
		}
		resp.Body.Close()
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

	// Enforce the size cap up front via the directory listing (there is
	// no stat endpoint); the agent's upload endpoint 404s a missing file
	// anyway.
	size, err := s.t.fileSize(ctx, p)
	if err != nil {
		return "", err
	}
	if size > maxPersistBytes {
		return "", fmt.Errorf("file too large to persist: %s (limit %dMB)", a.FilePath, maxPersistBytes/1024/1024)
	}

	// Object keys mirror the in-sandbox path under the session's
	// persisted/ tree, so re-persisting a path overwrites in place.
	key := "sessions/" + s.sessionID + "/persisted/" + strings.TrimPrefix(p, "/")

	// The sandbox uploads the file straight to S3 with a presigned PUT
	// URL — turnhive never relays the bytes.
	presigned, err := s.store.PresignPut(ctx, key, persistURLTTL)
	if err != nil {
		return "", fmt.Errorf("persist %s: %w", a.FilePath, err)
	}
	if err := s.t.sb.UploadFile(ctx, p, presigned, &ironhive.UploadOptions{Method: http.MethodPut}); err != nil {
		return "", fmt.Errorf("persist %s: %w", a.FilePath, err)
	}

	if s.onPersisted != nil {
		s.onPersisted(PersistedObject{
			Path:      a.FilePath,
			ObjectKey: key,
			Size:      size,
			At:        time.Now().UTC(),
		})
	}
	return fmt.Sprintf("File persisted: %s -> %s (%d bytes). It is recorded on the session and survives the sandbox.",
		a.FilePath, key, size), nil
}

// fileSize returns the size of the file at the in-sandbox path p, via the
// parent directory listing (ironhive has no stat endpoint).
func (t *sandboxTools) fileSize(ctx context.Context, p string) (int64, error) {
	entries, err := t.sb.ListDir(ctx, path.Dir(p))
	if err != nil {
		return 0, err
	}
	base := path.Base(p)
	for _, e := range entries {
		if e.Name == base {
			if e.Dir {
				return 0, fmt.Errorf("is a directory: %s", p)
			}
			return e.Size, nil
		}
	}
	return 0, fmt.Errorf("not found: %s", p)
}
