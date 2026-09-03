package agent

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"regexp"
	"strings"
	"time"

	"github.com/yankeguo/ironhive"
)

// FileNamePattern bounds the client-declared name of an attached
// file: plain base names only, no path components (the in-sandbox path
// is always FilesRoot/<name>, so nothing outside the files tree can
// be targeted).
var FileNamePattern = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._-]{0,127}$`)

// FilesRoot is the in-sandbox directory, relative to the sandbox's
// working directory, where user-provided files are placed. It is the
// only contract for referencing attached files in messages: turnhive
// deliberately does not compose message markers — the caller decides
// if, when and how a message mentions them.
const FilesRoot = ".agents/uploads"

// PresignURLTTL is the validity of the presigned URLs every
// sandbox-direct transfer uses (skill tarballs, persisted-file restore
// and user-file injection all have the sandbox fetch from S3 itself).
const PresignURLTTL = 15 * time.Minute

// FileRecord records one file the user provided to the session as an
// object key in the shared S3 bucket. It is session state, like
// PersistedObject: files are durable in object storage and are
// injected into the sandbox on creation and on every rebuild.
type FileRecord struct {
	// Name is the base file name inside the sandbox
	// (FilesRoot/<name>); it must match FileNamePattern.
	Name string `json:"name"`
	// ObjectKey is the S3 object key, relative to the store's configured
	// prefix (same convention as SkillSpec.ObjectKey). turnhive never
	// relays the bytes: the caller puts the object into the shared
	// bucket itself, and the sandbox downloads it through a presigned
	// URL.
	ObjectKey string `json:"object_key"`
	// Size is the file size in bytes as declared by the caller
	// (informational; echoed in the message marker).
	Size int64 `json:"size,omitempty"`
	// At is when the file was registered on the session (UTC).
	At time.Time `json:"at"`
}

// FileStore is the storage surface the user-files path needs.
// *storage.Store satisfies it.
type FileStore interface {
	PresignGet(ctx context.Context, key string, ttl time.Duration) (string, error)
}

// InjectFiles copies user-provided objects into the sandbox under
// FilesRoot/<name>. It runs on sandbox creation and on every sandbox
// rebuild (idle reap, adoption), alongside RestorePersisted.
//
// Files are injected via presigned URLs — the sandbox downloads each
// object itself (PUT /agent/v1/file with the url parameter, same
// mechanism as persisted files and skill tarballs) instead of turnhive
// relaying the bytes.
func InjectFiles(ctx context.Context, sb *ironhive.Sandbox, store FileStore, files []FileRecord, urlTTL time.Duration) error {
	for _, u := range files {
		presigned, err := store.PresignGet(ctx, u.ObjectKey, urlTTL)
		if err != nil {
			return fmt.Errorf("inject file %s: %w", u.Name, err)
		}
		resp, err := sb.AgentDo(ctx, http.MethodPut, "/agent/v1/file", url.Values{
			"path": {path.Join(FilesRoot, u.Name)},
			"url":  {presigned},
		}, nil)
		if err != nil {
			return fmt.Errorf("inject file %s: %w", u.Name, err)
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			snippet, _ := io.ReadAll(io.LimitReader(resp.Body, maxSkillErrorBody))
			resp.Body.Close()
			return fmt.Errorf("inject file %s: status %d: %s", u.Name, resp.StatusCode, strings.TrimSpace(string(snippet)))
		}
		resp.Body.Close()
	}
	return nil
}
