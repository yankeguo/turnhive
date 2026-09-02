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

// UploadFileNamePattern bounds the client-declared name of an uploaded
// file: plain base names only, no path components (the in-sandbox path
// is always UploadsRoot/<name>, so nothing outside the uploads tree can
// be targeted).
var UploadFileNamePattern = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._-]{0,127}$`)

// UploadsRoot is the in-sandbox directory, relative to the sandbox's
// working directory, where user-provided files are placed.
const UploadsRoot = ".agents/uploads"

// UploadURLTTL is the validity of the presigned URLs file injection
// uses (the sandbox transfers directly).
const UploadURLTTL = 15 * time.Minute

// UploadRecord records one file the user provided to the session as an
// object key in the shared S3 bucket. It is session state, like
// PersistedObject: uploads are durable in object storage and are
// injected into the sandbox on creation and on every rebuild.
type UploadRecord struct {
	// Name is the base file name inside the sandbox
	// (UploadsRoot/<name>); it must match UploadFileNamePattern.
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

// UploadStore is the storage surface the user-files path needs.
// *storage.Store satisfies it.
type UploadStore interface {
	PresignGet(ctx context.Context, key string, ttl time.Duration) (string, error)
}

// InjectUploads copies user-provided objects into the sandbox under
// UploadsRoot/<name>. It runs on sandbox creation and on every sandbox
// rebuild (idle reap, adoption), alongside RestorePersisted.
//
// Files are injected via presigned URLs — the sandbox downloads each
// object itself (PUT /agent/v1/file with the url parameter, same
// mechanism as persisted files and skill tarballs) instead of turnhive
// relaying the bytes.
func InjectUploads(ctx context.Context, sb *ironhive.Sandbox, store UploadStore, uploads []UploadRecord, urlTTL time.Duration) error {
	for _, u := range uploads {
		presigned, err := store.PresignGet(ctx, u.ObjectKey, urlTTL)
		if err != nil {
			return fmt.Errorf("inject file %s: %w", u.Name, err)
		}
		resp, err := sb.AgentDo(ctx, http.MethodPut, "/agent/v1/file", url.Values{
			"path": {path.Join(UploadsRoot, u.Name)},
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

// UploadsDoc is the documented contract for referencing attached files:
// turnhive deliberately does not compose message markers — the caller
// decides if, when and how a message mentions the files. This constant
// only documents where they live.
const UploadsDoc = "user-provided files are available in the sandbox under " + UploadsRoot + "/<name>"
