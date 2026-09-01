package agent

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/yankeguo/ironhive"
	"github.com/yankeguo/turnhive/storage"
)

// maxSkillErrorBody caps how much of a failing agent response body is
// included in the returned error.
const maxSkillErrorBody = 1024

// SkillRef references a skill tarball stored in S3.
type SkillRef struct {
	Name        string
	Description string
	// ObjectKey is the S3 object key of the skill tarball.
	ObjectKey string
}

// InstallSkills downloads every skill tarball into the sandbox at
// skillsRoot/<name> via S3 presigned URLs (the sandbox fetches the URL
// itself). A failure aborts with an error; the caller is expected to
// release the sandbox.
func InstallSkills(ctx context.Context, sb *ironhive.Sandbox, store *storage.Store, skills []SkillRef, skillsRoot string, urlTTL time.Duration) error {
	for _, skill := range skills {
		presigned, err := store.PresignGet(ctx, skill.ObjectKey, urlTTL)
		if err != nil {
			return fmt.Errorf("install skill %q: %w", skill.Name, err)
		}
		resp, err := sb.AgentDo(ctx, http.MethodPut, "/agent/v1/tar", url.Values{
			"path": {strings.TrimRight(skillsRoot, "/") + "/" + skill.Name},
			"url":  {presigned},
		}, nil)
		if err != nil {
			return fmt.Errorf("install skill %q: %w", skill.Name, err)
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			snippet, _ := io.ReadAll(io.LimitReader(resp.Body, maxSkillErrorBody))
			resp.Body.Close()
			return fmt.Errorf("install skill %q: status %d: %s", skill.Name, resp.StatusCode, strings.TrimSpace(string(snippet)))
		}
		resp.Body.Close()
	}
	return nil
}

// BuildSystemPrompt composes the system prompt: the session's system
// prompt plus one section per skill.
func BuildSystemPrompt(system string, skills []SkillRef, skillsRoot string) string {
	var b strings.Builder
	b.WriteString(system)
	for _, skill := range skills {
		if b.Len() > 0 {
			b.WriteString("\n\n")
		}
		fmt.Fprintf(&b, "### Skill: %s\n\n%s\n\n(Skill files on disk, read-only: %s/%s/)",
			skill.Name, skill.Description, strings.TrimRight(skillsRoot, "/"), skill.Name)
	}
	return b.String()
}
