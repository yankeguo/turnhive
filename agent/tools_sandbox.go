package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/yankeguo/ironhive"
	"github.com/yankeguo/turnhive/llm"
)

// skillsSandboxRoot is the fixed read-only root inside the sandbox where
// skills are installed; see InstallSkills.
const skillsSandboxRoot = "/skills"

// shellToolTimeout is the fixed per-call timeout of the shell tool.
// Cancelling the context makes the agent kill the command's process
// group.
const shellToolTimeout = 30 * time.Second

// sandboxTools implements the five workspace tools against an ironhive
// sandbox.
type sandboxTools struct {
	sb *ironhive.Sandbox
	// root is the workspace root inside the sandbox, e.g. "/workspace".
	root string
}

// SandboxTools returns the tools that operate inside the sandbox: read,
// write, edit, apply_patch and shell.
//
// Tool file_path arguments are relative to workspaceRoot; absolute paths
// are allowed only under workspaceRoot (read-write) or /skills
// (read-only). Containment is lexical: paths are cleaned and any escape
// from the two roots is rejected.
func SandboxTools(sb *ironhive.Sandbox, workspaceRoot string) []Tool {
	t := &sandboxTools{sb: sb, root: path.Clean(workspaceRoot)}
	return []Tool{
		sandboxRead{t},
		sandboxWrite{t},
		sandboxEdit{t},
		sandboxApplyPatch{t},
		sandboxShell{t},
	}
}

// resolveForRead resolves filePath against the sandbox for a read-only
// operation, returning the absolute in-sandbox path. Both the workspace
// root and /skills are accepted.
func (t *sandboxTools) resolveForRead(filePath string) (string, error) {
	abs, _, err := t.resolve(filePath)
	return abs, err
}

// resolveForWrite resolves filePath like resolveForRead but rejects the
// read-only /skills root.
func (t *sandboxTools) resolveForWrite(filePath string) (string, error) {
	abs, writable, err := t.resolve(filePath)
	if err != nil {
		return "", err
	}
	if !writable {
		return "", fmt.Errorf("path is read-only: %s", filePath)
	}
	return abs, nil
}

// resolve cleans filePath and checks lexical containment in one of the
// sandbox roots, returning the absolute path and whether the root it
// landed in is writable.
func (t *sandboxTools) resolve(filePath string) (abs string, writable bool, err error) {
	if filePath == "" {
		return "", false, errors.New("file_path is required")
	}
	if path.IsAbs(filePath) {
		abs = path.Clean(filePath)
		switch {
		case abs == t.root || strings.HasPrefix(abs, t.root+"/"):
			return abs, true, nil
		case abs == skillsSandboxRoot || strings.HasPrefix(abs, skillsSandboxRoot+"/"):
			return abs, false, nil
		default:
			return "", false, fmt.Errorf("path escapes workspace: %s", filePath)
		}
	}
	rel := path.Clean(filePath)
	if rel == ".." || strings.HasPrefix(rel, "../") {
		return "", false, fmt.Errorf("path escapes workspace: %s", filePath)
	}
	return path.Join(t.root, rel), true, nil
}

// getFile reads the whole file at the absolute in-sandbox path abs.
func (t *sandboxTools) getFile(ctx context.Context, abs string) (string, error) {
	r, err := t.sb.GetFile(ctx, abs)
	if err != nil {
		return "", err
	}
	defer r.Close()
	body, err := io.ReadAll(r)
	if err != nil {
		return "", fmt.Errorf("read %s: %w", abs, err)
	}
	return string(body), nil
}

// isNotExist reports whether err is the sandbox agent's 404.
func isNotExist(err error) bool {
	var ierr *ironhive.Error
	return errors.As(err, &ierr) && ierr.StatusCode == http.StatusNotFound
}

// filePathArgs is the shared arguments shape of the path-based tools.
type filePathArgs struct {
	FilePath string `json:"file_path"`
}

// ────────────────────────────── read ──────────────────────────────

// sandboxRead implements the read tool: files are returned with 1-based
// line numbers; directories as one entry per line.
type sandboxRead struct{ t *sandboxTools }

func (sandboxRead) Spec() llm.ToolDef {
	return llm.ToolDef{
		Name: "read",
		Description: `Read a file or directory from the workspace. If the path does not exist, an error is returned.

Usage:
- The file_path parameter should be relative to the workspace root.
- For files: returns content with each line prefixed by its line number.
- For directories: returns a list of entries (name, type, size).
- Call this tool in parallel when reading multiple files.`,
		Parameters: jsonSchema(map[string]any{
			"file_path": stringProp("Path to read (file or directory). Relative to workspace, or absolute under the workspace root or /skills (read-only)"),
		}, "file_path"),
	}
}

func (s sandboxRead) Execute(ctx context.Context, _ string, args json.RawMessage) (string, error) {
	var a filePathArgs
	if err := json.Unmarshal(args, &a); err != nil {
		return "", fmt.Errorf("invalid arguments: %w", err)
	}
	abs, err := s.t.resolveForRead(a.FilePath)
	if err != nil {
		return "", err
	}

	content, err := s.t.getFile(ctx, abs)
	if err != nil {
		// A directory cannot be fetched as a file; list it instead.
		entries, lerr := s.t.sb.ListDir(ctx, abs)
		if lerr != nil {
			return "", err
		}
		var b strings.Builder
		fmt.Fprintf(&b, "Directory listing for %q:\n", a.FilePath)
		if len(entries) == 0 {
			b.WriteString("(empty)")
		}
		for _, e := range entries {
			if e.Dir {
				fmt.Fprintf(&b, "%s/\n", e.Name)
			} else {
				fmt.Fprintf(&b, "%s (%d bytes)\n", e.Name, e.Size)
			}
		}
		return strings.TrimRight(b.String(), "\n"), nil
	}

	lines := strings.Split(content, "\n")
	var b strings.Builder
	for i, line := range lines {
		fmt.Fprintf(&b, "%d: %s\n", i+1, line)
	}
	return strings.TrimRight(b.String(), "\n"), nil
}

// ────────────────────────────── write ──────────────────────────────

// sandboxWrite implements the write tool; missing parent directories are
// created by the sandbox agent.
type sandboxWrite struct{ t *sandboxTools }

func (sandboxWrite) Spec() llm.ToolDef {
	return llm.ToolDef{
		Name: "write",
		Description: `Writes a file to the workspace.

Usage:
- This tool will overwrite the existing file if there is one at the provided path.
- ALWAYS prefer editing existing files. NEVER write new files unless explicitly required.
- NEVER proactively create documentation files (*.md) or README files.`,
		Parameters: jsonSchema(map[string]any{
			"file_path": stringProp("Path to write. Relative to workspace, or absolute under the workspace root"),
			"content":   stringProp("Content to write"),
		}, "file_path", "content"),
	}
}

func (s sandboxWrite) Execute(ctx context.Context, _ string, args json.RawMessage) (string, error) {
	var a struct {
		FilePath string `json:"file_path"`
		Content  string `json:"content"`
	}
	if err := json.Unmarshal(args, &a); err != nil {
		return "", fmt.Errorf("invalid arguments: %w", err)
	}
	abs, err := s.t.resolveForWrite(a.FilePath)
	if err != nil {
		return "", err
	}
	if err := s.t.sb.PutFile(ctx, abs, strings.NewReader(a.Content), nil); err != nil {
		return "", err
	}
	return "File written to: " + a.FilePath, nil
}

// ────────────────────────────── edit ──────────────────────────────

// sandboxEdit implements the edit tool: an exact string replacement whose
// old_string must occur exactly once in the file.
type sandboxEdit struct{ t *sandboxTools }

func (sandboxEdit) Spec() llm.ToolDef {
	return llm.ToolDef{
		Name: "edit",
		Description: `Performs exact string replacements in files.

Usage:
- Preserve the exact indentation (tabs/spaces) as it appears before.
- The edit will FAIL if old_string is not found in the file.
- The edit will FAIL if old_string is found multiple times in the file.
  Either provide a larger string with more surrounding context to make it unique.`,
		Parameters: jsonSchema(map[string]any{
			"file_path":  stringProp("Path to edit. Relative to workspace, or absolute under the workspace root"),
			"old_string": stringProp("Exact text to replace"),
			"new_string": stringProp("Replacement text"),
		}, "file_path", "old_string", "new_string"),
	}
}

func (s sandboxEdit) Execute(ctx context.Context, _ string, args json.RawMessage) (string, error) {
	var a struct {
		FilePath  string `json:"file_path"`
		OldString string `json:"old_string"`
		NewString string `json:"new_string"`
	}
	if err := json.Unmarshal(args, &a); err != nil {
		return "", fmt.Errorf("invalid arguments: %w", err)
	}
	if a.OldString == "" {
		return "", errors.New("old_string must not be empty")
	}
	abs, err := s.t.resolveForWrite(a.FilePath)
	if err != nil {
		return "", err
	}
	content, err := s.t.getFile(ctx, abs)
	if err != nil {
		return "", err
	}
	occurrences := strings.Count(content, a.OldString)
	if occurrences == 0 {
		return "", fmt.Errorf("old_string not found in %s", a.FilePath)
	}
	if occurrences > 1 {
		return "", fmt.Errorf("old_string found %d times in %s; provide more context to make it unique", occurrences, a.FilePath)
	}
	updated := strings.Replace(content, a.OldString, a.NewString, 1)
	if err := s.t.sb.PutFile(ctx, abs, strings.NewReader(updated), nil); err != nil {
		return "", err
	}
	return "File edited: " + a.FilePath, nil
}

// ─────────────────────────── apply_patch ───────────────────────────

// sandboxApplyPatch implements the apply_patch tool: a strict unified-diff
// applier ported from the TypeScript runner (see patch.go).
type sandboxApplyPatch struct{ t *sandboxTools }

func (sandboxApplyPatch) Spec() llm.ToolDef {
	return llm.ToolDef{
		Name: "apply_patch",
		Description: `Apply a unified-diff patch to workspace files. Multi-hunk, multi-file.

Usage:
- The patch should be in standard unified diff format.
- Each file section starts with --- a/path and +++ b/path headers.
- Lines starting with '-' are removed, '+' are added, ' ' are context.
- New files are created automatically if they don't exist.`,
		Parameters: jsonSchema(map[string]any{
			"patch": stringProp("Unified diff patch content"),
		}, "patch"),
	}
}

func (s sandboxApplyPatch) Execute(ctx context.Context, _ string, args json.RawMessage) (string, error) {
	var a struct {
		Patch string `json:"patch"`
	}
	if err := json.Unmarshal(args, &a); err != nil {
		return "", fmt.Errorf("invalid arguments: %w", err)
	}

	var modified []string
	for _, f := range parseUnifiedDiff(a.Patch) {
		abs, err := s.t.resolveForWrite(f.toPath)
		if err != nil {
			return "", err
		}
		isNew := f.fromPath == "/dev/null"
		existing := ""
		if !isNew {
			existing, err = s.t.getFile(ctx, abs)
			if err != nil {
				if isNotExist(err) {
					return "", fmt.Errorf("cannot apply patch to non-existent file %s (not a /dev/null creation)", f.toPath)
				}
				return "", err
			}
		}
		merged, err := applyHunksToText(existing, f.hunks, isNew)
		if err != nil {
			return "", err
		}
		if err := s.t.sb.PutFile(ctx, abs, strings.NewReader(merged), nil); err != nil {
			return "", err
		}
		modified = append(modified, f.toPath)
	}

	plural := ""
	if len(modified) != 1 {
		plural = "s"
	}
	return fmt.Sprintf("Patch applied. Modified %d file%s:\n%s", len(modified), plural, strings.Join(modified, "\n")), nil
}

// ────────────────────────────── shell ──────────────────────────────

// sandboxShell implements the shell tool: a command run via bash inside
// the sandbox with the workspace root as its working directory.
type sandboxShell struct{ t *sandboxTools }

func (sandboxShell) Spec() llm.ToolDef {
	return llm.ToolDef{
		Name: "shell",
		Description: `Execute a shell command in the workspace directory.

Usage:
- Commands run in the workspace root (cwd is locked).
- Stdout and stderr are captured separately and merged in the response.
- Commands time out after 30 seconds; a timed-out command is killed (whole process group).
- Use for running scripts, build commands, git operations, etc.`,
		Parameters: jsonSchema(map[string]any{
			"command": stringProp("Shell command to execute (runs via bash in the workspace root)"),
		}, "command"),
	}
}

func (s sandboxShell) Execute(ctx context.Context, _ string, args json.RawMessage) (string, error) {
	var a struct {
		Command string `json:"command"`
	}
	if err := json.Unmarshal(args, &a); err != nil {
		return "", fmt.Errorf("invalid arguments: %w", err)
	}
	if a.Command == "" {
		return "", errors.New("command is required")
	}

	ctx, cancel := context.WithTimeout(ctx, shellToolTimeout)
	defer cancel()

	var stdout, stderr strings.Builder
	exitCode := 0
	err := s.t.sb.Shell(ctx, a.Command, &ironhive.ShellOptions{Cwd: s.t.root}, func(ev ironhive.ShellEvent) error {
		switch ev.Type {
		case "stdout":
			stdout.WriteString(ev.Data)
			stdout.WriteByte('\n')
		case "stderr":
			stderr.WriteString(ev.Data)
			stderr.WriteByte('\n')
		case "exit":
			if code, cerr := strconv.Atoi(strings.TrimSpace(ev.Data)); cerr == nil {
				exitCode = code
			}
		}
		return nil
	})
	if err != nil {
		return "", err
	}

	var b strings.Builder
	b.WriteString(strings.TrimRight(stdout.String(), "\n"))
	if s := strings.TrimRight(stderr.String(), "\n"); s != "" {
		if b.Len() > 0 {
			b.WriteByte('\n')
		}
		b.WriteString("--- stderr ---\n")
		b.WriteString(s)
	}
	if exitCode != 0 {
		if b.Len() > 0 {
			b.WriteByte('\n')
		}
		fmt.Fprintf(&b, "exit code: %d", exitCode)
	}
	return b.String(), nil
}
