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
	"sync"
	"sync/atomic"
	"time"

	"github.com/yankeguo/ironhive"
	"github.com/yankeguo/turnhive/llm"
)

// skillsRelRoot is the read-only skills tree, relative to the sandbox's
// working directory; see InstallSkills.
const skillsRelRoot = ".agents/skills"

// spillDir holds oversized tool outputs spilled to files, relative to the
// sandbox's working directory; see SpillOutput. Same .agents root as the
// skills tree, mirroring the agentdesk runner's tool-results layout.
const spillDir = ".agents/tool-results"

// shellForegroundWindow is how long a shell command may run in the
// foreground. A command that outlives the window is NOT killed — it
// moves to the background and the tool returns its PID and output file
// paths, mirroring the agentdesk runner.
const shellForegroundWindow = 30 * time.Second

// shellLogsDir holds the per-call output files of the shell tool
// (stdout/stderr/exit code), under the .agents root.
const shellLogsDir = ".agents/shell-logs"

// shellPidTimeout bounds the wait for the pid event that acknowledges a
// successful spawn.
const shellPidTimeout = 10 * time.Second

// sandboxTools implements the five workspace tools against an ironhive
// sandbox.
type sandboxTools struct {
	sb *ironhive.Sandbox
	// spillN numbers the files written by SpillOutput.
	spillN atomic.Int64

	// shellMu guards the threaded shell state: the working directory and
	// environment reported by the previous foreground shell call, fed back
	// into the next one so cd/export persist within the session
	// (ironhive's documented cwd/env event loop).
	shellMu  sync.Mutex
	shellCwd string
	shellEnv []string
}

// SandboxTools returns the tools that operate inside the sandbox: read,
// write, edit, apply_patch and shell.
//
// Tool file_path arguments are relative to the sandbox's working
// directory (decided by the pool's pod template; turnhive does not
// assume one). ".." escapes are rejected lexically, and the
// .agents/skills tree is read-only. Absolute paths are passed through
// untouched — the sandbox is single-use and disposable.
func SandboxTools(sb *ironhive.Sandbox) []Tool {
	return newSandboxTools(sb).list(false)
}

func newSandboxTools(sb *ironhive.Sandbox) *sandboxTools {
	return &sandboxTools{sb: sb}
}

// list returns the sandbox tools; load_media is included only when
// withMedia is set (the session's model declares support_image).
func (t *sandboxTools) list(withMedia bool) []Tool {
	tools := []Tool{
		sandboxRead{t},
		sandboxWrite{t},
		sandboxEdit{t},
		sandboxApplyPatch{t},
		sandboxShell{t},
	}
	if withMedia {
		tools = append(tools, sandboxLoadMedia{t})
	}
	return tools
}

// SpillOutput implements OutputSpiller: oversized tool output is written
// to a freshly numbered file under .agents/tool-results (parent
// directories are created by the sandbox agent) and its path returned
// for the model.
func (t *sandboxTools) SpillOutput(ctx context.Context, toolName, content string) (string, error) {
	p := fmt.Sprintf("%s/%s-%04d.txt", spillDir, sanitizeFileName(toolName), t.spillN.Add(1))
	if err := t.sb.PutFile(ctx, p, strings.NewReader(content), nil); err != nil {
		return "", err
	}
	return p, nil
}

// sanitizeFileName keeps a tool name safe for use as a file name
// component.
func sanitizeFileName(name string) string {
	return strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_':
			return r
		default:
			return '_'
		}
	}, name)
}

// resolveForRead resolves filePath for a read-only operation, returning
// the in-sandbox path to use.
func (t *sandboxTools) resolveForRead(filePath string) (string, error) {
	return t.resolve(filePath, false)
}

// resolveForWrite resolves filePath like resolveForRead but rejects the
// read-only .agents/skills tree.
func (t *sandboxTools) resolveForWrite(filePath string) (string, error) {
	return t.resolve(filePath, true)
}

// resolve cleans filePath and rejects ".." escapes (and, for writes, the
// read-only skills tree). Relative paths stay relative — they resolve
// against the sandbox's working directory server-side. Absolute paths
// pass through cleaned.
func (t *sandboxTools) resolve(filePath string, write bool) (string, error) {
	if filePath == "" {
		return "", errors.New("file_path is required")
	}
	if path.IsAbs(filePath) {
		return path.Clean(filePath), nil
	}
	rel := path.Clean(filePath)
	if rel == ".." || strings.HasPrefix(rel, "../") {
		return "", fmt.Errorf("path escapes the working directory: %s", filePath)
	}
	if write && (rel == skillsRelRoot || strings.HasPrefix(rel, skillsRelRoot+"/")) {
		return "", fmt.Errorf("path is read-only: %s", filePath)
	}
	return rel, nil
}

// maxGetFileBytes caps how much of a file getFile reads into memory, so
// a pathological in-sandbox file (e.g. /dev/zero) cannot OOM the
// process. The read tool uses getFileCapped to still show the head.
const maxGetFileBytes = 8 * 1024 * 1024 // 8MB

// getFile reads the whole file at the in-sandbox path p; files beyond
// maxGetFileBytes are rejected (edit/apply_patch must never merge against
// a partial read).
func (t *sandboxTools) getFile(ctx context.Context, p string) (string, error) {
	body, truncated, err := t.getFileCapped(ctx, p, maxGetFileBytes)
	if err != nil {
		return "", err
	}
	if truncated {
		return "", fmt.Errorf("file too large: %s (limit %dMB)", p, maxGetFileBytes/1024/1024)
	}
	return body, nil
}

// getFileCapped reads at most limit bytes of the file at the in-sandbox
// path p; truncated reports whether the file is larger than that.
func (t *sandboxTools) getFileCapped(ctx context.Context, p string, limit int64) (body string, truncated bool, err error) {
	r, err := t.sb.GetFile(ctx, p)
	if err != nil {
		return "", false, err
	}
	defer r.Close()
	data, err := io.ReadAll(io.LimitReader(r, limit+1))
	if err != nil {
		return "", false, fmt.Errorf("read %s: %w", p, err)
	}
	if int64(len(data)) > limit {
		return string(data[:limit]), true, nil
	}
	return string(data), false, nil
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
// line numbers; directories as one entry per line. read bounds its own
// output with the generous DefaultMaxLines/DefaultMaxBytes budget, so
// dispatchTool skips the generic (stricter) spill for it — see
// selfTruncatingOutput.
type sandboxRead struct{ t *sandboxTools }

// selfTruncatingOutput marks read as bounding its own output.
func (sandboxRead) selfTruncatingOutput() {}

func (sandboxRead) Spec() llm.ToolDef {
	return llm.ToolDef{
		Name: "read",
		Description: `Read a file or directory. If the path does not exist, an error is returned.

Usage:
- The file_path parameter is relative to the sandbox working directory.
- For files: returns content with each line prefixed by its line number (1-based). Long files are truncated to fit the context; use grep via the shell tool to search the full content.
- For directories: returns a list of entries (name, type, size).
- Call this tool in parallel when reading multiple files.`,
		Parameters: jsonSchema(map[string]any{
			"file_path": stringProp("Path to read (file or directory). Relative to the sandbox working directory"),
		}, "file_path"),
	}
}

func (s sandboxRead) Execute(ctx context.Context, _ string, args json.RawMessage) (string, error) {
	var a filePathArgs
	if err := json.Unmarshal(args, &a); err != nil {
		return "", fmt.Errorf("invalid arguments: %w", err)
	}
	p, err := s.t.resolveForRead(a.FilePath)
	if err != nil {
		return "", err
	}

	content, capped, err := s.t.getFileCapped(ctx, p, maxGetFileBytes)
	if err != nil {
		// A directory cannot be fetched as a file; list it instead.
		entries, lerr := s.t.sb.ListDir(ctx, p)
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
		// read bounds its own output; a huge directory listing is no
		// exception.
		return Truncate(strings.TrimRight(b.String(), "\n"), WithHint(
			"Output was truncated. Use shell commands (ls, find) to list the directory in smaller slices.")), nil
	}

	lines := strings.Split(content, "\n")
	var b strings.Builder
	for i, line := range lines {
		fmt.Fprintf(&b, "%d: %s\n", i+1, line)
	}
	out := Truncate(strings.TrimRight(b.String(), "\n"), WithHint(
		"Output was truncated. Use grep via the shell tool to search specific content in the full file."))
	if capped {
		out += fmt.Sprintf("\n\nThe file exceeds the %dMB read limit; only its head is shown above. Use shell commands (grep, sed, tail) to inspect the rest.",
			maxGetFileBytes/1024/1024)
	}
	return out, nil
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
			"file_path": stringProp("Path to write. Relative to the sandbox working directory"),
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
			"file_path":  stringProp("Path to edit. Relative to the sandbox working directory"),
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
		Description: `Apply a unified-diff patch to files. Multi-hunk, multi-file. Paths are relative to the sandbox working directory.

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

	if len(modified) == 0 {
		// parseUnifiedDiff silently drops input without a valid file
		// section; reporting "Modified 0 files" would let the model
		// believe the patch took effect.
		return "", errors.New("no file sections found in patch")
	}

	plural := ""
	if len(modified) != 1 {
		plural = "s"
	}
	return fmt.Sprintf("Patch applied. Modified %d file%s:\n%s", len(modified), plural, strings.Join(modified, "\n")), nil
}

// ────────────────────────────── shell ──────────────────────────────

// sandboxShell implements the shell tool: a command run via bash inside
// the sandbox. The working directory and exported variables are threaded
// across foreground calls within the session (ironhive reports them as
// cwd/env events; the next call feeds them back), so cd and export
// persist.
//
// Output is redirected to per-call files under .agents/shell-logs from
// the start, so a command that outlives the 30s foreground window (or
// runs with bg: true) keeps writing after the tool returns; ironhive
// reports the command's pid (its process group, Setpgid) as the first
// SSE event, which is what the model signals to stop a backgrounded
// command.
type sandboxShell struct{ t *sandboxTools }

func (sandboxShell) Spec() llm.ToolDef {
	return llm.ToolDef{
		Name: "shell",
		Description: `Execute a shell command in the sandbox.

Usage:
- The first command runs in the sandbox working directory; cd and exported variables (use export, not plain assignments) persist to subsequent foreground shell calls within the session.
- Stdout and stderr are captured separately and merged in the response.
- Foreground commands return when they finish. A command still running after 30 seconds is NOT killed: it moves to the background and the tool returns its PID and stdout/stderr/exit-code file paths. Pass bg: true to get that behavior from the start (e.g. servers, watchers, long builds).
- For backgrounded commands: poll output with tail/cat on the returned files; the exit-code file appears when the process exits; stop it (whole process group) with: kill -- -<pid>.
- Backgrounded processes keep running even if the current turn ends; they die with the sandbox when the session is deleted.
- Use for running scripts, build commands, git operations, etc.`,
		Parameters: jsonSchema(map[string]any{
			"command": stringProp("Shell command to execute (runs via bash)"),
			"bg":      boolProp("Run in background from the start: returns immediately with the PID and output file paths instead of waiting"),
		}, "command"),
	}
}

// shellOutcome is the terminal state of one shell call, collected from
// the SSE stream after the command exits.
type shellOutcome struct {
	exitCode int
	cwd      string
	env      map[string]string
	err      error
}

func (s sandboxShell) Execute(ctx context.Context, callID string, args json.RawMessage) (string, error) {
	var a struct {
		Command string `json:"command"`
		Bg      bool   `json:"bg"`
	}
	if err := json.Unmarshal(args, &a); err != nil {
		return "", fmt.Errorf("invalid arguments: %w", err)
	}
	if a.Command == "" {
		return "", errors.New("command is required")
	}

	// Per-call output files. Inside the wrapper they are relative (the
	// command runs in the threaded cwd, so the fake and the real sandbox
	// both resolve them correctly); for read-back and for the model they
	// are anchored at that same cwd, absolute once it is known — a
	// backgrounded command's files stay findable after further cd-ing.
	s.t.shellMu.Lock()
	cwd := s.t.shellCwd
	var opts *ironhive.ShellOptions
	if cwd != "" || s.t.shellEnv != nil {
		opts = &ironhive.ShellOptions{Cwd: cwd, Env: s.t.shellEnv, StrictEnv: true}
	}
	s.t.shellMu.Unlock()

	base := shellLogsDir + "/" + sanitizeFileName(callID)
	anchor := func(rel string) string {
		if cwd == "" {
			return rel
		}
		return path.Join(cwd, rel)
	}
	stdoutFile := anchor(base + ".stdout")
	stderrFile := anchor(base + ".stderr")
	exitFile := anchor(base + ".exit")

	// The command runs detached from the tool call: cancelling the turn
	// or returning early must not kill it (ironhive SIGTERMs the process
	// group when the HTTP call aborts). Its lifetime is bounded by the
	// sandbox — releasing the session kills the stream.
	runCtx, runCancel := context.WithCancel(context.Background())
	done := make(chan shellOutcome, 1)
	pidCh := make(chan int, 1)
	go func() {
		defer runCancel()
		// exitCode starts at a sentinel: the exit event reports the
		// wrapper group's code, and the real code comes from the exit
		// file below. If both are missing or malformed, the reply must
		// say "unknown" rather than lie about 0.
		o := shellOutcome{exitCode: -1}
		pidSent := false
		err := s.t.sb.Shell(runCtx, buildShellWrapper(a.Command, shellLogsDir, base), opts, func(ev ironhive.ShellEvent) error {
			switch ev.Type {
			case "pid":
				if !pidSent {
					if pid, perr := strconv.Atoi(strings.TrimSpace(ev.Data)); perr == nil {
						pidCh <- pid
						pidSent = true
					}
				}
			case "exit":
				// A malformed exit event leaves the sentinel in place;
				// the exit file below is the authoritative source, and
				// if that also fails the reply reads "(exit code:
				// unknown)".
				if code, cerr := strconv.Atoi(strings.TrimSpace(ev.Data)); cerr == nil {
					o.exitCode = code
				}
			case "cwd":
				o.cwd = strings.TrimSpace(ev.Data)
			case "env":
				// The event data is a JSON object of the full environment.
				_ = json.Unmarshal([]byte(ev.Data), &o.env)
			}
			return nil
		})
		if err != nil {
			o.err = err
			done <- o
			return
		}
		// The exit event reports the wrapper group's code (always 0); the
		// real exit code is in the exit file. A SIGKILLed command leaves
		// no exit file — fall back to the event.
		if content, rerr := s.t.getFile(runCtx, exitFile); rerr == nil {
			if code, cerr := strconv.Atoi(strings.TrimSpace(content)); cerr == nil {
				o.exitCode = code
			}
		}
		done <- o
	}()

	// Wait for the spawn acknowledgement (pid event) or an early failure.
	var pid int
	select {
	case pid = <-pidCh:
	case o := <-done:
		if o.err != nil {
			return "", o.err
		}
		// Finished before the pid arrived; treat as a fast foreground
		// command regardless of bg.
		return s.foregroundResult(ctx, o, stdoutFile, stderrFile)
	case <-time.After(shellPidTimeout):
		runCancel()
		return "", errors.New("shell: no pid event within 10s; spawn likely failed")
	case <-ctx.Done():
		runCancel()
		return "", ctx.Err()
	}

	if !a.Bg {
		select {
		case o := <-done:
			if o.err != nil {
				return "", o.err
			}
			return s.foregroundResult(ctx, o, stdoutFile, stderrFile)
		case <-time.After(shellForegroundWindow):
			// Not killed: falls through to the background reply. The
			// outcome is discarded, so the threaded state only ever comes
			// from commands that completed in the foreground.
		case <-ctx.Done():
			// Turn aborted: kill the still-foreground command's process
			// group by cancelling the HTTP call.
			runCancel()
			return "", ctx.Err()
		}
	}

	return fmt.Sprintf("Command is running in the background (pid %d).\n"+
		"stdout: %s\nstderr: %s\nexit code file (written on exit): %s\n"+
		"Poll with tail/cat; stop it (whole process group) with: kill -- -%d",
		pid, stdoutFile, stderrFile, exitFile, pid), nil
}

// maxShellReadBackBytes caps how much of a completed command's
// stdout/stderr files is read back into the tool reply.
const maxShellReadBackBytes = 4 * 1024 * 1024 // 4MB

// foregroundResult threads the reported shell state into the next call
// and formats the completed command's output from its files.
func (s sandboxShell) foregroundResult(ctx context.Context, o shellOutcome, stdoutFile, stderrFile string) (string, error) {
	s.t.shellMu.Lock()
	if o.cwd != "" {
		s.t.shellCwd = o.cwd
	}
	// An empty env event ("{}") unmarshals to a non-nil empty map; it
	// must not clear the threaded environment — the next StrictEnv call
	// would wipe every variable.
	if len(o.env) > 0 {
		env := make([]string, 0, len(o.env))
		for k, v := range o.env {
			env = append(env, k+"="+v)
		}
		s.t.shellEnv = env
	}
	s.t.shellMu.Unlock()

	stdout, stdoutCapped, err := s.t.getFileCapped(ctx, stdoutFile, maxShellReadBackBytes)
	if err != nil {
		return "", fmt.Errorf("read back stdout: %w", err)
	}
	stderr, stderrCapped, err := s.t.getFileCapped(ctx, stderrFile, maxShellReadBackBytes)
	if err != nil {
		return "", fmt.Errorf("read back stderr: %w", err)
	}

	// Output format mirrors the agentdesk runner: stdout, then the stderr
	// section, "(no output)" when both are empty, and always the exit
	// code.
	out := strings.TrimRight(stdout, "\n")
	errOut := strings.TrimRight(stderr, "\n")
	var parts []string
	if out != "" {
		parts = append(parts, out)
	}
	if errOut != "" {
		parts = append(parts, "[stderr]\n"+errOut)
	}
	if out == "" && errOut == "" {
		parts = append(parts, "(no output)")
	}
	if stdoutCapped || stderrCapped {
		parts = append(parts, fmt.Sprintf("Output exceeded the %dMB read-back limit and was cut off. The full output is in the sandbox at %s and %s; inspect it with tail/grep.",
			maxShellReadBackBytes/1024/1024, stdoutFile, stderrFile))
	}
	if o.exitCode >= 0 {
		parts = append(parts, fmt.Sprintf("(exit code: %d)", o.exitCode))
	} else {
		parts = append(parts, "(exit code: unknown)")
	}
	return strings.Join(parts, "\n"), nil
}

// buildShellWrapper wraps command so its stdout/stderr and exit code go
// to per-call files from the start: a command that moves to the
// background keeps writing after the tool returns, and the foreground
// read-back is byte-exact (no SSE line framing loss). The paths are
// relative to the command's working directory; base is
// ".agents/shell-logs/<call>".
func buildShellWrapper(command, logDir, base string) string {
	q := func(p string) string { return "'" + strings.ReplaceAll(p, "'", `'\''`) + "'" }
	return "mkdir -p " + q(logDir) + "\n{\n" + command + "\ncode=$?\n" +
		"echo \"$code\" > " + q(base+".exit") + "\n} > " + q(base+".stdout") + " 2> " + q(base+".stderr")
}
