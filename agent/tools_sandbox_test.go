package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestPathResolution(t *testing.T) {
	st := &sandboxTools{}

	ok := []struct {
		in   string
		want string
	}{
		{"a/b.txt", "a/b.txt"},
		{"a/../b", "b"},
		{".", "."},
		{"./x", "x"},
		{".agents/skills/foo/SKILL.md", ".agents/skills/foo/SKILL.md"},
		// Absolute paths pass through cleaned.
		{"/etc/hostname", "/etc/hostname"},
	}
	for _, c := range ok {
		got, err := st.resolveForRead(c.in)
		if err != nil {
			t.Errorf("resolveForRead(%q): %v", c.in, err)
			continue
		}
		if got != c.want {
			t.Errorf("resolveForRead(%q) = %q, want %q", c.in, got, c.want)
		}
	}

	escapes := []string{"../etc/passwd", "a/../../b", ".."}
	for _, in := range escapes {
		if _, err := st.resolveForRead(in); err == nil || !strings.Contains(err.Error(), "path escapes the working directory") {
			t.Errorf("resolveForRead(%q): expected escape error, got %v", in, err)
		}
	}

	if _, err := st.resolveForRead(""); err == nil {
		t.Errorf("resolveForRead(\"\"): expected error")
	}

	// The skills tree is read-only.
	for _, p := range []string{".agents/skills", ".agents/skills/foo", "./.agents/skills/foo"} {
		if _, err := st.resolveForWrite(p); err == nil || !strings.Contains(err.Error(), "read-only") {
			t.Errorf("resolveForWrite(%q): expected read-only error, got %v", p, err)
		}
	}
	if got, err := st.resolveForWrite("dir/f.txt"); err != nil || got != "dir/f.txt" {
		t.Errorf("resolveForWrite(dir/f.txt) = %q, %v", got, err)
	}
}

// callTool runs one tool call by name with JSON args. Every invocation
// gets a unique call id, mirroring model-assigned tool call ids (the
// shell tool derives its per-call output file names from it).
var callToolN atomic.Int64

func callTool(t *testing.T, tools []Tool, name, args string) (string, error) {
	t.Helper()
	callID := fmt.Sprintf("call-%d", callToolN.Add(1))
	for _, tool := range tools {
		if tool.Spec().Name == name {
			return tool.Execute(context.Background(), callID, json.RawMessage(args))
		}
	}
	t.Fatalf("tool %q not found", name)
	return "", nil
}

func TestSandboxToolsRoundTrip(t *testing.T) {
	sb, f := newFakeIronhive(t)
	tools := SandboxTools(sb)
	ctx := context.Background()
	_ = ctx

	// write → read back with line numbers.
	out, err := callTool(t, tools, "write", `{"file_path": "sub/hello.txt", "content": "first\nsecond\n"}`)
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	if out != "File written to: sub/hello.txt" {
		t.Fatalf("unexpected write output %q", out)
	}

	out, err = callTool(t, tools, "read", `{"file_path": "sub/hello.txt"}`)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if out != "1: first\n2: second\n3: " {
		t.Fatalf("unexpected read output %q", out)
	}

	// edit: unique replacement.
	out, err = callTool(t, tools, "edit", `{"file_path": "sub/hello.txt", "old_string": "second", "new_string": "SECOND"}`)
	if err != nil {
		t.Fatalf("edit: %v", err)
	}
	if out != "File edited: sub/hello.txt" {
		t.Fatalf("unexpected edit output %q", out)
	}
	out, _ = callTool(t, tools, "read", `{"file_path": "./sub/hello.txt"}`)
	if !strings.Contains(out, "2: SECOND") {
		t.Fatalf("edit not applied: %q", out)
	}

	// edit: missing and ambiguous old_string.
	if _, err := callTool(t, tools, "edit", `{"file_path": "sub/hello.txt", "old_string": "nope", "new_string": "x"}`); err == nil || !strings.Contains(err.Error(), "old_string not found") {
		t.Fatalf("expected not-found error, got %v", err)
	}
	if _, err := callTool(t, tools, "write", `{"file_path": "dup.txt", "content": "foo\nfoo\n"}`); err != nil {
		t.Fatalf("write dup: %v", err)
	}
	if _, err := callTool(t, tools, "edit", `{"file_path": "dup.txt", "old_string": "foo", "new_string": "x"}`); err == nil || !strings.Contains(err.Error(), "found 2 times") {
		t.Fatalf("expected ambiguous error, got %v", err)
	}

	// write to the skills tree is rejected.
	if _, err := callTool(t, tools, "write", `{"file_path": ".agents/skills/x.txt", "content": "x"}`); err == nil || !strings.Contains(err.Error(), "read-only") {
		t.Fatalf("expected read-only error, got %v", err)
	}

	// apply_patch: modify existing + create new.
	patch := `--- a/sub/hello.txt
+++ b/sub/hello.txt
@@ -1,3 +1,3 @@
 first
-SECOND
+second
--- /dev/null
+++ b/created.txt
@@ -0,0 +1 @@
+brand new
`
	out, err = callTool(t, tools, "apply_patch", `{"patch": `+jsonString(patch)+`}`)
	if err != nil {
		t.Fatalf("apply_patch: %v", err)
	}
	if !strings.Contains(out, "Modified 2 files") || !strings.Contains(out, "sub/hello.txt") || !strings.Contains(out, "created.txt") {
		t.Fatalf("unexpected apply_patch output %q", out)
	}
	out, _ = callTool(t, tools, "read", `{"file_path": "created.txt"}`)
	if out != "1: brand new" {
		t.Fatalf("unexpected created file content %q", out)
	}

	// apply_patch: patching a non-existent file without /dev/null fails.
	missingPatch := `--- a/ghost.txt
+++ b/ghost.txt
@@ -1 +1 @@
-a
+b
`
	if _, err := callTool(t, tools, "apply_patch", `{"patch": `+jsonString(missingPatch)+`}`); err == nil || !strings.Contains(err.Error(), "non-existent file") {
		t.Fatalf("expected non-existent error, got %v", err)
	}

	// read a directory listing.
	out, err = callTool(t, tools, "read", `{"file_path": "."}`)
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}
	if !strings.Contains(out, "sub/") || !strings.Contains(out, "created.txt (9 bytes)") {
		t.Fatalf("unexpected dir listing %q", out)
	}

	// shell: stdout and always the exit code.
	out, err = callTool(t, tools, "shell", `{"command": "echo hello"}`)
	if err != nil {
		t.Fatalf("shell: %v", err)
	}
	if out != "hello\n(exit code: 0)" {
		t.Fatalf("unexpected shell output %q", out)
	}
	// The first call runs without threaded state; the fake reports cwd "/"
	// and a fixed env.
	if cwd, env, strict := f.lastShellParams(); cwd != "" || env != nil || strict != "" {
		t.Fatalf("first shell call must be stateless, got cwd=%q env=%v strict=%q", cwd, env, strict)
	}

	out, err = callTool(t, tools, "shell", `{"command": "echo oops >&2; exit 3"}`)
	if err != nil {
		t.Fatalf("shell: %v", err)
	}
	if !strings.Contains(out, "[stderr]\noops") || !strings.Contains(out, "(exit code: 3)") {
		t.Fatalf("unexpected shell failure output %q", out)
	}
	// The second call threads the reported cwd/env back with strict_env.
	if cwd, env, strict := f.lastShellParams(); cwd != "/" || len(env) != 1 || env[0] != "PATH=/usr/bin:/bin" || strict != "true" {
		t.Fatalf("second shell call must thread state, got cwd=%q env=%v strict=%q", cwd, env, strict)
	}

	// shell runs in the sandbox working directory.
	out, err = callTool(t, tools, "shell", `{"command": "ls sub"}`)
	if err != nil || !strings.Contains(out, "hello.txt") {
		t.Fatalf("expected sandbox cwd, got %q, %v", out, err)
	}

	// A command with no output reports the placeholder.
	out, err = callTool(t, tools, "shell", `{"command": "true"}`)
	if err != nil || !strings.Contains(out, "(no output)") {
		t.Fatalf("expected (no output), got %q, %v", out, err)
	}

	// The backing directory really holds the files, and .. escapes are
	// rejected.
	if _, err := callTool(t, tools, "read", `{"file_path": "../../etc/hostname"}`); err == nil {
		t.Fatalf("expected escape rejection")
	}
}

func TestSandboxShellBackground(t *testing.T) {
	sb, f := newFakeIronhive(t)
	tools := SandboxTools(sb)

	// bg: true returns immediately with the pid and output file paths.
	start := time.Now()
	out, err := callTool(t, tools, "shell", `{"command": "sleep 1; echo bg-done", "bg": true}`)
	if err != nil {
		t.Fatalf("shell bg: %v", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("bg call did not return immediately: %s", elapsed)
	}
	if !strings.Contains(out, "running in the background (pid ") ||
		!strings.Contains(out, ".agents/shell-logs/") ||
		!strings.Contains(out, "kill -- -") {
		t.Fatalf("unexpected bg reply %q", out)
	}

	// The process finishes on its own and leaves the exit code and
	// output files behind.
	deadline := time.Now().Add(10 * time.Second)
	for {
		entries, _ := os.ReadDir(f.local(shellLogsDir))
		var exitFound bool
		for _, e := range entries {
			if strings.HasSuffix(e.Name(), ".exit") {
				exitFound = true
			}
		}
		if exitFound {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("exit file never appeared in %s", f.local(shellLogsDir))
		}
		time.Sleep(100 * time.Millisecond)
	}
	entries, _ := os.ReadDir(f.local(shellLogsDir))
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".stdout") {
			content, _ := os.ReadFile(f.local(shellLogsDir + "/" + e.Name()))
			if strings.TrimSpace(string(content)) != "bg-done" {
				t.Fatalf("unexpected bg stdout %q", content)
			}
		}
	}
}

func TestSandboxReadSelfTruncates(t *testing.T) {
	sb, f := newFakeIronhive(t)
	tools := SandboxTools(sb)

	// A file beyond the generous read budget.
	var b strings.Builder
	for i := 1; i <= DefaultMaxLines+100; i++ {
		fmt.Fprintf(&b, "line %d\n", i)
	}
	if _, err := callTool(t, tools, "write", `{"file_path": "big.txt", "content": `+jsonString(b.String())+`}`); err != nil {
		t.Fatalf("write: %v", err)
	}

	out, err := callTool(t, tools, "read", `{"file_path": "big.txt"}`)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !strings.Contains(out, "1: line 1") || !strings.Contains(out, "truncated...") || !strings.Contains(out, "grep via the shell tool") {
		t.Fatalf("expected self-truncated read output, got (tail) %q", out[len(out)-200:])
	}
	// read truncates itself; nothing may be spilled.
	if _, err := os.Stat(f.local(spillDir)); !os.IsNotExist(err) {
		t.Fatalf("read must not spill, spill dir exists: %v", err)
	}
}

func TestSandboxReadDirectoryTruncates(t *testing.T) {
	sb, f := newFakeIronhive(t)
	tools := SandboxTools(sb)

	// More entries than the generous read budget.
	if err := os.Mkdir(f.local("many"), 0o755); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < DefaultMaxLines+100; i++ {
		if err := os.WriteFile(f.local(fmt.Sprintf("many/f%05d.txt", i)), nil, 0o644); err != nil {
			t.Fatal(err)
		}
	}

	out, err := callTool(t, tools, "read", `{"file_path": "many"}`)
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}
	if !strings.Contains(out, "f00000.txt") || !strings.Contains(out, "truncated...") {
		t.Fatalf("expected truncated directory listing, got (tail) %q", out[len(out)-200:])
	}
}

func TestSandboxReadOversizedFileCapped(t *testing.T) {
	sb, f := newFakeIronhive(t)
	tools := SandboxTools(sb)

	// A file beyond the in-memory read cap must still return its head
	// rather than OOM-ing or failing.
	if err := os.WriteFile(f.local("huge.bin"), []byte(strings.Repeat("x", maxGetFileBytes+1024)), 0o644); err != nil {
		t.Fatal(err)
	}
	out, err := callTool(t, tools, "read", `{"file_path": "huge.bin"}`)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !strings.Contains(out, "read limit") || !strings.Contains(out, "truncated...") {
		t.Fatalf("expected read-limit notice, got (tail) %q", out[len(out)-300:])
	}
}

func TestApplyPatchNoFileSections(t *testing.T) {
	sb, _ := newFakeIronhive(t)
	tools := SandboxTools(sb)

	// Input without a single valid file section is an error, not a
	// silent "Modified 0 files".
	if _, err := callTool(t, tools, "apply_patch", `{"patch": "this is not a patch at all"}`); err == nil || !strings.Contains(err.Error(), "no file sections found") {
		t.Fatalf("expected no-sections error, got %v", err)
	}
}

func TestShellReadBackCapped(t *testing.T) {
	sb, _ := newFakeIronhive(t)
	tools := SandboxTools(sb)

	// A command whose stdout exceeds the read-back cap: the reply is
	// cut and points at the full output file.
	out, err := callTool(t, tools, "shell", `{"command": "head -c 5000000 /dev/zero | tr '\\0' x"}`)
	if err != nil {
		t.Fatalf("shell: %v", err)
	}
	if !strings.Contains(out, "read-back limit") || !strings.Contains(out, ".agents/shell-logs/") {
		t.Fatalf("expected read-back cap notice, got (tail) %q", out[len(out)-300:])
	}
	if len(out) > maxShellReadBackBytes+4096 {
		t.Fatalf("read-back not capped: %d bytes", len(out))
	}
}

func TestShellEmptyEnvEventKeepsThreadedState(t *testing.T) {
	sb, _ := newFakeIronhive(t)
	st := newSandboxTools(sb)
	tools := st.list(false)

	// Thread state with a first call (the fake reports a fixed env).
	if _, err := callTool(t, tools, "shell", `{"command": "true"}`); err != nil {
		t.Fatalf("shell: %v", err)
	}
	st.shellMu.Lock()
	before := append([]string(nil), st.shellEnv...)
	st.shellMu.Unlock()
	if len(before) == 0 {
		t.Fatal("expected threaded env after first call")
	}

	// An empty env event ("{}") unmarshals to a non-nil empty map; it
	// must not clear the threaded environment.
	if _, err := callTool(t, tools, "write", `{"file_path": ".agents/shell-logs/e.stdout", "content": ""}`); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := callTool(t, tools, "write", `{"file_path": ".agents/shell-logs/e.stderr", "content": ""}`); err != nil {
		t.Fatalf("write: %v", err)
	}
	out, err := sandboxShell{st}.foregroundResult(context.Background(),
		shellOutcome{exitCode: 0, env: map[string]string{}},
		".agents/shell-logs/e.stdout", ".agents/shell-logs/e.stderr")
	if err != nil {
		t.Fatalf("foregroundResult: %v", err)
	}
	if !strings.Contains(out, "(exit code: 0)") {
		t.Fatalf("unexpected output %q", out)
	}
	st.shellMu.Lock()
	after := append([]string(nil), st.shellEnv...)
	st.shellMu.Unlock()
	if strings.Join(after, "\n") != strings.Join(before, "\n") {
		t.Fatalf("empty env event cleared threaded state: %v -> %v", before, after)
	}
}

func TestShellUnknownExitCode(t *testing.T) {
	sb, _ := newFakeIronhive(t)
	st := newSandboxTools(sb)
	tools := st.list(false)

	if _, err := callTool(t, tools, "write", `{"file_path": ".agents/shell-logs/u.stdout", "content": "partial"}`); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := callTool(t, tools, "write", `{"file_path": ".agents/shell-logs/u.stderr", "content": ""}`); err != nil {
		t.Fatalf("write: %v", err)
	}
	// Both the exit event and the exit file failed to parse: the reply
	// must not lie about exit code 0.
	out, err := sandboxShell{st}.foregroundResult(context.Background(),
		shellOutcome{exitCode: -1},
		".agents/shell-logs/u.stdout", ".agents/shell-logs/u.stderr")
	if err != nil {
		t.Fatalf("foregroundResult: %v", err)
	}
	if !strings.Contains(out, "(exit code: unknown)") {
		t.Fatalf("expected unknown exit code, got %q", out)
	}
}
