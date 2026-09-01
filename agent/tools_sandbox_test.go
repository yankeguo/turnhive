package agent

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

func TestPathResolution(t *testing.T) {
	st := &sandboxTools{root: "/workspace"}

	readOK := []struct {
		in   string
		want string
	}{
		{"a/b.txt", "/workspace/a/b.txt"},
		{"/workspace/x", "/workspace/x"},
		{"/workspace", "/workspace"},
		{"/skills/foo", "/skills/foo"},
		{"a/../b", "/workspace/b"},
		{"/workspace/../skills/x", "/skills/x"},
	}
	for _, c := range readOK {
		got, err := st.resolveForRead(c.in)
		if err != nil {
			t.Errorf("resolveForRead(%q): %v", c.in, err)
			continue
		}
		if got != c.want {
			t.Errorf("resolveForRead(%q) = %q, want %q", c.in, got, c.want)
		}
	}

	escapes := []string{"../etc/passwd", "/etc/passwd", "a/../../b", "..", "/skills/../etc"}
	for _, in := range escapes {
		if _, err := st.resolveForRead(in); err == nil || !strings.Contains(err.Error(), "path escapes workspace") {
			t.Errorf("resolveForRead(%q): expected escape error, got %v", in, err)
		}
	}

	if _, err := st.resolveForRead(""); err == nil {
		t.Errorf("resolveForRead(\"\"): expected error")
	}

	// /skills is read-only.
	if _, err := st.resolveForWrite("/skills/foo"); err == nil || !strings.Contains(err.Error(), "read-only") {
		t.Errorf("resolveForWrite(/skills/foo): expected read-only error, got %v", err)
	}
	if got, err := st.resolveForWrite("dir/f.txt"); err != nil || got != "/workspace/dir/f.txt" {
		t.Errorf("resolveForWrite(dir/f.txt) = %q, %v", got, err)
	}
}

// callTool runs one tool call by name with JSON args.
func callTool(t *testing.T, tools []Tool, name, args string) (string, error) {
	t.Helper()
	for _, tool := range tools {
		if tool.Spec().Name == name {
			return tool.Execute(context.Background(), "call-1", json.RawMessage(args))
		}
	}
	t.Fatalf("tool %q not found", name)
	return "", nil
}

func TestSandboxToolsRoundTrip(t *testing.T) {
	sb, f := newFakeIronhive(t)
	tools := SandboxTools(sb, "/workspace")
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
	out, _ = callTool(t, tools, "read", `{"file_path": "/workspace/sub/hello.txt"}`)
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

	// write to /skills is rejected.
	if _, err := callTool(t, tools, "write", `{"file_path": "/skills/x.txt", "content": "x"}`); err == nil || !strings.Contains(err.Error(), "read-only") {
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

	// shell: stdout, stderr and exit codes.
	out, err = callTool(t, tools, "shell", `{"command": "echo hello"}`)
	if err != nil {
		t.Fatalf("shell: %v", err)
	}
	if out != "hello" {
		t.Fatalf("unexpected shell output %q", out)
	}

	out, err = callTool(t, tools, "shell", `{"command": "echo oops >&2; exit 3"}`)
	if err != nil {
		t.Fatalf("shell: %v", err)
	}
	if !strings.Contains(out, "--- stderr ---\noops") || !strings.Contains(out, "exit code: 3") {
		t.Fatalf("unexpected shell failure output %q", out)
	}

	// shell runs in the workspace root.
	out, err = callTool(t, tools, "shell", `{"command": "ls sub"}`)
	if err != nil || !strings.Contains(out, "hello.txt") {
		t.Fatalf("expected workspace cwd, got %q, %v", out, err)
	}

	// The backing directory really holds the files.
	if _, err := callTool(t, tools, "read", `{"file_path": "../../etc/hostname"}`); err == nil {
		t.Fatalf("expected escape rejection")
	}
	_ = f
}
