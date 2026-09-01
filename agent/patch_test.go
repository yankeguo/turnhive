package agent

import (
	"strings"
	"testing"
)

func TestParsePatchPath(t *testing.T) {
	cases := map[string]string{
		"a/src/foo.ts":             "src/foo.ts",
		"b/src/foo.ts":             "src/foo.ts",
		"src/foo.ts":               "src/foo.ts",
		"a/x.ts\t2024-01-01 10:00": "x.ts",
		"/dev/null":                "/dev/null",
	}
	for in, want := range cases {
		if got := parsePatchPath(in); got != want {
			t.Errorf("parsePatchPath(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestParseUnifiedDiffNewFile(t *testing.T) {
	patch := `--- /dev/null
+++ b/new.txt
@@ -0,0 +1,2 @@
+hello
+world
`
	files := parseUnifiedDiff(patch)
	if len(files) != 1 {
		t.Fatalf("expected 1 file, got %d", len(files))
	}
	if files[0].fromPath != "/dev/null" || files[0].toPath != "new.txt" {
		t.Fatalf("unexpected paths: %+v", files[0])
	}
	merged, err := applyHunksToText("", files[0].hunks, true)
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if merged != "hello\nworld" {
		t.Fatalf("unexpected content %q", merged)
	}
}

func TestParseUnifiedDiffDeletionSkipped(t *testing.T) {
	patch := `--- a/old.txt
+++ /dev/null
@@ -1 +0,0 @@
-gone
--- a/kept.txt
+++ b/kept.txt
@@ -1 +1 @@
-a
+b
`
	files := parseUnifiedDiff(patch)
	if len(files) != 1 || files[0].toPath != "kept.txt" {
		t.Fatalf("expected deletion skipped, kept.txt parsed; got %+v", files)
	}
}

func TestApplyHunksSingleHunk(t *testing.T) {
	files := parseUnifiedDiff(`--- a/f.txt
+++ b/f.txt
@@ -1,3 +1,3 @@
 a
-b
+B
 c
`)
	if len(files) != 1 {
		t.Fatalf("expected 1 file, got %d", len(files))
	}
	merged, err := applyHunksToText("a\nb\nc\n", files[0].hunks, false)
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if merged != "a\nB\nc\n" {
		t.Fatalf("expected trailing newline preserved, got %q", merged)
	}
}

func TestApplyHunksMultiHunk(t *testing.T) {
	existing := "1\n2\n3\n4\n5\n6\n7\n8\n9\n10\n"
	files := parseUnifiedDiff(`--- a/n.txt
+++ b/n.txt
@@ -1,3 +1,3 @@
 1
-2
+two
 3
@@ -8,3 +8,3 @@
 8
-9
+nine
 10
`)
	if len(files) != 1 {
		t.Fatalf("expected 1 file, got %d", len(files))
	}
	merged, err := applyHunksToText(existing, files[0].hunks, false)
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	want := "1\ntwo\n3\n4\n5\n6\n7\n8\nnine\n10\n"
	if merged != want {
		t.Fatalf("got %q, want %q", merged, want)
	}
}

func TestApplyHunksContextMismatch(t *testing.T) {
	files := parseUnifiedDiff(`--- a/f.txt
+++ b/f.txt
@@ -1,3 +1,3 @@
 a
-b
+B
 c
`)
	_, err := applyHunksToText("a\nx\nc", files[0].hunks, false)
	if err == nil || !strings.Contains(err.Error(), "patch context mismatch at line 2") {
		t.Fatalf("expected context mismatch error, got %v", err)
	}
}

func TestApplyHunksNoTrailingNewline(t *testing.T) {
	files := parseUnifiedDiff(`--- a/f.txt
+++ b/f.txt
@@ -1,2 +1,2 @@
 a
-b
+B
`)
	merged, err := applyHunksToText("a\nb", files[0].hunks, false)
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if merged != "a\nB" {
		t.Fatalf("expected no trailing newline added, got %q", merged)
	}
}

func TestApplyHunksNewFileRejectsRemoval(t *testing.T) {
	files := parseUnifiedDiff(`--- /dev/null
+++ b/new.txt
@@ -0,0 +1 @@
-nope
`)
	if len(files) != 1 {
		t.Fatalf("expected 1 file, got %d", len(files))
	}
	_, err := applyHunksToText("", files[0].hunks, true)
	if err == nil || !strings.Contains(err.Error(), "cannot remove lines from a new (empty) file") {
		t.Fatalf("expected removal rejection, got %v", err)
	}
}

func TestApplyHunksContentOutsideHunksPreserved(t *testing.T) {
	existing := "head1\nhead2\na\nb\nc\ntail1\ntail2\n"
	files := parseUnifiedDiff(`--- a/f.txt
+++ b/f.txt
@@ -3,3 +3,3 @@
 a
-b
+B
 c
`)
	merged, err := applyHunksToText(existing, files[0].hunks, false)
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	want := "head1\nhead2\na\nB\nc\ntail1\ntail2\n"
	if merged != want {
		t.Fatalf("got %q, want %q", merged, want)
	}
}
