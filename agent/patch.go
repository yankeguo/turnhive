package agent

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// This file ports the minimal unified-diff applier of the agentdesk
// TypeScript runner (runner/src/workspace/index.ts). It is NOT a full GNU
// patch implementation — it covers what the model produces in practice
// (file headers + hunks):
//
//   - multi-file, multi-hunk strict unified diffs;
//   - new files (--- /dev/null), which must not contain '-' lines;
//   - deletions (+++ /dev/null) are silently skipped — file deletion is
//     deliberately not exposed to the model;
//   - strict context matching: each hunk's context/'-' lines must match
//     the existing file at the offset the hunk claims; a mismatch returns
//     an error so the model can re-issue a corrected patch (no fuzz —
//     silent fuzzy matching has produced subtly corrupted files);
//   - content outside the hunks is preserved verbatim.

// patchFile is one file section of a unified diff.
type patchFile struct {
	// fromPath is the --- header path; "/dev/null" marks a new file.
	fromPath string
	// toPath is the +++ header path, with any a//b/ prefix stripped.
	toPath string
	// hunks is the hunk body text (@@ blocks), trailing blank lines
	// trimmed, always ending with a newline when non-empty.
	hunks string
}

// parsePatchPath extracts the path from a `--- `/`+++ ` header payload.
// The payload may carry a trailing tab + timestamp
// ("--- a/x.ts\t2024-01-01 ..."), and git-style patches prefix paths with
// a/ (old) or b/ (new) — both must be stripped. "/dev/null" has no
// prefix; callers compare against it directly.
func parsePatchPath(raw string) string {
	token := strings.TrimSpace(raw)
	if i := strings.IndexByte(token, '\t'); i >= 0 {
		token = token[:i]
	}
	token = strings.TrimSpace(token)
	if token == "/dev/null" {
		return token
	}
	if len(token) > 2 && token[1] == '/' && (token[0] == 'a' || token[0] == 'b') {
		token = token[2:]
	}
	return token
}

// parseUnifiedDiff parses patch into per-file sections. Deletion sections
// (+++ /dev/null) are skipped.
func parseUnifiedDiff(patch string) []patchFile {
	lines := strings.Split(patch, "\n")
	var files []patchFile
	i := 0
	for i < len(lines) {
		for i < len(lines) && !strings.HasPrefix(lines[i], "--- ") {
			i++
		}
		if i >= len(lines) {
			break
		}
		fromPath := parsePatchPath(lines[i][len("--- "):])
		i++
		if i >= len(lines) || !strings.HasPrefix(lines[i], "+++ ") {
			continue
		}
		toPath := parsePatchPath(lines[i][len("+++ "):])
		i++
		if toPath == "/dev/null" {
			// deletion — not exposed to the model; skip. The hunk
			// lines that follow are not "--- " lines, so the scan
			// above walks past them.
			continue
		}
		// Collect hunks until the next file header.
		var hunks []string
		for i < len(lines) && !strings.HasPrefix(lines[i], "--- ") {
			hunks = append(hunks, lines[i])
			i++
		}
		for len(hunks) > 0 && hunks[len(hunks)-1] == "" {
			hunks = hunks[:len(hunks)-1]
		}
		hunkText := ""
		if len(hunks) > 0 {
			hunkText = strings.Join(hunks, "\n") + "\n"
		}
		files = append(files, patchFile{fromPath: fromPath, toPath: toPath, hunks: hunkText})
	}
	return files
}

// hunkHeader matches the @@ -oldStart,oldCount +newStart,newCount @@
// marker; only the old-side start is used, since the merged output is
// simply emitted in order.
var hunkHeader = regexp.MustCompile(`^@@\s+-(\d+)(?:,(\d+))?\s+\+(\d+)(?:,(\d+))?\s+@@`)

// applyHunksToText re-constructs a file from hunks. existing is the
// current file content; hunkText is the body of one or more @@ blocks;
// isNew marks a /dev/null creation. Content outside the hunks — before
// the first hunk, between hunks, and after the last one — is preserved
// verbatim. A trailing newline of existing is preserved. Context/'-'
// mismatches return an error.
func applyHunksToText(existing, hunkText string, isNew bool) (string, error) {
	existingLines := strings.Split(existing, "\n")
	// Drop the trailing empty element of a final newline so line numbers
	// match the model's expectations (hunks address 1-based lines, no
	// trailing empty).
	hadTrailingNewline := len(existingLines) > 0 && existingLines[len(existingLines)-1] == ""
	if hadTrailingNewline {
		existingLines = existingLines[:len(existingLines)-1]
	}

	var out []string
	// cursor is the running pointer into existingLines; everything before
	// copiedCursor has already been emitted to out (verbatim between
	// hunks, or via hunk context/'-' consumption).
	cursor := 0
	copiedCursor := 0

	for _, line := range strings.Split(hunkText, "\n") {
		if strings.HasPrefix(line, "@@") {
			m := hunkHeader.FindStringSubmatch(line)
			if m == nil {
				continue
			}
			oldStart, _ := strconv.Atoi(m[1])
			// Hunk addresses are 1-based: move the cursor to the line
			// *before* the hunk's start; the hunk pushes it forward as
			// it consumes context and '-' lines.
			newCursor := oldStart - 1
			// Emit the untouched region between the previous hunk and
			// this one verbatim, so content outside the hunks is not
			// silently dropped. newCursor is clamped like JS slice()
			// does; an out-of-file hunk start then fails at the first
			// context/'-' line below.
			if !isNew && newCursor > copiedCursor {
				end := min(newCursor, len(existingLines))
				out = append(out, existingLines[copiedCursor:end]...)
			}
			cursor = newCursor
			copiedCursor = newCursor
			continue
		}
		switch {
		case strings.HasPrefix(line, "+"):
			out = append(out, line[1:])
		case strings.HasPrefix(line, "-"):
			if isNew {
				return "", fmt.Errorf("cannot remove lines from a new (empty) file")
			}
			expected := line[1:]
			if cursor >= len(existingLines) || existingLines[cursor] != expected {
				return "", contextMismatchError(cursor, existingLines, expected)
			}
			cursor++
			copiedCursor = cursor
		case strings.HasPrefix(line, " "):
			expected := line[1:]
			if !isNew && (cursor >= len(existingLines) || existingLines[cursor] != expected) {
				return "", contextMismatchError(cursor, existingLines, expected)
			}
			out = append(out, expected)
			cursor++
			copiedCursor = cursor
		}
		// "\ No newline at end of file" markers and other directives
		// are ignored.
	}

	// Emit whatever follows the last hunk verbatim.
	if !isNew && copiedCursor < len(existingLines) {
		out = append(out, existingLines[copiedCursor:]...)
	}

	result := strings.Join(out, "\n")
	// Restore the trailing newline dropped above for line addressing —
	// otherwise applying a patch would silently strip it from the file.
	if !isNew && hadTrailingNewline && result != "" {
		result += "\n"
	}
	return result, nil
}

// contextMismatchError formats the strict-match failure at the 0-based
// cursor (reported 1-based), mirroring the TypeScript error text.
func contextMismatchError(cursor int, existingLines []string, expected string) error {
	var got string
	if cursor < len(existingLines) {
		got = strconv.Quote(existingLines[cursor])
	} else {
		got = "undefined"
	}
	return fmt.Errorf("patch context mismatch at line %d: expected %s got %s", cursor+1, got, strconv.Quote(expected))
}
