package workflow

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"
)

// Load reads a workflow YAML file from path and returns the parsed
// workflow. The file must have a top-level `workflow:` block — files
// that look like strict pipeline configs (schema_registry / pipelines
// at the top level) are rejected so the product's workflow-first
// story stays sharp.
//
// The workflow's ID is left as-authored; callers that need a default
// (e.g. the base name of the file) should call DefaultID after load.
func Load(path string) (*File, error) {
	fi, err := os.Lstat(path)
	if err != nil {
		return nil, fmt.Errorf("workflow file not found: %s", path)
	}
	if fi.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("workflow file must not be a symlink: %s", path)
	}
	if !fi.Mode().IsRegular() {
		return nil, fmt.Errorf("workflow path is not a regular file: %s", path)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading workflow file: %w", err)
	}
	return Parse(data, path)
}

// allowedTopLevelKeys is the closed set of top-level YAML keys a
// workflow file may declare. Any other top-level key (`pipelines`,
// `auth`, `schema_registry`, etc.) is a hard error — silently
// accepting them is a footgun, most visibly with top-level `auth`
// where a workflow can look secured while serving with auth
// disabled.
var allowedTopLevelKeys = map[string]struct{}{
	"version":  {},
	"workflow": {},
	"runtime":  {},
}

// Parse parses workflow YAML bytes. source is used for error messages
// and may be any human-readable label.
func Parse(data []byte, source string) (*File, error) {
	// Walk the document root first so we can produce a crisp
	// "unknown top-level key" error rather than the raw yaml.v3
	// `field X not found in type workflow.File` output. The extra
	// decode is bounded by document size and runs once per Load.
	if err := checkTopLevelKeys(data, source); err != nil {
		return nil, err
	}

	// Decode into File with KnownFields enforcement as a
	// belt-and-suspenders defense against struct drift: if a future
	// refactor ever adds a new top-level field to File without
	// updating allowedTopLevelKeys, KnownFields would still reject
	// files that carry the drifted key and checkTopLevelKeys would
	// be updated to match. Today the two layers are redundant.
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	var file File
	if err := dec.Decode(&file); err != nil {
		return nil, fmt.Errorf("parse workflow %s: %w", source, err)
	}
	if file.Version == "" {
		return nil, fmt.Errorf("workflow %s: missing version field", source)
	}
	if file.Version != "1" {
		return nil, fmt.Errorf("workflow %s: unsupported version %q (expected \"1\")", source, file.Version)
	}
	if file.Workflow == nil {
		return nil, fmt.Errorf("workflow %s: missing `workflow:` block", source)
	}
	if file.Workflow.ID == "" {
		file.Workflow.ID = defaultIDFromSource(source)
	}
	if err := Validate(file.Workflow); err != nil {
		return nil, fmt.Errorf("workflow %s: %w", source, err)
	}
	return &file, nil
}

// topLevelCommentStripper matches a full-line or end-of-line YAML
// comment so the top-level key scan ignores comments without needing
// a full YAML node walk. `#` inside a string value will be stripped
// too, but that's harmless for the top-level-key sniff: values at
// the document root are never scalars containing `:`.
var topLevelCommentStripper = regexp.MustCompile(`(?m)(^|\s)#.*$`)

// checkTopLevelKeys scans the YAML document root and returns an
// error naming every key outside allowedTopLevelKeys. Ran as a
// separate pass so operators see a targeted error message —
// `yaml: unmarshal errors: field auth not found in type workflow.File`
// is harder to parse than "workflow foo.yaml: unknown top-level key
// \"auth\"".
//
// The scan is intentionally lightweight: it only looks at lines that
// start at column 0 (YAML document root in block style). Flow-style
// root documents (e.g. `{workflow: ..., auth: ...}`) are rare for
// config files and still caught by the downstream KnownFields
// decoder.
func checkTopLevelKeys(data []byte, source string) error {
	stripped := topLevelCommentStripper.ReplaceAllString(string(data), "$1")
	for _, rawLine := range strings.Split(stripped, "\n") {
		// Only block-style document-root keys live at column 0.
		// Indentation means we're inside some other mapping.
		if len(rawLine) == 0 || rawLine[0] == ' ' || rawLine[0] == '\t' || rawLine[0] == '#' || rawLine[0] == '-' {
			continue
		}
		idx := strings.Index(rawLine, ":")
		if idx <= 0 {
			continue
		}
		key := strings.TrimSpace(rawLine[:idx])
		if key == "" {
			continue
		}
		// Skip YAML directives / document markers.
		if strings.HasPrefix(key, "%") || key == "---" || key == "..." {
			continue
		}
		if _, ok := allowedTopLevelKeys[key]; !ok {
			return fmt.Errorf(
				"workflow %s: unknown top-level key %q "+
					"(allowed: version, workflow, runtime) — "+
					"if you meant to author a strict pipeline config, "+
					"remove the `workflow:` block or use `overlord export --advanced`",
				source, key,
			)
		}
	}
	return nil
}

// defaultIDFromSource derives a workflow ID from a filesystem source.
// For non-path sources (bytes without a filename) it falls back to
// "workflow".
func defaultIDFromSource(source string) string {
	base := filepath.Base(source)
	if base == "" || base == "." || base == "/" {
		return "workflow"
	}
	ext := filepath.Ext(base)
	if ext != "" {
		base = strings.TrimSuffix(base, ext)
	}
	// Normalize to the shared identifier character class: first char must
	// be alnum, subsequent chars may include `.`, `-`, `_`.
	var b strings.Builder
	for i, r := range base {
		switch {
		case (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9'):
			b.WriteRune(r)
		case r == '_' || r == '-' || r == '.':
			if i == 0 {
				// Skip leading separators so the result still starts
				// with an alphanumeric character.
				continue
			}
			b.WriteRune(r)
		}
	}
	out := b.String()
	if out == "" {
		return "workflow"
	}
	return out
}

// IsWorkflowShape returns true when the YAML bytes declare a top-level
// `workflow:` block. Used by `overlord run` / `overlord serve` /
// `overlord export` to route to the workflow path when the config file
// is a workflow rather than a strict pipeline config.
//
// The check is intentionally structural: we only look for the
// `workflow:` key at the document root. A workflow file that also
// carries stray pipeline-only keys still detects as a workflow so the
// caller surfaces a clean "cannot mix" error rather than silently
// running the strict-pipeline code path.
func IsWorkflowShape(data []byte) bool {
	var probe struct {
		Workflow any `yaml:"workflow"`
	}
	if err := yaml.Unmarshal(data, &probe); err != nil {
		return false
	}
	return probe.Workflow != nil
}

// IsWorkflowFile returns true when the file at path carries a
// `workflow:` block. Non-existent files and read errors return false
// so callers can fall back to the strict-pipeline path without
// treating a typo as a workflow.
func IsWorkflowFile(path string) bool {
	fi, err := os.Lstat(path)
	if err != nil {
		return false
	}
	if !fi.Mode().IsRegular() || fi.Mode()&os.ModeSymlink != 0 {
		return false
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return false
	}
	return IsWorkflowShape(data)
}
