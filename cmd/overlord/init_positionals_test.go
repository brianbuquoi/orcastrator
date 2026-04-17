package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestResolveInitPositionals covers the documented grammar
// `overlord init [template] [dir]` where both positionals are
// optional. The prior implementation treated arg0 as a template name
// unconditionally; the audit reproduced `overlord init /tmp/my-project`
// failing with "unknown template" because the path was parsed as a
// template name. These cases lock in the disambiguation rules.
func TestResolveInitPositionals(t *testing.T) {
	const defaultTemplate = "starter"
	cases := []struct {
		name         string
		args         []string
		wantTemplate string
		wantTarget   string
		wantErr      bool
	}{
		{"no_args_defaults", nil, "starter", "./starter", false},
		{"template_only", []string{"hello"}, "hello", "./hello", false},
		{"dir_only_abs", []string{"/tmp/proj"}, "starter", "/tmp/proj", false},
		{"dir_only_dot_slash", []string{"./proj"}, "starter", "./proj", false},
		{"dir_only_dot_dot", []string{"../proj"}, "starter", "../proj", false},
		{"dir_only_cwd", []string{"."}, "starter", ".", false},
		{"dir_only_nested", []string{"workspace/proj"}, "starter", "workspace/proj", false},
		{"both_args", []string{"hello", "./dir"}, "hello", "./dir", false},
		{"empty_arg0", []string{""}, "starter", "./starter", false},
		{"empty_arg0_with_arg1", []string{"", "./dir"}, "", "", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			template, target, err := resolveInitPositionals(tc.args, defaultTemplate)
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if template != tc.wantTemplate {
				t.Errorf("template: got %q, want %q", template, tc.wantTemplate)
			}
			if target != tc.wantTarget {
				t.Errorf("target: got %q, want %q", target, tc.wantTarget)
			}
		})
	}
}

// TestInit_DirOnlyPositional verifies the end-to-end CLI behavior:
// `overlord init <dir>` must scaffold the default template into
// <dir> instead of producing "unknown template". This is the exact
// audit repro.
func TestInit_DirOnlyPositional(t *testing.T) {
	tmp := t.TempDir()
	targetDir := filepath.Join(tmp, "my-project")

	code, _, stderr := runInitCmd(t, targetDir, "--no-run")
	if code != initExitSuccess {
		t.Fatalf("expected exit 0 (dir-as-arg scaffolds default template), got %d\nstderr: %s", code, stderr)
	}
	// The starter overlord.yaml must land in the target dir, not in
	// ./starter (which would indicate the bug is still there).
	if _, err := os.Stat(filepath.Join(targetDir, "overlord.yaml")); err != nil {
		t.Errorf("expected overlord.yaml in target dir %q: %v", targetDir, err)
	}
	if strings.Contains(stderr, "unknown template") {
		t.Errorf("stderr must not claim the dir path is an unknown template; got: %s", stderr)
	}
}
