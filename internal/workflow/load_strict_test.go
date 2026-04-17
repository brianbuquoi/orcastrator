package workflow

import (
	"strings"
	"testing"
)

// TestParse_RejectsStrayTopLevelKeys is the regression test for the
// "workflow file silently accepts strict-mode keys" footgun. A file
// that declares `workflow:` plus any strict-mode top-level key
// (pipelines / auth / schema_registry / agents / stores / etc.) must
// hard-error at load time, with a message naming the offending key.
//
// The most operationally dangerous instance is a stray top-level
// `auth:` block: before this check, a workflow would compile, serve
// without auth, and look like it had auth configured until someone
// read the runtime logs.
func TestParse_RejectsStrayTopLevelKeys(t *testing.T) {
	base := `version: "1"
workflow:
  id: sample
  steps:
    - model: mock/x
      fixture: fx.json
      prompt: "hi"
`
	cases := []struct {
		name       string
		extraBlock string
		// expectKey is the exact key name the error message must
		// identify. Empty means "any unknown-key error is fine".
		expectKey string
	}{
		{
			name: "top-level auth",
			extraBlock: `auth:
  enabled: true
  keys:
    - name: op
      key_env: X
      scopes: [admin]
`,
			expectKey: `"auth"`,
		},
		{
			name: "top-level pipelines",
			extraBlock: `pipelines:
  - name: foo
`,
			expectKey: `"pipelines"`,
		},
		{
			name: "top-level schema_registry",
			extraBlock: `schema_registry:
  - name: x
    version: v1
    path: x.json
`,
			expectKey: `"schema_registry"`,
		},
		{
			name: "top-level stores",
			extraBlock: `stores:
  memory:
    max_tasks: 100
`,
			expectKey: `"stores"`,
		},
		{
			name: "top-level dashboard",
			extraBlock: `dashboard:
  enabled: false
`,
			expectKey: `"dashboard"`,
		},
		{
			name: "top-level agents",
			extraBlock: `agents:
  - id: foo
    provider: mock
`,
			expectKey: `"agents"`,
		},
		{
			name:       "unknown top-level key",
			extraBlock: `not_a_real_key: true` + "\n",
			expectKey:  `"not_a_real_key"`,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			_, err := Parse([]byte(base+tc.extraBlock), "sample.yaml")
			if err == nil {
				t.Fatal("expected Parse to reject stray top-level key, got nil error")
			}
			msg := err.Error()
			if !strings.Contains(msg, "unknown top-level key") {
				t.Fatalf("error message missing 'unknown top-level key' marker: %v", err)
			}
			if tc.expectKey != "" && !strings.Contains(msg, tc.expectKey) {
				t.Errorf("error should name the offending key %s; got: %v", tc.expectKey, err)
			}
			// The error must also point operators at the escape
			// hatch so the fix is obvious.
			if !strings.Contains(msg, "export --advanced") && !strings.Contains(msg, "remove the `workflow:` block") {
				t.Errorf("error should hint at remediation (strict export / remove workflow block); got: %v", err)
			}
		})
	}
}

// TestParse_AcceptsAllowedTopLevelKeys confirms the closed set of
// accepted keys still loads cleanly — we didn't regress the happy
// path while adding the strict check.
func TestParse_AcceptsAllowedTopLevelKeys(t *testing.T) {
	src := `version: "1"
workflow:
  id: sample
  steps:
    - model: mock/x
      fixture: fx.json
      prompt: "hi"
runtime:
  bind: 127.0.0.1:8080
  dashboard: true
`
	if _, err := Parse([]byte(src), "sample.yaml"); err != nil {
		t.Fatalf("Parse rejected an allowed top-level key set: %v", err)
	}
}

// TestParse_IgnoresComments keeps the top-level scan from misreading
// commented-out strict keys as live ones. The starter template ships
// with a commented `runtime:` block and sample configs frequently
// carry comments like `# auth: enabled: true`; neither should trip
// the check.
func TestParse_IgnoresComments(t *testing.T) {
	src := `version: "1"
# this file also has a commented auth: hint below
workflow:
  id: sample
  steps:
    - model: mock/x
      fixture: fx.json
      prompt: "hi"
# auth:
#   enabled: true
`
	if _, err := Parse([]byte(src), "sample.yaml"); err != nil {
		t.Fatalf("Parse rejected a file with commented strict keys: %v", err)
	}
}

// TestCompile_InvalidRuntimeAuthRejectedAtCompile covers Fix 5: the
// strict validator must run during workflow.Compile so malformed
// runtime.auth shapes (empty keys when enabled, duplicate key names,
// unknown scope strings) fail at Compile time rather than at
// broker-start time after workers are already running.
func TestCompile_InvalidRuntimeAuthRejectedAtCompile(t *testing.T) {
	cases := []struct {
		name       string
		runtimeYaml string
		expectSub  string // substring the error message must contain
	}{
		{
			name: "auth enabled with empty keys",
			runtimeYaml: `runtime:
  auth:
    enabled: true
    keys: []
`,
			expectSub: "at least one key",
		},
		{
			name: "duplicate key names",
			runtimeYaml: `runtime:
  auth:
    enabled: true
    keys:
      - name: dup
        key_env: OVERLORD_A
        scopes: [admin]
      - name: dup
        key_env: OVERLORD_B
        scopes: [read]
`,
			expectSub: "duplicate",
		},
		{
			name: "invalid scope string",
			runtimeYaml: `runtime:
  auth:
    enabled: true
    keys:
      - name: op
        key_env: OVERLORD_A
        scopes: [superuser]
`,
			expectSub: "invalid scope",
		},
	}

	base := `version: "1"
workflow:
  id: sample
  steps:
    - model: mock/x
      fixture: fx.json
      prompt: "hi"
`

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			file, err := Parse([]byte(base+tc.runtimeYaml), "sample.yaml")
			if err != nil {
				t.Fatalf("Parse failed before Compile could run: %v", err)
			}
			_, err = Compile(file, t.TempDir())
			if err == nil {
				t.Fatal("expected Compile to reject invalid runtime.auth config; got nil error")
			}
			if !strings.Contains(err.Error(), tc.expectSub) {
				t.Errorf("error message should mention %q; got: %v", tc.expectSub, err)
			}
		})
	}
}

// TestCompile_ValidRuntimeAuthAccepted confirms the fail-closed
// validator doesn't break the happy path: a workflow with a
// well-formed runtime.auth block still compiles.
func TestCompile_ValidRuntimeAuthAccepted(t *testing.T) {
	src := `version: "1"
workflow:
  id: sample
  steps:
    - model: mock/x
      fixture: fx.json
      prompt: "hi"
runtime:
  auth:
    enabled: true
    keys:
      - name: op
        key_env: OVERLORD_OP
        scopes: [admin]
`
	file, err := Parse([]byte(src), "sample.yaml")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if _, err := Compile(file, t.TempDir()); err != nil {
		t.Fatalf("Compile rejected valid runtime.auth: %v", err)
	}
}
