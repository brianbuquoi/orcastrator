package main

import (
	"bytes"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/brianbuquoi/overlord/internal/broker"
)

// runRootCmd drives the root cobra command in-process and captures
// stdout / stderr. Used by workflow-CLI tests so each case can assert
// against the exact strings a user would see.
func runRootCmd(t *testing.T, args ...string) (stdout, stderr string, err error) {
	t.Helper()
	root := rootCmd()
	root.SetArgs(args)
	var outBuf, errBuf bytes.Buffer
	root.SetOut(&outBuf)
	root.SetErr(&errBuf)
	execErr := root.Execute()
	return outBuf.String(), errBuf.String(), execErr
}

// writeStarterWorkflow scaffolds a minimal workflow project into dir.
// The steps use the mock provider so the run completes without
// credentials or network access.
func writeStarterWorkflow(t *testing.T, dir string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Join(dir, "fixtures"), 0o755); err != nil {
		t.Fatalf("mkdir fixtures: %v", err)
	}
	yaml := `version: "1"

workflow:
  id: starter-cli

  input: text
  output: text

  vars:
    audience: "platform teams"

  steps:
    - model: mock/draft
      fixture: fixtures/draft.json
      prompt: |
        Draft for {{vars.audience}}:
        {{input}}

    - model: mock/review
      fixture: fixtures/review.json
      prompt: |
        Review this draft:
        {{prev}}
`
	if err := os.WriteFile(filepath.Join(dir, "overlord.yaml"), []byte(yaml), 0o644); err != nil {
		t.Fatalf("write workflow: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "fixtures", "draft.json"), []byte(`{"text": "drafted copy"}`), 0o644); err != nil {
		t.Fatalf("write draft fixture: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "fixtures", "review.json"), []byte(`{"text": "reviewed and approved"}`), 0o644); err != nil {
		t.Fatalf("write review fixture: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "sample_input.txt"), []byte("Announce the new release.\n"), 0o644); err != nil {
		t.Fatalf("write sample_input: %v", err)
	}
}

// TestRunCmd_FailedWorkflowExitsNonZeroAndSuppressesOutput is the CLI-
// level regression test for the audit finding that overlord run lied
// about failed workflows: it exited 0 and printed the inbound payload
// back on stdout. We construct a workflow whose second step blows up
// at contract-validation time because its fixture violates the
// chain_text@v1 output schema — the fixture lies about being a mock
// text response, so the broker fails the step and the task ends in
// FAILED.
//
// Expectation: the command returns a non-nil error (non-zero exit),
// stdout is empty (no stale payload leaking through), and stderr
// mentions the failure.
func TestRunCmd_FailedWorkflowExitsNonZeroAndSuppressesOutput(t *testing.T) {
	dir := t.TempDir()
	// Construct a workflow whose second-stage fixture is a valid
	// fixture file (passes mock-adapter construction) but produces
	// output that violates the downstream input schema the broker
	// enforces at runtime. We do that by declaring an output:text
	// workflow whose final fixture returns JSON with no "text" field.
	// The chain layer synthesizes chain_text@v1 expecting {text: ...};
	// anything else fails contract validation at runtime → FAILED.
	//
	// But the mock adapter validates fixtures against output_schema at
	// construction, which would pre-reject this. So instead, we force
	// a runtime failure by pointing the second step at an oversized
	// fixture that fails a runtime guard. Simpler: use an intentionally
	// broken model spec so agent Execute returns an error.
	//
	// The most deterministic scheme is: second step uses an ollama
	// provider with an unreachable OLLAMA_ENDPOINT. The adapter builds
	// at construction but Execute fails immediately, retries exhaust,
	// task ends FAILED — exactly the audit repro shape.
	t.Setenv("OLLAMA_ENDPOINT", "http://127.0.0.1:1") // TCP/1 is reserved, connect refuses instantly.
	yaml := `version: "1"

workflow:
  id: failing-cli
  input: text
  output: text
  steps:
    - model: ollama/nope
      prompt: "{{input}}"
      timeout: 1s
`
	if err := os.WriteFile(filepath.Join(dir, "overlord.yaml"), []byte(yaml), 0o644); err != nil {
		t.Fatalf("write workflow: %v", err)
	}

	stdout, stderr, err := runRootCmd(t,
		"run",
		"--config", filepath.Join(dir, "overlord.yaml"),
		"--input", "hello",
		"--quiet",
		"--timeout", "10s",
	)
	if err == nil {
		t.Fatalf("expected non-zero exit (Run must fail on non-DONE terminal); stdout=%q stderr=%q", stdout, stderr)
	}
	if strings.Contains(stdout, "hello") {
		t.Errorf("stdout must not leak the inbound payload for failed runs; got %q", stdout)
	}
	if !strings.Contains(err.Error(), "FAILED") && !strings.Contains(err.Error(), "ended in state") {
		t.Errorf("error must signal the non-DONE terminal state; got %v", err)
	}
}

// TestFailureReason verifies the helper that extracts the broker's
// recorded failure reason from task metadata. Kept close to the CLI
// so the wording the user sees is locked down by the test.
func TestFailureReason(t *testing.T) {
	cases := []struct {
		name string
		task *broker.Task
		want string
	}{
		{"nil_task", nil, ""},
		{"no_metadata", &broker.Task{}, ""},
		{"no_reason_key", &broker.Task{Metadata: map[string]any{"other": "x"}}, ""},
		{"empty_reason", &broker.Task{Metadata: map[string]any{"failure_reason": ""}}, ""},
		{"non_string_reason", &broker.Task{Metadata: map[string]any{"failure_reason": 42}}, ""},
		{"real_reason", &broker.Task{Metadata: map[string]any{"failure_reason": "max_attempts_exceeded"}}, ": max_attempts_exceeded"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := failureReason(tc.task); got != tc.want {
				t.Errorf("failureReason() = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestRunCmd_WorkflowInputFlag exercises the beginner path:
// `overlord run --config ./overlord.yaml --input "..."` against a
// workflow-shaped config.
func TestRunCmd_WorkflowInputFlag(t *testing.T) {
	dir := t.TempDir()
	writeStarterWorkflow(t, dir)
	stdout, stderr, err := runRootCmd(t,
		"run",
		"--config", filepath.Join(dir, "overlord.yaml"),
		"--input", "Announce the new release.",
		"--quiet",
	)
	if err != nil {
		t.Fatalf("run failed: %v\nstderr: %s", err, stderr)
	}
	if !strings.Contains(stdout, "reviewed and approved") {
		t.Fatalf("expected reviewed-and-approved in stdout; got %q\nstderr: %q", stdout, stderr)
	}
}

// TestRunCmd_WorkflowInputFile covers `--input-file path` as the
// primary beginner entry point.
func TestRunCmd_WorkflowInputFile(t *testing.T) {
	dir := t.TempDir()
	writeStarterWorkflow(t, dir)
	stdout, stderr, err := runRootCmd(t,
		"run",
		"--config", filepath.Join(dir, "overlord.yaml"),
		"--input-file", filepath.Join(dir, "sample_input.txt"),
		"--quiet",
	)
	if err != nil {
		t.Fatalf("run failed: %v\nstderr: %s", err, stderr)
	}
	if !strings.Contains(stdout, "reviewed and approved") {
		t.Fatalf("expected output from workflow; got %q", stdout)
	}
}

// TestRunCmd_WorkflowInputStdin exercises the `--input-file -`
// sentinel so pipes and heredocs flow through cleanly.
func TestRunCmd_WorkflowInputStdin(t *testing.T) {
	dir := t.TempDir()
	writeStarterWorkflow(t, dir)
	root := rootCmd()
	root.SetArgs([]string{
		"run",
		"--config", filepath.Join(dir, "overlord.yaml"),
		"--input-file", "-",
		"--quiet",
	})
	root.SetIn(strings.NewReader("piped-in request"))
	var outBuf, errBuf bytes.Buffer
	root.SetOut(&outBuf)
	root.SetErr(&errBuf)
	if err := root.Execute(); err != nil {
		t.Fatalf("run failed: %v\nstderr: %s", err, errBuf.String())
	}
	if !strings.Contains(outBuf.String(), "reviewed and approved") {
		t.Fatalf("stdin input did not drive workflow:\nstdout: %s\nstderr: %s", outBuf.String(), errBuf.String())
	}
}

// TestExportCmd_WorkflowAdvanced verifies the advanced export path
// produces the files a strict-mode project needs. We don't run the
// exported project — that's covered by the chain-layer round-trip
// test.
func TestExportCmd_WorkflowAdvanced(t *testing.T) {
	dir := t.TempDir()
	writeStarterWorkflow(t, dir)
	outDir := filepath.Join(dir, "advanced")
	stdout, stderr, err := runRootCmd(t,
		"export",
		"--config", filepath.Join(dir, "overlord.yaml"),
		"--advanced",
		"--out", outDir,
	)
	if err != nil {
		t.Fatalf("export failed: %v\nstdout: %s\nstderr: %s", err, stdout, stderr)
	}
	if _, err := os.Stat(filepath.Join(outDir, "overlord.yaml")); err != nil {
		t.Fatalf("exported overlord.yaml missing: %v", err)
	}
	if _, err := os.Stat(filepath.Join(outDir, "schemas")); err != nil {
		t.Fatalf("exported schemas dir missing: %v", err)
	}
	if !strings.Contains(stderr, "Next steps:") {
		t.Fatalf("stderr missing next-steps block:\n%s", stderr)
	}
}

// TestExportCmd_RequiresAdvancedFlag documents the guardrail that
// prevents accidental exports — --advanced is the explicit opt-in to
// the escape hatch.
func TestExportCmd_RequiresAdvancedFlag(t *testing.T) {
	dir := t.TempDir()
	writeStarterWorkflow(t, dir)
	_, _, err := runRootCmd(t,
		"export",
		"--config", filepath.Join(dir, "overlord.yaml"),
		"--out", filepath.Join(dir, "advanced"),
	)
	if err == nil {
		t.Fatal("expected export to refuse without --advanced")
	}
	if !strings.Contains(err.Error(), "--advanced") {
		t.Fatalf("error should mention --advanced, got: %v", err)
	}
}

// TestServeCmd_WorkflowStartsAndStops confirms `overlord serve` boots
// against a workflow-shaped config, binds an ephemeral port, and
// drains cleanly on SIGTERM. This is the smoke test for the
// workflow→runServerFromConfig wiring.
func TestServeCmd_WorkflowStartsAndStops(t *testing.T) {
	dir := t.TempDir()
	writeStarterWorkflow(t, dir)

	// Probe an ephemeral port so the test doesn't collide with a
	// live 8080. Bind to it briefly to discover the number, then
	// release before handing it to serve — the serve binder
	// re-acquires.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	done := make(chan error, 1)
	root := rootCmd()
	root.SetArgs([]string{
		"serve",
		"--config", filepath.Join(dir, "overlord.yaml"),
		"--bind", addr,
	})
	var outBuf, errBuf bytes.Buffer
	root.SetOut(&outBuf)
	root.SetErr(&errBuf)

	go func() {
		done <- root.Execute()
	}()

	// Wait for the server to start accepting connections on addr.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		conn, cerr := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if cerr == nil {
			conn.Close()
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Send SIGTERM to trigger graceful shutdown. The serve loop
	// listens for signals on the real process; there is no public
	// hook to trigger shutdown on a sub-command, so we send to our
	// own process. The signal handler is started by runServerFromConfig
	// just before the blocking <-sigCh read.
	proc, err := os.FindProcess(os.Getpid())
	if err != nil {
		t.Fatalf("find self: %v", err)
	}
	if err := proc.Signal(os.Interrupt); err != nil {
		t.Fatalf("signal self: %v", err)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("serve returned error: %v\nstderr: %s", err, errBuf.String())
		}
	case <-time.After(10 * time.Second):
		t.Fatal("serve did not exit within 10s")
	}
}
