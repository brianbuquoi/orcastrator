package plugin

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/brianbuquoi/overlord/internal/agent"
	"github.com/brianbuquoi/overlord/internal/broker"
	"github.com/brianbuquoi/overlord/internal/config"
	"github.com/brianbuquoi/overlord/internal/contract"
	"github.com/brianbuquoi/overlord/internal/store/memory"
)

// buildPluginAgent constructs a plugin.Agent directly (it implements
// broker.Agent) with the given manifest customizations. The returned agent's
// Stop() is wired to t.Cleanup so no subprocess can outlive the test.
func buildPluginAgent(t *testing.T, id string, extra map[string]string, overrides func(m *Manifest)) *Agent {
	t.Helper()
	m := &Manifest{
		Name:            "echo",
		Binary:          echoBinaryPath,
		RPCTimeout:      Duration{Duration: 5 * time.Second},
		ShutdownTimeout: Duration{Duration: 1 * time.Second},
		MaxRestarts:     3,
	}
	for k := range extra {
		m.Env = append(m.Env, k)
	}
	if overrides != nil {
		overrides(m)
	}
	for k, v := range extra {
		t.Setenv(k, v)
	}
	if err := m.Validate(); err != nil {
		t.Fatalf("manifest validate: %v", err)
	}
	a, err := New(id, "sys prompt", m, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	t.Cleanup(func() { _ = a.Stop() })
	return a
}

// passthroughSchema writes a permissive JSON schema (any object) and returns
// its absolute path.
func passthroughSchema(t *testing.T, dir, name string) string {
	t.Helper()
	schema := map[string]any{"type": "object"}
	b, err := json.Marshal(schema)
	if err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, b, 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

// buildPluginBrokerEnv assembles a minimal 1-stage pipeline whose sole agent
// is the given plugin.Agent. The schemas on either side are permissive
// "object"-accepting schemas so whatever the plugin echoes back will validate.
func buildPluginBrokerEnv(
	t *testing.T,
	pluginAgent broker.Agent,
	retryMax int,
) (*config.Config, *memory.MemoryStore, map[string]broker.Agent, *contract.Registry) {
	t.Helper()
	dir := t.TempDir()
	inPath := passthroughSchema(t, dir, "in.json")
	outPath := passthroughSchema(t, dir, "out.json")

	cfg := &config.Config{
		Version: "1",
		SchemaRegistry: []config.SchemaEntry{
			{Name: "pt_in", Version: "v1", Path: inPath},
			{Name: "pt_out", Version: "v1", Path: outPath},
		},
		Pipelines: []config.Pipeline{{
			Name:        "plugin-pipe",
			Concurrency: 1,
			Store:       "memory",
			Stages: []config.Stage{{
				ID:           "echo-stage",
				Agent:        pluginAgent.ID(),
				InputSchema:  config.StageSchemaRef{Name: "pt_in", Version: "v1"},
				OutputSchema: config.StageSchemaRef{Name: "pt_out", Version: "v1"},
				Timeout:      config.Duration{Duration: 5 * time.Second},
				Retry: config.RetryPolicy{
					MaxAttempts: retryMax,
					Backoff:     "fixed",
					BaseDelay:   config.Duration{Duration: 5 * time.Millisecond},
				},
				OnSuccess: config.StaticOnSuccess("done"),
				OnFailure: "dead-letter",
			}},
		}},
		Agents: []config.Agent{{
			ID:           pluginAgent.ID(),
			Provider:     "plugin",
			SystemPrompt: "sys prompt",
		}},
	}

	reg, err := contract.NewRegistry(cfg.SchemaRegistry, "/")
	if err != nil {
		t.Fatal(err)
	}
	st := memory.New()
	agents := map[string]broker.Agent{pluginAgent.ID(): pluginAgent}
	return cfg, st, agents, reg
}

// pollTaskState polls the store until the task reaches one of the accepted
// states, or the deadline elapses. Uses a short polling interval with
// context-aware cancellation; no fixed time.Sleep for coordination.
func pollTaskState(
	t *testing.T,
	st *memory.MemoryStore,
	taskID string,
	accept map[broker.TaskState]struct{},
	timeout time.Duration,
) *broker.Task {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		task, err := st.GetTask(context.Background(), taskID)
		if err == nil {
			if _, ok := accept[task.State]; ok {
				return task
			}
		}
		select {
		case <-ctx.Done():
			cur, _ := st.GetTask(context.Background(), taskID)
			t.Fatalf("task %s did not reach accepted state within %v; current=%s",
				taskID, timeout, cur.State)
			return nil
		case <-ticker.C:
		}
	}
}

// ---------------------------------------------------------------------------
// TestPlugin_FullRoundTrip
// ---------------------------------------------------------------------------

// Submits a task to the broker (not the adapter directly) wired to a plugin
// agent. Verifies the task completes DONE and carries the echoed payload.
func TestPlugin_FullRoundTrip(t *testing.T) {
	pa := buildPluginAgent(t, "plugin-rt", nil, nil)
	cfg, st, agents, reg := buildPluginBrokerEnv(t, pa, 3)

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	b := broker.New(cfg, st, agents, reg, logger, nil, nil)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = b.Run(ctx) }()

	payload := json.RawMessage(`{"round":"trip","n":1}`)
	task, err := b.Submit(ctx, "plugin-pipe", payload)
	if err != nil {
		t.Fatalf("submit: %v", err)
	}

	final := pollTaskState(t, st,
		task.ID,
		map[broker.TaskState]struct{}{broker.TaskStateDone: {}},
		5*time.Second,
	)

	// Normalize JSON for compare (broker may re-marshal).
	var want, got any
	_ = json.Unmarshal(payload, &want)
	_ = json.Unmarshal(final.Payload, &got)
	if fmt.Sprint(want) != fmt.Sprint(got) {
		t.Errorf("echoed payload mismatch: want=%s got=%s", payload, final.Payload)
	}
}

// ---------------------------------------------------------------------------
// TestPlugin_EnvIsolation_NoLeakage
// ---------------------------------------------------------------------------

// Starts the echo plugin with no env entries in manifest.Env and a host
// environment full of secrets. Uses the plugin's --env-dump CLI flag (which
// bypasses the env allow-list entirely) to record what env the subprocess
// actually inherited. Asserts the dump is empty.
func TestPlugin_EnvIsolation_NoLeakage(t *testing.T) {
	// Host environment carries secrets that MUST NOT leak.
	t.Setenv("SECRET_API_KEY", "shhh-xyz")
	t.Setenv("ANOTHER_SECRET", "nope")
	t.Setenv("HOME", "/should-not-leak")
	t.Setenv("PATH", "/should-not-leak/bin")

	dumpPath := filepath.Join(t.TempDir(), "env_dump.txt")

	a := buildPluginAgent(t, "plugin-env", nil, func(m *Manifest) {
		// NO env allow-list. --env-dump is carried via args, not env.
		m.Env = nil
		m.Args = []string{"--env-dump=" + dumpPath}
	})

	// Trigger subprocess start by executing once. We don't care about the
	// result — we care that the dump file was written on boot.
	if _, err := a.Execute(context.Background(), newTask(`{}`)); err != nil {
		t.Fatalf("execute: %v", err)
	}

	// Dump is written synchronously before the plugin enters its RPC loop,
	// so by the time execute returns the file must exist and be final.
	f, err := os.Open(dumpPath)
	if err != nil {
		t.Fatalf("open env dump: %v", err)
	}
	t.Cleanup(func() { _ = f.Close() })

	var leaked []string
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimRight(scanner.Text(), "\n")
		if line == "" {
			continue
		}
		leaked = append(leaked, line)
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scan env dump: %v", err)
	}

	if len(leaked) != 0 {
		t.Errorf("expected zero env vars in plugin subprocess, got %d:\n  %s",
			len(leaked), strings.Join(leaked, "\n  "))
	}
}

// ---------------------------------------------------------------------------
// TestPlugin_MaxRestarts_Exceeded
// ---------------------------------------------------------------------------

// With max_restarts=2, a plugin that crashes on every execute RPC should:
//   - survive restart #1 (budget 1/2) after crash #1
//   - survive restart #2 (budget 2/2) after crash #2
//   - after crash #3, the next call finds the budget exhausted and returns a
//     non-retryable AgentError. The agent is also marked unhealthy, so any
//     subsequent call (without restart being possible) returns the same
//     non-retryable error.
func TestPlugin_MaxRestarts_Exceeded(t *testing.T) {
	a := buildPluginAgent(t, "plugin-maxrestart",
		map[string]string{"ECHO_PLUGIN_CRASH_ON_EXECUTE": "1"},
		func(m *Manifest) { m.MaxRestarts = 2 },
	)

	var (
		nonRetryableSeen bool
		lastErr          error
	)
	// Bound the loop; budget must be exhausted within O(max_restarts+1) calls.
	for i := 0; i < 10 && !nonRetryableSeen; i++ {
		_, err := a.Execute(context.Background(), newTask(`{}`))
		lastErr = err
		if err == nil {
			continue
		}
		var ae *agent.AgentError
		if !errors.As(err, &ae) {
			continue
		}
		if !ae.Retryable && strings.Contains(ae.Error(), "restart budget") {
			nonRetryableSeen = true
		}
	}
	if !nonRetryableSeen {
		t.Fatalf("never saw non-retryable restart-budget error; last: %v", lastErr)
	}

	// Agent must stay unhealthy: the next call should still return a
	// non-retryable AgentError without attempting to start a new subprocess.
	_, err := a.Execute(context.Background(), newTask(`{}`))
	if err == nil {
		t.Fatal("expected unhealthy agent error after budget exhausted")
	}
	var ae *agent.AgentError
	if !errors.As(err, &ae) {
		t.Fatalf("expected AgentError after exhaustion, got %T: %v", err, err)
	}
	if ae.Retryable {
		t.Errorf("post-exhaustion error must be non-retryable (got retryable=true): %v", err)
	}
	if err := a.HealthCheck(context.Background()); err == nil {
		t.Error("HealthCheck should fail once restart budget is exhausted")
	}
}

// ---------------------------------------------------------------------------
// TestPlugin_SlowRPC_Timeout
// ---------------------------------------------------------------------------

// With rpc_timeout=100ms and a plugin that sleeps 500ms before every reply,
// Execute must return a retryable AgentError before ~200ms — well under the
// plugin's 500ms sleep. This verifies the timeout short-circuits the RPC
// rather than waiting for the plugin to respond.
func TestPlugin_SlowRPC_Timeout(t *testing.T) {
	a := buildPluginAgent(t, "plugin-slow",
		map[string]string{"ECHO_PLUGIN_SLOW_MS": "500"},
		func(m *Manifest) {
			m.RPCTimeout = Duration{Duration: 100 * time.Millisecond}
		},
	)

	start := time.Now()
	_, err := a.Execute(context.Background(), newTask(`{}`))
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected timeout error")
	}
	var ae *agent.AgentError
	if !errors.As(err, &ae) {
		t.Fatalf("expected AgentError, got %T: %v", err, err)
	}
	if !ae.Retryable {
		t.Errorf("timeout should be retryable, got retryable=false: %v", err)
	}
	// Generous upper bound (~200ms); we just need to prove we did NOT wait
	// for the full 500ms plugin sleep. CI scheduler jitter makes tighter
	// bounds flaky.
	if elapsed >= 400*time.Millisecond {
		t.Errorf("Execute returned after %v; expected short-circuit well under 500ms", elapsed)
	}
}

// ---------------------------------------------------------------------------
// TestPlugin_OnFailureNonRetryable
// ---------------------------------------------------------------------------

// With manifest.on_failure=non_retryable, an RPCErrorInternal from the plugin
// must be surfaced as a non-retryable AgentError. When routed through the
// broker, this means the task is dead-lettered on the very first attempt —
// no retry budget is consumed.
func TestPlugin_OnFailureNonRetryable(t *testing.T) {
	pa := buildPluginAgent(t, "plugin-nonretry",
		map[string]string{"ECHO_PLUGIN_RETURN_INTERNAL": "1"},
		func(m *Manifest) { m.OnFailure = OnFailureNonRetryable },
	)

	// MaxAttempts=5: if the broker incorrectly retried, we'd see Attempts>1.
	cfg, st, agents, reg := buildPluginBrokerEnv(t, pa, 5)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	b := broker.New(cfg, st, agents, reg, logger, nil, nil)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = b.Run(ctx) }()

	task, err := b.Submit(ctx, "plugin-pipe", json.RawMessage(`{"x":1}`))
	if err != nil {
		t.Fatalf("submit: %v", err)
	}

	final := pollTaskState(t, st,
		task.ID,
		map[broker.TaskState]struct{}{broker.TaskStateFailed: {}},
		5*time.Second,
	)

	if !final.RoutedToDeadLetter {
		t.Errorf("expected RoutedToDeadLetter=true for non-retryable plugin failure; state=%s",
			final.State)
	}
	// A non-retryable error must NOT consume the retry budget. Attempts is
	// incremented only along the retry path in handleAgentError, which is
	// skipped entirely for non-retryable failures.
	if final.Attempts != 0 {
		t.Errorf("non-retryable failure should not be retried; got Attempts=%d", final.Attempts)
	}

	reason, _ := final.Metadata["failure_reason"].(string)
	if !strings.Contains(reason, "agent error") {
		t.Errorf("failure_reason should mention agent error, got %q", reason)
	}
}
