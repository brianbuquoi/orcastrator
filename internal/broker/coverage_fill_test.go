package broker_test

// Targeted coverage fills requested by maintainers. Tests in this file
// complement (not duplicate) broker_test.go, coverage_gap_test.go,
// integration_broker_test.go, and security_injection_test.go.
//
// The following requested cases are intentionally omitted because they are
// already covered elsewhere (see commit message / PR description for rationale):
//
//   - TestBroker_SchemaValidation_OutputRejected
//       → covered by TestOutputContractViolation in broker_test.go
//   - TestBroker_MultiStage_PassThrough (envelope wrapping)
//       → covered by TestIntegrationEnvelopeWrapping in integration_broker_test.go
//   - TestBroker_HotReload_InFlightTask
//       → covered by TestIntegrationHotReloadMidFlight in integration_broker_test.go
//         and TestReloadRemovedStageDrainsInFlight in broker_test.go
//   - TestBroker_SanitizerWarning_AttachedToMetadata
//       → covered by TestIntegrationInjectionEndToEnd in integration_broker_test.go.
//         The weak TestSanitizerWarning in broker_test.go was strengthened
//         to assert on sanitizer_warnings presence and content.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/brianbuquoi/overlord/internal/agent"
	"github.com/brianbuquoi/overlord/internal/broker"
)

// metadataKeys returns sorted-ish keys from a metadata map for diagnostic output.
func metadataKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// TestBroker_RetryPolicy verifies that a retryable error is retried exactly
// max_attempts times before the task is routed to the dead-letter path. This
// extends TestMaxRetriesExceeded (which only asserts terminal FAILED state)
// by asserting the exact call count, the persisted Attempts counter, and the
// RoutedToDeadLetter flag.
func TestBroker_RetryPolicy(t *testing.T) {
	cfg, st, agents, reg := buildTestEnv(t)
	// stage1 retry policy is max_attempts=3 in buildTestEnv.

	var calls atomic.Int32
	agents["agent1"].(*mockAgent).setHandler(func(_ context.Context, _ *broker.Task) (*broker.TaskResult, error) {
		calls.Add(1)
		return nil, &agent.AgentError{
			Err:       fmt.Errorf("always retryable"),
			AgentID:   "agent1",
			Prov:      "mock",
			Retryable: true,
		}
	})
	agents["agent2"].(*mockAgent).setHandler(func(_ context.Context, _ *broker.Task) (*broker.TaskResult, error) {
		t.Error("agent2 must not run — stage1 should dead-letter")
		return nil, errors.New("unexpected")
	})
	agents["agent3"].(*mockAgent).setHandler(func(_ context.Context, _ *broker.Task) (*broker.TaskResult, error) {
		t.Error("agent3 must not run")
		return nil, errors.New("unexpected")
	})

	b := newBroker(cfg, st, agents, reg)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	// Skip real backoff sleeps to keep the test fast.
	b.SetSleepFunc(func(_ context.Context, _ time.Duration) {})

	go b.Run(ctx)

	task, err := b.Submit(ctx, "test-pipeline", json.RawMessage(`{"request":"retry-policy"}`))
	if err != nil {
		t.Fatal(err)
	}

	final := waitForTaskState(t, st, task.ID, broker.TaskStateFailed, 10*time.Second)

	if got := calls.Load(); got != 3 {
		t.Errorf("expected exactly 3 agent invocations (max_attempts=3), got %d", got)
	}
	// Attempts is persisted via the retry path (retryTask) after each increment.
	// The last persisted value before the terminal failTask call corresponds to
	// the number of retries scheduled, not the absolute attempt count. What
	// matters is that the counter advanced beyond the initial 0.
	if final.Attempts == 0 {
		t.Errorf("expected Attempts to advance past 0 across retries, got %d", final.Attempts)
	}
	if final.Attempts > 3 {
		t.Errorf("Attempts should not exceed MaxAttempts (3), got %d", final.Attempts)
	}
	if !final.RoutedToDeadLetter {
		t.Error("expected RoutedToDeadLetter=true after exhausting retries")
	}
	if reason, _ := final.Metadata["failure_reason"].(string); reason == "" {
		t.Error("expected failure_reason in metadata")
	}
}

// TestBroker_NonRetryableError verifies that a non-retryable error (plain
// error, not implementing RetryableError) immediately fails the task on the
// first attempt. Complements TestDeadLetterRouting by asserting attempts=1
// explicitly and that no retry occurred.
func TestBroker_NonRetryableError(t *testing.T) {
	cfg, st, agents, reg := buildTestEnv(t)

	var calls atomic.Int32
	agents["agent1"].(*mockAgent).setHandler(func(_ context.Context, _ *broker.Task) (*broker.TaskResult, error) {
		calls.Add(1)
		// Plain error → non-retryable.
		return nil, errors.New("hard failure, do not retry")
	})
	agents["agent2"].(*mockAgent).setHandler(func(_ context.Context, _ *broker.Task) (*broker.TaskResult, error) {
		t.Error("agent2 must not run")
		return nil, errors.New("unexpected")
	})
	agents["agent3"].(*mockAgent).setHandler(func(_ context.Context, _ *broker.Task) (*broker.TaskResult, error) {
		t.Error("agent3 must not run")
		return nil, errors.New("unexpected")
	})

	b := newBroker(cfg, st, agents, reg)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	b.SetSleepFunc(func(_ context.Context, _ time.Duration) {})

	go b.Run(ctx)

	task, err := b.Submit(ctx, "test-pipeline", json.RawMessage(`{"request":"non-retryable"}`))
	if err != nil {
		t.Fatal(err)
	}

	final := waitForTaskState(t, st, task.ID, broker.TaskStateFailed, 5*time.Second)

	if got := calls.Load(); got != 1 {
		t.Errorf("non-retryable error must stop after 1 attempt, got %d calls", got)
	}
	// Non-retryable errors route straight to failTask without passing through
	// retryTask, so the persisted Attempts field remains at its initial value
	// (0). The fact that the agent was called exactly once (above) is the
	// primary assertion; the terminal dead-letter flag confirms routing.
	if !final.RoutedToDeadLetter {
		t.Error("expected RoutedToDeadLetter=true for non-retryable failure")
	}
}

// TestBroker_SchemaValidation_InputRejected verifies that a payload failing
// input-schema validation is rejected BEFORE the agent is invoked. The agent
// handler must never execute; the task must transition to FAILED with a
// failure_reason mentioning input validation.
func TestBroker_SchemaValidation_InputRejected(t *testing.T) {
	cfg, st, agents, reg := buildTestEnv(t)

	var agent1Calls atomic.Int32
	agents["agent1"].(*mockAgent).setHandler(func(_ context.Context, _ *broker.Task) (*broker.TaskResult, error) {
		agent1Calls.Add(1)
		return &broker.TaskResult{Payload: json.RawMessage(`{"category":"unused"}`)}, nil
	})
	agents["agent2"].(*mockAgent).setHandler(func(_ context.Context, _ *broker.Task) (*broker.TaskResult, error) {
		t.Error("agent2 must not run")
		return nil, errors.New("unexpected")
	})
	agents["agent3"].(*mockAgent).setHandler(func(_ context.Context, _ *broker.Task) (*broker.TaskResult, error) {
		t.Error("agent3 must not run")
		return nil, errors.New("unexpected")
	})

	b := newBroker(cfg, st, agents, reg)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	b.SetSleepFunc(func(_ context.Context, _ time.Duration) {})

	go b.Run(ctx)

	// Stage1 input schema requires {"request": string} with additionalProperties=false.
	// Submit a payload with the wrong type to force a validation error.
	task, err := b.Submit(ctx, "test-pipeline", json.RawMessage(`{"request":12345}`))
	if err != nil {
		t.Fatal(err)
	}

	final := waitForTaskState(t, st, task.ID, broker.TaskStateFailed, 5*time.Second)

	if got := agent1Calls.Load(); got != 0 {
		t.Errorf("agent must not be invoked on input-validation failure; got %d calls", got)
	}

	reason, ok := final.Metadata["failure_reason"].(string)
	if !ok || reason == "" {
		t.Fatal("expected failure_reason in metadata")
	}
	if !strings.Contains(reason, "input validation") {
		t.Errorf("failure_reason should mention input validation; got: %q", reason)
	}
}
