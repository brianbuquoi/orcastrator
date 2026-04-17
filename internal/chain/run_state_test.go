package chain

import (
	"encoding/json"
	"testing"

	"github.com/brianbuquoi/overlord/internal/broker"
)

// TestExtractFinalOutput_NonDoneReturnsEmpty codifies the state-aware
// behavior that stops `overlord run` from lying to its caller when a
// terminal task is not DONE. The audit reproduced overlord run exiting
// 0 and printing the input back for FAILED tasks because the previous
// implementation unwrapped the payload regardless of state; this test
// locks in the fix.
func TestExtractFinalOutput_NonDoneReturnsEmpty(t *testing.T) {
	payload, err := json.Marshal(map[string]string{"text": "drafted"})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	textChain := &Chain{Output: &Output{Type: "text"}}
	jsonChain := &Chain{Output: &Output{Type: "json"}}

	cases := []struct {
		name    string
		state   broker.TaskState
		chain   *Chain
		wantOut string
	}{
		{"done_text_unwraps", broker.TaskStateDone, textChain, "drafted"},
		{"done_json_passthrough", broker.TaskStateDone, jsonChain, string(payload)},
		{"failed_suppressed_text", broker.TaskStateFailed, textChain, ""},
		{"failed_suppressed_json", broker.TaskStateFailed, jsonChain, ""},
		{"discarded_suppressed", broker.TaskStateDiscarded, textChain, ""},
		{"replayed_suppressed", broker.TaskStateReplayed, textChain, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			task := &broker.Task{State: tc.state, Payload: payload}
			got := extractFinalOutput(tc.chain, task)
			if got != tc.wantOut {
				t.Errorf("extractFinalOutput(%s) = %q, want %q", tc.state, got, tc.wantOut)
			}
		})
	}
}
