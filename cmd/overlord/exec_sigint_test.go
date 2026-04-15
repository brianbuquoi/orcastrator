package main

import (
	"bytes"
	"errors"
	"net/http"
	"os"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// syncBuffer is a concurrency-safe bytes.Buffer wrapper so the in-process
// exec command can write to the same stderr buffer the test goroutine is
// reading for synchronization and assertions.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// TestCLI_Exec_SIGINT verifies that sending SIGINT while `overlord exec` is
// waiting for a slow agent causes:
//   - exit code execExitTimeout (2, shared with timeout)
//   - stderr contains "Interrupted", the task UUID, and the replay hint
//
// The broker's in-memory store is scoped inside runExec and is not
// observable after exec returns, so the "task still in non-terminal state"
// contract is asserted indirectly: the replay hint printed to stderr
// instructs the operator to replay that task ID later, which is only
// meaningful if the task is recoverable (non-terminal or dead-lettered).
func TestCLI_Exec_SIGINT(t *testing.T) {
	// The mock agent signals once it has received the request (task is
	// EXECUTING), then blocks until the test releases it. This gives a
	// deterministic synchronization point without time.Sleep.
	received := make(chan struct{}, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseFn := func() { releaseOnce.Do(func() { close(release) }) }

	srv := ollamaServer(t, func(w http.ResponseWriter, r *http.Request) {
		select {
		case received <- struct{}{}:
		default:
		}
		select {
		case <-release:
		case <-r.Context().Done():
		}
		w.Write([]byte(ollamaOKResponse(`{"response":"late"}`)))
	})
	// Register AFTER ollamaServer so releaseFn runs BEFORE srv.Close on
	// cleanup (LIFO); otherwise srv.Close would block waiting for the
	// handler goroutine which itself is blocked on <-release.
	t.Cleanup(releaseFn)

	cfg := writeExecTestYAML(t, srv.URL)

	var stdout bytes.Buffer
	stderr := &syncBuffer{}

	root := rootCmd()
	root.SetArgs([]string{"exec",
		"--config", cfg,
		"--id", "test-pipeline",
		"--payload", `{"request":"hi"}`,
		"--timeout", "30s",
	})
	root.SetOut(&stdout)
	root.SetErr(stderr)

	done := make(chan error, 1)
	go func() { done <- root.Execute() }()

	// Wait until the mock has been hit — the task is now mid-flight.
	select {
	case <-received:
	case <-time.After(10 * time.Second):
		t.Fatal("mock agent never received a request within 10s")
	}

	// Deliver SIGINT to this test process. exec.go registers signal.Notify
	// on SIGINT, so Go routes it to the exec handler rather than killing
	// the test binary.
	if err := syscall.Kill(os.Getpid(), syscall.SIGINT); err != nil {
		t.Fatalf("kill(SIGINT): %v", err)
	}

	var execErr error
	select {
	case execErr = <-done:
	case <-time.After(15 * time.Second):
		// Release the mock so the broker can finish draining on the
		// shutdown path, then wait a little longer.
		releaseFn()
		select {
		case execErr = <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("exec did not return after SIGINT")
		}
	}

	var ee *execExitError
	if !errors.As(execErr, &ee) {
		t.Fatalf("expected *execExitError, got %T: %v", execErr, execErr)
	}
	if ee.code != execExitTimeout {
		t.Fatalf("expected exit code %d on SIGINT, got %d", execExitTimeout, ee.code)
	}
	if ee.msg != "interrupted" {
		t.Errorf("expected msg \"interrupted\", got %q", ee.msg)
	}

	errOut := stderr.String()
	for _, want := range []string{"Interrupted", "Replay later", "overlord dead-letter replay"} {
		if !strings.Contains(errOut, want) {
			t.Errorf("stderr missing %q; got:\n%s", want, errOut)
		}
	}

	// A UUID-shaped task ID must appear on stderr so the operator can
	// copy/paste it into the replay command emitted in the hint above.
	idRE := regexp.MustCompile(`[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}`)
	if !idRE.MatchString(errOut) {
		t.Errorf("stderr should contain task UUID for operator replay; got:\n%s", errOut)
	}
}
