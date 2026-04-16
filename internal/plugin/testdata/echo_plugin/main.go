// echo_plugin is a minimal subprocess plugin used by plugin/agent_test.go.
// It speaks newline-delimited JSON-RPC 2.0 on stdin/stdout:
//
//   - method "execute"      → returns {"output": <payload>} (echoes the input)
//   - method "health_check" → returns {"healthy": true}
//
// Any other method produces a method-not-found error. Parse errors are
// logged to stderr and the process exits with a non-zero code so tests can
// exercise the restart path.
//
// Environment controls for tests:
//
//   ECHO_PLUGIN_CRASH_ON_EXECUTE=1  → exit(2) as soon as an execute RPC arrives
//   ECHO_PLUGIN_ECHO_EXIT_AFTER=N   → after N executes, return then exit
//   ECHO_PLUGIN_RETURN_INVALID=1    → return invalid_params (-32602) from execute
//   ECHO_PLUGIN_RETURN_INTERNAL=1   → return internal_error  (-32603) from execute
//   ECHO_PLUGIN_SLOW_MS=N           → sleep N ms before every reply
//   ECHO_PLUGIN_UNHEALTHY=1         → return healthy: false from health_check
//
// Command-line args (carried via manifest.args, NOT env — so they bypass the
// manifest env allow-list and can be used to observe what env the plugin
// actually receives at startup):
//
//   --env-dump=<path>   → write one environ entry per line to <path> on
//                         startup, then proceed normally. Used by tests to
//                         verify env isolation without relying on the env
//                         allow-list itself.
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type rpcRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      int64           `json:"id"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type rpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      int64           `json:"id"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *rpcError       `json:"error,omitempty"`
}

type executeParams struct {
	TaskID       string          `json:"task_id"`
	PipelineID   string          `json:"pipeline_id"`
	StageID      string          `json:"stage_id"`
	Payload      json.RawMessage `json:"payload"`
	SystemPrompt string          `json:"system_prompt,omitempty"`
}

type executeResult struct {
	Output   json.RawMessage   `json:"output"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

type healthResult struct {
	Healthy bool   `json:"healthy"`
	Message string `json:"message,omitempty"`
}

func main() {
	// Handle --env-dump=<path> before anything else so tests can observe the
	// exact env the subprocess booted with.
	for _, arg := range os.Args[1:] {
		if strings.HasPrefix(arg, "--env-dump=") {
			path := strings.TrimPrefix(arg, "--env-dump=")
			f, err := os.Create(path)
			if err != nil {
				fmt.Fprintf(os.Stderr, "env-dump create %q: %v\n", path, err)
				os.Exit(1)
			}
			for _, kv := range os.Environ() {
				fmt.Fprintln(f, kv)
			}
			_ = f.Close()
		}
	}

	executeCount := 0
	echoExitAfter, _ := strconv.Atoi(os.Getenv("ECHO_PLUGIN_ECHO_EXIT_AFTER"))
	slowMs, _ := strconv.Atoi(os.Getenv("ECHO_PLUGIN_SLOW_MS"))

	// When set, ignore SIGINT/SIGTERM and continue running even if stdin
	// closes — used by tests to exercise the SIGKILL-after-timeout path in
	// plugin.Agent.Stop.
	if os.Getenv("ECHO_PLUGIN_IGNORE_SHUTDOWN") == "1" {
		sigs := make(chan os.Signal, 1)
		signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
		go func() {
			for range sigs {
				// swallow
			}
		}()
		go func() {
			// Hold the process alive even after stdin EOF.
			time.Sleep(10 * time.Minute)
			os.Exit(0)
		}()
	}

	// When set, write a sentinel line to stdout on SIGTERM to prove the
	// signal was received (as opposed to exiting on stdin EOF).
	if os.Getenv("ECHO_PLUGIN_SIGTERM_SENTINEL") == "1" {
		termCh := make(chan os.Signal, 1)
		signal.Notify(termCh, syscall.SIGTERM)
		go func() {
			<-termCh
			fmt.Fprintln(os.Stdout, `{"signal":"SIGTERM"}`)
			os.Exit(0)
		}()
	}

	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 64*1024), 10*1024*1024)
	out := bufio.NewWriter(os.Stdout)

	for scanner.Scan() {
		var req rpcRequest
		if err := json.Unmarshal(scanner.Bytes(), &req); err != nil {
			fmt.Fprintf(os.Stderr, "parse: %v\n", err)
			os.Exit(1)
		}

		if slowMs > 0 {
			time.Sleep(time.Duration(slowMs) * time.Millisecond)
		}

		resp := rpcResponse{JSONRPC: "2.0", ID: req.ID}
		switch req.Method {
		case "execute":
			executeCount++
			if os.Getenv("ECHO_PLUGIN_CRASH_ON_EXECUTE") == "1" {
				os.Exit(2)
			}
			if os.Getenv("ECHO_PLUGIN_RETURN_INVALID") == "1" {
				resp.Error = &rpcError{Code: -32602, Message: "bad params"}
				break
			}
			if os.Getenv("ECHO_PLUGIN_RETURN_INTERNAL") == "1" {
				resp.Error = &rpcError{Code: -32603, Message: "boom"}
				break
			}
			var p executeParams
			if err := json.Unmarshal(req.Params, &p); err != nil {
				resp.Error = &rpcError{Code: -32602, Message: err.Error()}
				break
			}
			payload := p.Payload
			if len(payload) == 0 {
				payload = json.RawMessage(`{}`)
			}
			result := executeResult{
				Output: payload,
				Metadata: map[string]string{
					"echo_task_id": p.TaskID,
				},
			}
			b, _ := json.Marshal(result)
			resp.Result = b
		case "health_check":
			unhealthy := os.Getenv("ECHO_PLUGIN_UNHEALTHY") == "1"
			b, _ := json.Marshal(healthResult{Healthy: !unhealthy, Message: ""})
			resp.Result = b
		default:
			resp.Error = &rpcError{Code: -32601, Message: "method not found"}
		}

		line, _ := json.Marshal(resp)
		out.Write(line)
		out.WriteByte('\n')
		out.Flush()

		if echoExitAfter > 0 && executeCount >= echoExitAfter && req.Method == "execute" {
			os.Exit(0)
		}
	}
	// If configured to ignore shutdown signals, stay alive after stdin EOF
	// so the host's SIGKILL-after-timeout path is exercised.
	if os.Getenv("ECHO_PLUGIN_IGNORE_SHUTDOWN") == "1" {
		select {}
	}
}
