package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestReadPayloadFile_Symlink(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "real.json")
	if err := os.WriteFile(target, []byte(`{"ok":true}`), 0o644); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(dir, "link.json")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}

	_, err := readPayloadFile(link)
	if err == nil {
		t.Fatal("expected error for symlink")
	}
	if !strings.Contains(err.Error(), "symlink") {
		t.Errorf("expected 'symlink' in error, got: %v", err)
	}
}

func TestReadPayloadFile_NonRegular(t *testing.T) {
	dir := t.TempDir()
	_, err := readPayloadFile(dir)
	if err == nil {
		t.Fatal("expected error for directory")
	}
	if !strings.Contains(err.Error(), "regular file") {
		t.Errorf("expected 'regular file' in error, got: %v", err)
	}
}

func TestReadPayloadFile_NotFound(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "missing.json")
	_, err := readPayloadFile(missing)
	if err == nil {
		t.Fatal("expected error for missing file")
	}
	if !strings.Contains(err.Error(), "payload file not found") {
		t.Errorf("expected human-readable not-found message, got: %v", err)
	}
	if strings.Contains(err.Error(), "no such file") {
		t.Errorf("error leaks raw Go error: %v", err)
	}
}

func TestReadPayloadFile_HappyPath(t *testing.T) {
	dir := t.TempDir()
	f := filepath.Join(dir, "ok.json")
	if err := os.WriteFile(f, []byte(`{"x":1}`), 0o644); err != nil {
		t.Fatal(err)
	}
	data, err := readPayloadFile(f)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(data) != `{"x":1}` {
		t.Errorf("unexpected data: %s", string(data))
	}
}

func TestSubmit_PayloadFromSymlink_Hardened(t *testing.T) {
	// Submit with @symlink should fail with the same error as exec.
	dir := t.TempDir()
	target := filepath.Join(dir, "real.json")
	if err := os.WriteFile(target, []byte(`{"request":"hi"}`), 0o644); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(dir, "link.json")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}

	// We need a valid config file for the submit command to get past config loading.
	schemasDir := filepath.Join(dir, "schemas")
	os.MkdirAll(schemasDir, 0o755)
	os.WriteFile(filepath.Join(schemasDir, "in.json"),
		[]byte(`{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object","properties":{"request":{"type":"string"}},"required":["request"]}`), 0o644)
	os.WriteFile(filepath.Join(schemasDir, "out.json"),
		[]byte(`{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object","properties":{"response":{"type":"string"}},"required":["response"]}`), 0o644)

	yaml := `version: "1"
schema_registry:
  - name: task_in
    version: "v1"
    path: schemas/in.json
  - name: task_out
    version: "v1"
    path: schemas/out.json
pipelines:
  - name: test
    concurrency: 1
    store: memory
    stages:
      - id: s1
        agent: a1
        input_schema: { name: task_in, version: "v1" }
        output_schema: { name: task_out, version: "v1" }
        timeout: 5s
        retry: { max_attempts: 1, backoff: fixed, base_delay: 50ms }
        on_success: done
        on_failure: dead-letter
agents:
  - id: a1
    provider: ollama
    model: llama3
    system_prompt: "t"
    timeout: 5s
stores:
  memory:
    max_tasks: 100
`
	configPath := filepath.Join(dir, "config.yaml")
	os.WriteFile(configPath, []byte(yaml), 0o644)
	t.Setenv("OLLAMA_ENDPOINT", "http://localhost:11434")

	root := rootCmd()
	root.SetArgs([]string{
		"submit",
		"--config", configPath,
		"--id", "test",
		"--payload", "@" + link,
	})

	err := root.Execute()
	if err == nil {
		t.Fatal("expected error for symlink payload in submit")
	}
	if !strings.Contains(err.Error(), "symlink") {
		t.Errorf("expected 'symlink' in error, got: %v", err)
	}
}
