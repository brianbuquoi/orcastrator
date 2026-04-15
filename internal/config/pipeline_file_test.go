package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writePipelineTestInfra(t *testing.T) *Config {
	t.Helper()
	return &Config{
		Version: "1",
		Agents: []Agent{
			{ID: "reviewer", Provider: "anthropic", Model: "x", Auth: AuthConfig{APIKeyEnv: "X"}},
		},
		SchemaRegistry: []SchemaEntry{
			{Name: "shared_in", Version: "v1", Path: "/tmp/ignored_a.json"},
		},
	}
}

func writePipelineYAML(t *testing.T, body string) string {
	t.Helper()
	dir := t.TempDir()
	p := filepath.Join(dir, "pipeline.yaml")
	if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestPipelineFile_LoadAndMergeInto_Success(t *testing.T) {
	dir := t.TempDir()
	schemaPath := filepath.Join(dir, "in.json")
	if err := os.WriteFile(schemaPath, []byte(`{"type":"object"}`), 0o644); err != nil {
		t.Fatal(err)
	}

	body := `version: "1"
schema_registry:
  - name: pipe_in
    version: "v1"
    path: ` + schemaPath + `
  - name: pipe_out
    version: "v1"
    path: ` + schemaPath + `

pipelines:
  - name: extra-pipe
    concurrency: 1
    store: memory
    stages:
      - id: s1
        agent: reviewer
        input_schema: { name: pipe_in, version: "v1" }
        output_schema: { name: pipe_out, version: "v1" }
        timeout: 1s
        retry: { max_attempts: 1, backoff: fixed, base_delay: 1s }
        on_success: done
        on_failure: dead-letter
`
	p := writePipelineYAML(t, body)

	pf, err := LoadPipelineFile(p)
	if err != nil {
		t.Fatalf("LoadPipelineFile: %v", err)
	}
	cfg := writePipelineTestInfra(t)
	// Give infra a valid schema path for re-validation.
	cfg.SchemaRegistry[0].Path = schemaPath

	if err := pf.MergeInto(cfg); err != nil {
		t.Fatalf("MergeInto: %v", err)
	}
	if len(cfg.Pipelines) != 1 || cfg.Pipelines[0].Name != "extra-pipe" {
		t.Fatalf("expected merged pipeline extra-pipe, got %+v", cfg.Pipelines)
	}
	if len(cfg.SchemaRegistry) != 3 {
		t.Fatalf("expected 3 schemas after merge, got %d", len(cfg.SchemaRegistry))
	}
}

func TestPipelineFile_MergeInto_SchemaConflict(t *testing.T) {
	cfg := writePipelineTestInfra(t)
	pf := &PipelineFile{
		Version: "1",
		SchemaRegistry: []SchemaEntry{
			{Name: "shared_in", Version: "v1", Path: "/other.json"},
		},
		Pipelines: []Pipeline{},
	}
	err := pf.MergeInto(cfg)
	if err == nil {
		t.Fatal("expected schema conflict error, got nil")
	}
	if !strings.Contains(err.Error(), "conflicts") {
		t.Errorf("expected conflict error, got: %v", err)
	}
}

func TestPipelineFile_MergeInto_UnknownAgent(t *testing.T) {
	cfg := writePipelineTestInfra(t)
	pf := &PipelineFile{
		Version: "1",
		Pipelines: []Pipeline{{
			Name:        "bad",
			Concurrency: 1,
			Store:       "memory",
			Stages: []Stage{{
				ID:           "s1",
				Agent:        "does-not-exist",
				InputSchema:  StageSchemaRef{Name: "shared_in", Version: "v1"},
				OutputSchema: StageSchemaRef{Name: "shared_in", Version: "v1"},
				OnSuccess:    StaticOnSuccess("done"),
				OnFailure:    "dead-letter",
				Retry:        RetryPolicy{MaxAttempts: 1, Backoff: "fixed"},
			}},
		}},
	}
	err := pf.MergeInto(cfg)
	if err == nil {
		t.Fatal("expected unknown-agent error, got nil")
	}
	if !strings.Contains(err.Error(), "does-not-exist") {
		t.Errorf("expected error to mention agent id, got: %v", err)
	}
}

func TestPipelineFile_MergeInto_VersionMismatch(t *testing.T) {
	cfg := writePipelineTestInfra(t)
	pf := &PipelineFile{Version: "2", Pipelines: []Pipeline{{Name: "p"}}}
	if err := pf.MergeInto(cfg); err == nil || !strings.Contains(err.Error(), "version") {
		t.Fatalf("expected version-mismatch error, got %v", err)
	}
}
