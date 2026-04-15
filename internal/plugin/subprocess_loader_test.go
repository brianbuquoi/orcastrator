package plugin

import (
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/brianbuquoi/overlord/internal/config"
)

func writeLoaderManifest(t *testing.T, body string) string {
	t.Helper()
	dir := t.TempDir()
	p := filepath.Join(dir, "manifest.yaml")
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatalf("write manifest: %v", err)
	}
	return p
}

func TestLoadAndCreate_MissingBinary(t *testing.T) {
	mp := writeLoaderManifest(t, "name: x\nbinary: ./does_not_exist\n")
	_, err := LoadAndCreate(config.Agent{
		ID:           "missing-bin",
		Provider:     "plugin",
		ManifestPath: mp,
	}, slog.Default())
	if err == nil {
		t.Fatal("expected eager validation error, got nil")
	}
	var pve *PluginValidationError
	if !errors.As(err, &pve) {
		t.Fatalf("expected *PluginValidationError, got %T: %v", err, err)
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Errorf("expected 'not found' in error, got: %v", err)
	}
}

func TestLoadAndCreate_NonExecutableBinary(t *testing.T) {
	dir := t.TempDir()
	bin := filepath.Join(dir, "binary")
	if err := os.WriteFile(bin, []byte("#!/bin/sh\n"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	mp := filepath.Join(dir, "manifest.yaml")
	if err := os.WriteFile(mp, []byte("name: x\nbinary: ./binary\n"), 0o600); err != nil {
		t.Fatalf("write manifest: %v", err)
	}
	_, err := LoadAndCreate(config.Agent{
		ID:           "non-exec",
		Provider:     "plugin",
		ManifestPath: mp,
	}, slog.Default())
	if err == nil {
		t.Fatal("expected eager validation error")
	}
	var pve *PluginValidationError
	if !errors.As(err, &pve) {
		t.Fatalf("expected *PluginValidationError, got %T: %v", err, err)
	}
	if !strings.Contains(err.Error(), "not executable") {
		t.Errorf("expected 'not executable' in error, got: %v", err)
	}
}

func TestLoadAndCreate_EmptyManifestReturnsValidationError(t *testing.T) {
	_, err := LoadAndCreate(config.Agent{
		ID:       "nopath",
		Provider: "plugin",
	}, slog.Default())
	if err == nil {
		t.Fatal("expected error for empty manifest path")
	}
	var pve *PluginValidationError
	if !errors.As(err, &pve) {
		t.Fatalf("expected *PluginValidationError, got %T: %v", err, err)
	}
}
