package config

import (
	"strings"
	"testing"
)

// TestValidate_Exported_IsAliasOfValidate confirms the public
// Validate wrapper routes to the private validate implementation —
// so callers (notably internal/workflow) can't accidentally get a
// weaker subset of the checks Load runs.
//
// We don't try to re-cover every rule; the existing validate_test.go
// suite owns that. This is a smoke test.
func TestValidate_Exported_IsAliasOfValidate(t *testing.T) {
	// A config with auth.enabled=true but no keys is rejected by
	// validateAuth. The exported Validate should return the same
	// error.
	cfg := &Config{
		Version: "1",
		Auth: APIAuthConfig{
			Enabled: true,
			Keys:    nil,
		},
	}
	err := Validate(cfg)
	if err == nil {
		t.Fatal("expected Validate to reject auth.enabled=true with no keys")
	}
	if !strings.Contains(err.Error(), "at least one key") {
		t.Errorf("error should mention 'at least one key'; got: %v", err)
	}

	// Zero-config (no pipelines, no agents, no auth) is accepted by
	// validate today — nothing to validate. Confirm Validate agrees;
	// otherwise callers can't use it as a round-trip check.
	if err := Validate(&Config{Version: "1"}); err != nil {
		t.Errorf("Validate rejected an otherwise-empty Config: %v", err)
	}
}
