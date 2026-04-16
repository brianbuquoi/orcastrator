package main

import (
	"fmt"
	"os"
)

// readPayloadFile reads a payload from a file path with the same hardening
// applied to config and pipeline file loading: rejects symlinks and
// non-regular files. Errors are human-readable rather than wrapped Go
// stat errors.
func readPayloadFile(path string) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("payload file not found: %s", path)
		}
		return nil, fmt.Errorf("cannot access payload file %s: %v", path, err)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("payload file must not be a symlink: %s", path)
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("payload file must be a regular file: %s", path)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("cannot read payload file %s: %v", path, err)
	}
	return data, nil
}
