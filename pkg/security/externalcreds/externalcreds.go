// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package externalcreds validates and reads credential files under a
// caller-configured base directory.
package externalcreds

import (
	"os"
	"path/filepath"
	"strings"

	"github.com/cockroachdb/errors"
)

// Dir is a configured external credentials directory, stored as a clean
// absolute path. The zero value means no directory was configured; check
// with IsSet.
type Dir string

// NewDir resolves path to an absolute, cleaned Dir. An empty path returns
// the zero (unconfigured) Dir.
func NewDir(path string) (Dir, error) {
	if path == "" {
		return "", nil
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", errors.Wrap(err, "resolving external credentials directory")
	}
	return Dir(filepath.Clean(abs)), nil
}

// IsSet reports whether an external credentials directory was configured.
func (d Dir) IsSet() bool { return d != "" }

// Resolve validates that relPath is a relative path that resolves inside d
// and returns the absolute path. Fails with a "not configured" error if d
// is the zero value.
func (d Dir) Resolve(relPath string) (SecretPath, error) {
	if !d.IsSet() {
		return "", errors.WithHint(
			errors.New("external credentials directory is not configured"),
			"Configure an external credentials directory on every node, "+
				"pointing at the directory that holds credential files.")
	}
	if filepath.IsAbs(relPath) {
		return "", errors.Errorf(
			"path %q must be relative to the external credentials directory", relPath)
	}
	base := string(d)
	abs := filepath.Join(base, relPath) // Join cleans
	// Reject paths that escape d via "..". filepath.Rel handles the
	// base=="/" edge case where a prefix check on base+sep would falsely
	// reject any path under root.
	rel, err := filepath.Rel(base, abs)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) {
		return "", errors.WithHintf(
			errors.Errorf("path %q escapes external credentials directory %q", relPath, base),
			"Place the credential file under %q.", base)
	}
	return SecretPath(abs), nil
}

// SecretPath is a file path validated to live inside a Dir. Construct via
// Dir.Resolve.
type SecretPath string

// Read returns the contents of the file at p.
func (p SecretPath) Read() ([]byte, error) {
	return os.ReadFile(string(p))
}
