// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package secretdir provides read-only access to credential files under the
// directory configured by the --secret-directory startup flag.
package secretdir

import (
	"os"
	"path/filepath"

	"github.com/cockroachdb/cockroach/pkg/util/fileutil"
	"github.com/cockroachdb/errors"
)

// Reader reads credential files from a single jailed directory. A nil
// receiver represents the "not configured" state and rejects all reads with a
// clear error.
type Reader struct {
	absBase string
}

// NewReader returns a Reader rooted at secretDir. If secretDir is empty (the
// --secret-directory flag wasn't set), NewReader returns nil.
func NewReader(secretDir string) (*Reader, error) {
	if secretDir == "" {
		return nil, nil
	}
	abs, err := filepath.Abs(secretDir)
	if err != nil {
		return nil, errors.Wrap(err, "resolving --secret-directory")
	}
	return &Reader{absBase: abs}, nil
}

// Validate prevents a SQL user with credential-file privilege from coaxing
// the node into reading files outside the operator's chosen jail.
func (r *Reader) Validate(absPath string) error {
	if r == nil {
		return errors.New(
			"--secret-directory is not configured; the flag must be set at " +
				"node startup to use file-based credentials")
	}
	if !filepath.IsAbs(absPath) {
		return errors.Errorf("path %q must be absolute", absPath)
	}
	if !fileutil.IsContained(r.absBase, filepath.Clean(absPath)) {
		return errors.Errorf("path %q escapes --secret-directory %q", absPath, r.absBase)
	}
	return nil
}

// ReadFile reads the contents of absPath after validating it.
func (r *Reader) ReadFile(absPath string) ([]byte, error) {
	if err := r.Validate(absPath); err != nil {
		return nil, err
	}
	return os.ReadFile(filepath.Clean(absPath))
}
