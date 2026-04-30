// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package templates

import (
	"os"
	"path/filepath"

	"github.com/cockroachdb/errors"
)

// WriteBackendTF writes a backend.tf file into the specified working directory.
// If backend.tf already exists, it is overwritten.
func WriteBackendTF(workingDir string, content string) error {
	path := filepath.Join(workingDir, "backend.tf")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		return errors.Wrapf(err, "write backend.tf to %s", workingDir)
	}
	return nil
}
