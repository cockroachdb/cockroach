// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package templates

import (
	"os"
	"path/filepath"
	"strings"

	"github.com/cockroachdb/errors"
)

// listTFFiles returns all .tf files in a directory, with override files last.
// Skips hidden files (leading "."), vim backups (trailing "~"),
// and emacs backups ("#name#").
func listTFFiles(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, errors.Wrap(err, "read template directory")
	}

	var primary, override []string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasSuffix(name, ".tf") {
			continue
		}
		if isIgnoredFile(name) {
			continue
		}
		baseName := strings.TrimSuffix(name, ".tf")
		fullPath := filepath.Join(dir, name)
		if baseName == "override" || strings.HasSuffix(baseName, "_override") {
			override = append(override, fullPath)
		} else {
			primary = append(primary, fullPath)
		}
	}

	return append(primary, override...), nil
}

// isIgnoredFile returns true if the given filename should be ignored as an
// editor swap file or hidden file.
func isIgnoredFile(name string) bool {
	return strings.HasPrefix(name, ".") ||
		strings.HasSuffix(name, "~") ||
		(strings.HasPrefix(name, "#") && strings.HasSuffix(name, "#"))
}
