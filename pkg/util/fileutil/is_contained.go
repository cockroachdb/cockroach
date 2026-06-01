// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package fileutil

import (
	"os"
	"strings"
)

// IsContained reports whether path equals base or is a descendant of base.
// base must be an absolute path.
func IsContained(base, path string) bool {
	return path == base || strings.HasPrefix(path, base+string(os.PathSeparator))
}
