// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package testutils

import (
	"bufio"
	"io"
	"os"
	"path/filepath"
)

// ReadAllFiles reads all of the files matching pattern, thus ensuring they are
// in the OS buffer cache.
func ReadAllFiles(pattern string) {
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return
	}
	for _, m := range matches {
		f, err := os.Open(m)
		if err != nil {
			continue
		}
		_, _ = io.Copy(io.Discard, bufio.NewReader(f))
		f.Close()
	}
}
