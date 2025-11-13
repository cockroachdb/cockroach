// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package fileutil

import (
	"os"

	"github.com/cockroachdb/errors/oserror"
)

// ReadDir reads the directory and returns os.FileInfo for all entries sorted by
// filename. Files that are deleted between listing and stat are silently
// skipped, making this safe to use on directories with concurrent
// modifications.
func ReadDir(dirname string) ([]os.FileInfo, error) {
	entries, err := os.ReadDir(dirname)
	if err != nil {
		return nil, err
	}
	infos := make([]os.FileInfo, 0, len(entries))
	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil {
			// Skip files that disappeared between ReadDir and Info().
			// This can happen when the directory contains transient files
			// like Unix socket lock files that are created/deleted rapidly.
			if oserror.IsNotExist(err) {
				continue
			}
			return nil, err
		}
		infos = append(infos, info)
	}
	return infos, nil
}
