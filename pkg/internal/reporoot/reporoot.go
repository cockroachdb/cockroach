// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package reporoot contains utilities to determine the repository root.
package reporoot

import (
	"os"
	"path/filepath"

	"github.com/cockroachdb/errors/oserror"
)

// Get returns the repository root, relative to the
// current working directory.
//
// This is a replacement for running `git rev-parse
// --show-toplevel`.
func Get() string {
	return GetFor(".", ".git")
}

// GetFor starts at path and checks if the supplied
// relative directory is present, ascending on path
// until a hit is found (in which case the absolute
// path is returned). Returns an empty string on
// errors or no match.
func GetFor(path string, checkFor string) string {
	path, err := filepath.Abs(path)
	if err != nil {
		return ""
	}
	for {
		s, err := os.Stat(filepath.Join(path, checkFor))
		if err != nil {
			if !oserror.IsNotExist(err) {
				return ""
			}
		} else if s != nil {
			return path
		}
		path = filepath.Dir(path)
		if path == "/" {
			return ""
		}
	}
}
