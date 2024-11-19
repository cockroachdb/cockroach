// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package testutils

import (
	"path/filepath"
	"testing"
)

// GetTestFiles returns the set of test files that matches the Glob pattern.
func GetTestFiles(tb testing.TB, testdataGlob string) []string {
	paths, err := filepath.Glob(testdataGlob)
	if err != nil {
		tb.Fatal(err)
	}
	if len(paths) == 0 {
		tb.Fatalf("no testfiles found matching: %s", testdataGlob)
	}
	return paths
}

var _ = GetTestFiles
