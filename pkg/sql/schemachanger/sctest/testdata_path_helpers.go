// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sctest

import (
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/build/bazel"
	"github.com/cockroachdb/cockroach/pkg/testutils"
)

// testdataPath provides a path either relative to the current working
// directory or absolute to the input which is a relativePath compared
// to the root of the workspace.
func testdataPath(t *testing.T, relativePath string) string {
	if bazel.BuiltWithBazel() {
		return testutils.RewritableDataPath(t, relativePath)

	}
	callers := make([]uintptr, 512)
	callers = callers[:runtime.Callers(0, callers)]
	frames := runtime.CallersFrames(callers)

	// This logic assumes that the deepest function which is called
	// Test.* corresponds to the current working directory.
	var relativeTestTargetPath string
	for fr, ok := frames.Next(); ok; fr, ok = frames.Next() {
		fullFuncName := fr.Func.Name()
		var funcName, pkg string
		dot := strings.LastIndex(fullFuncName, ".")
		if dot == -1 {
			continue
		}
		pkg = fullFuncName[:dot]
		funcName = fullFuncName[dot+1:]
		const importPrefix = "github.com/cockroachdb/cockroach"
		if strings.HasPrefix(funcName, "Test") &&
			strings.HasPrefix(pkg, importPrefix) {
			relativeTestTargetPath = strings.TrimPrefix(pkg, importPrefix)
		}
	}
	n := strings.Count(relativeTestTargetPath, "/")
	return filepath.Join(strings.Repeat("../", n), relativePath)
}
