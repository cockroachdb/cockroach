// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Contains a definition of CrdbTestBuild for the Bazel build.
// Note the file begins with `a_` so that its `init()` function will be run
// before any of the other `init()` functions in this package. *shrug*

//go:build bazel
// +build bazel

package util

var (
	CrdbTestBuild bool

	crdbTestString string
)

func init() {
	CrdbTestBuild = crdbTestString != ""
}
