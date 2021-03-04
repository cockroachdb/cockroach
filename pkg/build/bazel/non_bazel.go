// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// +build !bazel

package bazel

import (
	_ "github.com/bazelbuild/rules_go/go/tools/bazel"
)

func BuiltWithBazel() bool {
	return false
}

func Runfile(string) (string, error) {
	panic("not built with Bazel")
}

func RunfilesPath() (string, error) {
	panic("not built with Bazel")
}

func TestTmpDir() string {
	panic("not built with Bazel")
}

func RelativeTestTargetPath() string {
	panic("not built with Bazel")
}

func SetGoEnv() {
	panic("not built with Bazel")
}
