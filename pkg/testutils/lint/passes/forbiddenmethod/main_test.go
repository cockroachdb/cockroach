// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package forbiddenmethod_test

import "github.com/cockroachdb/cockroach/pkg/build/bazel"

func init() {
	if bazel.BuiltWithBazel() {
		bazel.SetGoEnv()
	}
}
