// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package metamorphicutil

// NB: init() in pkg/util/metamorphic/constants.go may set this value to true.
// We don't put this variable in that package as we would like to have a way to
// reliably determine which packages use metamorphic constants, and
// `bazel query somepath(_, //pkg/util/metamorphic)` should be a good way to do
// that. However, some packages (like pkg/testutils/skip) need to check whether
// we're running a metamorphic build, without necessarily depending on the
// metamorphic package.
//
// Generally, you should use metamorphic.IsMetaMorphicBuild() instead of checking
// this value.
var IsMetamorphicBuild bool
