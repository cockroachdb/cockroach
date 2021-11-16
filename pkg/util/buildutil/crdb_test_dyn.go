// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//go:build bazel
// +build bazel

package buildutil

var (
	CrdbTestBuild  bool
	crdbTestString string
)

// This version of the buildutil package is compiled only for `dbg` or
// `fastbuild` Bazel builds. The original Go version of the package uses build
// tags to accomplish the same thing, but switching between build tags thrashes
// the Bazel cache. For `opt` builds, we instead use `crdb_test_on.go` or
// `crdb_test_off.go`, which makes `CrdbTestBuild` a `const`. This gives the
// compiler more room to optimize out unreachable code. Here, for dev scenarios,
// we instead dynamically set the value of the variable at package init() time.
// This means you can swap between build and test requiring only a re-link.
func init() {
	CrdbTestBuild = crdbTestString != ""
}
