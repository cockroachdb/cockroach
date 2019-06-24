// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// +build !acceptance

// Our Docker-based acceptance tests are comparatively slow to run, so we use
// the above build tag to separate invocations of `go test` which are intended
// to run the acceptance tests from those which are not. The corollary file to
// this one is main_test.go. Acceptance tests against remote clusters (which
// aren't run unless `-remote` is passed explicitly) are run through this file
// via the standard facilities (i.e. `make test` and without `acceptance` build
// tag).

package acceptance

import (
	"fmt"
	"os"
	"testing"
)

// MainTest is an exported implementation of TestMain for use by other
// packages.
func MainTest(m *testing.M) {
	fmt.Fprintln(os.Stderr, "not running with `acceptance` build tag; skipping")
}
