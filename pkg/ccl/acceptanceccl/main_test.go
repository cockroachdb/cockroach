// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Cockroach Community Licence (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/pkg/ccl/LICENSE

// +build acceptance

// Acceptance tests are comparatively slow to run, so we use the above build
// tag to separate invocations of `go test` which are intended to run the
// acceptance tests from those which are not. The corollary file to this one
// is stub_main_test.go

package acceptanceccl

import (
	"fmt"
	"os"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/acceptance"
)

func TestMain(m *testing.M) {
	if acceptance.IsRemote() {
		fmt.Fprintln(os.Stderr, "use `make test [...]` instead of `make acceptance [...]` when running remote cluster")
		os.Exit(1)
	}

	acceptance.RunTests(m)
}
