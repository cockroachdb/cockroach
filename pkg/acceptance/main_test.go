// Copyright 2015 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Peter Mattis (peter@cockroachlabs.com)

// +build acceptance

// Acceptance tests are comparatively slow to run, so we use the above build
// tag to separate invocations of `go test` which are intended to run the
// acceptance tests from those which are not. The corollary file to this one
// is stub_main_test.go

package acceptance

import (
	"fmt"
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	if *flagRemote {
		fmt.Fprintln(os.Stderr, "use `make test [...]` instead of `make acceptance [...]` when running remote cluster")
		os.Exit(1)
	}

	runTests(m)
}
