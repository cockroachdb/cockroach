// Copyright 2016 The Cockroach Authors.
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
// Author: Raphael 'kena' Poss (knz@cockroachlabs.com)

package acceptance

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/cockroach/acceptance/cluster"
	"github.com/cockroachdb/cockroach/util/log"
)

const testGlob = "../cli/interactive_tests/test*.tcl"
const containerPath = "/go/src/github.com/cockroachdb/cockroach/cli/interactive_tests"

var cmdBase = []string{
	"/usr/bin/env",
	"COCKROACH_SKIP_UPDATE_CHECK=1",
	"PGHOST=localhost",
	"PGPORT=",
	"/usr/bin/expect",
}

func TestDockerCLI(t *testing.T) {
	if err := testDockerOneShot(t, "cli_test", []string{"stat", cluster.CockroachBinaryInContainer}); err != nil {
		t.Skipf(`TODO(dt): No binary in one-shot container, see #6086: %s`, err)
	}

	paths, err := filepath.Glob(testGlob)
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) == 0 {
		t.Fatalf("no testfiles found (%v)", testGlob)
	}

	verbose := testing.Verbose() || log.V(1)
	for _, p := range paths {
		testFile := filepath.Base(p)
		testPath := filepath.Join(containerPath, testFile)
		if verbose {
			fmt.Printf("--- test: %s\n", testFile)
		}

		cmd := cmdBase
		if verbose {
			cmd = append(cmd, "-d")
		}
		cmd = append(cmd, "-f", testPath, cluster.CockroachBinaryInContainer)

		if err := testDockerOneShot(t, "cli_test", cmd); err != nil {
			fmt.Printf("--- %s FAIL\n", testFile)
			t.Fatal(err)
		}
		if verbose {
			fmt.Printf("--- %s SUCCESS\n", testFile)
		}
	}
}
