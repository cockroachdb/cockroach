// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package reduce

import (
	"io"
	"os"
	"testing"

	"github.com/cockroachdb/datadriven"
)

// Walk walks path for datadriven files and calls RunTest on them.
func Walk(
	t *testing.T, path string, interesting func(contains string) InterestingFn, passes []Pass,
) {
	datadriven.Walk(t, path, func(t *testing.T, path string) {
		RunTest(t, path, interesting, passes)
	})
}

// RunTest executes reducer commands. In verbose mode, output is written to
// stderr. Supported commands:
//
// "contains": Sets the substring that must be contained by an error message
// for a test case to be marked as interesting. This variable is passed to the
// `interesting` func.
//
// "reduce": Creates an InterestingFn based on the current value of contains
// and executes the reducer. Outputs the reduced case.
func RunTest(
	t *testing.T, path string, interesting func(contains string) InterestingFn, passes []Pass,
) {
	var contains string
	var log io.Writer
	if testing.Verbose() {
		log = os.Stderr
	}
	datadriven.RunTest(t, path, func(d *datadriven.TestData) string {
		switch d.Cmd {
		case "contains":
			contains = d.Input
			return ""
		case "reduce":
			output, err := Reduce(log, File(d.Input), interesting(contains), passes...)
			if err != nil {
				t.Fatal(err)
			}
			return string(output) + "\n"
		default:
			t.Fatalf("unknown command: %s", d.Cmd)
			return ""
		}
	})
}
