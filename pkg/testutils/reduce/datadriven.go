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
	t *testing.T,
	path string,
	filter func([]byte) ([]byte, error),
	interesting func(contains string) InterestingFn,
	mode Mode,
	passes []Pass,
) {
	datadriven.Walk(t, path, func(t *testing.T, path string) {
		RunTest(t, path, filter, interesting, mode, passes)
	})
}

// RunTest executes reducer commands. If the optional filter func is not nil,
// it is invoked on all "reduce" commands before the reduction algorithm is
// started. In verbose mode, output is written to stderr. Supported commands:
//
// "contains": Sets the substring that must be contained by an error message
// for a test case to be marked as interesting. This variable is passed to the
// `interesting` func.
//
// "reduce": Creates an InterestingFn based on the current value of contains
// and executes the reducer. Outputs the reduced case.
func RunTest(
	t *testing.T,
	path string,
	filter func([]byte) ([]byte, error),
	interesting func(contains string) InterestingFn,
	mode Mode,
	passes []Pass,
) {
	var contains string
	var log io.Writer
	if testing.Verbose() {
		log = os.Stderr
	}
	datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
		switch d.Cmd {
		case "contains":
			contains = d.Input
			return ""
		case "reduce":
			input := []byte(d.Input)
			if filter != nil {
				var err error
				input, err = filter(input)
				if err != nil {
					t.Fatal(err)
				}
			}
			output, err := Reduce(log, File(input), interesting(contains), 0, mode, passes...)
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
