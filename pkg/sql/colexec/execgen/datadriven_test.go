// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package execgen

import (
	"testing"

	"github.com/cockroachdb/datadriven"
)

// Walk walks path for datadriven files and calls RunTest on them.
func TestExecgen(t *testing.T) {
	datadriven.Walk(t, "testdata", func(t *testing.T, path string) {
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "inline":
				s, err := InlineFuncs(d.Input)
				if err != nil {
					t.Fatal(err)
				}
				return s
			default:
				t.Fatalf("unknown command: %s", d.Cmd)
				return ""
			}
		})
	})
}
