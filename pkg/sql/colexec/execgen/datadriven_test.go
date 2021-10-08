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
	"strings"
	"testing"

	"github.com/cockroachdb/datadriven"
	"github.com/dave/dst/decorator"
)

// Walk walks path for datadriven files and calls RunTest on them.
func TestExecgen(t *testing.T) {
	datadriven.Walk(t, "testdata", func(t *testing.T, path string) {
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			f, err := decorator.Parse(d.Input)
			if err != nil {
				t.Fatal(err)
			}
			switch d.Cmd {
			case "inline":
				inlineFuncs(f)
			case "template":
				expandTemplates(f)
			default:
				t.Fatalf("unknown command: %s", d.Cmd)
				return ""
			}
			var sb strings.Builder
			if err := decorator.Fprint(&sb, f); err != nil {
				return err.Error()
			}
			return sb.String()
		})
	})
}
