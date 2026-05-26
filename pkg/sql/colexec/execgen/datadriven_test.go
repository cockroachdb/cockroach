// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package execgen

import (
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/datadriven"
	"github.com/dave/dst/decorator"
)

// Walk walks path for datadriven files and calls RunTest on them.
func TestExecgen(t *testing.T) {
	datadriven.Walk(t, datapathutils.TestDataPath(t), func(t *testing.T, path string) {
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
