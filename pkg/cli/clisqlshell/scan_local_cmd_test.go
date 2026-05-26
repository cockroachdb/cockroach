// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package clisqlshell

import (
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/cli/clierror"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/datadriven"
)

func TestScanLocalCmdArgs(t *testing.T) {
	defer leaktest.AfterTest(t)()

	datadriven.Walk(t, "testdata/local_cmds", func(t *testing.T, path string) {
		datadriven.RunTest(t, path, func(t *testing.T, td *datadriven.TestData) string {
			switch td.Cmd {
			case "run":
				var buf strings.Builder
				args, err := scanLocalCmdArgs(td.Input)
				if err != nil {
					clierror.OutputError(&buf, err, true, false)
					return buf.String()
				}
				fmt.Fprintf(&buf, "%d args:\n", len(args))
				for _, arg := range args {
					fmt.Fprintf(&buf, "%q\n", arg)
				}
				return buf.String()

			default:
				t.Fatalf("unknown command: %s", td.Cmd)
				return "unreachable"
			}
		})
	})
}
