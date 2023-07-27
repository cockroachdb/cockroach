// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package clisqlshell_test

import (
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/cli"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/datadriven"
)

// Example_describe_unknown checks an error path.
func Example_describe_unknown() {
	c := cli.NewCLITest(cli.TestCLIParams{})
	defer c.Cleanup()

	c.RunWithArgs([]string{`sql`, `-e`, `\set echo`, `-e`, `\dz`})

	// Output:
	// sql -e \set echo -e \dz
	// ERROR: unsupported command: \dz with 0 arguments
	// HINT: Use the SQL SHOW statement to inspect your schema.
	// ERROR: -e: unsupported command: \dz with 0 arguments
	// HINT: Use the SQL SHOW statement to inspect your schema.
}

func TestDescribe(t *testing.T) {
	defer leaktest.AfterTest(t)()

	c := cli.NewCLITest(cli.TestCLIParams{T: t})
	defer c.Cleanup()

	db := serverutils.OpenDBConn(
		t, c.TestServer.ServingSQLAddr(), "defaultdb", false /* insecure */, c.TestServer.Stopper())

	var commonArgs []string

	datadriven.RunTest(t, "testdata/describe", func(t *testing.T, td *datadriven.TestData) string {
		switch td.Cmd {
		case "sql":
			_, err := db.Exec(td.Input)
			if err != nil {
				t.Fatalf("%s: sql error: %v", td.Pos, err)
			}
			return "ok"

		case "common":
			commonArgs = strings.Split(td.Input, "\n")
			return "ok"

		case "cli":
			args := strings.Split(td.Input, "\n")
			out, err := c.RunWithCaptureArgs(append(commonArgs, args...))
			if err != nil {
				t.Fatalf("%s: %v", td.Pos, err)
			}
			return out

		default:
			t.Fatalf("%s: unknown command: %q", td.Pos, td.Cmd)
			return "" // unreachable
		}
	})
}
