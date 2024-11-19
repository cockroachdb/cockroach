// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
	// ERROR: -e: unsupported command: \dz with 0 arguments
	// HINT: Use the SQL SHOW statement to inspect your schema.
}

func TestDescribe(t *testing.T) {
	defer leaktest.AfterTest(t)()

	c := cli.NewCLITest(cli.TestCLIParams{T: t})
	defer c.Cleanup()

	db := c.Server.SQLConn(t, serverutils.DBName("defaultdb"))

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
