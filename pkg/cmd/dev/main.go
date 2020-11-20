// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var devCmd = &cobra.Command{
	Use:   "dev [command] (flags)",
	Short: "Dev is the general-purpose dev tool for folks working on cockroachdb/cockroach.",
	Long: `
Dev is the general-purpose dev tool for folks working cockroachdb/cockroach. It
lets engineers do a few things:

- build various binaries (cockroach, optgen, ...)
- run arbitrary tests (unit tests, logic tests, ...)
- run tests under arbitrary configurations (under stress, using race builds, ...)
- generate code (bazel files, protobufs, ...)

...and much more.

(PS: Almost none of the above is implemented yet, haha.)
`,
}

func init() {
	devCmd.AddCommand(
		benchCmd,
		buildCmd,
		generateCmd,
		lintCmd,
		testCmd,
	)

	// Hide the `help` sub-command.
	devCmd.SetHelpCommand(&cobra.Command{
		Use:    "noop-help",
		Hidden: true,
	})
}

func main() {
	if err := devCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
