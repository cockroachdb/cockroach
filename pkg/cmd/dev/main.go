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
	"context"
	"os"
	"os/exec"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/spf13/cobra"
)

var devCmd = &cobra.Command{
	Use:     "dev [command] (flags)",
	Short:   "Dev is the general-purpose dev tool for folks working on cockroachdb/cockroach.",
	Version: "v0.0",
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
	// Disable automatic printing of usage information whenever an error
	// occurs. We presume that most errors will not the result of bad command
	// invocation; they'll be due to legitimate build/test errors. Printing out
	// the usage information in these cases obscures the real cause of the
	// error. Commands should manually print usage information when the error
	// is, in fact, a result of a bad invocation, e.g. too many arguments.
	SilenceUsage: true,
	// Disable automatic printing of the error. We want to also print
	// details and hints, which cobra does not do for us. Instead
	// we do the printing in the command implementation.
	SilenceErrors: true,
}

var bazel = "bazel"

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

func runDev(ctx context.Context) error {
	_, err := exec.LookPath(bazel)
	if err != nil {
		log.Errorf(ctx, "expected to find bazel in $PATH")
		return err
	}

	if err := devCmd.Execute(); err != nil {
		return err
	}
	return nil
}

func main() {
	ctx := context.Background()
	if err := runDev(ctx); err != nil {
		log.Errorf(ctx, "%v", err)
		os.Exit(1)
	}
}
