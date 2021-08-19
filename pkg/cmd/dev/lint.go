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

import "github.com/spf13/cobra"

// makeLintCmd constructs the subcommand used to run the specified linters.
func makeLintCmd(runE func(cmd *cobra.Command, args []string) error) *cobra.Command {
	lintCmd := &cobra.Command{
		Use:   "lint",
		Short: `Run the specified linters`,
		Long:  `Run the specified linters.`,
		Example: `
	dev lint --filter=TestLowercaseFunctionNames --short --timeout=1m`,
		Args: cobra.NoArgs,
		RunE: runE,
	}
	addCommonTestFlags(lintCmd)
	return lintCmd
}

func (d *dev) lint(cmd *cobra.Command, _ []string) error {
	ctx := cmd.Context()
	filter := mustGetFlagString(cmd, filterFlag)
	timeout := mustGetFlagDuration(cmd, timeoutFlag)
	short := mustGetFlagBool(cmd, shortFlag)

	var args []string
	// NOTE the --config=test here. It's very important we compile the test binary with the
	// appropriate stuff (gotags, etc.)
	args = append(args, "run", "--color=yes", "--config=test", "//build/bazelutil:lint")
	args = append(args, getConfigFlags()...)
	args = append(args, mustGetRemoteCacheArgs(remoteCacheAddr)...)
	args = append(args, "--", "-test.v")
	if short {
		args = append(args, "-test.short")
	}
	if timeout > 0 {
		args = append(args, "-test.timeout", timeout.String())
	}
	if filter != "" {
		args = append(args, "-test.run", filter)
	}

	return d.exec.CommandContextInheritingStdStreams(ctx, "bazel", args...)
}
