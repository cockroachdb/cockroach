// Copyright 2022 The Cockroach Authors.
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

	"github.com/spf13/cobra"
)

func makeComposeCmd(runE func(cmd *cobra.Command, args []string) error) *cobra.Command {
	composeCmd := &cobra.Command{
		Use:     "compose",
		Short:   "Run compose tests",
		Long:    "Run compose tests.",
		Example: "dev compose",
		Args:    cobra.ExactArgs(0),
		RunE:    runE,
	}
	addCommonBuildFlags(composeCmd)
	addCommonTestFlags(composeCmd)
	return composeCmd
}

func (d *dev) compose(cmd *cobra.Command, _ []string) error {
	ctx := cmd.Context()
	var (
		filter  = mustGetFlagString(cmd, filterFlag)
		short   = mustGetFlagBool(cmd, shortFlag)
		timeout = mustGetFlagDuration(cmd, timeoutFlag)
	)

	var args []string
	args = append(args, "run", "//pkg/compose:compose_test", "--config=test")
	args = append(args, mustGetRemoteCacheArgs(remoteCacheAddr)...)
	if numCPUs != 0 {
		args = append(args, fmt.Sprintf("--local_cpu_resources=%d", numCPUs))
	}
	if filter != "" {
		args = append(args, fmt.Sprintf("--test_filter=%s", filter))
	}
	if short {
		args = append(args, "--test_arg", "-test.short")
	}
	if timeout > 0 {
		args = append(args, fmt.Sprintf("--test_timeout=%d", int(timeout.Seconds())))
	}

	logCommand("bazel", args...)
	return d.exec.CommandContextInheritingStdStreams(ctx, "bazel", args...)
}
