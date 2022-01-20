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
	"path/filepath"
	"time"

	"github.com/spf13/cobra"
)

func makeAcceptanceCmd(runE func(cmd *cobra.Command, args []string) error) *cobra.Command {
	acceptanceCmd := &cobra.Command{
		Use:     "acceptance",
		Short:   "Run acceptance tests",
		Long:    "Run acceptance tests.",
		Example: "dev acceptance",
		Args:    cobra.ExactArgs(0),
		RunE:    runE,
	}
	addCommonBuildFlags(acceptanceCmd)
	addCommonTestFlags(acceptanceCmd)
	return acceptanceCmd
}

func (d *dev) acceptance(cmd *cobra.Command, _ []string) error {
	ctx := cmd.Context()
	var (
		// cpus, remote-cache
		filter  = mustGetFlagString(cmd, filterFlag)
		short   = mustGetFlagBool(cmd, shortFlag)
		timeout = mustGetFlagDuration(cmd, timeoutFlag)
	)

	workspace, err := d.getWorkspace(ctx)
	if err != nil {
		return err
	}
	logDir := filepath.Join(workspace, "artifacts", "acceptance")
	err = d.os.MkdirAll(logDir)
	if err != nil {
		return err
	}

	var args []string
	args = append(args, "run", "//pkg/acceptance:acceptance_test", "--config=test")
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
	args = append(args, fmt.Sprintf("--test_arg=-l=%s", logDir))
	args = append(args, "--test_env=TZ=America/New_York")

	logCommand("bazel", args...)
	return d.exec.CommandContextInheritingStdStreams(ctx, "bazel", args...)
}
