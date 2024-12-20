// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"fmt"
	"path/filepath"

	"github.com/spf13/cobra"
)

const (
	noRebuildCockroachFlag = "no-rebuild-cockroach"
	acceptanceVerboseFlag  = "verbose"
)

func makeAcceptanceCmd(runE func(cmd *cobra.Command, args []string) error) *cobra.Command {
	acceptanceCmd := &cobra.Command{
		Use:     "acceptance",
		Short:   "Run acceptance tests",
		Long:    "Run acceptance tests.",
		Example: "dev acceptance",
		Args:    cobra.MinimumNArgs(0),
		RunE:    runE,
	}

	addCommonBuildFlags(acceptanceCmd)
	addCommonTestFlags(acceptanceCmd)
	acceptanceCmd.Flags().Bool(noRebuildCockroachFlag, false, "set if it is unnecessary to rebuild cockroach (artifacts/cockroach-short must already exist, e.g. after being created by a previous `dev acceptance` run)")
	acceptanceCmd.Flags().String(volumeFlag, "bzlhome", "the Docker volume to use as the container home directory (only used for cross builds)")
	acceptanceCmd.Flags().BoolP(acceptanceVerboseFlag, "v", false, "show all output")
	return acceptanceCmd
}

func (d *dev) acceptance(cmd *cobra.Command, commandLine []string) error {
	_, additionalBazelArgs := splitArgsAtDash(cmd, commandLine)
	ctx := cmd.Context()
	var (
		filter             = mustGetFlagString(cmd, filterFlag)
		noRebuildCockroach = mustGetFlagBool(cmd, noRebuildCockroachFlag)
		short              = mustGetFlagBool(cmd, shortFlag)
		timeout            = mustGetFlagDuration(cmd, timeoutFlag)
		verbose            = mustGetFlagBool(cmd, acceptanceVerboseFlag)
	)

	// First we have to build cockroach.
	if !noRebuildCockroach {
		crossArgs, targets, err := d.getBasicBuildArgs(ctx, []string{"//pkg/cmd/cockroach-short:cockroach-short"})
		if err != nil {
			return err
		}
		volume := mustGetFlagString(cmd, volumeFlag)
		err = d.crossBuild(ctx, crossArgs, targets, "crosslinux", volume, nil)
		if err != nil {
			return err
		}
	}

	workspace, err := d.getWorkspace(ctx)
	if err != nil {
		return err
	}
	logDir := filepath.Join(workspace, "artifacts", "acceptance")
	err = d.os.MkdirAll(logDir)
	if err != nil {
		return err
	}
	cockroachBin := filepath.Join(workspace, "artifacts", "cockroach-short")

	var args []string
	args = append(args, "test", "//pkg/acceptance:acceptance_test")
	addCommonBazelArguments(&args)
	if filter != "" {
		args = append(args, fmt.Sprintf("--test_filter=%s", filter))
	}
	if short {
		args = append(args, "--test_arg", "-test.short")
	}
	if timeout > 0 {
		args = append(args, fmt.Sprintf("--test_timeout=%d", int(timeout.Seconds())))
	}
	if verbose {
		args = append(args, "--test_arg", "-test.v")
	}
	args = append(args, fmt.Sprintf("--test_arg=-l=%s", logDir))
	args = append(args, fmt.Sprintf("--sandbox_writable_path=%s", logDir))
	args = append(args, "--test_env=TZ=America/New_York")
	args = append(args, "--test_env=COCKROACH_RUN_ACCEPTANCE=true")
	args = append(args, fmt.Sprintf("--test_arg=-b=%s", cockroachBin))
	args = append(args, additionalBazelArgs...)

	logCommand("bazel", args...)
	return d.exec.CommandContextInheritingStdStreams(ctx, "bazel", args...)
}
