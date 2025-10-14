// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/spf13/cobra"
)

const eachFlag = "each"

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
	composeCmd.Flags().Bool(noRebuildCockroachFlag, false, "set if it is unnecessary to rebuild cockroach (artifacts/cockroach must already exist, e.g. after being created by a previous `dev compose` run)")
	composeCmd.Flags().String(volumeFlag, "bzlhome", "the Docker volume to use as the container home directory (only used for cross builds)")
	composeCmd.Flags().Duration(eachFlag, 10*time.Minute, "individual test timeout")
	return composeCmd
}

func (d *dev) compose(cmd *cobra.Command, _ []string) error {
	ctx := cmd.Context()
	var (
		filter             = mustGetFlagString(cmd, filterFlag)
		noRebuildCockroach = mustGetFlagBool(cmd, noRebuildCockroachFlag)
		short              = mustGetFlagBool(cmd, shortFlag)
		timeout            = mustGetFlagDuration(cmd, timeoutFlag)
		each               = mustGetFlagDuration(cmd, eachFlag)
	)

	if !noRebuildCockroach {
		crossArgs, targets, err := d.getBasicBuildArgs(ctx, []string{"//pkg/cmd/cockroach:cockroach", "//pkg/compose/compare/compare:compare_test", "//c-deps:libgeos"})
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
	artifactsDir := filepath.Join(workspace, "artifacts")
	cockroachBin := filepath.Join(artifactsDir, "cockroach")
	compareBin := filepath.Join(artifactsDir, "compare_test")

	// Copy libgeos files to a temporary directory, since the compose tests expect
	// libgeos files to be in their own directory.
	libGeosDir, err := os.MkdirTemp("", "lib")
	if err != nil {
		return err
	}
	defer func() {
		_ = os.RemoveAll(libGeosDir)
	}()
	for _, geoLib := range []string{"libgeos.so", "libgeos_c.so"} {
		src := filepath.Join(artifactsDir, geoLib)
		dst := filepath.Join(libGeosDir, geoLib)
		if err := d.os.CopyFile(src, dst); err != nil {
			return err
		}
	}

	var args []string
	args = append(args, "test", "//pkg/compose:compose_test")
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
	if each > 0 {
		args = append(args, "--test_arg", "-each", "--test_arg", each.String())
	}

	args = append(args, "--test_arg", "-cockroach", "--test_arg", cockroachBin)
	args = append(args, "--test_arg", "-libgeosdir", "--test_arg", libGeosDir)
	args = append(args, "--test_arg", "-compare", "--test_arg", compareBin)
	args = append(args, "--test_arg", "-test.v")
	args = append(args, "--test_output", "all")
	args = append(args, "--test_env", "COCKROACH_DEV_LICENSE")
	args = append(args, "--test_env", "COCKROACH_RUN_COMPOSE=true")
	args = append(args, "--sandbox_add_mount_pair", os.TempDir())

	logCommand("bazel", args...)
	return d.exec.CommandContextInheritingStdStreams(ctx, "bazel", args...)
}
