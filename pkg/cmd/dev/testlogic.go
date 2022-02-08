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
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/cobra"
)

const (
	filesFlag    = "files"
	subtestsFlag = "subtests"
	configFlag   = "config"
	showSQLFlag  = "show-sql"
)

func makeTestLogicCmd(runE func(cmd *cobra.Command, args []string) error) *cobra.Command {
	testLogicCmd := &cobra.Command{
		Use:   "testlogic {,base,ccl,opt}",
		Short: "Run logic tests",
		Long:  "Run logic tests.",
		Example: `
	dev testlogic
	dev testlogic ccl opt
	dev testlogic --files=fk --subtests='20042|20045' --config=local`,
		Args: cobra.MinimumNArgs(0),
		RunE: runE,
	}
	testLogicCmd.Flags().Duration(timeoutFlag, 0*time.Minute, "timeout for test")
	testLogicCmd.Flags().BoolP(vFlag, "v", false, "show testing process output")
	testLogicCmd.Flags().BoolP(showLogsFlag, "", false, "show crdb logs in-line")
	testLogicCmd.Flags().Int(countFlag, 1, "run test the given number of times")
	testLogicCmd.Flags().String(filesFlag, "", "run logic tests for files matching this regex")
	testLogicCmd.Flags().String(subtestsFlag, "", "run logic test subtests matching this regex")
	testLogicCmd.Flags().String(configFlag, "", "run logic tests under the specified config")
	testLogicCmd.Flags().Bool(ignoreCacheFlag, false, "ignore cached test runs")
	testLogicCmd.Flags().Bool(showSQLFlag, false, "show SQL statements/queries immediately before they are tested")
	testLogicCmd.Flags().String(rewriteFlag, "", "argument to pass to underlying (only applicable for certain tests, e.g. logic and datadriven tests). If unspecified, -rewrite will be passed to the test binary.")
	testLogicCmd.Flags().Lookup(rewriteFlag).NoOptDefVal = "-rewrite"
	testLogicCmd.Flags().Bool(stressFlag, false, "run tests under stress")
	testLogicCmd.Flags().String(stressArgsFlag, "", "additional arguments to pass to stress")
	testLogicCmd.Flags().String(testArgsFlag, "", "additional arguments to pass to go test binary")

	addCommonBuildFlags(testLogicCmd)
	return testLogicCmd
}

func (d *dev) testlogic(cmd *cobra.Command, commandLine []string) error {
	choices, additionalBazelArgs := splitArgsAtDash(cmd, commandLine)
	ctx := cmd.Context()

	var (
		config        = mustGetFlagString(cmd, configFlag)
		files         = mustGetFlagString(cmd, filesFlag)
		ignoreCache   = mustGetFlagBool(cmd, ignoreCacheFlag)
		rewrite       = mustGetFlagString(cmd, rewriteFlag)
		showLogs      = mustGetFlagBool(cmd, showLogsFlag)
		subtests      = mustGetFlagString(cmd, subtestsFlag)
		timeout       = mustGetFlagDuration(cmd, timeoutFlag)
		verbose       = mustGetFlagBool(cmd, vFlag)
		showSQL       = mustGetFlagBool(cmd, showSQLFlag)
		count         = mustGetFlagInt(cmd, countFlag)
		stress        = mustGetFlagBool(cmd, stressFlag)
		stressCmdArgs = mustGetFlagString(cmd, stressArgsFlag)
		testArgs      = mustGetFlagString(cmd, testArgsFlag)
	)

	validChoices := []string{"base", "ccl", "opt"}
	if len(choices) == 0 {
		// Default to all targets.
		choices = append(choices, validChoices...)
	}
	for _, choice := range choices {
		valid := false
		for _, candidate := range validChoices {
			if candidate == choice {
				valid = true
			}
		}
		if !valid {
			return fmt.Errorf("invalid choice for `dev testlogic`; got %s, must be one of %s", choice, strings.Join(validChoices, ","))
		}
	}

	for _, choice := range choices {
		var testTarget, selectorPrefix string
		switch choice {
		case "base":
			testTarget = "//pkg/sql/logictest:logictest_test"
			selectorPrefix = "TestLogic"
		case "ccl":
			testTarget = "//pkg/ccl/logictestccl:logictestccl_test"
			selectorPrefix = "Test(CCL|Tenant)Logic"
		case "opt":
			testTarget = "//pkg/sql/opt/exec/execbuilder:execbuilder_test"
			selectorPrefix = "TestExecBuild"
		}

		var args []string
		args = append(args, "test")
		args = append(args, "--test_env=GOTRACEBACK=all")
		if numCPUs != 0 {
			args = append(args, fmt.Sprintf("--local_cpu_resources=%d", numCPUs))
		}
		if ignoreCache {
			args = append(args, "--nocache_test_results")
		}
		if verbose {
			args = append(args, "--test_arg", "-test.v")
		}
		if showLogs {
			args = append(args, "--test_arg", "-show-logs")
		}
		if showSQL {
			args = append(args, "--test_arg", "-show-sql")
		}
		if count != 1 {
			args = append(args, "--test_arg", fmt.Sprintf("-test.count=%d", count))
		}
		if len(files) > 0 {
			args = append(args, "--test_arg", "-show-sql")
		}
		if len(config) > 0 {
			args = append(args, "--test_arg", "-config", "--test_arg", config)
		}

		if rewrite != "" {
			if stress {
				return fmt.Errorf("cannot combine --%s and --%s", stressFlag, rewriteFlag)
			}
			workspace, err := d.getWorkspace(ctx)
			if err != nil {
				return err
			}

			args = append(args, fmt.Sprintf("--test_env=COCKROACH_WORKSPACE=%s", workspace))
			args = append(args, "--test_arg", rewrite)

			dir := getDirectoryFromTarget(testTarget)
			args = append(args, fmt.Sprintf("--sandbox_writable_path=%s", filepath.Join(workspace, dir)))
		}
		if timeout > 0 && !stress {
			args = append(args, fmt.Sprintf("--test_timeout=%d", int(timeout.Seconds())))

			// If stress is specified, we'll pad the timeout below.
		}

		if stress {
			args = append(args, "--test_sharding_strategy=disabled")
			args = append(args, d.getStressArgs(stressCmdArgs, timeout)...)
		}
		if testArgs != "" {
			goTestArgs, err := d.getGoTestArgs(ctx, testArgs)
			if err != nil {
				return err
			}
			args = append(args, goTestArgs...)
		}

		// TODO(irfansharif): Is this right? --config and --files is optional.
		selector := fmt.Sprintf(
			"%s/%s/%s/%s",
			selectorPrefix,
			maybeAddBeginEndMarkers(config),
			maybeAddBeginEndMarkers(files),
			subtests)
		args = append(args, testTarget)
		args = append(args, "--test_filter", selector)
		args = append(args, d.getTestOutputArgs(stress, verbose, showLogs)...)
		args = append(args, additionalBazelArgs...)
		logCommand("bazel", args...)
		if err := d.exec.CommandContextInheritingStdStreams(ctx, "bazel", args...); err != nil {
			return err
		}
	}
	return nil
}

func maybeAddBeginEndMarkers(s string) string {
	if len(s) == 0 {
		return s
	}
	return "^" + s + "$"
}
