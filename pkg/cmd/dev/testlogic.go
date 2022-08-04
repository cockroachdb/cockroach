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
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/cobra"
)

const (
	bigtestFlag  = "bigtest"
	filesFlag    = "files"
	subtestsFlag = "subtests"
	configFlag   = "config"
	showSQLFlag  = "show-sql"
	noGenFlag    = "no-gen"
)

func makeTestLogicCmd(runE func(cmd *cobra.Command, args []string) error) *cobra.Command {
	testLogicCmd := &cobra.Command{
		Use:     "testlogic {,base,ccl,opt,sqlite,sqliteccl}",
		Aliases: []string{"logictest"},
		Short:   "Run logic tests",
		Long:    "Run logic tests.",
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
	testLogicCmd.Flags().Bool(bigtestFlag, false, "run long-running sqlite logic tests")
	testLogicCmd.Flags().Bool(ignoreCacheFlag, false, "ignore cached test runs")
	testLogicCmd.Flags().Bool(showSQLFlag, false, "show SQL statements/queries immediately before they are tested")
	testLogicCmd.Flags().Bool(rewriteFlag, false, "rewrite test files using results from test run")
	testLogicCmd.Flags().Bool(noGenFlag, false, "skip generating logic test files before running logic tests")
	testLogicCmd.Flags().Bool(streamOutputFlag, false, "stream test output during run")
	testLogicCmd.Flags().Bool(stressFlag, false, "run tests under stress")
	testLogicCmd.Flags().String(stressArgsFlag, "", "additional arguments to pass to stress")
	testLogicCmd.Flags().String(testArgsFlag, "", "additional arguments to pass to go test binary")
	testLogicCmd.Flags().Bool(showDiffFlag, false, "generate a diff for expectation mismatches when possible")

	addCommonBuildFlags(testLogicCmd)
	return testLogicCmd
}

func (d *dev) testlogic(cmd *cobra.Command, commandLine []string) error {
	choices, additionalBazelArgs := splitArgsAtDash(cmd, commandLine)
	ctx := cmd.Context()

	var (
		bigtest       = mustGetFlagBool(cmd, bigtestFlag)
		config        = mustGetFlagString(cmd, configFlag)
		files         = mustGetFlagString(cmd, filesFlag)
		ignoreCache   = mustGetFlagBool(cmd, ignoreCacheFlag)
		rewrite       = mustGetFlagBool(cmd, rewriteFlag)
		streamOutput  = mustGetFlagBool(cmd, streamOutputFlag)
		showLogs      = mustGetFlagBool(cmd, showLogsFlag)
		subtests      = mustGetFlagString(cmd, subtestsFlag)
		timeout       = mustGetFlagDuration(cmd, timeoutFlag)
		verbose       = mustGetFlagBool(cmd, vFlag)
		noGen         = mustGetFlagBool(cmd, noGenFlag)
		showSQL       = mustGetFlagBool(cmd, showSQLFlag)
		count         = mustGetFlagInt(cmd, countFlag)
		stress        = mustGetFlagBool(cmd, stressFlag)
		stressCmdArgs = mustGetFlagString(cmd, stressArgsFlag)
		testArgs      = mustGetFlagString(cmd, testArgsFlag)
		showDiff      = mustGetFlagBool(cmd, showDiffFlag)
	)
	if rewrite {
		ignoreCache = true
	}

	validChoices := []string{"base", "ccl", "opt", "sqlite", "sqliteccl"}
	if len(choices) == 0 {
		// Default to all targets if --bigtest, else all non-sqlite targets.
		if bigtest {
			choices = append(choices, validChoices...)
		} else {
			for _, choice := range validChoices {
				if !strings.HasPrefix(choice, "sqlite") {
					choices = append(choices, choice)
				}
			}
		}
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

	workspace, err := d.getWorkspace(ctx)
	if err != nil {
		return err
	}

	if !noGen {
		err := d.generateLogicTest(cmd)
		if err != nil {
			return err
		}
	}

	var targets []string
	args := []string{"test"}
	for _, choice := range choices {
		var testsDir string
		switch choice {
		case "base":
			testsDir = "//pkg/sql/logictest/tests"
		case "ccl":
			testsDir = "//pkg/ccl/logictestccl/tests"
		case "opt":
			testsDir = "//pkg/sql/opt/exec/execbuilder/tests"
		case "sqlite":
			testsDir = "//pkg/sql/sqlitelogictest/tests"
			bigtest = true
		case "sqliteccl":
			testsDir = "//pkg/ccl/sqlitelogictestccl/tests"
			bigtest = true
		}
		// Keep track of the relative path to the root of the tests directory
		// (i.e. not the subdirectory for the config). We'll need this path
		// to properly build the writable path for rewrite.
		baseTestsDir := strings.TrimPrefix(testsDir, "//")
		if config != "" {
			testsDir = testsDir + "/" + config
			exists, err := d.os.IsDir(filepath.Join(workspace, strings.TrimPrefix(testsDir, "//")))
			if err != nil && errors.Is(err, os.ErrNotExist) {
				// The config isn't supported for this choice.
				continue
			} else if err != nil {
				return err
			}
			if !exists {
				continue
			}
		}
		targets = append(targets, testsDir+"/...")

		if rewrite {
			dir := filepath.Join(filepath.Dir(baseTestsDir), "testdata")
			args = append(args, fmt.Sprintf("--sandbox_writable_path=%s", filepath.Join(workspace, dir)))
			if choice == "ccl" {
				// The ccl logictest target shares the testdata directory with the base
				// logictest target -- make an allowance explicitly for that.
				args = append(args, fmt.Sprintf("--sandbox_writable_path=%s",
					filepath.Join(workspace, "pkg/sql/logictest")))
			}
		}
	}

	if len(targets) == 0 {
		log.Printf("WARNING: no tests found")
		return nil
	}
	args = append(args, targets...)
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
	if bigtest {
		args = append(args, "--test_arg", "-bigtest")
	}
	if count != 1 {
		args = append(args, "--test_arg", fmt.Sprintf("-test.count=%d", count))
	}
	if len(files) > 0 {
		args = append(args, "--test_arg", "-show-sql")
	}

	if rewrite {
		if stress {
			return fmt.Errorf("cannot combine --%s and --%s", stressFlag, rewriteFlag)
		}
		args = append(args, fmt.Sprintf("--test_env=COCKROACH_WORKSPACE=%s", workspace))
		args = append(args, "--test_arg", "-rewrite")
	}
	if showDiff {
		args = append(args, "--test_arg", "-show-diff")
	}
	if timeout > 0 && !stress {
		// If stress is specified, we'll pad the timeout differently below.

		// The bazel timeout should be higher than the timeout passed to the
		// test binary (giving it ample time to clean up, 5 seconds is probably
		// enough).
		args = append(args, fmt.Sprintf("--test_timeout=%d", 5+int(timeout.Seconds())))
		args = append(args, "--test_arg", fmt.Sprintf("-test.timeout=%s", timeout.String()))

		// If --test-args '-test.timeout=X' is specified as well, or
		// -- --test_arg '-test.timeout=X', that'll take precedence further
		// below.
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

	if files != "" || subtests != "" {
		args = append(args, "--test_filter", munge(files)+"/"+subtests)
		args = append(args, "--test_sharding_strategy=disabled")
	}
	args = append(args, d.getTestOutputArgs(stress, verbose, showLogs, streamOutput)...)
	args = append(args, additionalBazelArgs...)
	logCommand("bazel", args...)
	if err := d.exec.CommandContextInheritingStdStreams(ctx, "bazel", args...); err != nil {
		return err
	}
	return nil
}

func munge(s string) string {
	if s == "" {
		return ".*"
	}
	s = strings.ReplaceAll(s, "-", "_")
	s = strings.ReplaceAll(s, ".", "_")
	s = strings.ReplaceAll(s, "/", "")
	return s
}
