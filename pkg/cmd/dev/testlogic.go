// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/spf13/cobra"
)

const (
	bigtestFlag   = "bigtest"
	filesFlag     = "files"
	subtestsFlag  = "subtests"
	configsFlag   = "config"
	showSQLFlag   = "show-sql"
	noGenFlag     = "no-gen"
	forceGenFlag  = "force-gen"
	flexTypesFlag = "flex-types"
	workmemFlag   = "default-workmem"
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
	testLogicCmd.Flags().StringSlice(configsFlag, nil, "run logic tests under the specified configs")
	testLogicCmd.Flags().Bool(bigtestFlag, false, "run long-running sqlite logic tests")
	testLogicCmd.Flags().Bool(ignoreCacheFlag, false, "ignore cached test runs")
	testLogicCmd.Flags().Bool(showSQLFlag, false, "show SQL statements/queries immediately before they are tested")
	testLogicCmd.Flags().Bool(rewriteFlag, false, "rewrite test files using results from test run")
	testLogicCmd.Flags().Bool(noGenFlag, false, "skip generating logic test files before running logic tests")
	testLogicCmd.Flags().Bool(forceGenFlag, false, "force generating logic test files before running logic tests")
	testLogicCmd.Flags().Bool(streamOutputFlag, false, "stream test output during run")
	testLogicCmd.Flags().Bool(stressFlag, false, "run tests under stress")
	testLogicCmd.Flags().String(testArgsFlag, "", "additional arguments to pass to go test binary")
	testLogicCmd.Flags().Bool(showDiffFlag, false, "generate a diff for expectation mismatches when possible")
	testLogicCmd.Flags().Bool(flexTypesFlag, false, "tolerate when a result column is produced with a different numeric type")
	testLogicCmd.Flags().Bool(workmemFlag, false, "disable randomization of sql.distsql.temp_storage.workmem")

	addCommonBuildFlags(testLogicCmd)
	return testLogicCmd
}

func (d *dev) testlogic(cmd *cobra.Command, commandLine []string) error {
	choices, additionalBazelArgs := splitArgsAtDash(cmd, commandLine)
	ctx := cmd.Context()

	var (
		bigtest        = mustGetFlagBool(cmd, bigtestFlag)
		configs        = mustGetFlagStringSlice(cmd, configsFlag)
		files          = mustGetFlagString(cmd, filesFlag)
		ignoreCache    = mustGetFlagBool(cmd, ignoreCacheFlag)
		rewrite        = mustGetFlagBool(cmd, rewriteFlag)
		streamOutput   = mustGetFlagBool(cmd, streamOutputFlag)
		showLogs       = mustGetFlagBool(cmd, showLogsFlag)
		subtests       = mustGetFlagString(cmd, subtestsFlag)
		timeout        = mustGetFlagDuration(cmd, timeoutFlag)
		verbose        = mustGetFlagBool(cmd, vFlag)
		noGen          = mustGetFlagBool(cmd, noGenFlag)
		forceGen       = mustGetFlagBool(cmd, forceGenFlag)
		showSQL        = mustGetFlagBool(cmd, showSQLFlag)
		count          = mustGetFlagInt(cmd, countFlag)
		stress         = mustGetFlagBool(cmd, stressFlag)
		testArgs       = mustGetFlagString(cmd, testArgsFlag)
		showDiff       = mustGetFlagBool(cmd, showDiffFlag)
		flexTypes      = mustGetFlagBool(cmd, flexTypesFlag)
		defaultWorkmem = mustGetFlagBool(cmd, workmemFlag)
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

	if !noGen && (forceGen || d.shouldGenerateLogicTests(ctx)) {
		err := d.generateLogicTest(cmd)
		if err != nil {
			return err
		}
	}

	var targets []string
	args := []string{"test"}
	var hasNonSqlite bool
	for _, choice := range choices {
		var testsDir string
		switch choice {
		case "base":
			testsDir = "//pkg/sql/logictest/tests"
			hasNonSqlite = true
		case "ccl":
			testsDir = "//pkg/ccl/logictestccl/tests"
			hasNonSqlite = true
		case "opt":
			testsDir = "//pkg/sql/opt/exec/execbuilder/tests"
			hasNonSqlite = true
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
		testsDirs := []string{testsDir}
		if len(configs) > 0 {
			testsDirs = nil
			for _, config := range configs {
				configTestsDir := testsDir + "/" + config
				exists, err := d.os.IsDir(filepath.Join(workspace, strings.TrimPrefix(configTestsDir, "//")))
				if err != nil && errors.Is(err, os.ErrNotExist) {
					// The config isn't supported for this choice.
					continue
				} else if err != nil {
					return err
				}
				if !exists {
					continue
				}
				testsDirs = append(testsDirs, configTestsDir)
			}
		}
		for _, testsDir := range testsDirs {
			targets = append(targets, testsDir+"/...")
		}

		if rewrite {
			writeablePathArg := func(dir string) string {
				return fmt.Sprintf("--sandbox_writable_path=%s", filepath.Join(workspace, dir))
			}

			dir := filepath.Join(filepath.Dir(baseTestsDir), "testdata")
			args = append(args, writeablePathArg(dir))
			if choice == "ccl" {
				// The ccl logictest target shares the testdata directory with the base
				// logictest target -- make an allowance explicitly for that.
				args = append(args, writeablePathArg("pkg/sql/logictest"))
				args = append(args, writeablePathArg("pkg/sql/opt/exec/execbuilder/testdata/"))
			}
		}
	}

	if len(targets) == 0 {
		log.Printf("WARNING: no tests found")
		return nil
	}
	args = append(args, targets...)
	args = append(args, "--test_env=GOTRACEBACK=all")
	addCommonBazelArguments(&args)
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
	if len(files) > 0 {
		args = append(args, "--test_arg", "-show-sql")
	}
	if !hasNonSqlite {
		// If we only have sqlite targets, then we always append --flex-types
		// argument to simulate what we do in the CI.
		flexTypes = true
	}
	if flexTypes {
		args = append(args, "--test_arg", "-flex-types")
	}
	if defaultWorkmem {
		args = append(args, "--test_arg", "-default-workmem")
	}

	if rewrite {
		if stress {
			return fmt.Errorf("cannot combine --%s and --%s", stressFlag, rewriteFlag)
		}
		args = append(args, fmt.Sprintf("--test_env=COCKROACH_WORKSPACE=%s", workspace))
		args = append(args, "--test_arg", "-rewrite")
	}
	if stress && streamOutput {
		return fmt.Errorf("cannot combine --%s and --%s", stressFlag, streamOutputFlag)
	}
	if showDiff {
		args = append(args, "--test_arg", "-show-diff")
	}
	if timeout > 0 {
		args = append(args, fmt.Sprintf("--test_timeout=%d", int(timeout.Seconds())))
	}
	if stress {
		if count == 1 {
			// Default to 500 unless a different count was provided.
			// NB: Logic tests are generally big. We use 500 instead
			// of 1000 (the default for `dev test`).
			count = 500
		}
		args = append(args,
			"--test_env=COCKROACH_STRESS=true",
			"--notest_keep_going",
		)
	}
	if count != 1 {
		args = append(args, fmt.Sprintf("--runs_per_test=%d", count))
	}
	if testArgs != "" {
		goTestArgs, err := d.getGoTestArgs(ctx, testArgs)
		if err != nil {
			return err
		}
		args = append(args, goTestArgs...)
	}

	if files != "" || subtests != "" {
		filesRegexp := spaceSeparatedRegexpsToRegexp(files)
		args = append(args, "--test_filter", filesRegexp+"/"+subtests)
		args = append(args, "--test_sharding_strategy=disabled")
	}
	args = append(args, d.getTestOutputArgs(verbose, showLogs, streamOutput)...)
	args = append(args, additionalBazelArgs...)
	logCommand("bazel", args...)
	if stress {
		d.warnAboutChangeInStressBehavior(timeout)
	}
	if err := d.exec.CommandContextInheritingStdStreams(ctx, "bazel", args...); err != nil {
		return err
	}
	return nil
}

// This function determines if any test_logic or execbuilder/testdata files were
// modified in the current branch, and if so, determines if we should re-generate logic tests.
func (d *dev) shouldGenerateLogicTests(ctx context.Context) bool {
	if buildutil.CrdbTestBuild {
		return true
	}
	base, _ := d.getMergeBaseHash(ctx)
	// Generate logic tests if the merge base hash isn't found
	if base == "" {
		return true
	}
	changedFiles, _ := d.exec.CommandContextSilent(ctx, "git", "diff", "--no-ext-diff", "--name-only", base, "--", "pkg/sql/logictest/logictestbase/** ", "pkg/sql/logictest/testdata/**", "pkg/sql/sqlitelogictest/BUILD.bazel", "pkg/sql/sqlitelogictest/sqlitelogictest.go", "pkg/ccl/logictestccl/testdata/**", "pkg/sql/opt/exec/execbuilder/testdata/**")
	return strings.TrimSpace(string(changedFiles)) != ""
}

// We know that the regular expressions for files should not contain whitespace
// because the file names do not contain whitespace. We support users passing
// whitespace separated regexps for multiple files.
func spaceSeparatedRegexpsToRegexp(s string) string {
	s = munge(s)
	split := strings.Fields(s)
	if len(split) < 2 {
		return s
	}
	for i, s := range split {
		split[i] = "(" + s + ")"
	}
	return "(" + strings.Join(split, "|") + ")"
}

func munge(s string) string {
	if s == "" {
		return ".*"
	}
	s = strings.ReplaceAll(s, "-", "_")
	s = strings.ReplaceAll(s, ".", "_")
	s = strings.ReplaceAll(s, "/", "")
	s = strings.ReplaceAll(s, "^", "^Test[a-zA-Z0-9]+_")
	return s
}
