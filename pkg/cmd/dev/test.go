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
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
)

// testCmd runs the specified cockroachdb tests.
var testCmd = &cobra.Command{
	Use:   "test [pkg..]",
	Short: `Run the specified tests`,
	Long:  `Run the specified tests.`,
	Example: `
	dev test
	dev test pkg/kv/kvserver --filter=TestReplicaGC* -v -show-logs --timeout=1m
	dev test --stress --race ...
	dev test --logic --files=prepare|fk --subtests=20042 --config=local
	dev test --fuzz sql/sem/tree --filter=Decimal`,
	Args: cobra.MinimumNArgs(0),
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := context.Background()
		return runTest(ctx, cmd, args)
	},
}

var (
	// General testing flags.
	filterFlag      = "filter"
	timeoutFlag     = "timeout"
	showLogsFlag    = "show-logs"
	vFlag           = "verbose"
	stressFlag      = "stress"
	raceFlag        = "race"
	ignoreCacheFlag = "ignore-cache"

	// Fuzz testing related flag.
	fuzzFlag = "fuzz"

	// Logic test related flags.
	logicFlag    = "logic"
	filesFlag    = "files"
	subtestsFlag = "subtests"
	configFlag   = "config"
)

func init() {
	// Attach flags for the test sub-command.
	testCmd.Flags().StringP(filterFlag, "f", "", "run unit tests matching this regex")
	testCmd.Flags().Duration(timeoutFlag, 20*time.Minute, "timeout for test")
	testCmd.Flags().Bool(showLogsFlag, false, "print logs instead of saving them in files")
	testCmd.Flags().BoolP(vFlag, "v", false, "enable logging during test runs")
	testCmd.Flags().Bool(stressFlag, false, "run tests under stress")
	testCmd.Flags().Bool(raceFlag, false, "run tests using race builds")
	testCmd.Flags().Bool(ignoreCacheFlag, false, "ignore cached test runs")

	// Fuzz testing related flag.
	testCmd.Flags().Bool(fuzzFlag, false, "run fuzz tests")

	// Logic test related flags.
	testCmd.Flags().Bool(logicFlag, false, "run logic tests")
	testCmd.Flags().String(filesFlag, "", "run logic tests for files matching this regex")
	testCmd.Flags().String(subtestsFlag, "", "run logic test subtests matching this regex")
	testCmd.Flags().String(configFlag, "", "run logic tests under the specified config")
}

func runTest(ctx context.Context, cmd *cobra.Command, pkgs []string) error {
	if logicTest := mustGetFlagBool(cmd, logicFlag); logicTest {
		return runLogicTest(cmd)
	}

	if fuzzTest := mustGetFlagBool(cmd, fuzzFlag); fuzzTest {
		return runFuzzTest(cmd, pkgs)
	}

	return runUnitTest(ctx, cmd, pkgs)
}

func runUnitTest(ctx context.Context, cmd *cobra.Command, pkgs []string) error {
	stress := mustGetFlagBool(cmd, stressFlag)
	race := mustGetFlagBool(cmd, raceFlag)
	filter := mustGetFlagString(cmd, filterFlag)
	timeout := mustGetFlagDuration(cmd, timeoutFlag)
	ignoreCache := mustGetFlagBool(cmd, ignoreCacheFlag)
	verbose := mustGetFlagBool(cmd, vFlag)
	showLogs := mustGetFlagBool(cmd, showLogsFlag)

	if showLogs {
		return errors.New("-show-logs unimplemented")
	}

	log.Printf("unit test args: stress=%t  race=%t  filter=%s  timeout=%s  ignore-cache=%t  pkgs=%s",
		stress, race, filter, timeout, ignoreCache, pkgs)

	var args []string
	args = append(args, "test")
	if race {
		args = append(args, "--features", "race")
	}
	args = append(args, "--color=yes")

	for _, pkg := range pkgs {
		if strings.HasSuffix(pkg, "...") {
			args = append(args, fmt.Sprintf("@cockroach//%s", pkg))
		} else {
			components := strings.Split(pkg, "/")
			pkgName := components[len(components)-1]
			args = append(args, fmt.Sprintf("@cockroach//%s:%s_test", pkg, pkgName))
		}
	}

	if ignoreCache {
		args = append(args, "--nocache_test_results")
	}
	if stress {
		// TODO(irfansharif): Should this be pulled into a top-level flag?
		// Should we just re-purpose timeout here?
		args = append(args, "--run_under", fmt.Sprintf("stress -maxtime=%s", timeout))
	}
	if filter != "" {
		args = append(args, fmt.Sprintf("--test_filter=%s", filter))
	}
	if verbose {
		args = append(args, "--test_output", "all", "--test_arg", "-test.v")
	}

	return execute(ctx, "bazel", args...)
}

func runLogicTest(cmd *cobra.Command) error {
	files := mustGetFlagString(cmd, filesFlag)
	subtests := mustGetFlagString(cmd, subtestsFlag)
	config := mustGetFlagString(cmd, configFlag)

	log.Printf("logic test args: files=%s  subtests=%s  config=%s",
		files, subtests, config)
	return errors.New("--logic unimplemented")
}

func runFuzzTest(cmd *cobra.Command, pkgs []string) error {
	filter := mustGetFlagString(cmd, filterFlag)

	log.Printf("fuzz test args: filter=%s  pkgs=%s", filter, pkgs)
	return errors.New("--fuzz unimplemented")
}
