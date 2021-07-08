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
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
)

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

func makeTestCmd(runE func(cmd *cobra.Command, args []string) error) *cobra.Command {
	// testCmd runs the specified cockroachdb tests.
	testCmd := &cobra.Command{
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
		RunE: runE,
	}
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
	return testCmd
}

// TODO(irfansharif): Add tests for the various bazel commands that get
// generated from the set of provided user flags.

func (d *dev) test(cmd *cobra.Command, pkgs []string) error {
	if logicTest := mustGetFlagBool(cmd, logicFlag); logicTest {
		return d.runLogicTest(cmd)
	}

	if fuzzTest := mustGetFlagBool(cmd, fuzzFlag); fuzzTest {
		return d.runFuzzTest(cmd, pkgs)
	}

	return d.runUnitTest(cmd, pkgs)
}

func (d *dev) runUnitTest(cmd *cobra.Command, pkgs []string) error {
	ctx := cmd.Context()
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

	d.log.Printf("unit test args: stress=%t  race=%t  filter=%s  timeout=%s  ignore-cache=%t  pkgs=%s",
		stress, race, filter, timeout, ignoreCache, pkgs)

	var args []string
	args = append(args, "test")
	args = append(args, "--color=yes")
	args = append(args, "--experimental_convenience_symlinks=ignore") // don't generate any convenience symlinks
	args = append(args, mustGetRemoteCacheArgs(remoteCacheAddr)...)
	if numCPUs != 0 {
		args = append(args, fmt.Sprintf("--local_cpu_resources=%d", numCPUs))
	}
	if race {
		args = append(args, "--features", "race")
	}

	for _, pkg := range pkgs {
		if !strings.HasPrefix(pkg, "pkg/") {
			return errors.Newf("malformed package %q, expecting %q", pkg, "pkg/{...}")
		}

		if strings.HasSuffix(pkg, "...") {
			// Similar to `go test`, we implement `...` expansion to allow
			// callers to use the following pattern to test all packages under a
			// named one:
			//
			//     dev test pkg/util/... -v
			//
			// NB: We'll want to filter for just the go_test targets here. Not
			// doing so prompts bazel to try and build all named targets. This
			// is undesirable for the various `*_proto` targets seeing as how
			// they're not buildable in isolation. This is because we often
			// attach methods to proto types in hand-written files, files that
			// are not picked up by the proto bazel targets[1]. Regular bazel
			// compilation is still fine seeing as how the top-level go_library
			// targets both embeds the proto target, and sources the
			// hand-written file. But the proto target in isolation may not be
			// buildable because without those additional methods, those types
			// may fail to satisfy required interfaces.
			//
			// So, blinding selecting for all targets won't work, and we'll want
			// to filter things out first.
			//
			// [1]: pkg/rpc/heartbeat.proto is one example of this pattern,
			// where we define `Stringer` separately for the `RemoteOffset`
			// type.
			{
				out, err := d.exec.CommandContext(ctx, "bazel", "query", fmt.Sprintf("kind(go_test,  //%s)", pkg))
				if err != nil {
					return err
				}
				targets := strings.TrimSpace(string(out))
				args = append(args, strings.Split(targets, "\n")...)
			}
		} else {
			components := strings.Split(pkg, "/")
			pkgName := components[len(components)-1]
			args = append(args, fmt.Sprintf("//%s:%s_test", pkg, pkgName))
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
	} else {
		args = append(args, "--test_output", "errors")
	}

	_, err := d.exec.CommandContext(ctx, "bazel", args...)
	return err
}

func (d *dev) runLogicTest(cmd *cobra.Command) error {
	files := mustGetFlagString(cmd, filesFlag)
	subtests := mustGetFlagString(cmd, subtestsFlag)
	config := mustGetFlagString(cmd, configFlag)

	d.log.Printf("logic test args: files=%s  subtests=%s  config=%s",
		files, subtests, config)
	return errors.New("--logic unimplemented")
}

func (d *dev) runFuzzTest(cmd *cobra.Command, pkgs []string) error {
	filter := mustGetFlagString(cmd, filterFlag)

	d.log.Printf("fuzz test args: filter=%s  pkgs=%s", filter, pkgs)
	return errors.New("--fuzz unimplemented")
}
