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

const (
	stressTarget = "@com_github_cockroachdb_stress//:stress"

	// General testing flags.
	vFlag           = "verbose"
	stressFlag      = "stress"
	stressArgsFlag  = "stress-args"
	raceFlag        = "race"
	ignoreCacheFlag = "ignore-cache"

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
	dev test pkg/kv/kvserver --filter=TestReplicaGC* -v --timeout=1m
	dev test --stress --race ...
	dev test --logic --files=prepare|fk --subtests=20042 --config=local`,
		Args: cobra.MinimumNArgs(0),
		RunE: runE,
	}
	// Attach flags for the test sub-command.
	addCommonTestFlags(testCmd)
	testCmd.Flags().BoolP(vFlag, "v", false, "enable logging during test runs")
	testCmd.Flags().Bool(stressFlag, false, "run tests under stress")
	testCmd.Flags().String(stressArgsFlag, "", "Additional arguments to pass to stress")
	testCmd.Flags().Bool(raceFlag, false, "run tests using race builds")
	testCmd.Flags().Bool(ignoreCacheFlag, false, "ignore cached test runs")

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

	return d.runUnitTest(cmd, pkgs)
}

func (d *dev) runUnitTest(cmd *cobra.Command, pkgs []string) error {
	ctx := cmd.Context()
	stress := mustGetFlagBool(cmd, stressFlag)
	stressArgs := mustGetFlagString(cmd, stressArgsFlag)
	race := mustGetFlagBool(cmd, raceFlag)
	filter := mustGetFlagString(cmd, filterFlag)
	timeout := mustGetFlagDuration(cmd, timeoutFlag)
	short := mustGetFlagBool(cmd, shortFlag)
	ignoreCache := mustGetFlagBool(cmd, ignoreCacheFlag)
	verbose := mustGetFlagBool(cmd, vFlag)

	d.log.Printf("unit test args: stress=%t  race=%t  filter=%s  timeout=%s  ignore-cache=%t  pkgs=%s",
		stress, race, filter, timeout, ignoreCache, pkgs)

	var args []string
	args = append(args, "test")
	args = append(args, "--color=yes")
	args = append(args, "--experimental_convenience_symlinks=ignore")
	args = append(args, getConfigFlags()...)
	args = append(args, mustGetRemoteCacheArgs(remoteCacheAddr)...)
	if numCPUs != 0 {
		args = append(args, fmt.Sprintf("--local_cpu_resources=%d", numCPUs))
	}
	if race {
		args = append(args, "--config=race")
	}

	for _, pkg := range pkgs {
		pkg = strings.TrimPrefix(pkg, "//")
		pkg = strings.TrimRight(pkg, "/")

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
				out, err := d.exec.CommandContextSilent(ctx, "bazel", "query", fmt.Sprintf("kind(go_test,  //%s)", pkg))
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
	if stress && timeout > 0 {
		args = append(args, "--run_under", fmt.Sprintf("%s -maxtime=%s %s", stressTarget, timeout, stressArgs))
		// The timeout should be a bit higher than the stress duration.
		// Bazel will probably think the timeout for this test isn't so
		// long.
		args = append(args, fmt.Sprintf("--test_timeout=%d", int((timeout+1*time.Second).Seconds())))
	} else if stress {
		args = append(args, "--run_under", fmt.Sprintf("%s %s", stressTarget, stressArgs))
	} else if timeout > 0 {
		args = append(args, fmt.Sprintf("--test_timeout=%d", int(timeout.Seconds())))
	}
	if filter != "" {
		args = append(args, fmt.Sprintf("--test_filter=%s", filter))
	}
	if short {
		args = append(args, "--test_arg", "-test.short")
	}
	if verbose {
		args = append(args, "--test_output", "all", "--test_arg", "-test.v")
	} else {
		args = append(args, "--test_output", "errors")
	}

	return d.exec.CommandContextInheritingStdStreams(ctx, "bazel", args...)
}

func (d *dev) runLogicTest(cmd *cobra.Command) error {
	files := mustGetFlagString(cmd, filesFlag)
	subtests := mustGetFlagString(cmd, subtestsFlag)
	config := mustGetFlagString(cmd, configFlag)

	d.log.Printf("logic test args: files=%s  subtests=%s  config=%s",
		files, subtests, config)
	return errors.New("--logic unimplemented")
}
