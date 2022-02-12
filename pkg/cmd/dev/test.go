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
	"path/filepath"
	"strings"
	"time"

	"github.com/google/shlex"
	"github.com/spf13/cobra"
)

const (
	stressTarget = "@com_github_cockroachdb_stress//:stress"

	// General testing flags.
	countFlag       = "count"
	vFlag           = "verbose"
	showLogsFlag    = "show-logs"
	stressFlag      = "stress"
	stressArgsFlag  = "stress-args"
	raceFlag        = "race"
	ignoreCacheFlag = "ignore-cache"
	rewriteFlag     = "rewrite"
	testArgsFlag    = "test-args"
	vModuleFlag     = "vmodule"
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
	dev test pkg/server -f=TestSpanStatsResponse -v --count=5 --vmodule='raft=1'
	dev test --stress --race ...`,
		Args: cobra.MinimumNArgs(0),
		RunE: runE,
	}
	// Attach flags for the test sub-command.
	addCommonBuildFlags(testCmd)
	addCommonTestFlags(testCmd)

	// Go's test runner runs tests in sub-processes; the stderr/stdout data from
	// the test process is first swallowed by go test and then only
	// conditionally released to the invoking user depending on flags passed to
	// `go test`. The `-v` switch controls whether `go test` shows the test
	// process' output (which test is being run, how long it took, etc.) always,
	// or only on failures. `--show-logs` by contrast is a flag for the process
	// under test, controlling whether the process-internal logs are made
	// visible.
	testCmd.Flags().BoolP(vFlag, "v", false, "show testing process output")
	testCmd.Flags().Int(countFlag, 1, "run test the given number of times")
	testCmd.Flags().BoolP(showLogsFlag, "", false, "show crdb logs in-line")
	testCmd.Flags().Bool(stressFlag, false, "run tests under stress")
	testCmd.Flags().String(stressArgsFlag, "", "additional arguments to pass to stress")
	testCmd.Flags().Bool(raceFlag, false, "run tests using race builds")
	testCmd.Flags().Bool(ignoreCacheFlag, false, "ignore cached test runs")
	testCmd.Flags().String(rewriteFlag, "", "argument to pass to underlying test binary (only applicable to certain tests)")
	testCmd.Flags().String(testArgsFlag, "", "additional arguments to pass to go test binary")
	testCmd.Flags().Lookup(rewriteFlag).NoOptDefVal = "-rewrite"
	testCmd.Flags().String(vModuleFlag, "", "comma-separated list of pattern=N settings for file-filtered logging")
	return testCmd
}

func (d *dev) test(cmd *cobra.Command, commandLine []string) error {
	pkgs, additionalBazelArgs := splitArgsAtDash(cmd, commandLine)
	ctx := cmd.Context()
	var (
		filter        = mustGetFlagString(cmd, filterFlag)
		ignoreCache   = mustGetFlagBool(cmd, ignoreCacheFlag)
		race          = mustGetFlagBool(cmd, raceFlag)
		rewrite       = mustGetFlagString(cmd, rewriteFlag)
		testArgs      = mustGetFlagString(cmd, testArgsFlag)
		short         = mustGetFlagBool(cmd, shortFlag)
		stress        = mustGetFlagBool(cmd, stressFlag)
		stressCmdArgs = mustGetFlagString(cmd, stressArgsFlag)
		timeout       = mustGetFlagDuration(cmd, timeoutFlag)
		verbose       = mustGetFlagBool(cmd, vFlag)
		showLogs      = mustGetFlagBool(cmd, showLogsFlag)
		count         = mustGetFlagInt(cmd, countFlag)
		vModule       = mustGetFlagString(cmd, vModuleFlag)
	)

	// Enumerate all tests to run.
	if len(pkgs) == 0 {
		// Empty `dev test` does the same thing as `dev test pkg/...`
		pkgs = append(pkgs, "pkg/...")
	}

	var args []string
	args = append(args, "test")
	if numCPUs != 0 {
		args = append(args, fmt.Sprintf("--local_cpu_resources=%d", numCPUs))
	}
	if race {
		args = append(args, "--config=race")
	} else if stress {
		args = append(args, "--test_sharding_strategy=disabled")
	}

	var testTargets []string
	for _, pkg := range pkgs {
		pkg = strings.TrimPrefix(pkg, "//")
		pkg = strings.TrimPrefix(pkg, "./")
		pkg = strings.TrimRight(pkg, "/")

		if !strings.HasPrefix(pkg, "pkg/") {
			return fmt.Errorf("malformed package %q, expecting %q", pkg, "pkg/{...}")
		}

		var target string
		if strings.Contains(pkg, ":") {
			// For parity with bazel, we allow specifying named build targets.
			target = pkg
		} else {
			target = fmt.Sprintf("%s:all", pkg)
		}
		testTargets = append(testTargets, target)
	}

	args = append(args, testTargets...)
	if ignoreCache {
		args = append(args, "--nocache_test_results")
	}
	args = append(args, "--test_env=GOTRACEBACK=all")
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
		for _, testTarget := range testTargets {
			dir := getDirectoryFromTarget(testTarget)
			args = append(args, fmt.Sprintf("--sandbox_writable_path=%s", filepath.Join(workspace, dir)))
		}
	}
	if timeout > 0 && !stress {
		args = append(args, fmt.Sprintf("--test_timeout=%d", int(timeout.Seconds())))

		// If stress is specified, we'll pad the timeout below.
	}

	if stress {
		args = append(args, d.getStressArgs(stressCmdArgs, timeout)...)
	}

	if filter != "" {
		args = append(args, fmt.Sprintf("--test_filter=%s", filter))
		// For sharded test packages, it doesn't make much sense to spawn multiple
		// test processes that don't end up running anything. Default to running
		// things in a single process if a filter is specified.
		args = append(args, "--test_sharding_strategy=disabled")
	}
	if short {
		args = append(args, "--test_arg", "-test.short")
	}
	if verbose {
		args = append(args, "--test_arg", "-test.v")
	}
	if showLogs {
		args = append(args, "--test_arg", "-show-logs")
	}
	if count != 1 {
		args = append(args, "--test_arg", fmt.Sprintf("-test.count=%d", count))
	}
	if vModule != "" {
		args = append(args, "--test_arg", fmt.Sprintf("-vmodule=%s", vModule))
	}
	if testArgs != "" {
		goTestArgs, err := d.getGoTestArgs(ctx, testArgs)
		if err != nil {
			return err
		}
		args = append(args, goTestArgs...)
	}

	args = append(args, d.getTestOutputArgs(stress, verbose, showLogs)...)
	args = append(args, additionalBazelArgs...)

	logCommand("bazel", args...)
	return d.exec.CommandContextInheritingStdStreams(ctx, "bazel", args...)

	// TODO(irfansharif): Both here and in `dev bench`, if the command is
	// unsuccessful we could explicitly check for "missing package" errors. The
	// situation is not so bad currently however:
	//
	//   [...] while parsing 'pkg/f:all': no such package 'pkg/f'
}

func (d *dev) getGoTestArgs(ctx context.Context, testArgs string) ([]string, error) {
	var goTestArgs []string
	if testArgs != "" {
		// The test output directory defaults to wherever "go test" is running,
		// which for us is somewhere in the sandbox. When passing arguments for
		// go test (-{mem,cpu}profile, -trace for e.g.), we may be interested in
		// output files. For that reason, configure it to write (if anything) to the
		// workspace by default.
		//
		// TODO(irfansharif): We could also elevate the go test flags that do write
		// output into top-level dev flags (like --rewrite) and only selectively
		// configure -test.outputdir and the sandbox writable path.
		workspace, err := d.getWorkspace(ctx)
		if err != nil {
			return nil, err
		}
		goTestArgs = append(goTestArgs,
			"--test_arg", fmt.Sprintf("-test.outputdir=%s", workspace),
			fmt.Sprintf("--sandbox_writable_path=%s", filepath.Join(workspace)),
		)

		parts, err := shlex.Split(testArgs)
		if err != nil {
			return nil, err
		}
		for _, part := range parts {
			goTestArgs = append(goTestArgs, "--test_arg", part)
		}
	}
	return goTestArgs, nil
}

func (d *dev) getTestOutputArgs(stress, verbose, showLogs bool) []string {
	testOutputArgs := []string{"--test_output", "errors"}
	if stress {
		// Stream the output to continually observe the number of successful
		// test iterations.
		testOutputArgs = []string{"--test_output", "streamed"}
	} else if verbose || showLogs {
		testOutputArgs = []string{"--test_output", "all"}
	}
	return testOutputArgs
}

func (d *dev) getStressArgs(stressCmdArg string, timeout time.Duration) []string {
	var stressArgs, stressCmdArgs []string
	if timeout > 0 {
		stressCmdArgs = append(stressCmdArgs, fmt.Sprintf("-maxtime=%s", timeout))
		// The bazel timeout should be higher than the stress duration, lets
		// generously give it an extra minute.
		stressArgs = append(stressArgs, fmt.Sprintf("--test_timeout=%d", int((timeout+time.Minute).Seconds())))
	} else {
		// We're running under stress and no timeout is specified. We want
		// to respect the timeout passed down to stress[1]. Similar to above
		// we want the bazel timeout to be longer, so lets just set it to
		// 24h.
		//
		// [1]: Through --stress-arg=-maxtime or if nothing is specified,
		//      -maxtime=0 is taken as "run forever".
		stressArgs = append(stressArgs, fmt.Sprintf("--test_timeout=%.0f", 24*time.Hour.Seconds()))
	}
	if numCPUs > 0 {
		stressCmdArgs = append(stressCmdArgs, fmt.Sprintf("-p=%d", numCPUs))
	}
	if stressCmdArg != "" {
		stressCmdArgs = append(stressCmdArgs, stressCmdArg)
	}
	stressArgs = append(stressArgs, "--run_under",
		// NB: Run with -bazel, which propagates `TEST_TMPDIR` to `TMPDIR`,
		// and -shardable-artifacts set such that we can merge the XML output
		// files.
		fmt.Sprintf("%s -bazel -shardable-artifacts 'XML_OUTPUT_FILE=%s merge-test-xmls' %s", stressTarget, d.getDevBin(), strings.Join(stressCmdArgs, " ")))
	return stressArgs
}

func getDirectoryFromTarget(target string) string {
	target = strings.TrimPrefix(target, "//")
	colon := strings.LastIndex(target, ":")
	if colon < 0 {
		return target
	}
	return target[:colon]
}
