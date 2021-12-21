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
	"errors"
	"fmt"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/alessio/shellescape"
	"github.com/spf13/cobra"
)

const (
	stressTarget = "@com_github_cockroachdb_stress//:stress"

	// General testing flags.
	vFlag           = "verbose"
	showLogsFlag    = "show-logs"
	stressFlag      = "stress"
	stressArgsFlag  = "stress-args"
	raceFlag        = "race"
	ignoreCacheFlag = "ignore-cache"
	rewriteFlag     = "rewrite"
	rewriteArgFlag  = "rewrite-arg"
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
	testCmd.Flags().BoolP(showLogsFlag, "", false, "show crdb logs in-line")
	testCmd.Flags().Bool(stressFlag, false, "run tests under stress")
	testCmd.Flags().String(stressArgsFlag, "", "additional arguments to pass to stress")
	testCmd.Flags().Bool(raceFlag, false, "run tests using race builds")
	testCmd.Flags().Bool(ignoreCacheFlag, false, "ignore cached test runs")
	testCmd.Flags().String(rewriteFlag, "", "argument to pass to underlying (only applicable for certain tests, e.g. logic and datadriven tests). If unspecified, -rewrite will be passed to the test binary.")
	testCmd.Flags().String(rewriteArgFlag, "", "additional argument to pass to -rewrite (implies --rewrite)")
	testCmd.Flags().Lookup(rewriteFlag).NoOptDefVal = "-rewrite"
	return testCmd
}

// TODO(irfansharif): Add tests for the various bazel commands that get
// generated from the set of provided user flags.

func (d *dev) test(cmd *cobra.Command, commandLine []string) error {
	pkgs, additionalBazelArgs := splitArgsAtDash(cmd, commandLine)
	ctx := cmd.Context()
	var (
		filter      = mustGetFlagString(cmd, filterFlag)
		ignoreCache = mustGetFlagBool(cmd, ignoreCacheFlag)
		race        = mustGetFlagBool(cmd, raceFlag)
		rewrite     = mustGetFlagString(cmd, rewriteFlag)
		rewriteArg  = mustGetFlagString(cmd, rewriteArgFlag)
		short       = mustGetFlagBool(cmd, shortFlag)
		stress      = mustGetFlagBool(cmd, stressFlag)
		stressArgs  = mustGetFlagString(cmd, stressArgsFlag)
		timeout     = mustGetFlagDuration(cmd, timeoutFlag)
		verbose     = mustGetFlagBool(cmd, vFlag)
		showLogs    = mustGetFlagBool(cmd, showLogsFlag)
	)
	if rewriteArg != "" && rewrite == "" {
		rewrite = "-rewrite"
	}

	var args []string
	args = append(args, "test")
	args = append(args, mustGetRemoteCacheArgs(remoteCacheAddr)...)
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

		if strings.HasSuffix(pkg, "/...") {
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
				queryArgs := []string{fmt.Sprintf("kind(go_test,  //%s)", pkg)}
				out, err := d.getQueryOutput(ctx, queryArgs...)
				if err != nil {
					return err
				}
				targets := strings.Split(strings.TrimSpace(string(out)), "\n")
				testTargets = append(testTargets, targets...)
			}
		} else if strings.Contains(pkg, ":") {
			testTargets = append(testTargets, pkg)
		} else {
			queryArgs := []string{fmt.Sprintf("kind(go_test, //%s:all)", pkg)}
			out, err := d.getQueryOutput(ctx, queryArgs...)
			if err != nil {
				return err
			}
			tests := strings.Split(strings.TrimSpace(string(out)), "\n")
			testTargets = append(testTargets, tests...)
		}
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
		if rewriteArg != "" {
			args = append(args, "--test_arg", rewriteArg)
		}
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
		var stressCmdArgs []string
		if timeout > 0 {
			stressCmdArgs = append(stressCmdArgs, fmt.Sprintf("-maxtime=%s", timeout))
			// The bazel timeout should be higher than the stress duration, lets
			// generously give it an extra minute.
			args = append(args, fmt.Sprintf("--test_timeout=%d", int((timeout+time.Minute).Seconds())))
		} else {
			// We're running under stress and no timeout is specified. We want
			// to respect the timeout passed down to stress[1]. Similar to above
			// we want the bazel timeout to be longer, so lets just set it to
			// 24h.
			//
			// [1]: Through --stress-arg=-maxtime or if nothing is specified,
			//      -maxtime=0 is taken as "run forever".
			args = append(args, fmt.Sprintf("--test_timeout=%.0f", 24*time.Hour.Seconds()))
		}
		if numCPUs > 0 {
			stressCmdArgs = append(stressCmdArgs, fmt.Sprintf("-p=%d", numCPUs))
		}
		stressCmdArgs = append(stressCmdArgs, stressArgs)
		args = append(args, "--run_under",
			fmt.Sprintf("%s %s", stressTarget, strings.Join(stressCmdArgs, " ")))
	}

	if filter != "" {
		args = append(args, fmt.Sprintf("--test_filter=%s", filter))
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

	{ // Handle test output flags.
		testOutputArgs := []string{"--test_output", "errors"}
		if stress {
			// Stream the output to continually observe the number of successful
			// test iterations.
			testOutputArgs = []string{"--test_output", "streamed"}
		} else if verbose || showLogs {
			testOutputArgs = []string{"--test_output", "all"}
		}
		args = append(args, testOutputArgs...)
	}

	args = append(args, additionalBazelArgs...)

	logCommand("bazel", args...)
	return d.exec.CommandContextInheritingStdStreams(ctx, "bazel", args...)
}

func getDirectoryFromTarget(target string) string {
	target = strings.TrimPrefix(target, "//")
	colon := strings.LastIndex(target, ":")
	if colon < 0 {
		return target
	}
	return target[:colon]
}

// getQueryOutput runs `bazel query` w/ the given arguments, but returns
// a more informative error if the query fails.
func (d *dev) getQueryOutput(ctx context.Context, args ...string) ([]byte, error) {
	queryArgs := []string{"query"}
	queryArgs = append(queryArgs, args...)
	stdoutBytes, err := d.exec.CommandContextSilent(ctx, "bazel", queryArgs...)
	if err == nil {
		return stdoutBytes, err
	}
	var cmderr *exec.ExitError
	var stdout, stderr string
	if len(stdoutBytes) > 0 {
		stdout = fmt.Sprintf("stdout: \"%s\" ", string(stdoutBytes))
	}
	if errors.As(err, &cmderr) && len(cmderr.Stderr) > 0 {
		stderr = fmt.Sprintf("stderr: \"%s\" ", strings.TrimSpace(string(cmderr.Stderr)))
	}
	return nil, fmt.Errorf("failed to run `bazel %s` %s%s(%w)",
		shellescape.QuoteCommand(queryArgs), stdout, stderr, err)

}
