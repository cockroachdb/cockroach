// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"
)

const (
	benchTimeFlag           = "bench-time"
	benchMemFlag            = "bench-mem"
	runSepProcessTenantFlag = "run-sep-process-tenant"
)

// makeBenchCmd constructs the subcommand used to run the specified benchmarks.
func makeBenchCmd(runE func(cmd *cobra.Command, args []string) error) *cobra.Command {
	benchCmd := &cobra.Command{
		Use:   "bench [pkg...]",
		Short: `Run the specified benchmarks`,
		Long: `Run the specified benchmarks.

Note that by default we explicitly restrict the benchmark to running on a single core (i.e., GOMAXPROCS=1).
This behavior can be overridden with --test-args='-test.cpu N'`,
		Example: `
	dev bench pkg/sql/parser --filter=BenchmarkParse
	dev bench pkg/bench -f='BenchmarkTracing/1node/scan/trace=off' --count=2 --bench-time=10x
	dev bench pkg/bench -f='BenchmarkTracing/1node/scan/trace=off' --ignore-cache --test-args='-test.cpuprofile=cpu.out -test.memprofile=mem.out' --bench-mem=false`,
		Args: cobra.MinimumNArgs(0),
		RunE: runE,
	}
	addCommonBuildFlags(benchCmd)
	addCommonTestFlags(benchCmd)

	benchCmd.Flags().BoolP(vFlag, "v", false, "show benchmark process output")
	benchCmd.Flags().BoolP(showLogsFlag, "", false, "show crdb logs in-line")
	benchCmd.Flags().Int(countFlag, 1, "run benchmark n times")
	benchCmd.Flags().Bool(ignoreCacheFlag, true, "ignore cached benchmark runs")
	// We use a string flag for benchtime instead of a duration; the go test
	// runner accepts input of the form "Nx" to run the benchmark N times (see
	// `go help testflag`).
	benchCmd.Flags().String(benchTimeFlag, "", "duration to run each benchmark for")
	benchCmd.Flags().Bool(benchMemFlag, true, "print memory allocations for benchmarks")
	benchCmd.Flags().Bool(streamOutputFlag, true, "stream bench output during run")
	benchCmd.Flags().String(testArgsFlag, "", "additional arguments to pass to go test binary")
	benchCmd.Flags().Bool(runSepProcessTenantFlag, false, "run separate process tenant benchmarks (these may freeze due to tenant limits)")

	return benchCmd
}

func (d *dev) bench(cmd *cobra.Command, commandLine []string) error {
	pkgs, additionalBazelArgs := splitArgsAtDash(cmd, commandLine)
	ctx := cmd.Context()
	var (
		filter              = mustGetFlagString(cmd, filterFlag)
		ignoreCache         = mustGetFlagBool(cmd, ignoreCacheFlag)
		timeout             = mustGetFlagDuration(cmd, timeoutFlag)
		short               = mustGetFlagBool(cmd, shortFlag)
		showLogs            = mustGetFlagBool(cmd, showLogsFlag)
		verbose             = mustGetFlagBool(cmd, vFlag)
		count               = mustGetFlagInt(cmd, countFlag)
		benchTime           = mustGetFlagString(cmd, benchTimeFlag)
		benchMem            = mustGetFlagBool(cmd, benchMemFlag)
		streamOutput        = mustGetFlagBool(cmd, streamOutputFlag)
		testArgs            = mustGetFlagString(cmd, testArgsFlag)
		runSepProcessTenant = mustGetFlagBool(cmd, runSepProcessTenantFlag)
	)

	// Enumerate all benches to run.
	if len(pkgs) == 0 {
		// Empty `dev bench` does the same thing as `dev bench pkg/...`
		pkgs = append(pkgs, "pkg/...")
	}

	var args []string
	args = append(args, "test")
	addCommonBazelArguments(&args)
	if timeout > 0 {
		args = append(args, fmt.Sprintf("--test_timeout=%d", int(timeout.Seconds())))
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

	args = append(args, "--test_arg", "-test.run=-")
	if filter == "" {
		args = append(args, "--test_arg", "-test.bench=.")
	} else {
		args = append(args, "--test_arg", fmt.Sprintf("-test.bench=%s", filter))
	}
	args = append(args, "--test_sharding_strategy=disabled")
	args = append(args, "--test_arg", "-test.cpu", "--test_arg", "1")
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
	if benchTime != "" {
		args = append(args, "--test_arg", fmt.Sprintf("-test.benchtime=%s", benchTime))
	}
	if benchMem {
		args = append(args, "--test_arg", "-test.benchmem")
	}
	if runSepProcessTenant {
		args = append(args, "--test_arg", "-run-sep-process-tenant")
	}
	args = append(args, "--crdb_test_off")
	if testArgs != "" {
		goTestArgs, err := d.getGoTestArgs(ctx, testArgs)
		if err != nil {
			return err
		}
		args = append(args, goTestArgs...)
	}
	args = append(args, d.getGoTestEnvArgs()...)
	args = append(args, d.getTestOutputArgs(verbose, showLogs, streamOutput)...)
	args = append(args, additionalBazelArgs...)
	logCommand("bazel", args...)
	return d.exec.CommandContextInheritingStdStreams(ctx, "bazel", args...)
}

func (d *dev) getGoTestEnvArgs() []string {
	var goTestEnv []string
	// Make the `$HOME/.cache/crdb-test-fixtures` directory available for reusable
	// test fixtures, if available. See testfixtures.ReuseOrGenerate().
	if cacheDir, err := d.os.UserCacheDir(); err == nil {
		dir := filepath.Join(cacheDir, "crdb-test-fixtures")
		if err := os.MkdirAll(dir, 0755); err == nil {
			goTestEnv = append(goTestEnv, "--test_env", fmt.Sprintf("COCKROACH_TEST_FIXTURES_DIR=%s", dir))
			goTestEnv = append(goTestEnv, fmt.Sprintf("--sandbox_writable_path=%s", dir))
		}
	}
	return goTestEnv
}
