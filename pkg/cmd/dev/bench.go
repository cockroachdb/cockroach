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
	"sort"
	"strings"

	"github.com/spf13/cobra"
)

const (
	benchTimeFlag = "bench-time"
	benchMemFlag  = "bench-mem"
)

// makeBenchCmd constructs the subcommand used to run the specified benchmarks.
func makeBenchCmd(runE func(cmd *cobra.Command, args []string) error) *cobra.Command {
	benchCmd := &cobra.Command{
		Use:   "bench [pkg...]",
		Short: `Run the specified benchmarks`,
		Long:  `Run the specified benchmarks.`,
		Example: `
	dev bench pkg/sql/parser --filter=BenchmarkParse
	dev bench pkg/bench -f='BenchmarkTracing/1node/scan/trace=off' --count=2 --bench-time=10x --bench-mem`,
		Args: cobra.MinimumNArgs(0),
		RunE: runE,
	}
	addCommonBuildFlags(benchCmd)
	addCommonTestFlags(benchCmd)

	benchCmd.Flags().BoolP(vFlag, "v", false, "show benchmark process output")
	benchCmd.Flags().BoolP(showLogsFlag, "", false, "show crdb logs in-line")
	benchCmd.Flags().Int(countFlag, 1, "run benchmark n times")
	// We use a string flag for benchtime instead of a duration; the go test
	// runner accepts input of the form "Nx" to run the benchmark N times (see
	// `go help testflag`).
	benchCmd.Flags().String(benchTimeFlag, "", "duration to run each benchmark for")
	benchCmd.Flags().Bool(benchMemFlag, false, "print memory allocations for benchmarks")

	return benchCmd
}

func (d *dev) bench(cmd *cobra.Command, commandLine []string) error {
	pkgs, additionalBazelArgs := splitArgsAtDash(cmd, commandLine)
	ctx := cmd.Context()
	var (
		filter    = mustGetFlagString(cmd, filterFlag)
		timeout   = mustGetFlagDuration(cmd, timeoutFlag)
		short     = mustGetFlagBool(cmd, shortFlag)
		showLogs  = mustGetFlagBool(cmd, showLogsFlag)
		verbose   = mustGetFlagBool(cmd, vFlag)
		count     = mustGetFlagInt(cmd, countFlag)
		benchTime = mustGetFlagString(cmd, benchTimeFlag)
		benchMem  = mustGetFlagBool(cmd, benchMemFlag)
	)

	// Enumerate all benches to run.
	if len(pkgs) == 0 {
		// Empty `dev bench` does the same thing as `dev bench pkg/...`
		pkgs = append(pkgs, "pkg/...")
	}
	benchesMap := make(map[string]bool)
	for _, pkg := range pkgs {
		dir, isRecursive, tag, err := d.parsePkg(pkg)
		if err != nil {
			return err
		}
		if isRecursive {
			// Use `git grep` to find all Go files that contain benchmark tests.
			out, err := d.exec.CommandContextSilent(ctx, "git", "grep", "-l", "^func Benchmark", "--", dir+"/*_test.go")
			if err != nil {
				return err
			}
			files := strings.Split(strings.TrimSpace(string(out)), "\n")
			for _, file := range files {
				dir, _ = filepath.Split(file)
				dir = strings.TrimSuffix(dir, "/")
				benchesMap[dir] = true
			}
		} else if tag != "" {
			return fmt.Errorf("malformed package %q, tags not supported in 'bench' command", pkg)
		} else {
			benchesMap[dir] = true
		}
	}
	// De-duplicate and sort the list of benches to run.
	var benches []string
	for pkg := range benchesMap {
		benches = append(benches, pkg)
	}
	sort.Slice(benches, func(i, j int) bool { return benches[i] < benches[j] })

	var argsBase []string
	// NOTE the --config=test here. It's very important we compile the test binary with the
	// appropriate stuff (gotags, etc.)
	argsBase = append(argsBase, "run", "--config=test", "--test_sharding_strategy=disabled")
	argsBase = append(argsBase, mustGetRemoteCacheArgs(remoteCacheAddr)...)
	if numCPUs != 0 {
		argsBase = append(argsBase, fmt.Sprintf("--local_cpu_resources=%d", numCPUs))
	}
	if verbose {
		argsBase = append(argsBase, "--test_arg", "-test.v")
	}
	if showLogs {
		argsBase = append(argsBase, "--test_arg", "-show-logs")
	}
	if count != 1 {
		argsBase = append(argsBase, "--test_arg", fmt.Sprintf("-test.count=%d", count))
	}
	if benchTime != "" {
		argsBase = append(argsBase, "--test_arg", fmt.Sprintf("-test.benchtime=%s", benchTime))
	}
	if benchMem {
		argsBase = append(argsBase, "--test_arg", "-test.benchmem")
	}

	for _, bench := range benches {
		args := make([]string, len(argsBase))
		copy(args, argsBase)
		base := filepath.Base(bench)
		target := "//" + bench + ":" + base + "_test"
		args = append(args, target)
		args = append(args, additionalBazelArgs...)
		args = append(args, "--", "-test.run=-")
		if filter == "" {
			args = append(args, "-test.bench=.")
		} else {
			args = append(args, "-test.bench="+filter)
		}
		if timeout > 0 {
			args = append(args, fmt.Sprintf("-test.timeout=%s", timeout.String()))
		}
		if short {
			args = append(args, "-test.short", "-test.benchtime=1ns")
		}
		logCommand("bazel", args...)
		err := d.exec.CommandContextInheritingStdStreams(ctx, "bazel", args...)
		if err != nil {
			return err
		}
	}

	return nil
}
