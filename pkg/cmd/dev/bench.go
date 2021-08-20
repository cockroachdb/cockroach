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

	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
)

// makeBenchCmd constructs the subcommand used to run the specified benchmarks.
func makeBenchCmd(runE func(cmd *cobra.Command, args []string) error) *cobra.Command {
	benchCmd := &cobra.Command{
		Use:   "bench [pkg...]",
		Short: `Run the specified benchmarks`,
		Long:  `Run the specified benchmarks.`,
		Example: `
	dev bench
        dev bench pkg/sql/parser
        dev bench pkg/sql/parser/...
        dev bench pkg/sql/parser --filter=BenchmarkParse`,
		Args: cobra.MinimumNArgs(0),
		RunE: runE,
	}
	addCommonTestFlags(benchCmd)
	return benchCmd
}

func (d *dev) bench(cmd *cobra.Command, pkgs []string) error {
	ctx := cmd.Context()
	filter := mustGetFlagString(cmd, filterFlag)
	timeout := mustGetFlagDuration(cmd, timeoutFlag)
	short := mustGetFlagBool(cmd, shortFlag)

	// Enumerate all benches to run.
	if len(pkgs) == 0 {
		// Empty `dev bench` does the same thing as `dev bench pkg/...`
		pkgs = append(pkgs, "pkg/...")
	}
	benchesMap := make(map[string]bool)
	for _, pkg := range pkgs {
		pkg = strings.TrimPrefix(pkg, "//")
		pkg = strings.TrimRight(pkg, "/")

		if !strings.HasPrefix(pkg, "pkg/") {
			return errors.Newf("malformed package %q, expecting %q", pkg, "pkg/{...}")
		}

		if strings.HasSuffix(pkg, "/...") {
			pkg = strings.TrimSuffix(pkg, "/...")
			// Use `git grep` to find all Go files that contain benchmark tests.
			out, err := d.exec.CommandContextSilent(ctx, "git", "grep", "-l", "^func Benchmark", "--", pkg+"/*_test.go")
			if err != nil {
				return err
			}
			files := strings.Split(strings.TrimSpace(string(out)), "\n")
			for _, file := range files {
				dir, _ := filepath.Split(file)
				dir = strings.TrimSuffix(dir, "/")
				benchesMap[dir] = true
			}
		} else {
			benchesMap[pkg] = true
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
	argsBase = append(argsBase,
		"run",
		"--color=yes",
		"--experimental_convenience_symlinks=ignore",
		"--config=test",
		"--test_sharding_strategy=disabled")
	argsBase = append(argsBase, getConfigFlags()...)
	argsBase = append(argsBase, mustGetRemoteCacheArgs(remoteCacheAddr)...)
	if numCPUs != 0 {
		argsBase = append(argsBase, fmt.Sprintf("--local_cpu_resources=%d", numCPUs))
	}

	for _, bench := range benches {
		args := make([]string, len(argsBase))
		copy(args, argsBase)
		base := filepath.Base(bench)
		target := "//" + bench + ":" + base + "_test"
		args = append(args, target, "--", "-test.run=-")
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
		err := d.exec.CommandContextInheritingStdStreams(ctx, "bazel", args...)
		if err != nil {
			return err
		}
	}

	return nil
}
