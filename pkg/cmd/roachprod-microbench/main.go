// Copyright 2022 The Cockroach Authors.
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
	"log"
	"os"

	"github.com/cockroachdb/cockroach/pkg/roachprod/ssh"
	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
)

func makeRoachprodMicrobenchCommand() *cobra.Command {
	command := &cobra.Command{
		Use:     "roachprod-microbench [command] (flags)",
		Short:   "roachprod-microbench is a utility to distribute and run microbenchmark binaries on a roachprod cluster.",
		Version: "v0.0",
		Long: `roachprod-microbench is a utility to distribute and run microbenchmark binaries on a roachprod cluster. Use it to:

- distribute and run archives containing microbenchmarks, generated with the dev tool using the dev test-binaries command.
- compare the output from different runs, using the compare subcommand and generate a google sheet with the results of the comparison.

Typical usage:
    roachprod-microbench run output_dir --cluster=roachprod-cluster --binaries=new-binaries.tar --compare-binaries=old-binaries.tar
        Run new and old binaries on a roachprod cluster and output the results to output_dir.
        Each binaries result will be stored to an indexed subdirectory in output_dir e.g., output_dir/i.

    roachprod-microbench run output_dir --cluster=roachprod-cluster
        Run binaries located in the default location ("bin/test_binaries.tar.gz") on a roachprod cluster and output the results to output_dir.

    roachprod-microbench compare output_dir/0 output_dir/1 --sheet-desc="master vs. release-20.1"
        Publish a Google Sheet containing the comparison of the results in output_dir/0 and output_dir/1.
`,
		SilenceUsage:  true,
		SilenceErrors: true,
	}

	// Add subcommands.
	command.AddCommand(makeCompareCommand())
	command.AddCommand(makeRunCommand())

	return command
}

func makeRunCommand() *cobra.Command {
	var (
		flagOutputDir       = "outputdir"
		flagLibDir          = "libdir"
		flagRemoteDir       = "remotedir"
		flagTimeout         = "timeout"
		flagShell           = "shell"
		flagCopy            = "copy"
		flagExclude         = "exclude"
		flagIterations      = "iterations"
		flagBinaries        = "binaries"
		flagCompareBinaries = "compare-binaries"
		flagLenient         = "lenient"
		flagAffinity        = "affinity"
		flagQuiet           = "quiet"
	)

	runCmdFunc := func(cmd *cobra.Command, commandLine []string) error {
		args, testArgs := splitArgsAtDash(cmd, commandLine)

		binaries := mustGetFlagString(cmd, flagBinaries)
		compareBinaries := mustGetFlagString(cmd, flagCompareBinaries)
		binariesList := []string{binaries}
		if compareBinaries != "" {
			binariesList = append(binariesList, compareBinaries)
		}
		excludeList := mustGetFlagStringSlice(cmd, flagExclude)
		iterations := mustGetConstrainedFlagInt(cmd, flagIterations, func(i int) error {
			if i < 1 {
				return errors.New("iterations must be greater than 0")
			}
			return nil
		})

		config := executorConfig{
			cluster:      args[0],
			outputDir:    mustGetFlagString(cmd, flagOutputDir),
			libDir:       mustGetFlagString(cmd, flagLibDir),
			remoteDir:    mustGetFlagString(cmd, flagRemoteDir),
			timeout:      mustGetFlagString(cmd, flagTimeout),
			shellCommand: mustGetFlagString(cmd, flagShell),
			copyBinaries: mustGetFlagBool(cmd, flagCopy),
			lenient:      mustGetFlagBool(cmd, flagLenient),
			affinity:     mustGetFlagBool(cmd, flagAffinity),
			quiet:        mustGetFlagBool(cmd, flagQuiet),
			excludeList:  excludeList,
			iterations:   iterations,
			binaries:     binariesList,
			testArgs:     testArgs,
		}

		e, err := makeExecutor(config)
		if err != nil {
			return err
		}

		return e.executeBenchmarks()
	}

	cmd := &cobra.Command{
		Use:   "run <cluster>",
		Short: "Run one or more microbenchmarks binaries archives on a roachprod cluster.",
		Long:  `Run one or more microbenchmarks binaries archives on a roachprod cluster.`,
		Args: func(cmd *cobra.Command, args []string) error {
			if err := cobra.ExactArgs(1)(cmd, args[:cmd.ArgsLenAtDash()]); err != nil {
				return err
			}
			return nil
		},
		RunE: runCmdFunc,
	}
	cmd.Flags().String(flagBinaries, "bin/test_binaries.tar.gz", "portable test binaries archive built with dev test-binaries")
	cmd.Flags().String(flagCompareBinaries, "", "run additional binaries from this archive and compare the results")
	cmd.Flags().String(flagOutputDir, "artifacts/roachprod-microbench", "output directory for run log and microbenchmark results")
	cmd.Flags().String(flagLibDir, "bin/lib", "location of libraries required by test binaries")
	cmd.Flags().String(flagRemoteDir, "/mnt/data1/microbench", "working directory on the target cluster")
	cmd.Flags().String(flagTimeout, "20m", "timeout for each benchmark e.g. 10m")
	cmd.Flags().String(flagShell, "COCKROACH_RANDOM_SEED=1", "additional shell command to run on node before benchmark execution")
	cmd.Flags().StringSlice(flagExclude, []string{}, "comma-separated regex of packages and benchmarks to exclude e.g. 'pkg/util/.*:BenchmarkIntPool,pkg/sql:.*'")
	cmd.Flags().Int(flagIterations, 1, "number of iterations to run each benchmark")
	cmd.Flags().Bool(flagCopy, true, "copy and extract test binaries and libraries to the target cluster")
	cmd.Flags().Bool(flagLenient, true, "tolerate errors in the benchmark results")
	cmd.Flags().Bool(flagAffinity, true, "run benchmarks with iterations and binaries having affinity to the same node, only applies when more than one archive is specified")
	cmd.Flags().Bool(flagQuiet, false, "suppress roachprod progress output")

	return cmd
}

func makeCompareCommand() *cobra.Command {
	flagSheetDesc := "sheet-desc"
	runCmdFunc := func(cmd *cobra.Command, commandLine []string) error {
		args, _ := splitArgsAtDash(cmd, commandLine)
		var (
			newDir    = args[0]
			oldDir    = args[1]
			sheetDesc = mustGetFlagString(cmd, flagSheetDesc)
		)

		c, err := makeCompare(compareConfig{newDir: newDir, oldDir: oldDir, sheetDesc: sheetDesc})
		if err != nil {
			return err
		}

		tableResults, err := c.compareBenchmarks()
		if err != nil {
			return err
		}

		if pubErr := c.publishToGoogleSheets(tableResults); pubErr != nil {
			return pubErr
		}

		return nil
	}

	cmd := &cobra.Command{
		Use:   "compare <new-dir> <old-dir>",
		Short: "Compare two sets of microbenchmark results.",
		Long:  `Compare two sets of microbenchmark results.`,
		Args:  cobra.ExactArgs(2),
		RunE:  runCmdFunc,
	}
	cmd.Flags().String(flagSheetDesc, "", "append a description to the sheet title when doing a comparison")
	return cmd
}

func main() {
	ssh.InsecureIgnoreHostKey = true
	cmd := makeRoachprodMicrobenchCommand()
	if err := cmd.Execute(); err != nil {
		log.Printf("ERROR: %v", err)
		os.Exit(1)
	}
}
