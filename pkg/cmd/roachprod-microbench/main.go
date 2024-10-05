// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"log"
	"os"

	"github.com/cockroachdb/cockroach/pkg/roachprod/ssh"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
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
	command.AddCommand(makeExportCommand())

	return command
}

func makeRunCommand() *cobra.Command {
	config := defaultExecutorConfig()
	runCmdFunc := func(cmd *cobra.Command, commandLine []string) error {
		args, testArgs := splitArgsAtDash(cmd, commandLine)
		config.cluster = args[0]
		config.testArgs = testArgs
		e, err := newExecutor(config)
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
			argsLenAtDash := cmd.ArgsLenAtDash()
			argsLen := len(args)
			if argsLenAtDash >= 0 {
				argsLen = argsLenAtDash
			}
			if err := cobra.ExactArgs(1)(cmd, args[:argsLen]); err != nil {
				return err
			}
			return nil
		},
		RunE: runCmdFunc,
	}
	cmd.Flags().StringVar(&config.binaries, "binaries", config.binaries, "portable test binaries archive built with dev test-binaries")
	cmd.Flags().StringVar(&config.compareBinaries, "compare-binaries", "", "run additional binaries from this archive and compare the results")
	cmd.Flags().StringVar(&config.outputDir, "output-dir", config.outputDir, "output directory for run log and microbenchmark results")
	cmd.Flags().StringVar(&config.libDir, "lib-dir", config.libDir, "location of libraries required by test binaries")
	cmd.Flags().StringVar(&config.remoteDir, "remote-dir", config.remoteDir, "working directory on the target cluster")
	cmd.Flags().StringVar(&config.timeout, "timeout", config.timeout, "timeout for each benchmark e.g. 10m")
	cmd.Flags().StringVar(&config.shellCommand, "shell", config.shellCommand, "additional shell command to run on node before benchmark execution")
	cmd.Flags().StringSliceVar(&config.excludeList, "exclude", []string{}, "comma-separated regex of packages and benchmarks to exclude e.g. 'pkg/util/.*:BenchmarkIntPool,pkg/sql:.*'")
	cmd.Flags().IntVar(&config.iterations, "iterations", config.iterations, "number of iterations to run each benchmark")
	cmd.Flags().BoolVar(&config.copyBinaries, "copy", config.copyBinaries, "copy and extract test binaries and libraries to the target cluster")
	cmd.Flags().BoolVar(&config.lenient, "lenient", config.lenient, "tolerate errors while running benchmarks")
	cmd.Flags().BoolVar(&config.affinity, "affinity", config.affinity, "run benchmarks with iterations and binaries having affinity to the same node, only applies when more than one archive is specified")
	cmd.Flags().BoolVar(&config.quiet, "quiet", config.quiet, "suppress roachprod progress output")

	return cmd
}

func makeCompareCommand() *cobra.Command {
	config := defaultCompareConfig()
	runCmdFunc := func(cmd *cobra.Command, commandLine []string) error {
		args, _ := splitArgsAtDash(cmd, commandLine)

		config.newDir = args[0]
		config.oldDir = args[1]
		c, err := newCompare(config)
		if err != nil {
			return err
		}

		metricMaps, err := c.readMetrics()
		if err != nil {
			return err
		}

		links, err := c.publishToGoogleSheets(metricMaps)
		if err != nil {
			return err
		}
		if config.slackToken != "" {
			err = c.postToSlack(links, metricMaps)
			if err != nil {
				return err
			}
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
	cmd.Flags().StringVar(&config.sheetDesc, "sheet-desc", config.sheetDesc, "append a description to the sheet title when doing a comparison")
	cmd.Flags().StringVar(&config.slackToken, "slack-token", config.slackToken, "pass a slack token to post the results to a slack channel")
	cmd.Flags().StringVar(&config.slackUser, "slack-user", config.slackUser, "slack user to post the results as")
	cmd.Flags().StringVar(&config.slackChannel, "slack-channel", config.slackChannel, "slack channel to post the results to")
	return cmd
}

func makeExportCommand() *cobra.Command {
	var (
		labels map[string]string
		ts     int64
	)
	runCmdFunc := func(cmd *cobra.Command, commandLine []string) error {
		args, _ := splitArgsAtDash(cmd, commandLine)
		return exportMetrics(args[0], os.Stdout, timeutil.Unix(ts, 0), labels)
	}

	cmd := &cobra.Command{
		Use:   "export <dir>",
		Short: "Export microbenchmark results to an open metrics format.",
		Long:  `Export microbenchmark results to an open metrics format.`,
		Args:  cobra.ExactArgs(1),
		RunE:  runCmdFunc,
	}
	cmd.Flags().StringToStringVar(&labels, "labels", nil, "comma-separated list of key=value pair labels to add to the metrics")
	cmd.Flags().Int64Var(&ts, "timestamp", timeutil.Now().Unix(), "unix timestamp to use for the metrics, defaults to now")
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
