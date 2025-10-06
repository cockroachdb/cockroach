// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"log"
	"os"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-microbench/util"
	roachprodConfig "github.com/cockroachdb/cockroach/pkg/roachprod/config"
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
	command.AddCommand(makeCleanCommand())
	command.AddCommand(makeCompressCommand())
	command.AddCommand(makeStageCommand())

	return command
}

func makeCleanCommand() *cobra.Command {
	var config cleanConfig
	runCmdFunc := func(cmd *cobra.Command, args []string) error {

		config.inputFilePath = args[0]
		config.outputFilePath = args[1]
		c, err := newClean(config)
		if err != nil {
			return err
		}

		return c.cleanBenchmarkOutputLog()
	}
	command := &cobra.Command{
		Use:   "clean <inputFilePath> <outputFilePath>",
		Short: "Summarise the benchmark output from the input file and write the summary to the output file",
		Long:  `Summarise the benchmark output from the input file and write the summary to the output file`,
		Args:  cobra.ExactArgs(2),
		RunE:  runCmdFunc,
	}
	return command
}

func makeRunCommand() *cobra.Command {
	config := defaultExecutorConfig()
	runCmdFunc := func(cmd *cobra.Command, commandLine []string) error {
		args, testArgs := util.SplitArgsAtDash(cmd, commandLine)
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
	cmd.Flags().StringToStringVar(&config.binaries, "binaries", config.binaries, "local output name and remote path of the test binaries to run (ex., experiment=<sha1>,baseline=<sha2>")
	cmd.Flags().StringVar(&config.outputDir, "output-dir", config.outputDir, "output directory for run log and microbenchmark results")
	cmd.Flags().StringVar(&config.timeout, "timeout", config.timeout, "timeout for each benchmark e.g. 10m")
	cmd.Flags().StringVar(&config.shellCommand, "shell", config.shellCommand, "additional shell command to run on node before benchmark execution")
	cmd.Flags().StringSliceVar(&config.excludeList, "exclude", []string{}, "benchmarks to exclude, in the form <pkg regex:benchmark regex> e.g. 'pkg/util/.*:BenchmarkIntPool,pkg/sql:.*'")
	cmd.Flags().StringSliceVar(&config.ignorePackageList, "ignore-package", []string{}, "packages to completely exclude from listing or execution'")
	cmd.Flags().IntVar(&config.iterations, "iterations", config.iterations, "number of iterations to run each benchmark")
	cmd.Flags().BoolVar(&config.lenient, "lenient", config.lenient, "tolerate errors while running benchmarks")
	cmd.Flags().BoolVar(&config.affinity, "affinity", config.affinity, "run benchmarks with iterations and binaries having affinity to the same node, only applies when more than one archive is specified")
	cmd.Flags().BoolVar(&config.quiet, "quiet", config.quiet, "suppress roachprod progress output")
	cmd.Flags().BoolVar(&config.recoverable, "recoverable", config.recoverable, "VMs are able to recover from transient failures (e.g., running spot instances on a MIG in GCE)")
	return cmd
}

func makeStageCommand() *cobra.Command {
	runCmdFunc := func(cmd *cobra.Command, args []string) error {
		cluster := args[0]
		src := args[1]
		dest := args[2]
		return stage(cluster, src, dest)
	}

	cmd := &cobra.Command{
		Use:   "stage <cluster> <src> <dest>",
		Short: "Stage a given test binaries archive on a roachprod cluster.",
		Long: `Copy and extract a portable test binaries archive to all nodes on the specified cluster and destination.

the destination is a directory created by this command where the binaries archive will be extracted.
The source can be a local path or a GCS URI.`,
		Args: cobra.ExactArgs(3),
		RunE: runCmdFunc,
	}
	cmd.Flags().BoolVar(&roachprodConfig.Quiet, "quiet", roachprodConfig.Quiet, "suppress roachprod progress output")
	return cmd
}

func makeCompareCommand() *cobra.Command {
	config := defaultCompareConfig()
	runCmdFunc := func(cmd *cobra.Command, args []string) error {
		config.experimentDir = args[0]
		config.baselineDir = args[1]
		c, err := newCompare(config)
		if err != nil {
			return err
		}

		metricMaps, err := c.readMetrics()
		if err != nil {
			return err
		}

		comparisonResult := c.createComparisons(metricMaps, "baseline", "experiment")

		var links map[string]string
		if c.sheetDesc != "" {
			links, err = c.publishToGoogleSheets(comparisonResult)
			if err != nil {
				return err
			}
			if c.slackConfig.token != "" {
				err = c.postToSlack(links, comparisonResult)
				if err != nil {
					return err
				}
			}
		}

		if c.influxConfig.token != "" {
			err = c.pushToInfluxDB()
			if err != nil {
				return err
			}
		}

		// if the threshold is set, we want to compare and fail the job in case of perf regressions
		if c.threshold != skipComparison {
			return c.compareUsingThreshold(comparisonResult)
		}
		return nil
	}

	cmd := &cobra.Command{
		Use:   "compare <experiment-dir> <baseline-dir>",
		Short: "Compare two sets of microbenchmark results.",
		Long: `Compare two sets of microbenchmark results.

- experiment and baseline directories should contain the results of running microbenchmarks using the run command.
- experiment is generally considered the results from a new version of the code, and baseline the results from a stable version.`,
		Args: cobra.ExactArgs(2),
		RunE: runCmdFunc,
	}
	cmd.Flags().StringVar(&config.sheetDesc, "sheet-desc", config.sheetDesc, "set a sheet description to publish the results to Google Sheets")
	cmd.Flags().StringVar(&config.slackConfig.token, "slack-token", config.slackConfig.token, "pass a slack token to post the results to a slack channel")
	cmd.Flags().StringVar(&config.slackConfig.user, "slack-user", config.slackConfig.user, "slack user to post the results as")
	cmd.Flags().StringVar(&config.slackConfig.channel, "slack-channel", config.slackConfig.channel, "slack channel to post the results to")
	cmd.Flags().StringVar(&config.influxConfig.host, "influx-host", config.influxConfig.host, "InfluxDB host to push the results to")
	cmd.Flags().StringVar(&config.influxConfig.token, "influx-token", config.influxConfig.token, "pass an InfluxDB auth token to push the results to InfluxDB")
	cmd.Flags().StringToStringVar(&config.influxConfig.metadata, "influx-metadata", config.influxConfig.metadata, "pass metadata to add to the InfluxDB measurement")
	cmd.Flags().Float64Var(&config.threshold, "threshold", config.threshold, "threshold in percentage value for detecting perf regression ")
	return cmd
}

func makeExportCommand() *cobra.Command {
	var (
		labels map[string]string
		ts     int64
	)
	runCmdFunc := func(cmd *cobra.Command, args []string) error {
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

func makeCompressCommand() *cobra.Command {
	runCmdFunc := func(cmd *cobra.Command, commandLine []string) error {
		return compress(cmd.OutOrStdout(), cmd.InOrStdin())
	}
	cmd := &cobra.Command{
		Use:   "compress",
		Short: "Compress data from stdin and output to stdout",
		Args:  cobra.ExactArgs(0),
		RunE:  runCmdFunc,
	}
	return cmd
}

func main() {
	ssh.InsecureIgnoreHostKey = true
	cmd := makeRoachprodMicrobenchCommand()
	if err := cmd.Execute(); err != nil {
		log.Printf("ERROR: %+v", err)
		os.Exit(1)
	}
}
