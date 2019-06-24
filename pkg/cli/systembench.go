// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cli

import (
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/cli/systembench"
	"github.com/spf13/cobra"
)

// An seqWriteBench command runs I/O benchmarks on cockroach.
var seqWriteBench = &cobra.Command{
	Use:   "seqwrite",
	Short: "Runs the sequential disk write benchmark.",
	Long: `
Runs the sequential disk write benchmark.
`,
	Args: cobra.NoArgs,
	RunE: MaybeDecorateGRPCError(RunSeqWriteBench),
}

// A cpuBench command runs CPU benchmarks on cockroach.
var cpuBench = &cobra.Command{
	Use:   "cpu",
	Short: "Runs the prime finding cpu benchmark.",
	Long: `
Runs the prime finding cpu benchmark.
`,
	Args: cobra.NoArgs,
	RunE: MaybeDecorateGRPCError(RunCPUBench),
}

// A networkBench command runs network benchmarks on cockroach.
var networkBench = &cobra.Command{
	Use:   "network",
	Short: "Runs network benchmarks.",
	Long: `
Runs network benchmarks..
`,
	Args: cobra.NoArgs,
	RunE: MaybeDecorateGRPCError(RunNetworkBench),
}

// RunSeqWriteBench runs a sequential write I/O benchmark.
func RunSeqWriteBench(cmd *cobra.Command, args []string) error {
	iOOpts := systembench.DiskOptions{
		Concurrency: systemBenchCtx.concurrency,
		Duration:    systemBenchCtx.duration,
		Dir:         systemBenchCtx.tempDir,

		Type:         systembench.SeqWriteTest,
		WriteSize:    systemBenchCtx.writeSize,
		SyncInterval: systemBenchCtx.syncInterval,
	}
	return systembench.Run(iOOpts)
}

// RunCPUBench runs the prime finding cpu benchmark.
func RunCPUBench(cmd *cobra.Command, args []string) error {
	cpuOptions := systembench.CPUOptions{
		Concurrency: systemBenchCtx.concurrency,
		Duration:    systemBenchCtx.duration,

		Type: systembench.CPUPrimeTest,
	}
	return systembench.RunCPU(cpuOptions)
}

// RunNetworkBench runs the network benchmark.
func RunNetworkBench(cmd *cobra.Command, args []string) error {
	if networkBenchCtx.server {
		serverOptions := systembench.ServerOptions{
			Port: strconv.Itoa(networkBenchCtx.port),
		}
		return systembench.RunServer(serverOptions)
	}

	clientOptions := systembench.ClientOptions{
		Concurrency: systemBenchCtx.concurrency,
		Duration:    systemBenchCtx.duration,

		Addresses:   networkBenchCtx.addresses,
		LatencyMode: networkBenchCtx.latency,
	}
	return systembench.RunClient(clientOptions)
}

var systemBenchCmds = []*cobra.Command{
	seqWriteBench,
	cpuBench,
	networkBench,
}

var systemBenchCmd = &cobra.Command{
	Use:   "systembench [command]",
	Short: "Run systembench",
	Long: `
Run cockroach hardware benchmarks, for options use --help.`,
	RunE: usageAndErr,
}

func init() {
	systemBenchCmd.AddCommand(systemBenchCmds...)
}
