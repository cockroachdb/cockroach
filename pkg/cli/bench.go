// Copyright 2015 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package cli

import (
	"github.com/cockroachdb/cockroach/pkg/cli/systembench"
	"github.com/spf13/cobra"
)

// An hDDBench command runs I/O benchmarks on cockroach.
var hDDBench = &cobra.Command{
	Use:   "hdd",
	Short: "Runs hdd benchmark.",
	Long: `
Runs hdd benchmark.
`,
	Args: cobra.NoArgs,
	RunE: MaybeDecorateGRPCError(RunHddBench),
}

func RunHddBench(cmd *cobra.Command, args []string) error {
	hddOpts := systembench.HDDOptions{
		Concurrency: systemBenchCtx.concurrency,
		Duration:    systemBenchCtx.duration,
		Dir:         systemBenchCtx.tempDir,
		Run128Test:  systemBenchCtx.runHDD128,
	}
	return systembench.Run(hddOpts)
}

var benchCmds = []*cobra.Command{
	hDDBench,
}

var benchCmd = &cobra.Command{
	Use:   "systembench [command]",
	Short: "Run systembench.",
	Long:  `Run cockroach hardware benchmarks, for options use --help.`,
	RunE:  usageAndErr,
}

func init() {
	benchCmd.AddCommand(benchCmds...)
}
