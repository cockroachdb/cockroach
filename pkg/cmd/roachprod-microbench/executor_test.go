// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"fmt"
	"path"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-microbench/parser"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

func TestExtractBenchmarkResultsDataDriven(t *testing.T) {
	ddFilePath := path.Join(datapathutils.TestDataPath(t), "benchmark.txt")
	datadriven.RunTest(t, ddFilePath, func(t *testing.T, d *datadriven.TestData) string {
		if d.Cmd != "benchmark" {
			d.Fatalf(t, "unknown command %s", d.Cmd)
		}
		result := parser.ExtractBenchmarkResults(d.Input)
		output := fmt.Sprintf("%v", result)
		return output
	})
}

func TestGenerateBenchmarkCommandsDataDriven(t *testing.T) {
	ddFilePath := path.Join(datapathutils.TestDataPath(t), "generate_commands.txt")
	datadriven.RunTest(t, ddFilePath, func(t *testing.T, d *datadriven.TestData) string {
		if d.Cmd != "generate" {
			d.Fatalf(t, "unknown command %s", d.Cmd)
		}

		// Parse test configuration
		config := defaultExecutorConfig()
		for _, arg := range d.CmdArgs {
			switch arg.Key {
			case "iterations":
				var err error
				config.iterations, err = strconv.Atoi(arg.Vals[0])
				require.NoError(t, err)
			case "affinity":
				config.affinity = arg.Vals[0] == "true"
			case "binaries":
				binaries := make(map[string]string)
				for _, name := range arg.Vals {
					binaries[name] = name
				}
				config.binaries = binaries
			default:
				t.Fatalf("unknown flag %s", arg.Key)
			}
		}

		// Create executor with the config
		e, err := newExecutor(config)
		require.NoError(t, err)

		// Parse benchmark input
		var benchmarks []benchmark
		for _, line := range strings.Split(d.Input, "\n") {
			line = strings.TrimSpace(line)
			if line == "" {
				continue
			}
			parts := strings.Split(line, " ")
			require.Len(t, parts, 2, "benchmark format should be 'pkg name'")
			benchmarks = append(benchmarks, benchmark{
				pkg:  parts[0],
				name: parts[1],
			})
		}

		// Generate commands
		commands, err := e.generateBenchmarkCommands(benchmarks)
		require.NoError(t, err)

		// Format output
		var output strings.Builder
		for i, cmdGroup := range commands {
			fmt.Fprintf(&output, "Command Group %d:\n", i+1)
			for _, cmd := range cmdGroup {
				metadata := cmd.Metadata.(benchmarkKey)
				fmt.Fprintf(&output, "\tPackage: %s\n", metadata.pkg)
				fmt.Fprintf(&output, "\tBenchmark: %s\n", metadata.name)
				fmt.Fprintf(&output, "\tBinary Key: %s\n", metadata.key)
			}
		}
		return output.String()
	})
}
