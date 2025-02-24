// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/datadriven"
	"github.com/google/pprof/profile"
	"github.com/stretchr/testify/require"
)

type (
	logIdentity struct {
		name string
		path string
	}
)

// emulateBenchExec returns an exec function that emulates the behavior of the
// benchmark executables by matching the call arguments to a map of expected log
// lines. It reads the expected output from the logLines map and returns it in
// order. Each line entry represents the output of one benchmark execution.
func emulateBenchExec(
	t *testing.T, tempDir string, logLines map[logIdentity][]string,
) func(cmd *exec.Cmd) ([]byte, error) {
	return func(cmd *exec.Cmd) ([]byte, error) {
		var name, outputDir string
		profiles := make([]string, 0)
		for idx, arg := range cmd.Args {
			if arg == "-test.bench" {
				name = cmd.Args[idx+1]
			}
			if arg == "-test.outputdir" {
				outputDir = cmd.Args[idx+1]
			}
			if strings.HasSuffix(arg, "profile") {
				profiles = append(profiles, cmd.Args[idx+1])
			}
		}

		// Create stub profile files.
		for _, profileName := range profiles {
			profilePath := path.Join(outputDir, profileName)
			p := &profile.Profile{}
			f, err := os.Create(profilePath)
			require.NoError(t, err)
			require.NoError(t, p.Write(f))
			require.NoError(t, f.Close())
		}

		// Determine the log identity based on the path of the benchmark binary.
		li := logIdentity{
			path: strings.TrimPrefix(cmd.Args[0], tempDir),
			name: name,
		}
		lines, ok := logLines[li]
		if !ok {
			t.Fatal("unexpected log identity", li)
		}
		logLines[li] = lines[1:]
		return []byte(lines[0]), nil
	}
}

// TestRunAndCompare is an end-to-end test for running and comparing benchmarks.
// It invokes the run and compare functions and checks that the output is as
// expected. Changes in the summary outputs and formatting requires regeneration
// of the expected output.
func TestRunAndCompare(t *testing.T) {
	datadriven.Walk(t, datapathutils.TestDataPath(t), func(t *testing.T, testPath string) {
		tempDir := t.TempDir()
		config = &Config{
			WorkingDir:          tempDir,
			BenchmarkConfigPath: path.Join(tempDir, "suite.yml"),
			SummaryPath:         path.Join(tempDir, "summary.json"),
			GitHubSummaryPath:   path.Join(tempDir, "github-summary.md"),
		}
		logLines := make(map[logIdentity][]string)
		execFunc = emulateBenchExec(t, tempDir, logLines)
		datadriven.RunTest(t, testPath, func(t *testing.T, td *datadriven.TestData) string {
			switch td.Cmd {
			case "config":
				td.ScanArgs(t, "new", &config.New)
				td.ScanArgs(t, "old", &config.Old)
				err := os.WriteFile(path.Join(tempDir, "suite.yml"), []byte(td.Input), 0644)
				require.NoError(t, err)
				err = config.loadSuite()
				require.NoError(t, err)
			case "logs":
				var li logIdentity
				td.ScanArgs(t, "name", &li.name)
				td.ScanArgs(t, "path", &li.path)
				logLines[li] = strings.Split(td.Input, "\n")
			case "run":
				td.ScanArgs(t, "group", &config.Group)
				err := suite.Benchmarks.run()
				require.NoError(t, err)
				results, err := suite.Benchmarks.compareBenchmarks()
				require.NoError(t, err)
				err = results.writeGitHubSummary(config.GitHubSummaryPath)
				require.NoError(t, err)
				err = results.writeJSONSummary(config.SummaryPath)
				require.NoError(t, err)
				data, err := os.ReadFile(config.GitHubSummaryPath)
				require.NoError(t, err)
				return string(data)
			case "json":
				data, err := os.ReadFile(config.SummaryPath)
				require.NoError(t, err)
				return string(data)
			case "tree":
				sb := strings.Builder{}
				err := filepath.Walk(tempDir, func(path string, info os.FileInfo, err error) error {
					sb.WriteString(strings.TrimPrefix(path, tempDir) + "\n")
					return nil
				})
				require.NoError(t, err)
				return sb.String()
			}
			return ""
		})
	})
}
