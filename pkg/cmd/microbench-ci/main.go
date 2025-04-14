// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"fmt"
	"log"
	"os"
	"path"
	"strings"

	"github.com/spf13/cobra"
)

type (
	Config struct {
		WorkingDir          string
		BenchmarkConfigPath string
		SummaryPath         string
		GitHubSummaryPath   string
		BuildID             string
		Old                 string
		New                 string
		Group               int
	}

	Suite struct {
		Benchmarks Benchmarks
		Revisions  Revisions
	}

	Revisions map[Revision]string
	Revision  string
)

const (
	Old Revision = "old"
	New Revision = "new"
)

// Bucket is the GCS bucket where artifacts are stored.
const Bucket = "cockroach-microbench-ci"

var (
	config *Config
	suite  *Suite
)

func defaultConfig() *Config {
	return &Config{
		WorkingDir:          "microbench-ci-artifacts",
		BenchmarkConfigPath: "pkg/cmd/microbench-ci/config/pull-request-suite.yml",
		SummaryPath:         "summary.json",
		GitHubSummaryPath:   "github-summary.md",
	}
}

func (c *Config) loadSuite() error {
	suite = &Suite{
		Revisions: Revisions{
			Old: config.Old,
			New: config.New,
		},
	}
	benchmarks, err := loadBenchmarkConfig(c.BenchmarkConfigPath)
	if err != nil {
		return err
	}
	suite.Benchmarks = benchmarks
	return nil
}

func (s *Suite) revisionDir(revision Revision) string {
	return path.Join(config.WorkingDir, s.revisionSHA(revision))
}

func (s *Suite) revisionSHA(revision Revision) string {
	return s.Revisions[revision]
}

func (s *Suite) binDir(revision Revision) string {
	return path.Join(s.revisionDir(revision), "bin")
}

func (s *Suite) artifactsDir(revision Revision) string {
	return path.Join(s.revisionDir(revision), "artifacts")
}

func (s *Suite) artifactsURL(revision Revision) string {
	return fmt.Sprintf("gs://%s/artifacts/%s/%s/",
		Bucket, s.revisionSHA(revision), config.BuildID)
}

func (s *Suite) binURL(revision Revision, benchmark *Benchmark) string {
	return fmt.Sprintf("gs://%s/builds/%s/bin/%s",
		Bucket, s.revisionSHA(revision), benchmark.binaryName())
}

func (s *Suite) changeDetected() bool {
	for _, status := range []Status{Regressed} {
		for _, benchmark := range suite.Benchmarks {
			markerFile := path.Join(s.artifactsDir(New), benchmark.markerName(status))
			_, err := os.Stat(markerFile)
			if err == nil {
				return true
			}
		}
	}
	return false
}

func makeRunCommand() *cobra.Command {
	cmdFunc := func(cmd *cobra.Command, commandLine []string) error {
		if err := config.loadSuite(); err != nil {
			return err
		}
		return suite.Benchmarks.run()
	}
	cmd := &cobra.Command{
		Use:   "run",
		Short: "run benchmarks and output artifacts",
		Args:  cobra.ExactArgs(0),
		RunE:  cmdFunc,
	}
	cmd.Flags().IntVar(&config.Group, "group", config.Group, "run only the benchmarks in the group (0 runs all groups)")
	return cmd
}

func makeCompareCommand() *cobra.Command {
	post := false
	cmdFunc := func(cmd *cobra.Command, commandLine []string) error {
		if err := config.loadSuite(); err != nil {
			return err
		}
		results, err := suite.Benchmarks.compareBenchmarks()
		if err != nil {
			return err
		}
		summaryText, err := results.githubSummary()
		if err != nil {
			return err
		}
		if err = writeToFile(config.GitHubSummaryPath, summaryText); err != nil {
			return err
		}
		if err = results.writeJSONSummary(config.SummaryPath); err != nil {
			return err
		}

		// If the `PerfLabel` is present on the PR, or a change was detected, we
		// post a comment to GitHub. If a change was detected, we also add the
		// label to the PR, if it is not already present.
		if post {
			github, err := NewGithubConfig()
			if err != nil {
				return err
			}
			hasLabel, err := github.hasLabel(PerfLabel)
			if err != nil {
				return err
			}
			if hasLabel || suite.changeDetected() {
				skipPermissionError := func(err error, action string) error {
					if err == nil {
						return nil
					}
					// If this is a permission error, we don't want to fail the build. This
					// can happen if the GitHub token has read-only access, for example when
					// testing the pull_request trigger from a fork.
					if strings.Contains(err.Error(), "403") {
						log.Printf("WARNING: Skipped %s, because GitHub token does not have write access to the repository", action)
						return nil
					}
					return err
				}
				err = github.postComment(summaryText)
				if skipPermissionError(err, "posting a comment") != nil {
					return err
				}
				if !hasLabel {
					err = github.addLabel(PerfLabel)
					if skipPermissionError(err, "adding a label") != nil {
						return err
					}
				}
			}
		}
		return nil
	}
	cmd := &cobra.Command{
		Use:   "compare",
		Short: "compare artifacts for the given revisions and output an HTML report",
		Args:  cobra.ExactArgs(0),
		RunE:  cmdFunc,
	}
	cmd.Flags().StringVar(&config.SummaryPath, "summary", config.SummaryPath, "path to write comparison results to (JSON)")
	cmd.Flags().BoolVar(&post, "post", false, "post the comparison results to GitHub")
	return cmd
}

func run() error {
	cmd := &cobra.Command{
		Use:           "microbench-ci [command] (flags)",
		Short:         "microbench-ci is used by the Microbenchmark CI pipeline to run benchmarks.",
		SilenceUsage:  true,
		SilenceErrors: true,
	}

	runCmd := makeRunCommand()
	compareCmd := makeCompareCommand()

	for _, c := range []*cobra.Command{runCmd, compareCmd} {
		c.Flags().StringVar(&config.WorkingDir, "working-dir", config.WorkingDir, "directory to store or load artifacts from")
		c.Flags().StringVar(&config.BenchmarkConfigPath, "config", config.BenchmarkConfigPath, "suite configuration file")
		c.Flags().StringVar(&config.Old, "old", "", "old commit")
		c.Flags().StringVar(&config.New, "new", "", "new commit")
		if err := c.MarkFlagRequired("old"); err != nil {
			return err
		}
		if err := c.MarkFlagRequired("new"); err != nil {
			return err
		}
	}
	compareCmd.Flags().StringVar(&config.GitHubSummaryPath, "github-summary", config.GitHubSummaryPath, "path to write comparison results to (GitHub Markdown)")
	compareCmd.Flags().StringVar(&config.BuildID, "build-id", config.BuildID, "GitHub build ID to identify this run")

	cmd.AddCommand(runCmd)
	cmd.AddCommand(compareCmd)

	return cmd.Execute()
}

func main() {
	config = defaultConfig()
	if err := run(); err != nil {
		log.Printf("ERROR: %+v", err)
		os.Exit(1)
	}
}

// writeToFile writes a string to a file.
func writeToFile(path string, text string) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()
	_, err = file.WriteString(text)
	return err
}
