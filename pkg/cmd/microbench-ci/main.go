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
	cmdFunc := func(cmd *cobra.Command, commandLine []string) error {
		if err := config.loadSuite(); err != nil {
			return err
		}
		results, err := suite.Benchmarks.compareBenchmarks()
		if err != nil {
			return err
		}
		if err = results.writeJSONSummary(config.SummaryPath); err != nil {
			return err
		}
		return results.writeGitHubSummary(config.GitHubSummaryPath)
	}
	cmd := &cobra.Command{
		Use:   "compare",
		Short: "compare artifacts for the given revisions and output an HTML report",
		Args:  cobra.ExactArgs(0),
		RunE:  cmdFunc,
	}
	cmd.Flags().StringVar(&config.SummaryPath, "summary", config.SummaryPath, "path to write comparison results to (JSON)")
	return cmd
}

func makePostCommand() (*cobra.Command, error) {
	repo := "cockroachdb/cockroach"
	var prNumber int
	cmdFunc := func(cmd *cobra.Command, commandLine []string) error {
		summaryText, err := os.ReadFile(config.GitHubSummaryPath)
		if err != nil {
			return err
		}
		return post(string(summaryText), repo, prNumber)
	}
	cmd := &cobra.Command{
		Use:   "post",
		Short: "post creates or updates a microbench-ci summary comment on a GitHub PR",
		Args:  cobra.ExactArgs(0),
		RunE:  cmdFunc,
	}
	cmd.Flags().StringVar(&repo, "repo", repo, "repository")
	cmd.Flags().IntVar(&prNumber, "pr-number", 0, "PR number")
	if err := cmd.MarkFlagRequired("pr-number"); err != nil {
		return nil, err
	}
	return cmd, nil
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
	postCmd, err := makePostCommand()
	if err != nil {
		return err
	}

	for _, c := range []*cobra.Command{runCmd, compareCmd} {
		c.Flags().StringVar(&config.WorkingDir, "working-dir", config.WorkingDir, "directory to store or load artifacts from")
		c.Flags().StringVar(&config.BenchmarkConfigPath, "config", config.BenchmarkConfigPath, "suite configuration file")
		c.Flags().StringVar(&config.Old, "old", "", "old commit")
		c.Flags().StringVar(&config.New, "new", "", "new commit")
		if err = c.MarkFlagRequired("old"); err != nil {
			return err
		}
		if err = c.MarkFlagRequired("new"); err != nil {
			return err
		}
	}

	for _, c := range []*cobra.Command{postCmd, compareCmd} {
		c.Flags().StringVar(&config.GitHubSummaryPath, "github-summary", config.GitHubSummaryPath, "path to write comparison results to (GitHub Markdown)")
	}

	compareCmd.Flags().StringVar(&config.BuildID, "build-id", config.BuildID, "GitHub build ID to identify this run")

	cmd.AddCommand(runCmd)
	cmd.AddCommand(compareCmd)
	cmd.AddCommand(postCmd)

	return cmd.Execute()
}

func main() {
	config = defaultConfig()
	if err := run(); err != nil {
		log.Printf("ERROR: %+v", err)
		os.Exit(1)
	}
}
