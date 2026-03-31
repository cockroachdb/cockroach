// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Command github-action-poller monitors the status of provided GitHub
// checks for a particular revision and reports their overall status.
// It expects for all checks to have status "completed" and conclusion
// "success" to exit zero.

package main

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/google/go-github/v61/github"
	"github.com/spf13/cobra"
)

var (
	owner, repo, sha string
	timeout          time.Duration
	sleepDuration    time.Duration
)

var rootCmd = &cobra.Command{
	Use:   "github-action-poller",
	Short: "Poll GitHub Action runs and wait for overall result",
	Args:  cobra.MinimumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		cmd.SilenceUsage = true

		deadline := timeutil.Now().Add(timeout)

		for {
			shouldRetry, err := run(cmd.Context(), owner, repo, sha, args)
			if err == nil {
				return nil
			}
			if !shouldRetry {
				return fmt.Errorf("failed: %w", err)
			}
			if timeutil.Now().After(deadline) {
				return fmt.Errorf("timed out: %w", err)
			}
			log.Printf("sleeping for %s, will stop in %s", sleepDuration, timeutil.Until(deadline))
			time.Sleep(sleepDuration)
		}
	},
}

func init() {
	flags := rootCmd.Flags()
	flags.StringVar(&owner, "owner", "", "repository owner")
	flags.StringVar(&repo, "repo", "", "repository")
	flags.StringVar(&sha, "sha", "", "SHA")
	flags.DurationVar(&timeout, "timeout", 30*time.Minute, "overall program timeout")
	flags.DurationVar(&sleepDuration, "sleep", 30*time.Second, "sleep between retries")

	must := func(err error) {
		if err != nil {
			panic(err)
		}
	}
	must(rootCmd.MarkFlagRequired("owner"))
	must(rootCmd.MarkFlagRequired("repo"))
	must(rootCmd.MarkFlagRequired("sha"))
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func run(ctx context.Context, owner, repo, sha string, requiredChecks []string) (bool, error) {
	statuses := make(map[string][]*github.CheckRun, len(requiredChecks))
	for _, c := range requiredChecks {
		statuses[c] = nil
	}

	client := github.NewClient(nil)
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	var checkRuns []*github.CheckRun
	opts := &github.ListCheckRunsOptions{
		ListOptions: github.ListOptions{
			PerPage: 100,
		},
	}
	for {
		checks, resp, err := client.Checks.ListCheckRunsForRef(ctx, owner, repo, sha, opts)
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				return true, fmt.Errorf("API request timed out")
			}
			if resp != nil && resp.StatusCode >= 500 {
				return true, fmt.Errorf("API server error: %w", err)
			}
			return false, fmt.Errorf("API request error: %w", err)
		}
		checkRuns = append(checkRuns, checks.CheckRuns...)
		if resp.NextPage == 0 {
			break
		}
		opts.Page = resp.NextPage
	}

	for _, check := range checkRuns {
		name := check.GetName()
		// Skip the check we are not interested in
		if _, ok := statuses[name]; !ok {
			continue
		}
		statuses[name] = append(statuses[name], check)
	}

	running := []string{}
	failures := []string{}
	completed := []string{}
	for name, checks := range statuses {
		if len(checks) == 0 {
			running = append(running, name)
			continue
		}
		// Assume the latest started check is the source of truth.
		slices.SortFunc(checks, func(a, b *github.CheckRun) int {
			return cmp.Compare(a.StartedAt.UnixNano(), b.StartedAt.UnixNano())
		})
		lastCheck := checks[len(checks)-1]

		if lastCheck.GetStatus() != "completed" {
			running = append(running, fmt.Sprintf("%s: %s", name, lastCheck.GetHTMLURL()))
			continue
		}
		if lastCheck.GetConclusion() == "success" {
			completed = append(completed, fmt.Sprintf("%s: %s", name, lastCheck.GetHTMLURL()))
			continue
		}
		failures = append(failures, fmt.Sprintf("%s: %s", name, lastCheck.GetHTMLURL()))
	}
	if len(failures) > 0 {
		return false, fmt.Errorf("failures: %s", strings.Join(failures, ", "))
	}
	if len(running) > 0 {
		log.Printf("waiting for:\n%s\n", strings.Join(running, "\n"))
		return true, fmt.Errorf("retry later, still waiting on: %s", strings.Join(running, ", "))
	}
	log.Println("Done.")
	log.Println(strings.Join(completed, "\n"))
	return false, nil
}
