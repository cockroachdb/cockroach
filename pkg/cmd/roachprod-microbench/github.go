// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
//

package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cmd/bazci/githubpost/issues"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-microbench/cluster"
	"github.com/cockroachdb/errors"
)

func postBenchmarkIssue(response cluster.RemoteResponse) error {
	benchmark := response.Metadata.(benchmarkKey).benchmark

	// Create a GitHub issue for the failed benchmark
	req := issues.PostRequest{
		TestName:    benchmark.name,
		PackageName: benchmark.pkg,
		Labels:      []string{"O-microbench", "C-test-failure"},
		Message:     formatBenchmarkFailureMessage(response),
		ExtraParams: map[string]string{
			"Command": strings.Join(response.Args, " "),
		},
		HelpCommand: issues.ReproductionCommandFromString(strings.Join(response.Args, " ")),
	}

	// Post the issue
	ctx := context.Background()
	opts := &issues.Options{
		Token: os.Getenv("GITHUB_API_TOKEN"),
		Org:   "cockroachdb",
		Repo:  "cockroach",
	}

	if opts.Token == "" {
		return errors.New("GITHUB_API_TOKEN environment variable not set")
	}

	_, err := issues.Post(ctx, log.Default(), issues.UnitTestFormatter, req, opts)
	return err
}

func formatBenchmarkFailureMessage(response cluster.RemoteResponse) string {
	var b strings.Builder

	// Add command that was run
	fmt.Fprintf(&b, "Failed benchmark command:\n```\n%s\n```\n\n", strings.Join(response.Args, " "))

	// Add stdout if present
	if response.Stdout != "" {
		fmt.Fprintf(&b, "Stdout:\n```\n%s\n```\n\n", response.Stdout)
	}

	// Add stderr if present
	if response.Stderr != "" {
		fmt.Fprintf(&b, "Stderr:\n```\n%s\n```\n\n", response.Stderr)
	}

	// Add error message if present
	if response.Err != nil {
		fmt.Fprintf(&b, "Error:\n```\n%v\n```\n\n", response.Err)
	}

	// Add exit status if non-zero
	if response.ExitStatus != 0 {
		fmt.Fprintf(&b, "Exit status: %d\n\n", response.ExitStatus)
	}

	return b.String()
}
