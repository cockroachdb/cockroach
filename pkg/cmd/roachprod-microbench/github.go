// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
//

package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/bazci/githubpost"
	"github.com/cockroachdb/cockroach/pkg/cmd/bazci/githubpost/issues"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-microbench/cluster"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
)

// createBenchmarkPostRequest creates a post request for a benchmark failure.
func createBenchmarkPostRequest(
	artifactsDir string, response cluster.RemoteResponse, timeout bool,
) (issues.IssueFormatter, issues.PostRequest) {
	b := response.Metadata.(benchmarkKey).benchmark
	var combinedOutput strings.Builder
	if timeout {
		combinedOutput.WriteString(fmt.Sprintf("%s timed out after %s\n", b.name, response.Duration.Round(time.Second)))
	} else {
		combinedOutput.WriteString(response.Stdout)
		combinedOutput.WriteString("\n")
		combinedOutput.WriteString(response.Stderr)
	}

	f := githubpost.MicrobenchmarkFailure(
		b.pkg,
		b.name,
		combinedOutput.String(),
	)
	formatter, req := githubpost.DefaultFormatter(context.Background(), f)
	req.Artifacts = artifactsDir
	req.Labels = append(req.Labels, "O-microbench")
	return formatter, req
}

// regressionInfo holds information about a single benchmark regression.
type regressionInfo struct {
	benchmarkName  string
	metricUnit     string
	percentChange  float64
	formattedDelta string
}

// createRegressionPostRequest creates a post request for benchmark performance regressions.
func createRegressionPostRequest(
	pkgName string, regressions []regressionInfo, sheetLink, description string,
) (issues.IssueFormatter, issues.PostRequest) {
	// Build the regression summary message
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("Performance regressions detected in package %s\n\n", pkgName))
	sb.WriteString(fmt.Sprintf("Comparison: %s\n\n", description))
	sb.WriteString(fmt.Sprintf("Found %d benchmark(s) with regressions ≥20%%:\n\n", len(regressions)))

	for i, reg := range regressions {
		if i >= 10 { // Limit to 10 regressions in the summary
			sb.WriteString(fmt.Sprintf("... and %d more regression(s)\n", len(regressions)-i))
			break
		}
		sb.WriteString(fmt.Sprintf("• %s (%s): %s (%.1f%%)\n",
			reg.benchmarkName, reg.metricUnit, reg.formattedDelta, reg.percentChange))
	}

	sb.WriteString(fmt.Sprintf("\nDetailed comparison: %s\n", sheetLink))

	benchmarkName := regressions[0].benchmarkName
	f := githubpost.MicrobenchmarkFailure(
		pkgName,
		benchmarkName,
		sb.String(),
	)

	formatter, req := githubpost.DefaultFormatter(context.Background(), f)
	req.Labels = append(req.Labels, "O-microbench", "C-performance")
	return formatter, req
}

// postBenchmarkIssue posts a benchmark issue to github.
func postBenchmarkIssue(
	ctx context.Context, l *logger.Logger, formatter issues.IssueFormatter, req issues.PostRequest,
) error {
	opts := issues.DefaultOptionsFromEnv()
	_, err := issues.Post(ctx, l, formatter, req, opts)
	return err
}
