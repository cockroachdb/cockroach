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
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-microbench/util"
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

// parseBenchmarkName extracts the package path and function name from a benchmark name.
// Benchmark names are formatted as: pkg/path→FunctionName/subtest-GOMAXPROCS
// Returns the package path and the benchmark function name (with "Benchmark" prefix).
func parseBenchmarkName(benchmarkName string) (packagePath, functionName string) {
	// Split by → to separate package path from function name
	parts := strings.Split(benchmarkName, util.PackageSeparator)
	if len(parts) != 2 {
		// Fallback
		return "", benchmarkName
	}

	packagePath = parts[0]

	// Extract function name (everything before the first /)
	funcPart := parts[1]
	if idx := strings.Index(funcPart, "/"); idx != -1 {
		funcPart = funcPart[:idx]
	}

	// Remove GOMAXPROCS suffix (e.g., -32)
	if idx := strings.LastIndex(funcPart, "-"); idx != -1 {
		funcPart = funcPart[:idx]
	}

	// Add "Benchmark" prefix if not present
	if !strings.HasPrefix(funcPart, "Benchmark") {
		functionName = "Benchmark" + funcPart
	} else {
		functionName = funcPart
	}

	return packagePath, functionName
}

// regressionFormatter is a custom formatter for microbenchmark performance regressions.
// It generates shorter, more informative titles compared to the default unit test formatter.
type regressionFormatter struct {
	versionContext string
}

// Title generates a short title that includes the version context for easier identification
// across different branch comparisons.
func (rf regressionFormatter) Title(data issues.TemplateData) string {
	return fmt.Sprintf("%s: regression (%s)", data.PackageNameShort, rf.versionContext)
}

// Body reuses the standard unit test body formatter.
func (rf regressionFormatter) Body(r *issues.Renderer, data issues.TemplateData) error {
	return issues.UnitTestFormatter.Body(r, data)
}

// createRegressionPostRequest creates a post request for benchmark performance regressions.
func createRegressionPostRequest(
	pkgName string, regressions []regressionInfo, description string,
) (issues.IssueFormatter, issues.PostRequest) {
	// Build the regression summary message
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("Performance regressions detected in package %s\n\n", pkgName))
	sb.WriteString(fmt.Sprintf("Comparison: %s\n\n", description))
	sb.WriteString(fmt.Sprintf("Found %d benchmark(s) with regressions ≥%.0f%%:\n\n", len(regressions), slackPercentageThreshold))

	for _, reg := range regressions {
		sb.WriteString(fmt.Sprintf("• %s (%s): %s (%.1f%%)\n",
			reg.benchmarkName, reg.metricUnit, reg.formattedDelta, reg.percentChange))
	}

	// Parse the first benchmark name to extract package and function for team lookup
	benchmarkName := regressions[0].benchmarkName
	packagePath, functionName := parseBenchmarkName(benchmarkName)

	// Use the parsed package path if available, otherwise fall back to pkgName
	if packagePath == "" {
		packagePath = pkgName
	}

	f := githubpost.MicrobenchmarkFailure(
		packagePath,
		functionName,
		sb.String(),
	)

	// Use custom regression formatter instead of default to get shorter,
	// more informative titles with version context.
	formatter := regressionFormatter{versionContext: description}
	_, req := githubpost.DefaultFormatter(context.Background(), f)
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
