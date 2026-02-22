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
	threshold      float64
}

const dashboardURL = "https://microbench.testeng.crdb.io/dashboard/"

// generateDashboardLink creates a dashboard link for a given benchmark name.
// Benchmark names are expected to be in the format: pkg/path→BenchmarkName/subtest-GOMAXPROCS
// The full benchmark name (including subtests and GOMAXPROCS) is preserved in the URL.
func generateDashboardLink(benchmarkName string) string {
	parts := strings.Split(benchmarkName, util.PackageSeparator)
	if len(parts) != 2 {
		return ""
	}
	packagePath := parts[0]
	benchName := parts[1]
	return fmt.Sprintf("%s?benchmark=%s&package=%s", dashboardURL, benchName, packagePath)
}

// extractBranchLabel extracts a branch label from the version context string.
// It parses the first part (baseline) of the comparison and creates a label like "branch-release-25.4".
// Examples:
//   - "v25.4.0 (4f417924) -> refs/heads/master (47a00e3e)" -> "branch-release-25.4"
//   - "release-24.1 -> master" -> "branch-release-24.1"
//   - "master -> release-24.2" -> "" (baseline is master, no release label)
func extractBranchLabel(versionContext string) string {
	// Split by "->" to get baseline and experiment
	parts := strings.Split(versionContext, "->")
	if len(parts) != 2 {
		return ""
	}

	// Get the baseline (first part) and trim spaces
	baseline := strings.TrimSpace(parts[0])

	// Remove commit hash in parentheses if present: "v25.4.0 (4f417924)" -> "v25.4.0"
	if idx := strings.Index(baseline, "("); idx != -1 {
		baseline = strings.TrimSpace(baseline[:idx])
	}

	// Check if baseline is a version number (starts with 'v' followed by digits)
	if strings.HasPrefix(baseline, "v") && len(baseline) > 1 {
		// Parse version: v25.4.0 -> 25.4.0
		versionStr := baseline[1:]

		// Split by '.' to get major.minor.patch
		versionParts := strings.Split(versionStr, ".")
		if len(versionParts) >= 2 {
			// Extract major.minor, drop patch version
			majorMinor := versionParts[0] + "." + versionParts[1]
			return "branch-release-" + majorMinor
		}
	}

	// Check if baseline is already a release branch name: "release-24.1"
	if strings.HasPrefix(baseline, "release-") {
		return "branch-" + baseline
	}

	// If baseline is master or refs/heads/master, don't add a branch label
	if baseline == "master" || baseline == "refs/heads/master" {
		return ""
	}

	// Unknown format, return empty
	return ""
}

// parseBenchmarkName extracts the package path and function name from a benchmark name.
// Benchmark names are formatted as: pkg/path→FunctionName/subtest-GOMAXPROCS
// Returns the package path and the benchmark function name (with "Benchmark" prefix).
// Returns an error if the benchmark name is malformed.
func parseBenchmarkName(benchmarkName string) (packagePath, functionName string, err error) {
	// Split by → to separate package path from function name
	parts := strings.Split(benchmarkName, util.PackageSeparator)
	if len(parts) != 2 {
		return "", "", fmt.Errorf("invalid benchmark name format (expected pkg→name): %s", benchmarkName)
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

	// Add a "Benchmark" prefix if not present
	if !strings.HasPrefix(funcPart, "Benchmark") {
		functionName = "Benchmark" + funcPart
	} else {
		functionName = funcPart
	}

	return packagePath, functionName, nil
}

// regressionFormatter is a custom formatter for microbenchmark performance regressions.
// It generates shorter, more informative titles compared to the default unit test formatter.
type regressionFormatter struct {
	versionContext string
}

// Title generates a canonical, simplified title. The version context is
// already in the issue body and reflected in the branch label.
func (rf regressionFormatter) Title(data issues.TemplateData) string {
	return fmt.Sprintf("%s: potential performance regression", data.PackageNameShort)
}

// Body renders the regression issue body with markdown-formatted content
// instead of wrapping everything in a code block.
func (rf regressionFormatter) Body(r *issues.Renderer, data issues.TemplateData) error {
	// Top-level notes if any
	for _, note := range data.TopLevelNotes {
		r.Escaped("**Note:** ")
		r.Escaped(note)
		r.Escaped("\n\n")
	}

	// Header with package name
	r.Escaped(fmt.Sprintf("%s ", data.PackageNameShort))
	r.A("performance regression", data.URL)
	r.Escaped(" on " + data.Branch + " @ ")
	r.A(data.Commit, data.CommitURL)
	r.Escaped(":\n\n")

	// Render the message as escaped markdown (not in code block)
	// This allows dashboard links to be clickable
	r.Escaped(string(data.CondensedMessage))

	// Parameters section (reuse from UnitTestFormatter)
	if len(data.Parameters) != 0 {
		params := make([]string, 0, len(data.Parameters))
		for name := range data.Parameters {
			params = append(params, name)
		}
		// Note: sort package not imported, skip sorting for now
		r.Escaped("\n\nParameters:\n")
		for _, name := range params {
			r.Escaped(fmt.Sprintf("- `%s=%s`\n", name, data.Parameters[name]))
		}
	}

	// Help command section
	if data.HelpCommand != nil {
		r.Collapsed("Help", func() {
			data.HelpCommand(r)
		})
	}

	// Related issues section
	if len(data.RelatedIssues) > 0 {
		r.Collapsed("Same failure on other branches", func() {
			for _, iss := range data.RelatedIssues {
				r.Escaped(fmt.Sprintf("\n- #%d %s", iss.GetNumber(), iss.GetTitle()))
			}
			r.Escaped("\n")
		})
	}

	// Internal log section
	if data.InternalLog != "" {
		r.Collapsed("Internal log", func() {
			r.CodeBlock("", data.InternalLog)
		})
		r.Escaped("\n")
	}

	// Mentions section
	if len(data.MentionOnCreate) > 0 {
		r.Escaped("/cc")
		for _, handle := range data.MentionOnCreate {
			r.Escaped(" ")
			r.Escaped(handle)
		}
		r.Escaped("\n")
	}

	return nil
}

// createRegressionPostRequest creates a post request for benchmark performance regressions.
// Returns an error if any benchmark name is malformed.
func createRegressionPostRequest(
	pkgName string, regressions []regressionInfo, description string,
) (issues.IssueFormatter, issues.PostRequest, error) {
	// Build the regression summary message
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("Performance regressions detected in package %s\n\n", pkgName))
	sb.WriteString(fmt.Sprintf("Comparison: %s\n\n", description))
	sb.WriteString(fmt.Sprintf("Found %d benchmark(s) with regressions exceeding thresholds:\n\n", len(regressions)))

	for _, reg := range regressions {
		// Make the entire line a clickable link to the dashboard
		dashboardLink := generateDashboardLink(reg.benchmarkName)
		if dashboardLink == "" {
			sb.WriteString(fmt.Sprintf("- [ ] %s (%s): %s (%.1f%%, threshold: %.1f%%)\n",
				reg.benchmarkName, reg.metricUnit, reg.formattedDelta, reg.percentChange, reg.threshold))
		} else {
			sb.WriteString(fmt.Sprintf("- [ ] [%s (%s): %s (%.1f%%, threshold: %.1f%%)](%s)\n",
				reg.benchmarkName, reg.metricUnit, reg.formattedDelta, reg.percentChange, reg.threshold, dashboardLink))
		}
	}

	// Parse the first benchmark name to extract package and function for team lookup
	benchmarkName := regressions[0].benchmarkName
	packagePath, functionName, err := parseBenchmarkName(benchmarkName)
	if err != nil {
		return nil, issues.PostRequest{}, fmt.Errorf("failed to parse benchmark name for team lookup: %w", err)
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

	// Extract and add branch label from version context
	if branchLabel := extractBranchLabel(description); branchLabel != "" {
		req.Labels = append(req.Labels, branchLabel)
	}

	return formatter, req, nil
}

// createSingleRegressionPostRequest creates a post request for a single
// benchmark regression, using the benchmark's benchdoc configuration to
// determine labels (e.g., release-blocker).
func createSingleRegressionPostRequest(
	reg regressionInfo, description string, releaseBlocker bool,
) (issues.IssueFormatter, issues.PostRequest, error) {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("Performance regression detected for benchmark %s\n\n", reg.benchmarkName))
	sb.WriteString(fmt.Sprintf("Comparison: %s\n\n", description))

	dashboardLink := generateDashboardLink(reg.benchmarkName)
	if dashboardLink == "" {
		sb.WriteString(fmt.Sprintf("- %s (%s): %s (%.1f%%, threshold: %.1f%%)\n",
			reg.benchmarkName, reg.metricUnit, reg.formattedDelta, reg.percentChange, reg.threshold))
	} else {
		sb.WriteString(fmt.Sprintf("- [%s (%s): %s (%.1f%%, threshold: %.1f%%)](%s)\n",
			reg.benchmarkName, reg.metricUnit, reg.formattedDelta, reg.percentChange, reg.threshold, dashboardLink))
	}

	packagePath, functionName, err := parseBenchmarkName(reg.benchmarkName)
	if err != nil {
		return nil, issues.PostRequest{}, err
	}

	f := githubpost.MicrobenchmarkFailure(packagePath, functionName, sb.String())
	formatter := regressionFormatter{versionContext: description}
	_, req := githubpost.DefaultFormatter(context.Background(), f)

	// Remove the release-blocker label if not required
	if !releaseBlocker {
		req.Labels = filterLabels(req.Labels, issues.ReleaseBlockerLabel)
	}
	req.Labels = append(req.Labels, "O-microbench", "C-performance")

	if branchLabel := extractBranchLabel(description); branchLabel != "" {
		req.Labels = append(req.Labels, branchLabel)
	}

	return formatter, req, nil
}

// filterLabels returns a new slice with the specified label removed.
func filterLabels(labels []string, remove string) []string {
	var filtered []string
	for _, l := range labels {
		if l != remove {
			filtered = append(filtered, l)
		}
	}
	return filtered
}

// postBenchmarkIssue posts a benchmark issue to github.
func postBenchmarkIssue(
	ctx context.Context, l *logger.Logger, formatter issues.IssueFormatter, req issues.PostRequest,
) error {
	opts := issues.DefaultOptionsFromEnv()
	_, err := issues.Post(ctx, l, formatter, req, opts)
	return err
}
