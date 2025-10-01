// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
)

const (
	// mochaDetailedFailureIndent is the indentation string used by Mocha for
	// lines in the detailed failure section (after "X failing").
	// This distinguishes them from the test output section which uses 2-4 spaces.
	mochaDetailedFailureIndent = "       " // 7 spaces
)

// mochaTestResult represents the result of a single test from Mocha output
type mochaTestResult struct {
	name   string
	status status
}

// parseMochaOutput parses Mocha test output and extracts test results.
// Mocha output format:
//
//	✓ passing test name
//	1) failing test name
//	- skipped test name
//
// Returns a slice of test results.
func parseMochaOutput(output string) []mochaTestResult {
	var results []mochaTestResult
	seen := make(map[string]bool) // Track seen test names to avoid duplicates.
	lines := strings.Split(output, "\n")

	// Regex patterns for different test result types.
	passingPattern := regexp.MustCompile(`^\s*✓\s+(.+)$`)
	failingPattern := regexp.MustCompile(`^\s*\d+\)\s+(.+?)(?::.*)?$`)
	skippedPattern := regexp.MustCompile(`^\s*-\s+(.+)$`)

	for i, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		if match := passingPattern.FindStringSubmatch(line); match != nil {
			testName := strings.TrimSpace(match[1])
			if !seen[testName] {
				seen[testName] = true
				results = append(results, mochaTestResult{
					name:   testName,
					status: statusPass,
				})
			}
		} else if match := failingPattern.FindStringSubmatch(line); match != nil {
			// Look ahead to see if this is a nested test failure.
			// Check next few lines to build the full test description.
			suiteName := strings.TrimSpace(match[1])
			var hierarchy []string
			foundDetailedFormat := false

			// Look for indented continuation lines that describe the full test path.
			// Mocha can have multiple levels: suite -> subsuite -> test.
			for j := i + 1; j < len(lines) && j < i+10; j++ {
				nextLine := lines[j]
				trimmed := strings.TrimSpace(nextLine)

				// Check if this is an indented line (part of the test hierarchy).
				// Use mochaDetailedFailureIndent to distinguish detailed failure section
				// from the test output section which has only 2-4 spaces.
				if strings.HasPrefix(nextLine, mochaDetailedFailureIndent) && len(trimmed) > 0 {
					// If it ends with :, it's the final test name.
					if strings.HasSuffix(trimmed, ":") {
						hierarchy = append(hierarchy, strings.TrimSuffix(trimmed, ":"))
						foundDetailedFormat = true
						break
					} else {
						// It's an intermediate level (subsuite).
						hierarchy = append(hierarchy, trimmed)
					}
				} else if trimmed != "" && !strings.HasPrefix(nextLine, " ") {
					// Non-indented non-empty line means we've left the failure description.
					break
				}
			}

			// Only process entries from the detailed failure section (which have indented continuation).
			// Skip the summary section entries that appear inline with test output.
			if !foundDetailedFormat {
				continue
			}

			// Build the full hierarchical test name.
			// Format: "suite => subsuite => ... => test"
			var combinedName string
			if len(hierarchy) > 0 {
				// Start with the suite name, then add all hierarchy levels.
				parts := []string{suiteName}
				parts = append(parts, hierarchy...)
				combinedName = strings.Join(parts, " => ")
			} else {
				// No hierarchy found, just use the suite name.
				combinedName = suiteName
			}

			if !seen[combinedName] {
				seen[combinedName] = true
				results = append(results, mochaTestResult{
					name:   combinedName,
					status: statusFail,
				})
			}
		} else if match := skippedPattern.FindStringSubmatch(line); match != nil {
			testName := strings.TrimSpace(match[1])
			if !seen[testName] {
				seen[testName] = true
				results = append(results, mochaTestResult{
					name:   testName,
					status: statusSkip,
				})
			}
		}
	}

	return results
}

// parseAndSummarizeNodeJSTestResults parses Node.js/Mocha test output and
// summarizes it. This is based off of parseAndSummarizeJavaORMTestsResults.
func parseAndSummarizeNodeJSTestResults(
	ctx context.Context,
	t test.Test,
	c cluster.Cluster,
	node option.NodeListOption,
	testName string,
	testOutput string,
	blocklistName string,
	expectedFailures blocklist,
	ignorelist blocklist,
	version string,
) {
	results := newORMTestsResults()

	// Parse the Mocha output.
	mochaResults := parseMochaOutput(testOutput)

	for _, testResult := range mochaResults {
		currentTestName := testResult.name

		// Check if test is in ignore or block lists.
		ignoredIssue, expectedIgnored := ignorelist[currentTestName]
		issue, expectedFailure := expectedFailures[currentTestName]

		if len(issue) == 0 || issue == "unknown" {
			issue = "unknown"
		}

		// Categorize the test result.
		switch {
		case expectedIgnored:
			results.results[currentTestName] = fmt.Sprintf("--- IGNORE: %s due to %s (expected)", currentTestName, ignoredIssue)
			results.ignoredCount++
		case testResult.status == statusSkip:
			results.results[currentTestName] = fmt.Sprintf("--- SKIP: %s", currentTestName)
			results.skipCount++
		case testResult.status == statusPass && !expectedFailure:
			results.results[currentTestName] = fmt.Sprintf("--- PASS: %s (expected)", currentTestName)
			results.passExpectedCount++
		case testResult.status == statusPass && expectedFailure:
			results.results[currentTestName] = fmt.Sprintf("--- PASS: %s - %s (unexpected)",
				currentTestName, maybeAddGithubLink(issue),
			)
			results.passUnexpectedCount++
		case testResult.status == statusFail && expectedFailure:
			results.results[currentTestName] = fmt.Sprintf("--- FAIL: %s - %s (expected)",
				currentTestName, maybeAddGithubLink(issue),
			)
			results.failExpectedCount++
			results.currentFailures = append(results.currentFailures, currentTestName)
		case testResult.status == statusFail && !expectedFailure:
			results.results[currentTestName] = fmt.Sprintf("--- FAIL: %s - %s (unexpected)",
				currentTestName, maybeAddGithubLink(issue))
			results.failUnexpectedCount++
			results.currentFailures = append(results.currentFailures, currentTestName)
		}

		results.runTests[currentTestName] = struct{}{}
		results.allTests = append(results.allTests, currentTestName)
	}

	results.summarizeAll(
		t, testName, blocklistName, expectedFailures, version, "N/A",
	)
}
