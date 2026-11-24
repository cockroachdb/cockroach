// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestflags"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
)

type testResult int

const (
	// NB: These are in a particular order corresponding to the order we
	// want these tests to appear in the generated Markdown report.
	testResultFailure testResult = iota
	testResultSuccess
	testResultSkip
)

type testSummaryReport struct {
	name          string
	duration      time.Duration
	status        testResult
	ignoreDetails string
}

// Write the test summary report to a TSV file, and optionally to markdown, if running inside GitHub Actions.
func (r *testRunner) writeTestReports(ctx context.Context, l *logger.Logger, artifactsDir string) {
	allTests := r.buildTestSummaryReport()
	if err := dumpSummaryReport(allTests, artifactsDir); err != nil {
		shout(ctx, l, os.Stdout, "failed to write test summary report (%+v)", err)
	}
	if err := maybeDumpGHSummaryMarkdown(allTests); err != nil {
		shout(ctx, l, os.Stdout, "failed to write to GITHUB_STEP_SUMMARY file (%+v)", err)
	}
}

func (r *testRunner) buildTestSummaryReport() []testSummaryReport {
	var allTests []testSummaryReport
	for test := range r.status.pass {
		allTests = append(allTests, testSummaryReport{
			name:     test.Name(),
			duration: test.duration(),
			status:   testResultSuccess,
		})
	}

	for test := range r.status.fail {
		allTests = append(allTests, testSummaryReport{
			name:     test.Name(),
			duration: test.duration(),
			status:   testResultFailure,
		})
	}

	for test := range r.status.skip {
		allTests = append(allTests, testSummaryReport{
			name:          test.Name(),
			duration:      test.duration(),
			status:        testResultSkip,
			ignoreDetails: r.status.skipDetails[test],
		})
	}
	// Sort the test results: first fails, then successes, then skips, and
	// within each category sort by test duration in descending order.
	// Ties are very unlikely to happen but we break them by test name.
	slices.SortFunc(allTests, func(a, b testSummaryReport) int {
		if a.status < b.status {
			return -1
		} else if a.status > b.status {
			return 1
		} else if a.duration > b.duration {
			return -1
		} else if a.duration < b.duration {
			return 1
		}
		return strings.Compare(a.name, b.name)
	})

	return allTests
}

// Dumps the test summary report to a TSV file.
// N.B. We can't use CSV since the test names can contain commas. (See `testNameRE`.)
func dumpSummaryReport(allTests []testSummaryReport, dir string) error {
	summaryPath := filepath.Join(dir, "test_summary.tsv")
	summaryFile, err := os.OpenFile(summaryPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer summaryFile.Close()

	_, err = fmt.Fprintf(summaryFile, "%s\t%s\t%s\t%s\n", "test_name", "status", "ignore_details", "duration")
	if err != nil {
		return err
	}
	for _, test := range allTests {
		var statusString string
		if test.status == testResultFailure {
			statusString = "failed"
		} else if test.status == testResultSuccess {
			statusString = "success"
		} else {
			statusString = "skipped"
		}
		_, err := fmt.Fprintf(summaryFile, "%s\t%s\t%s\t%d\n", test.name, statusString, test.ignoreDetails, test.duration.Milliseconds())
		if err != nil {
			return err
		}
	}

	return nil
}

// Dumps the markdown summary report if GITHUB_STEP_SUMMARY is set.
// See https://docs.github.com/en/actions/writing-workflows/choosing-what-your-workflow-does/workflow-commands-for-github-actions#adding-a-job-summary
func maybeDumpGHSummaryMarkdown(allTests []testSummaryReport) error {
	if !roachtestflags.GitHubActions {
		return nil
	}
	summaryPath := os.Getenv("GITHUB_STEP_SUMMARY")
	if summaryPath == "" {
		return nil
	}
	summaryFile, err := os.OpenFile(summaryPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer summaryFile.Close()

	_, err = summaryFile.WriteString(`| TestName | Status | IgnoreDetails | Duration |
| --- | --- | --- | --- |
`)
	if err != nil {
		return err
	}

	for _, test := range allTests {
		var statusString string
		if test.status == testResultFailure {
			statusString = "❌ FAILED"
		} else if test.status == testResultSuccess {
			statusString = "✅ SUCCESS"
		} else {
			statusString = "🟨 SKIPPED"
		}
		_, err := fmt.Fprintf(summaryFile, "| `%s` | %s | %s | `%s` |\n", test.name, statusString, test.ignoreDetails, test.duration.String())
		if err != nil {
			return err
		}
	}

	return nil
}
