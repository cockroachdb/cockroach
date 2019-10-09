// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

import (
	"fmt"
	"sort"
	"strings"
)

// summarizeORMTestsResults summarizes the result of running an ORM test suite
// against a cockroach node. If an unexpected result is observed (for example,
// a test unexpectedly failed or passed), a new blacklist is populated.
//
// allIssueHints (when non-nil) is a map from name of a failed test to a github
// issue that explains the failure, if the error message contained a reference
// to an issue.
func summarizeORMTestsResults(
	t *test,
	ormName, blacklistName string,
	expectedFailures blacklist,
	version, latestTag string,
	currentFailures, allTests []string,
	runTests map[string]struct{},
	results map[string]string,
	failUnexpectedCount, failExpectedCount, ignoredCount int,
	skipCount, unexpectedSkipCount int,
	passUnexpectedCount, passExpectedCount int,
	allIssueHints map[string]string,
) {
	// Collect all the tests that were not run.
	notRunCount := 0
	for test, issue := range expectedFailures {
		if _, ok := runTests[test]; ok {
			continue
		}
		allTests = append(allTests, test)
		results[test] = fmt.Sprintf("--- FAIL: %s - %s (not run)", test, maybeAddGithubLink(issue))
		notRunCount++
	}

	// Log all the test results. We re-order the tests alphabetically here.
	sort.Strings(allTests)
	for _, test := range allTests {
		result, ok := results[test]
		if !ok {
			t.Fatalf("can't find %s in test result list", test)
		}
		t.l.Printf("%s\n", result)
	}

	t.l.Printf("------------------------\n")

	printORMTestsResults(
		t, ormName, blacklistName, expectedFailures, version, latestTag,
		currentFailures, failUnexpectedCount, failExpectedCount, ignoredCount,
		skipCount, unexpectedSkipCount, passUnexpectedCount, passExpectedCount,
		notRunCount, allIssueHints)
}

// printORMTestsResults prints out the results of running an ORM test suite
// against a cockroach node. It is similar to summarizeORMTestsResults except
// that it doesn't pay attention to all the tests - only to the failed ones.
// If a test suite outputs only the failures, then this method should be used.
func printORMTestsResults(
	t *test,
	ormName, blacklistName string,
	expectedFailures blacklist,
	version, latestTag string,
	currentFailures []string,
	failUnexpectedCount, failExpectedCount, ignoredCount int,
	skipCount, unexpectedSkipCount int,
	passUnexpectedCount, passExpectedCount, notRunCount int,
	allIssueHints map[string]string,
) {
	var bResults strings.Builder
	fmt.Fprintf(&bResults, "Tests run on Cockroach %s\n", version)
	fmt.Fprintf(&bResults, "Tests run against %s %s\n", ormName, latestTag)
	fmt.Fprintf(&bResults, "%d Total Tests Run\n",
		passExpectedCount+passUnexpectedCount+failExpectedCount+failUnexpectedCount,
	)

	p := func(msg string, count int) {
		testString := "tests"
		if count == 1 {
			testString = "test"
		}
		fmt.Fprintf(&bResults, "%d %s %s\n", count, testString, msg)
	}
	p("passed", passUnexpectedCount+passExpectedCount)
	p("failed", failUnexpectedCount+failExpectedCount)
	p("skipped", skipCount)
	p("ignored", ignoredCount)
	p("passed unexpectedly", passUnexpectedCount)
	p("failed unexpectedly", failUnexpectedCount)
	p("expected failed but skipped", unexpectedSkipCount)
	p("expected failed but not run", notRunCount)

	fmt.Fprintf(&bResults, "For a full summary look at the %s artifacts \n", ormName)
	t.l.Printf("%s\n", bResults.String())
	t.l.Printf("------------------------\n")

	if failUnexpectedCount > 0 || passUnexpectedCount > 0 ||
		notRunCount > 0 || unexpectedSkipCount > 0 {
		// Create a new blacklist so we can easily update this test.
		sort.Strings(currentFailures)
		var b strings.Builder
		fmt.Fprintf(&b, "Here is new %s blacklist that can be used to update the test:\n\n", ormName)
		fmt.Fprintf(&b, "var %s = blacklist{\n", blacklistName)
		for _, test := range currentFailures {
			issue := expectedFailures[test]
			if (len(issue) == 0 || issue == "unknown") && allIssueHints != nil {
				issue = allIssueHints[test]
			}
			if len(issue) == 0 {
				issue = "unknown"
			}
			fmt.Fprintf(&b, "  \"%s\": \"%s\",\n", test, issue)
		}
		fmt.Fprintf(&b, "}\n\n")
		t.l.Printf("\n\n%s\n\n", b.String())
		t.l.Printf("------------------------\n")
		t.Fatalf("\n%s\nAn updated blacklist (%s) is available in the artifacts' %s log\n",
			bResults.String(),
			blacklistName,
			ormName,
		)
	}
}
