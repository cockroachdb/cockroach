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
	"context"
	"encoding/xml"
	"fmt"
	"regexp"
	"strings"
)

var issueRegexp = regexp.MustCompile(`See: https://github.com/cockroachdb/cockroach/issues/(\d+)`)

// extractFailureFromJUnitXML parses an XML report to find all failed tests. The
// return values are:
// - slice of all test names.
// - slice of bool for each test, with true indicating pass.
// - map from name of a failed test to a github issue that explains the failure,
//   if the error message contained a reference to an issue.
// - error if there was a problem parsing the XML.
func extractFailureFromJUnitXML(contents []byte) ([]string, []bool, map[string]string, error) {
	type Failure struct {
		Message string `xml:"message,attr"`
	}
	type Error struct {
		Message string `xml:"message,attr"`
	}
	type TestCase struct {
		Name      string    `xml:"name,attr"`
		ClassName string    `xml:"classname,attr"`
		Failure   Failure   `xml:"failure,omitempty"`
		Error     Error     `xml:"error,omitempty"`
		Skipped   *struct{} `xml:"skipped,omitempty"`
	}
	type TestSuite struct {
		XMLName   xml.Name   `xml:"testsuite"`
		TestCases []TestCase `xml:"testcase"`
	}

	var testSuite TestSuite
	_ = testSuite.XMLName

	if err := xml.Unmarshal(contents, &testSuite); err != nil {
		return nil, nil, nil, err
	}

	var tests []string
	var passed []bool
	var failedTestToIssue = make(map[string]string)
	for _, testCase := range testSuite.TestCases {
		if testCase.Skipped != nil {
			continue
		}
		testName := fmt.Sprintf("%s.%s", testCase.ClassName, testCase.Name)
		testPassed := len(testCase.Failure.Message) == 0 && len(testCase.Error.Message) == 0
		tests = append(tests, testName)
		passed = append(passed, testPassed)
		if !testPassed {
			message := testCase.Failure.Message
			if len(message) == 0 {
				message = testCase.Error.Message
			}

			issue := "unknown"
			match := issueRegexp.FindStringSubmatch(message)
			if match != nil {
				issue = match[1]
			}
			failedTestToIssue[testName] = issue
		}
	}

	return tests, passed, failedTestToIssue, nil
}

// parseAndSummarizeJavaORMTestsResults parses the test output of running a
// test suite for some Java ORM against cockroach and summarizes it. If an
// unexpected result is observed (for example, a test unexpectedly failed or
// passed), a new blacklist is populated.
func parseAndSummarizeJavaORMTestsResults(
	ctx context.Context,
	t *test,
	c *cluster,
	node nodeListOption,
	ormName string,
	testOutput []byte,
	blacklistName string,
	expectedFailures blacklist,
	version string,
	latestTag string,
) {
	var failUnexpectedCount, failExpectedCount int
	var passUnexpectedCount, passExpectedCount int
	// Put all the results in a giant map of [testname]result.
	results := make(map[string]string)
	// Put all issue hints in a map of [testname]issue.
	allIssueHints := make(map[string]string)
	// Current failures are any tests that reported as failed, regardless of if
	// they were expected or not.
	var currentFailures, allTests []string
	runTests := make(map[string]struct{})
	filesRaw := strings.Split(string(testOutput), "\n")

	// There is always at least one entry that's just space characters, remove
	// it.
	var files []string
	for _, f := range filesRaw {
		file := strings.TrimSpace(f)
		if len(file) > 0 {
			files = append(files, file)
		}
	}
	for i, file := range files {
		t.l.Printf("Parsing %d of %d: %s\n", i+1, len(files), file)
		fileOutput, err := repeatRunWithBuffer(
			ctx,
			c,
			t.l,
			node,
			fmt.Sprintf("fetching results file %s", file),
			fmt.Sprintf("cat %s", file),
		)
		if err != nil {
			t.Fatal(err)
		}
		tests, passed, issueHints, err := extractFailureFromJUnitXML(fileOutput)
		if err != nil {
			t.Fatal(err)
		}
		for testName, issue := range issueHints {
			allIssueHints[testName] = issue
		}
		for i, test := range tests {
			// There is at least a single test that's run twice, so if we already
			// have a result, skip it.
			if _, alreadyTested := results[test]; alreadyTested {
				continue
			}
			allTests = append(allTests, test)
			issue, expectedFailure := expectedFailures[test]
			if len(issue) == 0 || issue == "unknown" {
				issue = issueHints[test]
			}
			pass := passed[i]
			switch {
			case pass && !expectedFailure:
				results[test] = fmt.Sprintf("--- PASS: %s (expected)", test)
				passExpectedCount++
			case pass && expectedFailure:
				results[test] = fmt.Sprintf("--- PASS: %s - %s (unexpected)",
					test, maybeAddGithubLink(issue),
				)
				passUnexpectedCount++
			case !pass && expectedFailure:
				results[test] = fmt.Sprintf("--- FAIL: %s - %s (expected)",
					test, maybeAddGithubLink(issue),
				)
				failExpectedCount++
				currentFailures = append(currentFailures, test)
			case !pass && !expectedFailure:
				results[test] = fmt.Sprintf("--- FAIL: %s - %s (unexpected)",
					test, maybeAddGithubLink(issue))
				failUnexpectedCount++
				currentFailures = append(currentFailures, test)
			}
			runTests[test] = struct{}{}
		}
	}

	summarizeORMTestsResults(
		t, ormName, blacklistName, expectedFailures,
		version, latestTag, currentFailures, allTests, runTests, results,
		failUnexpectedCount, failExpectedCount, 0, /* ignoredCount */
		0 /* skipCount */, 0, /* unexpectedSkipCount */
		passUnexpectedCount, passExpectedCount, allIssueHints,
	)
}
