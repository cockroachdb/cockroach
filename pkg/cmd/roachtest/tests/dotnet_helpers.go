// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"encoding/xml"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
)

// extractFailureFromTRX parses a TRX report to find all failed tests. The
// return values are:
//   - slice of all test names.
//   - slice of status for each test.
//   - map from name of a failed test to a github issue that explains the failure,
//     if the error message contained a reference to an issue.
//   - error if there was a problem parsing the XML.
func extractFailureFromTRX(contents []byte) ([]string, []status, map[string]string, error) {
	type ErrorInfo struct {
		Message string `xml:"Message,omitempty"`
	}
	type Output struct {
		ErrorInfo ErrorInfo `xml:"ErrorInfo,omitempty"`
	}
	type UnitTestResult struct {
		Name    string `xml:"testName,attr"`
		Outcome string `xml:"outcome,attr"`
		TestID  string `xml:"testId,attr"`
		Output  Output `xml:"Output,omitempty"`
	}
	type Results struct {
		UnitTestResults []UnitTestResult `xml:"UnitTestResult"`
	}
	type TestMethod struct {
		// Name is identical to UnitTest.Name.
		Name      string `xml:"name,attr"`
		ClassName string `xml:"className,attr"`
	}
	type UnitTest struct {
		// Name is identical to TestMethod.Name.
		Name       string     `xml:"name,attr"`
		TestID     string     `xml:"id,attr"`
		TestMethod TestMethod `xml:"TestMethod"`
	}
	type TestDefinitions struct {
		UnitTests []UnitTest `xml:"UnitTest"`
	}
	type TestRun struct {
		XMLName         xml.Name        `xml:"TestRun"`
		Results         Results         `xml:"Results"`
		TestDefinitions TestDefinitions `xml:"TestDefinitions"`
	}

	var testRun TestRun
	_ = testRun.XMLName

	var tests []string
	var testStatuses []status
	var failedTestToIssue = make(map[string]string)

	if err := xml.Unmarshal(contents, &testRun); err != nil {
		return nil, nil, nil, err
	}

	// Create mapping from TestID to ClassName+TestName.
	idToFullName := make(map[string]string)
	for _, testDef := range testRun.TestDefinitions.UnitTests {
		idToFullName[testDef.TestID] = fmt.Sprintf("%s.%s", testDef.TestMethod.ClassName, testDef.TestMethod.Name)
	}

	npgsqlFlakeErrors := []string{
		"Received backend message ReadyForQuery while expecting",
		"Received unexpected backend message ReadyForQuery",
		"Got idle connector but State is Copy",
	}
	isAnyFlakeError := func(s string) bool {
		for _, e := range npgsqlFlakeErrors {
			if strings.Contains(s, e) {
				return true
			}
		}
		return false
	}

	// Check each result.
	for _, testCase := range testRun.Results.UnitTestResults {
		testName := idToFullName[testCase.TestID]
		tests = append(tests, testName)
		if testCase.Outcome == "Skipped" || testCase.Outcome == "NotExecuted" {
			testStatuses = append(testStatuses, statusSkip)
		} else if testCase.Outcome == "Passed" {
			testStatuses = append(testStatuses, statusPass)
		} else if isAnyFlakeError(testCase.Output.ErrorInfo.Message) {
			// npgsql tests frequently flake with this error. Until we resolve this
			// specific error, we will ignore all tests that failed for that reason.
			// See https://github.com/cockroachdb/cockroach/issues/108414.
			testStatuses = append(testStatuses, statusSkip)
		} else {
			testStatuses = append(testStatuses, statusFail)
			message := testCase.Output.ErrorInfo.Message
			issue := "unknown"
			match := issueRegexp.FindStringSubmatch(message)
			if match != nil {
				issue = match[1]
			}
			failedTestToIssue[testName] = issue
		}
	}

	return tests, testStatuses, failedTestToIssue, nil
}

// parseTRX parses testOutputInTRXFormat and updates the receiver
// accordingly.
func (r *ormTestsResults) parseTRX(
	t test.Test, expectedFailures, ignorelist blocklist, testOutputInTRXFormat []byte,
) {
	tests, statuses, issueHints, err := extractFailureFromTRX(testOutputInTRXFormat)
	if err != nil {
		t.Fatal(err)
	}
	for testName, issue := range issueHints {
		r.allIssueHints[testName] = issue
	}
	for i, testCase := range tests {
		// There is at least a single test that's run twice, so if we already
		// have a result, skip it.
		if _, alreadyTested := r.results[testCase]; alreadyTested {
			continue
		}
		r.allTests = append(r.allTests, testCase)
		ignoredIssue, expectedIgnored := ignorelist[testCase]
		issue, expectedFailure := expectedFailures[testCase]
		statusCode := statuses[i]
		switch {
		case expectedIgnored:
			r.results[testCase] = fmt.Sprintf("--- IGNORE: %s due to %s (expected)", testCase, ignoredIssue)
			r.ignoredCount++
		case statusCode == statusSkip:
			r.results[testCase] = fmt.Sprintf("--- SKIP: %s", testCase)
			r.skipCount++
		case statusCode == statusPass && !expectedFailure:
			r.results[testCase] = fmt.Sprintf("--- PASS: %s (expected)", testCase)
			r.passExpectedCount++
		case statusCode == statusPass && expectedFailure:
			r.results[testCase] = fmt.Sprintf("--- PASS: %s - %s (unexpected)",
				testCase, maybeAddGithubLink(issue),
			)
			r.passUnexpectedCount++
		case statusCode == statusFail && expectedFailure:
			r.results[testCase] = fmt.Sprintf("--- FAIL: %s - %s (expected)",
				testCase, maybeAddGithubLink(issue),
			)
			r.failExpectedCount++
			r.currentFailures = append(r.currentFailures, testCase)
		case statusCode == statusFail && !expectedFailure:
			r.results[testCase] = fmt.Sprintf("--- FAIL: %s - %s (unexpected)",
				testCase, maybeAddGithubLink(issue))
			r.failUnexpectedCount++
			r.currentFailures = append(r.currentFailures, testCase)
		}
		r.runTests[testCase] = struct{}{}
	}
}

// parseAndSummarizeDotNetTestsResults parses the test output of running a
// test suite for a .NET tool against cockroach and summarizes it. If an
// unexpected result is observed (for example, a test unexpectedly failed or
// passed), a new blocklist is populated.
func parseAndSummarizeDotNetTestsResults(
	ctx context.Context,
	t test.Test,
	c cluster.Cluster,
	node option.NodeListOption,
	ormName string,
	testResultFileName string,
	blocklistName string,
	expectedFailures blocklist,
	ignorelist blocklist,
	version string,
	tag string,
) {
	results := newORMTestsResults()
	t.L().Printf("Parsing %s\n", testResultFileName)
	result, err := repeatRunWithDetailsSingleNode(
		ctx,
		c,
		t,
		node,
		fmt.Sprintf("fetching results file %s", testResultFileName),
		fmt.Sprintf("cat %s", testResultFileName),
	)
	if err != nil {
		t.Fatal(err)
	}

	results.parseTRX(t, expectedFailures, ignorelist, []byte(result.Stdout))

	results.summarizeAll(
		t, ormName, blocklistName, expectedFailures, version, tag,
	)
}
