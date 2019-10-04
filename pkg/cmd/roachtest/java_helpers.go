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
	"encoding/xml"
	"fmt"
	"regexp"
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
