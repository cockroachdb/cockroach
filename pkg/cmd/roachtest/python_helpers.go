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
	"bufio"
	"bytes"
	"fmt"
	"regexp"
)

var pythonUnittestOutputRegex = regexp.MustCompile(`(?P<name>.*) \((?P<class>.*)\) \.\.\. (?P<result>[^ ']*)(?: u?['"](?P<reason>.*)['"])?`)

type pythonUnitTestOutputParser struct {
	failUnexpectedCount int
	failExpectedCount   int
	ignoredCount        int
	skipCount           int
	unexpectedSkipCount int
	passUnexpectedCount int
	passExpectedCount   int
	results             map[string]string
	currentFailures     []string
	allTests            []string
	runTests            map[string]struct{}

	expectedFailureList blacklist
	ignoredList         blacklist
}

func makePythonUnitTestOutputParser(
	expectedFailures blacklist, ignoredList blacklist,
) *pythonUnitTestOutputParser {
	return &pythonUnitTestOutputParser{
		failUnexpectedCount: 0,
		failExpectedCount:   0,
		ignoredCount:        0,
		skipCount:           0,
		unexpectedSkipCount: 0,
		passUnexpectedCount: 0,
		passExpectedCount:   0,
		results:             make(map[string]string),
		currentFailures:     make([]string, 0),
		allTests:            make([]string, 0),
		runTests:            make(map[string]struct{}),
		expectedFailureList: expectedFailures,
		ignoredList:         ignoredList,
	}
}

func (p *pythonUnitTestOutputParser) parsePythonUnitTestOutput(output []byte) {
	scanner := bufio.NewScanner(bytes.NewReader(output))
	for scanner.Scan() {
		match := pythonUnittestOutputRegex.FindStringSubmatch(scanner.Text())
		if match != nil {
			groups := map[string]string{}
			for i, name := range match {
				groups[pythonUnittestOutputRegex.SubexpNames()[i]] = name
			}
			test := fmt.Sprintf("%s.%s", groups["class"], groups["name"])
			var skipReason string
			if groups["result"] == "skipped" {
				skipReason = groups["reason"]
			}
			pass := groups["result"] == "ok"
			p.allTests = append(p.allTests, test)

			ignoredIssue, expectedIgnored := p.ignoredList[test]
			issue, expectedFailure := p.expectedFailureList[test]
			switch {
			case expectedIgnored:
				p.results[test] = fmt.Sprintf("--- SKIP: %s due to %s (expected)", test, ignoredIssue)
				p.ignoredCount++
			case len(skipReason) > 0 && expectedFailure:
				p.results[test] = fmt.Sprintf("--- SKIP: %s due to %s (unexpected)", test, skipReason)
				p.unexpectedSkipCount++
			case len(skipReason) > 0:
				p.results[test] = fmt.Sprintf("--- SKIP: %s due to %s (expected)", test, skipReason)
				p.skipCount++
			case pass && !expectedFailure:
				p.results[test] = fmt.Sprintf("--- PASS: %s (expected)", test)
				p.passExpectedCount++
			case pass && expectedFailure:
				p.results[test] = fmt.Sprintf("--- PASS: %s - %s (unexpected)",
					test, maybeAddGithubLink(issue),
				)
				p.passUnexpectedCount++
			case !pass && expectedFailure:
				p.results[test] = fmt.Sprintf("--- FAIL: %s - %s (expected)",
					test, maybeAddGithubLink(issue),
				)
				p.failExpectedCount++
				p.currentFailures = append(p.currentFailures, test)
			case !pass && !expectedFailure:
				p.results[test] = fmt.Sprintf("--- FAIL: %s (unexpected)", test)
				p.failUnexpectedCount++
				p.currentFailures = append(p.currentFailures, test)
			}
			p.runTests[test] = struct{}{}
		}
	}
}
