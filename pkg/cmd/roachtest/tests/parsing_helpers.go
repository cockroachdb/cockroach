// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"bufio"
	"bytes"
	"fmt"
	"regexp"
)

var rustUnitTestOutputRegex = regexp.MustCompile(`(?P<type>.*) (?P<class>.*)::(?P<name>.*) \.\.\. (?P<result>.*)`)
var pythonUnitTestOutputRegex = regexp.MustCompile(`(?P<name>.*) \((?P<class>.*)\) \.\.\. (?P<result>[^'"]*?)(?: u?['"](?P<reason>.*)['"])?$`)

func (r *ormTestsResults) parsePythonUnitTestOutput(
	input []byte, expectedFailures blocklist, ignoredList blocklist,
) {
	r.parseUnitTestOutput(pythonUnitTestOutputRegex, input, expectedFailures, ignoredList)
}

func (r *ormTestsResults) parseRustUnitTestOutput(
	input []byte, expectedFailures blocklist, ignoredList blocklist,
) {
	r.parseUnitTestOutput(rustUnitTestOutputRegex, input, expectedFailures, ignoredList)
}

func (r *ormTestsResults) parseUnitTestOutput(
	testOutputregex *regexp.Regexp, input []byte, expectedFailures blocklist, ignoredList blocklist,
) {
	scanner := bufio.NewScanner(bytes.NewReader(input))
	for scanner.Scan() {
		match := testOutputregex.FindStringSubmatch(scanner.Text())
		if match != nil {
			groups := map[string]string{}
			for i, name := range match {
				groups[testOutputregex.SubexpNames()[i]] = name
			}
			test := groups["name"]
			if c := groups["class"]; len(c) > 0 {
				test = fmt.Sprintf("%s.%s", c, test)
			}
			skipped := groups["result"] == "skipped" || groups["result"] == "expected failure" || groups["result"] == "Skipped"
			skipReason := ""
			if skipped {
				skipReason = groups["reason"]
			}
			pass := groups["result"] == "ok" || groups["result"] == "unexpected success"
			r.allTests = append(r.allTests, test)

			ignoredIssue, expectedIgnored := ignoredList[test]
			issue, expectedFailure := expectedFailures[test]
			switch {
			case expectedIgnored:
				r.results[test] = fmt.Sprintf("--- SKIP: %s due to %s (expected)", test, ignoredIssue)
				r.ignoredCount++
			case skipped && expectedFailure:
				r.results[test] = fmt.Sprintf("--- SKIP: %s due to %s (unexpected)", test, skipReason)
				r.unexpectedSkipCount++
			case skipped:
				r.results[test] = fmt.Sprintf("--- SKIP: %s due to %s (expected)", test, skipReason)
				r.skipCount++
			case pass && !expectedFailure:
				r.results[test] = fmt.Sprintf("--- PASS: %s (expected)", test)
				r.passExpectedCount++
			case pass && expectedFailure:
				r.results[test] = fmt.Sprintf("--- PASS: %s - %s (unexpected)",
					test, maybeAddGithubLink(issue),
				)
				r.passUnexpectedCount++
			case !pass && expectedFailure:
				r.results[test] = fmt.Sprintf("--- FAIL: %s - %s (expected)",
					test, maybeAddGithubLink(issue),
				)
				r.failExpectedCount++
				r.currentFailures = append(r.currentFailures, test)
			case !pass && !expectedFailure:
				r.results[test] = fmt.Sprintf("--- FAIL: %s (unexpected)", test)
				r.failUnexpectedCount++
				r.currentFailures = append(r.currentFailures, test)
			}
			r.runTests[test] = struct{}{}
		}
	}
}
