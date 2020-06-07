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

var pythonUnitTestOutputRegex = regexp.MustCompile(`(?P<name>.*) \((?P<class>.*)\) \.\.\. (?P<result>[^ ']*)(?: u?['"](?P<reason>.*)['"])?`)

func (r *ormTestsResults) parsePythonUnitTestOutput(
	input []byte, expectedFailures blocklist, ignoredList blocklist,
) {
	scanner := bufio.NewScanner(bytes.NewReader(input))
	for scanner.Scan() {
		match := pythonUnitTestOutputRegex.FindStringSubmatch(scanner.Text())
		if match != nil {
			groups := map[string]string{}
			for i, name := range match {
				groups[pythonUnitTestOutputRegex.SubexpNames()[i]] = name
			}
			test := fmt.Sprintf("%s.%s", groups["class"], groups["name"])
			var skipReason string
			if groups["result"] == "skipped" {
				skipReason = groups["reason"]
			}
			pass := groups["result"] == "ok"
			r.allTests = append(r.allTests, test)

			ignoredIssue, expectedIgnored := ignoredList[test]
			issue, expectedFailure := expectedFailures[test]
			switch {
			case expectedIgnored:
				r.results[test] = fmt.Sprintf("--- SKIP: %s due to %s (expected)", test, ignoredIssue)
				r.ignoredCount++
			case len(skipReason) > 0 && expectedFailure:
				r.results[test] = fmt.Sprintf("--- SKIP: %s due to %s (unexpected)", test, skipReason)
				r.unexpectedSkipCount++
			case len(skipReason) > 0:
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
