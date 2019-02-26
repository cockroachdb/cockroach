// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.

package main

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/retry"
)

var psycopgResultRegex = regexp.MustCompile(`(?P<name>.*) \((?P<class>.*)\) \.\.\. (?P<result>.*)`)

// This test runs psycopg full test suite against an single cockroach node.

func registerPsycopg(r *registry) {
	runPsycopg := func(
		ctx context.Context,
		t *test,
		c *cluster,
	) {
		if c.isLocal() {
			t.Fatal("cannot be run in local mode")
		}
		node := c.Node(1)
		t.Status("setting up cockroach")
		c.Put(ctx, cockroach, "./cockroach", c.All())
		c.Start(ctx, t, c.All())

		t.Status("cloning psycopg and installing prerequisites")
		opts := retry.Options{
			InitialBackoff: 10 * time.Second,
			Multiplier:     2,
			MaxBackoff:     5 * time.Minute,
		}
		for attempt, r := 0, retry.StartWithCtx(ctx, opts); r.Next(); {
			if ctx.Err() != nil {
				return
			}
			if c.t.Failed() {
				return
			}
			attempt++

			c.l.Printf("attempt %d - update dependencies", attempt)
			if err := c.RunE(ctx, node, `sudo apt-get -q update`); err != nil {
				continue
			}
			if err := c.RunE(
				ctx, node, `sudo apt-get -qy install make python3 libpq-dev python-dev gcc`,
			); err != nil {
				continue
			}

			c.l.Printf("attempt %d - cloning psycopg", attempt)
			if err := c.RunE(ctx, node, `rm -rf /mnt/data1/psycopg`); err != nil {
				continue
			}
			if err := c.GitCloneE(
				ctx,
				"https://github.com/psycopg/psycopg2.git",
				"/mnt/data1/psycopg",
				"2_7_7",
				node,
			); err != nil {
				continue
			}

			break
		}

		t.Status("building psycopg")
		for attempt, r := 0, retry.StartWithCtx(ctx, opts); r.Next(); {
			if ctx.Err() != nil {
				return
			}
			if c.t.Failed() {
				return
			}
			attempt++
			c.l.Printf("attempt %d - building psycopg", attempt)
			if err := c.RunE(
				ctx, node, `cd /mnt/data1/psycopg/ && make`,
			); err != nil {
				continue
			}
			break
		}

		version, err := fetchCockroachVersion(ctx, c, node[0])
		if err != nil {
			t.Fatal(err)
		}
		blacklistName, expectedFailureList, ignoredlistName, ignoredlist := getPsycopgBlacklistForVersion(version)
		if expectedFailureList == nil {
			t.Fatalf("No psycopg blacklist defined for cockroach version %s", version)
		}
		if ignoredlist == nil {
			t.Fatalf("No psycopg ignorelist defined for cockroach version %s", version)
		}
		c.l.Printf("Running cockroach version %s, using blacklist %s, using ignoredlist %s",
			version, blacklistName, ignoredlistName)

		t.Status("running psycopg test suite")
		// Note that this is expected to return an error, since the test suite
		// will fail. And it is safe to swallow it here.
		rawResults, _ := c.RunWithBuffer(ctx, t.l, node,
			`cd /mnt/data1/psycopg/ &&
			export PSYCOPG2_TESTDB=defaultdb &&
			export PSYCOPG2_TESTDB_USER=root &&
			export PSYCOPG2_TESTDB_PORT=26257 &&
			export PSYCOPG2_TESTDB_HOST=localhost &&
			make check`,
		)

		t.Status("collating the test results")
		c.l.Printf("Test Results: %s", rawResults)

		// Find all the failed and errored tests.

		var failUnexpectedCount, failExpectedCount, ignoredCount int
		var passUnexpectedCount, passExpectedCount, notRunCount int
		// Put all the results in a giant map of [testname]result
		results := make(map[string]string)
		// Current failures are any tests that reported as failed, regardless of if
		// they were expected or not.
		var currentFailures, allTests []string
		runTests := make(map[string]struct{})

		scanner := bufio.NewScanner(bytes.NewReader(rawResults))
		for scanner.Scan() {
			match := psycopgResultRegex.FindStringSubmatch(scanner.Text())
			if match != nil {
				groups := map[string]string{}
				for i, name := range match {
					groups[psycopgResultRegex.SubexpNames()[i]] = name
				}
				test := fmt.Sprintf("%s.%s", groups["class"], groups["name"])
				pass := groups["result"] == "ok"
				allTests = append(allTests, test)

				ignoredIssue, expectedIgnored := ignoredlist[test]
				issue, expectedFailure := expectedFailureList[test]
				switch {
				case expectedIgnored:
					results[test] = fmt.Sprintf("--- SKIP: %s due to %s (expected)", test, ignoredIssue)
					ignoredCount++
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
					results[test] = fmt.Sprintf("--- FAIL: %s (unexpected)", test)
					failUnexpectedCount++
					currentFailures = append(currentFailures, test)
				}
				runTests[test] = struct{}{}
			}
		}

		// Collect all the tests that were not run.
		for test, issue := range expectedFailureList {
			if _, ok := runTests[test]; ok {
				continue
			}
			allTests = append(allTests, test)
			results[test] = fmt.Sprintf("--- FAIL: %s - %s (not run)", test, maybeAddGithubLink(issue))
			notRunCount++
		}

		// Log all the test results. We re-order the tests alphabetically here
		// to ensure that we're doing .
		sort.Strings(allTests)
		for _, test := range allTests {
			result, ok := results[test]
			if !ok {
				t.Fatalf("can't find %s in test result list", test)
			}
			t.l.Printf("%s\n", result)
		}

		t.l.Printf("------------------------\n")

		var bResults strings.Builder
		fmt.Fprintf(&bResults, "Tests run on Cockroach %s\n", version)
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
		p("ignored", ignoredCount)
		p("passed unexpectedly", passUnexpectedCount)
		p("failed unexpectedly", failUnexpectedCount)
		p("expected failed but not run", notRunCount)

		fmt.Fprintf(&bResults, "For a full summary look at the psycopg artifacts. \n")
		t.l.Printf("%s\n", bResults.String())
		t.l.Printf("------------------------\n")

		if failUnexpectedCount > 0 || passUnexpectedCount > 0 || notRunCount > 0 {
			// Create a new psycopg_blacklist so we can easily update this test.
			sort.Strings(currentFailures)
			var b strings.Builder
			fmt.Fprintf(&b, "Here is new psycopg blacklist that can be used to update the test:\n\n")
			fmt.Fprintf(&b, "var %s = blacklist{\n", blacklistName)
			for _, test := range currentFailures {
				issue := expectedFailureList[test]
				if len(issue) == 0 {
					issue = "unknown"
				}
				fmt.Fprintf(&b, "  \"%s\": \"%s\",\n", test, issue)
			}
			fmt.Fprintf(&b, "}\n\n")
			t.l.Printf("\n\n%s\n\n", b.String())
			t.l.Printf("------------------------\n")
			t.Fatalf("\n%s\nAn updated blacklist (%s) is available in the logs\n",
				bResults.String(),
				blacklistName,
			)
		}
	}

	r.Add(testSpec{
		Name:    "psycopg",
		Cluster: makeClusterSpec(1),
		Run: func(ctx context.Context, t *test, c *cluster) {
			runPsycopg(ctx, t, c)
		},
	})
}
