// Copyright 2018 The Cockroach Authors.
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
	"context"
	"fmt"
	"regexp"
	"sort"
	"strings"
)

var psycopgResultRegex = regexp.MustCompile(`(?P<name>.*) \((?P<class>.*)\) \.\.\. (?P<result>[^ ']*)(?: u?['"](?P<reason>.*)['"])?`)
var psycopgReleaseTagRegex = regexp.MustCompile(`^(?P<major>\d+)(?:_(?P<minor>\d+)(?:_(?P<point>\d+)(?:_(?P<subpoint>\d+))?)?)?$`)

// This test runs psycopg full test suite against a single cockroach node.

func registerPsycopg(r *testRegistry) {
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
		latestTag, err := repeatGetLatestTag(ctx, c, "psycopg", "psycopg2", psycopgReleaseTagRegex)
		if err != nil {
			t.Fatal(err)
		}
		c.l.Printf("Latest Psycopg release is %s.", latestTag)

		if err := repeatRunE(
			ctx, c, node, "update apt-get", `sudo apt-get -qq update`,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatRunE(
			ctx,
			c,
			node,
			"install dependencies",
			`sudo apt-get -qq install make python3 libpq-dev python-dev gcc python3-setuptools python-setuptools`,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatRunE(
			ctx, c, node, "remove old Psycopg", `sudo rm -rf /mnt/data1/psycopg`,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatGitCloneE(
			ctx,
			t.l,
			c,
			"https://github.com/psycopg/psycopg2.git",
			"/mnt/data1/psycopg",
			latestTag,
			node,
		); err != nil {
			t.Fatal(err)
		}

		t.Status("building Psycopg")
		if err := repeatRunE(
			ctx, c, node, "building Psycopg", `cd /mnt/data1/psycopg/ && make`,
		); err != nil {
			t.Fatal(err)
		}

		version, err := fetchCockroachVersion(ctx, c, node[0])
		if err != nil {
			t.Fatal(err)
		}
		blacklistName, expectedFailureList, ignoredlistName, ignoredlist := psycopgBlacklists.getLists(version)
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

		var failUnexpectedCount, failExpectedCount, ignoredCount, skipCount, unexpectedSkipCount int
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
				var skipReason string
				if groups["result"] == "skipped" {
					skipReason = groups["reason"]
				}
				pass := groups["result"] == "ok"
				allTests = append(allTests, test)

				ignoredIssue, expectedIgnored := ignoredlist[test]
				issue, expectedFailure := expectedFailureList[test]
				switch {
				case expectedIgnored:
					results[test] = fmt.Sprintf("--- SKIP: %s due to %s (expected)", test, ignoredIssue)
					ignoredCount++
				case len(skipReason) > 0 && expectedFailure:
					results[test] = fmt.Sprintf("--- SKIP: %s due to %s (unexpected)", test, skipReason)
					unexpectedSkipCount++
				case len(skipReason) > 0:
					results[test] = fmt.Sprintf("--- SKIP: %s due to %s (expected)", test, skipReason)
					skipCount++
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
		fmt.Fprintf(&bResults, "Tests run against Pyscopg %s\n", latestTag)
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

		fmt.Fprintf(&bResults, "For a full summary look at the psycopg artifacts. \n")
		t.l.Printf("%s\n", bResults.String())
		t.l.Printf("------------------------\n")

		if failUnexpectedCount > 0 || passUnexpectedCount > 0 ||
			notRunCount > 0 || unexpectedSkipCount > 0 {
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
		Name:       "psycopg",
		Cluster:    makeClusterSpec(1),
		MinVersion: "v19.1.0",
		Run: func(ctx context.Context, t *test, c *cluster) {
			runPsycopg(ctx, t, c)
		},
	})
}
