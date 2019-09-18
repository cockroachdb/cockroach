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
	"fmt"
	"regexp"
	"sort"
	"strings"
)

var pgjdbcReleaseTagRegex = regexp.MustCompile(`^REL(?P<major>\d+)\.(?P<minor>\d+)\.(?P<point>\d+)$`)

// This test runs pgjdbc's full test suite against a single cockroach node.

func registerPgjdbc(r *testRegistry) {
	runPgjdbc := func(
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

		t.Status("cloning pgjdbc and installing prerequisites")
		latestTag, err := repeatGetLatestTag(
			ctx, c, "pgjdbc", "pgjdbc", pgjdbcReleaseTagRegex,
		)
		if err != nil {
			t.Fatal(err)
		}
		c.l.Printf("Latest pgjdbc release is %s.", latestTag)

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
			`sudo apt-get -qq install default-jre openjdk-8-jdk-headless maven`,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatRunE(
			ctx, c, node, "remove old pgjdbc", `rm -rf /mnt/data1/pgjdbc`,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatGitCloneE(
			ctx,
			t.l,
			c,
			"https://github.com/pgjdbc/pgjdbc.git",
			"/mnt/data1/pgjdbc",
			latestTag,
			node,
		); err != nil {
			t.Fatal(err)
		}

		// In order to get pgjdbc's test suite to connect to cockroach, we have
		// to override settings in build.local.properties
		if err := repeatRunE(
			ctx,
			c,
			node,
			"configuring tests for cockroach only",
			fmt.Sprintf(
				"echo \"%s\" > /mnt/data1/pgjdbc/build.local.properties", pgjdbcDatabaseParams,
			),
		); err != nil {
			t.Fatal(err)
		}

		t.Status("building pgjdbc (without tests)")
		// Build pgjdbc and run a single test, this step involves some
		// downloading, so it needs a retry loop as well. Just building was not
		// enough as the test libraries are not downloaded unless at least a
		// single test is invoked.
		if err := repeatRunE(
			ctx,
			c,
			node,
			"building pgjdbc (without tests)",
			`cd /mnt/data1/pgjdbc/pgjdbc/ && mvn -Dtest=OidToStringTest test`,
		); err != nil {
			t.Fatal(err)
		}

		version, err := fetchCockroachVersion(ctx, c, node[0])
		if err != nil {
			t.Fatal(err)
		}
		blacklistName, expectedFailures, _, _ := pgjdbcBlacklists.getLists(version)
		if expectedFailures == nil {
			t.Fatalf("No pgjdbc blacklist defined for cockroach version %s", version)
		}
		c.l.Printf("Running cockroach version %s, using blacklist %s", version, blacklistName)

		t.Status("running pgjdbc test suite")
		// Note that this is expected to return an error, since the test suite
		// will fail. And it is safe to swallow it here.
		_ = c.RunE(ctx, node,
			`cd /mnt/data1/pgjdbc/pgjdbc/ && mvn test`,
		)

		_ = c.RunE(ctx, node,
			`mkdir -p ~/logs/report/pgjdbc-results`,
		)

		t.Status("collecting the test results")
		// Copy all of the test results to the cockroach logs directory to be
		// copied to the artifacts.

		// Copy the individual test result files.
		if err := repeatRunE(
			ctx,
			c,
			node,
			"copy test result files",
			`cp /mnt/data1/pgjdbc/pgjdbc/target/surefire-reports ~/logs/report/pgjdbc-results -a`,
		); err != nil {
			t.Fatal(err)
		}

		// Load the list of all test results files and parse them individually.
		// Files are here: /mnt/data1/pgjdbc/pgjdbc-core/target/test-results/test
		output, err := repeatRunWithBuffer(
			ctx,
			c,
			t.l,
			node,
			"get list of test files",
			`ls /mnt/data1/pgjdbc/pgjdbc/target/surefire-reports/*.xml`,
		)
		if err != nil {
			t.Fatal(err)
		}
		if len(output) == 0 {
			t.Fatal("could not find any test result files")
		}

		var failUnexpectedCount, failExpectedCount int
		var passUnexpectedCount, passExpectedCount, notRunCount int
		// Put all the results in a giant map of [testname]result
		results := make(map[string]string)
		// Put all issue hints in a map of [testname]issue
		allIssueHints := make(map[string]string)
		// Current failures are any tests that reported as failed, regardless of if
		// they were expected or not.
		var currentFailures, allTests []string
		runTests := make(map[string]struct{})
		filesRaw := strings.Split(string(output), "\n")

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
		// Collect all the tests that were not run.
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

		var bResults strings.Builder
		fmt.Fprintf(&bResults, "Tests run on Cockroach %s\n", version)
		fmt.Fprintf(&bResults, "Tests run against pgjdbc %s\n", latestTag)
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
		p("passed unexpectedly", passUnexpectedCount)
		p("failed unexpectedly", failUnexpectedCount)
		p("expected failed, but not run", notRunCount)

		fmt.Fprintf(&bResults, "For a full summary look at the pgjdbc artifacts \n")
		t.l.Printf("%s\n", bResults.String())
		t.l.Printf("------------------------\n")

		if failUnexpectedCount > 0 || passUnexpectedCount > 0 || notRunCount > 0 {
			// Create a new pgjdbc_blacklist so we can easily update this test.
			sort.Strings(currentFailures)
			var b strings.Builder
			fmt.Fprintf(&b, "Here is a new pgjdbc blacklist that can be used to update the test:\n\n")
			fmt.Fprintf(&b, "var %s = blacklist{\n", blacklistName)
			for _, test := range currentFailures {
				issue := expectedFailures[test]
				if len(issue) == 0 || issue == "unknown" {
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
			t.Fatalf("\n%s\nAn updated blacklist (%s) is available in the artifacts' pgjdbc log\n",
				bResults.String(),
				blacklistName,
			)
		}
	}

	r.Add(testSpec{
		Name:    "pgjdbc",
		Cluster: makeClusterSpec(1),
		Run: func(ctx context.Context, t *test, c *cluster) {
			runPgjdbc(ctx, t, c)
		},
	})
}

const pgjdbcDatabaseParams = `
server=localhost
port=26257
secondaryServer=localhost
secondaryPort=5433
secondaryServer2=localhost
secondaryServerPort2=5434
database=defaultdb
username=root
password=
privilegedUser=root
privilegedPassword=
sspiusername=testsspi
preparethreshold=5
loggerLevel=DEBUG
loggerFile=target/pgjdbc-tests.log
protocolVersion=0
sslpassword=sslpwd
`
