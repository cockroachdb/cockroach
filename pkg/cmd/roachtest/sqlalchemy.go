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
	"unicode"
)

var sqlAlchemyResultRegex = regexp.MustCompile(`(?P<name>.*::.*::.*) (?P<result>PASSED|ERROR|FAILED)`)

var sqlAlchemyTestsets = map[string]string{
	"sqlalchemy1": "ABCDEFGHIJKLMNOPQRSTUVWXYZabc",
	"sqlalchemy2": "defghi",
	"sqlalchemy3": "jklmn",
	"sqlalchemy4": "opqr",
	"sqlalchemy5": "stuvwxyz1234567890",
}

var knownTestCrashes = []string{
	"test_join_cache",
	"test_mapper_reset",
	"test_savepoints",
	"test_unicode_warnings",
	"test_labeled_on_limitid_alias", // Panic #35437
}

// This test runs SQLAlchemy's full core test suite against an single cockroach
// node.

func registerSQLAlchemy(r *registry) {
	runSQLAlchemy := func(
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

		db := c.Conn(ctx, c.nodes)
		defer func() {
			_ = db.Close()
		}()

		// Get the testset based on the test name.
		sqlAlchemyTestset, ok := sqlAlchemyTestsets[t.Name()]
		if !ok {
			t.Fatalf("Could not find test set for %s", t.Name())
		}

		// Create a dbs for better isolation and convert the list of testsets into
		// an array.
		var remainingTestsets []string
		for _, l := range sqlAlchemyTestset {
			letter := string(l)
			remainingTestsets = append(remainingTestsets, letter)
			if unicode.IsUpper(l) {
				letter = strings.ToLower(letter + letter)
			}
			if _, err := db.Exec(fmt.Sprintf("create database db%s", letter)); err != nil {
				t.Fatal(err)
			}
		}

		t.Status("cloning SQL Alchemy and installing prerequisites")

		if err := c.RunE(ctx, c.All(), `sudo apt-get -qq update`); err != nil {
			t.Fatal(err)
		}
		if err := c.RunE(
			ctx, c.All(), `sudo apt-get -qq install make python3 libpq-dev python-dev gcc`,
		); err != nil {
			t.Fatal(err)
		}
		if err := c.RunE(
			ctx, c.All(), `curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py`,
		); err != nil {
			t.Fatal(err)
		}
		if err := c.RunE(
			ctx, c.All(), `sudo python3 get-pip.py`,
		); err != nil {
			t.Fatal(err)
		}
		if err := c.RunE(
			ctx, c.All(), `sudo pip install pytest -q`,
		); err != nil {
			t.Fatal(err)
		}
		if err := c.RunE(
			ctx, c.All(), `sudo pip install psycopg2-binary -q`,
		); err != nil {
			t.Fatal(err)
		}
		if err := c.RunE(ctx, c.All(), `rm -rf /mnt/data1/sqlalchemy`); err != nil {
			t.Fatal(err)
		}
		if err := c.GitCloneE(
			ctx,
			"https://github.com/sqlalchemy/sqlalchemy.git",
			"/mnt/data1/sqlalchemy",
			"rel_1_2_18",
			c.All(),
		); err != nil {
			t.Fatal(err)
		}

		// SQLAlchemy does a version check in a weird way. Override that check.
		if err := c.RunE(
			ctx, c.All(),
			`sed -i 's/`+
				`        v = connection.execute("select version()").scalar()/`+
				`        v = connection.execute("select version()").scalar()\n`+
				`        if v.startswith("CockroachDB"):\n`+
				`            return 9, 5, None/'`+
				` /mnt/data1/sqlalchemy/lib/sqlalchemy/dialects/postgresql/base.py`,
		); err != nil {
			t.Fatal(err)
		}

		// Check the version of Cockroach and find the correct blacklist.
		version, err := fetchCockroachVersion(ctx, c, c.Node(1)[0])
		if err != nil {
			t.Fatal(err)
		}
		// Prepend the version with the test name so we can have different
		// blacklists for each.
		prependedVersion := fmt.Sprintf("%s %s", t.Name(), version)
		blacklistName, expectedFailureList, _, _ := sqlalchemyBlacklists.getLists(prependedVersion)
		if expectedFailureList == nil {
			t.Fatalf("No SQL Alchemy blacklist defined for cockroach version %s with test set %s -- %s",
				version, t.Name(), prependedVersion,
			)
		}
		c.l.Printf("Running cockroach version %s, using blacklist %s", version, blacklistName)

		t.Status("running SQL Alchemy test suite")
		var excludedTests string
		for _, badTest := range knownTestCrashes {
			excludedTests += " and not " + badTest
		}

		runTestCommand := func(testset string) string {
			dbName := testset
			if unicode.IsUpper(rune(testset[0])) {
				dbName = strings.ToLower(dbName + dbName)
			}
			return fmt.Sprintf("cd /mnt/data1/sqlalchemy/ && "+
				"py.test --dburi=\"postgresql://root@localhost:26257/db%s?sslmode=disable\" "+
				"--maxfail=10000 "+
				"-k \"test_%s%s\" ", dbName, testset, excludedTests)
		}

		// Find all the failed and errored tests.
		var ignoredlist blacklist
		var failUnexpectedCount, failExpectedCount, ignoredCount int
		var passUnexpectedCount, passExpectedCount, notRunCount int
		// Put all the results in a giant map of [testname]result
		results := make(map[string]string)
		// Current failures are any tests that reported as failed, regardless of if
		// they were expected or not.
		var currentFailures, allTests []string
		runTests := make(map[string]struct{})

		getNextTestset := func() string {
			if t.Failed() || ctx.Err() != nil {
				return ""
			}
			if len(remainingTestsets) == 0 {
				return ""
			}
			testset := remainingTestsets[0]
			remainingTestsets = remainingTestsets[1:]
			return testset
		}

		collateResults := func(testset string, rawResults []byte) {
			scanner := bufio.NewScanner(bytes.NewReader(rawResults))
			for scanner.Scan() {
				match := sqlAlchemyResultRegex.FindStringSubmatch(scanner.Text())
				if match != nil {
					groups := map[string]string{}
					for i, name := range match {
						groups[sqlAlchemyResultRegex.SubexpNames()[i]] = name
					}
					// There are duplicate tests that match the passed in subset. So
					// apened the test set letter to the end of the test.
					test := fmt.Sprintf("%s--%s", groups["name"], testset)
					pass := groups["result"] == "PASSED"

					// Check for duplicates within the set and add a number. Luckily,
					// all tests are preformed in the same order on each run.
					x := 0
					for {
						newTestName := fmt.Sprintf("%s:%d", test, x)
						if _, ok := runTests[newTestName]; !ok {
							break
						}
						x++
					}
					test = fmt.Sprintf("%s:%d", test, x)

					runTests[test] = struct{}{}
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
				}
			}
		}

		for testset := getNextTestset(); len(testset) > 0; testset = getNextTestset() {
			t.Status(fmt.Sprintf("running SQL Alchemy test suite for tests %s", testset))
			c.l.Printf("running SQL Alchemy test suite for tests %s", testset)
			rawResults, _ := c.RunWithBuffer(ctx, t.l, node, runTestCommand(testset))
			c.l.Printf("Test Results for tests %s:\n%s", testset, rawResults)

			if t.Failed() || ctx.Err() != nil {
				return
			}

			c.l.Printf("collating the test results for tests %s", testset)
			collateResults(testset, rawResults)
			c.l.Printf("-- Remaining test sets: %v", remainingTestsets)
		}

		t.Status("collating all test results")

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

		fmt.Fprintf(&bResults, "For a full summary look at the sqlalchemy artifacts. \n")
		t.l.Printf("%s\n", bResults.String())
		t.l.Printf("------------------------\n")

		if failUnexpectedCount > 0 || passUnexpectedCount > 0 || notRunCount > 0 {
			// Create a new blacklist so we can easily update this test.
			sort.Strings(currentFailures)
			var b strings.Builder
			fmt.Fprintf(&b, "Here is new SQL Alchemy blacklist that can be used to update the test:\n\n")
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
		Name:    "sqlalchemy1",
		Cluster: makeClusterSpec(1),
		Run: func(ctx context.Context, t *test, c *cluster) {
			runSQLAlchemy(ctx, t, c)
		},
	})
	r.Add(testSpec{
		Name:    "sqlalchemy2",
		Cluster: makeClusterSpec(1),
		Run: func(ctx context.Context, t *test, c *cluster) {
			runSQLAlchemy(ctx, t, c)
		},
	})
	r.Add(testSpec{
		Name:    "sqlalchemy3",
		Cluster: makeClusterSpec(1),
		Run: func(ctx context.Context, t *test, c *cluster) {
			runSQLAlchemy(ctx, t, c)
		},
	})
	r.Add(testSpec{
		Name:    "sqlalchemy4",
		Cluster: makeClusterSpec(1),
		Run: func(ctx context.Context, t *test, c *cluster) {
			runSQLAlchemy(ctx, t, c)
		},
	})
	r.Add(testSpec{
		Name:    "sqlalchemy5",
		Cluster: makeClusterSpec(1),
		Run: func(ctx context.Context, t *test, c *cluster) {
			runSQLAlchemy(ctx, t, c)
		},
	})
}
