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
	"context"
	"encoding/xml"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/retry"
)

// This test runs hibernate-core's full test suite against an single cockroach
// node.

func registerHibernate(r *registry) {
	runHibernate := func(
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

		t.Status("cloning hibernate and installing prerequisites")
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
				ctx, node, `sudo apt-get -qy install default-jre openjdk-8-jdk-headless gradle`,
			); err != nil {
				continue
			}

			c.l.Printf("attempt %d - cloning hibernate", attempt)
			if err := c.RunE(ctx, node, `rm -rf /mnt/data1/hibernate`); err != nil {
				continue
			}
			if err := c.GitCloneE(
				ctx,
				"https://github.com/hibernate/hibernate-orm.git",
				"/mnt/data1/hibernate",
				"5.3.6",
				node,
			); err != nil {
				continue
			}

			// In order to get Hibernate's test suite to connect to cockroach, we have
			// to create a dbBundle as it not possible to specify the individual
			// properties. So here we just steamroll the file with our own config.
			if err := c.RunE(ctx, node, `
echo "ext {
  db = project.hasProperty('db') ? project.getProperty('db') : 'h2'
    dbBundle = [
     cockroach : [
       'db.dialect' : 'org.hibernate.dialect.PostgreSQL95Dialect',
       'jdbc.driver': 'org.postgresql.Driver',
       'jdbc.user'  : 'root',
       'jdbc.pass'  : '',
       'jdbc.url'   : 'jdbc:postgresql://localhost:26257/defaultdb?sslmode=disable'
     ],
    ]
}" > /mnt/data1/hibernate/gradle/databases.gradle`,
			); err != nil {
				continue
			}

			break
		}

		t.Status("building hibernate (without tests)")
		for attempt, r := 0, retry.StartWithCtx(ctx, opts); r.Next(); {
			if ctx.Err() != nil {
				return
			}
			if c.t.Failed() {
				return
			}
			attempt++
			c.l.Printf("attempt %d - building hibernate", attempt)
			// Build hibernate and run a single test, this step involves some
			// downloading, so it needs a retry loop as well. Just building was not
			// enough as the test libraries are not downloaded unless at least a
			// single test is invoked.
			if err := c.RunE(
				ctx, node, `cd /mnt/data1/hibernate/hibernate-core/ && ./../gradlew test -Pdb=cockroach `+
					`--tests org.hibernate.jdbc.util.BasicFormatterTest.*`,
			); err != nil {
				continue
			}
			break
		}

		version, err := fetchCockroachVersion(ctx, c, node[0])
		if err != nil {
			t.Fatal(err)
		}
		blacklistName, expectedFailures := getHibernateBlacklistForVersion(version)
		if expectedFailures == nil {
			t.Fatalf("No hibernate blacklist defined for cockroach version %s", version)
		}
		c.l.Printf("Running cockroach version %s, using blacklist %s", version, blacklistName)

		t.Status("running hibernate test suite, will take at least 3 hours")
		// When testing, it is helpful to run only a subset of the tests. To do so
		// add "--tests org.hibernate.test.annotations.lob.*" to the end of the
		// test run command.
		// Note that this will take upwards of 3 hours.
		// Also note that this is expected to return an error, since the test suite
		// will fail. And it is safe to swallow it here.
		_ = c.RunE(ctx, node,
			`cd /mnt/data1/hibernate/hibernate-core/ && ./../gradlew test -Pdb=cockroach`,
		)

		t.Status("collecting the test results")
		// Copy all of the test results to the cockroach logs directory to be
		// copied to the artifacts.

		// Copy the html report for the test.
		c.Run(ctx, node,
			`cp /mnt/data1/hibernate/hibernate-core/target/reports/tests/test ~/logs/report -a`,
		)

		// Copy the individual test result files.
		c.Run(ctx, node,
			`cp /mnt/data1/hibernate/hibernate-core/target/test-results/test ~/logs/report/results -a`,
		)

		// Load the list of all test results files and parse them individually.
		// Files are here: /mnt/data1/hibernate/hibernate-core/target/test-results/test
		output, err := c.RunWithBuffer(ctx, t.l, node,
			`ls /mnt/data1/hibernate/hibernate-core/target/test-results/test/*.xml`,
		)
		if err != nil {
			t.Fatal(err)
		}

		var failUnexpectedCount, failExpectedCount int
		var passUnexpectedCount, passExpectedCount, notRunCount int
		// Put all the results in a giant map of [testname]result
		results := make(map[string]string)
		// Current failures are any tests that reported as failed, regardless of if
		// they were expected or not.
		var currentFailures, allTests []string
		runTests := make(map[string]struct{})
		files := strings.Split(string(output), "\n")
		for i, file := range files {
			file = strings.TrimSpace(file)
			if len(file) == 0 {
				t.l.Printf("Skipping %d of %d: %s\n", i, len(files), file)
				continue
			}
			t.l.Printf("Parsing %d of %d: %s\n", i, len(files), file)
			fileOutput, err := c.RunWithBuffer(ctx, t.l, node, `cat `+file)
			if err != nil {
				t.Fatal(err)
			}
			tests, passed, err := extractFailureFromHibernateXML(fileOutput)
			if err != nil {
				t.Fatal(err)
			}
			for i, test := range tests {
				// There is at least a single test that's run twice, so if we already
				// have a result, skip it.
				if _, alreadyTested := results[test]; alreadyTested {
					continue
				}
				allTests = append(allTests, test)
				issue, expectedFailure := expectedFailures[test]
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
					results[test] = fmt.Sprintf("--- FAIL: %s (unexpected)", test)
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

		// Log all the test results. We re-order the tests alphabetically here as
		// gradle picks the test order based on a directory walk.
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
		fmt.Fprintf(&bResults, "%d Total Test Run\n",
			passExpectedCount+passUnexpectedCount+failExpectedCount+failUnexpectedCount,
		)
		fmt.Fprintf(&bResults, "%d tests passed\n", passUnexpectedCount+passExpectedCount)
		fmt.Fprintf(&bResults, "%d tests failed\n", failUnexpectedCount+failExpectedCount)
		fmt.Fprintf(&bResults, "%d tests passed unexpectedly\n", passUnexpectedCount)
		fmt.Fprintf(&bResults, "%d tests failed unexpectedly\n", failUnexpectedCount)
		fmt.Fprintf(&bResults, "%d tests expected failed, but not run \n", notRunCount)
		fmt.Fprintf(&bResults, "For a full summary, look at artifacts/hibernate/logs/report/index.html\n")
		t.l.Printf("%s\n", bResults.String())
		t.l.Printf("------------------------\n")

		if failUnexpectedCount > 0 || passUnexpectedCount > 0 || notRunCount > 0 {
			// Create a new hibernate_blacklist so we can easily update this test.
			sort.Strings(currentFailures)
			var b strings.Builder
			fmt.Fprintf(&b, "Here is new hibernate blacklist that can be used to update the test:\n\n")
			fmt.Fprintf(&b, "var %s = blacklist{\n", blacklistName)
			for _, test := range currentFailures {
				issue := expectedFailures[test]
				if len(issue) == 0 {
					issue = "unknown"
				}
				fmt.Fprintf(&b, "  \"%s\": \"%s\",\n", test, issue)
			}
			fmt.Fprintf(&b, "}\n\n")
			t.l.Printf("\n\n%s\n\n", b.String())
			t.l.Printf("------------------------\n")
			t.Fatalf("\n"+
				"%d tests failed unexpectedly\n"+
				"%d tests passed unexpectedly\n"+
				"%d tests were not run that were expected to fail\n"+
				"See artifacts/hibernate/log/logs/report for more details",
				failUnexpectedCount, passUnexpectedCount, notRunCount,
			)
		}
	}

	r.Add(testSpec{
		Name:   "hibernate",
		Nodes:  nodes(1),
		Stable: false,
		Run: func(ctx context.Context, t *test, c *cluster) {
			runHibernate(ctx, t, c)
		},
	})
}

func extractFailureFromHibernateXML(contents []byte) ([]string, []bool, error) {
	type Failure struct {
		Message string `xml:"message,attr"`
	}
	type TestCase struct {
		Name      string  `xml:"name,attr"`
		ClassName string  `xml:"classname,attr"`
		Failure   Failure `xml:"failure,omitempty"`
	}
	type TestSuite struct {
		XMLName   xml.Name   `xml:"testsuite"`
		TestCases []TestCase `xml:"testcase"`
	}

	var testSuite TestSuite
	_ = testSuite.XMLName

	if err := xml.Unmarshal(contents, &testSuite); err != nil {
		return nil, nil, err
	}

	var tests []string
	var passed []bool
	for _, testCase := range testSuite.TestCases {
		tests = append(tests, fmt.Sprintf("%s.%s", testCase.ClassName, testCase.Name))
		passed = append(passed, len(testCase.Failure.Message) == 0)
	}

	return tests, passed, nil
}

func fetchCockroachVersion(ctx context.Context, c *cluster, nodeIndex int) (string, error) {
	db, err := c.ConnE(ctx, nodeIndex)
	if err != nil {
		return "", err
	}
	defer db.Close()
	var version string
	if err := db.QueryRowContext(ctx,
		`SELECT value FROM crdb_internal.node_build_info where field = 'Version'`,
	).Scan(&version); err != nil {
		return "", err
	}
	return version, nil
}

// maybeAddGithubLink will take the issue and if it is just a number, then it
// will return a full github link.
func maybeAddGithubLink(issue string) string {
	if len(issue) == 0 {
		return ""
	}
	issueNum, err := strconv.Atoi(issue)
	if err != nil {
		return issue
	}
	return fmt.Sprintf("https://github.com/cockroachdb/cockroach/issues/%d", issueNum)
}
