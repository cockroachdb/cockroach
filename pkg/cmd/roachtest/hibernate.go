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
	"context"
	"encoding/xml"
	"fmt"
	"regexp"
	"sort"
	"strings"
)

var hibernateReleaseTagRegex = regexp.MustCompile(`^(?P<major>\d+)\.(?P<minor>\d+)\.(?P<point>\d+)$`)

// This test runs hibernate-core's full test suite against a single cockroach
// node.

func registerHibernate(r *testRegistry) {
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
		latestTag, err := repeatGetLatestTag(
			ctx, c, "hibernate", "hibernate-orm", hibernateReleaseTagRegex,
		)
		if err != nil {
			t.Fatal(err)
		}
		c.l.Printf("Latest Hibernate release is %s.", latestTag)

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
			`sudo apt-get -qq install default-jre openjdk-8-jdk-headless gradle`,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatRunE(
			ctx, c, node, "remove old Hibernate", `rm -rf /mnt/data1/hibernate`,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatGitCloneE(
			ctx,
			t.l,
			c,
			"https://github.com/hibernate/hibernate-orm.git",
			"/mnt/data1/hibernate",
			latestTag,
			node,
		); err != nil {
			t.Fatal(err)
		}

		// In order to get Hibernate's test suite to connect to cockroach, we have
		// to create a dbBundle as it not possible to specify the individual
		// properties. So here we just steamroll the file with our own config.
		if err := repeatRunE(
			ctx,
			c,
			node,
			"configuring tests for cockroach only",
			fmt.Sprintf(
				"echo \"%s\" > /mnt/data1/hibernate/gradle/databases.gradle", hibernateDatabaseGradle,
			),
		); err != nil {
			t.Fatal(err)
		}

		t.Status("building hibernate (without tests)")
		// Build hibernate and run a single test, this step involves some
		// downloading, so it needs a retry loop as well. Just building was not
		// enough as the test libraries are not downloaded unless at least a
		// single test is invoked.
		if err := repeatRunE(
			ctx,
			c,
			node,
			"building hibernate (without tests)",
			`cd /mnt/data1/hibernate/hibernate-core/ && ./../gradlew test -Pdb=cockroach `+
				`--tests org.hibernate.jdbc.util.BasicFormatterTest.*`,
		); err != nil {
			t.Fatal(err)
		}

		version, err := fetchCockroachVersion(ctx, c, node[0])
		if err != nil {
			t.Fatal(err)
		}
		blacklistName, expectedFailures, _, _ := hibernateBlacklists.getLists(version)
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
		if err := repeatRunE(
			ctx,
			c,
			node,
			"copy html report",
			`cp /mnt/data1/hibernate/hibernate-core/target/reports/tests/test ~/logs/report -a`,
		); err != nil {
			t.Fatal(err)
		}

		// Copy the individual test result files.
		if err := repeatRunE(
			ctx,
			c,
			node,
			"copy test result files",
			`cp /mnt/data1/hibernate/hibernate-core/target/test-results/test ~/logs/report/results -a`,
		); err != nil {
			t.Fatal(err)
		}

		// Load the list of all test results files and parse them individually.
		// Files are here: /mnt/data1/hibernate/hibernate-core/target/test-results/test
		output, err := repeatRunWithBuffer(
			ctx,
			c,
			t.l,
			node,
			"get list of test files",
			`ls /mnt/data1/hibernate/hibernate-core/target/test-results/test/*.xml`,
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
		fmt.Fprintf(&bResults, "Tests run on Cockroach %s\n", version)
		fmt.Fprintf(&bResults, "Tests run against Hibernate %s\n", latestTag)
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

		fmt.Fprintf(&bResults, "For a full summary look at the hibernate artifacts \n")
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
			t.Fatalf("\n%s\nAn updated blacklist (%s) is available in the artifacts' hibernate log\n",
				bResults.String(),
				blacklistName,
			)
		}
	}

	r.Add(testSpec{
		Name:    "hibernate",
		Cluster: makeClusterSpec(1),
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

const hibernateDatabaseGradle = `
ext {
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
}
`
