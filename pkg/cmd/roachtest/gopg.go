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
	"context"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// Currently, we're running a version like 'v9.0.0-beta.15'.
var gopgReleaseTagRegex = regexp.MustCompile(`^v(?P<major>\d+)(?:\.(?P<minor>\d+)(?:\.(?P<point>\d+)(?:-beta\.(?P<subpoint>\d+))?)?)?$`)

// This test runs gopg full test suite against a single cockroach node.
func registerGopg(r *testRegistry) {
	const (
		destPath        = `/mnt/data1/go-pg/pg`
		resultsDirPath  = `~/logs/report/gopg-results`
		resultsFilePath = resultsDirPath + `/raw_results`
	)

	runGopg := func(
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
		version, err := fetchCockroachVersion(ctx, c, node[0])
		if err != nil {
			t.Fatal(err)
		}

		if err := alterZoneConfigAndClusterSettings(ctx, version, c, node[0]); err != nil {
			t.Fatal(err)
		}

		t.Status("cloning gopg and installing prerequisites")
		latestTag, err := repeatGetLatestTag(ctx, c, "go-pg", "pg", gopgReleaseTagRegex)
		if err != nil {
			t.Fatal(err)
		}
		c.l.Printf("Latest gopg release is %s.", latestTag)

		installLatestGolang(ctx, t, c, node)

		if err := repeatRunE(
			ctx,
			c,
			node,
			"install dependencies",
			`sudo apt-get -qq install build-essential`,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatRunE(
			ctx, c, node, "remove old gopg",
			fmt.Sprintf(`sudo rm -rf %s`, destPath),
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatGitCloneE(
			ctx,
			t.l,
			c,
			"https://github.com/go-pg/pg.git",
			destPath,
			latestTag,
			node,
		); err != nil {
			t.Fatal(err)
		}

		// There is no environment variable to set the port correctly, so we simply
		// replace in the code all Postgres's 5432 ports with 26257 for Cockroach.
		if err := repeatRunE(
			ctx, c, node, "update PSQL's 5432 to CRDB's 26257",
			fmt.Sprintf(
				`cd %s && grep -rlE '[^6]5432' * | xargs -i@ sed -i 's/5432/26257/g' @`,
				destPath),
		); err != nil {
			t.Fatal(err)
		}

		// Similarly, it is not clear on how to disable SSL, so we simply remove
		// the default TLSConfig connection parameter from db_test.go (this way
		// gopg will always think that we're running in insecure mode - which we
		// are in this test).
		if err := repeatRunE(
			ctx, c, node, "remove TLSConfig default parameter",
			fmt.Sprintf(
				`cd %s &&
				grep -v '"crypto/tls"' db_test.go | sed -e '/TLSConfig\: \&tls.Config/,/}\,/d' > tmp &&
				mv tmp db_test.go`, destPath),
		); err != nil {
			t.Fatal(err)
		}

		blacklistName, expectedFailures, ignorelistName, ignorelist := gopgBlacklists.getLists(version)
		if expectedFailures == nil {
			t.Fatalf("No gopg blacklist defined for cockroach version %s", version)
		}
		if ignorelist == nil {
			t.Fatalf("No gopg ignorelist defined for cockroach version %s", version)
		}
		c.l.Printf("Running cockroach version %s, using blacklist %s, using ignorelist %s",
			version, blacklistName, ignorelistName)

		// Also, it is not clear on how to change the user and the database, so we
		// roll with the default options ('postgres' user and 'postgres' database).
		t.Status("creating user 'postgres' with ALL permissions on postgres db")
		db := c.Conn(ctx, 1)
		defer db.Close()
		if _, err := db.Exec("CREATE USER 'postgres'; " +
			"GRANT ALL ON DATABASE postgres TO postgres;"); err != nil {
			t.Fatal(err)
		}

		_ = c.RunE(ctx, node, fmt.Sprintf("mkdir -p %s", resultsDirPath))
		t.Status("running gopg test suite")

		// go test provides colorful output which - when redirected - interferes
		// with matching of the blacklisted tests, so we will strip off all color
		// code escape sequences.
		const removeColorCodes = `sed -r "s/\x1B\[([0-9]{1,2}(;[0-9]{1,2})?)?[mGK]//g"`
		// Note that this is expected to return an error, since the test suite
		// will fail. And it is safe to swallow it here.
		rawResults, _ := c.RunWithBuffer(ctx, t.l, node,
			fmt.Sprintf(
				`cd %s && go test -v ./... 2>&1 | %s | tee %s`,
				destPath, removeColorCodes, resultsFilePath),
		)

		t.Status("collating the test results")
		c.l.Printf("Test Results: %s", rawResults)
		results := newORMTestsResults()

		// gopg test suite consists of multiple tests, some of them being a full
		// test suites in themselves. Those are run with TestGinkgo test harness.
		// First, we parse the result of running TestGinkgo.
		if err := gormParseTestGinkgoOutput(
			results, rawResults, expectedFailures, ignorelist,
		); err != nil {
			t.Fatal(err)
		}

		// Now we parse the output of top-level tests.

		// Note that this is expected to return an error, since the test suite
		// will fail. And it is safe to swallow it here.
		xmlResults, _ := c.RunWithBuffer(ctx, t.l, node,
			// We pipe the test output into go-junit-report tool which will output
			// it in XML format.
			fmt.Sprintf(`cd %s &&
							GOPATH=%s go get -u github.com/jstemmer/go-junit-report &&
							cat %s | %s/bin/go-junit-report`,
				destPath, goPath, resultsFilePath, goPath),
		)

		results.parseJUnitXML(t, expectedFailures, xmlResults)
		results.summarizeFailed(
			t, "gopg", blacklistName, expectedFailures, version, latestTag,
			0, /* notRunCount */
		)
	}

	r.Add(testSpec{
		Name:       "gopg",
		Cluster:    makeClusterSpec(1),
		MinVersion: "v19.2.0",
		Run: func(ctx context.Context, t *test, c *cluster) {
			runGopg(ctx, t, c)
		},
	})
}

// gormParseTestGinkgoOutput parses the summary of failures of running internal
// test suites from gopg ORM tests. TestGinkgo is a test harness that runs
// several test suites described by gopg.
func gormParseTestGinkgoOutput(
	r *ormTestsResults, rawResults []byte, expectedFailures, ignorelist blacklist,
) (err error) {
	var (
		totalRunCount, totalTestCount int
		testSuiteStart                = regexp.MustCompile(`Running Suite: (?P<testsuite>[\w]+)`)
		summaryStartRegex             = regexp.MustCompile(`Summarizing [\d]+ Failures?:`)
		summaryEndRegex               = regexp.MustCompile(`Ran (?P<runCount>[\d]+) of (?P<testCount>[\d]+) Specs? in [\d]+\.[\d]+ seconds`)
		// The test failures are of the form '[Fail] DB.Select [It] selects bytea'.
		// We assign each test a name of the form
		// '<test suite name> | <class> | <name>'.
		failureRegex                      = regexp.MustCompile(`\[(Fail|Panic!)\] (?P<class>.*) \[[\w]+\] (?P<name>.*)`)
		testGinkgoInternalTestNamePattern = `%s | %s | %s`
		testGinkgoInternalTestNameRE      = regexp.MustCompile(`.* | .* | .*`)
	)
	scanner := bufio.NewScanner(bytes.NewReader(rawResults))
	for scanner.Scan() {
		line := scanner.Bytes()
		if testSuiteStart.Match(line) {
			match := testSuiteStart.FindSubmatch(line)
			if match == nil {
				return errors.New("unexpectedly didn't find the name of the internal test suite")
			}
			testSuiteName := string(match[1])
			// The test output is huge, but it doesn't print out the passed tests, so
			// we focus only on the failed ones. Conveniently, the output does
			// provide a summary of all failed tests, so we skip everything until we
			// get to it.

			// First, we skip all the details about the failures until we get to the
			// summary.
			for scanner.Scan() {
				line = scanner.Bytes()
				if testSuiteStart.Match(line) {
					// We have reached the beginning of the new internal test suite which
					// means that the current suite didn't have any failures, so we need
					// to update the test suite name.
					match = testSuiteStart.FindSubmatch(line)
					if match == nil {
						return errors.New("unexpectedly didn't find the name of the internal test suite")
					}
					testSuiteName = string(match[1])
				} else if summaryStartRegex.Match(line) {
					break
				}
			}
			// Now we parse all failures for the test suite until we get to the end
			// of the summary.
			for scanner.Scan() {
				line = scanner.Bytes()
				// The summary ends with a line like
				// 'Ran 134 of 134 Specs in 91.782 seconds',
				// so we first check whether we have reached the end of the summary,
				// and if so, we get the runCount and totalCount and stop parsing the
				// test output.
				match = summaryEndRegex.FindSubmatch(line)
				if match != nil {
					var runCount, totalCount int
					runCount, err = strconv.Atoi(string(match[1]))
					if err != nil {
						return err
					}
					totalCount, err = strconv.Atoi(string(match[2]))
					if err != nil {
						return err
					}
					totalRunCount += runCount
					totalTestCount += totalCount
					break
				}
				// The summary consists of entries like
				// ```
				//
				// [Fail] DB.Select [It] selects bytea
				// /mnt/data1/go-pg/pg/db_test.go:831
				// ```
				// for each test failure. We ignore all empty lines as well as all
				// lines that point to the code and parse only the "core" of the
				// entry.
				match = failureRegex.FindSubmatch(line)
				if match != nil {
					class := string(match[2])
					name := strings.TrimSpace(string(match[3]))
					failedTest := fmt.Sprintf(testGinkgoInternalTestNamePattern, testSuiteName, class, name)
					if _, ignore := ignorelist[failedTest]; ignore {
						// We ignore this test failure.
						r.ignoredCount++
						continue
					}
					r.currentFailures = append(r.currentFailures, failedTest)
					if _, ok := expectedFailures[failedTest]; ok {
						r.failExpectedCount++
					} else {
						r.failUnexpectedCount++
					}
				}
			}
		}
	}

	// Blacklist contains both the expected failures for "global" tests as well
	// as TestGinkgo's tests. We need to figure the number of the latter ones.
	testGinkgoExpectedFailures := 0
	for failure := range expectedFailures {
		if testGinkgoInternalTestNameRE.MatchString(failure) {
			testGinkgoExpectedFailures++
		}
	}

	// Since the passed tests within TestGinkgo are not printed out, we need to
	// do a little bit of math to figure out the numbers below.
	passCount := totalRunCount - r.failExpectedCount - r.failUnexpectedCount
	r.passUnexpectedCount = testGinkgoExpectedFailures - r.failExpectedCount
	r.passExpectedCount = passCount - r.passUnexpectedCount
	return nil /* err */
}
