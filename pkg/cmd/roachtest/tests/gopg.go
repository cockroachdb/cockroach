// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tests

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/errors"
)

// Currently, we're running a version like 'v9.0.1'.
var gopgReleaseTagRegex = regexp.MustCompile(`^v(?P<major>\d+)(?:\.(?P<minor>\d+)(?:\.(?P<point>\d+))?)?$`)
var gopgSupportedTag = "v10.9.0"

// This test runs gopg full test suite against a single cockroach node.
func registerGopg(r registry.Registry) {
	const (
		destPath        = `/mnt/data1/go-pg/pg`
		resultsDirPath  = `~/logs/report/gopg-results`
		resultsFilePath = resultsDirPath + `/raw_results`
	)

	runGopg := func(
		ctx context.Context,
		t test.Test,
		c cluster.Cluster,
	) {
		if c.IsLocal() {
			t.Fatal("cannot be run in local mode")
		}
		node := c.Node(1)
		t.Status("setting up cockroach")
		c.Put(ctx, t.Cockroach(), "./cockroach", c.All())
		c.Start(ctx, c.All())
		version, err := fetchCockroachVersion(ctx, c, node[0], nil)
		if err != nil {
			t.Fatal(err)
		}

		if err := alterZoneConfigAndClusterSettings(
			ctx, version, c, node[0], nil,
		); err != nil {
			t.Fatal(err)
		}

		t.Status("cloning gopg and installing prerequisites")
		gopgLatestTag, err := repeatGetLatestTag(ctx, t, "go-pg", "pg", gopgReleaseTagRegex)
		if err != nil {
			t.Fatal(err)
		}
		t.L().Printf("Latest gopg release is %s.", gopgLatestTag)
		t.L().Printf("Supported gopg release is %s.", gopgSupportedTag)

		installGolang(ctx, t, c, node)

		if err := repeatRunE(
			ctx, t, c, node, "remove old gopg",
			fmt.Sprintf(`sudo rm -rf %s`, destPath),
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatGitCloneE(
			ctx,
			t,
			c,
			"https://github.com/go-pg/pg.git",
			destPath,
			gopgSupportedTag,
			node,
		); err != nil {
			t.Fatal(err)
		}

		blocklistName, expectedFailures, ignorelistName, ignorelist := gopgBlocklists.getLists(version)
		if expectedFailures == nil {
			t.Fatalf("No gopg blocklist defined for cockroach version %s", version)
		}
		if ignorelist == nil {
			t.Fatalf("No gopg ignorelist defined for cockroach version %s", version)
		}
		t.L().Printf("Running cockroach version %s, using blocklist %s, using ignorelist %s",
			version, blocklistName, ignorelistName)

		if err := c.RunE(ctx, node, fmt.Sprintf("mkdir -p %s", resultsDirPath)); err != nil {
			t.Fatal(err)
		}
		t.Status("running gopg test suite")

		// go test provides colorful output which - when redirected - interferes
		// with matching of the blocklisted tests, so we will strip off all color
		// code escape sequences.
		const removeColorCodes = `sed -r "s/\x1B\[([0-9]{1,2}(;[0-9]{1,2})?)?[mGK]//g"`
		// Note that this is expected to return an error, since the test suite
		// will fail. And it is safe to swallow it here.
		rawResults, _ := c.RunWithBuffer(ctx, t.L(), node,
			fmt.Sprintf(
				`cd %s && PGPORT=26257 PGUSER=root PGSSLMODE=disable PGDATABASE=postgres go test -v ./... 2>&1 | %s | tee %s`,
				destPath, removeColorCodes, resultsFilePath),
		)

		t.Status("collating the test results")
		t.L().Printf("Test Results: %s", rawResults)
		results := newORMTestsResults()

		// gopg test suite consists of multiple tests, some of them being a full
		// test suites in themselves. Those are run with TestGinkgo test harness.
		// First, we parse the result of running TestGinkgo.
		if err := gopgParseTestGinkgoOutput(
			results, rawResults, expectedFailures, ignorelist,
		); err != nil {
			t.Fatal(err)
		}

		// Now we parse the output of top-level tests.

		// Note that this is expected to return an error, since the test suite
		// will fail. And it is safe to swallow it here.
		xmlResults, _ := c.RunWithBuffer(ctx, t.L(), node,
			// We pipe the test output into go-junit-report tool which will output
			// it in XML format.
			fmt.Sprintf(`cd %s &&
							GOPATH=%s go get -u github.com/jstemmer/go-junit-report &&
							cat %s | %s/bin/go-junit-report`,
				destPath, goPath, resultsFilePath, goPath),
		)

		results.parseJUnitXML(t, expectedFailures, ignorelist, xmlResults)
		results.summarizeFailed(
			t, "gopg", blocklistName, expectedFailures, version, gopgSupportedTag,
			0, /* notRunCount */
		)
	}

	r.Add(registry.TestSpec{
		Name:    "gopg",
		Owner:   registry.OwnerSQLExperience,
		Cluster: r.MakeClusterSpec(1),
		Tags:    []string{`default`, `orm`},
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runGopg(ctx, t, c)
		},
	})
}

// gopgParseTestGinkgoOutput parses the summary of failures of running internal
// test suites from gopg ORM tests. TestGinkgo is a test harness that runs
// several test suites described by gopg.
func gopgParseTestGinkgoOutput(
	r *ormTestsResults, rawResults []byte, expectedFailures, ignorelist blocklist,
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

	// Blocklist contains both the expected failures for "global" tests as well
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
