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
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

var (
	// Currently, we're running a version like 'v9.0.0-beta.15'.
	gopgReleaseTagRegex         = regexp.MustCompile(`^v(?P<major>\d+)(?:\.(?P<minor>\d+)(?:\.(?P<point>\d+)(?:-beta\.(?P<subpoint>\d+))?)?)?$`)
	gopgResultSummaryStartRegex = regexp.MustCompile(`Summarizing [\d]+ Failures:`)
	gopgResultSummaryEndRegex   = regexp.MustCompile(`Ran (?P<runCount>[\d]+) of (?P<testCount>[\d]+) Specs in [\d]+\.[\d]+ seconds`)
	// The test failures are of the form '[Fail] DB.Select [It] selects bytea'.
	// We assign each test a name of the form '<class>/<invocation>/<name>'.
	gopgTestFailureRegex = regexp.MustCompile(`\[(Fail|Panic!)\] (?P<class>.*) \[(?P<invocation>.*)\] (?P<name>.*)`)
)

const (
	destPath  = `/mnt/data1/go-pg/pg`
	goVersion = `go1.12.10`
)

// This test runs gopg full test suite against a single cockroach node.

func registerGopg(r *testRegistry) {
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

		t.Status("cloning gopg and installing prerequisites")
		latestTag, err := repeatGetLatestTag(ctx, c, "go-pg", "pg", gopgReleaseTagRegex)
		if err != nil {
			t.Fatal(err)
		}
		c.l.Printf("Latest gopg release is %s.", latestTag)

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
			`sudo apt-get -qq install build-essential`,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatRunE(
			ctx,
			c,
			node,
			"install golang",
			fmt.Sprintf(`wget -q https://dl.google.com/go/%s.linux-amd64.tar.gz &&
							sudo tar -xf %s.linux-amd64.tar.gz &&
							sudo mv go /usr/local`,
				goVersion, goVersion),
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
				`cd %s && grep -rli '5432' * | xargs -i@ sed -i 's/5432/26257/g' @`,
				destPath),
		); err != nil {
			t.Fatal(err)
		}

		// Similarly, it is not clear on how to fully disable SSL, so we simply
		// remove the default TLSConfig connection parameter from db_test.go (this
		// way gopg will always think that we're running in insecure mode - which
		// we are in this test).
		if err := repeatRunE(
			ctx, c, node, "remove TLSConfig default parameter",
			fmt.Sprintf(
				`cd %s &&
				grep -v '"crypto/tls"' db_test.go | sed -e '/TLSConfig\: \&tls.Config/,/}\,/d' > tmp &&
				mv tmp db_test.go`, destPath),
		); err != nil {
			t.Fatal(err)
		}

		version, err := fetchCockroachVersion(ctx, c, node[0])
		if err != nil {
			t.Fatal(err)
		}
		blacklistName, expectedFailures, _, _ := gopgBlacklists.getLists(version)
		if expectedFailures == nil {
			t.Fatalf("No gopg blacklist defined for cockroach version %s", version)
		}
		c.l.Printf("Running cockroach version %s, using blacklist %s",
			version, blacklistName)

		// Also, it is not clear on how to change the user and the database, so we
		// roll with the default options ('postgres' user and 'postgres' database).
		t.Status("creating user 'postgres' with ALL permissions on postgres db")
		db := c.Conn(ctx, 1)
		defer db.Close()
		if _, err := db.Exec("CREATE USER 'postgres'; " +
			"GRANT ALL ON DATABASE postgres TO postgres;"); err != nil {
			t.Fatal(err)
		}

		t.Status("running gopg test suite")
		// Note that this is expected to return an error, since the test suite
		// will fail. And it is safe to swallow it here.
		rawResults, _ := c.RunWithBuffer(ctx, t.l, node,
			fmt.Sprintf(`cd %s &&
							export GOROOT=/usr/local/go &&
							export PATH=$PATH:/usr/local/go/bin &&
							go test`, destPath),
		)

		t.Status("collating the test results")
		c.l.Printf("Test Results: %s", rawResults)

		// Find all the failed and errored tests.

		var failUnexpectedCount, failExpectedCount int
		var passUnexpectedCount, passExpectedCount int
		var runCount, totalCount int
		// Current failures are any tests that reported as failed, regardless of if
		// they were expected or not.
		var currentFailures []string

		scanner := bufio.NewScanner(bytes.NewReader(rawResults))
		for scanner.Scan() {
			line := scanner.Bytes()
			// The test output is huge, but it doesn't print out the passed tests, so
			// we focus only on the failed ones. Nicely, the output does provide a
			// summary of all failed tests, so we skip everything until we get to it.
			if gopgResultSummaryStartRegex.Match(line) {
				for scanner.Scan() {
					line = scanner.Bytes()
					// The summary ends with a line like
					// 'Ran 134 of 134 Specs in 91.782 seconds',
					// so we first check whether we have reached the end of the summary,
					// and if so, we get the runCount and totalCount and stop parsing the
					// test output.
					match := gopgResultSummaryEndRegex.FindSubmatch(line)
					if match != nil {
						runCount, err = strconv.Atoi(string(match[1]))
						if err != nil {
							panic(err)
						}
						totalCount, err = strconv.Atoi(string(match[2]))
						if err != nil {
							panic(err)
						}
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
					match = gopgTestFailureRegex.FindSubmatch(line)
					if match != nil {
						class := string(match[2])
						invocation := string(match[3])
						name := strings.TrimSpace(string(match[4]))
						failedTest := fmt.Sprintf("%s/%s/%s", class, invocation, name)
						currentFailures = append(currentFailures, failedTest)
						if _, ok := expectedFailures[failedTest]; ok {
							failExpectedCount++
						} else {
							failUnexpectedCount++
						}
					}
				}
			}
		}

		passCount := runCount - failExpectedCount - failUnexpectedCount
		passUnexpectedCount = len(expectedFailures) - failExpectedCount
		passExpectedCount = passCount - passUnexpectedCount
		notRunCount := totalCount - runCount

		printORMTestsResults(
			t, "gopg", blacklistName, expectedFailures, version, latestTag,
			currentFailures, failUnexpectedCount, failExpectedCount,
			0 /* ignoredCount */, 0, /* skipCount */
			0 /* unexpectedSkipCount */, passUnexpectedCount,
			passExpectedCount, notRunCount, nil /* allIssueHints */)
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
