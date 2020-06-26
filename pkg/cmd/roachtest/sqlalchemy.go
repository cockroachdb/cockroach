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
	"strings"
)

var sqlAlchemyResultRegex = regexp.MustCompile(`^(?P<test>test.*::.*::[^ \[\]]*(?:\[.*])?) (?P<result>\w+)\s+\[.+]$`)
var sqlAlchemyReleaseTagRegex = regexp.MustCompile(`^rel_(?P<major>\d+)_(?P<minor>\d+)_(?P<point>\d+)$`)

var supportedSQLAlchemyTag = "rel_1_3_17"

// This test runs the SQLAlchemy dialect test suite against a single Cockroach
// node.

func registerSQLAlchemy(r *testRegistry) {
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

		version, err := fetchCockroachVersion(ctx, c, node[0])
		if err != nil {
			t.Fatal(err)
		}

		if err := alterZoneConfigAndClusterSettings(ctx, version, c, node[0]); err != nil {
			t.Fatal(err)
		}

		t.Status("cloning sqlalchemy and installing prerequisites")
		latestTag, err := repeatGetLatestTag(ctx, c, "sqlalchemy", "sqlalchemy", sqlAlchemyReleaseTagRegex)
		if err != nil {
			t.Fatal(err)
		}
		c.l.Printf("Latest sqlalchemy release is %s.", latestTag)
		c.l.Printf("Supported sqlalchemy release is %s.", supportedSQLAlchemyTag)

		if err := repeatRunE(
			ctx, c, node, "update apt-get",
			`
				sudo add-apt-repository ppa:deadsnakes/ppa &&
				sudo apt-get -qq update`,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatRunE(
			ctx,
			c,
			node,
			"install dependencies",
			`sudo apt-get -qq install make python3.7 libpq-dev python3.7-dev gcc python3-setuptools python-setuptools build-essential`,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatRunE(
			ctx, c, node, "set python3.7 as default", `
				sudo update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.5 1
 				sudo update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.7 2
 				sudo update-alternatives --config python3`,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatRunE(
			ctx, c, node, "install pip",
			`curl https://bootstrap.pypa.io/get-pip.py | sudo -H python3.7`,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatRunE(
			ctx,
			c,
			node,
			"install pytest",
			`sudo pip3 install --upgrade --force-reinstall setuptools pytest pytest-xdist psycopg2`,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatRunE(
			ctx, c, node, "remove old sqlalchemy-cockroachdb", `sudo rm -rf /mnt/data1/sqlalchemy-cockroachdb`,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatGitCloneE(
			ctx,
			t.l,
			c,
			"https://github.com/cockroachdb/sqlalchemy-cockroachdb.git",
			"/mnt/data1/sqlalchemy-cockroachdb",
			"master",
			node,
		); err != nil {
			t.Fatal(err)
		}

		t.Status("installing sqlalchemy-cockroachdb")
		if err := repeatRunE(
			ctx, c, node, "installing sqlalchemy=cockroachdb",
			`cd /mnt/data1/sqlalchemy-cockroachdb && sudo python3 setup.py install`,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatRunE(
			ctx, c, node, "remove old sqlalchemy", `sudo rm -rf /mnt/data1/sqlalchemy`,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatGitCloneE(
			ctx,
			t.l,
			c,
			"https://github.com/sqlalchemy/sqlalchemy.git",
			"/mnt/data1/sqlalchemy",
			supportedSQLAlchemyTag,
			node,
		); err != nil {
			t.Fatal(err)
		}

		t.Status("building sqlalchemy")
		if err := repeatRunE(
			ctx, c, node, "building sqlalchemy", `cd /mnt/data1/sqlalchemy && python3 setup.py build`,
		); err != nil {
			t.Fatal(err)
		}

		blocklistName, expectedFailures, ignoredlistName, ignoredlist := sqlAlchemyBlocklists.getLists(version)
		if expectedFailures == nil {
			t.Fatalf("No sqlalchemy blocklist defined for cockroach version %s", version)
		}
		c.l.Printf("Running cockroach version %s, using blocklist %s, using ignoredlist %s",
			version, blocklistName, ignoredlistName)

		t.Status("running sqlalchemy test suite")
		// Note that this is expected to return an error, since the test suite
		// will fail. And it is safe to swallow it here.
		rawResults, _ := c.RunWithBuffer(ctx, t.l, node,
			`cd /mnt/data1/sqlalchemy/ && pytest --maxfail=0 `+
				`--requirements=cockroachdb.sqlalchemy.test_requirements:Requirements `+
				`--dburi=cockroachdb://root@localhost:26257/defaultdb?sslmode=disable `+
				`test/dialect/test_suite.py`)

		t.Status("collating the test results")
		c.l.Printf("Test Results: %s", rawResults)

		// Find all the failed and errored tests.
		results := newORMTestsResults()

		scanner := bufio.NewScanner(bytes.NewReader(rawResults))
		for scanner.Scan() {
			match := sqlAlchemyResultRegex.FindStringSubmatch(scanner.Text())
			if match == nil {
				continue
			}
			test, result := match[1], match[2]
			pass := result == "PASSED" || strings.Contains(result, "failed as expected")
			skipped := result == "SKIPPED"
			results.allTests = append(results.allTests, test)

			ignoredIssue, expectedIgnored := ignoredlist[test]
			issue, expectedFailure := expectedFailures[test]
			switch {
			case expectedIgnored:
				results.results[test] = fmt.Sprintf("--- SKIP: %s due to %s (expected)", test, ignoredIssue)
				results.ignoredCount++
			case skipped && expectedFailure:
				results.results[test] = fmt.Sprintf("--- SKIP: %s (unexpected)", test)
				results.unexpectedSkipCount++
			case skipped:
				results.results[test] = fmt.Sprintf("--- SKIP: %s (expected)", test)
				results.skipCount++
			case pass && !expectedFailure:
				results.results[test] = fmt.Sprintf("--- PASS: %s (expected)", test)
				results.passExpectedCount++
			case pass && expectedFailure:
				results.results[test] = fmt.Sprintf("--- PASS: %s - %s (unexpected)",
					test, maybeAddGithubLink(issue),
				)
				results.passUnexpectedCount++
			case !pass && expectedFailure:
				results.results[test] = fmt.Sprintf("--- FAIL: %s - %s (expected)",
					test, maybeAddGithubLink(issue),
				)
				results.failExpectedCount++
				results.currentFailures = append(results.currentFailures, test)
			case !pass && !expectedFailure:
				results.results[test] = fmt.Sprintf("--- FAIL: %s (unexpected)", test)
				results.failUnexpectedCount++
				results.currentFailures = append(results.currentFailures, test)
			}
			results.runTests[test] = struct{}{}
		}

		results.summarizeAll(
			t, "sqlalchemy" /* ormName */, blocklistName, expectedFailures, version, supportedSQLAlchemyTag)
	}

	r.Add(testSpec{
		Name:       "sqlalchemy",
		Owner:      OwnerAppDev,
		Cluster:    makeClusterSpec(1),
		MinVersion: "v2.1.0",
		Tags:       []string{`default`, `orm`},
		Run: func(ctx context.Context, t *test, c *cluster) {
			runSQLAlchemy(ctx, t, c)
		},
	})
}
