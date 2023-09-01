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
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	rperrors "github.com/cockroachdb/cockroach/pkg/roachprod/errors"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/errors"
)

var sqlAlchemyResultRegex = regexp.MustCompile(`^(?P<test>test.*::.*::[^ \[\]]*(?:\[.*])?) (?P<result>\w+)\s+\[.+]$`)
var sqlAlchemyReleaseTagRegex = regexp.MustCompile(`^rel_(?P<major>\d+)_(?P<minor>\d+)_(?P<point>\d+)$`)

var supportedSQLAlchemyTag = "2.0.20"

// This test runs the SQLAlchemy dialect test suite against a single Cockroach
// node.
func registerSQLAlchemy(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:    "sqlalchemy",
		Owner:   registry.OwnerSQLFoundations,
		Cluster: r.MakeClusterSpec(1),
		Leases:  registry.MetamorphicLeases,
		Tags:    registry.Tags(`default`, `orm`),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runSQLAlchemy(ctx, t, c)
		},
	})
}

func runSQLAlchemy(ctx context.Context, t test.Test, c cluster.Cluster) {
	if c.IsLocal() {
		t.Fatal("cannot be run in local mode")
	}

	node := c.Node(1)

	t.Status("cloning sqlalchemy and installing prerequisites")
	latestTag, err := repeatGetLatestTag(ctx, t, "sqlalchemy", "sqlalchemy", sqlAlchemyReleaseTagRegex)
	if err != nil {
		t.Fatal(err)
	}
	t.L().Printf("Latest sqlalchemy release is %s.", latestTag)
	t.L().Printf("Supported sqlalchemy release is %s.", supportedSQLAlchemyTag)

	if err := repeatRunE(ctx, t, c, node, "update apt-get", `
		sudo add-apt-repository ppa:deadsnakes/ppa &&
		sudo apt-get -qq update
	`); err != nil {
		t.Fatal(err)
	}

	if err := repeatRunE(ctx, t, c, node, "install dependencies", `
		sudo apt-get -qq install make python3.7 libpq-dev python3.7-dev gcc python3-setuptools python-setuptools build-essential python3.7-distutils python3-virtualenv
	`); err != nil {
		t.Fatal(err)
	}

	if err := repeatRunE(ctx, t, c, node, "set python3.7 as default", `
		sudo update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.5 1
		sudo update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.7 2
		sudo update-alternatives --config python3
	`); err != nil {
		t.Fatal(err)
	}

	if err := repeatRunE(ctx, t, c, node, "install pip", `
		curl https://bootstrap.pypa.io/get-pip.py | sudo -H python3.7
	`); err != nil {
		t.Fatal(err)
	}

	if err := repeatRunE(
		ctx, t, c, node, "create virtualenv", `virtualenv --clear venv`,
	); err != nil {
		t.Fatal(err)
	}

	if err := repeatRunE(ctx, t, c, node, "install pytest", fmt.Sprintf(`
		source venv/bin/activate &&
			pip3 install --upgrade --force-reinstall setuptools pytest==7.2.1 pytest-xdist psycopg2 alembic sqlalchemy==%s`,
		supportedSQLAlchemyTag)); err != nil {
		t.Fatal(err)
	}

	if err := repeatRunE(ctx, t, c, node, "remove old sqlalchemy-cockroachdb", `
		sudo rm -rf /mnt/data1/sqlalchemy-cockroachdb
	`); err != nil {
		t.Fatal(err)
	}

	if err := repeatGitCloneE(ctx, t, c,
		"https://github.com/cockroachdb/sqlalchemy-cockroachdb.git", "/mnt/data1/sqlalchemy-cockroachdb",
		"master", node); err != nil {
		t.Fatal(err)
	}

	t.Status("installing sqlalchemy-cockroachdb")
	if err := repeatRunE(ctx, t, c, node, "installing sqlalchemy=cockroachdb", `
		source venv/bin/activate && cd /mnt/data1/sqlalchemy-cockroachdb && pip3 install .
	`); err != nil {
		t.Fatal(err)
	}

	// Phew, after having setup all that, let's actually run the test.

	t.Status("setting up cockroach")
	c.Put(ctx, t.Cockroach(), "./cockroach", c.All())
	c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings(), c.All())

	version, err := fetchCockroachVersion(ctx, t.L(), c, node[0])
	if err != nil {
		t.Fatal(err)
	}

	if err := alterZoneConfigAndClusterSettings(ctx, t, version, c, node[0]); err != nil {
		t.Fatal(err)
	}

	blocklistName, expectedFailures := "sqlAlchemyBlocklist", sqlAlchemyBlocklist
	ignoredlistName, ignoredlist := "sqlAlchemyIgnoreList", sqlAlchemyIgnoreList
	t.L().Printf("Running cockroach version %s, using blocklist %s, using ignoredlist %s",
		version, blocklistName, ignoredlistName)

	t.Status("running sqlalchemy test suite")
	// Note that this is expected to return an error, since the test suite
	// will fail. And it is safe to swallow it here.
	result, err := c.RunWithDetailsSingleNode(ctx, t.L(), node,
		`source venv/bin/activate && cd /mnt/data1/sqlalchemy-cockroachdb/ && pytest --maxfail=0 \
		--dburi='cockroachdb://root@localhost:26257/defaultdb?sslmode=disable&disable_cockroachdb_telemetry=true' \
		test/test_suite_sqlalchemy.py
	`)

	// Fatal for a roachprod or SSH error. A roachprod error is when result.Err==nil.
	// Proceed for any other (command) errors
	if err != nil && (result.Err == nil || errors.Is(err, rperrors.ErrSSH255)) {
		t.Fatal(err)
	}

	rawResults := []byte(result.Stdout + result.Stderr)

	t.Status("collating the test results")
	t.L().Printf("Test Results: %s", rawResults)

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
