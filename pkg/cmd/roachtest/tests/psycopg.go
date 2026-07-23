// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"fmt"
	"regexp"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	rperrors "github.com/cockroachdb/cockroach/pkg/roachprod/errors"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
)

var psycopgReleaseTagRegex = regexp.MustCompile(`^(?P<major>\d+)(?:\.(?P<minor>\d+)(?:\.(?P<point>\d+)(?:\.(?P<subpoint>\d+))?)?)?$`)
var supportedPsycopgTag = "3.2.8"

// This test runs psycopg full test suite against a single cockroach node.
func registerPsycopg(r registry.Registry) {
	runPsycopg := func(
		ctx context.Context,
		t test.Test,
		c cluster.Cluster,
	) {
		if c.IsLocal() {
			t.Fatal("cannot be run in local mode")
		}
		node := c.Node(1)
		t.Status("setting up cockroach")
		c.Start(ctx, t.L(), option.NewStartOpts(sqlClientsInMemoryDB), install.MakeClusterSettings(), c.All())

		version, err := fetchCockroachVersion(ctx, t.L(), c, node[0])
		if err != nil {
			t.Fatal(err)
		}

		if err := alterZoneConfigAndClusterSettings(ctx, t, version, c, node[0]); err != nil {
			t.Fatal(err)
		}

		// Turn off autocommit_before_ddl and READ COMMITTED for this test
		// specifically. The psycopg tests rely on doing schema changes in
		// transactions with multiple statements.
		t.Status("turning off autocommit_before_ddl and using serializable isolation")
		db, err := c.ConnE(ctx, t.L(), node[0])
		if err != nil {
			t.Fatal(err)
		}
		for _, stmt := range []string{
			`ALTER ROLE ALL SET autocommit_before_ddl = 'false'`,
			`ALTER ROLE ALL SET default_transaction_isolation = 'serializable'`,
		} {
			if _, err := db.ExecContext(ctx, stmt); err != nil {
				t.Fatal(err)
			}
		}
		db.Close()

		t.Status("cloning psycopg and installing prerequisites")
		latestTag, err := repeatGetLatestTag(ctx, t, "psycopg", "psycopg", psycopgReleaseTagRegex)
		if err != nil {
			t.Fatal(err)
		}
		t.L().Printf("Latest Psycopg release is %s.", latestTag)
		t.L().Printf("Supported Psycopg release is %s.", supportedPsycopgTag)

		if err := repeatRunE(
			ctx, t, c, node, "update apt-get", `sudo apt-get -qq update`,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatRunE(
			ctx, t, c, node,
			"install dependencies",
			`sudo apt-get -qq install make python3.10 libpq-dev python3.10-dev gcc python3-virtualenv python3-setuptools python-setuptools python3.10-distutils`,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatRunE(
			ctx, t, c, node, "set python3.10 as default", `
				sudo update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.10 1
				sudo update-alternatives --config python3`,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatRunE(
			ctx, t, c, node, "install pip",
			`curl https://bootstrap.pypa.io/get-pip.py | sudo -H python3.10`,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatRunE(
			ctx, t, c, node, "create virtualenv", `virtualenv --clear venv`,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatRunE(
			ctx,
			t,
			c,
			node,
			"upgrade pip and install pytest",
			`source venv/bin/activate && pip install -U pip && pip install pytest pytest-xdist`,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatRunE(
			ctx, t, c, node, "remove old Psycopg", `sudo rm -rf /mnt/data1/psycopg`,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatGitCloneE(
			ctx, t, c,
			"https://github.com/psycopg/psycopg.git",
			"/mnt/data1/psycopg",
			supportedPsycopgTag,
			node,
		); err != nil {
			t.Fatal(err)
		}

		t.Status("building Psycopg")
		if err := repeatRunE(
			ctx, t, c, node, "building Psycopg",
			`source venv/bin/activate &&
			cd /mnt/data1/psycopg/ &&
			pip install --config-settings editable_mode=strict -e "./psycopg[dev,test]" &&  # for the base Python package
			pip install --config-settings editable_mode=strict -e ./psycopg_pool &&         # for the connection pool
			pip install --config-settings editable_mode=strict ./psycopg_c                  # for the C speedup module`,
		); err != nil {
			t.Fatal(err)
		}

		t.L().Printf("Running cockroach version %s, using blocklist %s, using ignoredlist %s",
			version, "psycopgBlockList", "psycopgIgnoreList")

		t.Status("running psycopg test suite")

		const testResultsXML = "/mnt/data1/psycopg/test_results.xml"
		result, err := c.RunWithDetailsSingleNode(ctx, t.L(), option.WithNodes(node), fmt.Sprintf(
			`source venv/bin/activate &&
			cd /mnt/data1/psycopg/ &&
			export PSYCOPG_TEST_DSN="host=localhost port={pgport:1} user=%[1]s password=%[2]s dbname=defaultdb" &&
			export PGPASSWORD=%[2]s
			pytest -vv -m "not timing" --junit-xml=%[3]s`,
			install.DefaultUser, install.DefaultPassword, testResultsXML))

		// Fatal for a roachprod or transient error. A roachprod error is when result.Err==nil.
		// Proceed for any other (command) errors
		if err != nil && (result.Err == nil || rperrors.IsTransient(err)) {
			t.Fatal(err)
		}

		// Result error contains stdout, stderr, and any error content returned by exec package.
		rawResults := []byte(result.Stdout + result.Stderr)
		t.Status("collating the test results")
		t.L().Printf("Test Results: %s", rawResults)

		result, err = repeatRunWithDetailsSingleNode(
			ctx,
			c,
			t,
			node,
			fmt.Sprintf("fetching results file %s", testResultsXML),
			fmt.Sprintf("cat %s", testResultsXML),
		)
		if err != nil {
			t.Fatal(err)
		}

		// Find all the failed and errored tests.
		results := newORMTestsResults()
		results.parseJUnitXML(t, psycopgBlockList, psycopgIgnoreList, []byte(result.Stdout))
		results.summarizeAll(
			t, "psycopg" /* ormName */, "psycopgBlockList", psycopgBlockList,
			version, supportedPsycopgTag,
		)
	}

	r.Add(registry.TestSpec{
		Name:             "psycopg",
		Owner:            registry.OwnerSQLFoundations,
		Cluster:          r.MakeClusterSpec(1),
		Leases:           registry.MetamorphicLeases,
		CompatibleClouds: registry.AllClouds.NoAWS().NoIBM(),
		Suites:           registry.Suites(registry.Nightly, registry.Driver),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runPsycopg(ctx, t, c)
		},
	})
}
