// Copyright 2018 The Cockroach Authors.
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
	"context"
	"fmt"
	"regexp"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/errors"
)

var psycopgReleaseTagRegex = regexp.MustCompile(`^(?P<major>\d+)(?:_(?P<minor>\d+)(?:_(?P<point>\d+)(?:_(?P<subpoint>\d+))?)?)?$`)
var supportedPsycopgTag = "3c58e96e1000ef60060fb8139687028cb274838d"

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
		c.Put(ctx, t.Cockroach(), "./cockroach", c.All())
		c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings(), c.All())

		version, err := fetchCockroachVersion(ctx, t.L(), c, node[0])
		if err != nil {
			t.Fatal(err)
		}

		if err := alterZoneConfigAndClusterSettings(ctx, t, version, c, node[0]); err != nil {
			t.Fatal(err)
		}

		t.Status("cloning psycopg and installing prerequisites")
		latestTag, err := repeatGetLatestTag(ctx, t, "psycopg", "psycopg2", psycopgReleaseTagRegex)
		if err != nil {
			t.Fatal(err)
		}
		t.L().Printf("Latest Psycopg release is %s.", latestTag)
		t.L().Printf("Supported Psycopg release is %s.", supportedPsycopgTag)

		if err := c.RepeatRunE(
			ctx, t, node, "update apt-get", `sudo apt-get -qq update`,
		); err != nil {
			t.Fatal(err)
		}

		if err := c.RepeatRunE(
			ctx, t, node,
			"install dependencies",
			`sudo apt-get -qq install make python3 libpq-dev python3-dev gcc python3-setuptools python-setuptools`,
		); err != nil {
			t.Fatal(err)
		}

		if err := c.RepeatRunE(
			ctx, t, node, "remove old Psycopg", `sudo rm -rf /mnt/data1/psycopg`,
		); err != nil {
			t.Fatal(err)
		}

		// TODO(rafi): When psycopg 2.9.4 is released and tagged,
		//    use the tag version instead of the commit.
		//if err := repeatGitCloneE(
		//	ctx, t, c,
		//	"https://github.com/psycopg/psycopg2.git",
		//	"/mnt/data1/psycopg",
		//	supportedPsycopgTag,
		//	node,
		//); err != nil {
		//	t.Fatal(err)
		//}
		if err = c.RunE(ctx, node, "git clone https://github.com/psycopg/psycopg2.git /mnt/data1/psycopg"); err != nil {
			t.Fatal(err)
		}
		if err = c.RunE(ctx, node, fmt.Sprintf("cd /mnt/data1/psycopg/ && git checkout %s", supportedPsycopgTag)); err != nil {
			t.Fatal(err)
		}

		t.Status("building Psycopg")
		if err := c.RepeatRunE(
			ctx, t, node, "building Psycopg", `cd /mnt/data1/psycopg/ && make PYTHON_VERSION=3`,
		); err != nil {
			t.Fatal(err)
		}

		blocklistName, expectedFailures, ignoredlistName, ignoredlist := psycopgBlocklists.getLists(version)
		if expectedFailures == nil {
			t.Fatalf("No psycopg blocklist defined for cockroach version %s", version)
		}
		if ignoredlist == nil {
			t.Fatalf("No psycopg ignorelist defined for cockroach version %s", version)
		}
		t.L().Printf("Running cockroach version %s, using blocklist %s, using ignoredlist %s",
			version, blocklistName, ignoredlistName)

		t.Status("running psycopg test suite")

		result, err := c.RunWithDetailsSingleNode(ctx, t.L(), node,
			`cd /mnt/data1/psycopg/ &&
			export PSYCOPG2_TESTDB=defaultdb &&
			export PSYCOPG2_TESTDB_USER=root &&
			export PSYCOPG2_TESTDB_PORT=26257 &&
			export PSYCOPG2_TESTDB_HOST=localhost &&
			make check PYTHON_VERSION=3`,
		)

		// Expected to fail but we should still scan the error to check if
		// there's an SSH/roachprod error.
		if err != nil {
			// install.NonZeroExitCode includes unrelated to SSH errors ("255")
			// or roachprod errors, so we call t.Fatal if the error is not an
			// install.NonZeroExitCode error
			commandError := (*install.NonZeroExitCode)(nil)
			if !errors.As(err, &commandError) {
				t.Fatal(err)
			}
		}

		// Result error contains stdout, stderr, and any error content returned by exec package.
		rawResults := []byte(result.Stdout + result.Stderr)

		t.Status("collating the test results")
		t.L().Printf("Test Results: %s", rawResults)

		// Find all the failed and errored tests.
		results := newORMTestsResults()
		results.parsePythonUnitTestOutput(rawResults, expectedFailures, ignoredlist)
		results.summarizeAll(
			t, "psycopg" /* ormName */, blocklistName, expectedFailures,
			version, supportedPsycopgTag,
		)
	}

	r.Add(registry.TestSpec{
		Name:    "psycopg",
		Owner:   registry.OwnerSQLExperience,
		Cluster: r.MakeClusterSpec(1),
		Tags:    []string{`default`, `driver`},
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runPsycopg(ctx, t, c)
		},
	})
}
