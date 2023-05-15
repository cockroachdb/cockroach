// Copyright 2021 The Cockroach Authors.
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
	"regexp"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
)

const asyncpgRunTestCmd = `
source venv/bin/activate && 
cd /mnt/data1/asyncpg && 
PGPORT=26257 PGHOST=localhost PGUSER=root PGDATABASE=defaultdb python3 setup.py test > asyncpg.stdout
`

var asyncpgReleaseTagRegex = regexp.MustCompile(`^(?P<major>v\d+)\.(?P<minor>\d+)\.(?P<point>\d+)$`)

var asyncpgSupportedTag = "v0.24.0"

func registerAsyncpg(r registry.Registry) {
	runAsyncpg := func(
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

		t.Status("cloning asyncpg and installing prerequisites")

		latestTag, err := repeatGetLatestTag(
			ctx, t, "MagicStack", "asyncpg", asyncpgReleaseTagRegex,
		)
		if err != nil {
			t.Fatal(err)
		}

		t.L().Printf("Latest asyncpg release is %s.", latestTag)
		t.L().Printf("Supported asyncpg release is %s.", latestTag)

		if err := gitCloneWithRecurseSubmodules(
			ctx,
			c,
			t.L(),
			"https://github.com/MagicStack/asyncpg.git",
			"/mnt/data1/asyncpg",
			asyncpgSupportedTag,
			node,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatRunE(
			ctx,
			t,
			c,
			node,
			"install python and pip",
			`sudo apt-get -qq install python3.7 python3-pip libpq-dev python-dev python3-virtualenv`,
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
			"install asyncpg's dependencies",
			"source venv/bin/activate && cd /mnt/data1/asyncpg && pip3 install -e ."); err != nil {
			t.Fatal(err)
		}

		blocklistName, expectedFailureList := "asyncpgBlocklist", asyncpgBlocklist
		ignoredlistName, ignoredlist := "asyncpgIgnoreList", asyncpgIgnoreList
		if ignoredlist == nil {
			t.Fatalf("No asyncpg ignorelist defined for cockroach version %s", version)
		}
		t.L().Printf("Running cockroach version %s, using blocklist %s, using ignorelist %s",
			version, blocklistName, ignoredlistName)

		t.Status("Running asyncpg tests ")
		result, err := c.RunWithDetailsSingleNode(
			ctx, t.L(), node, asyncpgRunTestCmd)
		if err != nil {
			t.L().Printf("error during asyncpg run (may be ok): %v\n", err)
		}
		t.L().Printf("Test results for asyncpg: %s", result.Stdout+result.Stderr)
		t.L().Printf("Test stdout for asyncpg")
		if err := c.RunE(
			ctx, node, "cd /mnt/data1/asyncpg && cat asyncpg.stdout",
		); err != nil {
			t.Fatal(err)
		}

		t.Status("collating test results")

		results := newORMTestsResults()
		results.parsePythonUnitTestOutput([]byte(result.Stdout+result.Stderr), expectedFailureList, ignoredlist)
		results.summarizeAll(
			t, "asyncpg" /* ormName */, blocklistName, expectedFailureList, version, asyncpgSupportedTag,
		)
	}

	r.Add(registry.TestSpec{
		Name:    "asyncpg",
		Owner:   registry.OwnerSQLFoundations,
		Cluster: r.MakeClusterSpec(1, spec.CPU(16)),
		Tags:    registry.Tags(`default`, `orm`),
		Leases:  registry.MetamorphicLeases,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runAsyncpg(ctx, t, c)
		},
	})
}
