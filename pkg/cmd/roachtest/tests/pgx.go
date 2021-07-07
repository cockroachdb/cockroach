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
	"context"
	"fmt"
	"regexp"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
)

var pgxReleaseTagRegex = regexp.MustCompile(`^v(?P<major>\d+)\.(?P<minor>\d+)\.(?P<point>\d+)$`)
var supportedPGXTag = "v4.11.0"

// This test runs pgx's full test suite against a single cockroach node.

func registerPgx(r registry.Registry) {
	runPgx := func(
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

		t.Status("setting up go")
		installGolang(ctx, t, c, node)

		t.Status("getting pgx")
		if err := repeatGitCloneE(
			ctx,
			t,
			c,
			"https://github.com/jackc/pgx.git",
			"/mnt/data1/pgx",
			supportedPGXTag,
			node,
		); err != nil {
			t.Fatal(err)
		}

		latestTag, err := repeatGetLatestTag(ctx, t, "jackc", "pgx", pgxReleaseTagRegex)
		if err != nil {
			t.Fatal(err)
		}
		t.L().Printf("Latest jackc/pgx release is %s.", latestTag)
		t.L().Printf("Supported release is %s.", supportedPGXTag)

		t.Status("installing go-junit-report")
		if err := repeatRunE(
			ctx, t, c, node, "install go-junit-report", "go get -u github.com/jstemmer/go-junit-report",
		); err != nil {
			t.Fatal(err)
		}

		t.Status("checking blocklist")
		blocklistName, expectedFailures, ignorelistName, ignorelist := pgxBlocklists.getLists(version)
		if expectedFailures == nil {
			t.Fatalf("No pgx blocklist defined for cockroach version %s", version)
		}
		status := fmt.Sprintf("Running cockroach version %s, using blocklist %s", version, blocklistName)
		if ignorelist != nil {
			status = fmt.Sprintf("Running cockroach version %s, using blocklist %s, using ignorelist %s",
				version, blocklistName, ignorelistName)
		}
		t.L().Printf("%s", status)

		t.Status("setting up test db")
		db, err := c.ConnE(ctx, node[0])
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		if _, err = db.ExecContext(
			ctx, `drop database if exists pgx_test; create database pgx_test;`,
		); err != nil {
			t.Fatal(err)
		}

		// This is expected to fail because the feature is unsupported by Cockroach, but pgx expects it.
		// https://github.com/cockroachdb/cockroach/issues/27796
		_, _ = db.ExecContext(
			ctx, `create domain uint64 as numeric(20,0);`,
		)

		t.Status("running pgx test suite")
		// Running the test suite is expected to error out, so swallow the error.
		xmlResults, _ := repeatRunWithBuffer(
			ctx, c, t, node,
			"run pgx test suite",
			"cd /mnt/data1/pgx && "+
				"PGX_TEST_DATABASE='postgresql://root:@localhost:26257/pgx_test' go test -v 2>&1 | "+
				"`go env GOPATH`/bin/go-junit-report",
		)

		results := newORMTestsResults()
		results.parseJUnitXML(t, expectedFailures, ignorelist, xmlResults)
		results.summarizeAll(
			t, "pgx", blocklistName, expectedFailures, version, supportedPGXTag,
		)
	}

	r.Add(registry.TestSpec{
		Name:    "pgx",
		Owner:   registry.OwnerSQLExperience,
		Cluster: r.MakeClusterSpec(1),
		Tags:    []string{`default`, `driver`},
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runPgx(ctx, t, c)
		},
	})
}
