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
	"context"
	"fmt"
	"regexp"
)

var pgxReleaseTagRegex = regexp.MustCompile(`^v(?P<major>\d+)\.(?P<minor>\d+)\.(?P<point>\d+)$`)
var supportedTag = "v4.6.0"

// This test runs pgx's full test suite against a single cockroach node.

func registerPgx(r *testRegistry) {
	runPgx := func(
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

		t.Status("setting up go")
		installLatestGolang(ctx, t, c, node)

		t.Status("installing pgx")
		if err := repeatRunE(
			ctx, c, node, "install pgx", "go get -u github.com/jackc/pgx",
		); err != nil {
			t.Fatal(err)
		}

		latestTag, err := repeatGetLatestTag(ctx, c, "jackc", "pgx", pgxReleaseTagRegex)
		if err != nil {
			t.Fatal(err)
		}
		c.l.Printf("Latest jackc/pgx release is %s.", latestTag)
		c.l.Printf("Supported release is %s.", supportedTag)

		t.Status("installing go-junit-report")
		if err := repeatRunE(
			ctx, c, node, "install pgx", "go get -u github.com/jstemmer/go-junit-report",
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
		c.l.Printf("%s", status)

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
			ctx, c, t.l, node,
			"run pgx test suite",
			"cd `go env GOPATH`/src/github.com/jackc/pgx && "+
				"PGX_TEST_DATABASE='postgresql://root:@localhost:26257/pgx_test' go test -v 2>&1 | "+
				"`go env GOPATH`/bin/go-junit-report",
		)

		results := newORMTestsResults()
		results.parseJUnitXML(t, expectedFailures, ignorelist, xmlResults)
		results.summarizeAll(
			t, "pgx", blocklistName, expectedFailures, version, supportedTag,
		)
	}

	r.Add(testSpec{
		Name:       "pgx",
		Owner:      OwnerAppDev,
		Cluster:    makeClusterSpec(1),
		MinVersion: "v19.2.0",
		Tags:       []string{`default`, `driver`},
		Run: func(ctx context.Context, t *test, c *cluster) {
			runPgx(ctx, t, c)
		},
	})
}
