// Copyright 2019 The Cockroach Authors.
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

var pgxReleaseTagRegex = regexp.MustCompile(`^v(?P<major>\d+)\.(?P<minor>\d+)\.(?P<point>\d+)$`)

// WARNING: DO NOT MODIFY the name of the below constant/variable without approval from the docs team.
// This is used by docs automation to produce a list of supported versions for ORM's.
var supportedPGXTag = "v5.3.1"

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
		c.Start(ctx, t.L(), option.NewStartOpts(sqlClientsInMemoryDB), install.MakeClusterSettings(), c.All())

		version, err := fetchCockroachVersion(ctx, t.L(), c, node[0])
		if err != nil {
			t.Fatal(err)
		}

		if err := alterZoneConfigAndClusterSettings(ctx, t, version, c, node[0]); err != nil {
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
			ctx, t, c, node, "install go-junit-report", "go install github.com/jstemmer/go-junit-report@latest",
		); err != nil {
			t.Fatal(err)
		}

		for original, replacement := range map[string]string{
			"create type fruit": "drop type if exists public.fruit; create type fruit",
			"t.Parallel()":      "", // SAFE FOR TESTING
		} {
			if err := repeatRunE(
				ctx, t, c, node, "patch test to workaround flaky cleanup logic",
				fmt.Sprintf(`find /mnt/data1/pgx/ -name "query_test.go" | xargs sed -i -e "s/%s/%s/g"`, original, replacement),
			); err != nil {
				t.Fatal(err)
			}
		}

		// It's safer to clean up dependencies this way than it is to give the cluster
		// wipe root access.
		defer func() {
			c.Run(ctx, option.WithNodes(c.All()), "go clean -modcache")
		}()

		RunningStatus := fmt.Sprintf("Running cockroach version %s, using blocklist %s, using ignorelist %s",
			version, "pgxBlocklist", "pgxIgnorelist")
		t.L().Printf("%s", RunningStatus)

		t.Status("setting up test db")
		db, err := c.ConnE(ctx, t.L(), node[0])
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
		result, err := repeatRunWithDetailsSingleNode(
			ctx, c, t, node,
			"run pgx test suite",
			fmt.Sprintf("cd /mnt/data1/pgx && "+
				"PGX_TEST_DATABASE='postgresql://%s:%s@localhost:{pgport:1}/pgx_test?sslmode=require' go test -v 2>&1 | "+
				"`go env GOPATH`/bin/go-junit-report", install.DefaultUser, install.DefaultPassword),
		)

		// Fatal for a roachprod or transient error. A roachprod error is when result.Err==nil.
		// Proceed for any other (command) errors
		if err != nil && (result.Err == nil || rperrors.IsTransient(err)) {
			t.Fatal(err)
		}

		// Result error contains stdout, stderr, and any error content returned by exec package.
		xmlResults := []byte(result.Stdout + result.Stderr)

		results := newORMTestsResults()
		results.parseJUnitXML(t, pgxBlocklist, pgxIgnorelist, xmlResults)
		results.summarizeAll(
			t, "pgx", "pgxBlocklist", pgxBlocklist, version, supportedPGXTag,
		)
	}

	r.Add(registry.TestSpec{
		Name:             "pgx",
		Owner:            registry.OwnerSQLFoundations,
		Cluster:          r.MakeClusterSpec(1),
		Leases:           registry.MetamorphicLeases,
		CompatibleClouds: registry.AllExceptAWS,
		Suites:           registry.Suites(registry.Nightly, registry.Driver),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runPgx(ctx, t, c)
		},
	})
}
