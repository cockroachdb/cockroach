// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"fmt"
	"net/url"
	"regexp"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
)

var rustSqlxReleaseTagRegex = regexp.MustCompile(
	`^v(?P<major>\d+)\.(?P<minor>\d+)\.(?P<point>\d+)$`,
)

// rustSqlxTestOutputRegex matches Rust cargo test output lines. Unlike
// the shared rustUnitTestOutputRegex, this also handles tests without a
// module path (e.g., "test it_connects ... ok").
var rustSqlxTestOutputRegex = regexp.MustCompile(
	`(?P<type>test) (?:(?P<class>.+)::)?(?P<name>\S+) \.\.\. (?P<result>.+)`,
)

func registerRustSqlx(r registry.Registry) {
	runRustSqlx := func(ctx context.Context, t test.Test, c cluster.Cluster) {
		node := c.Node(1)
		t.Status("setting up cockroach")

		// Start CockroachDB in insecure mode with an in-memory store.
		// sqlx connects via DATABASE_URL, so we use the default SQL port.
		c.Start(ctx, t.L(), option.NewStartOpts(sqlClientsInMemoryDB),
			install.MakeClusterSettings(install.SimpleSecureOption(false)), c.All())

		db := c.Conn(ctx, t.L(), 1)
		if _, err := db.Exec("CREATE DATABASE IF NOT EXISTS sqlx"); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(
			"CREATE USER IF NOT EXISTS postgres WITH CREATEDB CREATELOGIN CREATEROLE CANCELQUERY",
		); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec("GRANT ALL ON DATABASE sqlx TO postgres"); err != nil {
			t.Fatal(err)
		}
		// Match PostgreSQL's default integer size (INT4) so that sqlx's
		// type expectations align with CockroachDB's inferred types.
		if _, err := db.Exec("SET CLUSTER SETTING sql.defaults.default_int_size = 4"); err != nil {
			t.Fatal(err)
		}

		version, err := fetchCockroachVersion(ctx, t.L(), c, node[0])
		if err != nil {
			t.Fatal(err)
		}

		if err := alterZoneConfigAndClusterSettings(ctx, t, version, c, node[0]); err != nil {
			t.Fatal(err)
		}

		t.Status("cloning rust-sqlx and installing prerequisites")

		latestTag, err := repeatGetLatestTag(
			ctx, t, "launchbadge", "sqlx", rustSqlxReleaseTagRegex,
		)
		if err != nil {
			t.Fatal(err)
		}
		t.L().Printf("Latest sqlx release is %s.", latestTag)

		// Use /tmp for local runs, /mnt/data1 for remote.
		sqlxDir := "/mnt/data1/sqlx"
		cargoPath := "/home/ubuntu/.cargo/bin/cargo"
		if c.IsLocal() {
			sqlxDir = "/tmp/sqlx"
			cargoPath = "cargo"
		}

		if err := repeatRunE(
			ctx, t, c, node,
			"remove old rust-sqlx",
			fmt.Sprintf(`rm -rf %s`, sqlxDir),
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatGitCloneE(
			ctx, t, c,
			"https://github.com/launchbadge/sqlx.git",
			sqlxDir,
			latestTag,
			node,
		); err != nil {
			t.Fatal(err)
		}

		// Skip system dependency installation for local runs (assumes
		// rust, cargo, and build tools are already installed).
		if !c.IsLocal() {
			if err := repeatRunE(
				ctx, t, c, node,
				"install rust and cargo",
				`curl https://sh.rustup.rs -sSf | sh -s -- -y`,
			); err != nil {
				t.Fatal(err)
			}

			if err := repeatRunE(
				ctx, t, c, node,
				"install C linker",
				"sudo apt-get install build-essential -y",
			); err != nil {
				t.Fatal(err)
			}

			if err := repeatRunE(
				ctx, t, c, node,
				"install build dependencies",
				"sudo apt-get install -y pkg-config libssl-dev",
			); err != nil {
				t.Fatal(err)
			}
		}

		// Build the DATABASE_URL from the cluster's external PG URL.
		urls, err := c.ExternalPGUrl(ctx, t.L(), node, roachprod.PGURLOptions{})
		if err != nil {
			t.Fatal(err)
		}
		pgURL, err := url.Parse(urls[0])
		if err != nil {
			t.Fatal(err)
		}
		pgURL.Path = "/sqlx"
		q := pgURL.Query()
		q.Set("sslmode", "disable")
		pgURL.RawQuery = q.Encode()
		databaseURL := pgURL.String()

		blocklistName := "rustSqlxBlockList"
		ignorelistName := "rustSqlxIgnoreList"
		expectedFailures := rustSqlxBlocklist
		ignorelist := rustSqlxIgnoreList

		status := fmt.Sprintf("running cockroach version %s, using blocklist %s",
			version, blocklistName)
		if ignorelist != nil {
			status = fmt.Sprintf(
				"running cockroach %s, using blocklist %s, using ignorelist %s",
				version, blocklistName, ignorelistName)
		}
		t.L().Printf("%s", status)

		t.Status("running rust-sqlx test suite")

		// We run only test targets that do not use sqlx::query!()
		// compile-time macros. Those macros connect to the database at
		// compile time; CockroachDB's INT8 default and missing setup
		// tables cause type mismatches and missing-relation errors that
		// prevent compilation. The targets below exercise sqlx's runtime
		// query and type layer without compile-time checking.
		result, err := c.RunWithDetailsSingleNode(
			ctx, t.L(),
			option.WithNodes(node),
			fmt.Sprintf(
				`cd %s && DATABASE_URL='%s' `+
					`%s test `+
					`--features postgres,runtime-tokio,tls-none,_unstable-all-types `+
					`--test postgres `+
					`--test postgres-types `+
					`--test postgres-describe `+
					`--test postgres-error `+
					`--test postgres-query-builder `+
					`--no-fail-fast > rustsqlx.stdout 2>&1`,
				sqlxDir, databaseURL, cargoPath))
		if err != nil {
			t.L().Printf("error during rust-sqlx run (may be ok): %v\n", err)
		}
		_ = result

		t.L().Printf("Test stdout for rust-sqlx")
		result, err = c.RunWithDetailsSingleNode(
			ctx, t.L(), option.WithNodes(node),
			fmt.Sprintf("cd %s && cat rustsqlx.stdout", sqlxDir),
		)
		if err != nil {
			t.Fatal(err)
		}

		t.L().Printf("Test results for rust-sqlx: %s", result.Stdout+result.Stderr)
		t.Status("collating the test results")

		results := newORMTestsResults()
		results.parseUnitTestOutput(
			rustSqlxTestOutputRegex,
			[]byte(result.Stdout+result.Stderr), expectedFailures, ignorelist,
		)
		t.L().Printf("Test pass for rust-sqlx: %d, Test fail for rust-sqlx: %d",
			results.passExpectedCount, results.failUnexpectedCount)
		results.summarizeAll(
			t, "rust-sqlx" /* ormName */, blocklistName,
			expectedFailures, version, latestTag,
		)
	}

	r.Add(registry.TestSpec{
		Name:    "rust-sqlx",
		Owner:   registry.OwnerSQLFoundations,
		Cluster: r.MakeClusterSpec(1, spec.CPU(16)),
		Leases:  registry.MetamorphicLeases,
		// This test requires service registration which is currently only
		// supported on GCE.
		CompatibleClouds: registry.Clouds(spec.GCE, spec.Local),
		Suites:           registry.Suites(registry.Nightly, registry.ORM),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runRustSqlx(ctx, t, c)
		},
	})
}
