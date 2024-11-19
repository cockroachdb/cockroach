// Copyright 2023 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	rperrors "github.com/cockroachdb/cockroach/pkg/roachprod/errors"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
)

var npgsqlReleaseTagRegex = regexp.MustCompile(`^v(?P<major>\d+)\.(?P<minor>\d+)\.(?P<point>\d+)$`)

// WARNING: DO NOT MODIFY the name of the below constant/variable without approval from the docs team.
// This is used by docs automation to produce a list of supported versions for ORM's.
var npgsqlSupportedTag = "v7.0.2"

// This test runs npgsql's full test suite against a single cockroach node.
func registerNpgsql(r registry.Registry) {
	runNpgsql := func(
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
		db, err := c.ConnE(ctx, t.L(), node[0])
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()

		for _, cmd := range []string{
			`CREATE USER npgsql_tests PASSWORD 'npgsql_tests'`,
			`GRANT admin TO npgsql_tests`,
			`DROP DATABASE IF EXISTS npgsql_tests`,
			`CREATE DATABASE npgsql_tests`,
		} {
			if _, err := db.ExecContext(ctx, cmd); err != nil {
				t.Fatal(err)
			}
		}

		// Install dotnet as per these docs:
		// https://learn.microsoft.com/en-us/dotnet/core/install/linux-ubuntu
		t.Status("setting up dotnet")
		if err := repeatRunE(
			ctx,
			t,
			c,
			node,
			"install dependencies",
			`sudo snap install dotnet-sdk --channel=7.0/stable --classic && \
sudo snap alias dotnet-sdk.dotnet dotnet && \
sudo ln -s /snap/dotnet-sdk/current/dotnet /usr/local/bin/dotnet`,
		); err != nil {
			t.Fatal(err)
		}

		t.Status("getting npgsql")
		if err := repeatGitCloneE(
			ctx,
			t,
			c,
			"https://github.com/npgsql/npgsql.git",
			"/mnt/data1/npgsql",
			npgsqlSupportedTag,
			node,
		); err != nil {
			t.Fatal(err)
		}

		latestTag, err := repeatGetLatestTag(ctx, t, "npgsql", "npgsql", npgsqlReleaseTagRegex)
		if err != nil {
			t.Fatal(err)
		}
		t.L().Printf("Latest npgsql release is %s.", latestTag)
		t.L().Printf("Supported release is %s.", npgsqlSupportedTag)

		result, err := c.RunWithDetailsSingleNode(ctx, t.L(), option.WithNodes(c.Nodes(1)), "echo -n {pgport:1}")
		if err != nil {
			t.Fatal(err)
		}

		// The `sed` command configures the test to not run on .NET 3.1 since we
		// haven't installed it on this system. (The tests will only run on .NET 7.)
		// The git patch changes the connection string and test setup.
		t.Status("modifying connection settings")
		if err := repeatRunE(
			ctx,
			t,
			c,
			node,
			"modify connection settings",
			fmt.Sprintf(`cd /mnt/data1/npgsql && \
sed -e 's/netcoreapp3.1//g' -i test/Directory.Build.props && \
echo '%s' | git apply --ignore-whitespace -`, fmt.Sprintf(npgsqlPatch, result.Stdout)),
		); err != nil {
			t.Fatal(err)
		}

		r := fmt.Sprintf("Running cockroach version %s, using blocklist %s, using ignorelist %s",
			version, "npgsqlBlocklist", "npgsqlIgnorelist")
		t.L().Printf("%s", r)

		t.Status("running npgsql test suite")
		// Running the test suite is expected to error out, so swallow the error.
		result, err = c.RunWithDetailsSingleNode(
			ctx, t.L(), option.WithNodes(node),
			`cd /mnt/data1/npgsql && dotnet test test/Npgsql.Tests --logger trx`,
		)

		rawResults := "stdout:\n" + result.Stdout + "\n\nstderr:\n" + result.Stderr
		t.L().Printf("Test results for npgsql: %s", rawResults)

		// Fatal for a roachprod or transient error. A roachprod error is when result.Err==nil.
		// Proceed for any other (command) errors
		if err != nil && (result.Err == nil || rperrors.IsTransient(err)) {
			t.Fatal(err)
		}

		// Load the list of all test results files and parse them individually.
		// Files are here: /mnt/data1/npgsql/test/Npgsql.Tests/TestResults/
		t.Status("collating test results")
		result, err = repeatRunWithDetailsSingleNode(
			ctx,
			c,
			t,
			node,
			"get list of test files",
			`ls /mnt/data1/npgsql/test/Npgsql.Tests/TestResults/*.trx`,
		)
		if err != nil {
			t.Fatal(err)
		}

		if len(result.Stdout) == 0 {
			t.Fatal("could not find any test result files")
		}

		parseAndSummarizeDotNetTestsResults(
			ctx, t, c, node, "npgsql" /* ormName */, result.Stdout,
			"npgsqlBlocklist", npgsqlBlocklist, npgsqlIgnoreList, version, npgsqlSupportedTag,
		)
	}

	r.Add(registry.TestSpec{
		Name:  "npgsql",
		Owner: registry.OwnerSQLFoundations,
		// .NET only supports AMD64 arch for 7.0.
		Cluster:          r.MakeClusterSpec(1, spec.Arch(vm.ArchAMD64)),
		Leases:           registry.MetamorphicLeases,
		CompatibleClouds: registry.AllExceptAWS,
		Suites:           registry.Suites(registry.Nightly, registry.Driver),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runNpgsql(ctx, t, c)
		},
	})
}

// This patch modifies the connection string so it connects on the CockroachDB
// port, and removes all the calls to pg_advisory_xact_lock that occur during
// test setup.
const npgsqlPatch = `diff --git a/test/Npgsql.Tests/TestUtil.cs b/test/Npgsql.Tests/TestUtil.cs
index ecfdd85f..17527129 100644
--- a/test/Npgsql.Tests/TestUtil.cs
+++ b/test/Npgsql.Tests/TestUtil.cs
@@ -19,7 +19,7 @@ public static class TestUtil
     /// test database.
     /// </summary>
     public const string DefaultConnectionString =
-        "Server=localhost;Username=npgsql_tests;Password=npgsql_tests;Database=npgsql_tests;Timeout=0;Command Timeout=0;SSL Mode=Disable";
+        "Server=127.0.0.1;Username=npgsql_tests;Password=npgsql_tests;Database=npgsql_tests;Port=%s;Timeout=0;Command Timeout=0;SSL Mode=Prefer;Include Error Detail=true";

     /// <summary>
     /// The connection string that will be used when opening the connection to the tests database.
@@ -186,7 +186,6 @@ internal static async Task<string> CreateTempTable(NpgsqlConnection conn, string

         await conn.ExecuteNonQueryAsync(@$"
 START TRANSACTION;
-SELECT pg_advisory_xact_lock(0);
 DROP TABLE IF EXISTS {tableName} CASCADE;
 COMMIT;
 CREATE TABLE {tableName} ({columns});");
@@ -203,7 +202,6 @@ internal static async Task<string> GetTempTableName(NpgsqlConnection conn)
         var tableName = "temp_table" + Interlocked.Increment(ref _tempTableCounter);
         await conn.ExecuteNonQueryAsync(@$"
 START TRANSACTION;
-SELECT pg_advisory_xact_lock(0);
 DROP TABLE IF EXISTS {tableName} CASCADE;
 COMMIT");
         return tableName;
@@ -218,7 +216,6 @@ internal static async Task<string> CreateTempTable(NpgsqlDataSource dataSource,
         var tableName = "temp_table" + Interlocked.Increment(ref _tempTableCounter);
         await dataSource.ExecuteNonQueryAsync(@$"
 START TRANSACTION;
-SELECT pg_advisory_xact_lock(0);
 DROP TABLE IF EXISTS {tableName} CASCADE;
 COMMIT;
 CREATE TABLE {tableName} ({columns});");`
