// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
)

var supportedJasyncCommit = "6301aa1b9ef8a0d4c5cf6f3c095b30a388c62dc0"

func registerJasyncSQL(r registry.Registry) {
	runJasyncSQL := func(ctx context.Context, t test.Test, c cluster.Cluster) {
		if c.IsLocal() {
			t.Fatal("can not be run in local mode")
		}
		node := c.Node(1)
		t.Status("setting up cockroach")
		// jasync does not support changing the default sslmode for postgresql, defaulting
		// sslmode=disable. See: https://github.com/jasync-sql/jasync-sql/issues/422
		// TODO(darrylwong): If the above issue is addressed we can enable secure mode
		c.Start(
			ctx, t.L(), option.NewStartOpts(sqlClientsInMemoryDB),
			install.MakeClusterSettings(install.SecureOption(false)),
		)

		version, err := fetchCockroachVersion(ctx, t.L(), c, node[0])
		if err != nil {
			t.Fatal(err)
		}

		if err := alterZoneConfigAndClusterSettings(ctx, t, version, c, node[0]); err != nil {
			t.Fatal(err)
		}

		t.Status("cloning jasync-sql and installing prerequisites")

		// Remove old jasync folder
		if err := repeatRunE(
			ctx,
			t,
			c,
			node,
			"remove old jasync-sql",
			`rm -rf /mnt/data1/jasyncsql`,
		); err != nil {
			t.Fatal(err)
		}

		if err := c.RunE(
			ctx,
			option.WithNodes(node),
			"cd /mnt/data1 && git clone https://github.com/jasync-sql/jasync-sql.git",
		); err != nil {
			t.Fatal(err)
		}

		// TODO: Currently we are pointing to a JasyncSQL branch, we will change
		// this once the official release is available
		if err := c.RunE(ctx, option.WithNodes(node), fmt.Sprintf("cd /mnt/data1/jasync-sql && git checkout %s",
			supportedJasyncCommit)); err != nil {
			t.Fatal(err)
		}
		if err := repeatRunE(
			ctx,
			t,
			c,
			node,
			"install java and gradle",
			`sudo apt-get -qq install default-jre openjdk-11-jdk-headless gradle`,
		); err != nil {
			t.Fatal(err)
		}
		t.Status("building jasyncsql (without tests)")

		status := fmt.Sprintf(
			"Running cockroach %s, using blocklist %s, using ignorelist %s",
			version,
			"jasyncSqlBlocklist",
			"jasyncIgnorelist")
		t.L().Printf("%s", status)

		t.Status("running jasyncsql test suite")

		_ = c.RunE(
			ctx,
			option.WithNodes(node),
			`cd /mnt/data1/jasync-sql && PGHOST=localhost PGUSER=test_admin PGPORT={pgport:1} PGDATABASE=defaultdb ./gradlew :postgresql-async:test`,
		)

		_ = c.RunE(ctx, option.WithNodes(node), `mkdir -p ~/logs/report/jasyncsql-results`)

		t.Status("making test directory")

		_ = c.RunE(ctx, option.WithNodes(node),
			`mkdir -p ~/logs/report/jasyncsql-results`,
		)

		t.Status("collecting the test results")

		if err := repeatRunE(
			ctx,
			t,
			c,
			node,
			"copy test result files",
			`cp /mnt/data1/jasync-sql/postgresql-async/build/test-results/test/*.xml ~/logs/report/jasyncsql-results -a`,
		); err != nil {
			t.Fatal(err)
		}

		// Load all test results
		result, err := repeatRunWithDetailsSingleNode(
			ctx,
			c,
			t,
			node,
			"get list of test files",
			`ls /mnt/data1/jasync-sql/postgresql-async/build/test-results/test/*xml`,
		)
		if err != nil {
			t.Fatal(err)
		}
		if len(result.Stdout) == 0 {
			t.Fatal("could not find any test result files")
		}

		parseAndSummarizeJavaORMTestsResults(
			ctx, t, c, node, "jasyncsql", []byte(result.Stdout),
			"jasyncSqlBlocklist", jasyncSqlBlocklist, jasyncsqlIgnoreList, version, supportedJasyncCommit)
	}

	r.Add(registry.TestSpec{
		Name:             "jasync",
		Owner:            registry.OwnerSQLFoundations,
		Cluster:          r.MakeClusterSpec(1),
		Leases:           registry.MetamorphicLeases,
		CompatibleClouds: registry.AllExceptAWS,
		Suites:           registry.Suites(registry.Nightly, registry.ORM),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runJasyncSQL(ctx, t, c)
		},
	})
}
