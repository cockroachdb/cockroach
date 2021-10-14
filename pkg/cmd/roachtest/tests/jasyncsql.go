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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
)

func registerJasyncSQL(r registry.Registry) {
	runJasyncSQL := func(ctx context.Context, t test.Test, c cluster.Cluster) {
		if c.IsLocal() {
			t.Fatal("can not be run in local mode")
		}
		node := c.Node(1)
		t.Status("setting up cockroach")
		c.Put(ctx, t.Cockroach(), "./cockroach", c.All())
		c.Start(ctx, c.All())

		version, err := fetchCockroachVersion(ctx, c, node[0])
		if err != nil {
			t.Fatal(err)
		}

		if err := alterZoneConfigAndClusterSettings(ctx, version, c, node[0]); err != nil {
			t.Fatal(err)
		}

		t.Status("cloning jasync-sql and installing prerequisites")

		//remove old
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

		if err := repeatGitCloneE(
			ctx,
			t,
			c,
			"https://github.com/e-mbrown/jasync-sql.git",
			"/mnt/data1/jasyncsql",
			"master",
			node,
		); err != nil {
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

		blocklistName, expectedFailures, ignorelistName, ignorelist := jasyncsqlBlocklists.getLists(version)
		if expectedFailures == nil {
			t.Fatalf("No jasyncsql blocklist defined for cockroach version %s", version)
		}
		status := fmt.Sprintf("running cockraoch version %s, using blocklist %s", version, blocklistName)
		if ignorelist != nil {
			status = fmt.Sprintf(
				"Running cockroach %s, using blocklist %s, using ignorelist %s",
				version,
				blocklistName,
				ignorelistName)
		}
		t.L().Printf("%s", status)

		t.Status("running jasyncsql test suite")

		if err := repeatRunE(
			ctx,
			t,
			c,
			node,
			"run jasync test suite",
			`cd /mnt/data1/jasyncsql && ./gradlew :postgresql-async:test`,
		); err != nil {
			t.Fatal(err)
		}

		_ = c.RunE(ctx, node, `mkdir -p ~/logs/report/jasyncsql-results`)

		t.Status("collecting the test results")

		if err := repeatRunE(
			ctx,
			t,
			c,
			node,
			"copy test result files",
			`cp /mnt/data1/jasyncsql/build/test-result/test/*.xml`,
		); err != nil {
			t.Fatal(err)
		}

		//Load all test results
		output, err := repeatRunWithBuffer(
			ctx,
			c,
			t,
			node,
			"get list of test files",
			`ls /mnt/data1/jasyncsql/build/test-results/test/*xml`,
		)
		if err != nil {
			t.Fatal(err)
		}
		if len(output) == 0 {
			t.Fatal("could not find any test result files")
		}

		// Is there a way to do this for jasync?
		// Do we need this?
		//parseAndSummarizeJavaORMTestsResults(
		//	ctx, t, c, node, "jasyncsql", output,
		//	blocklistName, expectedFailures, ignorelist, version, )
		//

	}

	r.Add(registry.TestSpec{
		Name:    "jasync",
		Owner:   registry.OwnerSQLExperience,
		Cluster: r.MakeClusterSpec(1),
		Tags:    []string{`default`, `orm`},
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runJasyncSQL(ctx, t, c)
		},
	})
}
