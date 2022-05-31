// Copyright 2022 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
)

const rustPostgresSupportedTag = "postgres-v0.19.3"

func registerRustPostgres(r registry.Registry) {
	runRustPostgres := func(ctx context.Context, t test.Test, c cluster.Cluster) {
		if c.IsLocal() {
			t.Fatal("can not be run in local mode")
		}
		node := c.Node(1)
		t.Status("setting up cockroach")
		c.Put(ctx, t.Cockroach(), "./cockroach", c.All())

		c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings(), c.All())
		db := c.Conn(ctx, t.L(), 1)
		_, err := db.Exec("create user postgres with createdb createlogin createrole cancelquery")
		if err != nil {
			t.Fatal(err)
		}
		version, err := fetchCockroachVersion(ctx, t.L(), c, node[0])
		if err != nil {
			t.Fatal(err)
		}

		if err := alterZoneConfigAndClusterSettings(ctx, t, version, c, node[0]); err != nil {
			t.Fatal(err)
		}

		t.Status("cloning rust-postgres and installing prerequisites")
		if err := repeatRunE(
			ctx,
			t,
			c,
			node,
			"remove old rust-postgres",
			`rm -rf /mnt/data1/rustpostgres`,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatGitCloneE(
			ctx,
			t,
			c,
			"https://github.com/sfackler/rust-postgres.git",
			"/mnt/data1/rust-postgres",
			rustPostgresSupportedTag,
			node,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatRunE(
			ctx,
			t,
			c,
			node,
			"install rust and cargo",
			`curl https://sh.rustup.rs -sSf | sh -s -- -y
`,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatRunE(
			ctx,
			t,
			c,
			node,
			"install C linker",
			"sudo apt-get install build-essential -y",
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatRunE(
			ctx,
			t,
			c,
			node,
			" Installing more build essentials",
			"sudo apt-get install -y pkg-config libssl-dev",
		); err != nil {
			t.Fatal(err)
		}

		t.Status("building rust postgres (without test)")

		blocklistName := "rustPostgresBlockList"
		ignorelistName := "rustPostgresIgnoreList"
		expectedFailures := rustPostgresBlocklist
		ignorelist := rustPostgresIgnoreList

		status := fmt.Sprintf("running cockroach version %s, using blocklist %s", version, blocklistName)
		if ignorelist != nil {
			status = fmt.Sprintf(
				"Running cockroach %s, using blocklist %s, using ignorelist %s",
				version,
				blocklistName,
				ignorelistName)
		}
		t.L().Printf("%s", status)

		// We stop the cluster and restart with a port of 5433 since Rust postgres
		// has all of it's test hardcoded to use that port.
		c.Stop(ctx, t.L(), option.DefaultStopOpts(), c.All())
		startOpts := option.DefaultStartOpts()
		startOpts.RoachprodOpts.ExtraArgs = []string{"--port=5433"}
		c.Start(ctx, t.L(), startOpts, install.MakeClusterSettings(), c.All())

		t.Status("Running rust-postgres test suite")

		result, err := c.RunWithDetailsSingleNode(
			ctx,
			t.L(),
			node,
			`cd /mnt/data1/rust-postgres && /home/ubuntu/.cargo/bin/cargo test 2>&1 > rustpostgres.stdout --no-fail-fast`)
		if err != nil {
			t.L().Printf("error during rust postgres run (may be ok): %v\n", err)
		}

		t.L().Printf("Test stdout for rust-postgres")
		result, err = c.RunWithDetailsSingleNode(
			ctx, t.L(), node, "cd /mnt/data1/rust-postgres && cat rustpostgres.stdout",
		)
		if err != nil {
			t.Fatal(err)
		}

		t.L().Printf("Test results for rust postgres: %s", result.Stdout+result.Stderr)
		t.Status("collating the test results")

		results := newORMTestsResults()

		results.parseRustUnitTestOutput([]byte(result.Stdout+result.Stderr), expectedFailures, ignorelist)
		t.L().Printf("Test pass for rust postgres: %d, Test fail for rust postgres: %d", results.passExpectedCount, results.failUnexpectedCount)
		results.summarizeAll(
			t, "rust-postgres" /* ormName */, blocklistName, expectedFailures, version, "",
		)
	}

	r.Add(registry.TestSpec{
		Name:    "rust-postgres",
		Owner:   registry.OwnerSQLExperience,
		Cluster: r.MakeClusterSpec(1, spec.CPU(16)),
		Tags:    []string{`default`, `orm`},
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runRustPostgres(ctx, t, c)
		},
	})
}
