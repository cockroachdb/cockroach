// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
)

// This test tests that we can add and drop regions even if the locality flags
// of a node no longer match the regions that already were added to the
// database.
func runMismatchedLocalityTest(ctx context.Context, t test.Test, c cluster.Cluster) {
	// Start 3 nodes with a different localities.
	startOpts := option.DefaultStartOpts()
	startOpts.RoachprodOpts.ExtraArgs = []string{"--locality=region=east"}
	c.Start(ctx, t.L(), startOpts, install.MakeClusterSettings(), c.Nodes(1))
	startOpts.RoachprodOpts.ExtraArgs = []string{"--locality=region=central"}
	c.Start(ctx, t.L(), startOpts, install.MakeClusterSettings(), c.Nodes(2))
	startOpts.RoachprodOpts.ExtraArgs = []string{"--locality=region=west"}
	c.Start(ctx, t.L(), startOpts, install.MakeClusterSettings(), c.Nodes(3))

	// Add the 3 regions to the database.
	db := c.Conn(ctx, t.L(), 1)
	if _, err := db.Exec(`ALTER DATABASE defaultdb PRIMARY REGION "east";`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`ALTER DATABASE defaultdb ADD REGION "central";`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`ALTER DATABASE defaultdb ADD REGION "west";`); err != nil {
		t.Fatal(err)
	}

	// Restart all the nodes with new localities.
	c.Stop(ctx, t.L(), option.DefaultStopOpts(), c.Nodes(1))
	startOpts.RoachprodOpts.ExtraArgs = []string{"--locality=region=mars"}
	c.Start(ctx, t.L(), startOpts, install.MakeClusterSettings(), c.Nodes(1))
	c.Stop(ctx, t.L(), option.DefaultStopOpts(), c.Nodes(2))
	startOpts.RoachprodOpts.ExtraArgs = []string{"--locality=region=jupiter"}
	c.Start(ctx, t.L(), startOpts, install.MakeClusterSettings(), c.Nodes(2))
	c.Stop(ctx, t.L(), option.DefaultStopOpts(), c.Nodes(3))
	startOpts.RoachprodOpts.ExtraArgs = []string{"--locality=region=venus"}
	c.Start(ctx, t.L(), startOpts, install.MakeClusterSettings(), c.Nodes(3))

	// Verify that we can add and drop regions for the database. There's no longer
	// any node with the old localities, but that's fine.
	db = c.Conn(ctx, t.L(), 3)
	if err := roachtestutil.WaitFor3XReplication(ctx, t.L(), db); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`ALTER DATABASE defaultdb ADD REGION "venus";`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`ALTER DATABASE defaultdb DROP REGION "central";`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`ALTER DATABASE defaultdb ADD REGION "jupiter";`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`ALTER DATABASE defaultdb ADD REGION "mars";`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`ALTER DATABASE defaultdb SET PRIMARY REGION "mars";`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`ALTER DATABASE defaultdb DROP REGION "west";`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`ALTER DATABASE defaultdb DROP REGION "east";`); err != nil {
		t.Fatal(err)
	}
}
