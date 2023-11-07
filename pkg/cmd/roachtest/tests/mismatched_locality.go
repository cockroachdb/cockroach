// Copyright 2023 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
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

	// Restart a node and change the locality to something new.
	c.Stop(ctx, t.L(), option.DefaultStopOpts(), c.Nodes(3))
	startOpts.RoachprodOpts.ExtraArgs = []string{"--locality=region=mars"}
	c.Start(ctx, t.L(), startOpts, install.MakeClusterSettings(), c.Nodes(3))

	// Verify that we can add the new region to the database, and drop existing
	// ones. There's no longer any node with a "west" locality, but that's fine.
	db = c.Conn(ctx, t.L(), 3)
	if _, err := db.Exec(`ALTER DATABASE defaultdb ADD REGION "mars";`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`ALTER DATABASE defaultdb DROP REGION "central";`); err != nil {
		t.Fatal(err)
	}

	// Restart the same node and change the locality back to "west".
	c.Stop(ctx, t.L(), option.DefaultStopOpts(), c.Nodes(3))
	startOpts.RoachprodOpts.ExtraArgs = []string{"--locality=region=west"}
	c.Start(ctx, t.L(), startOpts, install.MakeClusterSettings(), c.Nodes(3))

	// Verify that we can drop the "mars" region from the database. There's no
	// longer any node with a "mars" locality, but that's fine.
	db = c.Conn(ctx, t.L(), 1)
	if _, err := db.Exec(`ALTER DATABASE defaultdb DROP REGION "mars";`); err != nil {
		t.Fatal(err)
	}
}
