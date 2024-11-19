// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
)

// runDecommissionSelf decommissions n2 through n2. This is an acceptance test.
//
// See https://github.com/cockroachdb/cockroach/issues/56718
func runDecommissionSelf(ctx context.Context, t test.Test, c cluster.Cluster) {
	n1, n2 := 1, 2
	c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings())

	if err := fullyDecommission(ctx, c, n2, n2, test.DefaultCockroachPath); err != nil {
		t.Fatal(err)
	}

	t.L().Printf("n2 decommissioned")
	db := c.Conn(ctx, t.L(), n1)
	defer db.Close()

	if err := newLivenessInfo(db).membershipEquals("decommissioned").eventuallyOnlyNode(n2); err != nil {
		t.Fatal(err)
	}
}
