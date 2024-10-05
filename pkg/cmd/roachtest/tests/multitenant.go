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
	"github.com/cockroachdb/cockroach/pkg/roachprod"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/stretchr/testify/require"
)

func runAcceptanceMultitenant(ctx context.Context, t test.Test, c cluster.Cluster) {
	// Start the storage layer.
	storageNodes := c.All()
	c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings(), storageNodes)

	// Start a virtual cluster.
	const virtualClusterName = "acceptance-tenant"
	virtualClusterNode := c.Node(1)
	c.StartServiceForVirtualCluster(
		ctx, t.L(),
		option.StartVirtualClusterOpts(virtualClusterName, virtualClusterNode),
		install.MakeClusterSettings(),
	)

	virtualClusterURL := func() string {
		urls, err := c.ExternalPGUrl(ctx, t.L(), virtualClusterNode, roachprod.PGURLOptions{
			VirtualClusterName: virtualClusterName,
		})
		require.NoError(t, err)

		return urls[0]
	}()

	t.L().Printf("checking that a client can connect to the tenant server")
	verifySQL(t, virtualClusterURL,
		mkStmt(`CREATE TABLE foo (id INT PRIMARY KEY, v STRING)`),
		mkStmt(`INSERT INTO foo VALUES($1, $2)`, 1, "bar"),
		mkStmt(`SELECT * FROM foo LIMIT 1`).
			withResults([][]string{{"1", "bar"}}))

	// Verify that we are able to stop the virtual cluster instance.
	t.L().Printf("stopping the virtual cluster instance")
	c.StopServiceForVirtualCluster(
		ctx, t.L(),
		option.StopVirtualClusterOpts(virtualClusterName, virtualClusterNode),
	)

	db := c.Conn(
		ctx, t.L(), virtualClusterNode[0], option.VirtualClusterName(virtualClusterName),
	)
	defer db.Close()

	_, err := db.ExecContext(ctx, "CREATE TABLE bar (id INT PRIMARY KEY)")
	require.Error(t, err)
	t.L().Printf("after virtual cluster stopped, received error: %v", err)
}
