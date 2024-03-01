// Copyright 2020 The Cockroach Authors.
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
	const sqlInstance = 0 // only one instance of this virtual cluster
	virtualClusterNode := c.Node(1)
	c.StartServiceForVirtualCluster(
		ctx, t.L(), virtualClusterNode,
		option.DefaultStartVirtualClusterOpts(virtualClusterName, sqlInstance),
		install.MakeClusterSettings(),
		storageNodes,
	)

	virtualClusterURL := func() string {
		urls, err := c.ExternalPGUrl(ctx, t.L(), virtualClusterNode, roachprod.PGURLOptions{
			VirtualClusterName: virtualClusterName, SQLInstance: sqlInstance,
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
		option.DefaultStopVirtualClusterOpts(virtualClusterName, sqlInstance),
		virtualClusterNode,
	)

	db := c.Conn(
		ctx, t.L(), virtualClusterNode[0], option.VirtualClusterName(virtualClusterName), option.SQLInstance(sqlInstance),
	)
	defer db.Close()

	_, err := db.ExecContext(ctx, "CREATE TABLE bar (id INT PRIMARY KEY)")
	require.Error(t, err)
	t.L().Printf("after virtual cluster stopped, received error: %v", err)
}
