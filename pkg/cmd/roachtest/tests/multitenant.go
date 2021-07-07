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
	"path/filepath"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func runAcceptanceMultitenant(ctx context.Context, t test.Test, c cluster.Cluster) {
	c.Put(ctx, t.Cockroach(), "./cockroach")

	c.Start(ctx, c.All())

	const tenantID = 123
	{
		_, err := c.Conn(ctx, 1).Exec(`SELECT crdb_internal.create_tenant($1)`, tenantID)
		require.NoError(t, err)
	}

	kvAddrs, err := c.ExternalAddr(ctx, c.All())
	require.NoError(t, err)

	tenantCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	const (
		tenantHTTPPort = 8081
		tenantSQLPort  = 36258
	)
	const tenantNode = 1
	tenant := createTenantNode(tenantCtx, t, c, "./cockroach", kvAddrs,
		tenantID, tenantNode, tenantHTTPPort, tenantSQLPort)

	t.Status("checking that a client can connect to the tenant server")

	verifySQL(t, tenant.pgURL,
		mkStmt(`CREATE TABLE foo (id INT PRIMARY KEY, v STRING)`),
		mkStmt(`INSERT INTO foo VALUES($1, $2)`, 1, "bar"),
		mkStmt(`SELECT * FROM foo LIMIT 1`).
			withResults([][]string{{"1", "bar"}}))

	t.Status("stopping the server ahead of checking for the tenant server")

	// Stop the server, which also ensures that log files get flushed.
	tenant.stop(ctx, t, c)

	t.Status("checking log file contents")

	// Check that the server identifiers are present in the tenant log file.
	logFile := filepath.Join(tenant.logDir(), "cockroach.log")
	if err := c.RunE(ctx, c.Node(1),
		"grep", "-q", "'\\[config\\] .* clusterID:'", logFile); err != nil {
		t.Fatal(errors.Wrap(err, "cluster ID not found in log file"))
	}
	if err := c.RunE(ctx, c.Node(1),
		"grep", "-q", "'\\[config\\] .* tenantID:'", logFile); err != nil {
		t.Fatal(errors.Wrap(err, "tenant ID not found in log file"))
	}
	if err := c.RunE(ctx, c.Node(1),
		"grep", "-q", "'\\[config\\] .* instanceID:'", logFile); err != nil {
		t.Fatal(errors.Wrap(err, "SQL instance ID not found in log file"))
	}
}
