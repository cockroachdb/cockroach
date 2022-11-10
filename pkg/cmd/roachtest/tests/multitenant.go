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
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func runAcceptanceMultitenant(ctx context.Context, t test.Test, c cluster.Cluster) {
	c.Put(ctx, t.Cockroach(), "./cockroach")

	c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings(install.SecureOption(true)), c.All())

	const tenantID = 123
	{
		_, err := c.Conn(ctx, t.L(), 1).Exec(`SELECT crdb_internal.create_tenant($1::INT)`, tenantID)
		require.NoError(t, err)
	}

	const (
		tenantHTTPPort = 8081
		tenantSQLPort  = 30258
	)
	const tenantNode = 1
	tenant := createTenantNode(ctx, t, c, c.All(), tenantID, tenantNode, tenantHTTPPort, tenantSQLPort)
	tenant.start(ctx, t, c, "./cockroach")

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
	logFile := filepath.Join(tenant.logDir(), "*.log")
	if err := c.RunE(ctx, c.Node(1),
		"grep", "-q", "'start\\.go.*clusterID:'", logFile); err != nil {
		t.Fatal(errors.Wrap(err, "cluster ID not found in log file"))
	}
	if err := c.RunE(ctx, c.Node(1),
		"grep", "-q", "'start\\.go.*tenantID:'", logFile); err != nil {
		t.Fatal(errors.Wrap(err, "tenant ID not found in log file"))
	}
	if err := c.RunE(ctx, c.Node(1),
		"grep", "-q", "'start\\.go.*instanceID:'", logFile); err != nil {
		t.Fatal(errors.Wrap(err, "SQL instance ID not found in log file"))
	}
}
