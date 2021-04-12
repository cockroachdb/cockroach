// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

import (
	"context"
	"net/url"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func runAcceptanceMultitenant(ctx context.Context, t *test, c *cluster) {
	c.Put(ctx, cockroach, "./cockroach")

	c.Start(ctx, t, c.All())

	const tenantID = 123
	{
		_, err := c.Conn(ctx, 1).Exec(`SELECT crdb_internal.create_tenant($1)`, tenantID)
		require.NoError(t, err)
	}

	kvAddrs := c.ExternalAddr(ctx, c.All())

	tenantCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	errCh := startTenantServer(
		tenantCtx, c, "./cockroach", kvAddrs, tenantID,
		// Ensure that log files get created.
		"--log='file-defaults: {dir: logs/mt}'",
	)
	u, err := url.Parse(c.ExternalPGUrl(ctx, c.Node(1))[0])
	require.NoError(t, err)
	u.Host = c.ExternalIP(ctx, c.Node(1))[0] + ":36257"
	url := u.String()
	c.l.Printf("sql server should be running at %s", url)

	time.Sleep(time.Second)

	select {
	case err := <-errCh:
		t.Fatal(err)
	default:
	}

	t.Status("checking that a client can connect to the tenant server")

	verifySQL(t, url,
		mkStmt(`CREATE TABLE foo (id INT PRIMARY KEY, v STRING)`),
		mkStmt(`INSERT INTO foo VALUES($1, $2)`, 1, "bar"),
		mkStmt(`SELECT * FROM foo LIMIT 1`).
			withResults([][]string{{"1", "bar"}}))

	t.Status("stopping the server ahead of checking for the tenant server")

	// Stop the server, which also ensures that log files get flushed.
	cancel()
	<-errCh

	t.Status("checking log file contents")

	// Check that the server identifiers are present in the tenant log file.
	if err := c.RunE(ctx, c.Node(1),
		"grep", "-q", "'\\[config\\] .* clusterID:'", "logs/mt/cockroach.log"); err != nil {
		t.Fatal(errors.Wrap(err, "cluster ID not found in log file"))
	}
	if err := c.RunE(ctx, c.Node(1),
		"grep", "-q", "'\\[config\\] .* tenantID:'", "logs/mt/cockroach.log"); err != nil {
		t.Fatal(errors.Wrap(err, "tenant ID not found in log file"))
	}
	if err := c.RunE(ctx, c.Node(1),
		"grep", "-q", "'\\[config\\] .* instanceID:'", "logs/mt/cockroach.log"); err != nil {
		t.Fatal(errors.Wrap(err, "SQL instance ID not found in log file"))
	}
}
