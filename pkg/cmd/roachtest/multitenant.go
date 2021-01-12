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
	gosql "database/sql"
	"net/url"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func runAcceptanceMultitenant(ctx context.Context, t *test, c *cluster) {
	c.Put(ctx, cockroach, "./cockroach")

	c.Start(ctx, t, c.All())

	_, err := c.Conn(ctx, 1).Exec(`SELECT crdb_internal.create_tenant(123)`)
	require.NoError(t, err)

	kvAddrs := c.ExternalAddr(ctx, c.All())

	tenantCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	errCh := make(chan error, 1)
	go func() {
		errCh <- c.RunE(tenantCtx, c.Node(1),
			"./cockroach", "mt", "start-sql",
			// TODO(tbg): make this test secure.
			// "--certs-dir", "certs",
			"--insecure",
			"--tenant-id", "123",
			"--http-addr", "127.0.0.1:8081",
			"--kv-addrs", strings.Join(kvAddrs, ","),
			// Don't bind to external interfaces when running locally.
			"--sql-addr", ifLocal("127.0.0.1", "0.0.0.0")+":36257",
			// Ensure that log files get created.
			"--log='file-defaults: {dir: .}'",
		)
		close(errCh)
	}()
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

	db, err := gosql.Open("postgres", url)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	_, err = db.Exec(`CREATE TABLE foo (id INT PRIMARY KEY, v STRING)`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO foo VALUES($1, $2)`, 1, "bar")
	require.NoError(t, err)

	var id int
	var v string
	require.NoError(t, db.QueryRow(`SELECT * FROM foo LIMIT 1`).Scan(&id, &v))
	require.Equal(t, 1, id)
	require.Equal(t, "bar", v)

	t.Status("stopping the server ahead of checking for the tenant server")

	// Stop the server, which also ensures that log files get flushed.
	cancel()
	<-errCh

	t.Status("checking log file contents")

	// Check that the server identifiers are present in the tenant log file.
	if err := c.RunE(ctx, c.Node(1),
		"grep", "-q", "'\\[config\\] clusterID:'", "cockroach.log"); err != nil {
		t.Fatal(errors.Wrap(err, "cluster ID not found in log file"))
	}
	if err := c.RunE(ctx, c.Node(1),
		"grep", "-q", "'\\[config\\] tenantID:'", "cockroach.log"); err != nil {
		t.Fatal(errors.Wrap(err, "tenant ID not found in log file"))
	}
	if err := c.RunE(ctx, c.Node(1),
		"grep", "-q", "'\\[config\\] instanceID:'", "cockroach.log"); err != nil {
		t.Fatal(errors.Wrap(err, "SQL instance ID not found in log file"))
	}
}
