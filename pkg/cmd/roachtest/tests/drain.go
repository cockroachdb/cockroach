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
	gosql "database/sql"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/jackc/pgx/v4"
	"github.com/stretchr/testify/require"
)

func registerDrain(r registry.Registry) {
	{
		r.Add(registry.TestSpec{
			Name:    "drain/early-exit-conn-wait",
			Owner:   registry.OwnerSQLFoundations,
			Cluster: r.MakeClusterSpec(1),
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				runEarlyExitInConnectionWait(ctx, t, c)
			},
		})

		r.Add(registry.TestSpec{
			Name:    "drain/warn-conn-wait-timeout",
			Owner:   registry.OwnerSQLFoundations,
			Cluster: r.MakeClusterSpec(1),
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				runWarningForConnWait(ctx, t, c)
			},
		})

		r.Add(registry.TestSpec{
			Name:    "drain/not-at-quorum",
			Owner:   registry.OwnerSQLFoundations,
			Cluster: r.MakeClusterSpec(3),
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				runClusterNotAtQuorum(ctx, t, c)
			},
		})
	}
}

// runEarlyExitInConnectionWait is to verify that draining proceeds immediately
// after connections are closed client-side.
func runEarlyExitInConnectionWait(ctx context.Context, t test.Test, c cluster.Cluster) {
	var err error
	const (
		// Set the duration of each phase of the draining period.
		drainWaitDuration      = 10 * time.Second
		connectionWaitDuration = 100 * time.Second
		queryWaitDuration      = 10 * time.Second

		// server.shutdown.lease_transfer_wait defaults to 5 seconds.
		leaseTransferWaitDuration = 5 * time.Second

		// pokeDuringDrainWaitDelay is the amount of time after drain begins when we
		// will check that the server is reporting itself as unhealthy to load
		// balancers. It should be set smaller than drainWaitDuration.
		pokeDuringDrainWaitDelay = 5 * time.Second

		// pokeDuringConnWaitDelay is the amount of time after drain begins when we
		// will check that the server is waiting for SQL connections to close. It
		// should be set larger than drainWaitDuration, but smaller than
		// (drainWaitDuration + connectionWaitDuration).
		pokeDuringConnWaitDelay = 20 * time.Second

		connMaxLifetime = 10 * time.Second
		connMaxCount    = 5
		nodeToDrain     = 1
	)

	prepareCluster(ctx, t, c, drainWaitDuration, connectionWaitDuration, queryWaitDuration)

	db := c.Conn(ctx, t.L(), nodeToDrain)
	defer db.Close()

	db.SetConnMaxLifetime(connMaxLifetime)
	db.SetMaxOpenConns(connMaxCount)

	var conns []*gosql.Conn

	// Get two connections from the connection pools.
	for j := 0; j < 2; j++ {
		conn, err := db.Conn(ctx)
		require.NoError(t, err, "failed to a SQL connection from the connection pool")
		conns = append(conns, conn)
	}

	// Start draining the node.
	m := c.NewMonitor(ctx, c.Node(nodeToDrain))

	m.Go(func(ctx context.Context) error {
		t.Status(fmt.Sprintf("start draining node %d", nodeToDrain))
		results, err := c.RunWithDetailsSingleNode(
			ctx,
			t.L(),
			c.Node(nodeToDrain),
			// --drain-wait is set to a low value so that we can confirm that it
			// gets automatically upgraded to use a higher value larger than the sum
			// of server.shutdown.drain_wait, server.shutdown.connection_wait,
			// server.shutdown.query_wait times two, and
			// server.shutdown.lease_transfer_wait.
			"./cockroach node drain --self --insecure --drain-wait=10s",
		)
		if err != nil {
			return err
		}

		expectedDrain := drainWaitDuration + connectionWaitDuration + queryWaitDuration*2 + leaseTransferWaitDuration + 10*time.Second
		if !strings.Contains(
			results.Stderr,
			fmt.Sprintf(
				"cluster settings require a value of at least %s; using the larger value",
				expectedDrain),
		) {
			return errors.Newf("expected --drain-wait to be upgraded to %s", expectedDrain)
		}

		return nil
	})

	drainStartTimestamp := timeutil.Now()

	// Sleep till the server is in the status of reporting itself as unhealthy.
	// Verify that the server is still allowing new SQL connections now.
	time.Sleep(pokeDuringDrainWaitDelay)
	t.Status(fmt.Sprintf("%s after draining starts, health check returns false", pokeDuringDrainWaitDelay))
	conn, err := db.Conn(ctx)
	require.NoError(t, err, "new SQL connection should be allowed during drain_wait")
	err = conn.Close()
	require.NoError(t, err)
	addr, err := c.ExternalAdminUIAddr(ctx, t.L(), c.Node(nodeToDrain))
	require.NoError(t, err)
	url := `http://` + addr[0] + `/health?ready=1`
	resp, err := httputil.Get(ctx, url)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equalf(t, http.StatusServiceUnavailable, resp.StatusCode, "expected healthcheck to fail during drain")
	bodyBytes, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Contains(t, string(bodyBytes), "node is shutting down")

	// Sleep till the server is in the status of waiting for users to close SQL
	// connections. Verify that the server is rejecting new SQL connections now.
	time.Sleep(drainStartTimestamp.Add(pokeDuringConnWaitDelay).Sub(timeutil.Now()))
	t.Status(fmt.Sprintf("%s after draining starts, server is rejecting new SQL connections", pokeDuringConnWaitDelay))
	if _, err := db.Conn(ctx); err != nil {
		require.ErrorContains(t, err, "server is not accepting clients, try another node")
	} else {
		t.Fatal(errors.New("new SQL connections should not be allowed when the server " +
			"starts waiting for the user to close SQL connections"))
	}

	t.Status("number of open connections: ", db.Stats().OpenConnections)
	require.Equalf(t, db.Stats().OpenConnections, 2, "number of open connections should be 2")

	randConn := conns[rand.Intn(len(conns))]
	t.Status("execing sql query with connection")

	// When server is waiting clients to close connections, verify that SQL
	// queries do not fail.
	_, err = randConn.ExecContext(ctx, "SELECT 1;")
	require.NoError(t, err, "expected query not to fail before the "+
		"server starts draining SQL connections")

	for _, conn := range conns {
		err := conn.Close()
		require.NoError(t, err,
			"expected connection to be able to be successfully closed client-side")
	}

	t.Status("all SQL connections are closed")

	err = m.WaitE()
	require.NoError(t, err, "error waiting for the draining to finish")

	drainEndTimestamp := timeutil.Now()
	actualDrainDuration := drainEndTimestamp.Sub(drainStartTimestamp)

	t.L().Printf("the draining lasted %f seconds", actualDrainDuration)

	totalWaitDuration := drainWaitDuration + connectionWaitDuration + queryWaitDuration
	if actualDrainDuration >= totalWaitDuration-10*time.Second {
		t.Fatal(errors.New("the draining process didn't early exit " +
			"when waiting for server to close all SQL connections"))
	}

}

// runWarningForConnWait is to verify a warning exists in the case that
// connectionWait expires.
func runWarningForConnWait(ctx context.Context, t test.Test, c cluster.Cluster) {
	var err error
	const (
		// Set the duration of the draining period.
		drainWaitDuration      = 0 * time.Second
		connectionWaitDuration = 10 * time.Second
		queryWaitDuration      = 20 * time.Second
		// pokeDuringQueryWaitDelay is the amount of time after drain begins when we
		// will check that the server does not allow any new query to begin on a
		// connection that is still open. It
		// should be set larger than (drainWaitDuration + connectionWaitDuration),
		// but smaller than
		// (drainWaitDuration + connectionWaitDuration + queryWaitDuration).
		pokeDuringQueryWaitDelay = 15 * time.Second
		nodeToDrain              = 1
	)

	prepareCluster(ctx, t, c, drainWaitDuration, connectionWaitDuration, queryWaitDuration)

	pgURL, err := c.ExternalPGUrl(ctx, t.L(), c.Node(nodeToDrain), "" /* tenant */)
	require.NoError(t, err)
	connNoTxn, err := pgx.Connect(ctx, pgURL[0])
	require.NoError(t, err)
	connWithTxn, err := pgx.Connect(ctx, pgURL[0])
	require.NoError(t, err)
	connWithSleep, err := pgx.Connect(ctx, pgURL[0])
	require.NoError(t, err)

	m := c.NewMonitor(ctx, c.Node(nodeToDrain))
	m.Go(func(ctx context.Context) error {
		t.Status(fmt.Sprintf("draining node %d", nodeToDrain))
		return c.RunE(ctx,
			c.Node(nodeToDrain),
			"./cockroach node drain --self --insecure --drain-wait=600s",
		)
	})

	// The connection should work still.
	var result int
	err = connNoTxn.QueryRow(ctx, "SELECT 1").Scan(&result)
	require.NoError(t, err)
	require.Equal(t, 1, result)

	// A query that takes longer than the total wait duration should be canceled.
	m.Go(func(ctx context.Context) error {
		_, err := connWithSleep.Exec(ctx, "SELECT pg_sleep(200)")
		if testutils.IsError(err, "(query execution canceled|server is shutting down|connection reset by peer|unexpected EOF)") {
			return nil
		}
		if err == nil {
			return errors.New("expected pg_sleep query to fail")
		}
		return errors.Wrapf(err, "expected pg_sleep query to fail; but got the wrong error")
	})

	// Start a txn before the query_wait period begins.
	tx, err := connWithTxn.Begin(ctx)
	require.NoError(t, err)

	time.Sleep(pokeDuringQueryWaitDelay)
	t.Status(fmt.Sprintf("%s after draining starts, server is rejecting new "+
		"queries on existing SQL connections", pokeDuringQueryWaitDelay))

	// A connection with no open transaction should have been closed
	// automatically by this point.
	err = connNoTxn.QueryRow(ctx, "SELECT 1").Scan(&result)
	if !testutils.IsError(err, "(server is shutting down|connection reset by peer|unexpected EOF)") {
		require.FailNowf(t, "expected error from trying to use an idle connection", "the actual error was %v", err)
	}

	// A transaction that was already open should still work.
	err = tx.QueryRow(ctx, "SELECT 2").Scan(&result)
	require.NoError(t, err)
	require.Equal(t, 2, result)
	err = tx.Commit(ctx)
	require.NoError(t, err)
	// And that connection should be closed right after the commit happens.
	_, err = connWithTxn.Exec(ctx, "SELECT 1")
	if !testutils.IsError(err, "(server is shutting down|connection reset by peer|unexpected EOF)") {
		require.FailNowf(t, "expected error from trying to use an idle connection", "the actual error was %v", err)
	}

	err = m.WaitE()
	require.NoError(t, err, "error waiting for the draining to finish")

	logFile := filepath.Join("logs", "*.log")
	err = c.RunE(ctx, c.Node(nodeToDrain),
		"grep", "-q", "'proceeding to drain SQL connections'", logFile)
	require.NoError(t, err, "warning is not logged in the log file")
}

// runClusterNotAtQuorum is to verify that draining works even when the cluster
// is not at quorum.
func runClusterNotAtQuorum(ctx context.Context, t test.Test, c cluster.Cluster) {
	err := c.PutE(ctx, t.L(), t.Cockroach(), "./cockroach", c.All())
	if err != nil {
		t.Fatal(err)
	}

	c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings(), c.All())
	db := c.Conn(ctx, t.L(), 1)
	defer func() { _ = db.Close() }()

	err = WaitFor3XReplication(ctx, t, db)
	require.NoError(t, err)

	stopOpts := option.DefaultStopOpts()
	stopOpts.RoachprodOpts.Sig = 9 // SIGKILL

	c.Stop(ctx, t.L(), stopOpts, c.Node(1))
	c.Stop(ctx, t.L(), stopOpts, c.Node(2))

	t.Status("start draining node 3")
	// Ignore the error, since the command is expected to time out.
	results, _ := c.RunWithDetailsSingleNode(
		ctx,
		t.L(),
		c.Node(3),
		"./cockroach node drain --self --insecure --drain-wait=10s",
	)
	t.L().Printf("drain output:\n%s\n%s\n", results.Stdout, results.Stderr)
	require.Regexp(t, "(cluster settings require a value of at least|could not check drain related cluster settings)", results.Stderr)
}

// prepareCluster is to start the server on nodes in the given cluster, and set
// the cluster setting for duration of each phase of the draining process.
func prepareCluster(
	ctx context.Context,
	t test.Test,
	c cluster.Cluster,
	drainWait time.Duration,
	connectionWait time.Duration,
	queryWait time.Duration,
) {
	var err error
	err = c.PutE(ctx, t.L(), t.Cockroach(), "./cockroach", c.All())
	if err != nil {
		t.Fatal(err)
	}

	c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings(), c.All())

	db := c.Conn(ctx, t.L(), 1)
	defer db.Close()

	waitPhasesSettingStmts := []string{
		fmt.Sprintf("SET CLUSTER SETTING server.shutdown.drain_wait = '%fs';", drainWait.Seconds()),
		fmt.Sprintf("SET CLUSTER SETTING server.shutdown.query_wait = '%fs'", queryWait.Seconds()),
		fmt.Sprintf("SET CLUSTER SETTING server.shutdown.connection_wait = '%fs'", connectionWait.Seconds()),
	}
	for _, stmt := range waitPhasesSettingStmts {
		_, err = db.ExecContext(ctx, stmt)
		require.NoError(t, err, "cannot set cluster setting")
	}
}
