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
	"math/rand"
	"path/filepath"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func registerDrain(r registry.Registry) {
	{
		r.Add(registry.TestSpec{
			Name:    "drain/early-exit-conn-wait",
			Owner:   registry.OwnerSQLExperience,
			Cluster: r.MakeClusterSpec(1),
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				runEarlyExitInConnectionWait(ctx, t, c)
			},
		})

		r.Add(registry.TestSpec{
			Name:    "drain/warn-conn-wait-timeout",
			Owner:   registry.OwnerSQLExperience,
			Cluster: r.MakeClusterSpec(1),
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				runTestWarningForConnWait(ctx, t, c)
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
		drainWaitDuration      = 5 * time.Second
		connectionWaitDuration = 100 * time.Second
		queryWaitDuration      = 10 * time.Second
		// pokeDuringConnWaitTimestamp is the timestamp after the server
		// starts waiting for SQL connections to close (with the start of the whole
		// draining process marked as timestamp 0). It should be set larger than
		// drainWaitDuration, but smaller than (drainWaitDuration +
		// connectionWaitDuration).
		pokeDuringConnWaitTimestamp = 15 * time.Second
		connMaxLifetime             = 10 * time.Second
		connMaxCount                = 5
		nodeToDrain                 = 1
	)
	totalWaitDuration := drainWaitDuration + connectionWaitDuration + queryWaitDuration

	prepareCluster(ctx, t, c, drainWaitDuration, connectionWaitDuration, queryWaitDuration)

	db := c.Conn(ctx, t.L(), nodeToDrain, "")
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
		return c.RunE(ctx,
			c.Node(nodeToDrain),
			fmt.Sprintf("./cockroach node drain --insecure --drain-wait=%fs",
				totalWaitDuration.Seconds()))
	})

	drainStartTimestamp := timeutil.Now()

	// Sleep till the server is in the status of waiting for users to close SQL
	// connections. Verify that the server is rejecting new SQL connections now.
	time.Sleep(pokeDuringConnWaitTimestamp)

	if _, err := db.Conn(ctx); err != nil {
		t.Status(fmt.Sprintf("%s after draining starts, server is rejecting "+
			"new SQL connections: %v", pokeDuringConnWaitTimestamp, err))
	} else {
		t.Fatal(errors.New("new SQL connections should not be allowed when the server " +
			"starts waiting for the user to close SQL connections"))
	}

	require.Equalf(t, db.Stats().OpenConnections, 2, "number of open connections should be 2")

	t.Status("number of open connections: ", db.Stats().OpenConnections)

	randConn := conns[rand.Intn(len(conns))]
	t.Status("execting sql query with connection %s", randConn)

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

	t.Status("all SQL connections are put back to the connection pool")

	err = m.WaitE()
	require.NoError(t, err, "error waiting for the draining to finish")

	drainEndTimestamp := timeutil.Now()
	actualDrainDuration := drainEndTimestamp.Sub(drainStartTimestamp).Seconds()

	t.L().Printf("the draining lasted %f seconds", actualDrainDuration)

	if actualDrainDuration >= float64(totalWaitDuration)-10 {
		t.Fatal(errors.New("the draining process didn't early exit " +
			"when waiting for server to close all SQL connections"))
	}

}

// runTestWarningForConnWait is to verify a warning exists in the case that
// connectionWait expires.
func runTestWarningForConnWait(ctx context.Context, t test.Test, c cluster.Cluster) {
	var err error
	const (
		// Set the duration of the draining period.
		drainWaitDuration      = 0 * time.Second
		connectionWaitDuration = 10 * time.Second
		queryWaitDuration      = 20 * time.Second
		nodeToDrain            = 1
	)

	totalWaitDuration := drainWaitDuration + connectionWaitDuration + queryWaitDuration

	prepareCluster(ctx, t, c, drainWaitDuration, connectionWaitDuration, queryWaitDuration)

	db := c.Conn(ctx, t.L(), nodeToDrain, "")
	defer db.Close()

	// Get a connection from the connection pool.
	_, err = db.Conn(ctx)

	require.NoError(t, err, "cannot get a SQL connection from the connection pool")

	m := c.NewMonitor(ctx, c.Node(nodeToDrain))
	m.Go(func(ctx context.Context) error {
		t.Status(fmt.Sprintf("draining node %d", nodeToDrain))
		return c.RunE(ctx,
			c.Node(nodeToDrain),
			fmt.Sprintf("./cockroach node drain --insecure --drain-wait=%fs",
				totalWaitDuration.Seconds()))
	})

	err = m.WaitE()
	require.NoError(t, err, "error waiting for the draining to finish")

	logFile := filepath.Join("logs", "*.log")
	err = c.RunE(ctx, c.Node(nodeToDrain),
		"grep", "-q", "'proceeding to drain SQL connections'", logFile)
	require.NoError(t, err, "warning is not logged in the log file")
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
	require.NoError(t, err, "cannot mount cockroach binary")

	c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings(), c.All())

	db := c.Conn(ctx, t.L(), 1, "")
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
