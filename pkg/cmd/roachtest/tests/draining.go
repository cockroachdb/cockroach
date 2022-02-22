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
		numNodes := 2
		duration := time.Hour

		r.Add(registry.TestSpec{
			Name:    "drain/early-exit-conn-wait",
			Owner:   registry.OwnerSQLExperience,
			Cluster: r.MakeClusterSpec(numNodes),
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				runEarlyExitInConnectionWait(ctx, t, c, numNodes, duration)
			},
		})

		r.Add(registry.TestSpec{
			Name:    "drain/warn-conn-wait-timeout",
			Owner:   registry.OwnerSQLExperience,
			Cluster: r.MakeClusterSpec(numNodes),
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				runTestWarningForConnWait(ctx, t, c, numNodes)
			},
		})
	}
}

// testEarlyExitInConnectionWait test if the server early exit the current
// draining phase if all connections are closed by user before the
// duration set by `server.shutdown.connection_wait` times out.
func runEarlyExitInConnectionWait(
	ctx context.Context, t test.Test, c cluster.Cluster, nodes int, duration time.Duration,
) {

	var err error

	const (
		// Set the duration of each phase of the draining period.
		drainWaitDurationSec      = 10
		connectionWaitDurationSec = 60
		queryWaitDurationSec      = 10
		// pokeDuringConnWaitTimestamp is the timestamp after the server
		// starts waiting SQL connections to close (with the start of the whole
		// draining process marked as timestamp 0). It should be set larger than
		// drainWaitDurationSec, but smaller than (drainWaitDurationSec +
		// connectionWaitDurationSec).
		pokeDuringConnWaitTimestamp = 25
	)
	totalWaitDurationSec := drainWaitDurationSec + connectionWaitDurationSec + queryWaitDurationSec

	prepareCluster(ctx, t, c, nodes, drainWaitDurationSec, connectionWaitDurationSec, queryWaitDurationSec)

	// We will drain the node 1.
	nodeToDrain := 1
	db := c.Conn(ctx, t.L(), nodeToDrain)
	defer db.Close()

	db.SetConnMaxLifetime(30 * time.Second)
	db.SetMaxOpenConns(5)

	var conns []*gosql.Conn

	// Get two connections from the connection pools.
	for j := 0; j < 2; j++ {
		conn, err := db.Conn(ctx)
		if err != nil {
			require.NoError(t, err, "failed to a SQL connection from the connection pool")
		}
		conns = append(conns, conn)
	}

	// Start draining the node.
	m := c.NewMonitor(ctx, c.Range(nodeToDrain, nodeToDrain))

	m.Go(func(ctx context.Context) error {
		t.Status(fmt.Sprintf("start draining node %d", nodeToDrain))
		return c.RunE(ctx,
			c.Node(nodeToDrain),
			fmt.Sprintf("./cockroach node drain --insecure --drain-wait=%ds",
				totalWaitDurationSec))
	})

	drainStartTimestamp := timeutil.Now()

	// Sleep till the server is in the status of waiting for users to close SQL
	// connections. Verify that the server is rejecting new SQL connections now.
	time.Sleep((pokeDuringConnWaitTimestamp) * time.Second)
	_, err = db.Conn(ctx)
	if err != nil {
		t.Status(fmt.Sprintf("%ds after draining starts, server is rejecting "+
			"new SQL connections: %v", pokeDuringConnWaitTimestamp, err))
	} else {
		require.NoError(t, errors.New("new SQL connections should not be allowed when the server "+
			"starts waiting for the user to close SQL connections"))
	}

	if db.Stats().OpenConnections != 2 {
		require.Equalf(t, db.Stats().OpenConnections, 2, "number of open connections should be 2")
	}
	t.Status("number of open connections: ", db.Stats().OpenConnections)

	randConn := conns[rand.Intn(len(conns))]

	// Before the server starts draining SQL connections (i.e. the phase whose
	// duration is set by server.shutdown.query_wait), verify that SQL queries do
	// not fail.

	_, err = randConn.ExecContext(ctx, "SELECT 1;")

	if err != nil {
		require.NoError(t, err, "expected query not to fail before the "+
			"server starts draining SQL connections")
	}

	for _, conn := range conns {
		if err := conn.Close(); err != nil {
			require.NoError(t, err,
				"expected connection to be able to be successfully closed client-side")
		}
	}

	t.Status("all SQL connections are put back to the connection pool")

	if err := m.WaitE(); err != nil {
		require.NoError(t, err, "error waiting for the draining to finish")
	}

	drainEndTimestamp := timeutil.Now()
	actualDrainDuration := drainEndTimestamp.Sub(drainStartTimestamp).Seconds()

	t.L().Printf("the draining lasted %f seconds", actualDrainDuration)

	if actualDrainDuration >= float64(totalWaitDurationSec)-10 {
		require.NoError(t, errors.New("the draining process didn't early exit "+
			"when waiting for server to close all SQL connections"))
	}
}

// runTestWarningForConnWait is to verify if a warning
// ("server.shutdown.connection_wait times out") is logged when there are
// SQL connections unclosed after connection_wait times out.
func runTestWarningForConnWait(ctx context.Context, t test.Test, c cluster.Cluster, nodes int) {

	const (
		// Set the duration of the draining period.
		drainWaitDurationSec      = 0
		connectionWaitDurationSec = 10
		queryWaitDurationSec      = 20
	)

	totalWaitDurationSec := drainWaitDurationSec + connectionWaitDurationSec + queryWaitDurationSec

	prepareCluster(ctx, t, c, nodes, drainWaitDurationSec, connectionWaitDurationSec, queryWaitDurationSec)

	// We will drain the node 1.
	nodeToDrain := 1
	db := c.Conn(ctx, t.L(), nodeToDrain)
	defer db.Close()

	// Get a connection from the connection pool.
	_, err := db.Conn(ctx)
	if err != nil {
		require.NoError(t, err, "cannot get a SQL connection from the connection pool")
	}

	m := c.NewMonitor(ctx, c.Range(nodeToDrain, nodeToDrain))
	m.Go(func(ctx context.Context) error {
		t.Status(fmt.Sprintf("draining node %d", nodeToDrain))
		return c.RunE(ctx,
			c.Node(nodeToDrain),
			fmt.Sprintf("./cockroach node drain --insecure --drain-wait=%ds",
				totalWaitDurationSec))
	})

	if err := m.WaitE(); err != nil {
		require.NoError(t, err, "error waiting for the draining to finish")
	}

	logFile := filepath.Join("logs", "*.log")
	if err := c.RunE(ctx, c.Node(nodeToDrain),
		"grep", "-q", "'server.shutdown.connection_wait times out'", logFile); err != nil {
		require.NoError(t, err, "warning is not logged in the log file")
	}
}

// prepareCluster is to start the server on nodes in the given cluster, and set
// the cluster setting for duration of each phase of the draining process.
func prepareCluster(
	ctx context.Context,
	t test.Test,
	c cluster.Cluster,
	nodes int,
	drainWait int,
	connectionWait int,
	queryWait int,
) {

	c.Put(ctx, t.Cockroach(), "./cockroach", c.All())

	// Start the cockroach server.
	for i := 1; i <= nodes; i++ {
		startOpts := option.DefaultStartOpts()
		startOpts.RoachprodOpts.ExtraArgs = append(startOpts.RoachprodOpts.ExtraArgs, fmt.Sprintf("--attrs=node%d", i))
		c.Start(ctx, t.L(), startOpts, install.MakeClusterSettings(), c.Node(i))
	}

	db := c.Conn(ctx, t.L(), 1)
	defer db.Close()

	waitPhasesSettingStmt := fmt.Sprintf(
		"SET CLUSTER SETTING server.shutdown.drain_wait = '%ds'; "+
			"SET CLUSTER SETTING server.shutdown.connection_wait = '%ds';"+
			"SET CLUSTER SETTING server.shutdown.query_wait = '%ds';",
		drainWait,
		connectionWait,
		queryWait,
	)

	if _, err := db.ExecContext(ctx, waitPhasesSettingStmt); err != nil {
		require.NoError(t, err)
	}
}
