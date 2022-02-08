package tests

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math/rand"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/errors"
	"golang.org/x/sync/errgroup"
)

func registerDraining(r registry.Registry) {
	{
		numNodes := 2
		duration := time.Hour

		r.Add(registry.TestSpec{
			Name:    fmt.Sprintf("sqlexp/draining-early-exit-conn-wait"),
			Owner:   registry.OwnerSQLExperience,
			Cluster: r.MakeClusterSpec(numNodes),
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				testEarlyExitInConnectionWait(ctx, t, c, numNodes, duration)
			},
		})

		r.Add(registry.TestSpec{
			Name:    fmt.Sprintf("sqlexp/draining-early-exit-query-wait"),
			Owner:   registry.OwnerSQLExperience,
			Cluster: r.MakeClusterSpec(numNodes),
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				testEarlyExitInQueryWait(ctx, t, c, numNodes, duration)
			},
		})
	}
}

// testEarlyExitInConnectionWait test if the server early exit the current
// draining phase if all connections are closed by user before the
// duration set by `server.shutdown.connection_wait` times out.
func testEarlyExitInConnectionWait(
	ctx context.Context, t test.Test, c cluster.Cluster, nodes int, duration time.Duration,
) {

	var err error

	// Set the duration of each phase of the draining period.
	drainWaitDurationSec := 20
	connectionWaitDurationSec := 200
	queryWaitDurationSec := 20
	totalWaitDurationSec := drainWaitDurationSec + connectionWaitDurationSec + queryWaitDurationSec

	const (
		// pokeHealthBeforeConnDrainTimestamp is the timestamp before the server
		// starts waiting SQL connections to close (with the start of the whole
		// draining process marked as timestamp 0). It should be set smaller than
		// drainWaitDurationSec.
		pokeHealthBeforeConnDrainTimestamp = 10
		// pokeHealthAfterConnDrainTimestamp is the timestamp after the server
		// starts waiting SQL connections to close (with the start of the whole
		// draining process marked as timestamp 0). It should be set larger than
		// drainWaitDurationSec, but smaller than totalWaitDurationSec.
		pokeHealthAfterConnDrainTimestamp = 25
	)

	prepareCluster(ctx, t, c, nodes, drainWaitDurationSec, connectionWaitDurationSec, queryWaitDurationSec)

	// We will drain the node 1.
	nodeToDrain := 1
	db := c.Conn(ctx, t.L(), nodeToDrain)
	defer func() {
		_ = db.Close()
	}()

	db.SetConnMaxLifetime(30 * time.Second)
	db.SetMaxOpenConns(5)

	var conns []*sql.Conn

	// Get two connections from the connection pools.
	for j := 0; j < 2; j++ {
		conn, err := db.Conn(ctx)
		if err != nil {
			t.Fatal(errors.Wrap(err, "cannot get a SQL connection from the connection pool"))
		}
		conns = append(conns, conn)
	}

	adminAddrs, err := c.InternalAdminUIAddr(ctx, t.L(), c.Node(nodeToDrain))
	if err != nil {
		t.Fatal(errors.Wrapf(err, "cannot get the admin address of node %d", nodeToDrain))
	}

	var result install.RunResultDetails

	// Check if the node is ready for new sql client.
	result, err = c.RunWithDetailsSingleNode(ctx, t.L(), c.Node(nodeToDrain),
		"curl", fmt.Sprintf("http://%s/health\\?ready\\=1",
			adminAddrs[0]))

	if err != nil {
		t.L().Printf("cannot get the readiness of the node before drain starts")
	}

	var healthEndpointRes map[string]interface{}
	if err := json.Unmarshal([]byte(result.Stdout), &healthEndpointRes); err != nil {
		t.L().Printf(fmt.Sprintf("cannot unmarshal stdout to json: %v", err))
	}

	if _, ok := healthEndpointRes["error"]; ok {
		t.Fatal("node should be ready to receive new SQL connections " +
			"before draining starts")
	}

	// Start draining the node.
	var m *errgroup.Group
	m, ctx = errgroup.WithContext(ctx)

	m.Go(func() error {
		t.Status(fmt.Sprintf("start draining node %d", nodeToDrain))
		return c.RunE(ctx,
			c.Node(nodeToDrain),
			fmt.Sprintf("./cockroach node drain --insecure --drain-wait=%ds",
				totalWaitDurationSec))
	})

	drainStartTimestamp := time.Now()

	// Make sure we're in the middle of the phase set by `server.shutdown.drain_wait`.
	// At this stage, the /health\?ready=1 endpoint should return error message
	// "node is shutting down", but the node still accept new SQL connections.
	// pokeHealthTimestamp should be set shorter than drainWaitDurationSec.

	time.Sleep(pokeHealthBeforeConnDrainTimestamp * time.Second)

	result, err = c.RunWithDetailsSingleNode(ctx, t.L(), c.Node(nodeToDrain),
		"curl", fmt.Sprintf("http://%s/health\\?ready\\=1",
			adminAddrs[0]))

	if err != nil {
		t.L().Printf("cannot get the readiness of the node after drain starts")
	}

	if err := json.Unmarshal([]byte(result.Stdout), &healthEndpointRes); err != nil {
		t.L().Printf(fmt.Sprintf("cannot unmarshal stdout to json:%v", err))
	}

	errorMessage, ok := healthEndpointRes["error"]
	if !ok {
		t.Fatal("node should be in shutting-down mode now")
	}

	t.Status(
		"readiness endpoint's stdout after draining starts: %s",
		errorMessage,
	)

	// Before we start waiting client to close SQL connections, new SQL connections
	// are always allowed, even though the node is unready for new clients.
	var newConn *sql.Conn
	newConn, err = db.Conn(ctx)

	if err != nil {
		t.Fatal(errors.Wrapf(err, "%ds after drain starts (at drain_wait), "+
			"cannot grab new SQL conn from the pool", pokeHealthBeforeConnDrainTimestamp))
	}
	conns = append(conns, newConn)

	// Sleep till the server is in the status of waiting users to close SQL
	// connections. No new SQL connections are allowed now.
	time.Sleep((pokeHealthAfterConnDrainTimestamp - pokeHealthBeforeConnDrainTimestamp) * time.Second)
	newConn, err = db.Conn(ctx)
	if err != nil {
		t.Status("%ds after draining starts, cannot make new SQL "+
			"connection from the pool: %v", pokeHealthAfterConnDrainTimestamp, err)
	} else {
		t.Fatal("new SQL connections should not be allowed when the server " +
			"starts waiting the user to close SQL connections.")
	}

	if db.Stats().OpenConnections != 3 {
		t.Fatal("number of open connections should be 3 at connection_wait.")
	}
	t.Status("number of open connections: %d", db.Stats().OpenConnections)

	randConn := conns[rand.Intn(len(conns))]

	// Before the server starts draining SQL connections (enter the phase whose
	// duration is set by server.shutdown.query_wait), new SQL queries are always
	// allowed.
	var queryRes string
	row := randConn.QueryRowContext(ctx, "SELECT 1;")
	if row.Err() != nil {
		t.Fatal("cannot run query before the server draining SQL connections")
	}
	if err := row.Scan(&queryRes); err != nil {
		t.Fatal("cannot scan the result from query")
	}

	t.L().Printf("query succeed: %s", queryRes)

	for _, conn := range conns {
		if err := conn.Close(); err != nil {
			t.Fatal(errors.Wrap(err, "cannot close connection when the server "+
				"waits users to close connections"))
		}
	}

	t.Status("all SQL connections are put back to the connection pool")

	if err := m.Wait(); err != nil {
		t.Fatal(errors.Wrap(err, "error waiting for the draining to finish"))
	}

	drainEndTimestamp := time.Now()
	actualDrainDuration := drainEndTimestamp.Sub(drainStartTimestamp).Seconds()

	t.L().Printf("the duration lasted %f seconds", actualDrainDuration)

	if actualDrainDuration >= float64(totalWaitDurationSec)-10 {
		t.Fatal("the draining process didn't early exit when waiting all SQL " +
			"connections to close")
	}
}

// testEarlyExitInQueryWait test if the server early exit the current
// draining phase if all SQL queries are finished before the
// duration set by `server.shutdown.query_wait` times out.
func testEarlyExitInQueryWait(
	ctx context.Context, t test.Test, c cluster.Cluster, nodes int, duration time.Duration,
) {
	// Set the duration of the draining period.
	drainWaitDurationSec := 10
	connectionWaitDurationSec := 10
	queryWaitDurationSec := 200
	totalWaitDurationSec := drainWaitDurationSec + connectionWaitDurationSec + queryWaitDurationSec

	const (
		// longQueryDuration is the duration of a SQL transaction. It should be
		// set longer than (drainWaitDurationSec + connectionWaitDurationSec), but
		// smaller than (drainWaitDurationSec + connectionWaitDurationSec +
		// queryWaitDurationSec).
		longQueryDuration = 60
	)

	prepareCluster(ctx, t, c, nodes, drainWaitDurationSec, connectionWaitDurationSec, queryWaitDurationSec)

	// We will drain the node 1.
	nodeToDrain := 1
	db := c.Conn(ctx, t.L(), nodeToDrain)
	defer func() {
		_ = db.Close()
	}()

	db.SetConnMaxLifetime(30 * time.Second)
	db.SetMaxOpenConns(5)

	var conns []*sql.Conn

	// Get 3 connections from the connection pools.
	for j := 0; j < 3; j++ {
		conn, err := db.Conn(ctx)
		if err != nil {
			t.Fatal(errors.Wrap(err, "cannot get a SQL connection from the connection pool"))
		}
		conns = append(conns, conn)
	}

	conn := conns[rand.Intn(len(conns))]

	var longQuery *errgroup.Group
	longQuery, ctx = errgroup.WithContext(ctx)
	longQuery.Go(func() error {
		if _, err := conn.ExecContext(
			ctx,
			fmt.Sprintf(
				"BEGIN; SELECT pg_sleep(%d); COMMIT;",
				longQueryDuration,
			),
		); err != nil {
			return err
		}
		return nil
	})

	var drainNode *errgroup.Group
	drainNode, ctx = errgroup.WithContext(ctx)
	drainNode.Go(func() error {
		t.Status(fmt.Sprintf("draining node %d", nodeToDrain))
		return c.RunE(ctx,
			c.Node(nodeToDrain),
			fmt.Sprintf("./cockroach node drain --insecure --drain-wait=%ds",
				totalWaitDurationSec))
	})

	drainStartTimestamp := time.Now()

	// TODO(janexing): Add test for the number of active connections before and
	// and after the beginning of server draining SQL connection. Note that
	// in the "after" case, db.Stats().OpenConnections does not give the actual
	// number of alive SQL connections.

	if err := drainNode.Wait(); err != nil {
		t.Fatal(errors.Wrap(err, "error waiting for the draining to finish"))
	}

	drainEndTimestamp := time.Now()
	actualDrainDuration := drainEndTimestamp.Sub(drainStartTimestamp).Seconds()

	t.L().Printf("the duration lasted %f seconds", actualDrainDuration)

	if actualDrainDuration >= float64(totalWaitDurationSec)-10 {
		t.Fatal("the draining process didn't early exit when waiting all SQL " +
			"queries to finish")
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
	defer func() {
		_ = db.Close()
	}()

	waitPhasesSettingStmt := fmt.Sprintf(
		"SET CLUSTER SETTING server.shutdown.drain_wait = '%ds'; "+
			"SET CLUSTER SETTING server.shutdown.connection_wait = '%ds';"+
			"SET CLUSTER SETTING server.shutdown.query_wait = '%ds';",
		drainWait,
		connectionWait,
		queryWait,
	)

	if _, err := db.ExecContext(ctx, waitPhasesSettingStmt); err != nil {
		t.Fatal(err)
	}
}
