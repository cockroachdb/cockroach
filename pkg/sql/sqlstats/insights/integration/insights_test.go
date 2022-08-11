// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package integration

import (
	"context"
	gosql "database/sql"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security/securityassets"
	"github.com/cockroachdb/cockroach/pkg/security/securitytest"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/insights"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	securityassets.SetLoader(securitytest.EmbeddedAssets)
	serverutils.InitTestServerFactory(server.TestServerFactory)
	os.Exit(m.Run())
}

func TestInsightsIntegration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Start the cluster. (One node is sufficient; the outliers system is currently in-memory only.)
	ctx := context.Background()
	settings := cluster.MakeTestingClusterSettings()
	args := base.TestClusterArgs{ServerArgs: base.TestServerArgs{Settings: settings}}
	tc := testcluster.StartTestCluster(t, 1, args)
	defer tc.Stopper().Stop(ctx)
	conn := tc.ServerConn(0)

	// Enable detection by setting a latencyThreshold > 0.
	latencyThreshold := 250 * time.Millisecond
	insights.LatencyThreshold.Override(ctx, &settings.SV, latencyThreshold)

	// See no recorded insights.
	var count int
	row := conn.QueryRowContext(ctx, "SELECT count(*) FROM crdb_internal.cluster_execution_insights")
	err := row.Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 0, count)

	queryDelayInSeconds := 2 * latencyThreshold.Seconds()
	// Execute a "long-running" statement, running longer than our latencyThreshold.
	_, err = conn.ExecContext(ctx, "SELECT pg_sleep($1)", queryDelayInSeconds)
	require.NoError(t, err)

	// Eventually see one recorded insight.
	testutils.SucceedsWithin(t, func() error {
		row = conn.QueryRowContext(ctx, "SELECT count(*) FROM crdb_internal.cluster_execution_insights")
		if err = row.Scan(&count); err != nil {
			return err
		}
		if count != 1 {
			return fmt.Errorf("expected 1, but was %d", count)
		}
		return nil
	}, 1*time.Second)

	// Verify the table content is valid.
	testutils.SucceedsWithin(t, func() error {
		row = conn.QueryRowContext(ctx, "SELECT "+
			"query, "+
			"status, "+
			"start_time, "+
			"end_time, "+
			"full_scan "+
			"FROM crdb_internal.node_execution_insights")

		var query, status string
		var startInsights, endInsights time.Time
		var fullScan bool
		err = row.Scan(&query, &status, &startInsights, &endInsights, &fullScan)

		require.NoError(t, err)
		require.Equal(t, "SELECT pg_sleep($1)", query)
		require.Equal(t, "completed", status)
		require.GreaterOrEqual(t, endInsights.Sub(startInsights).Seconds(), queryDelayInSeconds)

		return nil
	}, 1*time.Second)
}

func TestInsightsIntegrationForContention(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Start the cluster. (One node is sufficient; the outliers system is currently in-memory only.)
	ctx := context.Background()
	settings := cluster.MakeTestingClusterSettings()
	args := base.TestClusterArgs{ServerArgs: base.TestServerArgs{Settings: settings}}
	tc := testcluster.StartTestCluster(t, 1, args)
	defer tc.Stopper().Stop(ctx)
	conn := tc.ServerConn(0)

	_, err := conn.Exec("SET tracing = true;")
	require.NoError(t, err)
	_, err = conn.Exec("SET cluster setting sql.txn_stats.sample_rate  = 1;")
	require.NoError(t, err)
	_, err = conn.Exec("CREATE TABLE t (id string, s string);")
	require.NoError(t, err)

	// Enable detection by setting a latencyThreshold > 0.
	latencyThreshold := 100 * time.Millisecond
	insights.LatencyThreshold.Override(ctx, &settings.SV, latencyThreshold)

	// Create a new connection, and then in a go routine have it start a transaction, update a row,
	// sleep for a time, and then complete the transaction.
	// With original connection attempt to update the same row being updated concurrently
	// in the separate go routine, this will be blocked until the original transaction completes.
	var wgTxnStarted sync.WaitGroup
	wgTxnStarted.Add(1)

	// Lock to wait for the txn to complete to avoid the test finishing before the txn is committed
	var wgTxnDone sync.WaitGroup
	wgTxnDone.Add(1)

	go func() {
		tx, errTxn := conn.BeginTx(ctx, &gosql.TxOptions{})
		require.NoError(t, errTxn)
		_, errTxn = tx.ExecContext(ctx, "INSERT INTO t (id, s) VALUES ('test', 'originalValue');")
		require.NoError(t, errTxn)
		wgTxnStarted.Done()
		_, errTxn = tx.ExecContext(ctx, "select pg_sleep(.5);")
		require.NoError(t, errTxn)
		errTxn = tx.Commit()
		require.NoError(t, errTxn)
		wgTxnDone.Done()
	}()

	start := timeutil.Now()

	// Need to wait for the txn to start to ensure lock contention
	wgTxnStarted.Wait()
	// This will be blocked until the updateRowWithDelay finishes.
	_, err = conn.ExecContext(ctx, "UPDATE t SET s = 'mainThread' where id = 'test';")
	require.NoError(t, err)
	end := timeutil.Now()
	require.GreaterOrEqual(t, end.Sub(start), 500*time.Millisecond)

	wgTxnDone.Wait()

	// Verify the table content is valid.
	testutils.SucceedsWithin(t, func() error {
		rows, err := conn.QueryContext(ctx, "SELECT "+
			"query, "+
			"contention::FLOAT "+
			"FROM crdb_internal.node_execution_insights where query like 'UPDATE t SET s =%'")
		if err != nil {
			return err
		}

		rowCount := 0
		for rows.Next() {
			rowCount++
			if err != nil {
				return err
			}

			var contentionFromQuery float64
			var queryText string
			err = rows.Scan(&queryText, &contentionFromQuery)
			if err != nil {
				return err
			}

			if contentionFromQuery < .2 {
				return fmt.Errorf("contention time is %f should be greater than .2 since block is delayed by .5 seconds", contentionFromQuery)
			}
		}

		if rowCount < 1 {
			return fmt.Errorf("node_execution_insights did not return any rows")
		}

		return nil
	}, 1*time.Second)
}
