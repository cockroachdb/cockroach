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
	"strings"
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
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
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

	const appName = "TestInsightsIntegration"

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

	_, err := conn.ExecContext(ctx, "SET SESSION application_name=$1", appName)
	require.NoError(t, err)

	// See no recorded insights.
	var count int
	var queryText string
	row := conn.QueryRowContext(ctx, "SELECT count(*), coalesce(string_agg(query, ';'),'') "+
		"FROM crdb_internal.cluster_execution_insights where app_name = $1 ", appName)
	err = row.Scan(&count, &queryText)
	require.NoError(t, err)
	require.Equal(t, 0, count, "expect:0, actual:%d, queries:%s", count, queryText)

	queryDelayInSeconds := latencyThreshold.Seconds()
	// Execute a "long-running" statement, running longer than our latencyThreshold.
	_, err = conn.ExecContext(ctx, "SELECT pg_sleep($1)", queryDelayInSeconds)
	require.NoError(t, err)

	// Eventually see one recorded insight.
	testutils.SucceedsWithin(t, func() error {
		row = conn.QueryRowContext(ctx, "SELECT count(*), coalesce(string_agg(query, ';'),'') "+
			"FROM crdb_internal.cluster_execution_insights where app_name = $1 ", appName)
		if err = row.Scan(&count, &queryText); err != nil {
			return err
		}
		if count != 1 {
			return fmt.Errorf("expected 1, but was %d, queryText:%s", count, queryText)
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
			"full_scan, "+
			"implicit_txn "+
			"FROM crdb_internal.node_execution_insights where "+
			"query = $1 and app_name = $2 ", "SELECT pg_sleep($1)", appName)

		var query, status string
		var startInsights, endInsights time.Time
		var fullScan bool
		var implicitTxn bool
		err = row.Scan(&query, &status, &startInsights, &endInsights, &fullScan, &implicitTxn)

		if err != nil {
			return err
		}

		if status != "Completed" {
			return fmt.Errorf("expected 'Completed', but was %s", status)
		}

		delayFromTable := endInsights.Sub(startInsights).Seconds()
		if delayFromTable < queryDelayInSeconds {
			return fmt.Errorf("expected at least %f, but was %f", delayFromTable, queryDelayInSeconds)
		}

		return nil
	}, 1*time.Second)
}

func TestInsightsPriorityIntegration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const appName = "TestInsightsPriorityIntegration"

	// Start the cluster. (One node is sufficient; the outliers system is currently in-memory only.)
	ctx := context.Background()
	settings := cluster.MakeTestingClusterSettings()
	args := base.TestClusterArgs{ServerArgs: base.TestServerArgs{Settings: settings}}
	tc := testcluster.StartTestCluster(t, 1, args)
	defer tc.Stopper().Stop(ctx)
	conn := tc.ServerConn(0)

	// Enable detection by setting a latencyThreshold > 0.
	latencyThreshold := 50 * time.Millisecond
	insights.LatencyThreshold.Override(ctx, &settings.SV, latencyThreshold)

	_, err := conn.ExecContext(ctx, "SET SESSION application_name=$1", appName)
	require.NoError(t, err)

	_, err = conn.Exec("CREATE TABLE t (id string, s string);")
	require.NoError(t, err)

	// Execute a "long-running" statement, running longer than our latencyThreshold (100ms).
	_, err = conn.ExecContext(ctx, "SELECT pg_sleep(.11)")
	require.NoError(t, err)

	testutils.SucceedsWithin(t, func() error {
		row := conn.QueryRowContext(ctx, "SELECT "+
			"implicit_txn "+
			"FROM crdb_internal.node_execution_insights where "+
			"app_name = $1 and query = $2 ", appName, "SELECT pg_sleep(_)")

		var implicitTxn bool
		err = row.Scan(&implicitTxn)
		if err != nil {
			return err
		}

		if implicitTxn != true {
			return fmt.Errorf("expected implicit_txn '%v', but was %v", true, implicitTxn)
		}

		return nil
	}, 2*time.Second)

	var priorities = []struct {
		setPriorityQuery      string
		query                 string
		queryNoValues         string
		expectedPriorityValue string
	}{
		{
			setPriorityQuery:      "SET TRANSACTION PRIORITY LOW",
			query:                 "INSERT INTO t(id, s) VALUES ('test', 'originalValue')",
			queryNoValues:         "INSERT INTO t(id, s) VALUES ('_', '_')",
			expectedPriorityValue: "low",
		},
		{
			setPriorityQuery:      "SET TRANSACTION PRIORITY NORMAL",
			query:                 "UPDATE t set s = 'updatedValue' where id = 'test'",
			queryNoValues:         "UPDATE t SET s = '_' WHERE id = '_'",
			expectedPriorityValue: "normal",
		},
		{
			setPriorityQuery:      "SELECT 1", // use a dummy query to validate default scenario
			query:                 "UPDATE t set s = 'updatedValue'",
			queryNoValues:         "UPDATE t SET s = '_'",
			expectedPriorityValue: "normal",
		},
		{
			setPriorityQuery:      "SET TRANSACTION PRIORITY HIGH",
			query:                 "DELETE FROM t WHERE t.s = 'originalValue'",
			queryNoValues:         "DELETE FROM t WHERE t.s = '_'",
			expectedPriorityValue: "high",
		},
	}

	for _, p := range priorities {
		testutils.SucceedsWithin(t, func() error {
			tx, errTxn := conn.BeginTx(ctx, &gosql.TxOptions{})
			require.NoError(t, errTxn)

			_, errTxn = tx.ExecContext(ctx, p.setPriorityQuery)
			require.NoError(t, errTxn)

			_, errTxn = tx.ExecContext(ctx, p.query)
			require.NoError(t, errTxn)

			_, errTxn = tx.ExecContext(ctx, "select pg_sleep(.1);")
			require.NoError(t, errTxn)
			errTxn = tx.Commit()
			require.NoError(t, errTxn)
			return nil
		}, 2*time.Second)

		testutils.SucceedsWithin(t, func() error {
			row := conn.QueryRowContext(ctx, "SELECT "+
				"query, "+
				"priority, "+
				"implicit_txn "+
				"FROM crdb_internal.node_execution_insights where "+
				"app_name = $1 and query = $2  ", appName, p.queryNoValues)

			var query, priority string
			var implicitTxn bool
			err = row.Scan(&query, &priority, &implicitTxn)

			if err != nil {
				return err
			}

			if query != p.queryNoValues {
				return fmt.Errorf("expected query '%s', but was %s", p.queryNoValues, query)
			}

			if priority != p.expectedPriorityValue {
				return fmt.Errorf("expected priority '%s', but was %s", p.expectedPriorityValue, priority)
			}

			if implicitTxn != false {
				return fmt.Errorf("expected implicit_txn '%v', but was %v", false, implicitTxn)
			}

			return nil
		}, 2*time.Second)
	}
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

// Testing that the index recommendation is included
// in the insights table
func TestInsightsIndexRecommendationIntegration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderStressRace(t, "expensive tests")

	ctx := context.Background()
	settings := cluster.MakeTestingClusterSettings()
	args := base.TestClusterArgs{ServerArgs: base.TestServerArgs{Settings: settings}}
	tc := testcluster.StartTestCluster(t, 1, args)
	defer tc.Stopper().Stop(ctx)

	// Enable detection by setting a latencyThreshold > 0.
	latencyThreshold := 30 * time.Millisecond
	insights.LatencyThreshold.Override(ctx, &settings.SV, latencyThreshold)

	sqlConn := tc.ServerConn(0)

	_, err := sqlConn.ExecContext(ctx, "CREATE TABLE t1 (k INT, i INT, f FLOAT, s STRING)")
	require.NoError(t, err)
	_, err = sqlConn.ExecContext(ctx, "CREATE TABLE t2 (k INT, i INT, s STRING)")
	require.NoError(t, err)

	query := "SELECT t1.k FROM t1 JOIN t2 ON t1.k = t2.k WHERE t1.i > 3 AND t2.i > 3"

	// Execute enough times to have index recommendations generated.
	// This will generate two recommendations.
	for i := 0; i < 10; i++ {
		tx, err := sqlConn.BeginTx(ctx, &gosql.TxOptions{})
		require.NoError(t, err)
		_, err = tx.ExecContext(ctx, "select pg_sleep(.05);")
		require.NoError(t, err)
		_, err = tx.ExecContext(ctx, query)
		require.NoError(t, err)
		err = tx.Commit()
		require.NoError(t, err)
	}

	// Verify the table content is valid.
	testutils.SucceedsWithin(t, func() error {
		rows, err := sqlConn.QueryContext(ctx, "SELECT "+
			"query, "+
			"array_to_string(index_recommendations, ';') as cmb_index_recommendations "+
			"FROM crdb_internal.node_execution_insights "+
			"where array_length(index_recommendations, 1) > 0 and "+
			"query like 'SELECT t1.k FROM t1 JOIN t2 ON t1.k = t2.k%' ")

		if err != nil {
			return err
		}

		var rowCount int
		for rows.Next() {
			var query string
			var idxRecommendation string
			err := rows.Scan(&query, &idxRecommendation)
			if err != nil {
				return err
			}

			if query != "SELECT t1.k FROM t1 JOIN t2 ON t1.k = t2.k WHERE (t1.i > _) AND (t2.i > _)" {
				return fmt.Errorf("'SELECT t1.k FROM t1 JOIN t2 ON t1.k = t2.k WHERE (t1.i > _) AND (t2.i > _)' should be %s", query)
			}

			if idxRecommendation == "" {
				return fmt.Errorf("index recommendation should not be empty '%s'", idxRecommendation)
			}

			if !strings.Contains(idxRecommendation, "CREATE INDEX") {
				return fmt.Errorf("index recommendation should contain 'CREATE INDEX' actual:'%s'", idxRecommendation)
			}

			rowCount++
		}

		if rowCount < 1 {
			return fmt.Errorf("no rows found with index recommendation")
		}

		return nil
	}, 1*time.Second)
}
