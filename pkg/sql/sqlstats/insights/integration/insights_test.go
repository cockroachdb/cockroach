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
	"fmt"
	"os"
	"strings"
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
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
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

// Testing that the index recommendation is included
// in the insights table
func TestInsightsIndexRecommendationIntegration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderStressRace(t, "expensive tests")

	// Start the cluster. (One node is sufficient; the outliers system is currently in-memory only.)
	ctx := context.Background()
	settings := cluster.MakeTestingClusterSettings()
	args := base.TestClusterArgs{ServerArgs: base.TestServerArgs{Settings: settings}}
	tc := testcluster.StartTestCluster(t, 1, args)
	defer tc.Stopper().Stop(ctx)

	// Enable detection by setting a latencyThreshold > 0.
	latencyThreshold := 1 * time.Nanosecond
	insights.LatencyThreshold.Override(ctx, &settings.SV, latencyThreshold)

	sqlConn := sqlutils.MakeSQLRunner(tc.ServerConn(0 /* idx */))

	sqlConn.Exec(t, "CREATE TABLE t1 (k INT, i INT, f FLOAT, s STRING)")
	sqlConn.Exec(t, "CREATE TABLE t2 (k INT, i INT, s STRING)")

	query := "SELECT t1.k FROM t1 JOIN t2 ON t1.k = t2.k WHERE t1.i > 3 AND t2.i > 3"

	// Execute enough times to have index recommendations generated.
	// This will generate two recommendations.
	for i := 0; i < 7; i++ {
		sqlConn.Exec(t, query)
	}

	// Verify the table content is valid.
	testutils.SucceedsWithin(t, func() error {
		row := sqlConn.Query(t, "SELECT "+
			"query, "+
			"array_to_string(index_recommendations, ';') as cmb_index_recommendations "+
			"FROM crdb_internal.node_execution_insights "+
			"where array_length(index_recommendations, 1) > 0 and "+
			"query like 'SELECT t1.k FROM t1 JOIN t2 ON t1.k = t2.k%' ")

		var rowCount int
		for row.Next() {
			var query string
			var idxRecommendation string
			err := row.Scan(&query, &idxRecommendation)
			require.NoError(t, err)

			require.Equal(t, "SELECT t1.k FROM t1 JOIN t2 ON t1.k = t2.k WHERE (t1.i > _) AND (t2.i > _)", query)
			require.NotEmpty(t, idxRecommendation)
			require.True(t, strings.Contains(idxRecommendation, "CREATE INDEX"), idxRecommendation)
			rowCount++
		}

		require.GreaterOrEqual(t, rowCount, 1)

		return nil
	}, 1*time.Second)
}
