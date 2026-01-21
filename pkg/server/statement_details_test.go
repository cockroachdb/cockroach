// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/persistedsqlstats/sqlstatstestutil"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestStatementDetails(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	testServer, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			SQLStatsKnobs: &sqlstats.TestingKnobs{
				SynchronousSQLStats: true,
			},
		},
	})
	defer testServer.Stopper().Stop(ctx)

	client := testServer.GetStatusClient(t)
	conn := sqlutils.MakeSQLRunner(db)

	conn.Exec(t, "CREATE DATABASE testdb;")
	conn.Exec(t, "USE testdb;")
	conn.Exec(t, "CREATE TABLE samples (id INT PRIMARY KEY, body TEXT);")
	conn.Exec(t, "INSERT INTO samples VALUES (1, 'foo'), (2, 'bar'), (3, 'baz');")

	// generate statistics.
	testQuery := "SELECT * FROM samples"
	for i := 0; i < 5; i++ {
		_, err := db.Exec(testQuery)
		require.NoError(t, err)
	}

	sqlstatstestutil.WaitForStatementEntriesAtLeast(t, conn, 1,
		sqlstatstestutil.StatementFilter{Query: testQuery})

	stmts, err := client.Statements(ctx, &serverpb.StatementsRequest{})
	require.NoError(t, err)

	var fingerprintID string
	for _, stmt := range stmts.Statements {
		if stmt.Key.KeyData.Query == testQuery {
			fingerprintID = stmt.ID.String()
			break
		}
	}
	require.NotEmpty(t, fingerprintID, "fingerprint ID not found for test query")

	resp, err := client.StatementDetails(ctx, &serverpb.StatementDetailsRequest{
		FingerprintId: fingerprintID,
	})
	require.NoError(t, err)
	require.NotNil(t, resp)

	require.NotNil(t, resp.Statement)
	require.NotEmpty(t, resp.Statement.Metadata.Query)
	require.NotEmpty(t, resp.Statement.Metadata.FingerprintID)
	require.Greater(t, resp.Statement.Stats.Count, int64(0))

	require.NotEmpty(t, resp.StatementStatisticsPerAggregatedTs,
		"should have statistics grouped by aggregated timestamp")
	for _, stat := range resp.StatementStatisticsPerAggregatedTs {
		require.NotZero(t, stat.AggregatedTs)
		require.Greater(t, stat.Stats.Count, int64(0))
	}

	require.NotEmpty(t, resp.StatementStatisticsPerPlanHash,
		"should have statistics grouped by plan hash")
	for _, stat := range resp.StatementStatisticsPerPlanHash {
		require.NotZero(t, stat.PlanHash)
		require.Greater(t, stat.Stats.Count, int64(0))
		require.Greater(t, stat.Metadata.TotalCount, int64(0))
	}

	require.NotEmpty(t, resp.StatementStatisticsPerAggregatedTsAndPlanHash,
		"should have plan distribution over time")
	for _, stat := range resp.StatementStatisticsPerAggregatedTsAndPlanHash {
		require.NotZero(t, stat.AggregatedTs)
		require.NotZero(t, stat.PlanHash)
		require.Greater(t, stat.ExecutionCount, int64(0))
	}
}
