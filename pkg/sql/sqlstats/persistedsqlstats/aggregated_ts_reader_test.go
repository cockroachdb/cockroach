// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package persistedsqlstats_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/persistedsqlstats"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

type earliestAggTsTestCase struct {
	tableName  string
	runWithTxn bool
}

var earliestAggTsTestCases = []earliestAggTsTestCase{
	{
		tableName:  "system.statement_statistics",
		runWithTxn: false,
	},
	{
		tableName:  "system.transaction_statistics",
		runWithTxn: true,
	},
}

func TestScanEarliestAggregatedTs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	for _, tc := range earliestAggTsTestCases {
		baseTime := timeutil.Now()
		aggInterval := time.Hour
		// chosen to ensure a different truncated aggTs
		advancementInterval := time.Hour * 2
		fakeTime := stubTime{
			aggInterval: aggInterval,
		}
		fakeTime.setTime(baseTime)

		s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
			Knobs: base.TestingKnobs{
				SQLStatsKnobs: &sqlstats.TestingKnobs{
					StubTimeNow: fakeTime.Now,
				},
			},
		})
		ctx := context.Background()
		defer s.Stopper().Stop(ctx)

		sqlStats := s.SQLServer().(*sql.Server).GetSQLStatsProvider().(*persistedsqlstats.PersistedSQLStats)
		sqlConn := sqlutils.MakeSQLRunner(db)

		truncatedBaseTime := baseTime.Truncate(aggInterval)

		t.Run(fmt.Sprintf("%s empty table", tc.tableName), func(t *testing.T) {
			// generate un-flushed stats with a distribution of hash shard values
			// there should also be un-flushed stats from internal queries
			runStatements(t, sqlConn, systemschema.SQLStatsHashShardBucketCount*2, tc.runWithTxn)

			verifyTableEmpty(t, sqlConn, tc.tableName)

			earliestAggTs, err := sqlStats.ScanEarliestAggregatedTs(
				ctx, s.InternalExecutor().(*sql.InternalExecutor), tc.tableName,
			)
			if err != nil {
				t.Fatal(err)
			}
			require.Equal(t, truncatedBaseTime, earliestAggTs)
		})

		t.Run(fmt.Sprintf("%s populated table", tc.tableName), func(t *testing.T) {
			// flush the stats at baseTime
			sqlStats.Flush(ctx)

			// run and flush one single statement/transaction at the earliest time
			beforeBaseTime := baseTime.Add(-advancementInterval)
			fakeTime.setTime(beforeBaseTime)
			truncatedBeforeBaseTime := beforeBaseTime.Truncate(aggInterval)
			runStatements(t, sqlConn, 1, tc.runWithTxn)
			sqlStats.Flush(ctx)

			// run and flush stats at a later time
			afterBaseTime := baseTime.Add(advancementInterval)
			fakeTime.setTime(afterBaseTime)
			runStatements(t, sqlConn, systemschema.SQLStatsHashShardBucketCount*2, tc.runWithTxn)
			sqlStats.Flush(ctx)

			verifyDistinctAggregatedTs(
				t, sqlConn, tc.tableName, []time.Time{truncatedBeforeBaseTime, truncatedBaseTime, afterBaseTime.Truncate(aggInterval)},
			)
			verifyExpectedEntriesForEarliestTime(t, sqlConn, tc.tableName, truncatedBeforeBaseTime)

			earliestAggTs, err := sqlStats.ScanEarliestAggregatedTs(
				ctx, s.InternalExecutor().(*sql.InternalExecutor), tc.tableName,
			)
			if err != nil {
				t.Fatal(err)
			}
			require.Equal(t, truncatedBeforeBaseTime, earliestAggTs)
		})
	}

}

func runStatements(t *testing.T, sqlConn *sqlutils.SQLRunner, numStatements int, withTxn bool) {
	for i := 1; i < numStatements+1; i++ {
		// generate statements with unique fingerprints,
		// so that they have different hash shard values
		ones := make([]string, i)
		for j := 0; j < len(ones); j++ {
			ones[j] = "1"
		}
		stmt := fmt.Sprintf("SELECT %[1]s", strings.Join(ones, ", "))
		if withTxn {
			stmt = fmt.Sprintf("BEGIN; %[1]s; COMMIT;", stmt)
		}
		sqlConn.Exec(t, stmt)
	}
}

func verifyTableEmpty(t *testing.T, sqlConn *sqlutils.SQLRunner, tableName string) {
	stmt := fmt.Sprintf(`SELECT count(*) FROM %[1]s`, tableName)
	row := sqlConn.QueryRow(t, stmt)
	var count int
	row.Scan(&count)
	require.Equal(t, count, 0)
}

func verifyDistinctAggregatedTs(
	t *testing.T,
	sqlConn *sqlutils.SQLRunner,
	tableName string,
	sortedExpectedAggregatedTsValues []time.Time,
) {
	// [1]: table name
	stmt := fmt.Sprintf(`SELECT DISTINCT aggregated_ts FROM %[1]s ORDER BY aggregated_ts`,
		tableName,
	)
	rows := sqlConn.Query(t, stmt)
	defer rows.Close()
	aggregatedTsValues := []time.Time{}

	for rows.Next() {
		var aggregatedTs time.Time
		if err := rows.Scan(&aggregatedTs); err != nil {
			t.Fatal(err)
		}
		aggregatedTsValues = append(aggregatedTsValues, aggregatedTs)

	}

	require.Equal(t, sortedExpectedAggregatedTsValues, aggregatedTsValues)

}

func verifyExpectedEntriesForEarliestTime(
	t *testing.T, sqlConn *sqlutils.SQLRunner, tableName string, expectedEarliestAggTs time.Time,
) {
	// This exists to verify that the earliest time is not found across all hash
	// shard values, thus verifying that the implementation checks across hash
	// shard values.
	var expectedCount int

	switch tableName {
	case "system.statement_statistics":
		// one from the query itself, one from inserting statistics
		expectedCount = 2
	case "system.transaction_statistics":
		expectedCount = 1
	default:
		t.Errorf("Unexpected table name %s", tableName)
	}
	require.Less(t, expectedCount, systemschema.SQLStatsHashShardBucketCount)

	stmt := fmt.Sprintf(`SELECT count(*) FROM %[1]s WHERE aggregated_ts = $1::TIMESTAMP`,
		tableName,
	)
	row := sqlConn.QueryRow(t, stmt, expectedEarliestAggTs)
	var count int
	row.Scan(&count)
	require.Equal(t, expectedCount, count)
}
