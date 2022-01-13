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
	tableName          string
	runWithExplicitTxn bool
}

var earliestAggTsTestCases = []earliestAggTsTestCase{
	{
		// Fixme how do I indicate which function to use?
		//functionToTest:  GetEarliestStatementAggregatedTs,
		tableName:          "system.statement_statistics",
		runWithExplicitTxn: false,
	},
	{
		// Fixme how do I indicate which function to use?
		//functionToTest:  GetEarliestTransactionAggregatedTs,
		tableName:          "system.transaction_statistics",
		runWithExplicitTxn: true,
	},
}

func TestScanEarliestAggregatedTs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	baseTime := timeutil.Now()
	aggInterval := time.Hour
	// Chosen to ensure a different truncated aggTs.
	advancementInterval := time.Hour * 2

	fakeTime := stubTime{
		aggInterval: aggInterval,
	}
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

	for _, tc := range earliestAggTsTestCases {
		sqlConn.Exec(t, "SELECT crdb_internal.reset_sql_stats()")

		fakeTime.setTime(baseTime)

		truncatedBaseTime := baseTime.Truncate(aggInterval)

		t.Run(fmt.Sprintf("%s empty table", tc.tableName), func(t *testing.T) {
			/*
			   Generate un-flushed stats with a distribution of hash shard values.
			   There should also be un-flushed stats from internal queries.
			*/
			runStatements(t, sqlConn, systemschema.SQLStatsHashShardBucketCount*2, tc.runWithExplicitTxn)

			verifyTableEmpty(t, sqlConn, tc.tableName)

			earliestAggTs, err := sqlStats.GetEarliestStatementAggregatedTs(
				ctx,
			)
			if err != nil {
				t.Fatal(err)
			}
			require.Equal(t, truncatedBaseTime, earliestAggTs)
		})

		t.Run(fmt.Sprintf("%s populated table", tc.tableName), func(t *testing.T) {
			// Flush the stats at baseTime.
			sqlStats.Flush(ctx)

			/*
				The implementation of the tested function finds the earliest aggregated ts
				on per-shard basis.
				We want to test that the function does check all shards. One proxy for this
				is to set up our test such that the expected earliest aggregated ts is not
				found for all shard values.
				Thus, run and flush one single statement/transaction at the earliest time.
			*/
			beforeBaseTime := baseTime.Add(-advancementInterval)
			fakeTime.setTime(beforeBaseTime)
			truncatedBeforeBaseTime := beforeBaseTime.Truncate(aggInterval)
			runStatements(t, sqlConn, 1, tc.runWithExplicitTxn)
			sqlStats.Flush(ctx)

			// Run and flush stats at a later time.
			afterBaseTime := baseTime.Add(advancementInterval)
			fakeTime.setTime(afterBaseTime)
			runStatements(t, sqlConn, systemschema.SQLStatsHashShardBucketCount*2, tc.runWithExplicitTxn)
			sqlStats.Flush(ctx)

			verifyDistinctAggregatedTs(
				t, sqlConn, tc.tableName, []time.Time{truncatedBeforeBaseTime, truncatedBaseTime, afterBaseTime.Truncate(aggInterval)},
			)
			verifyNotAllShardValuesHaveExpectedEarliestAggTs(t, sqlConn, tc.tableName, truncatedBeforeBaseTime)

			earliestAggTs, err := sqlStats.GetEarliestStatementAggregatedTs(
				ctx,
			)
			if err != nil {
				t.Fatal(err)
			}
			require.Equal(t, truncatedBeforeBaseTime, earliestAggTs)
		})
	}
	t.Fatal("Need to implement testing both functions, see fixme")

}

func runStatements(
	t *testing.T, sqlConn *sqlutils.SQLRunner, numStatements int, withExplicitTxn bool,
) {
	for i := 1; i < numStatements+1; i++ {
		/*
			Generate statements with unique fingerprints,
			so that they have different hash shard values
		*/
		var ones []string
		for j := 0; j < i; j++ {
			ones = append(ones, "1")
		}
		stmt := fmt.Sprintf("SELECT %[1]s", strings.Join(ones, ", "))
		if withExplicitTxn {
			stmt = fmt.Sprintf("BEGIN; %[1]s; COMMIT;", stmt)
		}
		sqlConn.Exec(t, stmt)
	}
}

func verifyTableEmpty(t *testing.T, sqlConn *sqlutils.SQLRunner, tableName string) {
	stmt := fmt.Sprintf(`SELECT count(*) FROM %[1]s`, tableName)
	sqlConn.CheckQueryResults(
		t,
		stmt,
		[][]string{{`0`}},
	)
}

func verifyDistinctAggregatedTs(
	t *testing.T,
	sqlConn *sqlutils.SQLRunner,
	tableName string,
	sortedExpectedAggregatedTs []time.Time,
) {
	var sortedExpectedAggregatedTsStrings [][]string
	for _, aggregatedTs := range sortedExpectedAggregatedTs {
		sortedExpectedAggregatedTsStrings = append(sortedExpectedAggregatedTsStrings,
			[]string{aggregatedTs.String()})
	}

	// [1]: table name
	stmt := fmt.Sprintf(`SELECT DISTINCT aggregated_ts FROM %[1]s ORDER BY aggregated_ts`,
		tableName,
	)
	sqlConn.CheckQueryResults(
		t,
		stmt,
		sortedExpectedAggregatedTsStrings,
	)
}

func verifyNotAllShardValuesHaveExpectedEarliestAggTs(
	t *testing.T, sqlConn *sqlutils.SQLRunner, tableName string, expectedEarliestAggTs time.Time,
) {
	/*
		In our setup, we only run one statement/transaction at the earliest time.
		This sets up our test such that the expected earliest aggregated ts is not
		found for all shard values.
		However, due to statistics generated by internal queries there is a risk
		generating additional statistics. This function runs a stricter test to
		verify that this expectation still holds.
	*/
	stmt := fmt.Sprintf(`SELECT count(*) FROM %[1]s WHERE aggregated_ts = $1::TIMESTAMP`,
		tableName,
	)
	row := sqlConn.QueryRow(t, stmt, expectedEarliestAggTs)
	var count int
	row.Scan(&count)
	require.Less(t, count, systemschema.SQLStatsHashShardBucketCount)
}
