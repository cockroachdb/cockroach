// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sslocal

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/persistedsqlstats/sqlstatsutil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestStmtStatsBulkIngestWithRandomMetadata generates a sequence of random
// serverpb.StatementsResponse_CollectedStatementStatistics that simulates the
// response from RPC fanout, and use a temporary SQLStats object to ingest
// that sequence. This test checks if the metadata are being properly
// updated in the temporary SQLStats object.
func TestStmtStatsBulkIngestWithRandomMetadata(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var testData []serverpb.StatementsResponse_CollectedStatementStatistics

	for i := 0; i < 50; i++ {
		var stats serverpb.StatementsResponse_CollectedStatementStatistics
		randomData := sqlstatsutil.GetRandomizedCollectedStatementStatisticsForTest(t)
		stats.Key.KeyData = randomData.Key
		testData = append(testData, stats)
	}

	sqlStats, err := NewTempSQLStatsFromExistingStmtStats(testData)
	require.NoError(t, err)

	require.NoError(t,
		sqlStats.IterateStatementStats(
			context.Background(),
			&sqlstats.IteratorOptions{},
			func(
				ctx context.Context,
				statistics *roachpb.CollectedStatementStatistics,
			) error {
				var found bool
				for i := range testData {
					if testData[i].Key.KeyData == statistics.Key {
						found = true
						break
					}
				}
				require.True(t, found, "expected metadata %+v, but not found", statistics.Key)
				return nil
			}))

}

func TestSQLStatsStmtStatsBulkIngest(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testData := []struct {
		id    roachpb.StmtFingerprintID
		key   roachpb.StatementStatisticsKey
		stats roachpb.StatementStatistics
	}{
		{
			id: 0,
			key: roachpb.StatementStatisticsKey{
				App:      "app1",
				Query:    "SELECT 1",
				Database: "testdb",
			},
			stats: roachpb.StatementStatistics{
				Count: 7,
			},
		},
		{
			id: 0,
			key: roachpb.StatementStatisticsKey{
				App:      "app0",
				Query:    "SELECT 1",
				Database: "testdb",
			},
			stats: roachpb.StatementStatistics{
				Count: 2,
			},
		},
		{
			id: 1,
			key: roachpb.StatementStatisticsKey{
				App:      "app100",
				Query:    "SELECT 1,1",
				Database: "testdb",
			},
			stats: roachpb.StatementStatistics{
				Count: 31,
			},
		},
		{
			id: 1,
			key: roachpb.StatementStatisticsKey{
				App:      "app0",
				Query:    "SELECT 1,1",
				Database: "testdb",
			},
			stats: roachpb.StatementStatistics{
				Count: 32,
			},
		},
		{
			id: 0,
			key: roachpb.StatementStatisticsKey{
				App:      "app1",
				Query:    "SELECT 1",
				Database: "testdb",
			},
			stats: roachpb.StatementStatistics{
				Count: 33,
			},
		},
		{
			id: 1,
			key: roachpb.StatementStatisticsKey{
				App:      "app100",
				Query:    "SELECT 1,1",
				Database: "testdb",
			},
			stats: roachpb.StatementStatistics{
				Count: 2,
			},
		},
	}

	expectedCount := make(map[string]int64)
	input :=
		make([]serverpb.StatementsResponse_CollectedStatementStatistics, 0, len(testData))

	for i := range testData {
		var stats serverpb.StatementsResponse_CollectedStatementStatistics
		stats.Stats = testData[i].stats
		stats.ID = testData[i].id
		stats.Key.KeyData = testData[i].key
		input = append(input, stats)
		expectedCountKey := testData[i].key.App + testData[i].key.Query
		if count, ok := expectedCount[expectedCountKey]; ok {
			expectedCount[expectedCountKey] = testData[i].stats.Count + count
		} else {
			expectedCount[expectedCountKey] = testData[i].stats.Count
		}
	}

	sqlStats, err := NewTempSQLStatsFromExistingStmtStats(input)
	require.NoError(t, err)

	foundStats := make(map[string]int64)
	require.NoError(t,
		sqlStats.IterateStatementStats(
			context.Background(),
			&sqlstats.IteratorOptions{},
			func(
				ctx context.Context,
				statistics *roachpb.CollectedStatementStatistics,
			) error {
				require.Equal(t, "testdb", statistics.Key.Database)
				foundStats[statistics.Key.App+statistics.Key.Query] = statistics.Stats.Count
				return nil
			}))

	require.Equal(t, expectedCount, foundStats)
}

func TestSQLStatsTxnStatsBulkIngest(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testData := []struct {
		stats roachpb.CollectedTransactionStatistics
	}{
		{
			stats: roachpb.CollectedTransactionStatistics{
				TransactionFingerprintID: roachpb.TransactionFingerprintID(0),
				App:                      "app1",
				Stats: roachpb.TransactionStatistics{
					Count: 7,
				},
			},
		},
		{
			stats: roachpb.CollectedTransactionStatistics{
				TransactionFingerprintID: roachpb.TransactionFingerprintID(0),
				App:                      "app0",
				Stats: roachpb.TransactionStatistics{
					Count: 2,
				},
			},
		},
		{
			stats: roachpb.CollectedTransactionStatistics{
				TransactionFingerprintID: roachpb.TransactionFingerprintID(1),
				App:                      "app100",
				Stats: roachpb.TransactionStatistics{
					Count: 31,
				},
			},
		},
		{
			stats: roachpb.CollectedTransactionStatistics{
				TransactionFingerprintID: roachpb.TransactionFingerprintID(1),
				App:                      "app0",
				Stats: roachpb.TransactionStatistics{
					Count: 32,
				},
			},
		},
		{
			stats: roachpb.CollectedTransactionStatistics{
				TransactionFingerprintID: roachpb.TransactionFingerprintID(0),
				App:                      "app1",
				Stats: roachpb.TransactionStatistics{
					Count: 33,
				},
			},
		},
		{
			stats: roachpb.CollectedTransactionStatistics{
				TransactionFingerprintID: roachpb.TransactionFingerprintID(1),
				App:                      "app100",
				Stats: roachpb.TransactionStatistics{
					Count: 2,
				},
			},
		},
	}

	expectedCount := make(map[roachpb.TransactionFingerprintID]int64)
	input :=
		make([]serverpb.StatementsResponse_ExtendedCollectedTransactionStatistics, 0, len(testData))

	for i := range testData {
		var stats serverpb.StatementsResponse_ExtendedCollectedTransactionStatistics
		stats.StatsData.Stats = testData[i].stats.Stats
		input = append(input, stats)
		if count, ok := expectedCount[stats.StatsData.TransactionFingerprintID]; ok {
			expectedCount[stats.StatsData.TransactionFingerprintID] = testData[i].stats.Stats.Count + count
		} else {
			expectedCount[stats.StatsData.TransactionFingerprintID] = testData[i].stats.Stats.Count
		}
	}

	sqlStats, err := NewTempSQLStatsFromExistingTxnStats(input)
	require.NoError(t, err)

	foundStats := make(map[roachpb.TransactionFingerprintID]int64)
	require.NoError(t,
		sqlStats.IterateTransactionStats(
			context.Background(),
			&sqlstats.IteratorOptions{},
			func(
				ctx context.Context,
				statistics *roachpb.CollectedTransactionStatistics,
			) error {
				foundStats[statistics.TransactionFingerprintID] = statistics.Stats.Count
				return nil
			}))

	require.Equal(t, expectedCount, foundStats)
}
