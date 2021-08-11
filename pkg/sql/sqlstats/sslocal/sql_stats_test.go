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
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestSQLStatsBulkIngest(t *testing.T) {
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
				App:   "app1",
				Query: "SELECT 1",
			},
			stats: roachpb.StatementStatistics{
				Count: 7,
			},
		},
		{
			id: 0,
			key: roachpb.StatementStatisticsKey{
				App:   "app0",
				Query: "SELECT 1",
			},
			stats: roachpb.StatementStatistics{
				Count: 2,
			},
		},
		{
			id: 1,
			key: roachpb.StatementStatisticsKey{
				App:   "app100",
				Query: "SELECT 1,1",
			},
			stats: roachpb.StatementStatistics{
				Count: 31,
			},
		},
		{
			id: 1,
			key: roachpb.StatementStatisticsKey{
				App:   "app0",
				Query: "SELECT 1,1",
			},
			stats: roachpb.StatementStatistics{
				Count: 32,
			},
		},
		{
			id: 0,
			key: roachpb.StatementStatisticsKey{
				App:   "app1",
				Query: "SELECT 1",
			},
			stats: roachpb.StatementStatistics{
				Count: 33,
			},
		},
		{
			id: 1,
			key: roachpb.StatementStatisticsKey{
				App:   "app100",
				Query: "SELECT 1,1",
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

	sqlStats, err := NewTempSQLStatsFromExistingData(input)
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
				foundStats[statistics.Key.App+statistics.Key.Query] = statistics.Stats.Count
				return nil
			}))

	require.Equal(t, expectedCount, foundStats)
}
