// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package stats

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

func TestDeleteOldStatsForColumns(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	s, _, db := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	ex := s.InternalExecutor().(sqlutil.InternalExecutor)
	cache := NewTableStatisticsCache(
		10, /* cacheSize */
		gossip.MakeExposedGossip(s.GossipI().(*gossip.Gossip)),
		db,
		ex,
		keys.SystemSQLCodec,
	)

	// The test data must be ordered by CreatedAt DESC so the calculated set of
	// expected deleted stats is correct.
	testData := []TableStatisticProto{
		{
			TableID:       sqlbase.ID(100),
			StatisticID:   1,
			Name:          AutoStatsName,
			ColumnIDs:     []sqlbase.ColumnID{1},
			CreatedAt:     timeutil.Now().Add(-1 * time.Hour),
			RowCount:      1000,
			DistinctCount: 1000,
			NullCount:     0,
		},
		{
			TableID:       sqlbase.ID(100),
			StatisticID:   2,
			Name:          AutoStatsName,
			ColumnIDs:     []sqlbase.ColumnID{1},
			CreatedAt:     timeutil.Now().Add(-2 * time.Hour),
			RowCount:      1000,
			DistinctCount: 1000,
			NullCount:     0,
		},
		{
			TableID:       sqlbase.ID(100),
			StatisticID:   3,
			Name:          "stat_100_1",
			ColumnIDs:     []sqlbase.ColumnID{1},
			CreatedAt:     timeutil.Now().Add(-3 * time.Hour),
			RowCount:      1000,
			DistinctCount: 1000,
			NullCount:     0,
		},
		{
			TableID:       sqlbase.ID(100),
			StatisticID:   4,
			Name:          AutoStatsName,
			ColumnIDs:     []sqlbase.ColumnID{1},
			CreatedAt:     timeutil.Now().Add(-4 * time.Hour),
			RowCount:      1000,
			DistinctCount: 1000,
			NullCount:     0,
		},
		{
			TableID:       sqlbase.ID(100),
			StatisticID:   5,
			Name:          AutoStatsName,
			ColumnIDs:     []sqlbase.ColumnID{1},
			CreatedAt:     timeutil.Now().Add(-5 * time.Hour),
			RowCount:      1000,
			DistinctCount: 1000,
			NullCount:     0,
		},
		{
			TableID:       sqlbase.ID(100),
			StatisticID:   6,
			Name:          AutoStatsName,
			ColumnIDs:     []sqlbase.ColumnID{1},
			CreatedAt:     timeutil.Now().Add(-6 * time.Hour),
			RowCount:      1000,
			DistinctCount: 1000,
			NullCount:     0,
		},
		{
			TableID:       sqlbase.ID(100),
			StatisticID:   7,
			Name:          AutoStatsName,
			ColumnIDs:     []sqlbase.ColumnID{2, 3},
			CreatedAt:     timeutil.Now().Add(-7 * time.Hour),
			RowCount:      1000,
			DistinctCount: 1000,
			NullCount:     5,
		},
		{
			TableID:       sqlbase.ID(100),
			StatisticID:   8,
			Name:          AutoStatsName,
			ColumnIDs:     []sqlbase.ColumnID{2, 3},
			CreatedAt:     timeutil.Now().Add(-8 * time.Hour),
			RowCount:      1000,
			DistinctCount: 1000,
			NullCount:     5,
		},
		{
			TableID:       sqlbase.ID(100),
			StatisticID:   9,
			Name:          "stat_100_2_3",
			ColumnIDs:     []sqlbase.ColumnID{2, 3},
			CreatedAt:     timeutil.Now().Add(-9 * time.Hour),
			RowCount:      1000,
			DistinctCount: 1000,
			NullCount:     5,
		},
		{
			TableID:       sqlbase.ID(100),
			StatisticID:   10,
			Name:          AutoStatsName,
			ColumnIDs:     []sqlbase.ColumnID{2, 3},
			CreatedAt:     timeutil.Now().Add(-10 * time.Hour),
			RowCount:      1000,
			DistinctCount: 1000,
			NullCount:     5,
		},
		{
			TableID:       sqlbase.ID(100),
			StatisticID:   11,
			Name:          AutoStatsName,
			ColumnIDs:     []sqlbase.ColumnID{2, 3},
			CreatedAt:     timeutil.Now().Add(-11 * time.Hour),
			RowCount:      1000,
			DistinctCount: 1000,
			NullCount:     5,
		},
		{
			TableID:       sqlbase.ID(100),
			StatisticID:   12,
			Name:          AutoStatsName,
			ColumnIDs:     []sqlbase.ColumnID{2, 3},
			CreatedAt:     timeutil.Now().Add(-12 * time.Hour),
			RowCount:      1000,
			DistinctCount: 1000,
			NullCount:     5,
		},
		{
			TableID:       sqlbase.ID(100),
			StatisticID:   13,
			Name:          AutoStatsName,
			ColumnIDs:     []sqlbase.ColumnID{2},
			CreatedAt:     timeutil.Now().Add(-13 * time.Hour),
			RowCount:      1000,
			DistinctCount: 1000,
			NullCount:     5,
		},
		{
			TableID:       sqlbase.ID(100),
			StatisticID:   14,
			Name:          "stat_100_1_3",
			ColumnIDs:     []sqlbase.ColumnID{1, 3},
			CreatedAt:     timeutil.Now().Add(-14 * time.Hour),
			RowCount:      1000,
			DistinctCount: 1000,
			NullCount:     5,
		},
		{
			TableID:       sqlbase.ID(100),
			StatisticID:   15,
			Name:          AutoStatsName,
			ColumnIDs:     []sqlbase.ColumnID{3, 2},
			CreatedAt:     timeutil.Now().Add(-15 * time.Hour),
			RowCount:      1000,
			DistinctCount: 1000,
			NullCount:     5,
		},
		{
			TableID:       sqlbase.ID(101),
			StatisticID:   16,
			Name:          "stat_101_1",
			ColumnIDs:     []sqlbase.ColumnID{1},
			CreatedAt:     timeutil.Now().Add(-16 * time.Hour),
			RowCount:      320000,
			DistinctCount: 300000,
			NullCount:     100,
		},
		{
			TableID:       sqlbase.ID(102),
			StatisticID:   17,
			Name:          AutoStatsName,
			ColumnIDs:     []sqlbase.ColumnID{2, 3},
			CreatedAt:     timeutil.Now().Add(-17 * time.Hour),
			RowCount:      0,
			DistinctCount: 0,
			NullCount:     0,
		},
	}

	for i := range testData {
		stat := &testData[i]
		if err := insertTableStat(ctx, db, ex, stat); err != nil {
			t.Fatal(err)
		}
	}

	// findStat searches for a statistic in the given list of stats and returns
	// an error if expectDeleted is true but the statistic is found. Likewise, it
	// returns an error if expectDeleted is false but the statistic is not found.
	findStat := func(
		stats []*TableStatistic, tableID sqlbase.ID, statisticID uint64, expectDeleted bool,
	) error {
		for j := range stats {
			if stats[j].StatisticID == statisticID {
				if expectDeleted {
					return fmt.Errorf(
						"expected statistic %d in table %d to be deleted, but it was not",
						statisticID, tableID,
					)
				}
				return nil
			}
		}

		if !expectDeleted {
			return fmt.Errorf(
				"expected statistic %d in table %d not to be deleted, but it was",
				statisticID, tableID,
			)
		}
		return nil
	}

	// checkDelete deletes old statistics for the given table and column IDs and
	// checks that only the statisticIDs contained in expectDeleted have been
	// deleted.
	checkDelete := func(
		tableID sqlbase.ID, columnIDs []sqlbase.ColumnID, expectDeleted map[uint64]struct{},
	) error {
		if err := s.DB().Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			return DeleteOldStatsForColumns(ctx, ex, txn, tableID, columnIDs)
		}); err != nil {
			return err
		}

		cache.InvalidateTableStats(ctx, tableID)
		tableStats, err := cache.GetTableStats(ctx, tableID)
		if err != nil {
			return err
		}

		for i := range testData {
			stat := &testData[i]
			if stat.TableID != tableID {
				cache.InvalidateTableStats(ctx, stat.TableID)
				stats, err := cache.GetTableStats(ctx, stat.TableID)
				if err != nil {
					return err
				}
				// No stats from other tables should be deleted.
				if err := findStat(
					stats, stat.TableID, stat.StatisticID, false, /* expectDeleted */
				); err != nil {
					return err
				}
				continue
			}

			// Check whether this stat should have been deleted.
			_, expectDeleted := expectDeleted[stat.StatisticID]
			if err := findStat(tableStats, tableID, stat.StatisticID, expectDeleted); err != nil {
				return err
			}
		}

		return nil
	}

	expectDeleted := make(map[uint64]struct{}, len(testData))
	getExpectDeleted := func(tableID sqlbase.ID, columnIDs []sqlbase.ColumnID) {
		keptStats := 0
		for i := range testData {
			stat := &testData[i]
			if stat.TableID != tableID {
				continue
			}
			if !reflect.DeepEqual(stat.ColumnIDs, columnIDs) {
				continue
			}
			if stat.Name == AutoStatsName && keptStats < keepCount {
				keptStats++
				continue
			}
			expectDeleted[stat.StatisticID] = struct{}{}
		}
	}

	// Delete stats for column 1 in table 100.
	tableID := sqlbase.ID(100)
	columnIDs := []sqlbase.ColumnID{1}
	getExpectDeleted(tableID, columnIDs)
	if err := checkDelete(tableID, columnIDs, expectDeleted); err != nil {
		t.Fatal(err)
	}

	// Delete stats for columns {2, 3} in table 100.
	tableID = sqlbase.ID(100)
	columnIDs = []sqlbase.ColumnID{2, 3}
	getExpectDeleted(tableID, columnIDs)
	if err := checkDelete(tableID, columnIDs, expectDeleted); err != nil {
		t.Fatal(err)
	}
}
