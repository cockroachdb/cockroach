// Copyright 2017 The Cockroach Authors.
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
	"math/rand"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

func insertTableStat(
	ctx context.Context, db *kv.DB, ex sqlutil.InternalExecutor, stat *TableStatisticProto,
) error {
	insertStatStmt := `
INSERT INTO system.table_statistics ("tableID", "statisticID", name, "columnIDs", "createdAt",
	"rowCount", "distinctCount", "nullCount", histogram)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
`
	columnIDs := tree.NewDArray(types.Int)
	for _, id := range stat.ColumnIDs {
		if err := columnIDs.Append(tree.NewDInt(tree.DInt(int(id)))); err != nil {
			return err
		}
	}

	args := []interface{}{
		stat.TableID,
		stat.StatisticID,
		nil, // name
		columnIDs,
		stat.CreatedAt,
		stat.RowCount,
		stat.DistinctCount,
		stat.NullCount,
		nil, // histogram
	}
	if len(stat.Name) != 0 {
		args[2] = stat.Name
	}
	if stat.HistogramData != nil {
		histogramBytes, err := protoutil.Marshal(stat.HistogramData)
		if err != nil {
			return err
		}
		args[8] = histogramBytes
	}

	var rows int
	rows, err := ex.Exec(ctx, "insert-stat", nil /* txn */, insertStatStmt, args...)
	if err != nil {
		return err
	}
	if rows != 1 {
		return errors.Errorf("%d rows affected by stats insertion; expected exactly one row affected.", rows)
	}
	return nil

}

func lookupTableStats(
	ctx context.Context, sc *TableStatisticsCache, tableID sqlbase.ID,
) ([]*TableStatistic, bool) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if e, ok := sc.mu.cache.Get(tableID); ok {
		return e.(*cacheEntry).stats, true
	}
	return nil, false
}

func checkStatsForTable(
	ctx context.Context,
	sc *TableStatisticsCache,
	expected []*TableStatisticProto,
	tableID sqlbase.ID,
) error {
	// Initially the stats won't be in the cache.
	if statsList, ok := lookupTableStats(ctx, sc, tableID); ok {
		return errors.Errorf("lookup of missing key %d returned: %s", tableID, statsList)
	}

	// Perform the lookup and refresh, and confirm the
	// returned stats match the expected values.
	statsList, err := sc.GetTableStats(ctx, tableID)
	if err != nil {
		return errors.Wrap(err, "retrieving stats")
	}
	if !checkStats(statsList, expected) {
		return errors.Errorf("for lookup of key %d, expected stats %s, got %s", tableID, expected, statsList)
	}

	// Now the stats should be in the cache.
	if _, ok := lookupTableStats(ctx, sc, tableID); !ok {
		return errors.Errorf("for lookup of key %d, expected stats %s", tableID, expected)
	}
	return nil
}

func checkStats(actual []*TableStatistic, expected []*TableStatisticProto) bool {
	if len(actual) == 0 && len(expected) == 0 {
		// DeepEqual differentiates between nil and empty slices, we don't.
		return true
	}
	var protoList []*TableStatisticProto
	for i := range actual {
		protoList = append(protoList, &actual[i].TableStatisticProto)
	}
	return reflect.DeepEqual(protoList, expected)
}

func initTestData(
	ctx context.Context, db *kv.DB, ex sqlutil.InternalExecutor,
) (map[sqlbase.ID][]*TableStatisticProto, error) {
	// The expected stats must be ordered by TableID+, CreatedAt- so they can
	// later be compared with the returned stats using reflect.DeepEqual.
	expStatsList := []TableStatisticProto{
		{
			TableID:       sqlbase.ID(100),
			StatisticID:   0,
			Name:          "table0",
			ColumnIDs:     []sqlbase.ColumnID{1},
			CreatedAt:     time.Date(2010, 11, 20, 11, 35, 24, 0, time.UTC),
			RowCount:      32,
			DistinctCount: 30,
			NullCount:     0,
			HistogramData: &HistogramData{ColumnType: types.Int, Buckets: []HistogramData_Bucket{
				{NumEq: 3, NumRange: 30, UpperBound: encoding.EncodeVarintAscending(nil, 3000)}},
			},
		},
		{
			TableID:       sqlbase.ID(100),
			StatisticID:   1,
			ColumnIDs:     []sqlbase.ColumnID{2, 3},
			CreatedAt:     time.Date(2010, 11, 20, 11, 35, 23, 0, time.UTC),
			RowCount:      32,
			DistinctCount: 5,
			NullCount:     5,
		},
		{
			TableID:       sqlbase.ID(101),
			StatisticID:   0,
			ColumnIDs:     []sqlbase.ColumnID{0},
			CreatedAt:     time.Date(2017, 11, 20, 11, 35, 23, 0, time.UTC),
			RowCount:      320000,
			DistinctCount: 300000,
			NullCount:     100,
		},
		{
			TableID:       sqlbase.ID(102),
			StatisticID:   34,
			Name:          "table2",
			ColumnIDs:     []sqlbase.ColumnID{1, 2, 3},
			CreatedAt:     time.Date(2001, 1, 10, 5, 25, 14, 0, time.UTC),
			RowCount:      0,
			DistinctCount: 0,
			NullCount:     0,
		},
	}

	// Insert the stats into system.table_statistics
	// and store them in maps for fast retrieval.
	expectedStats := make(map[sqlbase.ID][]*TableStatisticProto)
	for i := range expStatsList {
		stat := &expStatsList[i]

		if err := insertTableStat(ctx, db, ex, stat); err != nil {
			return nil, err
		}

		expectedStats[stat.TableID] = append(expectedStats[stat.TableID], stat)
	}

	// Add another TableID for which we don't have stats.
	expectedStats[sqlbase.ID(103)] = nil

	return expectedStats, nil
}

func TestCacheBasic(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	s, _, db := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	ex := s.InternalExecutor().(sqlutil.InternalExecutor)

	expectedStats, err := initTestData(ctx, db, ex)
	if err != nil {
		t.Fatal(err)
	}

	// Collect the tableIDs and sort them so we can iterate over them in a
	// consistent order (Go randomizes the order of iteration over maps).
	var tableIDs sqlbase.IDs
	for tableID := range expectedStats {
		tableIDs = append(tableIDs, tableID)
	}
	sort.Sort(tableIDs)

	// Create a cache and iteratively query the cache for each tableID. This
	// will result in the cache getting populated. When the stats cache size is
	// exceeded, entries should be evicted according to the LRU policy.
	sc := NewTableStatisticsCache(
		2, /* cacheSize */
		gossip.MakeExposedGossip(s.GossipI().(*gossip.Gossip)),
		db,
		ex,
		keys.SystemSQLCodec,
	)
	for _, tableID := range tableIDs {
		if err := checkStatsForTable(ctx, sc, expectedStats[tableID], tableID); err != nil {
			t.Fatal(err)
		}
	}

	// Table IDs 0 and 1 should have been evicted since the cache size is 2.
	tableIDs = []sqlbase.ID{sqlbase.ID(100), sqlbase.ID(101)}
	for _, tableID := range tableIDs {
		if statsList, ok := lookupTableStats(ctx, sc, tableID); ok {
			t.Fatalf("lookup of evicted key %d returned: %s", tableID, statsList)
		}
	}

	// Table IDs 2 and 3 should still be in the cache.
	tableIDs = []sqlbase.ID{sqlbase.ID(102), sqlbase.ID(103)}
	for _, tableID := range tableIDs {
		if _, ok := lookupTableStats(ctx, sc, tableID); !ok {
			t.Fatalf("for lookup of key %d, expected stats %s", tableID, expectedStats[tableID])
		}
	}

	// After invalidation Table ID 2 should be gone.
	tableID := sqlbase.ID(102)
	sc.InvalidateTableStats(ctx, tableID)
	if statsList, ok := lookupTableStats(ctx, sc, tableID); ok {
		t.Fatalf("lookup of invalidated key %d returned: %s", tableID, statsList)
	}
}

func TestCacheUserDefinedTypes(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	s, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	if _, err := sqlDB.Exec(`
SET experimental_enable_enums=true;
CREATE DATABASE t;
USE t;
CREATE TYPE t AS ENUM ('hello');
CREATE TABLE tt (x t PRIMARY KEY);
INSERT INTO tt VALUES ('hello');
CREATE STATISTICS s FROM tt;
`); err != nil {
		t.Fatal(err)
	}
	_ = kvDB
	// Make a stats cache.
	sc := NewTableStatisticsCache(
		1,
		gossip.MakeExposedGossip(s.GossipI().(*gossip.Gossip)),
		kvDB,
		s.InternalExecutor().(sqlutil.InternalExecutor),
		keys.SystemSQLCodec,
	)
	tbl := sqlbase.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "tt")
	// Get stats for our table. We are ensuring here that the access to the stats
	// for tt properly hydrates the user defined type t before access.
	_, err := sc.GetTableStats(ctx, tbl.ID)
	if err != nil {
		t.Fatal(err)
	}
}

// TestCacheWait verifies that when a table gets invalidated, we only retrieve
// the stats one time, even if there are multiple callers asking for them.
func TestCacheWait(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	s, _, db := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	ex := s.InternalExecutor().(sqlutil.InternalExecutor)

	expectedStats, err := initTestData(ctx, db, ex)
	if err != nil {
		t.Fatal(err)
	}

	// Collect the tableIDs and sort them so we can iterate over them in a
	// consistent order (Go randomizes the order of iteration over maps).
	var tableIDs sqlbase.IDs
	for tableID := range expectedStats {
		tableIDs = append(tableIDs, tableID)
	}
	sort.Sort(tableIDs)
	sc := NewTableStatisticsCache(
		len(tableIDs), /* cacheSize */
		gossip.MakeExposedGossip(s.GossipI().(*gossip.Gossip)),
		db,
		ex,
		keys.SystemSQLCodec,
	)
	for _, tableID := range tableIDs {
		if err := checkStatsForTable(ctx, sc, expectedStats[tableID], tableID); err != nil {
			t.Fatal(err)
		}
	}

	for run := 0; run < 10; run++ {
		before := sc.mu.numInternalQueries

		id := tableIDs[rand.Intn(len(tableIDs))]
		sc.InvalidateTableStats(ctx, id)
		// Run GetTableStats multiple times in parallel.
		var wg sync.WaitGroup
		for n := 0; n < 10; n++ {
			wg.Add(1)
			go func() {
				stats, err := sc.GetTableStats(ctx, id)
				if err != nil {
					t.Error(err)
				} else if !checkStats(stats, expectedStats[id]) {
					t.Errorf("for table %d, expected stats %s, got %s", id, expectedStats[id], stats)
				}
				wg.Done()
			}()
		}
		wg.Wait()

		if t.Failed() {
			return
		}

		// Verify that we only issued one read from the statistics table.
		if num := sc.mu.numInternalQueries - before; num != 1 {
			t.Fatalf("expected 1 query, got %d", num)
		}
	}
}
