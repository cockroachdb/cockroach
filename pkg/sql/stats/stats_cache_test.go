// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package stats

import (
	"context"
	"testing"

	"time"

	"reflect"

	"sort"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/pkg/errors"
)

func insertTableStat(
	ctx context.Context,
	db *client.DB,
	ex sqlutil.InternalExecutor,
	stat *TableStatistic,
	histogram *HistogramData,
) error {
	insertStatStmt := `
INSERT INTO system.public.table_statistics ("tableID", "statisticID", name, "columnIDs", "createdAt",
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
	if histogram != nil {
		histogramBytes, err := protoutil.Marshal(histogram)
		if err != nil {
			return err
		}
		args[8] = histogramBytes
	}

	var rows int
	if err := db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		var err error
		rows, err = ex.ExecuteStatementInTransaction(ctx, "insert-table-stats", txn, insertStatStmt, args...)
		return err
	}); err != nil {
		return err
	}
	if rows != 1 {
		return errors.Errorf("%d rows affected by stats insertion; expected exactly one row affected.", rows)
	}
	return nil

}

func checkStatsForTable(
	ctx context.Context, sc *TableStatisticsCache, expected []*TableStatistic, tableID sqlbase.ID,
) error {
	// Initially the stats won't be in the cache.
	if statsList, ok := sc.lookupTableStats(ctx, tableID); ok {
		return errors.Errorf("lookup of missing key %d returned: %s", tableID, statsList)
	}

	// Perform the lookup and refresh, and confirm the
	// returned stats match the expected values.
	statsList, err := sc.GetTableStats(ctx, tableID)
	if err != nil {
		return errors.Errorf(err.Error())
	}
	testutils.SortStructs(statsList, "TableID", "StatisticID")
	if !reflect.DeepEqual(statsList, expected) {
		return errors.Errorf("for lookup of key %d, expected stats %s, got %s", tableID, expected, statsList)
	}

	// Now the stats should be in the cache.
	if _, ok := sc.lookupTableStats(ctx, tableID); !ok {
		return errors.Errorf("for lookup of key %d, expected stats %s", tableID, expected)
	}
	return nil
}

func checkHistForTable(
	ctx context.Context,
	sc *TableStatisticsCache,
	expected *HistogramData,
	tableID sqlbase.ID,
	statisticID uint64,
) error {
	// Initially the histogram won't be in the cache.
	if histogram, ok := sc.lookupHistogram(ctx, tableID, statisticID); ok {
		return errors.Errorf("lookup of missing key {table %d, statistic %d} returned: %s",
			tableID, statisticID, histogram)
	}

	// Perform the lookup and refresh, and confirm the
	// returned histogram matches the expected value.
	histogram, err := sc.GetHistogram(ctx, tableID, statisticID)
	if expected == nil {
		// GetHistogram should return an error if the requested histogram doesn't exist.
		if err == nil {
			return errors.Errorf("expected an error for lookup of nonexistent histogram with key {table %d, statistic %d}",
				tableID, statisticID)
		}
		return nil
	}
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(histogram, expected) {
		return errors.Errorf("for lookup of table %d and statistic %d, expected stat %s, got %s",
			tableID, statisticID, expected, histogram)
	}

	// Now the histogram should be in the cache.
	if _, ok := sc.lookupHistogram(ctx, tableID, statisticID); !ok {
		return errors.Errorf("for lookup of table %d and statistic %d, expected stats %s",
			tableID, statisticID, expected)
	}
	return nil
}

func initTestData(
	ctx context.Context, db *client.DB, ex sqlutil.InternalExecutor,
) (map[sqlbase.ID][]*TableStatistic, map[HistogramCacheKey]*HistogramData, error) {
	// The expected stats must be ordered by TableID, StatisticID so they can
	// later be compared with the returned stats using reflect.DeepEqual.
	expStatsList := []struct {
		Stat      TableStatistic
		Histogram *HistogramData
	}{
		{
			Stat: TableStatistic{
				TableID:       sqlbase.ID(0),
				StatisticID:   0,
				Name:          "table0",
				ColumnIDs:     []sqlbase.ColumnID{1},
				CreatedAt:     time.Date(2010, 11, 20, 11, 35, 23, 0, time.UTC),
				RowCount:      32,
				DistinctCount: 30,
				NullCount:     0,
				HasHistogram:  true,
			},
			Histogram: &HistogramData{Buckets: []HistogramData_Bucket{
				{NumEq: 3, NumRange: 30, UpperBound: encoding.EncodeVarintAscending(nil, 3000)}},
			},
		},
		{
			Stat: TableStatistic{
				TableID:       sqlbase.ID(0),
				StatisticID:   1,
				ColumnIDs:     []sqlbase.ColumnID{2, 3},
				CreatedAt:     time.Date(2010, 11, 20, 11, 35, 23, 0, time.UTC),
				RowCount:      32,
				DistinctCount: 5,
				NullCount:     5,
			},
		},
		{
			Stat: TableStatistic{
				TableID:       sqlbase.ID(1),
				StatisticID:   0,
				ColumnIDs:     []sqlbase.ColumnID{0},
				CreatedAt:     time.Date(2017, 11, 20, 11, 35, 23, 0, time.UTC),
				RowCount:      320000,
				DistinctCount: 300000,
				NullCount:     100,
			},
		},
		{
			Stat: TableStatistic{
				TableID:       sqlbase.ID(2),
				StatisticID:   34,
				Name:          "table2",
				ColumnIDs:     []sqlbase.ColumnID{1, 2, 3},
				CreatedAt:     time.Date(2001, 1, 10, 5, 25, 14, 0, time.UTC),
				RowCount:      0,
				DistinctCount: 0,
				NullCount:     0,
			},
		},
	}

	// Insert the stats into system.table_statistics
	// and store them in maps for fast retrieval.
	expectedStats := make(map[sqlbase.ID][]*TableStatistic)
	expectedHist := make(map[HistogramCacheKey]*HistogramData)
	for i := range expStatsList {
		stat := &expStatsList[i].Stat
		histogram := expStatsList[i].Histogram

		if err := insertTableStat(ctx, db, ex, stat, histogram); err != nil {
			return nil, nil, err
		}

		expectedStats[stat.TableID] = append(expectedStats[stat.TableID], stat)
		if stat.HasHistogram != (histogram != nil) {
			return nil, nil, errors.Errorf("HasHistogram must be true iff there is a histogram. Data: %s",
				expStatsList[i])
		}
		if histogram != nil {
			histCacheKey := HistogramCacheKey{TableID: stat.TableID, StatisticID: stat.StatisticID}
			expectedHist[histCacheKey] = histogram
		}
	}

	// Add another TableID for which we don't have stats.
	expectedStats[sqlbase.ID(3)] = nil
	expectedHist[HistogramCacheKey{TableID: sqlbase.ID(3), StatisticID: 0}] = nil

	return expectedStats, expectedHist, nil
}

func TestTableStatisticsCache(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	s, _, db := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	ex := s.InternalExecutor().(sqlutil.InternalExecutor)

	expectedStats, expectedHist, err := initTestData(ctx, db, ex)
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
	statsCacheSize, histogramCacheSize := 2, 2
	sc := NewTableStatisticsCache(statsCacheSize, histogramCacheSize, db, ex)
	for _, tableID := range tableIDs {
		if err := checkStatsForTable(ctx, sc, expectedStats[tableID], tableID); err != nil {
			t.Fatal(err)
		}
	}

	// Table IDs 0 and 1 should have been evicted since the cache size is 2.
	tableIDs = []sqlbase.ID{sqlbase.ID(0), sqlbase.ID(1)}
	for _, tableID := range tableIDs {
		if statsList, ok := sc.lookupTableStats(ctx, tableID); ok {
			t.Fatalf("lookup of evicted key %d returned: %s", tableID, statsList)
		}
	}

	// Table IDs 2 and 3 should still be in the cache.
	tableIDs = []sqlbase.ID{sqlbase.ID(2), sqlbase.ID(3)}
	for _, tableID := range tableIDs {
		if _, ok := sc.lookupTableStats(ctx, tableID); !ok {
			t.Fatalf("for lookup of key %d, expected stats %s", tableID, expectedStats[tableID])
		}
	}

	// After invalidation Table ID 2 should be gone.
	tableID := sqlbase.ID(2)
	sc.InvalidateTableStats(ctx, tableID)
	if statsList, ok := sc.lookupTableStats(ctx, tableID); ok {
		t.Fatalf("lookup of invalidated key %d returned: %s", tableID, statsList)
	}

	// Now test the histogram cache.
	for key, hist := range expectedHist {
		if err := checkHistForTable(ctx, sc, hist, key.TableID, key.StatisticID); err != nil {
			t.Fatal(err)
		}
	}

	// Key {0, 0} should still be in the cache.
	key := HistogramCacheKey{TableID: 0, StatisticID: 0}
	if _, ok := sc.lookupHistogram(ctx, key.TableID, key.StatisticID); !ok {
		t.Fatalf("for lookup of table %d and statistic %d, expected histogram %s",
			key.TableID, key.StatisticID, expectedHist[key])
	}

	// After invalidation key {0, 0} should be gone.
	sc.InvalidateHistogram(ctx, key.TableID, key.StatisticID)
	if histogram, ok := sc.lookupHistogram(ctx, key.TableID, key.StatisticID); ok {
		t.Fatalf("lookup of invalidated key {table %d, statistic %d} returned: %s",
			key.TableID, key.StatisticID, histogram)
	}
}
