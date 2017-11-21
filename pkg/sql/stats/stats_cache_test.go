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

package stats_test

import (
	"testing"

	"golang.org/x/net/context"

	"time"

	"reflect"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/sql/stats"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/pkg/errors"
)

func staticSize(size int64) func() int64 {
	return func() int64 {
		return size
	}
}

func insertTableStats(
	ctx context.Context, txn *client.Txn, sqlExecutor sqlutil.InternalExecutor, stats *stats.Stats,
) error {
	insertStatsStmt := `
INSERT INTO system.table_statistics ("tableID", "statisticID", name, "columnIDs", "createdAt",
	"rowCount", "distinctCount", "nullCount", histogram)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
`
	columnIDs := tree.NewDArray(types.Int)
	for _, id := range stats.ColumnIDs {
		if err := columnIDs.Append(tree.NewDInt(tree.DInt(int(id)))); err != nil {
			return err
		}
	}

	args := []interface{}{
		stats.TableID,
		stats.StatisticID,
		nil, // name
		columnIDs,
		stats.CreatedAt,
		stats.RowCount,
		stats.DistinctCount,
		stats.NullCount,
		nil, // histogram
	}
	if len(stats.Name) != 0 {
		args[2] = stats.Name
	}
	if stats.Histogram != nil {
		histogramBytes, err := protoutil.Marshal(stats.Histogram)
		if err != nil {
			return err
		}
		args[8] = histogramBytes
	}

	rows, err := sqlExecutor.ExecuteStatementInTransaction(ctx, "insert-table-stats", txn, insertStatsStmt, args...)
	if err != nil {
		return err
	}
	if rows != 1 {
		return errors.Errorf("%d rows affected by stats insertion; expected exactly one row affected.", rows)
	}
	return nil

}

func checkStatsForTable(
	ctx context.Context,
	txn *client.Txn,
	sc *stats.TableStatisticsCache,
	expected []*stats.Stats,
	tableID sqlbase.ID,
) error {
	// Initially the stats won't be in the cache.
	if statsList, ok := sc.Lookup(ctx, tableID); ok {
		return errors.Errorf("lookup of missing key %d returned: %s", tableID, statsList)
	}

	// Perform the lookup and refresh, and confirm the
	// returned stats match the expected values.
	statsList, err := sc.LookupAndMaybeRefresh(ctx, txn, tableID)
	if err != nil {
		return errors.Errorf(err.Error())
	} else if statsList == nil {
		return errors.Errorf("for lookup of key %d, expected stats %s", tableID, expected)
	} else {
		testutils.SortStructs(statsList, "TableID", "StatisticID")
		if !reflect.DeepEqual(statsList, expected) {
			return errors.Errorf("for lookup of key %d, expected stats %s, got %s", tableID, expected, statsList)
		}
	}

	// Now the stats should be in the cache.
	if _, ok := sc.Lookup(ctx, tableID); !ok {
		return errors.Errorf("for lookup of key %d, expected stats %s", tableID, expected)
	}
	return nil
}

func TestTableStatisticsCache(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	s, _, db := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	ex := sql.InternalExecutor{LeaseManager: s.LeaseManager().(*sql.LeaseManager)}

	if err := db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		expStatsList := []stats.Stats{
			{
				TableID:       sqlbase.ID(0),
				StatisticID:   0,
				Name:          "table0",
				ColumnIDs:     []sqlbase.ColumnID{1, 2},
				CreatedAt:     time.Date(2010, 11, 20, 11, 35, 23, 0, time.UTC),
				RowCount:      32,
				DistinctCount: 30,
				NullCount:     0,
				Histogram: &stats.HistogramData{Buckets: []stats.HistogramData_Bucket{
					{NumEq: 3, NumRange: 30, UpperBound: encoding.EncodeVarintAscending(nil, 3000)}},
				},
			},
			{
				TableID:       sqlbase.ID(0),
				StatisticID:   1,
				ColumnIDs:     []sqlbase.ColumnID{3},
				CreatedAt:     time.Date(2010, 11, 20, 11, 35, 23, 0, time.UTC),
				RowCount:      32,
				DistinctCount: 5,
				NullCount:     5,
			},
			{
				TableID:       sqlbase.ID(1),
				StatisticID:   0,
				ColumnIDs:     []sqlbase.ColumnID{0},
				CreatedAt:     time.Date(2017, 11, 20, 11, 35, 23, 0, time.UTC),
				RowCount:      320000,
				DistinctCount: 300000,
				NullCount:     100,
			},
			{
				TableID:       sqlbase.ID(2),
				StatisticID:   34,
				Name:          "table2",
				ColumnIDs:     []sqlbase.ColumnID{1, 2, 3},
				CreatedAt:     time.Date(2001, 1, 10, 5, 25, 14, 0, time.UTC),
				RowCount:      0,
				DistinctCount: 0,
				NullCount:     0,
			},
		}

		// Sort the expected stats so they can later be compared with the returned
		// stats using reflect.DeepEqual.
		testutils.SortStructs(expStatsList, "TableID", "StatisticID")

		// Insert the stats into system.table_statistics
		// and store them in a map keyed by table ID for fast retrieval.
		expected := make(map[sqlbase.ID][]*stats.Stats)
		for i := range expStatsList {
			stats := &expStatsList[i]
			if err := insertTableStats(ctx, txn, ex, stats); err != nil {
				return err
			}
			expected[stats.TableID] = append(expected[stats.TableID], stats)
		}

		cacheSize := 2
		sc := stats.NewTableStatisticsCache(staticSize(int64(cacheSize)), ex)
		for tableID := range expected {
			if err := checkStatsForTable(ctx, txn, sc, expected[tableID], tableID); err != nil {
				return err
			}
		}

		// Table ID 0 should have been evicted since the cache size is 2.
		tableID := sqlbase.ID(0)
		if statsList, ok := sc.Lookup(ctx, tableID); ok {
			return errors.Errorf("lookup of evicted key %d returned: %s", tableID, statsList)
		}

		// Table ID 1 should still be in the cache.
		tableID = sqlbase.ID(1)
		if _, ok := sc.Lookup(ctx, tableID); !ok {
			return errors.Errorf("for lookup of key %d, expected stats %s", tableID, expected[tableID])
		}

		// After invalidation Table ID 1 should be gone.
		sc.Invalidate(ctx, tableID)
		if statsList, ok := sc.Lookup(ctx, tableID); ok {
			return errors.Errorf("lookup of invalidated key %d returned: %s", tableID, statsList)
		}

		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
