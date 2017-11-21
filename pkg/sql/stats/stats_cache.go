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
	"time"

	"golang.org/x/net/context"

	"github.com/aws/aws-sdk-go/aws/awsutil"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/util/cache"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/pkg/errors"
)

// A Stats object holds a statistic for a particular column or group of columns.
// It mirrors the structure of the system.table_statistics table.
type Stats struct {
	// The ID of the table.
	TableID sqlbase.ID

	// The ID for this statistic.  It need not be globally unique,
	// but must be unique for this table.
	StatisticID uint32

	// Optional user-defined name for the statistic.
	Name string

	// The column ID(s) for which this statistic is generated.
	ColumnIDs []sqlbase.ColumnID

	// The time at which the statistic was created.
	CreatedAt time.Time

	// The total number of rows in the table.
	RowCount uint64

	// The estimated number of distinct values of the columns in ColumnIDs.
	DistinctCount uint64

	// The number of rows that have a NULL in any of the columns in ColumnIDs.
	NullCount uint64

	// Optional, and can only be set if there is a single column in ColumnIDs.
	// Defines a histogram of the distribution of values in the column.
	Histogram *HistogramData
}

func (s Stats) String() string {
	return awsutil.Prettify(s)
}

// A TableStatisticsCache is a cache of statistics for different columns
// and column groups, keyed by table ID.
type TableStatisticsCache struct {
	// NB: This can't be a RWMutex for lookup because UnorderedCache.Get
	// manipulates an internal LRU list.
	mu struct {
		syncutil.Mutex
		cache *cache.UnorderedCache
	}
	SQLExecutor sqlutil.InternalExecutor
}

// NewTableStatisticsCache creates a new TableStatisticsCache of the given size.
// The underlying cache internally uses a hash map, so lookups are cheap.
func NewTableStatisticsCache(size int, sqlExecutor sqlutil.InternalExecutor) *TableStatisticsCache {
	tableStatsCache := &TableStatisticsCache{SQLExecutor: sqlExecutor}
	tableStatsCache.mu.cache = cache.NewUnorderedCache(cache.Config{
		Policy:      cache.CacheLRU,
		ShouldEvict: func(s int, key, value interface{}) bool { return s > size },
	})
	return tableStatsCache
}

// Lookup returns the cached statistics of the given table ID.
func (sc *TableStatisticsCache) Lookup(ctx context.Context, tableID sqlbase.ID) ([]*Stats, bool) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if v, ok := sc.mu.cache.Get(tableID); ok {
		if log.V(2) {
			log.Infof(ctx, "r%d: lookup statistics for table: %s", tableID, v)
		}
		return v.([]*Stats), true
	}
	if log.V(2) {
		log.Infof(ctx, "r%d: lookup statistics for table: not found", tableID)
	}
	return nil, false
}

// Refresh updates the cached statistics for the given table ID
// by issuing a query to system.table_statistics, and returns the statistics.
func (sc *TableStatisticsCache) Refresh(
	ctx context.Context, db *client.DB, tableID sqlbase.ID,
) ([]*Stats, error) {
	var tableStatistics []*Stats
	if err := db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		var err error
		tableStatistics, err = sc.getTableStatistics(ctx, txn, tableID)
		return err
	}); err != nil {
		return nil, err
	}

	if log.V(2) {
		log.Infof(ctx, "r%d: updating statistics for table: %s", tableID, tableStatistics)
	}
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.mu.cache.Add(tableID, tableStatistics)
	return tableStatistics, nil
}

// LookupAndMaybeRefresh is a convenience function to look up statistics for
// the requested table ID in the cache using Lookup, and if the stats are not
// present in the cache, it looks them up in system.table_statistics using Refresh.
func (sc *TableStatisticsCache) LookupAndMaybeRefresh(
	ctx context.Context, db *client.DB, tableID sqlbase.ID,
) ([]*Stats, error) {
	if stats, ok := sc.Lookup(ctx, tableID); ok {
		return stats, nil
	}
	return sc.Refresh(ctx, db, tableID)
}

// Invalidate invalidates the cached statistics for the given table ID.
func (sc *TableStatisticsCache) Invalidate(ctx context.Context, tableID sqlbase.ID) {
	if log.V(2) {
		log.Infof(ctx, "r%d: evicting statistics for table", tableID)
	}
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.mu.cache.Del(tableID)
}

const (
	tableIDIndex = iota
	statisticsIDIndex
	nameIndex
	columnIDsIndex
	createdAtIndex
	rowCountIndex
	distinctCountIndex
	nullCountIndex
	histogramIndex
	statsLen
)

// parseStats converts the given datums to a Stats object. It only includes
// a histogram in the output if the histogram in datums is non-null and
// includeHistogram is true.
func parseStats(datums tree.Datums, includeHistogram bool) (*Stats, error) {
	if datums == nil || datums.Len() == 0 {
		return nil, nil
	}

	// Validate the input length.
	if datums.Len() != statsLen {
		return nil, errors.Errorf("%d values returned from table statistics lookup. Expected %d", datums.Len(), statsLen)
	}

	// Validate the input types.
	expectedTypes := []struct {
		fieldName    string
		fieldIndex   int
		expectedType types.T
		nullable     bool
	}{
		{"tableID", tableIDIndex, types.Int, false},
		{"statisticsID", statisticsIDIndex, types.Int, false},
		{"name", nameIndex, types.String, true},
		{"columnIDs", columnIDsIndex, types.TArray{Typ: types.Int}, false},
		{"createdAt", createdAtIndex, types.Timestamp, false},
		{"rowCount", rowCountIndex, types.Int, false},
		{"distinctCount", distinctCountIndex, types.Int, false},
		{"nullCount", nullCountIndex, types.Int, false},
		{"histogram", histogramIndex, types.Bytes, true},
	}
	for _, v := range expectedTypes {
		if datums[v.fieldIndex].ResolvedType() != v.expectedType &&
			(!v.nullable || datums[v.fieldIndex].ResolvedType() != types.Null) {
			return nil, errors.Errorf("%s returned from table statistics lookup has type %s. Expected %s",
				v.fieldName, datums[v.fieldIndex].ResolvedType(), v.expectedType)
		}
	}

	// Extract datum values.
	tableStatistic := &Stats{
		TableID:       sqlbase.ID((int32)(*datums[tableIDIndex].(*tree.DInt))),
		StatisticID:   (uint32)(*datums[statisticsIDIndex].(*tree.DInt)),
		CreatedAt:     datums[createdAtIndex].(*tree.DTimestamp).Time,
		RowCount:      (uint64)(*datums[rowCountIndex].(*tree.DInt)),
		DistinctCount: (uint64)(*datums[distinctCountIndex].(*tree.DInt)),
		NullCount:     (uint64)(*datums[nullCountIndex].(*tree.DInt)),
	}
	columnIDs := datums[columnIDsIndex].(*tree.DArray)
	tableStatistic.ColumnIDs = make([]sqlbase.ColumnID, len(columnIDs.Array))
	for i, d := range columnIDs.Array {
		tableStatistic.ColumnIDs[i] = sqlbase.ColumnID((int32)(*d.(*tree.DInt)))
	}
	if datums[nameIndex].ResolvedType() == types.String {
		tableStatistic.Name = string(*datums[nameIndex].(*tree.DString))
	}
	if includeHistogram && datums[histogramIndex].ResolvedType() == types.Bytes {
		tableStatistic.Histogram = &HistogramData{}
		if err := protoutil.Unmarshal([]byte(*datums[histogramIndex].(*tree.DBytes)), tableStatistic.Histogram); err != nil {
			return nil, err
		}
	}

	return tableStatistic, nil
}

// getTableStatistics retrieves the statistics in system.table_statistics
// for the given table ID.
func (sc *TableStatisticsCache) getTableStatistics(
	ctx context.Context, txn *client.Txn, tableID sqlbase.ID,
) ([]*Stats, error) {
	const getTableStatisticsStmt = `
SELECT "tableID", "statisticID", name, "columnIDs", "createdAt", "rowCount", "distinctCount", "nullCount", histogram
FROM system.table_statistics
WHERE "tableID" = $1
`
	rows, err := sc.SQLExecutor.QueryRowsInTransaction(ctx, "get-table-statistics", txn, getTableStatisticsStmt, tableID)
	if err != nil {
		return nil, err
	}

	var statsList []*Stats
	for _, row := range rows {
		stats, err := parseStats(row, true /* includeHistogram */)
		if err != nil {
			return nil, err
		}
		statsList = append(statsList, stats)
	}

	return statsList, nil
}
