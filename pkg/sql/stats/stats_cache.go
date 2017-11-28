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

// A TableStatistic object holds a statistic for a particular column or group
// of columns. It mirrors the structure of the system.table_statistics table.
type TableStatistic struct {
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

func (s TableStatistic) String() string {
	return awsutil.Prettify(s)
}

// HistogramCacheKey is used as the key type for entries in the TableStatisticsCache
// which contain a histogram.
type HistogramCacheKey struct {
	TableID     sqlbase.ID
	StatisticID uint32
}

// A TableStatisticsCache is a cache of TableStatistic objects, keyed by
// table ID or HistogramCacheKey{table ID, statistic ID}. The entries keyed
// by table ID contain all the statistics for different columns and column
// groups for the given table, and do not include histograms.  The entries
// keyed by HistogramCacheKey{table ID, statistic ID} contain a single
// TableStatistic object, including the histogram.
//
// Users should probably create two different caches for these different
// entry types using two calls to NewTableStatisticsCache(), but they may
// also keep them in the same cache for simplicity.
type TableStatisticsCache struct {
	// NB: This can't be a RWMutex for lookup because UnorderedCache.Get
	// manipulates an internal LRU list.
	mu struct {
		syncutil.Mutex
		cache *cache.UnorderedCache
	}
	ClientDB    *client.DB
	SQLExecutor sqlutil.InternalExecutor
}

// NewTableStatisticsCache creates a new TableStatisticsCache of the given size.
// The underlying cache internally uses a hash map, so lookups are cheap.
func NewTableStatisticsCache(
	size int, db *client.DB, sqlExecutor sqlutil.InternalExecutor,
) *TableStatisticsCache {
	tableStatsCache := &TableStatisticsCache{
		ClientDB:    db,
		SQLExecutor: sqlExecutor,
	}
	tableStatsCache.mu.cache = cache.NewUnorderedCache(cache.Config{
		Policy:      cache.CacheLRU,
		ShouldEvict: func(s int, key, value interface{}) bool { return s > size },
	})
	return tableStatsCache
}

// LookupTableStats returns the cached statistics of the given table ID,
// excluding the histograms.
// The second return value is true if the stats were found in the
// cache, and false otherwise.
func (sc *TableStatisticsCache) LookupTableStats(
	ctx context.Context, tableID sqlbase.ID,
) ([]*TableStatistic, bool) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if v, ok := sc.mu.cache.Get(tableID); ok {
		if log.V(2) {
			log.Infof(ctx, "lookup statistics for table %d: %s", tableID, v)
		}
		return v.([]*TableStatistic), true
	}
	if log.V(2) {
		log.Infof(ctx, "lookup statistics for table %d: not found", tableID)
	}
	return nil, false
}

// LookupHistogram returns the cached statistics and histogram of
// the given table ID and statistic ID.
// The second return value is true if the stats were found in the
// cache, and false otherwise.
func (sc *TableStatisticsCache) LookupHistogram(
	ctx context.Context, tableID sqlbase.ID, statisticID uint32,
) (*TableStatistic, bool) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if v, ok := sc.mu.cache.Get(HistogramCacheKey{tableID, statisticID}); ok {
		if log.V(2) {
			log.Infof(ctx, "lookup histogram for table %d, statistic %d: %s", tableID, statisticID, v)
		}
		return v.(*TableStatistic), true
	}
	if log.V(2) {
		log.Infof(ctx, "lookup histogram for table %d, statistic %d: not found", tableID, statisticID)
	}
	return nil, false
}

// RefreshTableStats updates the cached statistics for the given table ID
// by issuing a query to system.table_statistics, and returns the statistics.
// It excludes the histograms.
func (sc *TableStatisticsCache) RefreshTableStats(
	ctx context.Context, tableID sqlbase.ID,
) ([]*TableStatistic, error) {
	tableStatistics, err := sc.getTableStatistics(ctx, tableID)
	if err != nil {
		return nil, err
	}

	if log.V(2) {
		log.Infof(ctx, "updating statistics for table %d: %s", tableID, tableStatistics)
	}
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.mu.cache.Add(tableID, tableStatistics)
	return tableStatistics, nil
}

// RefreshHistogram updates the cached statistics and histogram for the given
// table ID and statistic ID by issuing a query to system.table_statistics, and
// returns the statistics.
func (sc *TableStatisticsCache) RefreshHistogram(
	ctx context.Context, tableID sqlbase.ID, statisticID uint32,
) (*TableStatistic, error) {
	tableStatistic, err := sc.getHistogram(ctx, tableID, statisticID)
	if err != nil {
		return nil, err
	}

	if log.V(2) {
		log.Infof(ctx, "updating histogram for table %d, statistic %d: %s", tableID, statisticID, tableStatistic)
	}
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.mu.cache.Add(HistogramCacheKey{tableID, statisticID}, tableStatistic)
	return tableStatistic, nil
}

// GetTableStats is a convenience function to look up statistics for the
// requested table ID in the cache using LookupTableStats, and if the stats
// are not present in the cache, it looks them up in system.table_statistics
// using RefreshTableStats.
func (sc *TableStatisticsCache) GetTableStats(
	ctx context.Context, tableID sqlbase.ID,
) ([]*TableStatistic, error) {
	if stats, ok := sc.LookupTableStats(ctx, tableID); ok {
		return stats, nil
	}
	return sc.RefreshTableStats(ctx, tableID)
}

// GetHistogram is a convenience function to look up the statistic and
// histogram for the requested table ID and statistic ID in the cache using
// LookupHistogram, and if the histogram is not present in the cache, it looks
// it up in system.table_statistics using RefreshHistogram.
func (sc *TableStatisticsCache) GetHistogram(
	ctx context.Context, tableID sqlbase.ID, statisticID uint32,
) (*TableStatistic, error) {
	if stats, ok := sc.LookupHistogram(ctx, tableID, statisticID); ok {
		return stats, nil
	}
	return sc.RefreshHistogram(ctx, tableID, statisticID)
}

// InvalidateTableStats invalidates the cached statistics for the given table ID.
func (sc *TableStatisticsCache) InvalidateTableStats(ctx context.Context, tableID sqlbase.ID) {
	if log.V(2) {
		log.Infof(ctx, "evicting statistics for table %d", tableID)
	}
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.mu.cache.Del(tableID)
}

// InvalidateHistogram invalidates the cached statistic for the given table ID
// and statistic ID.
func (sc *TableStatisticsCache) InvalidateHistogram(
	ctx context.Context, tableID sqlbase.ID, statisticID uint32,
) {
	if log.V(2) {
		log.Infof(ctx, "evicting histogram for table %d, statistic %d", tableID, statisticID)
	}
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.mu.cache.Del(HistogramCacheKey{tableID, statisticID})
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

// parseStats converts the given datums to a TableStatistic object. It only includes
// a histogram in the output if the histogram in datums is non-null and
// includeHistogram is true.
func parseStats(datums tree.Datums, includeHistogram bool) (*TableStatistic, error) {
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
	tableStatistic := &TableStatistic{
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
// for the given table ID, excluding the histograms.
func (sc *TableStatisticsCache) getTableStatistics(
	ctx context.Context, tableID sqlbase.ID,
) ([]*TableStatistic, error) {
	const getTableStatisticsStmt = `
SELECT "tableID", "statisticID", name, "columnIDs", "createdAt", "rowCount", "distinctCount", "nullCount", NULL AS histogram
FROM system.table_statistics
WHERE "tableID" = $1
`
	var rows []tree.Datums
	if err := sc.ClientDB.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		var err error
		rows, err = sc.SQLExecutor.QueryRowsInTransaction(ctx, "get-table-statistics", txn, getTableStatisticsStmt, tableID)
		return err
	}); err != nil {
		return nil, err
	}

	var statsList []*TableStatistic
	for _, row := range rows {
		stats, err := parseStats(row, false /* includeHistogram */)
		if err != nil {
			return nil, err
		}
		statsList = append(statsList, stats)
	}

	return statsList, nil
}

// getHistogram retrieves the statistic and histogram in system.table_statistics
// for the given table ID and statistic ID.
func (sc *TableStatisticsCache) getHistogram(
	ctx context.Context, tableID sqlbase.ID, statisticID uint32,
) (*TableStatistic, error) {
	const getHistogramStmt = `
SELECT "tableID", "statisticID", name, "columnIDs", "createdAt", "rowCount", "distinctCount", "nullCount", histogram
FROM system.table_statistics
WHERE "tableID" = $1 AND "statisticID" = $2
`
	var row tree.Datums
	if err := sc.ClientDB.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		var err error
		row, err = sc.SQLExecutor.QueryRowInTransaction(ctx, "get-histogram", txn,
			getHistogramStmt, tableID, statisticID)
		return err
	}); err != nil {
		return nil, err
	}

	stats, err := parseStats(row, true /* includeHistogram */)
	if err != nil {
		return nil, err
	}

	return stats, nil
}
