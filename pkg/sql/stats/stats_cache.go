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
	"time"

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
// of columns. It mirrors the structure of the system.table_statistics table,
// excluding the histogram.
type TableStatistic struct {
	// The ID of the table.
	TableID sqlbase.ID

	// The ID for this statistic.  It need not be globally unique,
	// but must be unique for this table.
	StatisticID uint64

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

	// Indicates whether or not there is a histogram for this statistic.
	HasHistogram bool
}

func (s TableStatistic) String() string {
	return awsutil.Prettify(s)
}

// HistogramCacheKey is used as the key type for entries in the TableStatisticsCache
// which contain a histogram.
type HistogramCacheKey struct {
	TableID     sqlbase.ID
	StatisticID uint64
}

// A TableStatisticsCache contains two underlying LRU caches:
// (1) A cache of []*TableStatistic objects, keyed by table ID.
//     Each entry consists of all the statistics for different columns and
//     column groups for the given table.
// (2) A cache of *HistogramData objects, keyed by
//     HistogramCacheKey{table ID, statistic ID}.
type TableStatisticsCache struct {
	// NB: This can't be a RWMutex for lookup because UnorderedCache.Get
	// manipulates an internal LRU list.
	mu struct {
		syncutil.Mutex
		statsCache     *cache.UnorderedCache
		histogramCache *cache.UnorderedCache
	}
	ClientDB    *client.DB
	SQLExecutor sqlutil.InternalExecutor
}

// NewTableStatisticsCache creates a new TableStatisticsCache, with the
// size of the underlying statsCache set to statsCacheSize, and the size of
// the underlying histogramCache set to histogramCacheSize.
// Both underlying caches internally use a hash map, so lookups are cheap.
func NewTableStatisticsCache(
	statsCacheSize int, histogramCacheSize int, db *client.DB, sqlExecutor sqlutil.InternalExecutor,
) *TableStatisticsCache {
	tableStatsCache := &TableStatisticsCache{
		ClientDB:    db,
		SQLExecutor: sqlExecutor,
	}
	tableStatsCache.mu.statsCache = cache.NewUnorderedCache(cache.Config{
		Policy:      cache.CacheLRU,
		ShouldEvict: func(s int, key, value interface{}) bool { return s > statsCacheSize },
	})
	tableStatsCache.mu.histogramCache = cache.NewUnorderedCache(cache.Config{
		Policy:      cache.CacheLRU,
		ShouldEvict: func(s int, key, value interface{}) bool { return s > histogramCacheSize },
	})
	return tableStatsCache
}

// lookupTableStats returns the cached statistics of the given table ID.
// The second return value is true if the stats were found in the
// cache, and false otherwise.
func (sc *TableStatisticsCache) lookupTableStats(
	ctx context.Context, tableID sqlbase.ID,
) ([]*TableStatistic, bool) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if v, ok := sc.mu.statsCache.Get(tableID); ok {
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

// lookupHistogram returns the cached histogram of the given table ID and
// statistic ID. The second return value is true if the histogram was found
// in the cache, and false otherwise.
func (sc *TableStatisticsCache) lookupHistogram(
	ctx context.Context, tableID sqlbase.ID, statisticID uint64,
) (*HistogramData, bool) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if v, ok := sc.mu.histogramCache.Get(HistogramCacheKey{tableID, statisticID}); ok {
		if log.V(2) {
			log.Infof(ctx, "lookup histogram for table %d, statistic %d: %s", tableID, statisticID, v)
		}
		return v.(*HistogramData), true
	}
	if log.V(2) {
		log.Infof(ctx, "lookup histogram for table %d, statistic %d: not found", tableID, statisticID)
	}
	return nil, false
}

// refreshTableStats updates the cached statistics for the given table ID
// by issuing a query to system.table_statistics, and returns the statistics.
func (sc *TableStatisticsCache) refreshTableStats(
	ctx context.Context, tableID sqlbase.ID,
) ([]*TableStatistic, error) {
	tableStatistics, err := sc.getTableStatsFromDB(ctx, tableID)
	if err != nil {
		return nil, err
	}

	if log.V(2) {
		log.Infof(ctx, "updating statistics for table %d: %s", tableID, tableStatistics)
	}
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.mu.statsCache.Add(tableID, tableStatistics)
	return tableStatistics, nil
}

// refreshHistogram updates the cached histogram for the given table ID and
// statistic ID by issuing a query to system.table_statistics, and
// returns the histogram.
func (sc *TableStatisticsCache) refreshHistogram(
	ctx context.Context, tableID sqlbase.ID, statisticID uint64,
) (*HistogramData, error) {
	histogram, err := sc.getHistogramFromDB(ctx, tableID, statisticID)
	if err != nil {
		return nil, err
	}

	if log.V(2) {
		log.Infof(ctx, "updating histogram for table %d, statistic %d: %s", tableID, statisticID, histogram)
	}
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.mu.histogramCache.Add(HistogramCacheKey{tableID, statisticID}, histogram)
	return histogram, nil
}

// GetTableStats looks up statistics for the requested table ID in the cache,
// and if the stats are not present in the cache, it looks them up in
// system.table_statistics.
func (sc *TableStatisticsCache) GetTableStats(
	ctx context.Context, tableID sqlbase.ID,
) ([]*TableStatistic, error) {
	if stats, ok := sc.lookupTableStats(ctx, tableID); ok {
		return stats, nil
	}
	return sc.refreshTableStats(ctx, tableID)
}

// GetHistogram looks up the histogram for the requested table ID and
// statistic ID in the cache, and if the histogram is not present in the
// cache, it looks it up in system.table_statistics.
func (sc *TableStatisticsCache) GetHistogram(
	ctx context.Context, tableID sqlbase.ID, statisticID uint64,
) (*HistogramData, error) {
	if histogram, ok := sc.lookupHistogram(ctx, tableID, statisticID); ok {
		return histogram, nil
	}
	return sc.refreshHistogram(ctx, tableID, statisticID)
}

// InvalidateTableStats invalidates the cached statistics for the given table ID.
func (sc *TableStatisticsCache) InvalidateTableStats(ctx context.Context, tableID sqlbase.ID) {
	if log.V(2) {
		log.Infof(ctx, "evicting statistics for table %d", tableID)
	}
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.mu.statsCache.Del(tableID)
}

// InvalidateHistogram invalidates the cached histogram for the given table ID
// and statistic ID.
func (sc *TableStatisticsCache) InvalidateHistogram(
	ctx context.Context, tableID sqlbase.ID, statisticID uint64,
) {
	if log.V(2) {
		log.Infof(ctx, "evicting histogram for table %d, statistic %d", tableID, statisticID)
	}
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.mu.histogramCache.Del(HistogramCacheKey{tableID, statisticID})
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
	hasHistogramIndex
	statsLen
)

// parseStats converts the given datums to a TableStatistic object.
func parseStats(datums tree.Datums) (*TableStatistic, error) {
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
		{"histogram", hasHistogramIndex, types.Bool, false},
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
		StatisticID:   (uint64)(*datums[statisticsIDIndex].(*tree.DInt)),
		CreatedAt:     datums[createdAtIndex].(*tree.DTimestamp).Time,
		RowCount:      (uint64)(*datums[rowCountIndex].(*tree.DInt)),
		DistinctCount: (uint64)(*datums[distinctCountIndex].(*tree.DInt)),
		NullCount:     (uint64)(*datums[nullCountIndex].(*tree.DInt)),
		HasHistogram:  (bool)(*datums[hasHistogramIndex].(*tree.DBool)),
	}
	columnIDs := datums[columnIDsIndex].(*tree.DArray)
	tableStatistic.ColumnIDs = make([]sqlbase.ColumnID, len(columnIDs.Array))
	for i, d := range columnIDs.Array {
		tableStatistic.ColumnIDs[i] = sqlbase.ColumnID((int32)(*d.(*tree.DInt)))
	}
	if datums[nameIndex].ResolvedType() == types.String {
		tableStatistic.Name = string(*datums[nameIndex].(*tree.DString))
	}

	return tableStatistic, nil
}

// parseHistogram converts the given datums to a HistogramData object.
func parseHistogram(datums tree.Datums) (*HistogramData, error) {
	if datums == nil || datums.Len() == 0 {
		return nil, nil
	}

	// Validate the input length.
	if datums.Len() != 1 {
		return nil, errors.Errorf("%d values returned from table statistics lookup. Expected %d", datums.Len(), 1)
	}
	datum := datums[0]

	// Validate the input type.
	if datum.ResolvedType() != types.Bytes && datum.ResolvedType() != types.Null {
		return nil, errors.Errorf("histogram returned from table statistics lookup has type %s. Expected %s",
			datum.ResolvedType(), types.Bytes)
	}

	// Extract datum value.
	if datum.ResolvedType() == types.Bytes {
		histogram := &HistogramData{}
		if err := protoutil.Unmarshal([]byte(*datum.(*tree.DBytes)), histogram); err != nil {
			return nil, err
		}
		return histogram, nil
	}

	return nil, nil
}

// getTableStatsFromDB retrieves the statistics in system.table_statistics
// for the given table ID.
func (sc *TableStatisticsCache) getTableStatsFromDB(
	ctx context.Context, tableID sqlbase.ID,
) ([]*TableStatistic, error) {
	const getTableStatisticsStmt = `
SELECT "tableID", "statisticID", name, "columnIDs", "createdAt", "rowCount", "distinctCount", "nullCount", histogram IS NOT NULL
FROM system.public.table_statistics
WHERE "tableID" = $1
`
	var rows []tree.Datums
	if err := sc.ClientDB.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		var err error
		rows, _ /* cols */, err = sc.SQLExecutor.QueryRowsInTransaction(
			ctx, "get-table-statistics", txn, getTableStatisticsStmt, tableID,
		)
		return err
	}); err != nil {
		return nil, err
	}

	var statsList []*TableStatistic
	for _, row := range rows {
		stats, err := parseStats(row)
		if err != nil {
			return nil, err
		}
		statsList = append(statsList, stats)
	}

	return statsList, nil
}

// getHistogramFromDB retrieves the histogram in system.table_statistics
// for the given table ID and statistic ID. Returns an error if the histogram
// does not exist.
func (sc *TableStatisticsCache) getHistogramFromDB(
	ctx context.Context, tableID sqlbase.ID, statisticID uint64,
) (*HistogramData, error) {
	const getHistogramStmt = `
SELECT histogram
FROM system.public.table_statistics
WHERE "tableID" = $1 AND "statisticID" = $2
`
	var row tree.Datums
	if err := sc.ClientDB.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		var err error
		row, err = sc.SQLExecutor.QueryRowInTransaction(ctx, "get-histogram", txn, getHistogramStmt, tableID, statisticID)
		return err
	}); err != nil {
		return nil, err
	}

	histogram, err := parseHistogram(row)
	if err != nil {
		return nil, err
	}

	if histogram == nil {
		return nil, errors.Errorf("histogram not found for table %d, statistic %d", tableID, statisticID)
	}

	return histogram, nil
}
