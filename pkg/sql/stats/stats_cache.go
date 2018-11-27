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
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
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

	// Histogram (if available)
	// TODO(radu): perhaps store the histogram in a more convenient format (Datums
	// instead of bytes).
	Histogram *HistogramData
}

func (s TableStatistic) String() string {
	return awsutil.Prettify(s)
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
		cache *cache.UnorderedCache
	}
	Gossip      *gossip.Gossip
	ClientDB    *client.DB
	SQLExecutor sqlutil.InternalExecutor
}

// NewTableStatisticsCache creates a new TableStatisticsCache that can hold
// statistics for <cacheSize> tables.
func NewTableStatisticsCache(
	cacheSize int, g *gossip.Gossip, db *client.DB, sqlExecutor sqlutil.InternalExecutor,
) *TableStatisticsCache {
	tableStatsCache := &TableStatisticsCache{
		Gossip:      g,
		ClientDB:    db,
		SQLExecutor: sqlExecutor,
	}
	tableStatsCache.mu.cache = cache.NewUnorderedCache(cache.Config{
		Policy:      cache.CacheLRU,
		ShouldEvict: func(s int, key, value interface{}) bool { return s > cacheSize },
	})
	// The stat cache requires redundant callbacks as it is using gossip to
	// signal the presence of new stats, not to actually propagate them.
	g.RegisterCallback(
		gossip.MakePrefixPattern(gossip.KeyTableStatAddedPrefix),
		tableStatsCache.tableStatAddedGossipUpdate,
		gossip.Redundant,
	)
	return tableStatsCache
}

// tableStatAddedGossipUpdate is the gossip callback that fires when a new
// statistic is available for a table.
func (sc *TableStatisticsCache) tableStatAddedGossipUpdate(key string, value roachpb.Value) {
	tableID, err := gossip.TableIDFromTableStatAddedKey(key)
	if err != nil {
		log.Errorf(context.Background(), "tableStatAddedGossipUpdate(%s) error: %v", key, err)
		return
	}
	sc.InvalidateTableStats(context.Background(), sqlbase.ID(tableID))
}

// lookupTableStats returns the cached statistics of the given table ID.
// The second return value is true if the stats were found in the
// cache, and false otherwise.
//
// The statistics are ordered by their CreatedAt time (newest-to-oldest).
func (sc *TableStatisticsCache) lookupTableStats(
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
	sc.mu.cache.Add(tableID, tableStatistics)
	return tableStatistics, nil
}

// GetTableStats looks up statistics for the requested table ID in the cache,
// and if the stats are not present in the cache, it looks them up in
// system.table_statistics.
func (sc *TableStatisticsCache) GetTableStats(
	ctx context.Context, tableID sqlbase.ID,
) ([]*TableStatistic, error) {
	if sqlbase.IsReservedID(tableID) {
		// Don't try to get statistics for system tables (most importantly,
		// for table_statistics itself).
		return nil, nil
	}
	if tableID == keys.VirtualDescriptorID {
		// Don't try to get statistics for virtual tables.
		return nil, nil
	}

	if stats, ok := sc.lookupTableStats(ctx, tableID); ok {
		return stats, nil
	}
	return sc.refreshTableStats(ctx, tableID)
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
		{"histogram", histogramIndex, types.Bytes, true},
	}
	for _, v := range expectedTypes {
		if datums[v.fieldIndex].ResolvedType() != v.expectedType &&
			(!v.nullable || datums[v.fieldIndex].ResolvedType() != types.Unknown) {
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
	}
	columnIDs := datums[columnIDsIndex].(*tree.DArray)
	tableStatistic.ColumnIDs = make([]sqlbase.ColumnID, len(columnIDs.Array))
	for i, d := range columnIDs.Array {
		tableStatistic.ColumnIDs[i] = sqlbase.ColumnID((int32)(*d.(*tree.DInt)))
	}
	if datums[nameIndex] != tree.DNull {
		tableStatistic.Name = string(*datums[nameIndex].(*tree.DString))
	}
	if datums[histogramIndex] != tree.DNull {
		tableStatistic.Histogram = &HistogramData{}
		if err := protoutil.Unmarshal(
			[]byte(*datums[histogramIndex].(*tree.DBytes)),
			tableStatistic.Histogram,
		); err != nil {
			return nil, err
		}
	}

	return tableStatistic, nil
}

// getTableStatsFromDB retrieves the statistics in system.table_statistics
// for the given table ID.
func (sc *TableStatisticsCache) getTableStatsFromDB(
	ctx context.Context, tableID sqlbase.ID,
) ([]*TableStatistic, error) {
	const getTableStatisticsStmt = `
SELECT
  "tableID",
	"statisticID",
	name,
	"columnIDs",
	"createdAt",
	"rowCount",
	"distinctCount",
	"nullCount",
	histogram
FROM system.table_statistics
WHERE "tableID" = $1
ORDER BY "createdAt" DESC
`
	rows, _ /* cols */, err := sc.SQLExecutor.Query(
		ctx, "get-table-statistics", nil /* txn */, getTableStatisticsStmt, tableID,
	)
	if err != nil {
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
