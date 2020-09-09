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
	"sync"

	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/cache"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/pkg/errors"
)

// A TableStatistic object holds a statistic for a particular column or group
// of columns.
type TableStatistic struct {
	TableStatisticProto

	// Histogram is the decoded histogram data.
	Histogram []cat.HistogramBucket
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
		// Used for testing; keeps track of how many times we actually read stats
		// from the system table.
		numInternalQueries int64
	}
	Gossip      *gossip.Gossip
	ClientDB    *kv.DB
	SQLExecutor sqlutil.InternalExecutor
}

// The cache stores *cacheEntry objects. The fields are protected by the
// cache-wide mutex.
type cacheEntry struct {
	// If mustWait is true, we do not have any statistics for this table and we
	// are in the process of fetching the stats from the database. Other callers
	// can wait on the waitCond until this is false.
	mustWait bool
	waitCond sync.Cond

	// If refreshing is true, the current statistics for this table are stale,
	// and we are in the process of fetching the updated stats from the database.
	// In the mean time, other callers can use the stale stats and do not need to
	// wait.
	//
	// If a goroutine tries to perform a refresh when a refresh is already
	// in progress, it will see that refreshing=true and will set the
	// mustRefreshAgain flag to true before returning. When the original
	// goroutine that was performing the refresh returns from the database and
	// sees that mustRefreshAgain=true, it will trigger another refresh.
	refreshing       bool
	mustRefreshAgain bool

	stats []*TableStatistic

	// err is populated if the internal query to retrieve stats hit an error.
	err error
}

// NewTableStatisticsCache creates a new TableStatisticsCache that can hold
// statistics for <cacheSize> tables.
func NewTableStatisticsCache(
	cacheSize int, g *gossip.Gossip, db *kv.DB, sqlExecutor sqlutil.InternalExecutor,
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
	sc.RefreshTableStats(context.Background(), sqlbase.ID(tableID))
}

// GetTableStats looks up statistics for the requested table ID in the cache,
// and if the stats are not present in the cache, it looks them up in
// system.table_statistics.
//
// The statistics are ordered by their CreatedAt time (newest-to-oldest).
func (sc *TableStatisticsCache) GetTableStats(
	ctx context.Context, tableID sqlbase.ID,
) ([]*TableStatistic, error) {
	if sqlbase.IsReservedID(tableID) {
		// Don't try to get statistics for system tables (most importantly,
		// for table_statistics itself).
		return nil, nil
	}
	if sqlbase.IsVirtualTable(tableID) {
		// Don't try to get statistics for virtual tables.
		return nil, nil
	}

	sc.mu.Lock()
	defer sc.mu.Unlock()

	if found, e := sc.lookupStatsLocked(ctx, tableID); found {
		return e.stats, e.err
	}

	return sc.addCacheEntryLocked(ctx, tableID)
}

// lookupStatsLocked retrieves any existing stats for the given table.
//
// If another goroutine is in the process of retrieving the same stats, this
// method waits until that completes.
//
// Assumes that the caller holds sc.mu. Note that the mutex can be unlocked and
// locked again if we need to wait (this can only happen when found=true).
func (sc *TableStatisticsCache) lookupStatsLocked(
	ctx context.Context, tableID sqlbase.ID,
) (found bool, e *cacheEntry) {
	eUntyped, ok := sc.mu.cache.Get(tableID)
	if !ok {
		return false, nil
	}
	e = eUntyped.(*cacheEntry)

	if e.mustWait {
		// We are in the process of grabbing stats for this table. Wait until
		// that is complete, at which point e.stats will be populated.
		log.VEventf(ctx, 1, "waiting for statistics for table %d", tableID)
		e.waitCond.Wait()
		log.VEventf(ctx, 1, "finished waiting for statistics for table %d", tableID)
	} else {
		// This is the expected "fast" path; don't emit an event.
		if log.V(2) {
			log.Infof(ctx, "statistics for table %d found in cache", tableID)
		}
	}
	return true, e
}

// addCacheEntryLocked creates a new cache entry and retrieves table statistics
// from the database. It does this in a way so that the other goroutines that
// need the same stats can wait on us:
//  - an cache entry with wait=true is created;
//  - mutex is unlocked;
//  - stats are retrieved from database:
//  - mutex is locked again and the entry is updated.
//
func (sc *TableStatisticsCache) addCacheEntryLocked(
	ctx context.Context, tableID sqlbase.ID,
) (stats []*TableStatistic, err error) {
	// Add a cache entry that other queries can find and wait on until we have the
	// stats.
	e := &cacheEntry{
		mustWait: true,
		waitCond: sync.Cond{L: &sc.mu},
	}
	sc.mu.cache.Add(tableID, e)
	sc.mu.numInternalQueries++

	func() {
		sc.mu.Unlock()
		defer sc.mu.Lock()

		log.VEventf(ctx, 1, "reading statistics for table %d", tableID)
		stats, err = sc.getTableStatsFromDB(ctx, tableID)
		log.VEventf(ctx, 1, "finished reading statistics for table %d", tableID)
	}()

	e.mustWait = false
	e.stats, e.err = stats, err

	// Wake up any other callers that are waiting on these stats.
	e.waitCond.Broadcast()

	if err != nil {
		// Don't keep the cache entry around, so that we retry the query.
		sc.mu.cache.Del(tableID)
	}

	return stats, err
}

// refreshCacheEntry retrieves table statistics from the database and updates
// an existing cache entry. It does this in a way so that the other goroutines
// can continue using the stale stats from the existing entry until the new
// stats are added:
//  - the existing cache entry is retrieved;
//  - mutex is unlocked;
//  - stats are retrieved from database:
//  - mutex is locked again and the entry is updated.
//
func (sc *TableStatisticsCache) refreshCacheEntry(ctx context.Context, tableID sqlbase.ID) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	// If the stats don't already exist in the cache, don't bother performing
	// the refresh. If e.err is not nil, the stats are in the process of being
	// removed from the cache (see addCacheEntryLocked), so don't refresh in this
	// case either.
	found, e := sc.lookupStatsLocked(ctx, tableID)
	if !found || e.err != nil {
		return
	}

	// Don't perform a refresh if a refresh is already in progress, but let that
	// goroutine know it needs to refresh again.
	if e.refreshing {
		e.mustRefreshAgain = true
		return
	}
	e.refreshing = true

	var stats []*TableStatistic
	var err error
	for {
		func() {
			sc.mu.numInternalQueries++
			sc.mu.Unlock()
			defer sc.mu.Lock()

			log.VEventf(ctx, 1, "refreshing statistics for table %d", tableID)
			stats, err = sc.getTableStatsFromDB(ctx, tableID)
			log.VEventf(ctx, 1, "done refreshing statistics for table %d", tableID)
		}()
		if !e.mustRefreshAgain {
			break
		}
		e.mustRefreshAgain = false
	}

	e.stats, e.err = stats, err
	e.refreshing = false

	if err != nil {
		// Don't keep the cache entry around, so that we retry the query.
		sc.mu.cache.Del(tableID)
	}
}

// RefreshTableStats refreshes the cached statistics for the given table ID
// by fetching the new stats from the database.
func (sc *TableStatisticsCache) RefreshTableStats(ctx context.Context, tableID sqlbase.ID) {
	log.VEventf(ctx, 1, "refreshing statistics for table %d", tableID)
	ctx, span := tracing.ForkCtxSpan(ctx, "refresh-table-stats")
	// Perform an asynchronous refresh of the cache.
	go func() {
		defer tracing.FinishSpan(span)
		sc.refreshCacheEntry(ctx, tableID)
	}()
}

// InvalidateTableStats invalidates the cached statistics for the given table ID.
//
// Note that RefreshTableStats should normally be used instead of this function.
// This function is used only when we want to guarantee that the next query
// uses updated stats.
func (sc *TableStatisticsCache) InvalidateTableStats(ctx context.Context, tableID sqlbase.ID) {
	log.VEventf(ctx, 1, "evicting statistics for table %d", tableID)
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
		expectedType *types.T
		nullable     bool
	}{
		{"tableID", tableIDIndex, types.Int, false},
		{"statisticsID", statisticsIDIndex, types.Int, false},
		{"name", nameIndex, types.String, true},
		{"columnIDs", columnIDsIndex, types.IntArray, false},
		{"createdAt", createdAtIndex, types.Timestamp, false},
		{"rowCount", rowCountIndex, types.Int, false},
		{"distinctCount", distinctCountIndex, types.Int, false},
		{"nullCount", nullCountIndex, types.Int, false},
		{"histogram", histogramIndex, types.Bytes, true},
	}
	for _, v := range expectedTypes {
		if !datums[v.fieldIndex].ResolvedType().Equivalent(v.expectedType) &&
			(!v.nullable || datums[v.fieldIndex].ResolvedType().Family() != types.UnknownFamily) {
			return nil, errors.Errorf("%s returned from table statistics lookup has type %s. Expected %s",
				v.fieldName, datums[v.fieldIndex].ResolvedType(), v.expectedType)
		}
	}

	// Extract datum values.
	res := &TableStatistic{
		TableStatisticProto: TableStatisticProto{
			TableID:       sqlbase.ID((int32)(*datums[tableIDIndex].(*tree.DInt))),
			StatisticID:   (uint64)(*datums[statisticsIDIndex].(*tree.DInt)),
			CreatedAt:     datums[createdAtIndex].(*tree.DTimestamp).Time,
			RowCount:      (uint64)(*datums[rowCountIndex].(*tree.DInt)),
			DistinctCount: (uint64)(*datums[distinctCountIndex].(*tree.DInt)),
			NullCount:     (uint64)(*datums[nullCountIndex].(*tree.DInt)),
		},
	}
	columnIDs := datums[columnIDsIndex].(*tree.DArray)
	res.ColumnIDs = make([]sqlbase.ColumnID, len(columnIDs.Array))
	for i, d := range columnIDs.Array {
		res.ColumnIDs[i] = sqlbase.ColumnID((int32)(*d.(*tree.DInt)))
	}
	if datums[nameIndex] != tree.DNull {
		res.Name = string(*datums[nameIndex].(*tree.DString))
	}
	if datums[histogramIndex] != tree.DNull {
		res.HistogramData = &HistogramData{}
		if err := protoutil.Unmarshal(
			[]byte(*datums[histogramIndex].(*tree.DBytes)),
			res.HistogramData,
		); err != nil {
			return nil, err
		}

		// Decode the histogram data so that it's usable by the opt catalog.
		res.Histogram = make([]cat.HistogramBucket, len(res.HistogramData.Buckets))
		typ := &res.HistogramData.ColumnType
		var a sqlbase.DatumAlloc
		for i := range res.Histogram {
			bucket := &res.HistogramData.Buckets[i]
			datum, _, err := sqlbase.DecodeTableKey(&a, typ, bucket.UpperBound, encoding.Ascending)
			if err != nil {
				return nil, err
			}
			res.Histogram[i] = cat.HistogramBucket{
				NumEq:         float64(bucket.NumEq),
				NumRange:      float64(bucket.NumRange),
				DistinctRange: bucket.DistinctRange,
				UpperBound:    datum,
			}
		}
	}

	return res, nil
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
	rows, err := sc.SQLExecutor.Query(
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
