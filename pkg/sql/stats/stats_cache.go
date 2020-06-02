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
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/resolver"
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
	"github.com/cockroachdb/errors"
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
	ClientDB    *kv.DB
	SQLExecutor sqlutil.InternalExecutor
	Codec       keys.SQLCodec
}

// The cache stores *cacheEntry objects. The fields are protected by the
// cache-wide mutex.
type cacheEntry struct {
	// If true, we are in the process of updating the statistics for this
	// table. Other callers can wait on the waitCond until this is false.
	mustWait bool
	waitCond sync.Cond

	stats []*TableStatistic

	// err is populated if the internal query to retrieve stats hit an error.
	err error
}

// NewTableStatisticsCache creates a new TableStatisticsCache that can hold
// statistics for <cacheSize> tables.
func NewTableStatisticsCache(
	cacheSize int,
	gw gossip.DeprecatedGossip,
	db *kv.DB,
	sqlExecutor sqlutil.InternalExecutor,
	codec keys.SQLCodec,
) *TableStatisticsCache {
	tableStatsCache := &TableStatisticsCache{
		ClientDB:    db,
		SQLExecutor: sqlExecutor,
		Codec:       codec,
	}
	tableStatsCache.mu.cache = cache.NewUnorderedCache(cache.Config{
		Policy:      cache.CacheLRU,
		ShouldEvict: func(s int, key, value interface{}) bool { return s > cacheSize },
	})
	// The stat cache requires redundant callbacks as it is using gossip to
	// signal the presence of new stats, not to actually propagate them.
	if g, ok := gw.Optional(47925); ok {
		g.RegisterCallback(
			gossip.MakePrefixPattern(gossip.KeyTableStatAddedPrefix),
			tableStatsCache.tableStatAddedGossipUpdate,
			gossip.Redundant,
		)
	}
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

	if found, stats, err := sc.lookupStatsLocked(ctx, tableID); found {
		return stats, err
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
) (found bool, _ []*TableStatistic, _ error) {
	eUntyped, ok := sc.mu.cache.Get(tableID)
	if !ok {
		return false, nil, nil
	}
	e := eUntyped.(*cacheEntry)

	if e.mustWait {
		// We are in the process of grabbing stats for this table. Wait until
		// that is complete, at which point e.stats will be populated.
		if log.V(1) {
			log.Infof(ctx, "waiting for statistics for table %d", tableID)
		}
		e.waitCond.Wait()
	} else {
		if log.V(2) {
			log.Infof(ctx, "statistics for table %d found in cache", tableID)
		}
	}
	return true, e.stats, e.err
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
	if log.V(1) {
		log.Infof(ctx, "reading statistics for table %d", tableID)
	}

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

		stats, err = sc.getTableStatsFromDB(ctx, tableID)
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

// InvalidateTableStats invalidates the cached statistics for the given table ID.
func (sc *TableStatisticsCache) InvalidateTableStats(ctx context.Context, tableID sqlbase.ID) {
	if log.V(1) {
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

// parseStats converts the given datums to a TableStatistic object. It might
// need to run a query to get user defined type metadata.
func parseStats(
	ctx context.Context, db *kv.DB, codec keys.SQLCodec, datums tree.Datums,
) (*TableStatistic, error) {
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
		typ := res.HistogramData.ColumnType
		// Hydrate the type in case any user defined types are present.
		// There are cases where typ is nil, so don't do anything if so.
		if typ != nil && typ.UserDefined() {
			// TODO (rohany): This should instead query a leased copy of the type.
			// TODO (rohany): If we are caching data about types here, then this
			//  cache needs to be invalidated as well when type metadata changes.
			// TODO (rohany): It might be better to store the type metadata used when
			//  collecting the stats in the HistogramData object itself, and avoid
			//  this query and caching/leasing problem.
			// The metadata accessed here is never older than the metadata used when
			// collecting the stats. Changes to types are backwards compatible across
			// versions, so using a newer version of the type metadata here is safe.
			err := db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
				typeLookup := func(id sqlbase.ID) (*tree.TypeName, sqlbase.TypeDescriptorInterface, error) {
					return resolver.ResolveTypeDescByID(ctx, txn, codec, id, tree.ObjectLookupFlags{})
				}
				name, typeDesc, err := typeLookup(sqlbase.ID(typ.StableTypeID()))
				if err != nil {
					return err
				}
				return typeDesc.HydrateTypeInfoWithName(typ, name, typeLookup)
			})
			if err != nil {
				return nil, err
			}
		}
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
		stats, err := parseStats(ctx, sc.ClientDB, sc.Codec, row)
		if err != nil {
			return nil, err
		}
		statsList = append(statsList, stats)
	}

	return statsList, nil
}
