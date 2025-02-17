// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package stats

import (
	"context"
	"fmt"
	"math"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catenumpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/cache"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
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
//
//	Each entry consists of all the statistics for different columns and
//	column groups for the given table.
//
// (2) A cache of *HistogramData objects, keyed by
//
//	HistogramCacheKey{table ID, statistic ID}.
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
	db       descs.DB
	settings *cluster.Settings
	stopper  *stop.Stopper

	// Used when decoding KV from the range feed.
	datumAlloc tree.DatumAlloc

	// generation is incremented any time the statistics cache is
	// modified.
	generation atomic.Int64
}

// The cache stores *cacheEntry objects. The fields are protected by the
// cache-wide mutex.
type cacheEntry struct {
	// If mustWait is true, we do not have any statistics for this table and we
	// are in the process of fetching the stats from the database. Other callers
	// can wait on the waitCond until this is false.
	mustWait bool
	waitCond sync.Cond

	// lastRefreshTimestamp is the timestamp at which the last refresh was
	// requested; note that the refresh may be ongoing.
	// It is zero for entries that were not refreshed since they were added to the
	// cache.
	lastRefreshTimestamp hlc.Timestamp

	// If refreshing is true, we are in the process of fetching the updated stats
	// from the database.  In the mean time, other callers can use the stale stats
	// and do not need to wait.
	//
	// If a goroutine tries to perform a refresh when a refresh is already in
	// progress, it will see that refreshing=true and will just update
	// lastRefreshTimestamp before returning. When the original goroutine that was
	// performing the refresh returns from the database and sees that the
	// timestamp was moved, it will trigger another refresh.
	refreshing bool

	// forecast is true if stats could contain forecasts.
	forecast bool

	// userDefinedTypes holds the hydrated user-defined types used in
	// histograms. A change to one of these types requires evicting the cacheEntry
	// so that we can re-hydrate them.
	userDefinedTypes map[descpb.ColumnID]*types.T

	stats []*TableStatistic

	// err is populated if the internal query to retrieve stats hit an error.
	err error
}

// NewTableStatisticsCache creates a new TableStatisticsCache that can hold
// statistics for <cacheSize> tables.
func NewTableStatisticsCache(
	cacheSize int, settings *cluster.Settings, db descs.DB, stopper *stop.Stopper,
) *TableStatisticsCache {
	tableStatsCache := &TableStatisticsCache{
		db:       db,
		settings: settings,
		stopper:  stopper,
	}
	tableStatsCache.mu.cache = cache.NewUnorderedCache(cache.Config{
		Policy:      cache.CacheLRU,
		ShouldEvict: func(s int, key, value interface{}) bool { return s > cacheSize },
	})
	return tableStatsCache
}

// Clear removes all entries from the stats cache.
func (sc *TableStatisticsCache) Clear() {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.mu.cache.Clear()
	defer sc.generation.Add(1)
}

// GetGeneration returns the current generation, which will change if any
// modifications happen to the cache.
func (sc *TableStatisticsCache) GetGeneration() int64 {
	return sc.generation.Load()
}

// Start begins watching for updates in the stats table.
func (sc *TableStatisticsCache) Start(
	ctx context.Context, codec keys.SQLCodec, rangeFeedFactory *rangefeed.Factory,
) error {
	// Set up a range feed to watch for updates to system.table_statistics.

	statsTablePrefix := codec.TablePrefix(keys.TableStatisticsTableID)
	statsTableSpan := roachpb.Span{
		Key:    statsTablePrefix,
		EndKey: statsTablePrefix.PrefixEnd(),
	}

	var lastTableID descpb.ID
	var lastTS hlc.Timestamp

	handleEvent := func(ctx context.Context, kv *kvpb.RangeFeedValue) {
		tableID, err := decodeTableStatisticsKV(codec, kv, &sc.datumAlloc)
		if err != nil {
			log.Warningf(ctx, "failed to decode table statistics row %v: %v", kv.Key, err)
			return
		}
		ts := kv.Value.Timestamp
		// A statistics collection inserts multiple rows in one transaction. We
		// don't want to call refreshTableStats for each row since it has
		// non-trivial overhead.
		if tableID == lastTableID && ts == lastTS {
			return
		}
		lastTableID = tableID
		lastTS = ts
		sc.refreshTableStats(ctx, tableID, ts)
	}

	// Notes:
	//  - the range feed automatically stops on server shutdown, we don't need to
	//    call Close() ourselves.
	//  - an error here only happens if the server is already shutting down; we
	//    can safely ignore it.
	_, err := rangeFeedFactory.RangeFeed(
		ctx,
		"table-stats-cache",
		[]roachpb.Span{statsTableSpan},
		sc.db.KV().Clock().Now(),
		handleEvent,
		rangefeed.WithSystemTablePriority(),
	)
	return err
}

// decodeTableStatisticsKV decodes the table ID from a range feed event on
// system.table_statistics.
func decodeTableStatisticsKV(
	codec keys.SQLCodec, kv *kvpb.RangeFeedValue, da *tree.DatumAlloc,
) (tableDesc descpb.ID, err error) {
	// The primary key of table_statistics is (tableID INT, statisticID INT).
	types := []*types.T{types.Int, types.Int}
	dirs := []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC, catenumpb.IndexColumn_ASC}
	keyVals := make([]rowenc.EncDatum, 2)
	if _, err := rowenc.DecodeIndexKey(codec, keyVals, dirs, kv.Key); err != nil {
		return 0, err
	}

	if err := keyVals[0].EnsureDecoded(types[0], da); err != nil {
		return 0, err
	}

	tableID, ok := keyVals[0].Datum.(*tree.DInt)
	if !ok {
		return 0, errors.New("invalid tableID value")
	}
	return descpb.ID(uint32(*tableID)), nil
}

// GetTableStats looks up statistics for the requested table in the cache,
// and if the stats are not present in the cache, it looks them up in
// system.table_statistics.
//
// typeResolver argument is optional and will be used to hydrate all
// user-defined types. If the resolver is not provided, then the latest
// committed type metadata will be used.
//
// The function returns an error if we could not query the system table. It
// silently ignores any statistics that can't be decoded (e.g. because
// user-defined types don't exit).
//
// The statistics are ordered by their CreatedAt time (newest-to-oldest).
func (sc *TableStatisticsCache) GetTableStats(
	ctx context.Context, table catalog.TableDescriptor, typeResolver *descs.DistSQLTypeResolver,
) (stats []*TableStatistic, err error) {
	if !statsUsageAllowed(table, sc.settings) {
		return nil, nil
	}
	forecast := forecastAllowed(table, sc.settings)
	return sc.getTableStatsFromCache(
		ctx, table.GetID(), &forecast, table.UserDefinedTypeColumns(), typeResolver,
	)
}

// DisallowedOnSystemTable returns true if this tableID belongs to a special
// system table on which we want to disallow stats collection and stats usage.
func DisallowedOnSystemTable(tableID descpb.ID) bool {
	switch tableID {
	// Disable stats on system.table_statistics because it can lead to deadlocks
	// around the stats cache (which issues an internal query in
	// getTableStatsFromDB to fetch statistics for a single table, and that
	// query in turn will want table stats on system.table_statistics to come up
	// with a plan).
	//
	// Disable stats on system.lease since it's known to cause hangs.
	// TODO(yuzefovich): check whether it's still a problem.
	//
	// Disable stats on system.scheduled_jobs because the table is mutated too
	// frequently and would trigger too many stats collections. The potential
	// benefit is not worth the potential performance hit.
	// TODO(yuzefovich): re-evaluate this assumption. Perhaps we could at
	// least enable manual collection on this table.
	// Disable stats on system.span_configurations since we've seen excessively
	// many collections on it in some cases, and the stats are unlikely to
	// provide any benefit on this table.
	case keys.TableStatisticsTableID, keys.LeaseTableID, keys.ScheduledJobsTableID, keys.SpanConfigurationsTableID:
		return true
	}
	return false
}

// statsUsageAllowed returns true if statistics on `table` are allowed to be
// used by the query optimizer.
func statsUsageAllowed(table catalog.TableDescriptor, clusterSettings *cluster.Settings) bool {
	if catalog.IsSystemDescriptor(table) {
		if DisallowedOnSystemTable(table.GetID()) {
			return false
		}
		// Return whether the optimizer is allowed to use stats on system tables.
		return UseStatisticsOnSystemTables.Get(&clusterSettings.SV)
	}
	return tableTypeCanHaveStats(table)
}

// autostatsCollectionAllowed returns true if statistics are allowed to be
// automatically collected on the table.
func autostatsCollectionAllowed(
	table catalog.TableDescriptor, clusterSettings *cluster.Settings,
) bool {
	if catalog.IsSystemDescriptor(table) {
		if DisallowedOnSystemTable(table.GetID()) {
			return false
		}
		// Return whether autostats collection is allowed on system tables,
		// according to the cluster settings.
		return AutomaticStatisticsOnSystemTables.Get(&clusterSettings.SV)
	}
	return tableTypeCanHaveStats(table)
}

// tableTypeCanHaveStats returns true if manual collection of statistics on the
// table type via CREATE STATISTICS or ANALYZE is allowed. Note that specific
// system tables may have stats collection disabled in create_stats.go. This
// function just indicates if the type of table may have stats manually
// collected.
func tableTypeCanHaveStats(table catalog.TableDescriptor) bool {
	if table.IsVirtualTable() {
		// Don't try to get statistics for virtual tables.
		return false
	}
	if table.IsView() {
		// Don't try to get statistics for views.
		return false
	}
	return true
}

// forecastAllowed returns true if statistics forecasting is allowed for the
// given table.
func forecastAllowed(table catalog.TableDescriptor, clusterSettings *cluster.Settings) bool {
	if enabled, ok := table.ForecastStatsEnabled(); ok {
		return enabled
	}
	return UseStatisticsForecasts.Get(&clusterSettings.SV)
}

// getTableStatsFromCache is like GetTableStats but assumes that the table ID
// is safe to fetch statistics for: non-system, non-virtual, non-view, etc.
func (sc *TableStatisticsCache) getTableStatsFromCache(
	ctx context.Context,
	tableID descpb.ID,
	forecast *bool,
	udtCols []catalog.Column,
	typeResolver *descs.DistSQLTypeResolver,
) ([]*TableStatistic, error) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	if found, e := sc.lookupStatsLocked(ctx, tableID, false /* stealthy */); found {
		if e.isStale(forecast, udtCols) {
			// Evict the cache entry and build it again.
			sc.mu.cache.Del(tableID)
		} else {
			return e.stats, e.err
		}
	}

	return sc.addCacheEntryLocked(ctx, tableID, forecast != nil && *forecast, typeResolver)
}

// isStale checks whether we need to evict and re-load the cache entry.
func (e *cacheEntry) isStale(forecast *bool, udtCols []catalog.Column) bool {
	// Check whether forecast settings have changed.
	if forecast != nil && e.forecast != *forecast {
		return true
	}
	// Check whether user-defined types have changed (this is similar to
	// UserDefinedTypeColsHaveSameVersion).
	for _, col := range udtCols {
		colType := col.GetType()
		if histType, ok := e.userDefinedTypes[col.GetID()]; ok {
			if histType.Oid() != colType.Oid() {
				// This should never be true, but if it is, we'll catch it in
				// optTableStat.init and ignore the statistic. For now just skip it.
				continue
			}
			if histType.TypeMeta.Version != colType.TypeMeta.Version {
				return true
			}
		}
	}
	return false
}

// lookupStatsLocked retrieves any existing stats for the given table.
//
// If another goroutine is in the process of retrieving the same stats, this
// method waits until that completes.
//
// Assumes that the caller holds sc.mu. Note that the mutex can be unlocked and
// locked again if we need to wait (this can only happen when found=true).
//
// If stealthy=true, this is not considered an access with respect to the cache
// eviction policy.
func (sc *TableStatisticsCache) lookupStatsLocked(
	ctx context.Context, tableID descpb.ID, stealthy bool,
) (found bool, e *cacheEntry) {
	var eUntyped interface{}
	var ok bool

	if !stealthy {
		eUntyped, ok = sc.mu.cache.Get(tableID)
	} else {
		eUntyped, ok = sc.mu.cache.StealthyGet(tableID)
	}
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
//   - an cache entry with wait=true is created;
//   - mutex is unlocked;
//   - stats are retrieved from database:
//   - mutex is locked again and the entry is updated.
func (sc *TableStatisticsCache) addCacheEntryLocked(
	ctx context.Context, tableID descpb.ID, forecast bool, typeResolver *descs.DistSQLTypeResolver,
) (stats []*TableStatistic, err error) {
	defer sc.generation.Add(1)
	// Add a cache entry that other queries can find and wait on until we have the
	// stats.
	e := &cacheEntry{
		mustWait: true,
		waitCond: sync.Cond{L: &sc.mu},
	}
	sc.mu.cache.Add(tableID, e)
	sc.mu.numInternalQueries++

	var udts map[descpb.ColumnID]*types.T
	func() {
		sc.mu.Unlock()
		defer sc.mu.Lock()
		log.VEventf(ctx, 1, "reading statistics for table %d", tableID)
		stats, udts, err = sc.getTableStatsFromDB(ctx, tableID, forecast, sc.settings, typeResolver)
		log.VEventf(ctx, 1, "finished reading statistics for table %d", tableID)
	}()

	e.mustWait = false
	e.forecast, e.userDefinedTypes, e.stats, e.err = forecast, udts, stats, err

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
//   - the existing cache entry is retrieved;
//   - mutex is unlocked;
//   - stats are retrieved from database:
//   - mutex is locked again and the entry is updated.
func (sc *TableStatisticsCache) refreshCacheEntry(
	ctx context.Context, tableID descpb.ID, ts hlc.Timestamp,
) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	defer sc.generation.Add(1)

	// If the stats don't already exist in the cache, don't bother performing
	// the refresh. If e.err is not nil, the stats are in the process of being
	// removed from the cache (see addCacheEntryLocked), so don't refresh in this
	// case either.
	found, e := sc.lookupStatsLocked(ctx, tableID, true /* stealthy */)
	if !found || e.err != nil {
		return
	}
	if ts.LessEq(e.lastRefreshTimestamp) {
		// We already refreshed at this (or a later) timestamp.
		return
	}
	e.lastRefreshTimestamp = ts

	// Don't perform a refresh if a refresh is already in progress; that goroutine
	// will know it needs to refresh again because we changed the
	// lastRefreshTimestamp.
	if e.refreshing {
		return
	}
	e.refreshing = true

	forecast := e.forecast
	var stats []*TableStatistic
	var udts map[descpb.ColumnID]*types.T
	var err error
	for {
		func() {
			sc.mu.numInternalQueries++
			sc.mu.Unlock()
			defer sc.mu.Lock()

			log.VEventf(ctx, 1, "refreshing statistics for table %d", tableID)
			// TODO(radu): pass the timestamp and use AS OF SYSTEM TIME.
			stats, udts, err = sc.getTableStatsFromDB(ctx, tableID, forecast, sc.settings, nil /* typeResolver */)
			log.VEventf(ctx, 1, "done refreshing statistics for table %d", tableID)
		}()
		if e.lastRefreshTimestamp.Equal(ts) {
			break
		}
		// The timestamp has changed; another refresh was requested.
		ts = e.lastRefreshTimestamp
	}

	e.userDefinedTypes, e.stats, e.err = udts, stats, err
	e.refreshing = false

	if err != nil {
		// Don't keep the cache entry around, so that we retry the query.
		sc.mu.cache.Del(tableID)
	}
}

// refreshTableStats refreshes the cached statistics for the given table ID by
// fetching the new stats from the database.
func (sc *TableStatisticsCache) refreshTableStats(
	ctx context.Context, tableID descpb.ID, ts hlc.Timestamp,
) {
	log.VEventf(ctx, 1, "refreshing statistics for table %d", tableID)
	// Perform an asynchronous refresh of the cache. An error is returned only
	// on the server shutdown at which point we don't care about the refresh.
	_ = sc.stopper.RunAsyncTask(ctx, "refresh-table-stats", func(ctx context.Context) {
		sc.refreshCacheEntry(ctx, tableID, ts)
	})
}

// InvalidateTableStats invalidates the cached statistics for the given table ID.
func (sc *TableStatisticsCache) InvalidateTableStats(ctx context.Context, tableID descpb.ID) {
	log.VEventf(ctx, 1, "evicting statistics for table %d", tableID)
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.mu.cache.Del(tableID)
	defer sc.generation.Add(1)
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
	avgSizeIndex
	partialPredicateIndex
	histogramIndex
	fullStatisticsIdIndex
	statsLen
)

// NewTableStatisticProto converts a row of datums from system.table_statistics
// into a TableStatisticsProto. Note that any user-defined types in the
// HistogramData will be unresolved.
func NewTableStatisticProto(datums tree.Datums) (*TableStatisticProto, error) {
	if datums == nil || datums.Len() == 0 {
		return nil, nil
	}

	hgIndex := histogramIndex
	numStats := statsLen
	// Validate the input length.
	if datums.Len() != numStats {
		return nil, errors.Errorf("%d values returned from table statistics lookup. Expected %d", datums.Len(), numStats)
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
		{"avgSize", avgSizeIndex, types.Int, false},
		{"partialPredicate", partialPredicateIndex, types.String, true},
		{"histogram", hgIndex, types.Bytes, true},
		{"fullStatisticID", fullStatisticsIdIndex, types.Int, true},
	}

	for _, v := range expectedTypes {
		if !datums[v.fieldIndex].ResolvedType().Equivalent(v.expectedType) &&
			(!v.nullable || datums[v.fieldIndex].ResolvedType().Family() != types.UnknownFamily) {
			return nil, errors.Errorf("%s returned from table statistics lookup has type %s. Expected %s",
				v.fieldName, datums[v.fieldIndex].ResolvedType(), v.expectedType)
		}
	}

	// Extract datum values.
	res := &TableStatisticProto{
		TableID:       descpb.ID((int32)(*datums[tableIDIndex].(*tree.DInt))),
		StatisticID:   (uint64)(*datums[statisticsIDIndex].(*tree.DInt)),
		CreatedAt:     datums[createdAtIndex].(*tree.DTimestamp).Time,
		RowCount:      (uint64)(*datums[rowCountIndex].(*tree.DInt)),
		DistinctCount: (uint64)(*datums[distinctCountIndex].(*tree.DInt)),
		NullCount:     (uint64)(*datums[nullCountIndex].(*tree.DInt)),
		AvgSize:       (uint64)(*datums[avgSizeIndex].(*tree.DInt)),
	}
	columnIDs := datums[columnIDsIndex].(*tree.DArray)
	res.ColumnIDs = make([]descpb.ColumnID, len(columnIDs.Array))
	for i, d := range columnIDs.Array {
		res.ColumnIDs[i] = descpb.ColumnID((int32)(*d.(*tree.DInt)))
	}
	if datums[nameIndex] != tree.DNull {
		res.Name = string(*datums[nameIndex].(*tree.DString))
	}
	if datums[partialPredicateIndex] != tree.DNull {
		res.PartialPredicate = string(*datums[partialPredicateIndex].(*tree.DString))
	}
	if datums[fullStatisticsIdIndex] != tree.DNull {
		res.FullStatisticID = uint64(*datums[fullStatisticsIdIndex].(*tree.DInt))
	}
	if datums[hgIndex] != tree.DNull {
		res.HistogramData = &HistogramData{}
		if err := protoutil.Unmarshal(
			[]byte(*datums[hgIndex].(*tree.DBytes)),
			res.HistogramData,
		); err != nil {
			return nil, err
		}
	}
	return res, nil
}

// parseStats converts the given datums to a TableStatistic object. It might
// need to run a query to get user defined type metadata.
func (sc *TableStatisticsCache) parseStats(
	ctx context.Context, datums tree.Datums, typeResolver *descs.DistSQLTypeResolver,
) (_ *TableStatistic, _ *types.T, err error) {
	defer func() {
		if r := recover(); r != nil {
			// In the event of a "safe" panic, we only want to log the error and
			// continue executing the query without stats for this table. This is only
			// possible because the code does not update shared state and does not
			// manipulate locks.
			if ok, e := errorutil.ShouldCatch(r); ok {
				err = e
			} else {
				// Other panic objects can't be considered "safe" and thus are
				// propagated as crashes that terminate the session.
				panic(r)
			}
		}
	}()

	var tsp *TableStatisticProto
	tsp, err = NewTableStatisticProto(datums)
	if err != nil {
		return nil, nil, err
	}
	res := &TableStatistic{TableStatisticProto: *tsp}
	var udt *types.T
	if res.HistogramData != nil && (len(res.HistogramData.Buckets) > 0 || res.RowCount == res.NullCount) {
		// Hydrate the type in case any user defined types are present.
		// There are cases where typ is nil, so don't do anything if so.
		if typ := res.HistogramData.ColumnType; typ != nil && typ.UserDefined() {
			if typeResolver != nil {
				udt, err = typeResolver.ResolveTypeByOID(ctx, typ.Oid())
				if err != nil {
					return nil, nil, err
				}
				res.HistogramData.ColumnType = udt
			} else {
				// The metadata accessed here is never older than the metadata
				// used when collecting the stats. Changes to types are
				// backwards compatible across versions, so using a newer
				// version of the type metadata here is safe.
				if err = sc.db.DescsTxn(ctx, func(
					ctx context.Context, txn descs.Txn,
				) error {
					resolver := descs.NewDistSQLTypeResolver(txn.Descriptors(), txn.KV())
					udt, err = resolver.ResolveTypeByOID(ctx, typ.Oid())
					res.HistogramData.ColumnType = udt
					return err
				}); err != nil {
					return nil, nil, err
				}
			}
		}
		if err = DecodeHistogramBuckets(res); err != nil {
			return nil, nil, err
		}
	}
	return res, udt, nil
}

// DecodeHistogramBuckets decodes encoded HistogramData in tabStat and writes
// the resulting buckets into tabStat.Histogram.
func DecodeHistogramBuckets(tabStat *TableStatistic) error {
	h := tabStat.HistogramData
	if tabStat.NullCount > 0 {
		// A bucket for NULL is not persisted, but we create a fake one to
		// make histograms easier to work with. The length of res.Histogram
		// is therefore 1 greater than the length of the histogram data
		// buckets.
		// TODO(michae2): Combine this with setHistogramBuckets, especially if we
		// need to change both after #6224 is fixed (NULLS LAST in index ordering).
		tabStat.Histogram = make([]cat.HistogramBucket, 1, len(h.Buckets)+1)
		tabStat.Histogram[0] = cat.HistogramBucket{
			NumEq:         float64(tabStat.NullCount),
			NumRange:      0,
			DistinctRange: 0,
			UpperBound:    tree.DNull,
		}
	} else {
		tabStat.Histogram = make([]cat.HistogramBucket, 0, len(h.Buckets))
	}

	// Decode the histogram data so that it's usable by the opt catalog.
	var a tree.DatumAlloc
	for i := range h.Buckets {
		bucket := &h.Buckets[i]
		datum, err := DecodeUpperBound(h.Version, h.ColumnType, &a, bucket.UpperBound)
		if err != nil {
			if h.ColumnType.Family() == types.EnumFamily && errors.Is(err, types.EnumValueNotFound) {
				// Skip over buckets for enum values that were dropped.
				continue
			}
			return err
		}
		tabStat.Histogram = append(tabStat.Histogram, cat.HistogramBucket{
			NumEq:         float64(bucket.NumEq),
			NumRange:      float64(bucket.NumRange),
			DistinctRange: bucket.DistinctRange,
			UpperBound:    datum,
		})
	}
	return nil
}

// setHistogramBuckets shallow-copies the passed histogram into the
// TableStatistic, and prepends a bucket for NULL rows using the
// TableStatistic's null count. The resulting TableStatistic looks the same as
// if DecodeHistogramBuckets had been called.
func (tabStat *TableStatistic) setHistogramBuckets(hist histogram) {
	tabStat.Histogram = hist.buckets
	if tabStat.NullCount > 0 {
		tabStat.Histogram = append([]cat.HistogramBucket{{
			NumEq:      float64(tabStat.NullCount),
			UpperBound: tree.DNull,
		}}, tabStat.Histogram...)
	}
	// Round NumEq and NumRange, as if this had come from HistogramData. (We also
	// round these counts in histogram.toHistogramData.)
	for i := 0; i < len(tabStat.Histogram); i++ {
		tabStat.Histogram[i].NumEq = math.Round(tabStat.Histogram[i].NumEq)
		tabStat.Histogram[i].NumRange = math.Round(tabStat.Histogram[i].NumRange)
	}
}

// nonNullHistogram returns the TableStatistic histogram with the NULL bucket
// removed.
func (tabStat *TableStatistic) nonNullHistogram() histogram {
	if len(tabStat.Histogram) > 0 && tabStat.Histogram[0].UpperBound == tree.DNull {
		return histogram{buckets: tabStat.Histogram[1:]}
	}
	return histogram{buckets: tabStat.Histogram}
}

// String implements the fmt.Stringer interface.
func (tabStat *TableStatistic) String() string {
	return fmt.Sprintf(
		"%s histogram:%s", &tabStat.TableStatisticProto, histogram{buckets: tabStat.Histogram},
	)
}

// IsPartial returns true if this statistic was collected with a where clause.
func (tsp *TableStatisticProto) IsPartial() bool {
	return tsp.PartialPredicate != ""
}

// IsMerged returns true if this statistic was created by merging a partial and
// a full statistic.
func (tsp *TableStatisticProto) IsMerged() bool {
	return tsp.Name == jobspb.MergedStatsName
}

// IsForecast returns true if this statistic was created by forecasting.
func (tsp *TableStatisticProto) IsForecast() bool {
	return tsp.Name == jobspb.ForecastStatsName
}

// IsAuto returns true if this statistic was collected automatically.
func (tsp *TableStatisticProto) IsAuto() bool {
	return tsp.Name == jobspb.AutoStatsName
}

// getTableStatsFromDB retrieves the statistics in system.table_statistics
// for the given table ID.
//
// It ignores any statistics that cannot be decoded (e.g. because a user-defined
// type that doesn't exist) and returns the rest (with no error).
func (sc *TableStatisticsCache) getTableStatsFromDB(
	ctx context.Context,
	tableID descpb.ID,
	forecast bool,
	st *cluster.Settings,
	typeResolver *descs.DistSQLTypeResolver,
) (_ []*TableStatistic, _ map[descpb.ColumnID]*types.T, err error) {
	getTableStatisticsStmt := `
SELECT
	"tableID",
	"statisticID",
	name,
	"columnIDs",
	"createdAt",
	"rowCount",
	"distinctCount",
	"nullCount",
	"avgSize",
	"partialPredicate",
	histogram,
	"fullStatisticID"
FROM system.table_statistics
WHERE "tableID" = $1
ORDER BY "createdAt" DESC, "columnIDs" DESC, "statisticID" DESC
`
	// TODO(michae2): Add an index on system.table_statistics (tableID, createdAt,
	// columnIDs, statisticID).

	it, err := sc.db.Executor().QueryIteratorEx(
		ctx, "get-table-statistics", nil /* txn */, sessiondata.NodeUserSessionDataOverride, getTableStatisticsStmt, tableID,
	)
	if err != nil {
		return nil, nil, err
	}

	// Guard against crashes in the code below.
	defer func() {
		if r := recover(); r != nil {
			// In the event of a "safe" panic, we only want to log the error and
			// continue executing the query without stats for this table. This is only
			// possible because the code does not update shared state and does not
			// manipulate locks.
			if ok, e := errorutil.ShouldCatch(r); ok {
				err = e
			} else {
				// Other panic objects can't be considered "safe" and thus are
				// propagated as crashes that terminate the session.
				panic(r)
			}
		}
	}()

	var statsList []*TableStatistic
	var udts map[descpb.ColumnID]*types.T
	var ok bool
	for ok, err = it.Next(ctx); ok; ok, err = it.Next(ctx) {
		stats, udt, err := sc.parseStats(ctx, it.Cur(), typeResolver)
		if err != nil {
			log.Warningf(ctx, "could not decode statistic for table %d: %v", tableID, err)
			continue
		}
		statsList = append(statsList, stats)
		// Keep track of user-defined types used in histograms.
		if udt != nil {
			// TODO(49698): If we ever support multi-column histograms we'll need to
			// build this mapping in a different way.
			if len(stats.ColumnIDs) == 1 {
				colID := stats.ColumnIDs[0]
				if udts == nil {
					udts = make(map[descpb.ColumnID]*types.T)
				}
				// Keep the first type we see for the column.
				if _, ok := udts[colID]; !ok {
					udts[colID] = udt
				}
			}
		}
	}
	if err != nil {
		return nil, nil, err
	}

	merged := MergedStatistics(ctx, statsList, st)
	statsList = append(merged, statsList...)

	if forecast {
		forecasts := ForecastTableStatistics(ctx, sc.settings, statsList)
		statsList = append(statsList, forecasts...)
		// Some forecasts could have a CreatedAt time before or after some collected
		// stats, so make sure the list is sorted in descending CreatedAt order.
		sort.SliceStable(statsList, func(i, j int) bool {
			return statsList[i].CreatedAt.After(statsList[j].CreatedAt)
		})
	}

	return statsList, udts, nil
}
