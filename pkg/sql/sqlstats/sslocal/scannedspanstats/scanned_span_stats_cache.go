// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scannedspanstats

import (
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

type spanStatsInfo struct {
	spanStats     enginepb.MVCCStats
	lastRequested time.Time
}

type fingerprintInfo struct {
	tableIDs      []cat.StableID
	lastRequested time.Time
}

// SpanStatsCache stores the map of fingerprints to table IDs and the map of
// table IDs to span stats and execution count.
type SpanStatsCache struct {
	st *cluster.Settings

	mu struct {
		syncutil.RWMutex

		// statsMap maps table IDs to span statistics.
		statsMap map[cat.StableID]spanStatsInfo

		// fingerprintMap maps fingerprints to tableIDs.
		fingerprintMap map[string]fingerprintInfo

		// lastCleanupTs tracks the last time we cleaned up the cache.
		lastCleanupTs time.Time

		// statsLastCollectedTs tracks the last time we collected span stats.
		statsLastCollectedTs time.Time
	}

	atomic struct {
		// uniqueSpanStatsInfo tracks the number of unique span statistics we are
		// storing in memory.
		uniqueSpanStatsInfo int64
	}
}

// NewSpanStatsCache creates new maps to be used as a cache for span statistics.
func NewSpanStatsCache(setting *cluster.Settings) *SpanStatsCache {
	spanStatsCache := &SpanStatsCache{
		st: setting,
	}
	spanStatsCache.mu.statsMap = make(map[cat.StableID]spanStatsInfo)
	spanStatsCache.mu.fingerprintMap = make(map[string]fingerprintInfo)

	// For a new cache, we want the lastCollectedTs to be in the past so we
	// collect stats immediately.
	refreshInterval := sqlstats.ScannedSpanStatsRefresh.Get(&setting.SV)
	spanStatsCache.mu.statsLastCollectedTs = timeutil.Now().Add(-refreshInterval)
	return spanStatsCache
}

// CheckFingerprintInfo returns true if the statement fingerprint has been
// stored in the spanStats cache. When a statement is found in the cache,
// we reset the lastRequested timestamp.
func (spanStats *SpanStatsCache) CheckFingerprintInfo(fingerprint string) bool {
	_, found := spanStats.getFingerprintInfo(fingerprint)
	if found {
		now := timeutil.Now()
		spanStats.updateFingerprintInfo(fingerprint, now)
	}
	return found
}

// ShouldCacheFingerprintInfo returns true if the type of statement should have
// span stats cached for the tables that its plan scans during execution.
// We only want to get span stats if span statistics are enabled,
// the statement is DML, and if the statement is not internal.
func (spanStats *SpanStatsCache) ShouldCacheFingerprintInfo(
	stmtType tree.StatementType, isInternal bool,
) bool {
	return sqlstats.CollectScannedSpanStats.Get(&spanStats.st.
		SV) && stmtType == tree.TypeDML && !isInternal
}

// CreateFingerprintInfo creates a fingerprintInfo entry in the spanStats cache.
func (spanStats *SpanStatsCache) CreateFingerprintInfo(
	fingerprint string, tableIDs []cat.StableID,
) bool {
	_, found := spanStats.getFingerprintInfo(fingerprint)
	if !found {
		now := timeutil.Now()
		spanStats.setFingerprintInfo(fingerprint, tableIDs, now)
		for _, tabID := range tableIDs {
			spanStats.setSpanStatistics(tabID, now, enginepb.MVCCStats{})
		}
	}
	return found
}

// ShouldCollectSpanStats returns true if no span stats have been collected
// since the interval specified by the scanned_span_stats_collection.interval
// cluster setting.
func (spanStats *SpanStatsCache) ShouldCollectSpanStats() ([]cat.StableID, bool) {
	lastCollected := spanStats.getStatsLastCollected()
	timeSinceLastCollected := timeutil.Since(lastCollected)
	refreshInterval := sqlstats.ScannedSpanStatsRefresh.Get(&spanStats.st.SV)
	if timeSinceLastCollected >= refreshInterval {
		var tabIDs []cat.StableID
		allSpanStats := spanStats.getAllSpanStats()
		for key := range allSpanStats {
			tabIDs = append(tabIDs, key)
		}
		return tabIDs, true
	}
	return []cat.StableID{}, false
}

// UpdateSpanStatistics updates the spanStatsInfo cache.
func (spanStats *SpanStatsCache) UpdateSpanStatistics(
	tableID cat.StableID, stats enginepb.MVCCStats, resetRefreshInterval bool,
) {

	now := timeutil.Now()
	spanStats.setSpanStatistics(tableID, now, stats)
	if resetRefreshInterval {
		spanStats.setStatsLastCollected(now)
	}
}

// FingerprintSpanStats gets the spanStatsInfo for the fingerprint.
func (spanStats *SpanStatsCache) FingerprintSpanStats(fingerprint string) []enginepb.MVCCStats {
	stats, _ := spanStats.getSpanStatsForFingerprint(fingerprint)
	return stats
}

// getFingerprintInfo gets the fingerprintInfo for the fingerprint.
func (spanStats *SpanStatsCache) getFingerprintInfo(fingerprint string) (fingerprintInfo, bool) {
	spanStats.mu.RLock()
	defer spanStats.mu.RUnlock()

	info, found := spanStats.mu.fingerprintMap[fingerprint]

	return info, found
}

// setFingerprintInfo sets the fingerprintInfo for the fingerprint.
func (spanStats *SpanStatsCache) setFingerprintInfo(
	fingerprint string, tableIDs []cat.StableID, time time.Time,
) {
	spanStats.mu.Lock()
	defer spanStats.mu.Unlock()

	spanStats.mu.fingerprintMap[fingerprint] = fingerprintInfo{
		tableIDs:      tableIDs,
		lastRequested: time,
	}
}

// getStatsLastCollected gets the lastCollected timestamp for the cache.
func (spanStats *SpanStatsCache) getStatsLastCollected() time.Time {
	spanStats.mu.RLock()
	defer spanStats.mu.RUnlock()

	return spanStats.mu.statsLastCollectedTs
}

// getStatsLastCollected gets the lastCollected timestamp for the cache.
func (spanStats *SpanStatsCache) setStatsLastCollected(time time.Time) {
	spanStats.mu.Lock()
	defer spanStats.mu.Unlock()

	spanStats.mu.statsLastCollectedTs = time
}

// checkFingerprintInfo checks the cache for fingerprintInfo for a given
// fingerprint.
func (spanStats *SpanStatsCache) checkFingerprintInfo(fingerprint string) (fingerprintInfo, bool) {
	info, found := spanStats.getFingerprintInfo(fingerprint)
	if found {
		return info, true
	}
	return fingerprintInfo{}, false
}

// getAllSpanStats returns all span stats in the cache.
func (spanStats *SpanStatsCache) getAllSpanStats() map[cat.
	StableID]spanStatsInfo {
	spanStats.mu.RLock()
	defer spanStats.mu.RUnlock()

	return spanStats.mu.statsMap
}

// getSpanStatsInfo returns the spanStatsInfo for a given table ID.
func (spanStats *SpanStatsCache) getSpanStatsInfo(tableID cat.StableID) (spanStatsInfo, bool) {
	spanStats.mu.RLock()
	defer spanStats.mu.RUnlock()

	statsInfo, found := spanStats.mu.statsMap[tableID]

	return statsInfo, found
}

// getSpanStatsForFingerprint returns stats for the tableIDs in the
// fingerprintInfo map.
func (spanStats *SpanStatsCache) getSpanStatsForFingerprint(
	fingerprint string,
) ([]enginepb.MVCCStats, bool) {

	info, found := spanStats.getFingerprintInfo(fingerprint)
	if !found {
		return nil, false
	}

	// We only want to return span stats if they are available
	// for all tableIDs in the fingerprintInfo map, as returning an incomplete
	// picture of the MVCC stats from the tables scanned under a plan could be
	// misleading. For example, reporting a much higher "live data percentage" in
	// the statement statistics for an execution than we actually have could lead
	// user to believe that MVCC garbage is not a reason for the query's slowness,
	// when it could have been.
	spanStatsArray := make([]enginepb.MVCCStats, 0, len(info.tableIDs))

	for _, tabID := range info.tableIDs {
		statsInfo, statsFound := spanStats.getSpanStatsInfo(tabID)
		if statsFound {
			spanStatsArray = append(spanStatsArray, statsInfo.spanStats)
		} else {
			return []enginepb.MVCCStats{}, false
		}
	}

	return spanStatsArray, found
}

func (spanStats *SpanStatsCache) getOrCreateSpanStatistics(
	tableID cat.StableID,
) (spanStatsInfo, bool) {
	statsInfo, found := spanStats.getSpanStatsInfo(tableID)
	if found {
		return statsInfo, true
	}

	// Get the cluster setting value of the limit on number of unique span
	// stats info we can store in memory.
	limit := sqlstats.MaxMemScannedSpanStats.Get(&spanStats.st.SV)
	incrementedCount := atomic.AddInt64(&spanStats.atomic.uniqueSpanStatsInfo,
		int64(1))

	// If a matching entry was not found, check if a new entry can be created,
	// without passing the limit of unique span stats from the cache.
	if incrementedCount > limit {
		atomic.AddInt64(&spanStats.atomic.uniqueSpanStatsInfo, -int64(1))
		// If we have exceeded the limit of unique span stats try to delete older
		// data.
		spanStats.clearOldSpanStats()

		// Confirm if after the cleanup we can add new entries.
		incrementedCount = atomic.AddInt64(&spanStats.atomic.uniqueSpanStatsInfo,
			int64(1))
		// Abort if no entries were deleted.
		if incrementedCount > limit {
			atomic.AddInt64(&spanStats.atomic.uniqueSpanStatsInfo, -int64(1))
			return spanStatsInfo{}, false
		}
	}

	spanStats.mu.Lock()
	defer spanStats.mu.Unlock()
	// Confirm no entry was created in another thread between the check above
	// and the new creation.
	statsInfo, found = spanStats.mu.statsMap[tableID]
	if found {
		return statsInfo, true
	}

	statsInfo = spanStatsInfo{
		lastRequested: timeutil.Now(),
		spanStats:     enginepb.MVCCStats{},
	}
	spanStats.mu.statsMap[tableID] = statsInfo

	return statsInfo, true
}

// setSpanStatistics sets span stats for a specific table ID.
func (spanStats *SpanStatsCache) setSpanStatistics(
	tableID cat.StableID, time time.Time, stats enginepb.MVCCStats,
) {
	_, found := spanStats.getOrCreateSpanStatistics(tableID)

	if found {
		spanStats.mu.Lock()
		defer spanStats.mu.Unlock()

		spanStats.mu.statsMap[tableID] = spanStatsInfo{
			lastRequested: time,
			spanStats:     stats,
		}
	}
}

// updateFingerprintInfo updates the fingerprintInfo.lastRequested field for a
// given fingerprint.
func (spanStats *SpanStatsCache) updateFingerprintInfo(fingerprint string, time time.Time) {
	spanStats.mu.Lock()
	defer spanStats.mu.Unlock()

	mapRef := spanStats.mu.fingerprintMap[fingerprint]
	mapRef.lastRequested = time
}

// clearOldSpanStats clears all cached values that were last requested
// over 24 hours ago.
func (spanStats *SpanStatsCache) clearOldSpanStats() {
	timeSinceLastCleanup := timeutil.Since(spanStats.getLastCleanupTs())
	// Check if has been at least 5min since last cleanup, to avoid
	// lock contention when we reached the limit.
	if timeSinceLastCleanup.Minutes() >= 5 {
		spanStats.mu.Lock()
		defer spanStats.mu.Unlock()

		deleted := 0
		for key, value := range spanStats.mu.statsMap {
			if timeutil.Since(value.lastRequested).Hours() >= 24 {
				delete(spanStats.mu.statsMap, key)
				deleted++
			}
		}
		atomic.AddInt64(&spanStats.atomic.uniqueSpanStatsInfo, int64(-deleted))

		// There is no limit on unique fingerprint maps stored, as this number could
		// be very large for workloads with generated queries and most overhead will
		// come from collecting and storing table-specific span statistics.
		for key, value := range spanStats.mu.fingerprintMap {
			if timeutil.Since(value.lastRequested).Hours() >= 24 {
				delete(spanStats.mu.fingerprintMap, key)
			}
		}

		spanStats.mu.lastCleanupTs = timeutil.Now()
	}
}

func (spanStats *SpanStatsCache) getLastCleanupTs() time.Time {
	spanStats.mu.RLock()
	defer spanStats.mu.RUnlock()

	return spanStats.mu.lastCleanupTs
}
