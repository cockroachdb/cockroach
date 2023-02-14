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
	lastCollectedTs time.Time
	spanStats       enginepb.MVCCStats
}

type fingerprintInfo struct {
	tableIDs        []cat.StableID
	lastCollectedTs time.Time
	executionCount  int64
}

// SpanStatsCache stores the map of fingerprints to table IDs and the map of table IDs to span stats and execution count.
type SpanStatsCache struct {
	st *cluster.Settings

	// minExecCount is the minimum value for execution count that a statement must have before we collect span statistics
	// on the tables its plan scans. This minimum value should filter out most generated statement fingerprints.
	minExecCount int64

	mu struct {
		syncutil.RWMutex

		// statsMap maps table IDs to span statistics
		statsMap map[cat.StableID]spanStatsInfo

		// fingerprintMap maps fingerprints to tableIDs, execution count
		fingerprintMap map[string]fingerprintInfo

		// lastCleanupTs has the last time we cleaned up the cache.
		lastCleanupTs time.Time
	}

	atomic struct {
		// uniqueSpanStatsInfo is the number of unique span statistics we are storing in memory.
		uniqueSpanStatsInfo int64
	}
}

// NewSpanStatsCache creates new maps to be used as a cache for span statistics.
func NewSpanStatsCache(setting *cluster.Settings) *SpanStatsCache {
	spanStatsCache := &SpanStatsCache{
		st:           setting,
		minExecCount: 5,
	}
	spanStatsCache.mu.statsMap = make(map[cat.StableID]spanStatsInfo)
	spanStatsCache.mu.fingerprintMap = make(map[string]fingerprintInfo)
	spanStatsCache.mu.lastCleanupTs = time.Time{}
	return spanStatsCache
}

// ShouldCacheFingerprintInfo returns true if the type of statement should have span stats cached for it.
// We only want to get span stats if span statistics are enabled, the statement is DML, the statement is
// not internal, and if the statement has at least 1 scan in its plan.
func (spanStats *SpanStatsCache) ShouldCacheFingerprintInfo(
	stmtType tree.StatementType, isInternal bool, scanCount int64,
) bool {
	return sqlstats.CollectScannedSpanStats.Get(&spanStats.st.SV) && stmtType == tree.TypeDML && !isInternal && scanCount > 0
}

// ShouldCollectSpanStatsForFingerprint returns true if the execution count for the fingerprint is above the minimum
// and if no span stats have been collected for the fingerprint in the past hour.
// If no cache exists for the fingerprint, it creates one and returns true.
// If no cache can be created, it returns false.
func (spanStats *SpanStatsCache) ShouldCollectSpanStatsForFingerprint(
	fingerprint string, tableIDs []cat.StableID,
) bool {
	info, found := spanStats.getOrCreateFingerprintInfo(fingerprint, tableIDs)
	if !found {
		return false
	}

	_, foundStats := spanStats.getSpanStatsForFingerprint(fingerprint)

	timeSinceLastCollected := timeutil.Since(info.lastCollectedTs)
	return info.executionCount >= spanStats.minExecCount && (timeSinceLastCollected.Hours() >= 1 || !foundStats)
}

// ShouldCollectSpanStatsForTable returns true if span stats have not been collected for the table in the past hour.
// If no cache exists for the span, it creates one. If no cache can be created, it returns false.
func (spanStats *SpanStatsCache) ShouldCollectSpanStatsForTable(tableID cat.StableID) bool {
	statsInfo, found := spanStats.getOrCreateSpanStatistics(tableID)
	if !found {
		return false
	}

	timeSinceLastCollected := timeutil.Since(statsInfo.lastCollectedTs)
	return timeSinceLastCollected.Hours() >= 1
}

// UpdateSpanStatistics updates the spanStatsInfo cache if any new stats were collected, and returns the latest
// statistics for a given table. If reset is true, new span stats were collected, so we reset the lastCollectedTs on
// the cache. If reset is false, update the cache with whatever is returned by getOrCreateSpanStatistic.
func (spanStats *SpanStatsCache) UpdateSpanStatistics(
	tableID cat.StableID, stats enginepb.MVCCStats, reset bool,
) enginepb.MVCCStats {

	if reset {
		now := timeutil.Now()
		spanStats.setSpanStatistics(tableID, now, stats)
		return stats
	}

	statsInfo, found := spanStats.getOrCreateSpanStatistics(tableID)
	if !found {
		return stats
	}

	spanStats.setSpanStatistics(
		tableID,
		statsInfo.lastCollectedTs,
		statsInfo.spanStats,
	)

	return statsInfo.spanStats
}

// UpdateFingerprintInfo updates the fingerprintInfo cache and returns an array of all span stats in the tables scanned
// by the fingerprint's plan. If reset is true, new span stats were collected, so we reset the lastCollectedTs and
// execution counter, and return the stats passed in. If reset is false, we look for existing table stats for the
// fingerprint's scanned tables, increment the execution counter if it's below the minimum for collection, and then
// return the stats in the spanStatsInfo cache for each scanned table.
func (spanStats *SpanStatsCache) UpdateFingerprintInfo(
	fingerprint string, tableIDs []cat.StableID, statsArray []enginepb.MVCCStats, reset bool,
) []enginepb.MVCCStats {

	if reset && len(statsArray) == len(tableIDs) {
		now := timeutil.Now()
		spanStats.setFingerprintInfo(fingerprint, tableIDs, now, 0)
		return statsArray
	}

	info, found := spanStats.getOrCreateFingerprintInfo(fingerprint, tableIDs)
	if !found {
		return []enginepb.MVCCStats{}
	}

	if info.executionCount < spanStats.minExecCount {
		spanStats.setFingerprintInfo(fingerprint, tableIDs, info.lastCollectedTs,
			info.executionCount+1)
	}

	stats, found := spanStats.getSpanStatsForFingerprint(fingerprint)
	if !found {
		return []enginepb.MVCCStats{}
	}

	return stats

}

func (spanStats *SpanStatsCache) getSpanStatsInfoFromTableID(
	tableID cat.StableID,
) (spanStatsInfo, bool) {
	spanStats.mu.RLock()
	defer spanStats.mu.RUnlock()

	statsInfo, found := spanStats.mu.statsMap[tableID]

	return statsInfo, found
}

func (spanStats *SpanStatsCache) getSpanStatsForFingerprint(
	fingerprint string,
) ([]enginepb.MVCCStats, bool) {

	var spanStatsArray []enginepb.MVCCStats

	info, found := spanStats.getFingerprintInfoFromFingerprint(fingerprint)
	if found {
		for _, tabID := range info.tableIDs {
			statsInfo, statsFound := spanStats.getSpanStatsInfoFromTableID(tabID)
			if statsFound {
				spanStatsArray = append(spanStatsArray, statsInfo.spanStats)
			} else {
				return []enginepb.MVCCStats{}, false
			}
		}
	}

	return spanStatsArray, found
}

func (spanStats *SpanStatsCache) getFingerprintInfoFromFingerprint(
	fingerprint string,
) (fingerprintInfo, bool) {
	spanStats.mu.RLock()
	defer spanStats.mu.RUnlock()

	info, found := spanStats.mu.fingerprintMap[fingerprint]

	return info, found
}

func (spanStats *SpanStatsCache) getOrCreateFingerprintInfo(
	fingerprint string, tableIDs []cat.StableID,
) (fingerprintInfo, bool) {
	info, found := spanStats.getFingerprintInfoFromFingerprint(fingerprint)
	if found {
		return info, true
	}

	spanStats.mu.Lock()
	defer spanStats.mu.Unlock()
	// Confirm no entry was created in another thread between the check above and the new creation.
	info, found = spanStats.mu.fingerprintMap[fingerprint]
	if found {
		return info, true
	}
	// For a new entry, we want the lastCollectedTs to be in the past, in case we reach the execution count, we should
	// collect new span stats.
	info = fingerprintInfo{
		lastCollectedTs: timeutil.Now().Add(-time.Hour),
		executionCount:  0,
		tableIDs:        tableIDs,
	}
	spanStats.mu.fingerprintMap[fingerprint] = info

	return info, true
}

func (spanStats *SpanStatsCache) getOrCreateSpanStatistics(
	tableID cat.StableID,
) (spanStatsInfo, bool) {
	statsInfo, found := spanStats.getSpanStatsInfoFromTableID(tableID)
	if found {
		return statsInfo, true
	}

	// Get the cluster setting value of the limit on number of unique span stats info we can store in memory.
	limit := sqlstats.MaxMemScannedSpanStats.Get(&spanStats.st.SV)
	incrementedCount :=
		atomic.AddInt64(&spanStats.atomic.uniqueSpanStatsInfo, int64(1))

	// If a matching entry was not found, check if a new entry can be created, without passing the limit of unique span
	// stats from the cache.
	if incrementedCount > limit {
		atomic.AddInt64(&spanStats.atomic.uniqueSpanStatsInfo, -int64(1))
		// If we have exceeded the limit of unique span stats try to delete older data.
		spanStats.clearOldSpanStats()

		// Confirm if after the cleanup we can add new entries.
		incrementedCount =
			atomic.AddInt64(&spanStats.atomic.uniqueSpanStatsInfo, int64(1))
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

	// For a new entry, we want the lastCollectedTs to be in the past, in case we reach
	// the execution count on a fingerprint that scans the table, we should collect new span stats.
	statsInfo = spanStatsInfo{
		lastCollectedTs: timeutil.Now().Add(-time.Hour),
		spanStats:       enginepb.MVCCStats{},
	}
	spanStats.mu.statsMap[tableID] = statsInfo

	return statsInfo, true
}

func (spanStats *SpanStatsCache) setSpanStatistics(
	tableID cat.StableID, time time.Time, stats enginepb.MVCCStats,
) {
	_, found := spanStats.getOrCreateSpanStatistics(tableID)

	if found {
		spanStats.mu.Lock()
		defer spanStats.mu.Unlock()

		spanStats.mu.statsMap[tableID] = spanStatsInfo{
			lastCollectedTs: time,
			spanStats:       stats,
		}
	}
}

func (spanStats *SpanStatsCache) setFingerprintInfo(
	fingerprint string, tableIDs []cat.StableID, time time.Time, executionCount int64,
) {
	_, found := spanStats.getFingerprintInfoFromFingerprint(fingerprint)

	if found {
		spanStats.mu.Lock()
		defer spanStats.mu.Unlock()

		spanStats.mu.fingerprintMap[fingerprint] = fingerprintInfo{
			tableIDs:        tableIDs,
			lastCollectedTs: time,
			executionCount:  executionCount,
		}
	}
}

// clearOldSpanStats clears all cache values that were last updated more than an hour ago.
func (spanStats *SpanStatsCache) clearOldSpanStats() {
	timeSinceLastCleanup := timeutil.Since(spanStats.getLastCleanupTs())
	// Check if has been at least 5min since last cleanup, to avoid
	// lock contention when we reached the limit.
	if timeSinceLastCleanup.Minutes() >= 5 {
		spanStats.mu.Lock()
		defer spanStats.mu.Unlock()

		deleted := 0
		for key, value := range spanStats.mu.statsMap {
			if timeutil.Since(value.lastCollectedTs).Hours() >= 1 {
				delete(spanStats.mu.statsMap, key)
				deleted++
			}
		}
		atomic.AddInt64(&spanStats.atomic.uniqueSpanStatsInfo, int64(-deleted))

		// There is no limit on unique fingerprint maps stored, as this number could be very large for workloads with
		// generated queries and most overhead will come from collecting and storing table-specific span statistics.
		for key, value := range spanStats.mu.fingerprintMap {
			if timeutil.Since(value.lastCollectedTs).Hours() >= 1 {
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
