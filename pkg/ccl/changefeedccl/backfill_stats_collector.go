// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedccl

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

type backfillStats struct {
	TotalRanges   int64
	PendingRanges int64
}

type backfillStatsCollector struct {
	mu             syncutil.Mutex
	stats          map[int32]*backfillStats
	processorCount int32
}

func newBackfillStatsCollector(processorCount int32) *backfillStatsCollector {
	return &backfillStatsCollector{
		stats:          make(map[int32]*backfillStats),
		processorCount: processorCount,
	}
}

func (b *backfillStatsCollector) add(aggregatorID int32, totalRanges int64, pendingRanges int64) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.stats[aggregatorID] = &backfillStats{
		TotalRanges:   totalRanges,
		PendingRanges: pendingRanges,
	}
}

// rollupStats aggregates the backfill stats across all changefeed aggregators
// and returns the initial scan progress as well as a human-readable status message.
func (b *backfillStatsCollector) rollupStats(
	initialScan bool,
) (initialScanProgress float32, backfillStatus string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	var total struct {
		TotalRanges   int64
		PendingRanges int64
	}
	for _, aggregatorStats := range b.stats {
		total.TotalRanges += aggregatorStats.TotalRanges
		total.PendingRanges += aggregatorStats.PendingRanges
	}

	// Don't report a percentage until all aggregators have reported.
	if len(b.stats) != int(b.processorCount) || total.TotalRanges == 0 {
		return 0, fmt.Sprintf("starting streams (%d out of %d)", len(b.stats), b.processorCount)
	}

	if initialScan {
		// A job can only report either the fraction completed or the highwater, so we only want
		// to report the fraction during initial scan when the highwater is not meaningful.
		fractionCompleted := max(
			// Use a tiny fraction completed to start with a nearly empty
			// progress bar until we get the first batch of range stats.
			float32(0.0001),
			float32(total.TotalRanges-total.PendingRanges)/float32(total.TotalRanges))

		return fractionCompleted, fmt.Sprintf("initial scan on %d out of %d ranges", total.PendingRanges, total.TotalRanges)
	}
	if total.PendingRanges != 0 {
		return 1, fmt.Sprintf("schemachange backfill on %d out of %d ranges", total.PendingRanges, total.TotalRanges)
	}
	return 1, ""
}
