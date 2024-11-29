// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package logical

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

type rangeStatsByProcessorID struct {
	mu             syncutil.Mutex
	stats          map[int32]*streampb.StreamEvent_RangeStats
	processorCount int
}

func (r *rangeStatsByProcessorID) Add(processorID int32, stats *streampb.StreamEvent_RangeStats) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.stats[processorID] = stats
}

func (r *rangeStatsByProcessorID) RollupStats() (streampb.StreamEvent_RangeStats, float32, string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var total streampb.StreamEvent_RangeStats
	for _, producerStats := range r.stats {
		total.RangeCount += producerStats.RangeCount
		total.ScanningRangeCount += producerStats.ScanningRangeCount
		total.LaggingRangeCount += producerStats.LaggingRangeCount
	}
	initialScanComplete := total.ScanningRangeCount == 0
	incompleteCount := total.ScanningRangeCount
	if initialScanComplete {
		incompleteCount = total.LaggingRangeCount
	}

	fractionCompleted := max(
		// Use a tiny fraction completed to start with a nearly empty
		// progress bar until we get the first batch of range stats.
		float32(0.0001),
		(float32(total.RangeCount-incompleteCount) / float32(total.RangeCount)))

	if len(r.stats) != r.processorCount || total.RangeCount == 0 {
		return streampb.StreamEvent_RangeStats{}, 0, fmt.Sprintf("starting streams (%d out of %d)", len(r.stats), r.processorCount)
	}
	if !initialScanComplete {
		return total, fractionCompleted, fmt.Sprintf("initial scan on %d out of %d ranges", total.ScanningRangeCount, total.RangeCount)
	}
	if total.LaggingRangeCount != 0 {
		return total, fractionCompleted, fmt.Sprintf("catching up on %d out of %d ranges", total.LaggingRangeCount, total.RangeCount)
	}
	return total, 1, ""
}

func newRangeStatsCollector(processorCount int) rangeStatsByProcessorID {
	return rangeStatsByProcessorID{
		stats:          make(map[int32]*streampb.StreamEvent_RangeStats),
		processorCount: processorCount,
	}
}
