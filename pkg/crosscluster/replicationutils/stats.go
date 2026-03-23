// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package replicationutils

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/util/rangescanstats/rangescanstatspb"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	pbtypes "github.com/gogo/protobuf/types"
)

// StreamRangeStatsToProgressMeta converts a range statistics from a rangefeed
// StreamEvent and converts it to a ProducerMetadata that can be passed through
// the DistSQL pipeline.
func StreamRangeStatsToProgressMeta(
	flowCtx *execinfra.FlowCtx, procID int32, stats *rangescanstatspb.RangeStats,
) (*execinfrapb.ProducerMetadata, error) {
	asAny, err := pbtypes.MarshalAny(stats)
	if err != nil {
		return nil, errors.Wrap(err, "unable to convert range stats to any proto")
	}
	return &execinfrapb.ProducerMetadata{
		BulkProcessorProgress: &execinfrapb.RemoteProducerMetadata_BulkProcessorProgress{
			NodeID:          flowCtx.NodeID.SQLInstanceID(),
			FlowID:          flowCtx.ID,
			ProcessorID:     procID,
			ProgressDetails: *asAny,
		},
	}, nil
}

// AggregateRangeStatsCollector collects rangefeed StreamEvent range stats from
// multiple processors and aggregates them into single metrics.
type AggregateRangeStatsCollector struct {
	mu             syncutil.Mutex
	stats          map[int32]*rangescanstatspb.RangeStats
	processorCount int
}

func NewAggregateRangeStatsCollector(processorCount int) AggregateRangeStatsCollector {
	return AggregateRangeStatsCollector{
		stats:          make(map[int32]*rangescanstatspb.RangeStats),
		processorCount: processorCount,
	}
}

// Add adds range states from a processor to the collector.
func (r *AggregateRangeStatsCollector) Add(processorID int32, stats *rangescanstatspb.RangeStats) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.stats[processorID] = stats
}

// RollupStats aggregates the collected stats and returns the total range stats,
// the fraction of ranges that have reached the steady state, and a human
// readable status message.
func (r *AggregateRangeStatsCollector) RollupStats() (
	rangescanstatspb.RangeStats,
	float32,
	string,
) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var total rangescanstatspb.RangeStats
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
		return rangescanstatspb.RangeStats{}, 0, fmt.Sprintf("starting streams (%d out of %d)", len(r.stats), r.processorCount)
	}
	if !initialScanComplete {
		return total, fractionCompleted, fmt.Sprintf("initial scan on %d out of %d ranges", total.ScanningRangeCount, total.RangeCount)
	}
	if total.LaggingRangeCount != 0 {
		return total, fractionCompleted, fmt.Sprintf("catching up on %d out of %d ranges", total.LaggingRangeCount, total.RangeCount)
	}
	return total, 1, ""
}
