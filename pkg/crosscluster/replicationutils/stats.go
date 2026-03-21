// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package replicationutils

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
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
//
// During a mixed-version upgrade from 26.1 to 26.2, we marshal using the old
// streampb.StreamEvent_RangeStats type so that both 26.1 and 26.2 nodes can
// unmarshal the Any proto. Once the cluster is fully on 26.2, we switch to
// the canonical rangescanstatspb.RangeStats type.
func StreamRangeStatsToProgressMeta(
	ctx context.Context, flowCtx *execinfra.FlowCtx, procID int32, stats *rangescanstatspb.RangeStats,
) (*execinfrapb.ProducerMetadata, error) {
	var asAny *pbtypes.Any
	var err error
	if flowCtx.Cfg.Settings.Version.IsActive(ctx, clusterversion.V26_2) {
		asAny, err = pbtypes.MarshalAny(stats)
	} else {
		// Use the old type for compatibility with 26.1 nodes.
		oldStats := &streampb.StreamEvent_RangeStats{
			RangeCount:         stats.RangeCount,
			ScanningRangeCount: stats.ScanningRangeCount,
			LaggingRangeCount:  stats.LaggingRangeCount,
		}
		asAny, err = pbtypes.MarshalAny(oldStats)
	}
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

// UnmarshalRangeStats attempts to unmarshal range stats from an Any proto,
// trying both the new rangescanstatspb.RangeStats type and the old
// streampb.StreamEvent_RangeStats type for mixed-version compatibility.
//
// TODO(kev-cao): Remove the fallback to StreamEvent_RangeStats in 26.3.
func UnmarshalRangeStats(any *pbtypes.Any) (*rangescanstatspb.RangeStats, error) {
	var stats rangescanstatspb.RangeStats
	if err := pbtypes.UnmarshalAny(any, &stats); err != nil {
		// Try the old type for mixed-version compatibility with 26.1 nodes.
		var oldStats streampb.StreamEvent_RangeStats
		if err := pbtypes.UnmarshalAny(any, &oldStats); err != nil {
			return nil, errors.Wrap(err, "unable to unmarshal progress details")
		}
		stats = rangescanstatspb.RangeStats{
			RangeCount:         oldStats.RangeCount,
			ScanningRangeCount: oldStats.ScanningRangeCount,
			LaggingRangeCount:  oldStats.LaggingRangeCount,
		}
	}
	return &stats, nil
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
