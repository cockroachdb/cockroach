// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package bulkpb

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/codahale/hdrhistogram"
	proto "github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/require"
)

func TestIngestionPerformanceStatsAggregation(t *testing.T) {
	tr := tracing.NewTracer()
	ctx := context.Background()

	makeEvent := func(v int64, sendWaitByStore map[roachpb.StoreID]time.Duration) *IngestionPerformanceStats {
		return &IngestionPerformanceStats{
			LogicalDataSize:   v,
			SSTDataSize:       v,
			BufferFlushes:     v,
			FlushesDueToSize:  v,
			Batches:           v,
			BatchesDueToRange: v,
			BatchesDueToSize:  v,
			SplitRetries:      v,
			Splits:            v,
			Scatters:          v,
			ScatterMoved:      v,
			AsWrites:          v,
			FillWait:          time.Duration(v),
			SortWait:          time.Duration(v),
			FlushWait:         time.Duration(v),
			BatchWait:         time.Duration(v),
			SendWait:          time.Duration(v),
			SplitWait:         time.Duration(v),
			ScatterWait:       time.Duration(v),
			CommitWait:        time.Duration(v),
			Duration:          time.Duration(v),
			SendWaitByStore:   sendWaitByStore,
			BatchWaitHist:     nil,
			SstSizeHist:       nil,
		}
	}

	assertAggContainsStats := func(t *testing.T, agg *tracing.TracingAggregator, expected *IngestionPerformanceStats) {
		agg.ForEachAggregatedEvent(func(name string, event tracing.AggregatorEvent) {
			require.Equal(t, name, proto.MessageName(expected))
			var actual *IngestionPerformanceStats
			var ok bool
			if actual, ok = event.(*IngestionPerformanceStats); !ok {
				t.Fatal("failed to cast event to expected type")
			}
			require.Equal(t, expected, actual)
		})
	}

	// First, start a root tracing span with a tracing aggregator.
	ctx, root := tr.StartSpanCtx(ctx, "root", tracing.WithRecording(tracingpb.RecordingVerbose))
	defer root.Finish()
	agg := tracing.TracingAggregatorForContext(ctx)
	ctx, withListener := tr.StartSpanCtx(ctx, "withListener",
		tracing.WithEventListeners(agg), tracing.WithParent(root))
	defer withListener.Finish()

	// Second, start a child span on the root that also has its own tracing
	// aggregator.
	child1Agg := tracing.TracingAggregatorForContext(ctx)
	child1Ctx, child1AggSp := tr.StartSpanCtx(ctx, "child1",
		tracing.WithEventListeners(child1Agg), tracing.WithParent(withListener))
	defer child1AggSp.Finish()

	// In addition, start a child span on the first child span.
	_, child1Child := tracing.ChildSpan(child1Ctx, "child1Child")
	defer child1Child.Finish()

	// Finally, start a second child span on the root.
	_, child2 := tracing.ChildSpan(ctx, "child2")
	defer child2.Finish()

	// Record a structured event on all child spans.
	child1AggSp.RecordStructured(makeEvent(1, map[roachpb.StoreID]time.Duration{1: 101}))
	child1Child.RecordStructured(makeEvent(2, map[roachpb.StoreID]time.Duration{1: 102, 2: 202}))
	child2.RecordStructured(makeEvent(3, map[roachpb.StoreID]time.Duration{2: 203, 3: 303}))

	// Verify that the root and child1 aggregators has the expected aggregated
	// stats.
	t.Run("child1Agg", func(t *testing.T) {
		child1AggLatencyHist := hdrhistogram.New(minLatency.Nanoseconds(), maxLatency.Nanoseconds(), sigFigs)
		require.NoError(t, child1AggLatencyHist.RecordValue(1))
		require.NoError(t, child1AggLatencyHist.RecordValue(2))
		child1AggLatencyHistSnapshot := child1AggLatencyHist.Export()
		child1AggExpectedEvent := makeEvent(3, map[roachpb.StoreID]time.Duration{1: 203, 2: 202})
		child1AggExpectedEvent.BatchWaitHist = &HistogramData{
			DataType:              HistogramDataTypeLatency,
			LowestTrackableValue:  child1AggLatencyHistSnapshot.LowestTrackableValue,
			HighestTrackableValue: child1AggLatencyHistSnapshot.HighestTrackableValue,
			SignificantFigures:    child1AggLatencyHistSnapshot.SignificantFigures,
			Counts:                child1AggLatencyHistSnapshot.Counts,
		}
		child1AggDataHist := hdrhistogram.New(minBytes, maxBytes, sigFigs)
		require.NoError(t, child1AggDataHist.RecordValue(1))
		require.NoError(t, child1AggDataHist.RecordValue(2))
		child1AggDataHistSnapshot := child1AggDataHist.Export()
		child1AggExpectedEvent.SstSizeHist = &HistogramData{
			DataType:              HistogramDataTypeBytes,
			LowestTrackableValue:  child1AggDataHistSnapshot.LowestTrackableValue,
			HighestTrackableValue: child1AggDataHistSnapshot.HighestTrackableValue,
			SignificantFigures:    child1AggDataHistSnapshot.SignificantFigures,
			Counts:                child1AggDataHistSnapshot.Counts,
		}
		assertAggContainsStats(t, child1Agg, child1AggExpectedEvent)
	})

	t.Run("root", func(t *testing.T) {
		aggLatencyHist := hdrhistogram.New(minLatency.Nanoseconds(), maxLatency.Nanoseconds(), sigFigs)
		require.NoError(t, aggLatencyHist.RecordValue(1))
		require.NoError(t, aggLatencyHist.RecordValue(2))
		require.NoError(t, aggLatencyHist.RecordValue(3))
		aggLatencyHistSnapshot := aggLatencyHist.Export()

		aggExpectedEvent := makeEvent(6, map[roachpb.StoreID]time.Duration{1: 203, 2: 405, 3: 303})
		aggExpectedEvent.BatchWaitHist = &HistogramData{
			DataType:              HistogramDataTypeLatency,
			LowestTrackableValue:  aggLatencyHistSnapshot.LowestTrackableValue,
			HighestTrackableValue: aggLatencyHistSnapshot.HighestTrackableValue,
			SignificantFigures:    aggLatencyHistSnapshot.SignificantFigures,
			Counts:                aggLatencyHistSnapshot.Counts,
		}
		aggDataHist := hdrhistogram.New(minBytes, maxBytes, sigFigs)
		require.NoError(t, aggDataHist.RecordValue(1))
		require.NoError(t, aggDataHist.RecordValue(2))
		require.NoError(t, aggDataHist.RecordValue(3))
		aggDataHistSnapshot := aggDataHist.Export()
		aggExpectedEvent.SstSizeHist = &HistogramData{
			DataType:              HistogramDataTypeBytes,
			LowestTrackableValue:  aggDataHistSnapshot.LowestTrackableValue,
			HighestTrackableValue: aggDataHistSnapshot.HighestTrackableValue,
			SignificantFigures:    aggDataHistSnapshot.SignificantFigures,
			Counts:                aggDataHistSnapshot.Counts,
		}
		assertAggContainsStats(t, agg, aggExpectedEvent)
	})
}
