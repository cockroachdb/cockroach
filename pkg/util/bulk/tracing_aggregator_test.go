// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package bulk_test

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl/backuppb"
	"github.com/cockroachdb/cockroach/pkg/kv/bulk/bulkpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/bulk"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/stretchr/testify/require"
)

func TestAggregator(t *testing.T) {
	tr := tracing.NewTracer()
	ctx := context.Background()
	ctx, root := tr.StartSpanCtx(ctx, "root", tracing.WithRecording(tracingpb.RecordingVerbose))
	defer root.Finish()

	ctx, agg := bulk.MakeTracingAggregatorWithSpan(ctx, "mockAggregator", tr)
	aggSp := tracing.SpanFromContext(ctx)
	defer agg.Close()

	child := tr.StartSpan("child", tracing.WithParent(root),
		tracing.WithEventListeners(agg))
	defer child.Finish()
	child.RecordStructured(&backuppb.ExportStats{
		NumFiles: 10,
		DataSize: 10,
		Duration: time.Minute,
	})

	_, childChild := tracing.ChildSpan(ctx, "childChild")
	defer childChild.Finish()
	childChild.RecordStructured(&backuppb.ExportStats{
		NumFiles: 20,
		DataSize: 20,
		Duration: time.Minute,
	})

	remoteChild := tr.StartSpan("remoteChild", tracing.WithRemoteParentFromSpanMeta(childChild.Meta()))
	remoteChild.RecordStructured(&backuppb.ExportStats{
		NumFiles: 30,
		DataSize: 30,
		Duration: time.Minute,
	})

	// We only expect to see the aggregated stats from the local children since we
	// have not imported the remote children's Recording.
	exportStatsTag, found := aggSp.GetLazyTag("ExportStats")
	require.True(t, found)
	var es *backuppb.ExportStats
	var ok bool
	if es, ok = exportStatsTag.(*backuppb.ExportStats); !ok {
		t.Fatal("failed to cast LazyTag to expected type")
	}
	require.Equal(t, backuppb.ExportStats{
		NumFiles: 30,
		DataSize: 30,
		Duration: 2 * time.Minute,
	}, *es)

	// Import the remote recording into its parent.
	rec := remoteChild.FinishAndGetConfiguredRecording()
	childChild.ImportRemoteRecording(rec)

	// Now, we expect the ExportStats from the remote child to show up in the
	// aggregator.
	exportStatsTag, found = aggSp.GetLazyTag("ExportStats")
	require.True(t, found)
	if es, ok = exportStatsTag.(*backuppb.ExportStats); !ok {
		t.Fatal("failed to cast LazyTag to expected type")
	}
	require.Equal(t, backuppb.ExportStats{
		NumFiles: 60,
		DataSize: 60,
		Duration: 3 * time.Minute,
	}, *es)
}

func TestIngestionPerformanceStatsAggregation(t *testing.T) {
	tr := tracing.NewTracer()
	ctx := context.Background()

	makeEvent := func(v int64, sendWaitByStore map[roachpb.StoreID]time.Duration) *bulkpb.IngestionPerformanceStats {
		return &bulkpb.IngestionPerformanceStats{
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
		}
	}

	requireStatsTag := func(aggSp *tracing.Span, expected *bulkpb.IngestionPerformanceStats) {
		exportStatsTag, found := aggSp.GetLazyTag(expected.Tag())
		require.True(t, found)
		var actual *bulkpb.IngestionPerformanceStats
		var ok bool
		if actual, ok = exportStatsTag.(*bulkpb.IngestionPerformanceStats); !ok {
			t.Fatal("failed to cast LazyTag to expected type")
		}

		require.Equal(t, expected, actual)
	}

	// First, start a root tracing span with a tracing aggregator.
	ctx, root := tr.StartSpanCtx(ctx, "root", tracing.WithRecording(tracingpb.RecordingVerbose))
	defer root.Finish()
	ctx, agg := bulk.MakeTracingAggregatorWithSpan(ctx, "mockAggregator", tr)
	aggSp := tracing.SpanFromContext(ctx)
	defer agg.Close()

	// Second, start a child span on the root that also has its own tracing
	// aggregator.
	_, child1 := tracing.ChildSpan(ctx, "child1")
	defer child1.Finish()
	child1Ctx, child1Agg := bulk.MakeTracingAggregatorWithSpan(ctx, "mockChildAggregator", tr)
	child1AggSp := tracing.SpanFromContext(child1Ctx)
	defer child1Agg.Close()

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
	requireStatsTag(child1AggSp, makeEvent(3, map[roachpb.StoreID]time.Duration{1: 203, 2: 202}))
	requireStatsTag(aggSp, makeEvent(6, map[roachpb.StoreID]time.Duration{1: 203, 2: 405, 3: 303}))
}
