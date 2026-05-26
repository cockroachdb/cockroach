// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tracing_test

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/backup/backuppb"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/require"
)

func TestAggregator(t *testing.T) {
	tr := tracing.NewTracer()
	ctx := context.Background()
	ctx, root := tr.StartSpanCtx(ctx, "root", tracing.WithRecording(tracingpb.RecordingVerbose))
	defer root.Finish()

	agg := tracing.TracingAggregatorForContext(ctx)
	ctx, withListener := tr.StartSpanCtx(ctx, "withListener",
		tracing.WithEventListeners(agg), tracing.WithParent(root))
	defer withListener.Finish()

	child := tr.StartSpan("child", tracing.WithParent(withListener))
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
	agg.ForEachAggregatedEvent(func(name string, event tracing.AggregatorEvent) {
		require.Equal(t, name, proto.MessageName(&backuppb.ExportStats{}))
		var es *backuppb.ExportStats
		var ok bool
		if es, ok = event.(*backuppb.ExportStats); !ok {
			t.Fatal("failed to cast event to expected type")
		}
		require.Equal(t, backuppb.ExportStats{
			NumFiles: 30,
			DataSize: 30,
			Duration: 2 * time.Minute,
		}, *es)
	})

	// Import the remote recording into its parent.
	rec := remoteChild.FinishAndGetConfiguredRecording()
	childChild.ImportRemoteRecording(rec)

	// Now, we expect the ExportStats from the remote child to show up in the
	// aggregator.
	agg.ForEachAggregatedEvent(func(name string, event tracing.AggregatorEvent) {
		require.Equal(t, name, proto.MessageName(&backuppb.ExportStats{}))
		var es *backuppb.ExportStats
		var ok bool
		if es, ok = event.(*backuppb.ExportStats); !ok {
			t.Fatal("failed to cast event to expected type")
		}
		require.Equal(t, backuppb.ExportStats{
			NumFiles: 60,
			DataSize: 60,
			Duration: 3 * time.Minute,
		}, *es)
	})
}
