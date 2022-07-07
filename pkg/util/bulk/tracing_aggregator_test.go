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
