// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package service

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingservicepb"
	"github.com/stretchr/testify/require"
)

func TestTracingServiceGetSpanRecordings(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tracer1 := tracing.NewTracer()
	setupTraces := func() (tracingpb.TraceID, func()) {
		// Start a root span.
		root1 := tracer1.StartSpan("root1", tracing.WithRecording(tracing.RecordingVerbose))
		child1 := tracer1.StartSpan("root1.child", tracing.WithParent(root1))
		child2 := tracer1.StartSpan("root1.child.detached", tracing.WithParent(child1), tracing.WithDetachedRecording())
		// Create a span that will be added to the tracers' active span map, but
		// will share the same traceID as root.
		child3 := tracer1.StartSpan("root1.remote_child", tracing.WithRemoteParentFromSpanMeta(child2.Meta()))

		time.Sleep(10 * time.Millisecond)

		// Start span with different trace ID.
		root2 := tracer1.StartSpan("root2", tracing.WithRecording(tracing.RecordingVerbose))
		root2.Record("root2")

		return root1.TraceID(), func() {
			for _, span := range []*tracing.Span{root1, child1, child2, child3, root2} {
				span.Finish()
			}
		}
	}

	traceID1, cleanup := setupTraces()
	defer cleanup()

	ctx := context.Background()
	s := New(tracer1)
	resp, err := s.GetSpanRecordings(ctx, &tracingservicepb.GetSpanRecordingsRequest{TraceID: traceID1})
	require.NoError(t, err)
	// We expect two Recordings.
	// 1. root1, root1.child
	// 2. fork1
	require.Equal(t, 2, len(resp.Recordings))
	// Sort the response based on the start time of the root spans in the
	// recordings.
	sort.SliceStable(resp.Recordings, func(i, j int) bool {
		return resp.Recordings[i].RecordedSpans[0].StartTime.Before(resp.Recordings[j].RecordedSpans[0].StartTime)
	})
	require.NoError(t, tracing.CheckRecordedSpans(resp.Recordings[0].RecordedSpans, `
			span: root1
				tags: _unfinished=1 _verbose=1
				span: root1.child
					tags: _unfinished=1 _verbose=1
					span: root1.child.detached
						tags: _unfinished=1 _verbose=1
`))
	require.NoError(t, tracing.CheckRecordedSpans(resp.Recordings[1].RecordedSpans, `
			span: root1.remote_child
				tags: _unfinished=1 _verbose=1
`))
}
