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
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingservicepb"
	"github.com/stretchr/testify/require"
)

func TestTracingServiceGetSpanRecordings(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tracer1 := tracing.NewTracer()
	setupTraces := func() (uint64, func()) {
		// Start a root span.
		root1 := tracer1.StartSpan("root1", tracing.WithForceRealSpan())
		root1.SetVerbose(true)

		child1 := tracer1.StartSpan("root1.child", tracing.WithParentAndAutoCollection(root1))

		time.Sleep(10 * time.Millisecond)

		// Create a span that will be added to the tracers' active span map, but
		// will share the same traceID as root.
		fork1 := tracer1.StartSpan("fork1", tracing.WithParentAndManualCollection(root1.Meta()))

		// Start span with different trace ID.
		root2 := tracer1.StartSpan("root2", tracing.WithForceRealSpan())
		root2.SetVerbose(true)
		root2.Record("root2")

		return root1.TraceID(), func() {
			for _, span := range []*tracing.Span{root1, child1, fork1, root2} {
				span.Finish()
			}
		}
	}

	traceID1, cleanup := setupTraces()
	defer cleanup()

	ctx := context.Background()
	s := New(tracer1)
	resp, err := s.GetSpanRecordings(ctx, &tracingservicepb.SpanRecordingRequest{TraceID: traceID1})
	require.NoError(t, err)
	sort.SliceStable(resp.SpanRecordings, func(i, j int) bool {
		return resp.SpanRecordings[i].StartTime.Before(resp.SpanRecordings[j].StartTime)
	})
	require.NoError(t, tracing.TestingCheckRecordedSpans(resp.SpanRecordings, `
			span: root1
				tags: _unfinished=1 _verbose=1
				span: root1.child
					tags: _unfinished=1 _verbose=1
				span: fork1
					tags: _unfinished=1 _verbose=1
`))
}
