// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tracingservice

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/cockroachdb/cockroach/pkg/util/tracingservice/tracingservicepb"
	"github.com/gogo/protobuf/types"
	"github.com/stretchr/testify/require"
)

func setupTraces(localTracer, remoteTracer *tracing.Tracer) (uint64, uint64) {
	// Start a root span on "node 1".
	root := localTracer.StartSpan("root", tracing.WithForceRealSpan())
	root.SetVerbose(true)
	root.RecordStructured(&types.StringValue{Value: "root"})
	// Start a child span on "node 1".
	child := localTracer.StartSpan("root.child", tracing.WithParentAndAutoCollection(root))

	// Start a remote child span on "node 2".
	childChild := remoteTracer.StartSpan("root.child.remotechild", tracing.WithParentAndManualCollection(child.Meta()))
	childChild.RecordStructured(&types.StringValue{Value: "root.child.remotechild"})
	// Start another remote child span on "node 2" that we finish.
	childChildFinished := remoteTracer.StartSpan("root.child.remotechilddone", tracing.WithParentAndManualCollection(child.Meta()))
	child.ImportRemoteSpans(childChildFinished.GetRecording())
	childChildFinished.Finish()

	// Start a root span on "node 2".
	root2 := remoteTracer.StartSpan("root2", tracing.WithForceRealSpan())
	root2.SetVerbose(true)
	root2.RecordStructured(&types.StringValue{Value: "root2"})
	// Start a child span on "node 2".
	child2 := remoteTracer.StartSpan("root2.child", tracing.WithParentAndAutoCollection(root2))
	// Start a remote child span on "node 1".
	_ = localTracer.StartSpan("root2.child.remotechild", tracing.WithParentAndManualCollection(child2.Meta()))
	return root.TraceID(), root2.TraceID()
}

func checkSpanRecordings(t *testing.T, expected, actual []tracingpb.RecordedSpan) {
	for i, rec := range expected {
		actualRec := actual[i]
		require.Equal(t, rec.TraceID, actualRec.TraceID)
		require.Equal(t, rec.Operation, actualRec.Operation)
		require.Equal(t, rec.InternalStructured, actualRec.InternalStructured)
	}
}

func TestTraceServiceGetSpanRecordings(t *testing.T) {
	traceNode1 := tracing.NewTracer()
	traceNode2 := tracing.NewTracer()
	tID1, tID2 := setupTraces(traceNode1, traceNode2)

	s1 := NewTraceService(traceNode1)
	s2 := NewTraceService(traceNode2)

	ctx := context.Background()
	t.Run("node1TraceID1", func(t *testing.T) {
		respTrace1TID1, err := s1.GetSpanRecordings(ctx, &tracingservicepb.SpanRecordingRequest{TraceID: tID1})
		require.NoError(t, err)
		expectedNode1TID1 := []tracingpb.RecordedSpan{
			{
				TraceID: tID1,
				InternalStructured: []*types.Any{{TypeUrl: "type.googleapis.com/google.protobuf.StringValue",
					Value: []byte{0xa, 0x4, 0x72, 0x6f, 0x6f, 0x74},
				}},
				Operation: "root",
			},
			{
				TraceID:   tID1,
				Operation: "root.child",
			},
			{
				TraceID:   tID1,
				Operation: "root.child.remotechilddone",
			},
		}
		checkSpanRecordings(t, expectedNode1TID1, respTrace1TID1.SpanRecordings)

	})

	t.Run("node2TraceID1", func(t *testing.T) {
		respNode2TID1, err := s2.GetSpanRecordings(ctx, &tracingservicepb.SpanRecordingRequest{TraceID: tID1})
		require.NoError(t, err)
		expectedNode2TID2 := []tracingpb.RecordedSpan{
			{
				TraceID: tID1,
				InternalStructured: []*types.Any{{TypeUrl: "type.googleapis.com/google.protobuf.StringValue",
					Value: []byte{0xa, 0x16, 0x72, 0x6f, 0x6f, 0x74, 0x2e, 0x63, 0x68, 0x69, 0x6c, 0x64, 0x2e, 0x72, 0x65, 0x6d, 0x6f, 0x74, 0x65, 0x63, 0x68, 0x69, 0x6c, 0x64},
				}},
				Operation: "root.child.remotechild",
			},
		}
		checkSpanRecordings(t, expectedNode2TID2, respNode2TID1.SpanRecordings)
	})

	t.Run("node1TraceID2", func(t *testing.T) {
		respNode1TID2, err := s1.GetSpanRecordings(ctx, &tracingservicepb.SpanRecordingRequest{TraceID: tID2})
		require.NoError(t, err)
		expectedNode1TID2 := []tracingpb.RecordedSpan{
			{
				TraceID:   tID2,
				Operation: "root2.child.remotechild",
			},
		}
		checkSpanRecordings(t, expectedNode1TID2, respNode1TID2.SpanRecordings)
	})

	t.Run("node2TraceID2", func(t *testing.T) {
		respNode2TID2, err := s2.GetSpanRecordings(ctx, &tracingservicepb.SpanRecordingRequest{TraceID: tID2})
		require.NoError(t, err)
		expectedNode2TID2 := []tracingpb.RecordedSpan{
			{
				TraceID: tID2,
				InternalStructured: []*types.Any{{TypeUrl: "type.googleapis.com/google.protobuf.StringValue",
					Value: []byte{0xa, 0x5, 0x72, 0x6f, 0x6f, 0x74, 0x32},
				}},
				Operation: "root2",
			},
			{
				TraceID:   tID2,
				Operation: "root2.child",
			},
		}
		checkSpanRecordings(t, expectedNode2TID2, respNode2TID2.SpanRecordings)
	})
}
