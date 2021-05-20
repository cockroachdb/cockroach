// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tests

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/tracingservice"
	"github.com/cockroachdb/cockroach/pkg/util/tracingservice/tracingservicepb"
	"github.com/gogo/protobuf/types"
	"github.com/stretchr/testify/require"
)

// testStructuredImpl is a testing implementation of Structured event.
type testStructuredImpl struct {
	*types.StringValue
}

var _ tracing.Structured = &testStructuredImpl{}

func (t *testStructuredImpl) String() string {
	return fmt.Sprintf("structured=%s", t.Value)
}

func newTestStructured(i string) *testStructuredImpl {
	return &testStructuredImpl{
		&types.StringValue{Value: i},
	}
}

func setupTraces(t1, t2 *tracing.Tracer) (uint64, uint64, func()) {
	// Start a root span on "node 1".
	root := t1.StartSpan("root", tracing.WithForceRealSpan())
	root.SetVerbose(true)
	root.RecordStructured(newTestStructured("root"))
	// Start a child span on "node 1".
	child := t1.StartSpan("root.child", tracing.WithParentAndAutoCollection(root))

	// Start a remote child span on "node 2".
	childRemoteChild := t2.StartSpan("root.child.remotechild", tracing.WithParentAndManualCollection(child.Meta()))
	childRemoteChild.RecordStructured(newTestStructured("root.child.remotechild"))
	// Start another remote child span on "node 2" that we finish.
	childRemoteChildFinished := t2.StartSpan("root.child.remotechilddone", tracing.WithParentAndManualCollection(child.Meta()))
	childRemoteChildFinished.Finish()
	child.ImportRemoteSpans(childRemoteChildFinished.GetRecording())

	// Start a root span on "node 2".
	root2 := t2.StartSpan("root2", tracing.WithForceRealSpan())
	root2.SetVerbose(true)
	root2.RecordStructured(newTestStructured("root2"))
	// Start a child span on "node 2".
	child2 := t2.StartSpan("root2.child", tracing.WithParentAndAutoCollection(root2))
	// Start a remote child span on "node 1".
	child2RemoteChild := t1.StartSpan("root2.child.remotechild", tracing.WithParentAndManualCollection(child2.Meta()))
	// Start another remote child span on "node 1".
	anotherChild2RemoteChild := t1.StartSpan("root2.child.remotechild2", tracing.WithParentAndManualCollection(child2.Meta()))
	return root.TraceID(), root2.TraceID(), func() {
		root.Finish()
		child.Finish()
		childRemoteChild.Finish()
		root2.Finish()
		child2.Finish()
		child2RemoteChild.Finish()
		anotherChild2RemoteChild.Finish()
	}
}

func TestTracingServiceGetSpanRecordings(t *testing.T) {
	defer leaktest.AfterTest(t)()

	traceNode1 := tracing.NewTracer()
	traceNode2 := tracing.NewTracer()
	tID1, _, cleanup := setupTraces(traceNode1, traceNode2)
	defer cleanup()

	s := tracingservice.NewTracingService(traceNode1)

	ctx := context.Background()
	resp, err := s.GetSpanRecordings(ctx, &tracingservicepb.SpanRecordingRequest{TraceID: tID1})
	require.NoError(t, err)
	require.NoError(t, tracing.TestingCheckRecordedSpans(resp.SpanRecordings, `
			span: root
				tags: _unfinished=1 _verbose=1
				event: structured=root
				span: root.child
					tags: _unfinished=1 _verbose=1
					span: root.child.remotechilddone
						tags: _verbose=1
`))
}
