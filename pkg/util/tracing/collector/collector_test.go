// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package collector_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/collector"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
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

// setupTraces takes two tracers (potentially on different nodes), and creates
// two span hierarchies as depicted below. The method returns the traceIDs for
// both these span hierarchies, along with a cleanup method to Finish() all the
// opened spans.
//
// Trace for t1:
// -------------
// root													<-- traceID1
// 		root.child								<-- traceID1
// root2.child.remotechild 			<-- traceID2
// root2.child.remotechild2 		<-- traceID2
//
// Trace for t2:
// -------------
// root.child.remotechild				<-- traceID1
// root.child.remotechilddone		<-- traceID1
// root2												<-- traceID2
// 		root2.child								<-- traceID2
func setupTraces(t1, t2 *tracing.Tracer) (tracingpb.TraceID, tracingpb.TraceID, func()) {
	// Start a root span on "node 1".
	root := t1.StartSpan("root", tracing.WithRecording(tracing.RecordingVerbose))
	root.RecordStructured(newTestStructured("root"))

	time.Sleep(10 * time.Millisecond)

	// Start a child span on "node 1".
	child := t1.StartSpan("root.child", tracing.WithParent(root))

	// Sleep a bit so that everything that comes afterwards has higher timestamps
	// than the one we just assigned. Otherwise the sorting is not deterministic.
	time.Sleep(10 * time.Millisecond)

	// Start a remote child span on "node 2".
	childRemoteChild := t2.StartSpan("root.child.remotechild", tracing.WithRemoteParentFromSpanMeta(child.Meta()))
	childRemoteChild.RecordStructured(newTestStructured("root.child.remotechild"))

	time.Sleep(10 * time.Millisecond)

	// Start another remote child span on "node 2" that we finish.
	childRemoteChildFinished := t2.StartSpan("root.child.remotechilddone", tracing.WithRemoteParentFromSpanMeta(child.Meta()))
	child.ImportRemoteSpans(childRemoteChildFinished.FinishAndGetRecording(tracing.RecordingVerbose))

	// Start a root span on "node 2".
	root2 := t2.StartSpan("root2", tracing.WithRecording(tracing.RecordingVerbose))
	root2.RecordStructured(newTestStructured("root2"))

	// Start a child span on "node 2".
	child2 := t2.StartSpan("root2.child", tracing.WithParent(root2))
	// Start a remote child span on "node 1".
	child2RemoteChild := t1.StartSpan("root2.child.remotechild", tracing.WithRemoteParentFromSpanMeta(child2.Meta()))

	time.Sleep(10 * time.Millisecond)

	// Start another remote child span on "node 1".
	anotherChild2RemoteChild := t1.StartSpan("root2.child.remotechild2", tracing.WithRemoteParentFromSpanMeta(child2.Meta()))
	return root.TraceID(), root2.TraceID(), func() {
		for _, span := range []*tracing.Span{root, child, childRemoteChild, root2, child2,
			child2RemoteChild, anotherChild2RemoteChild} {
			span.Finish()
		}
	}
}

func TestTracingCollectorGetSpanRecordings(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	args := base.TestClusterArgs{}
	tc := testcluster.StartTestCluster(t, 2 /* nodes */, args)
	defer tc.Stopper().Stop(ctx)

	localTracer := tc.Server(0).TracerI().(*tracing.Tracer)
	remoteTracer := tc.Server(1).TracerI().(*tracing.Tracer)

	traceCollector := collector.New(
		tc.Server(0).NodeDialer().(*nodedialer.Dialer),
		tc.Server(0).NodeLiveness().(*liveness.NodeLiveness), localTracer)
	localTraceID, remoteTraceID, cleanup := setupTraces(localTracer, remoteTracer)
	defer cleanup()

	getSpansFromAllNodes := func(traceID tracingpb.TraceID) map[roachpb.NodeID][]tracing.Recording {
		res := make(map[roachpb.NodeID][]tracing.Recording)

		var iter *collector.Iterator
		var err error
		for iter, err = traceCollector.StartIter(ctx, traceID); err == nil && iter.Valid(); iter.Next() {
			nodeID, recording := iter.Value()
			res[nodeID] = append(res[nodeID], recording)
		}
		require.NoError(t, err)
		require.NoError(t, iter.Error())
		return res
	}

	t.Run("fetch-local-recordings", func(t *testing.T) {
		nodeRecordings := getSpansFromAllNodes(localTraceID)
		node1Recordings := nodeRecordings[roachpb.NodeID(1)]
		require.Equal(t, 1, len(node1Recordings))
		require.NoError(t, tracing.CheckRecordedSpans(node1Recordings[0], `
				span: root
					tags: _unfinished=1 _verbose=1
					event: structured=root
					span: root.child
						tags: _unfinished=1 _verbose=1
						span: root.child.remotechilddone
							tags: _verbose=1
	`))
		node2Recordings := nodeRecordings[roachpb.NodeID(2)]
		require.Equal(t, 1, len(node2Recordings))
		require.NoError(t, tracing.CheckRecordedSpans(node2Recordings[0], `
				span: root.child.remotechild
					tags: _unfinished=1 _verbose=1
					event: structured=root.child.remotechild
	`))
	})

	// The traceCollector is running on node 1, so most of the recordings for this
	// subtest will be passed back by node 2 over RPC.
	t.Run("fetch-remote-recordings", func(t *testing.T) {
		nodeRecordings := getSpansFromAllNodes(remoteTraceID)
		node1Recordings := nodeRecordings[roachpb.NodeID(1)]
		require.Equal(t, 2, len(node1Recordings))
		require.NoError(t, tracing.CheckRecordedSpans(node1Recordings[0], `
				span: root2.child.remotechild
					tags: _unfinished=1 _verbose=1
	`))
		require.NoError(t, tracing.CheckRecordedSpans(node1Recordings[1], `
				span: root2.child.remotechild2
					tags: _unfinished=1 _verbose=1
	`))

		node2Recordings := nodeRecordings[roachpb.NodeID(2)]
		require.Equal(t, 1, len(node2Recordings))
		require.NoError(t, tracing.CheckRecordedSpans(node2Recordings[0], `
				span: root2
					tags: _unfinished=1 _verbose=1
					event: structured=root2
					span: root2.child
						tags: _unfinished=1 _verbose=1
	`))
	})
}
