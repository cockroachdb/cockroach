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
	"net"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/migration/nodelivenesstest"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/netutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/cockroachdb/cockroach/pkg/util/tracingservice/tracingservicepb"
	"github.com/gogo/protobuf/types"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

func setUpService(
	t testing.TB,
	rpcContext *rpc.Context,
	localNodeID roachpb.NodeID,
	remoteNodeID roachpb.NodeID,
	localTracer *tracing.Tracer,
	remoteTracer *tracing.Tracer,
) *TraceClientDialer {
	s := rpc.NewServer(rpcContext)
	remoteTraceServer := NewTraceService(remoteTracer)
	tracingservicepb.RegisterTraceServer(s, remoteTraceServer)
	ln, err := netutil.ListenAndServeGRPC(rpcContext.Stopper, s, util.TestAddr)
	require.NoError(t, err)

	s2 := rpc.NewServer(rpcContext)
	localBlobServer := NewTraceService(localTracer)
	require.NoError(t, err)
	tracingservicepb.RegisterTraceServer(s2, localBlobServer)
	ln2, err := netutil.ListenAndServeGRPC(rpcContext.Stopper, s2, util.TestAddr)
	require.NoError(t, err)

	localDialer := nodedialer.New(rpcContext,
		func(nodeID roachpb.NodeID) (net.Addr, error) {
			if nodeID == remoteNodeID {
				return ln.Addr(), nil
			} else if nodeID == localNodeID {
				return ln2.Addr(), nil
			}
			return nil, errors.Errorf("node %d not found", nodeID)
		},
	)

	testNodeLiveness := nodelivenesstest.New(2 /* numNodes */)
	return NewTraceClientDialer(
		localNodeID,
		localDialer,
		testNodeLiveness,
		localTracer,
	)
}

func TestTraceClientGetSpanRecordings(t *testing.T) {
	localNodeID := roachpb.NodeID(1)
	remoteNodeID := roachpb.NodeID(2)
	localTracer := tracing.NewTracer()
	remoteTracer := tracing.NewTracer()

	stopper := stop.NewStopper()
	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	rpcContext := rpc.NewInsecureTestingContext(clock, stopper)
	rpcContext.TestingAllowNamedRPCToAnonymousServer = true

	traceDialer := setUpService(t, rpcContext, localNodeID, remoteNodeID, localTracer, remoteTracer)
	localTID, remoteTID := setupTraces(localTracer, remoteTracer)

	ctx := context.Background()
	t.Run("fetch-localTID-recordings", func(t *testing.T) {
		recordedSpan, err := traceDialer.GetSpanRecordingsFromCluster(ctx, localTID)
		require.NoError(t, err)

		expected := []tracingpb.RecordedSpan{
			{
				TraceID: localTID,
				InternalStructured: []*types.Any{{TypeUrl: "type.googleapis.com/google.protobuf.StringValue",
					Value: []byte{0xa, 0x4, 0x72, 0x6f, 0x6f, 0x74},
				}},
				Operation: "root",
			},
			{
				TraceID:   localTID,
				Operation: "root.child",
			},
			// This is child on node 2 that has not been imported.
			{
				TraceID:   localTID,
				Operation: "root.child.remotechild",
				InternalStructured: []*types.Any{{TypeUrl: "type.googleapis.com/google.protobuf.StringValue",
					Value: []byte{0xa, 0x16, 0x72, 0x6f, 0x6f, 0x74, 0x2e, 0x63, 0x68, 0x69, 0x6c, 0x64, 0x2e, 0x72, 0x65, 0x6d, 0x6f, 0x74, 0x65, 0x63, 0x68, 0x69, 0x6c, 0x64},
				}},
			},
			{
				TraceID:   localTID,
				Operation: "root.child.remotechilddone",
			},
		}
		checkSpanRecordings(t, expected, recordedSpan)
	})

	// The traceDialer is running on node 1, so most of the recordings for this
	// subtest will be passed back by node 2 over RPC.
	t.Run("fetch-remoteTID-recordings", func(t *testing.T) {
		recordedSpan, err := traceDialer.GetSpanRecordingsFromCluster(ctx, remoteTID)
		require.NoError(t, err)

		expected := []tracingpb.RecordedSpan{
			{
				TraceID: remoteTID,
				InternalStructured: []*types.Any{{TypeUrl: "type.googleapis.com/google.protobuf.StringValue",
					Value: []byte{0xa, 0x5, 0x72, 0x6f, 0x6f, 0x74, 0x32},
				}},
				Operation: "root2",
			},
			{
				TraceID:   remoteTID,
				Operation: "root2.child",
			},
			{
				TraceID:   remoteTID,
				Operation: "root2.child.remotechild",
			},
		}
		checkSpanRecordings(t, expected, recordedSpan)
	})
}
