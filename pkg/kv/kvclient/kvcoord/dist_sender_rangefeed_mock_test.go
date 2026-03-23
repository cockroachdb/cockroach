// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvcoord

import (
	"context"
	"fmt"
	"net"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangecache/rangecachemock"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb/kvpbmock"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	gomock "github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
)

// Tests that the range feed handles transport errors appropriately. In
// particular, that when encountering other decommissioned nodes it will refresh
// its range descriptor and retry, but if this node is decommissioned it will
// bail out. Regression test for:
// https://github.com/cockroachdb/cockroach/issues/66636
func TestDistSenderRangeFeedRetryOnTransportErrors(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	for _, spec := range []struct {
		errorCode   codes.Code
		expectRetry bool
	}{
		{codes.FailedPrecondition, true}, // target node is decommissioned; retry
		{codes.PermissionDenied, false},  // this node is decommissioned; abort
		{codes.Unauthenticated, false},   // this node is not part of cluster; abort
	} {
		t.Run(fmt.Sprintf("transport_error=%s", spec.errorCode),
			func(t *testing.T) {
				clock := hlc.NewClockForTesting(nil)
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				stopper := stop.NewStopper()
				defer stopper.Stop(ctx)
				rpcContext := rpc.NewInsecureTestingContext(ctx, clock, stopper)
				g := makeGossip(t, stopper, rpcContext)

				desc := roachpb.RangeDescriptor{
					RangeID:    1,
					Generation: 1,
					StartKey:   roachpb.RKeyMin,
					EndKey:     roachpb.RKeyMax,
					InternalReplicas: []roachpb.ReplicaDescriptor{
						{NodeID: 1, StoreID: 1, ReplicaID: 1},
						{NodeID: 2, StoreID: 2, ReplicaID: 2},
					},
				}
				for _, repl := range desc.InternalReplicas {
					require.NoError(t, g.AddInfoProto(
						gossip.MakeNodeIDKey(repl.NodeID),
						newNodeDesc(repl.NodeID),
						gossip.NodeDescriptorTTL,
					))
				}

				ctrl := gomock.NewController(t)
				transport := NewMockTransport(ctrl)
				rangeDB := rangecachemock.NewMockRangeDescriptorDB(ctrl)

				// We start off with a cached lease on r1.
				cachedLease := roachpb.Lease{
					Replica:  desc.InternalReplicas[0],
					Sequence: 1,
				}

				// All nodes return the specified error code. We expect the range feed to
				// keep trying all replicas in sequence regardless of error.
				for _, repl := range desc.InternalReplicas {
					transport.EXPECT().IsExhausted().Return(false)
					transport.EXPECT().NextReplica().Return(repl)
					transport.EXPECT().NextInternalClient(gomock.Any()).Return(
						nil, grpcstatus.Error(spec.errorCode, ""))
				}
				transport.EXPECT().IsExhausted().Return(true)
				transport.EXPECT().Release()

				// Once all replicas have failed, it should try to refresh the lease using
				// the range cache. We let this succeed once.
				rangeDB.EXPECT().RangeLookup(gomock.Any(), roachpb.RKeyMin, kvpb.INCONSISTENT, false).Return([]roachpb.RangeDescriptor{desc}, nil, nil)

				// It then tries the replicas again. This time we just report the
				// transport as exhausted immediately.
				transport.EXPECT().IsExhausted().Return(true)
				transport.EXPECT().Release()

				// This invalidates the cache yet again. This time we error.
				rangeDB.EXPECT().RangeLookup(gomock.Any(), roachpb.RKeyMin, kvpb.INCONSISTENT, false).Return(nil, nil, grpcstatus.Error(spec.errorCode, ""))

				// If we expect a range lookup retry, allow the retry to succeed by
				// returning a range descriptor and a client that immediately
				// cancels the context and closes the range feed stream.
				if spec.expectRetry {
					rangeDB.EXPECT().RangeLookup(gomock.Any(), roachpb.RKeyMin, kvpb.INCONSISTENT, false).MinTimes(1).Return([]roachpb.RangeDescriptor{desc}, nil, nil) //.FirstRange().Return(&desc, nil)
					client := kvpbmock.NewMockRPCInternalClient(ctrl)

					stream := kvpbmock.NewMockRPCInternal_MuxRangeFeedClient(ctrl)
					stream.EXPECT().Send(gomock.Any()).Return(nil)
					stream.EXPECT().Recv().Do(func() {
						cancel()
					}).Return(nil, context.Canceled).AnyTimes()
					client.EXPECT().MuxRangeFeed(gomock.Any()).Return(stream, nil).AnyTimes()

					transport.EXPECT().IsExhausted().Return(false).AnyTimes()
					transport.EXPECT().NextReplica().Return(desc.InternalReplicas[0]).AnyTimes()
					transport.EXPECT().NextInternalClient(gomock.Any()).Return(client, nil).AnyTimes()
					transport.EXPECT().Release().AnyTimes()
				}

				ds := NewDistSender(DistSenderConfig{
					AmbientCtx:      log.MakeTestingAmbientCtxWithNewTracer(),
					Clock:           clock,
					NodeDescs:       g,
					RPCRetryOptions: &retry.Options{MaxRetries: 10},
					Stopper:         stopper,
					TransportFactory: func(SendOptions, ReplicaSlice) Transport {
						return transport
					},
					RangeDescriptorDB: rangeDB,
					Settings:          cluster.MakeTestingClusterSettings(),
				})
				ds.rangeCache.Insert(ctx, roachpb.RangeInfo{
					Desc:  desc,
					Lease: cachedLease,
				})

				err := ds.RangeFeed(ctx, []SpanTimePair{{Span: roachpb.Span{Key: keys.MinKey, EndKey: keys.MaxKey}}}, nil)
				require.Error(t, err)
			})
	}
}

// TestMuxRangeFeedTransportRace tests for a data race on activeMuxRangeFeed.transport.
//
// The race occurs between:
//
//   - Goroutine A: activeMuxRangeFeed.start() retry loop accessing s.transport
//   - Goroutine B: restartActiveRangeFeed() calling resetRouting() which nils s.transport
//
// The race happens because startRangeFeed stores the activeMuxRangeFeed in the
// streams map before calling Send(). So while Send() is blocked, Recv() can
// return a RangeFeedError for that streamID. This triggers restartActiveRangeFeed
// which calls resetRouting(), niling transport. Then Send() unblocks and returns
// an error, causing start() to continue its retry loop and access the now-nil
// transport.
//
// This test hits a nil pointer exception in the presence of the bug.
func TestMuxRangeFeedTransportRace(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	clock := hlc.NewClockForTesting(nil)
	ctx := context.Background()

	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	var cancel func()
	ctx, cancel = stopper.WithCancelOnQuiesce(ctx)
	defer cancel()

	rpcContext := rpc.NewInsecureTestingContext(ctx, clock, stopper)
	g := makeGossip(t, stopper, rpcContext)

	desc := roachpb.RangeDescriptor{
		RangeID:    1,
		Generation: 1,
		StartKey:   roachpb.RKeyMin,
		EndKey:     roachpb.RKeyMax,
		InternalReplicas: []roachpb.ReplicaDescriptor{
			{NodeID: 1, StoreID: 1, ReplicaID: 1},
			{NodeID: 2, StoreID: 2, ReplicaID: 2},
		},
	}
	for _, repl := range desc.InternalReplicas {
		require.NoError(t, g.AddInfoProto(
			gossip.MakeNodeIDKey(repl.NodeID),
			newNodeDesc(repl.NodeID),
			gossip.NodeDescriptorTTL,
		))
	}

	ctrl := gomock.NewController(t)
	rangeDB := rangecachemock.NewMockRangeDescriptorDB(ctrl)

	// Synchronization channels.
	var (
		// capturedStreamID stores the streamID from Send() so Recv() can use it.
		capturedStreamID atomic.Int64
		streamStarted    atomic.Bool

		// sendReady signals that Send() has captured the streamID and is waiting.
		sendReady = make(chan struct{})
		// resetDone signals that resetRouting has completed (transport is nil).
		resetDone = make(chan struct{})
		// proceedAfterReset allows the afterRoutingReset hook to continue.
		proceedAfterReset = make(chan struct{})
	)

	// Create mock client and stream.
	client := kvpbmock.NewMockRPCInternalClient(ctrl)
	stream := kvpbmock.NewMockRPCInternal_MuxRangeFeedClient(ctrl)

	client.EXPECT().MuxRangeFeed(gomock.Any()).Return(stream, nil).AnyTimes()

	stream.EXPECT().Send(gomock.Any()).DoAndReturn(func(req *kvpb.RangeFeedRequest) error {
		capturedStreamID.Store(req.StreamID)
		close(sendReady)
		<-resetDone
		return net.ErrClosed
	}).AnyTimes()

	stream.EXPECT().Recv().DoAndReturn(func() (*kvpb.MuxRangeFeedEvent, error) {
		select {
		case <-sendReady:
		case <-ctx.Done():
			return nil, ctx.Err()
		}

		if streamStarted.Swap(true) {
			// We only need to return an error the first time. Just block on
			// subsequent calls.
			<-ctx.Done()
			return nil, ctx.Err()
		}

		streamID := capturedStreamID.Load()
		return &kvpb.MuxRangeFeedEvent{
			StreamID: streamID,
			RangeID:  desc.RangeID,
			RangeFeedEvent: kvpb.RangeFeedEvent{
				Error: &kvpb.RangeFeedError{
					Error: *kvpb.NewError(&kvpb.RangeNotFoundError{RangeID: desc.RangeID}),
				},
			},
		}, nil
	}).AnyTimes()

	// Transport factory returns mock transports.
	transportFactory := func() Transport {
		transport := NewMockTransport(ctrl)
		transport.EXPECT().IsExhausted().Return(false).AnyTimes()
		transport.EXPECT().NextReplica().Return(desc.InternalReplicas[0]).AnyTimes()
		transport.EXPECT().NextInternalClient(gomock.Any()).Return(client, nil).AnyTimes()
		transport.EXPECT().Release().AnyTimes()
		return transport
	}

	rangeDB.EXPECT().RangeLookup(gomock.Any(), roachpb.RKeyMin, kvpb.INCONSISTENT, false).
		Return([]roachpb.RangeDescriptor{desc}, nil, nil).AnyTimes()

	cachedLease := roachpb.Lease{
		Replica:  desc.InternalReplicas[0],
		Sequence: 1,
	}
	ds := NewDistSender(DistSenderConfig{
		AmbientCtx:      log.MakeTestingAmbientCtxWithNewTracer(),
		Clock:           clock,
		NodeDescs:       g,
		RPCRetryOptions: &retry.Options{MaxRetries: 10},
		Stopper:         stopper,
		TransportFactory: func(SendOptions, ReplicaSlice) Transport {
			return transportFactory()
		},
		RangeDescriptorDB: rangeDB,
		Settings:          cluster.MakeTestingClusterSettings(),
	})
	ds.rangeCache.Insert(ctx, roachpb.RangeInfo{
		Desc:  desc,
		Lease: cachedLease,
	})

	afterResetFn := func() {
		close(resetDone)
		select {
		case <-proceedAfterReset:
		case <-ctx.Done():
		}
	}

	require.NoError(t, stopper.RunAsyncTask(ctx, "range-feed", func(ctx context.Context) {
		_ = ds.RangeFeed(
			ctx,
			[]SpanTimePair{{Span: roachpb.Span{Key: keys.MinKey, EndKey: keys.MaxKey}}},
			nil,
			TestingWithAfterRoutingReset(afterResetFn),
		)
	}))

	<-resetDone
	close(proceedAfterReset)
}
