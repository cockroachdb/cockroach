// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvcoord

import (
	"context"
	"fmt"
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
					client := kvpbmock.NewMockInternalClient(ctrl)

					stream := kvpbmock.NewMockInternal_MuxRangeFeedClient(ctrl)
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
