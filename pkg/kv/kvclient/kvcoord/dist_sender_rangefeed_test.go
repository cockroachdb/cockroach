// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvcoord

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangecache"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
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
		t.Run(spec.errorCode.String(), func(t *testing.T) {
			clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			stopper := stop.NewStopper()
			defer stopper.Stop(ctx)
			rpcContext := rpc.NewInsecureTestingContext(clock, stopper)
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
			rangeDB := rangecache.NewMockRangeDescriptorDB(ctrl)

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
					ctx, nil, grpcstatus.Error(spec.errorCode, ""))
			}
			transport.EXPECT().IsExhausted().Return(true)
			transport.EXPECT().Release()

			// Once all replicas have failed, it should try to refresh the lease using
			// the range cache. We let this succeed once.
			rangeDB.EXPECT().FirstRange().Return(&desc, nil)

			// It then tries the replicas again. This time we just report the
			// transport as exhausted immediately.
			transport.EXPECT().IsExhausted().Return(true)
			transport.EXPECT().Release()

			// This invalidates the cache yet again. This time we error.
			rangeDB.EXPECT().FirstRange().Return(nil, grpcstatus.Error(spec.errorCode, ""))

			// If we expect a range lookup retry, allow the retry to succeed by
			// returning a range descriptor and a client that immediately
			// cancels the context and closes the range feed stream.
			if spec.expectRetry {
				rangeDB.EXPECT().FirstRange().Return(&desc, nil)
				stream := roachpb.NewMockInternal_RangeFeedClient(ctrl)
				stream.EXPECT().Recv().Do(cancel).Return(nil, io.EOF)
				client := roachpb.NewMockInternalClient(ctrl)
				client.EXPECT().RangeFeed(gomock.Any(), gomock.Any()).Return(stream, nil)
				transport.EXPECT().IsExhausted().Return(false)
				transport.EXPECT().NextReplica().Return(desc.InternalReplicas[0])
				transport.EXPECT().NextInternalClient(gomock.Any()).Return(ctx, client, nil)
				transport.EXPECT().Release()
			}

			ds := NewDistSender(DistSenderConfig{
				AmbientCtx:      log.AmbientContext{Tracer: tracing.NewTracer()},
				Clock:           clock,
				NodeDescs:       g,
				RPCRetryOptions: &retry.Options{MaxRetries: 10},
				RPCContext:      rpcContext,
				TestingKnobs: ClientTestingKnobs{
					TransportFactory: func(SendOptions, *nodedialer.Dialer, ReplicaSlice) (Transport, error) {
						return transport, nil
					},
				},
				RangeDescriptorDB: rangeDB,
				NodeDialer:        nodedialer.New(rpcContext, gossip.AddressResolver(g)),
				Settings:          cluster.MakeTestingClusterSettings(),
			})
			ds.rangeCache.Insert(ctx, roachpb.RangeInfo{
				Desc:  desc,
				Lease: cachedLease,
			})

			err := ds.RangeFeed(ctx, roachpb.Span{Key: keys.MinKey, EndKey: keys.MaxKey}, hlc.Timestamp{}, false, nil)
			require.Error(t, err)
		})
	}
}
