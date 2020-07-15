// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package kvtenantccl

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/netutil"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var rpcRetryOpts = retry.Options{
	InitialBackoff: 1 * time.Microsecond,
	MaxBackoff:     4 * time.Microsecond,
}

type mockServer struct {
	rangeLookupFn func(context.Context, *roachpb.RangeLookupRequest) (*roachpb.RangeLookupResponse, error)
	gossipSubFn   func(*roachpb.GossipSubscriptionRequest, roachpb.Internal_GossipSubscriptionServer) error
}

func (m *mockServer) RangeLookup(
	ctx context.Context, req *roachpb.RangeLookupRequest,
) (*roachpb.RangeLookupResponse, error) {
	return m.rangeLookupFn(ctx, req)
}

func (m *mockServer) GossipSubscription(
	req *roachpb.GossipSubscriptionRequest, stream roachpb.Internal_GossipSubscriptionServer,
) error {
	return m.gossipSubFn(req, stream)
}

func (*mockServer) Batch(context.Context, *roachpb.BatchRequest) (*roachpb.BatchResponse, error) {
	panic("unimplemented")
}

func (*mockServer) RangeFeed(*roachpb.RangeFeedRequest, roachpb.Internal_RangeFeedServer) error {
	panic("unimplemented")
}

func gossipEventForNodeDesc(desc *roachpb.NodeDescriptor) *roachpb.GossipSubscriptionEvent {
	val, err := protoutil.Marshal(desc)
	if err != nil {
		panic(err)
	}
	return &roachpb.GossipSubscriptionEvent{
		Key:            gossip.MakeNodeIDKey(desc.NodeID),
		Content:        roachpb.MakeValueFromBytesAndTimestamp(val, hlc.Timestamp{}),
		PatternMatched: gossip.MakePrefixPattern(gossip.KeyNodeIDPrefix),
	}
}

func waitForNodeDesc(t *testing.T, p *Proxy, nodeID roachpb.NodeID) {
	t.Helper()
	testutils.SucceedsSoon(t, func() error {
		_, err := p.GetNodeDescriptor(nodeID)
		return err
	})
}

// TestProxyGossipSubscription tests Proxy's role as a kvcoord.NodeDescStore.
func TestProxyGossipSubscription(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	rpcContext := rpc.NewInsecureTestingContext(clock, stopper)
	s := rpc.NewServer(rpcContext)

	gossipSubC := make(chan *roachpb.GossipSubscriptionEvent)
	defer close(gossipSubC)
	gossipSubFn := func(req *roachpb.GossipSubscriptionRequest, stream roachpb.Internal_GossipSubscriptionServer) error {
		assert.Len(t, req.Patterns, 1)
		assert.Equal(t, "node:.*", req.Patterns[0])
		for gossipSub := range gossipSubC {
			if err := stream.Send(gossipSub); err != nil {
				return err
			}
		}
		return nil
	}
	roachpb.RegisterInternalServer(s, &mockServer{gossipSubFn: gossipSubFn})
	ln, err := netutil.ListenAndServeGRPC(stopper, s, util.TestAddr)
	require.NoError(t, err)

	addrs := []string{ln.Addr().String()}
	p := NewProxy(log.AmbientContext{Tracer: tracing.NewTracer()}, rpcContext, rpcRetryOpts, addrs)

	// Start should block until the first GossipSubscription response.
	startedC := make(chan error)
	go func() {
		startedC <- p.Start(ctx)
	}()
	select {
	case err := <-startedC:
		t.Fatalf("Start unexpectedly completed with err=%v", err)
	case <-time.After(10 * time.Millisecond):
	}

	// Return first GossipSubscription response.
	node1 := &roachpb.NodeDescriptor{NodeID: 1, Address: util.MakeUnresolvedAddr("tcp", "1.1.1.1")}
	node2 := &roachpb.NodeDescriptor{NodeID: 2, Address: util.MakeUnresolvedAddr("tcp", "2.2.2.2")}
	gossipSubC <- gossipEventForNodeDesc(node1)
	gossipSubC <- gossipEventForNodeDesc(node2)
	require.NoError(t, <-startedC)

	// Test kvcoord.NodeDescStore impl. Wait for full update first.
	waitForNodeDesc(t, p, 2)
	desc, err := p.GetNodeDescriptor(1)
	require.Equal(t, node1, desc)
	require.NoError(t, err)
	desc, err = p.GetNodeDescriptor(2)
	require.Equal(t, node2, desc)
	require.NoError(t, err)
	desc, err = p.GetNodeDescriptor(3)
	require.Nil(t, desc)
	require.Regexp(t, "unable to look up descriptor for n3", err)

	// Return updated GossipSubscription response.
	node1Up := &roachpb.NodeDescriptor{NodeID: 1, Address: util.MakeUnresolvedAddr("tcp", "1.2.3.4")}
	node3 := &roachpb.NodeDescriptor{NodeID: 3, Address: util.MakeUnresolvedAddr("tcp", "2.2.2.2")}
	gossipSubC <- gossipEventForNodeDesc(node1Up)
	gossipSubC <- gossipEventForNodeDesc(node3)

	// Test kvcoord.NodeDescStore impl. Wait for full update first.
	waitForNodeDesc(t, p, 3)
	desc, err = p.GetNodeDescriptor(1)
	require.Equal(t, node1Up, desc)
	require.NoError(t, err)
	desc, err = p.GetNodeDescriptor(2)
	require.Equal(t, node2, desc)
	require.NoError(t, err)
	desc, err = p.GetNodeDescriptor(3)
	require.Equal(t, node3, desc)
	require.NoError(t, err)
}

// TestProxyGossipSubscription tests Proxy's role as a kvcoord.RangeDescriptorDB.
func TestProxyRangeLookup(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	rpcContext := rpc.NewInsecureTestingContext(clock, stopper)
	s := rpc.NewServer(rpcContext)

	rangeLookupRespC := make(chan *roachpb.RangeLookupResponse, 1)
	rangeLookupFn := func(_ context.Context, req *roachpb.RangeLookupRequest) (*roachpb.RangeLookupResponse, error) {
		// Validate request.
		assert.Equal(t, roachpb.RKey("a"), req.Key)
		assert.Equal(t, roachpb.READ_UNCOMMITTED, req.ReadConsistency)
		assert.Equal(t, int64(0), req.PrefetchNum)
		assert.Equal(t, false, req.PrefetchReverse)

		// Respond.
		return <-rangeLookupRespC, nil
	}
	server := &mockServer{rangeLookupFn: rangeLookupFn}
	roachpb.RegisterInternalServer(s, server)
	ln, err := netutil.ListenAndServeGRPC(stopper, s, util.TestAddr)
	require.NoError(t, err)

	addrs := []string{ln.Addr().String()}
	p := NewProxy(log.AmbientContext{Tracer: tracing.NewTracer()}, rpcContext, rpcRetryOpts, addrs)
	// NOTE: we don't actually start the proxy worker. That's ok, as
	// RangeDescriptorDB methods don't require it to be running.

	// Success case.
	descs := []roachpb.RangeDescriptor{{RangeID: 1}, {RangeID: 2}}
	preDescs := []roachpb.RangeDescriptor{{RangeID: 3}, {RangeID: 4}}
	rangeLookupRespC <- &roachpb.RangeLookupResponse{
		Descriptors: descs, PrefetchedDescriptors: preDescs,
	}
	resDescs, resPreDescs, err := p.RangeLookup(ctx, roachpb.RKey("a"), false /* useReverseScan */)
	require.Equal(t, descs, resDescs)
	require.Equal(t, preDescs, resPreDescs)
	require.NoError(t, err)

	// Error case.
	rangeLookupRespC <- &roachpb.RangeLookupResponse{
		Error: roachpb.NewErrorf("hit error"),
	}
	resDescs, resPreDescs, err = p.RangeLookup(ctx, roachpb.RKey("a"), false /* useReverseScan */)
	require.Nil(t, resDescs)
	require.Nil(t, resPreDescs)
	require.Regexp(t, "hit error", err)

	// Context cancelation.
	canceledCtx, cancel := context.WithCancel(ctx)
	blockingC := make(chan struct{})
	server.rangeLookupFn = func(ctx context.Context, _ *roachpb.RangeLookupRequest) (*roachpb.RangeLookupResponse, error) {
		<-blockingC
		<-ctx.Done()
		return nil, ctx.Err()
	}
	go func() {
		blockingC <- struct{}{}
		cancel()
	}()
	resDescs, resPreDescs, err = p.RangeLookup(canceledCtx, roachpb.RKey("a"), false /* useReverseScan */)
	require.Nil(t, resDescs)
	require.Nil(t, resPreDescs)
	require.Regexp(t, context.Canceled.Error(), err)

	// FirstRange always returns error.
	desc, err := p.FirstRange()
	require.Nil(t, desc)
	require.Regexp(t, "does not have access to FirstRange", err)
}

// TestProxyRetriesUnreachable tests that Proxy iterates over each of its
// provided addresses and retries until it is able to establish a connection on
// one of them.
func TestProxyRetriesUnreachable(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	rpcContext := rpc.NewInsecureTestingContext(clock, stopper)
	s := rpc.NewServer(rpcContext)

	node1 := &roachpb.NodeDescriptor{NodeID: 1, Address: util.MakeUnresolvedAddr("tcp", "1.1.1.1")}
	node2 := &roachpb.NodeDescriptor{NodeID: 2, Address: util.MakeUnresolvedAddr("tcp", "2.2.2.2")}
	gossipSubEvents := []*roachpb.GossipSubscriptionEvent{
		gossipEventForNodeDesc(node1),
		gossipEventForNodeDesc(node2),
	}
	gossipSubFn := func(req *roachpb.GossipSubscriptionRequest, stream roachpb.Internal_GossipSubscriptionServer) error {
		assert.Len(t, req.Patterns, 1)
		assert.Equal(t, "node:.*", req.Patterns[0])
		for _, event := range gossipSubEvents {
			if err := stream.Send(event); err != nil {
				return err
			}
		}
		<-stream.Context().Done()
		return stream.Context().Err()
	}
	roachpb.RegisterInternalServer(s, &mockServer{gossipSubFn: gossipSubFn})
	// Decompose netutil.ListenAndServeGRPC so we can listen before serving.
	ln, err := net.Listen(util.TestAddr.Network(), util.TestAddr.String())
	require.NoError(t, err)
	stopper.RunWorker(ctx, func(context.Context) {
		<-stopper.ShouldQuiesce()
		netutil.FatalIfUnexpected(ln.Close())
		<-stopper.ShouldStop()
		s.Stop()
	})

	// Add listen address into list of other bogus addresses.
	addrs := []string{"1.1.1.1:9999", ln.Addr().String(), "2.2.2.2:9999"}
	p := NewProxy(log.AmbientContext{Tracer: tracing.NewTracer()}, rpcContext, rpcRetryOpts, addrs)
	p.rpcDialTimeout = 5 * time.Millisecond // speed up test

	// Start should block until the first GossipSubscription response.
	startedC := make(chan error)
	go func() {
		startedC <- p.Start(ctx)
	}()
	select {
	case err := <-startedC:
		t.Fatalf("Start unexpectedly completed with err=%v", err)
	case <-time.After(25 * time.Millisecond):
	}

	// Begin serving on gRPC server. Proxy should quickly connect
	// and complete startup.
	stopper.RunWorker(ctx, func(context.Context) {
		netutil.FatalIfUnexpected(s.Serve(ln))
	})
	require.NoError(t, <-startedC)

	// Test kvcoord.NodeDescStore impl. Wait for full update first.
	waitForNodeDesc(t, p, 2)
	desc, err := p.GetNodeDescriptor(1)
	require.Equal(t, node1, desc)
	require.NoError(t, err)
	desc, err = p.GetNodeDescriptor(2)
	require.Equal(t, node2, desc)
	require.NoError(t, err)
	desc, err = p.GetNodeDescriptor(3)
	require.Nil(t, desc)
	require.Regexp(t, "unable to look up descriptor for n3", err)
}
