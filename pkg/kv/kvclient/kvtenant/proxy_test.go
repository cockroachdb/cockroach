// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvtenant

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/netutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var rpcRetryOpts = retry.Options{
	InitialBackoff: 1 * time.Microsecond,
	MaxBackoff:     4 * time.Microsecond,
}

type mockServer struct {
	rangeLookupFn func(*roachpb.RangeLookupRequest) (*roachpb.RangeLookupResponse, error)
	nodeInfoFn    func(*roachpb.NodeInfoRequest, roachpb.Internal_NodeInfoServer) error
}

func (m *mockServer) RangeLookup(
	_ context.Context, req *roachpb.RangeLookupRequest,
) (*roachpb.RangeLookupResponse, error) {
	return m.rangeLookupFn(req)
}

func (m *mockServer) NodeInfo(
	req *roachpb.NodeInfoRequest, stream roachpb.Internal_NodeInfoServer,
) error {
	return m.nodeInfoFn(req, stream)
}

func (*mockServer) Batch(context.Context, *roachpb.BatchRequest) (*roachpb.BatchResponse, error) {
	panic("unimplemented")
}

func (*mockServer) RangeFeed(*roachpb.RangeFeedRequest, roachpb.Internal_RangeFeedServer) error {
	panic("unimplemented")
}

// TestProxyNodeInfo tests Proxy's role as a kvcoord.NodeDescStore.
func TestProxyNodeInfo(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	rpcContext := rpc.NewInsecureTestingContext(clock, stopper)
	s := rpc.NewServer(rpcContext)

	nodeInfoC := make(chan *roachpb.NodeInfoResponse)
	defer close(nodeInfoC)
	nodeInfoFn := func(req *roachpb.NodeInfoRequest, stream roachpb.Internal_NodeInfoServer) error {
		_ = req // empty, nothing to assert
		for nodeInfo := range nodeInfoC {
			if err := stream.Send(nodeInfo); err != nil {
				return err
			}
		}
		return nil
	}
	roachpb.RegisterInternalServer(s, &mockServer{nodeInfoFn: nodeInfoFn})
	ln, err := netutil.ListenAndServeGRPC(stopper, s, util.TestAddr)
	require.NoError(t, err)

	addrs := []string{ln.Addr().String()}
	p := NewProxy(stopper, rpcContext, rpcRetryOpts, addrs)

	// Start should block until the first NodeInfo response.
	startedC := make(chan error)
	go func() {
		startedC <- p.Start(ctx)
	}()
	select {
	case err := <-startedC:
		t.Fatalf("Start unexpectedly completed with err=%v", err)
	case <-time.After(10 * time.Millisecond):
	}

	// Return first NodeInfo response.
	node1 := &roachpb.NodeDescriptor{NodeID: 1, Address: util.MakeUnresolvedAddr("tcp", "1.1.1.1")}
	node2 := &roachpb.NodeDescriptor{NodeID: 2, Address: util.MakeUnresolvedAddr("tcp", "2.2.2.2")}
	nodeInfoC <- &roachpb.NodeInfoResponse{
		Descriptors: []*roachpb.NodeDescriptor{node1, node2},
	}
	require.NoError(t, <-startedC)

	// Test kvcoord.NodeDescStore impl.
	desc, err := p.GetNodeDescriptor(1)
	require.Equal(t, node1, desc)
	require.NoError(t, err)
	desc, err = p.GetNodeDescriptor(2)
	require.Equal(t, node2, desc)
	require.NoError(t, err)
	desc, err = p.GetNodeDescriptor(3)
	require.Nil(t, desc)
	require.Regexp(t, "unable to look up descriptor for n3", err)

	// Return updated NodeInfo response.
	node1Up := &roachpb.NodeDescriptor{NodeID: 1, Address: util.MakeUnresolvedAddr("tcp", "1.2.3.4")}
	node3 := &roachpb.NodeDescriptor{NodeID: 3, Address: util.MakeUnresolvedAddr("tcp", "2.2.2.2")}
	nodeInfoC <- &roachpb.NodeInfoResponse{
		Descriptors: []*roachpb.NodeDescriptor{node1Up, node2, node3},
	}

	// Test kvcoord.NodeDescStore impl. Wait for update first.
	testutils.SucceedsSoon(t, func() error {
		if _, err := p.GetNodeDescriptor(3); err != nil {
			return err
		}
		return nil
	})
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

// TestProxyNodeInfo tests Proxy's role as a kvcoord.RangeDescriptorDB.
func TestProxyRangeLookup(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	rpcContext := rpc.NewInsecureTestingContext(clock, stopper)
	s := rpc.NewServer(rpcContext)

	rangeLookupRespC := make(chan *roachpb.RangeLookupResponse, 1)
	rangeLookupFn := func(req *roachpb.RangeLookupRequest) (*roachpb.RangeLookupResponse, error) {
		// Validate request.
		assert.Equal(t, roachpb.RKey("a"), req.Key)
		assert.Equal(t, roachpb.READ_UNCOMMITTED, req.ReadConsistency)
		assert.Equal(t, int64(0), req.PrefetchNum)
		assert.Equal(t, false, req.PrefetchReverse)

		// Respond.
		resp := <-rangeLookupRespC
		if resp == nil {
			return nil, errors.Errorf("RangeLookup hit error")
		}
		return resp, nil
	}
	roachpb.RegisterInternalServer(s, &mockServer{rangeLookupFn: rangeLookupFn})
	ln, err := netutil.ListenAndServeGRPC(stopper, s, util.TestAddr)
	require.NoError(t, err)

	addrs := []string{ln.Addr().String()}
	p := NewProxy(stopper, rpcContext, rpcRetryOpts, addrs)
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
	rangeLookupRespC <- nil // error
	resDescs, resPreDescs, err = p.RangeLookup(ctx, roachpb.RKey("a"), false /* useReverseScan */)
	require.Nil(t, resDescs)
	require.Nil(t, resPreDescs)
	require.Regexp(t, "hit error", err)

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
	nodeInfoResp := &roachpb.NodeInfoResponse{
		Descriptors: []*roachpb.NodeDescriptor{node1, node2},
	}
	nodeInfoFn := func(req *roachpb.NodeInfoRequest, stream roachpb.Internal_NodeInfoServer) error {
		_ = req // empty, nothing to assert
		if err := stream.Send(nodeInfoResp); err != nil {
			return err
		}
		<-stream.Context().Done()
		return stream.Context().Err()
	}
	roachpb.RegisterInternalServer(s, &mockServer{nodeInfoFn: nodeInfoFn})
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
	p := NewProxy(stopper, rpcContext, rpcRetryOpts, addrs)
	p.rpcDialTimeout = 5 * time.Millisecond // speed up test

	// Start should block until the first NodeInfo response.
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

	// Test kvcoord.NodeDescStore impl.
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
