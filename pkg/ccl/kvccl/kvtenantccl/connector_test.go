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
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvtenant"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/grpcutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/netutil"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
)

var rpcRetryOpts = retry.Options{
	InitialBackoff: 1 * time.Microsecond,
	MaxBackoff:     4 * time.Microsecond,
}

var _ roachpb.InternalServer = &mockServer{}

type mockServer struct {
	rangeLookupFn    func(context.Context, *roachpb.RangeLookupRequest) (*roachpb.RangeLookupResponse, error)
	gossipSubFn      func(*roachpb.GossipSubscriptionRequest, roachpb.Internal_GossipSubscriptionServer) error
	tenantSettingsFn func(request *roachpb.TenantSettingsRequest, server roachpb.Internal_TenantSettingsServer) error
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

func (m *mockServer) TenantSettings(
	req *roachpb.TenantSettingsRequest, stream roachpb.Internal_TenantSettingsServer,
) error {
	if m.tenantSettingsFn == nil {
		return stream.Send(&roachpb.TenantSettingsEvent{
			Precedence:  roachpb.SpecificTenantOverrides,
			Incremental: false,
			Overrides:   nil,
		})
	}
	return m.tenantSettingsFn(req, stream)
}

func (*mockServer) ResetQuorum(
	context.Context, *roachpb.ResetQuorumRequest,
) (*roachpb.ResetQuorumResponse, error) {
	panic("unimplemented")
}

func (*mockServer) Batch(context.Context, *roachpb.BatchRequest) (*roachpb.BatchResponse, error) {
	panic("unimplemented")
}

func (*mockServer) RangeFeed(*roachpb.RangeFeedRequest, roachpb.Internal_RangeFeedServer) error {
	panic("unimplemented")
}

func (*mockServer) Join(
	context.Context, *roachpb.JoinNodeRequest,
) (*roachpb.JoinNodeResponse, error) {
	panic("unimplemented")
}

func (*mockServer) TokenBucket(
	ctx context.Context, in *roachpb.TokenBucketRequest,
) (*roachpb.TokenBucketResponse, error) {
	panic("unimplemented")
}

func (m *mockServer) GetSpanConfigs(
	context.Context, *roachpb.GetSpanConfigsRequest,
) (*roachpb.GetSpanConfigsResponse, error) {
	panic("unimplemented")
}

func (m *mockServer) UpdateSpanConfigs(
	context.Context, *roachpb.UpdateSpanConfigsRequest,
) (*roachpb.UpdateSpanConfigsResponse, error) {
	panic("unimplemented")
}

func gossipEventForClusterID(clusterID uuid.UUID) *roachpb.GossipSubscriptionEvent {
	return &roachpb.GossipSubscriptionEvent{
		Key:            gossip.KeyClusterID,
		Content:        roachpb.MakeValueFromBytesAndTimestamp(clusterID.GetBytes(), hlc.Timestamp{}),
		PatternMatched: gossip.KeyClusterID,
	}
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

func gossipEventForSystemConfig(cfg *config.SystemConfigEntries) *roachpb.GossipSubscriptionEvent {
	val, err := protoutil.Marshal(cfg)
	if err != nil {
		panic(err)
	}
	return &roachpb.GossipSubscriptionEvent{
		Key:            gossip.KeyDeprecatedSystemConfig,
		Content:        roachpb.MakeValueFromBytesAndTimestamp(val, hlc.Timestamp{}),
		PatternMatched: gossip.KeyDeprecatedSystemConfig,
	}
}

func waitForNodeDesc(t *testing.T, c *Connector, nodeID roachpb.NodeID) {
	t.Helper()
	testutils.SucceedsSoon(t, func() error {
		_, err := c.GetNodeDescriptor(nodeID)
		return err
	})
}

// TestConnectorGossipSubscription tests Connector's roles as a
// kvcoord.NodeDescStore and as a config.SystemConfigProvider.
func TestConnectorGossipSubscription(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	rpcContext := rpc.NewInsecureTestingContext(ctx, clock, stopper)
	s := rpc.NewServer(rpcContext)

	// Test setting the cluster ID by setting it to nil then ensuring it's later
	// set to the original ID value.
	clusterID := rpcContext.ClusterID.Get()
	rpcContext.ClusterID.Reset(uuid.Nil)

	gossipSubC := make(chan *roachpb.GossipSubscriptionEvent)
	defer close(gossipSubC)
	gossipSubFn := func(req *roachpb.GossipSubscriptionRequest, stream roachpb.Internal_GossipSubscriptionServer) error {
		assert.Len(t, req.Patterns, 3)
		assert.Equal(t, "cluster-id", req.Patterns[0])
		assert.Equal(t, "node:.*", req.Patterns[1])
		assert.Equal(t, "system-db", req.Patterns[2])
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

	cfg := kvtenant.ConnectorConfig{
		AmbientCtx:      log.MakeTestingAmbientContext(stopper.Tracer()),
		RPCContext:      rpcContext,
		RPCRetryOptions: rpcRetryOpts,
	}
	addrs := []string{ln.Addr().String()}
	c := NewConnector(cfg, addrs)

	// Start should block until the first GossipSubscription response.
	startedC := make(chan error)
	go func() {
		startedC <- c.Start(ctx)
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
	gossipSubC <- gossipEventForClusterID(clusterID)
	require.NoError(t, <-startedC)

	// Ensure that ClusterID was updated.
	require.Equal(t, clusterID, rpcContext.ClusterID.Get())

	// Test kvcoord.NodeDescStore impl. Wait for full update first.
	waitForNodeDesc(t, c, 2)
	desc, err := c.GetNodeDescriptor(1)
	require.Equal(t, node1, desc)
	require.NoError(t, err)
	desc, err = c.GetNodeDescriptor(2)
	require.Equal(t, node2, desc)
	require.NoError(t, err)
	desc, err = c.GetNodeDescriptor(3)
	require.Nil(t, desc)
	require.Regexp(t, "unable to look up descriptor for n3", err)

	// Return updated GossipSubscription response.
	node1Up := &roachpb.NodeDescriptor{NodeID: 1, Address: util.MakeUnresolvedAddr("tcp", "1.2.3.4")}
	node3 := &roachpb.NodeDescriptor{NodeID: 3, Address: util.MakeUnresolvedAddr("tcp", "2.2.2.2")}
	gossipSubC <- gossipEventForNodeDesc(node1Up)
	gossipSubC <- gossipEventForNodeDesc(node3)

	// Test kvcoord.NodeDescStore impl. Wait for full update first.
	waitForNodeDesc(t, c, 3)
	desc, err = c.GetNodeDescriptor(1)
	require.Equal(t, node1Up, desc)
	require.NoError(t, err)
	desc, err = c.GetNodeDescriptor(2)
	require.Equal(t, node2, desc)
	require.NoError(t, err)
	desc, err = c.GetNodeDescriptor(3)
	require.Equal(t, node3, desc)
	require.NoError(t, err)

	// Test config.SystemConfigProvider impl. Should not have a SystemConfig yet.
	sysCfg := c.GetSystemConfig()
	require.Nil(t, sysCfg)
	sysCfgC, _ := c.RegisterSystemConfigChannel()
	require.Len(t, sysCfgC, 0)

	// Return first SystemConfig response.
	sysCfgEntries := &config.SystemConfigEntries{Values: []roachpb.KeyValue{
		{Key: roachpb.Key("a")},
		{Key: roachpb.Key("b")},
	}}
	gossipSubC <- gossipEventForSystemConfig(sysCfgEntries)

	// Test config.SystemConfigProvider impl. Wait for update first.
	<-sysCfgC
	sysCfg = c.GetSystemConfig()
	require.NotNil(t, sysCfg)
	require.Equal(t, sysCfgEntries.Values, sysCfg.Values)

	// Return updated SystemConfig response.
	sysCfgEntriesUp := &config.SystemConfigEntries{Values: []roachpb.KeyValue{
		{Key: roachpb.Key("a")},
		{Key: roachpb.Key("c")},
	}}
	gossipSubC <- gossipEventForSystemConfig(sysCfgEntriesUp)

	// Test config.SystemConfigProvider impl. Wait for update first.
	<-sysCfgC
	sysCfg = c.GetSystemConfig()
	require.NotNil(t, sysCfg)
	require.Equal(t, sysCfgEntriesUp.Values, sysCfg.Values)

	// A newly registered SystemConfig channel will be immediately notified.
	sysCfgC2, _ := c.RegisterSystemConfigChannel()
	require.Len(t, sysCfgC2, 1)
}

// TestConnectorGossipSubscription tests Connector's role as a
// kvcoord.RangeDescriptorDB.
func TestConnectorRangeLookup(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	rpcContext := rpc.NewInsecureTestingContext(ctx, clock, stopper)
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

	cfg := kvtenant.ConnectorConfig{
		AmbientCtx:      log.MakeTestingAmbientContext(stopper.Tracer()),
		RPCContext:      rpcContext,
		RPCRetryOptions: rpcRetryOpts,
	}
	addrs := []string{ln.Addr().String()}
	c := NewConnector(cfg, addrs)
	// NOTE: we don't actually start the connector worker. That's ok, as
	// RangeDescriptorDB methods don't require it to be running.

	// Success case.
	descs := []roachpb.RangeDescriptor{{RangeID: 1}, {RangeID: 2}}
	preDescs := []roachpb.RangeDescriptor{{RangeID: 3}, {RangeID: 4}}
	rangeLookupRespC <- &roachpb.RangeLookupResponse{
		Descriptors: descs, PrefetchedDescriptors: preDescs,
	}
	resDescs, resPreDescs, err := c.RangeLookup(ctx, roachpb.RKey("a"), false /* useReverseScan */)
	require.Equal(t, descs, resDescs)
	require.Equal(t, preDescs, resPreDescs)
	require.NoError(t, err)

	// Error case.
	rangeLookupRespC <- &roachpb.RangeLookupResponse{
		Error: roachpb.NewErrorf("hit error"),
	}
	resDescs, resPreDescs, err = c.RangeLookup(ctx, roachpb.RKey("a"), false /* useReverseScan */)
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
	resDescs, resPreDescs, err = c.RangeLookup(canceledCtx, roachpb.RKey("a"), false /* useReverseScan */)
	require.Nil(t, resDescs)
	require.Nil(t, resPreDescs)
	require.Regexp(t, context.Canceled.Error(), err)

	// FirstRange always returns error.
	desc, err := c.FirstRange()
	require.Nil(t, desc)
	require.Regexp(t, "does not have access to FirstRange", err)
	require.True(t, grpcutil.IsAuthError(err))
}

// TestConnectorRetriesUnreachable tests that Connector iterates over each of
// its provided addresses and retries until it is able to establish a connection
// on one of them.
func TestConnectorRetriesUnreachable(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	rpcContext := rpc.NewInsecureTestingContext(ctx, clock, stopper)
	s := rpc.NewServer(rpcContext)

	node1 := &roachpb.NodeDescriptor{NodeID: 1, Address: util.MakeUnresolvedAddr("tcp", "1.1.1.1")}
	node2 := &roachpb.NodeDescriptor{NodeID: 2, Address: util.MakeUnresolvedAddr("tcp", "2.2.2.2")}
	gossipSubEvents := []*roachpb.GossipSubscriptionEvent{
		gossipEventForClusterID(rpcContext.ClusterID.Get()),
		gossipEventForNodeDesc(node1),
		gossipEventForNodeDesc(node2),
	}
	gossipSubFn := func(req *roachpb.GossipSubscriptionRequest, stream roachpb.Internal_GossipSubscriptionServer) error {
		assert.Len(t, req.Patterns, 3)
		assert.Equal(t, "cluster-id", req.Patterns[0])
		assert.Equal(t, "node:.*", req.Patterns[1])
		assert.Equal(t, "system-db", req.Patterns[2])
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
	stopper.AddCloser(stop.CloserFn(s.Stop))
	_ = stopper.RunAsyncTask(ctx, "wait-quiesce", func(context.Context) {
		<-stopper.ShouldQuiesce()
		netutil.FatalIfUnexpected(ln.Close())
	})

	// Add listen address into list of other bogus addresses.
	cfg := kvtenant.ConnectorConfig{
		AmbientCtx:      log.MakeTestingAmbientContext(stopper.Tracer()),
		RPCContext:      rpcContext,
		RPCRetryOptions: rpcRetryOpts,
	}
	addrs := []string{"1.1.1.1:9999", ln.Addr().String(), "2.2.2.2:9999"}
	c := NewConnector(cfg, addrs)
	c.rpcDialTimeout = 5 * time.Millisecond // speed up test

	// Start should block until the first GossipSubscription response.
	startedC := make(chan error)
	go func() {
		startedC <- c.Start(ctx)
	}()
	select {
	case err := <-startedC:
		t.Fatalf("Start unexpectedly completed with err=%v", err)
	case <-time.After(25 * time.Millisecond):
	}

	// Begin serving on gRPC server. Connector should quickly connect
	// and complete startup.
	_ = stopper.RunAsyncTask(ctx, "serve", func(context.Context) {
		netutil.FatalIfUnexpected(s.Serve(ln))
	})
	require.NoError(t, <-startedC)

	// Test kvcoord.NodeDescStore impl. Wait for full update first.
	waitForNodeDesc(t, c, 2)
	desc, err := c.GetNodeDescriptor(1)
	require.Equal(t, node1, desc)
	require.NoError(t, err)
	desc, err = c.GetNodeDescriptor(2)
	require.Equal(t, node2, desc)
	require.NoError(t, err)
	desc, err = c.GetNodeDescriptor(3)
	require.Nil(t, desc)
	require.Regexp(t, "unable to look up descriptor for n3", err)
}

// TestConnectorRetriesError tests that Connector iterates over each of
// its provided addresses and retries if the error is retriable or bails out
// immediately if it is not.
func TestConnectorRetriesError(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	rpcContext := rpc.NewInsecureTestingContext(ctx, clock, stopper)

	// Function to create rpc server that would delegate to gossip and range lookup
	// callbacks.
	// Returns address on which server is listening for use in connector.
	createServer := func(
		t *testing.T,
		gossipSubFn func(req *roachpb.GossipSubscriptionRequest, stream roachpb.Internal_GossipSubscriptionServer) error,
		rangeLookupFn func(_ context.Context, req *roachpb.RangeLookupRequest) (*roachpb.RangeLookupResponse, error),
	) string {
		internalServer := rpc.NewServer(rpcContext)
		roachpb.RegisterInternalServer(internalServer, &mockServer{rangeLookupFn: rangeLookupFn, gossipSubFn: gossipSubFn})
		ln, err := net.Listen(util.TestAddr.Network(), util.TestAddr.String())
		require.NoError(t, err)
		stopper.AddCloser(stop.CloserFn(internalServer.Stop))
		_ = stopper.RunAsyncTask(ctx, "wait-quiesce", func(context.Context) {
			<-stopper.ShouldQuiesce()
			netutil.FatalIfUnexpected(ln.Close())
		})
		_ = stopper.RunAsyncTask(ctx, "serve", func(context.Context) {
			netutil.FatalIfUnexpected(internalServer.Serve(ln))
		})
		return ln.Addr().String()
	}

	for _, spec := range []struct {
		code        codes.Code
		shouldRetry bool
	}{
		{codes.Unauthenticated, false},
		{codes.PermissionDenied, false},
		{codes.FailedPrecondition, true},
	} {
		t.Run(fmt.Sprintf("error %v retries %v", spec.code, spec.shouldRetry), func(t *testing.T) {

			gossipSubFn := func(req *roachpb.GossipSubscriptionRequest, stream roachpb.Internal_GossipSubscriptionServer) error {
				return stream.Send(gossipEventForClusterID(rpcContext.ClusterID.Get()))
			}

			rangeLookupFn := func(_ context.Context, req *roachpb.RangeLookupRequest) (*roachpb.RangeLookupResponse, error) {
				descs := []roachpb.RangeDescriptor{{RangeID: 1}, {RangeID: 2}}
				preDescs := []roachpb.RangeDescriptor{{RangeID: 3}, {RangeID: 4}}
				return &roachpb.RangeLookupResponse{
					Descriptors: descs, PrefetchedDescriptors: preDescs,
				}, nil
			}

			var errorsReported int32 = 0
			rangeLookupRejectorFn := func(_ context.Context, req *roachpb.RangeLookupRequest) (*roachpb.RangeLookupResponse, error) {
				// Respond with error always
				atomic.AddInt32(&errorsReported, 1)
				return nil, grpcstatus.Errorf(spec.code, "range lookup rejected")
			}

			addr1 := createServer(t, gossipSubFn, rangeLookupFn)
			addr2 := createServer(t, gossipSubFn, rangeLookupRejectorFn)

			// Add listen address into list of other bogus addresses.
			cfg := kvtenant.ConnectorConfig{
				AmbientCtx:      log.MakeTestingAmbientContext(stopper.Tracer()),
				RPCContext:      rpcContext,
				RPCRetryOptions: rpcRetryOpts,
			}
			addrs := []string{addr1, addr2}
			c := NewConnector(cfg, addrs)
			c.rpcDialTimeout = 5 * time.Millisecond // speed up test
			require.NoError(t, c.Start(ctx), "Connector can't start")

			// Test will try to make range lookups until the server returning errors
			// is hit. It then checks that error was propagated or not. We use multiple
			// iterations as server choice is random and we need to hit failure only once
			// to check if it was retried.
			for i := 0; i < 100; i++ {
				_, _, err := c.RangeLookup(ctx, roachpb.RKey("a"), false)
				if atomic.LoadInt32(&errorsReported) == 0 {
					continue
				}
				if spec.shouldRetry {
					require.NoError(t, err, "Lookup should retry instead of failing")
				} else {
					require.Error(t, err, "Lookup should propagate error immediately")
				}
				break
			}
		})
	}
}
