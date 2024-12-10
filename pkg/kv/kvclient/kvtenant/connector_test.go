// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvtenant

import (
	"context"
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/multitenant/mtinfopb"
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
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
)

var rpcRetryOpts = retry.Options{
	InitialBackoff: 1 * time.Microsecond,
	MaxBackoff:     4 * time.Microsecond,
}

var _ kvpb.InternalServer = &mockServer{}

type mockServer struct {
	rangeLookupFn    func(context.Context, *kvpb.RangeLookupRequest) (*kvpb.RangeLookupResponse, error)
	gossipSubFn      func(*kvpb.GossipSubscriptionRequest, kvpb.Internal_GossipSubscriptionServer) error
	tenantSettingsFn func(request *kvpb.TenantSettingsRequest, server kvpb.Internal_TenantSettingsServer) error

	emulateOldVersionSettingServer bool
}

func (m *mockServer) RangeLookup(
	ctx context.Context, req *kvpb.RangeLookupRequest,
) (*kvpb.RangeLookupResponse, error) {
	return m.rangeLookupFn(ctx, req)
}

func (m *mockServer) GossipSubscription(
	req *kvpb.GossipSubscriptionRequest, stream kvpb.Internal_GossipSubscriptionServer,
) error {
	return m.gossipSubFn(req, stream)
}

func (m *mockServer) TenantSettings(
	req *kvpb.TenantSettingsRequest, stream kvpb.Internal_TenantSettingsServer,
) error {
	if m.tenantSettingsFn == nil {
		// First message - required by startup protocol.
		if err := stream.Send(&kvpb.TenantSettingsEvent{
			EventType:   kvpb.TenantSettingsEvent_SETTING_EVENT,
			Precedence:  kvpb.TenantSettingsEvent_TENANT_SPECIFIC_OVERRIDES,
			Incremental: false,
			Overrides:   nil,
		}); err != nil {
			return err
		}
		if !m.emulateOldVersionSettingServer {
			// Initial tenant metadata.
			if err := stream.Send(&kvpb.TenantSettingsEvent{
				EventType: kvpb.TenantSettingsEvent_METADATA_EVENT,
				Name:      "foo",
				// TODO(knz): remove cast after the dep cycle has been resolved.
				DataState:   uint32(mtinfopb.DataStateReady),
				ServiceMode: uint32(mtinfopb.ServiceModeExternal),

				// Need to ensure this looks like a fake no-op setting override event.
				Precedence:  kvpb.TenantSettingsEvent_TENANT_SPECIFIC_OVERRIDES,
				Incremental: true,
			}); err != nil {
				return err
			}
		}
		// Finish startup.
		if err := stream.Send(&kvpb.TenantSettingsEvent{
			EventType:   kvpb.TenantSettingsEvent_SETTING_EVENT,
			Precedence:  kvpb.TenantSettingsEvent_ALL_TENANTS_OVERRIDES,
			Incremental: false,
			Overrides:   nil,
		}); err != nil {
			return err
		}

		// Ensure the stream doesn't immediately finish, which can cause
		// flakes in tests due to the retry loop in the client.
		<-stream.Context().Done()
		return nil
	}
	return m.tenantSettingsFn(req, stream)
}

func (*mockServer) ResetQuorum(
	context.Context, *kvpb.ResetQuorumRequest,
) (*kvpb.ResetQuorumResponse, error) {
	panic("unimplemented")
}

func (*mockServer) Batch(context.Context, *kvpb.BatchRequest) (*kvpb.BatchResponse, error) {
	panic("unimplemented")
}

func (m *mockServer) BatchStream(stream kvpb.Internal_BatchStreamServer) error {
	panic("implement me")
}

func (m *mockServer) MuxRangeFeed(server kvpb.Internal_MuxRangeFeedServer) error {
	panic("implement me")
}

func (*mockServer) Join(context.Context, *kvpb.JoinNodeRequest) (*kvpb.JoinNodeResponse, error) {
	panic("unimplemented")
}

func (*mockServer) TokenBucket(
	ctx context.Context, in *kvpb.TokenBucketRequest,
) (*kvpb.TokenBucketResponse, error) {
	panic("unimplemented")
}

func (m *mockServer) GetSpanConfigs(
	context.Context, *roachpb.GetSpanConfigsRequest,
) (*roachpb.GetSpanConfigsResponse, error) {
	panic("unimplemented")
}

func (m *mockServer) GetAllSystemSpanConfigsThatApply(
	context.Context, *roachpb.GetAllSystemSpanConfigsThatApplyRequest,
) (*roachpb.GetAllSystemSpanConfigsThatApplyResponse, error) {
	panic("unimplemented")
}

func (m *mockServer) UpdateSpanConfigs(
	context.Context, *roachpb.UpdateSpanConfigsRequest,
) (*roachpb.UpdateSpanConfigsResponse, error) {
	panic("unimplemented")
}

func (m *mockServer) SpanConfigConformance(
	context.Context, *roachpb.SpanConfigConformanceRequest,
) (*roachpb.SpanConfigConformanceResponse, error) {
	panic("unimplemented")
}

func (m *mockServer) GetRangeDescriptors(
	*kvpb.GetRangeDescriptorsRequest, kvpb.Internal_GetRangeDescriptorsServer,
) error {
	panic("unimplemented")
}

func gossipEventForClusterID(clusterID uuid.UUID) *kvpb.GossipSubscriptionEvent {
	return &kvpb.GossipSubscriptionEvent{
		Key:            gossip.KeyClusterID,
		Content:        roachpb.MakeValueFromBytesAndTimestamp(clusterID.GetBytes(), hlc.Timestamp{}),
		PatternMatched: gossip.KeyClusterID,
	}
}

func gossipEventForNodeDesc(desc *roachpb.NodeDescriptor) *kvpb.GossipSubscriptionEvent {
	val, err := protoutil.Marshal(desc)
	if err != nil {
		panic(err)
	}
	return &kvpb.GossipSubscriptionEvent{
		Key:            gossip.MakeNodeIDKey(desc.NodeID),
		Content:        roachpb.MakeValueFromBytesAndTimestamp(val, hlc.Timestamp{}),
		PatternMatched: gossip.MakePrefixPattern(gossip.KeyNodeDescPrefix),
	}
}

func gossipEventForStoreDesc(desc *roachpb.StoreDescriptor) *kvpb.GossipSubscriptionEvent {
	val, err := protoutil.Marshal(desc)
	if err != nil {
		panic(err)
	}
	return &kvpb.GossipSubscriptionEvent{
		Key:            gossip.MakeStoreDescKey(desc.StoreID),
		Content:        roachpb.MakeValueFromBytesAndTimestamp(val, hlc.Timestamp{}),
		PatternMatched: gossip.MakePrefixPattern(gossip.KeyStoreDescPrefix),
	}
}

func gossipEventForSystemConfig(cfg *config.SystemConfigEntries) *kvpb.GossipSubscriptionEvent {
	val, err := protoutil.Marshal(cfg)
	if err != nil {
		panic(err)
	}
	return &kvpb.GossipSubscriptionEvent{
		Key:            gossip.KeyDeprecatedSystemConfig,
		Content:        roachpb.MakeValueFromBytesAndTimestamp(val, hlc.Timestamp{}),
		PatternMatched: gossip.KeyDeprecatedSystemConfig,
	}
}

func waitForNodeDesc(t *testing.T, c *connector, nodeID roachpb.NodeID) {
	t.Helper()
	testutils.SucceedsSoon(t, func() error {
		_, err := c.GetNodeDescriptor(nodeID)
		return err
	})
}

func waitForStoreDesc(t *testing.T, c *connector, storeID roachpb.StoreID) {
	t.Helper()
	testutils.SucceedsSoon(t, func() error {
		_, err := c.GetStoreDescriptor(storeID)
		return err
	})
}

func newConnector(cfg ConnectorConfig, addrs []string) *connector {
	return NewConnector(cfg, addrs).(*connector)
}

// TestConnectorGossipSubscription tests connector's roles as a
// kvclient.NodeDescStore and as a config.SystemConfigProvider.
func TestConnectorGossipSubscription(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	clock := hlc.NewClockForTesting(nil)
	rpcContext := rpc.NewInsecureTestingContext(ctx, clock, stopper)
	s, err := rpc.NewServer(ctx, rpcContext)
	require.NoError(t, err)

	// Test setting the cluster ID by setting it to nil then ensuring it's later
	// set to the original ID value.
	clusterID := rpcContext.StorageClusterID.Get()
	rpcContext.StorageClusterID.Reset(uuid.Nil)

	gossipSubC := make(chan *kvpb.GossipSubscriptionEvent)
	defer close(gossipSubC)
	gossipSubFn := func(req *kvpb.GossipSubscriptionRequest, stream kvpb.Internal_GossipSubscriptionServer) error {
		assert.Len(t, req.Patterns, 4)
		assert.Equal(t, "cluster-id", req.Patterns[0])
		assert.Equal(t, "node:.*", req.Patterns[1])
		assert.Equal(t, "store:.*", req.Patterns[2])
		assert.Equal(t, "system-db", req.Patterns[3])
		for gossipSub := range gossipSubC {
			if err := stream.Send(gossipSub); err != nil {
				return err
			}
		}
		return nil
	}
	kvpb.RegisterInternalServer(s, &mockServer{gossipSubFn: gossipSubFn})
	ln, err := netutil.ListenAndServeGRPC(stopper, s, util.TestAddr)
	require.NoError(t, err)

	cfg := ConnectorConfig{
		AmbientCtx:      log.MakeTestingAmbientContext(stopper.Tracer()),
		RPCContext:      rpcContext,
		RPCRetryOptions: rpcRetryOpts,
	}
	addrs := []string{ln.Addr().String()}
	c := newConnector(cfg, addrs)

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
	select {
	case err := <-startedC:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatalf("failed to see start complete")
	}

	// Ensure that ClusterID was updated.
	require.Equal(t, clusterID, rpcContext.StorageClusterID.Get())

	// Test kvclient.NodeDescStore impl. Wait for full update first.
	waitForNodeDesc(t, c, 2)
	desc, err := c.GetNodeDescriptor(1)
	require.Equal(t, node1, desc)
	require.NoError(t, err)
	desc, err = c.GetNodeDescriptor(2)
	require.Equal(t, node2, desc)
	require.NoError(t, err)
	desc, err = c.GetNodeDescriptor(3)
	require.Nil(t, desc)
	require.Regexp(t, "node descriptor with node ID 3 was not found", err)

	// Test GetStoreDescriptor.
	storeID1 := roachpb.StoreID(1)
	store1 := &roachpb.StoreDescriptor{StoreID: storeID1, Node: *node1}
	storeID2 := roachpb.StoreID(2)
	store2 := &roachpb.StoreDescriptor{StoreID: storeID2, Node: *node2}
	gossipSubC <- gossipEventForStoreDesc(store1)
	gossipSubC <- gossipEventForStoreDesc(store2)
	waitForStoreDesc(t, c, storeID1)
	storeDesc, err := c.GetStoreDescriptor(storeID1)
	require.NoError(t, err)
	require.Equal(t, store1, storeDesc)
	waitForStoreDesc(t, c, storeID2)
	storeDesc, err = c.GetStoreDescriptor(storeID2)
	require.NoError(t, err)
	require.Equal(t, store2, storeDesc)
	storeDesc, err = c.GetStoreDescriptor(3)
	require.Nil(t, storeDesc)
	require.Regexp(t, "store descriptor with store ID 3 was not found", err)

	// Return updated GossipSubscription response.
	node1Up := &roachpb.NodeDescriptor{NodeID: 1, Address: util.MakeUnresolvedAddr("tcp", "1.2.3.4")}
	node3 := &roachpb.NodeDescriptor{NodeID: 3, Address: util.MakeUnresolvedAddr("tcp", "2.2.2.2")}
	gossipSubC <- gossipEventForNodeDesc(node1Up)
	gossipSubC <- gossipEventForNodeDesc(node3)

	// Test kvclient.NodeDescStore impl. Wait for full update first.
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

// TestConnectorGossipSubscription tests connector's role as a
// kvcoord.RangeDescriptorDB.
func TestConnectorRangeLookup(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	clock := hlc.NewClockForTesting(nil)
	rpcContext := rpc.NewInsecureTestingContext(ctx, clock, stopper)
	s, err := rpc.NewServer(ctx, rpcContext)
	require.NoError(t, err)

	rangeLookupRespC := make(chan *kvpb.RangeLookupResponse, 1)
	rangeLookupFn := func(_ context.Context, req *kvpb.RangeLookupRequest) (*kvpb.RangeLookupResponse, error) {
		// Validate request.
		assert.Equal(t, roachpb.RKey("a"), req.Key)
		assert.Equal(t, kvpb.READ_UNCOMMITTED, req.ReadConsistency)
		assert.Equal(t, int64(kvcoord.RangeLookupPrefetchCount), req.PrefetchNum)
		assert.Equal(t, false, req.PrefetchReverse)

		// Respond.
		return <-rangeLookupRespC, nil
	}
	server := &mockServer{rangeLookupFn: rangeLookupFn}
	kvpb.RegisterInternalServer(s, server)
	ln, err := netutil.ListenAndServeGRPC(stopper, s, util.TestAddr)
	require.NoError(t, err)

	cfg := ConnectorConfig{
		AmbientCtx:      log.MakeTestingAmbientContext(stopper.Tracer()),
		RPCContext:      rpcContext,
		RPCRetryOptions: rpcRetryOpts,
	}
	addrs := []string{ln.Addr().String()}
	c := newConnector(cfg, addrs)
	// NOTE: we don't actually start the connector worker. That's ok, as
	// RangeDescriptorDB methods don't require it to be running.

	// Success case.
	descs := []roachpb.RangeDescriptor{{RangeID: 1}, {RangeID: 2}}
	preDescs := []roachpb.RangeDescriptor{{RangeID: 3}, {RangeID: 4}}
	rangeLookupRespC <- &kvpb.RangeLookupResponse{
		Descriptors: descs, PrefetchedDescriptors: preDescs,
	}
	const rc = kvpb.READ_UNCOMMITTED
	resDescs, resPreDescs, err := c.RangeLookup(ctx, roachpb.RKey("a"), rc, false /* useReverseScan */)
	require.Equal(t, descs, resDescs)
	require.Equal(t, preDescs, resPreDescs)
	require.NoError(t, err)

	// Error case.
	rangeLookupRespC <- &kvpb.RangeLookupResponse{
		Error: kvpb.NewErrorf("hit error"),
	}
	resDescs, resPreDescs, err = c.RangeLookup(ctx, roachpb.RKey("a"), rc, false /* useReverseScan */)
	require.Nil(t, resDescs)
	require.Nil(t, resPreDescs)
	require.Regexp(t, "hit error", err)

	// Context cancelation.
	canceledCtx, cancel := context.WithCancel(ctx)
	blockingC := make(chan struct{})
	server.rangeLookupFn = func(ctx context.Context, _ *kvpb.RangeLookupRequest) (*kvpb.RangeLookupResponse, error) {
		<-blockingC
		<-ctx.Done()
		return nil, ctx.Err()
	}
	go func() {
		blockingC <- struct{}{}
		cancel()
	}()
	resDescs, resPreDescs, err = c.RangeLookup(canceledCtx, roachpb.RKey("a"), rc, false /* useReverseScan */)
	require.Nil(t, resDescs)
	require.Nil(t, resPreDescs)
	require.Regexp(t, context.Canceled.Error(), err)
}

// TestConnectorRetriesUnreachable tests that connector iterates over each of
// its provided addresses and retries until it is able to establish a connection
// on one of them.
func TestConnectorRetriesUnreachable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	clock := hlc.NewClockForTesting(nil)
	rpcContext := rpc.NewInsecureTestingContext(ctx, clock, stopper)
	s, err := rpc.NewServer(ctx, rpcContext)
	require.NoError(t, err)

	node1 := &roachpb.NodeDescriptor{NodeID: 1, Address: util.MakeUnresolvedAddr("tcp", "1.1.1.1")}
	node2 := &roachpb.NodeDescriptor{NodeID: 2, Address: util.MakeUnresolvedAddr("tcp", "2.2.2.2")}
	gossipSubEvents := []*kvpb.GossipSubscriptionEvent{
		gossipEventForClusterID(rpcContext.StorageClusterID.Get()),
		gossipEventForNodeDesc(node1),
		gossipEventForNodeDesc(node2),
	}
	gossipSubFn := func(req *kvpb.GossipSubscriptionRequest, stream kvpb.Internal_GossipSubscriptionServer) error {
		assert.Len(t, req.Patterns, 4)
		assert.Equal(t, "cluster-id", req.Patterns[0])
		assert.Equal(t, "node:.*", req.Patterns[1])
		assert.Equal(t, "store:.*", req.Patterns[2])
		assert.Equal(t, "system-db", req.Patterns[3])
		for _, event := range gossipSubEvents {
			if err := stream.Send(event); err != nil {
				return err
			}
		}
		<-stream.Context().Done()
		return stream.Context().Err()
	}
	kvpb.RegisterInternalServer(s, &mockServer{gossipSubFn: gossipSubFn})
	// Decompose netutil.ListenAndServeGRPC so we can listen before serving.
	ln, err := net.Listen(util.TestAddr.Network(), util.TestAddr.String())
	require.NoError(t, err)
	stopper.AddCloser(stop.CloserFn(s.Stop))
	_ = stopper.RunAsyncTask(ctx, "wait-quiesce", func(context.Context) {
		<-stopper.ShouldQuiesce()
		netutil.FatalIfUnexpected(ln.Close())
	})

	// Add listen address into list of other bogus addresses.
	cfg := ConnectorConfig{
		AmbientCtx:      log.MakeTestingAmbientContext(stopper.Tracer()),
		RPCContext:      rpcContext,
		RPCRetryOptions: rpcRetryOpts,
	}
	addrs := []string{"1.1.1.1:9999", ln.Addr().String(), "2.2.2.2:9999"}
	c := newConnector(cfg, addrs)
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

	// Begin serving on gRPC server. connector should quickly connect
	// and complete startup.
	_ = stopper.RunAsyncTask(ctx, "serve", func(context.Context) {
		netutil.FatalIfUnexpected(s.Serve(ln))
	})
	require.NoError(t, <-startedC)

	// Test kvclient.NodeDescStore impl. Wait for full update first.
	waitForNodeDesc(t, c, 2)
	desc, err := c.GetNodeDescriptor(1)
	require.Equal(t, node1, desc)
	require.NoError(t, err)
	desc, err = c.GetNodeDescriptor(2)
	require.Equal(t, node2, desc)
	require.NoError(t, err)
	desc, err = c.GetNodeDescriptor(3)
	require.Nil(t, desc)
	require.True(t, errors.HasType(err, &kvpb.DescNotFoundError{}))
	require.Regexp(t, "node descriptor with node ID 3 was not found", err)
}

// TestConnectorRetriesError tests that connector iterates over each of
// its provided addresses and retries if the error is retriable or bails out
// immediately if it is not.
func TestConnectorRetriesError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	clock := hlc.NewClockForTesting(nil)
	rpcContext := rpc.NewInsecureTestingContext(ctx, clock, stopper)

	// Function to create rpc server that would delegate to gossip and range lookup
	// callbacks.
	// Returns address on which server is listening for use in connector.
	createServer := func(
		t *testing.T,
		gossipSubFn func(req *kvpb.GossipSubscriptionRequest, stream kvpb.Internal_GossipSubscriptionServer) error,
		rangeLookupFn func(_ context.Context, req *kvpb.RangeLookupRequest) (*kvpb.RangeLookupResponse, error),
	) string {
		internalServer, err := rpc.NewServer(ctx, rpcContext)
		require.NoError(t, err)
		kvpb.RegisterInternalServer(internalServer, &mockServer{rangeLookupFn: rangeLookupFn, gossipSubFn: gossipSubFn})
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

			gossipSubFn := func(req *kvpb.GossipSubscriptionRequest, stream kvpb.Internal_GossipSubscriptionServer) error {
				return stream.Send(gossipEventForClusterID(rpcContext.StorageClusterID.Get()))
			}

			rangeLookupFn := func(_ context.Context, req *kvpb.RangeLookupRequest) (*kvpb.RangeLookupResponse, error) {
				descs := []roachpb.RangeDescriptor{{RangeID: 1}, {RangeID: 2}}
				preDescs := []roachpb.RangeDescriptor{{RangeID: 3}, {RangeID: 4}}
				return &kvpb.RangeLookupResponse{
					Descriptors: descs, PrefetchedDescriptors: preDescs,
				}, nil
			}

			var errorsReported int32 = 0
			rangeLookupRejectorFn := func(_ context.Context, req *kvpb.RangeLookupRequest) (*kvpb.RangeLookupResponse, error) {
				// Respond with error always
				atomic.AddInt32(&errorsReported, 1)
				return nil, grpcstatus.Errorf(spec.code, "range lookup rejected")
			}

			addr1 := createServer(t, gossipSubFn, rangeLookupFn)
			addr2 := createServer(t, gossipSubFn, rangeLookupRejectorFn)

			// Add listen address into list of other bogus addresses.
			cfg := ConnectorConfig{
				AmbientCtx:      log.MakeTestingAmbientContext(stopper.Tracer()),
				RPCContext:      rpcContext,
				RPCRetryOptions: rpcRetryOpts,
			}
			addrs := []string{addr1, addr2}
			c := newConnector(cfg, addrs)
			c.rpcDialTimeout = 5 * time.Millisecond // speed up test
			require.NoError(t, c.Start(ctx), "connector can't start")

			// Test will try to make range lookups until the server returning errors
			// is hit. It then checks that error was propagated or not. We use multiple
			// iterations as server choice is random and we need to hit failure only once
			// to check if it was retried.
			for i := 0; i < 100; i++ {
				_, _, err := c.RangeLookup(
					ctx, roachpb.RKey("a"), kvpb.READ_UNCOMMITTED, false,
				)
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
