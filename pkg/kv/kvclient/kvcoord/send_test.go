// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvcoord

import (
	"context"
	"net"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangecache"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/netutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

var _ kvpb.InternalServer = Node(0)

type Node time.Duration

func (n Node) Batch(ctx context.Context, args *kvpb.BatchRequest) (*kvpb.BatchResponse, error) {
	if n > 0 {
		time.Sleep(time.Duration(n))
	}
	return &kvpb.BatchResponse{}, nil
}

func (n Node) RangeLookup(
	_ context.Context, _ *kvpb.RangeLookupRequest,
) (*kvpb.RangeLookupResponse, error) {
	panic("unimplemented")
}

func (n Node) RangeFeed(_ *kvpb.RangeFeedRequest, _ kvpb.Internal_RangeFeedServer) error {
	panic("unimplemented")
}

func (n Node) MuxRangeFeed(server kvpb.Internal_MuxRangeFeedServer) error {
	panic("unimplemented")
}

func (n Node) GossipSubscription(
	_ *kvpb.GossipSubscriptionRequest, _ kvpb.Internal_GossipSubscriptionServer,
) error {
	panic("unimplemented")
}

func (n Node) Join(context.Context, *kvpb.JoinNodeRequest) (*kvpb.JoinNodeResponse, error) {
	panic("unimplemented")
}

func (n Node) ResetQuorum(
	context.Context, *kvpb.ResetQuorumRequest,
) (*kvpb.ResetQuorumResponse, error) {
	panic("unimplemented")
}

func (n Node) TokenBucket(
	ctx context.Context, in *kvpb.TokenBucketRequest,
) (*kvpb.TokenBucketResponse, error) {
	panic("unimplemented")
}

func (n Node) GetSpanConfigs(
	_ context.Context, _ *roachpb.GetSpanConfigsRequest,
) (*roachpb.GetSpanConfigsResponse, error) {
	panic("unimplemented")
}

func (n Node) GetAllSystemSpanConfigsThatApply(
	_ context.Context, _ *roachpb.GetAllSystemSpanConfigsThatApplyRequest,
) (*roachpb.GetAllSystemSpanConfigsThatApplyResponse, error) {
	panic("unimplemented")
}

func (n Node) UpdateSpanConfigs(
	_ context.Context, _ *roachpb.UpdateSpanConfigsRequest,
) (*roachpb.UpdateSpanConfigsResponse, error) {
	panic("unimplemented")
}

func (n Node) SpanConfigConformance(
	context.Context, *roachpb.SpanConfigConformanceRequest,
) (*roachpb.SpanConfigConformanceResponse, error) {
	panic("unimplemented")
}

func (n Node) TenantSettings(
	*kvpb.TenantSettingsRequest, kvpb.Internal_TenantSettingsServer,
) error {
	panic("unimplemented")
}

func (n Node) GetRangeDescriptors(
	*kvpb.GetRangeDescriptorsRequest, kvpb.Internal_GetRangeDescriptorsServer,
) error {
	panic("unimplemented")
}

// TestSendToOneClient verifies that Send correctly sends a request
// to one server using the heartbeat RPC.
func TestSendToOneClient(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	clock := hlc.NewClockForTesting(nil)
	rpcContext := rpc.NewInsecureTestingContext(ctx, clock, stopper)
	// This test uses the testing function sendBatch() which does not
	// support setting the node ID on GRPCDialNode(). Disable Node ID
	// checks to avoid log.Fatal.
	rpcContext.TestingAllowNamedRPCToAnonymousServer = true

	s, err := rpc.NewServer(ctx, rpcContext)
	require.NoError(t, err)
	kvpb.RegisterInternalServer(s, Node(0))
	ln, err := netutil.ListenAndServeGRPC(rpcContext.Stopper, s, util.TestAddr)
	require.NoError(t, err)
	nodeDialer := nodedialer.New(rpcContext, func(roachpb.NodeID) (net.Addr, roachpb.Locality, error) {
		return ln.Addr(), roachpb.Locality{}, nil
	})

	reply, err := sendBatch(ctx, t, nil, []net.Addr{ln.Addr()}, rpcContext, nodeDialer)
	require.NoError(t, err)
	if reply == nil {
		t.Errorf("expected reply")
	}
}

// firstNErrorTransport is a mock transport that sends an error on
// requests to the first N addresses, then succeeds.
type firstNErrorTransport struct {
	replicas  ReplicaSlice
	numErrors int
	numSent   int
}

func (f *firstNErrorTransport) IsExhausted() bool {
	return f.numSent >= len(f.replicas)
}

func (f *firstNErrorTransport) Release() {}

func (f *firstNErrorTransport) SendNext(
	_ context.Context, _ *kvpb.BatchRequest,
) (*kvpb.BatchResponse, error) {
	var err error
	if f.numSent < f.numErrors {
		err = errors.New("firstNErrorTransport injected error")
	}
	f.numSent++
	return &kvpb.BatchResponse{}, err
}

func (f *firstNErrorTransport) NextInternalClient(
	ctx context.Context,
) (rpc.RestrictedInternalClient, error) {
	panic("unimplemented")
}

func (f *firstNErrorTransport) NextReplica() roachpb.ReplicaDescriptor {
	return f.replicas[f.numSent].ReplicaDescriptor
}

func (f *firstNErrorTransport) SkipReplica() {
	panic("SkipReplica not supported")
}

func (*firstNErrorTransport) MoveToFront(roachpb.ReplicaDescriptor) bool {
	return true
}

// TestComplexScenarios verifies various complex success/failure scenarios by
// mocking sendOne.
func TestComplexScenarios(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	clock := hlc.NewClockForTesting(nil)
	rpcContext := rpc.NewInsecureTestingContext(ctx, clock, stopper)
	// We're going to serve multiple node IDs with that one
	// context. Disable node ID checks.
	rpcContext.TestingAllowNamedRPCToAnonymousServer = true
	nodeDialer := nodedialer.New(rpcContext, nil)

	// TODO(bdarnell): the retryable flag is no longer used for RPC errors.
	// Rework this test to incorporate application-level errors carried in
	// the BatchResponse.
	testCases := []struct {
		numServers int
		numErrors  int
		success    bool
	}{
		// --- Success scenarios ---
		{1, 0, true},
		{5, 0, true},
		// There are some errors, but enough RPCs succeed.
		{5, 1, true},
		{5, 4, true},
		{5, 2, true},

		// --- Failure scenarios ---
		// All RPCs fail.
		{5, 5, false},
	}
	for i, test := range testCases {
		var serverAddrs []net.Addr
		for j := 0; j < test.numServers; j++ {
			serverAddrs = append(serverAddrs, util.NewUnresolvedAddr("dummy",
				strconv.Itoa(j)))
		}

		reply, err := sendBatch(
			ctx,
			t,
			func(
				_ SendOptions,
				_ *nodedialer.Dialer,
				replicas ReplicaSlice,
			) (Transport, error) {
				return &firstNErrorTransport{
					replicas:  replicas,
					numErrors: test.numErrors,
				}, nil
			},
			serverAddrs,
			rpcContext,
			nodeDialer,
		)
		if test.success {
			if err != nil {
				t.Errorf("%d: unexpected error: %s", i, err)
			}
			if reply == nil {
				t.Errorf("%d: expected reply", i)
			}
		} else {
			if err == nil {
				t.Errorf("%d: unexpected success", i)
			}
		}
	}
}

// TestSplitHealthy tests that the splitHealthy method sorts healthy nodes
// before unhealthy nodes.
func TestSplitHealthy(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	type batchClient struct {
		replica roachpb.ReplicaDescriptor
		healthy bool
	}

	testData := []struct {
		in  []batchClient
		out []roachpb.ReplicaDescriptor
	}{
		{nil, []roachpb.ReplicaDescriptor{}},
		{
			[]batchClient{
				{replica: roachpb.ReplicaDescriptor{NodeID: 1}, healthy: false},
				{replica: roachpb.ReplicaDescriptor{NodeID: 2}, healthy: false},
				{replica: roachpb.ReplicaDescriptor{NodeID: 3}, healthy: true},
			},
			[]roachpb.ReplicaDescriptor{{NodeID: 3}, {NodeID: 1}, {NodeID: 2}},
		},
		{
			[]batchClient{
				{replica: roachpb.ReplicaDescriptor{NodeID: 1}, healthy: true},
				{replica: roachpb.ReplicaDescriptor{NodeID: 2}, healthy: false},
				{replica: roachpb.ReplicaDescriptor{NodeID: 3}, healthy: true},
			},
			[]roachpb.ReplicaDescriptor{{NodeID: 1}, {NodeID: 3}, {NodeID: 2}},
		},
		{
			[]batchClient{
				{replica: roachpb.ReplicaDescriptor{NodeID: 1}, healthy: true},
				{replica: roachpb.ReplicaDescriptor{NodeID: 2}, healthy: true},
				{replica: roachpb.ReplicaDescriptor{NodeID: 3}, healthy: true},
			},
			[]roachpb.ReplicaDescriptor{{NodeID: 1}, {NodeID: 2}, {NodeID: 3}},
		},
	}

	for _, td := range testData {
		t.Run("", func(t *testing.T) {
			replicas := make([]roachpb.ReplicaDescriptor, len(td.in))
			var health util.FastIntMap
			for i, r := range td.in {
				replicas[i] = r.replica
				if r.healthy {
					health.Set(i, healthHealthy)
				} else {
					health.Set(i, healthUnhealthy)
				}
			}
			gt := grpcTransport{
				replicas:      replicas,
				replicaHealth: health,
			}
			gt.splitHealthy()
			if !reflect.DeepEqual(gt.replicas, td.out) {
				t.Errorf("splitHealthy(...) = %+v not %+v", replicas, td.out)
			}
		})
	}
}

// sendBatch sends Batch requests to specified addresses using send.
func sendBatch(
	ctx context.Context,
	t *testing.T,
	transportFactory TransportFactory,
	addrs []net.Addr,
	rpcContext *rpc.Context,
	nodeDialer *nodedialer.Dialer,
) (*kvpb.BatchResponse, error) {
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	g := makeGossip(t, stopper, rpcContext)

	desc := new(roachpb.RangeDescriptor)
	desc.StartKey = roachpb.RKeyMin
	desc.EndKey = roachpb.RKeyMax
	for i, addr := range addrs {
		nd := &roachpb.NodeDescriptor{
			NodeID:  roachpb.NodeID(i + 1),
			Address: util.MakeUnresolvedAddr(addr.Network(), addr.String()),
		}
		err := g.AddInfoProto(gossip.MakeNodeIDKey(nd.NodeID), nd, gossip.NodeDescriptorTTL)
		require.NoError(t, err)

		desc.InternalReplicas = append(desc.InternalReplicas,
			roachpb.ReplicaDescriptor{
				NodeID:    nd.NodeID,
				StoreID:   0,
				ReplicaID: roachpb.ReplicaID(i + 1),
			})
	}

	ds := NewDistSender(DistSenderConfig{
		AmbientCtx:         log.MakeTestingAmbientCtxWithNewTracer(),
		Settings:           cluster.MakeTestingClusterSettings(),
		NodeDescs:          g,
		RPCContext:         rpcContext,
		NodeDialer:         nodeDialer,
		FirstRangeProvider: g,
		TestingKnobs: ClientTestingKnobs{
			TransportFactory: transportFactory,
		},
	})
	ds.rangeCache.Insert(ctx, roachpb.RangeInfo{
		Desc:  *desc,
		Lease: roachpb.Lease{},
	})
	routing, err := ds.getRoutingInfo(ctx, desc.StartKey, rangecache.EvictionToken{}, false /* useReverseScan */)
	require.NoError(t, err)

	return ds.sendToReplicas(ctx, &kvpb.BatchRequest{}, routing, false /* withCommit */)
}
