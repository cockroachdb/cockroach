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

	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangecache"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

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

func (f *firstNErrorTransport) Reset() {
	f.numSent = 0
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
				replicas ReplicaSlice,
			) Transport {
				return &firstNErrorTransport{
					replicas:  replicas,
					numErrors: test.numErrors,
				}
			},
			serverAddrs,
			rpcContext,
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
		Stopper:            stopper,
		TransportFactory:   transportFactory,
		FirstRangeProvider: g,
	})
	ds.rangeCache.Insert(ctx, roachpb.RangeInfo{
		Desc:  *desc,
		Lease: roachpb.Lease{},
	})
	routing, err := ds.getRoutingInfo(ctx, desc.StartKey, rangecache.EvictionToken{}, false /* useReverseScan */)
	require.NoError(t, err)

	return ds.sendToReplicas(ctx, &kvpb.BatchRequest{}, routing, false /* withCommit */)
}
