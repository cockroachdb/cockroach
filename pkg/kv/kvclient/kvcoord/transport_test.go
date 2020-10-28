// Copyright 2016 The Cockroach Authors.
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
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/rpcutils"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/caller"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestOrderReplicasExcludesVoters(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	rd := &roachpb.RangeDescriptor{
		InternalReplicas: []roachpb.ReplicaDescriptor{
			{NodeID: 1, StoreID: 1},
			{NodeID: 2, StoreID: 2},
			{NodeID: 3, StoreID: 3},
		},
	}
	ns := &mockNodeStore{
		nodes: []roachpb.NodeDescriptor{
			{
				NodeID:  1,
				Address: util.UnresolvedAddr{},
			},
			{
				NodeID:  2,
				Address: util.UnresolvedAddr{},
			},
			{
				NodeID:  3,
				Address: util.UnresolvedAddr{},
			},
		},
	}
	replicas, err := OrderReplicas(
		ctx, DontOrderByLatency,
		nil, /* curNode - we don't care about ordering */
		ns,
		rpcutils.AllGoodHealthChecker{},
		nil, /* latencyFn - we don't care about ordering */
		rd,
		roachpb.ReplicaDescriptor{}, /* leaseholder - we don't care about ordering */
	)
	require.NoError(t, err)
	require.Len(t, replicas, 3)

	// Check that learners are not included.
	typLearner := roachpb.LEARNER
	rd.InternalReplicas[2].Type = &typLearner
	replicas, err = OrderReplicas(
		ctx, DontOrderByLatency,
		nil, /* curNode - we don't care about ordering */
		ns,
		rpcutils.AllGoodHealthChecker{},
		nil, /* latencyFn - we don't care about ordering */
		rd,
		roachpb.ReplicaDescriptor{}, /* leaseholder - we don't care about ordering */
	)
	require.NoError(t, err)
	require.Len(t, replicas, 2)

	// Check that, if the leasehoder points to a learner, that learner is
	// included.
	leaseholder := roachpb.ReplicaDescriptor{NodeID: 3, StoreID: 3}

	replicas, err = OrderReplicas(
		ctx, DontOrderByLatency,
		nil, /* curNode - we don't care about ordering */
		ns,
		rpcutils.AllGoodHealthChecker{},
		nil, /* latencyFn - we don't care about ordering */
		rd,
		leaseholder,
	)
	require.NoError(t, err)
	require.Len(t, replicas, 3)
}

func TestOrderReplicasOrdersByLatency(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	testCases := []struct {
		name       string
		node       *roachpb.NodeDescriptor
		latencies  map[string]time.Duration
		slice      []testReplica
		expOrdered []roachpb.NodeID
	}{
		{
			name: "order by locality matching",
			node: nodeDesc(t, 1, 1, []string{"country=us", "region=west", "city=la"}),
			slice: []testReplica{
				info(t, 2, 2, []string{"country=us", "region=west", "city=sf"}),
				info(t, 3, 3, []string{"country=uk", "city=london"}),
				info(t, 4, 4, []string{"country=us", "region=east", "city=ny"}),
			},
			expOrdered: []roachpb.NodeID{2, 4, 3},
		},
		{
			name: "order by locality matching, put node first",
			node: nodeDesc(t, 1, 1, []string{"country=us", "region=west", "city=la"}),
			slice: []testReplica{
				info(t, 1, 1, []string{"country=us", "region=west", "city=la"}),
				info(t, 2, 2, []string{"country=us", "region=west", "city=sf"}),
				info(t, 3, 3, []string{"country=uk", "city=london"}),
				info(t, 4, 4, []string{"country=us", "region=east", "city=ny"}),
			},
			expOrdered: []roachpb.NodeID{1, 2, 4, 3},
		},
		{
			name: "order by latency",
			node: nodeDesc(t, 1, 1, []string{"country=us", "region=west", "city=la"}),
			latencies: map[string]time.Duration{
				"2:2": time.Hour,
				"3:3": time.Minute,
				"4:4": time.Second,
			},
			slice: []testReplica{
				info(t, 2, 2, []string{"country=us", "region=west", "city=sf"}),
				info(t, 4, 4, []string{"country=us", "region=east", "city=ny"}),
				info(t, 3, 3, []string{"country=uk", "city=london"}),
			},
			expOrdered: []roachpb.NodeID{4, 3, 2},
		},
	}
	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			var latencyFn LatencyFunc
			if test.latencies != nil {
				latencyFn = func(addr string) (time.Duration, bool) {
					lat, ok := test.latencies[addr]
					return lat, ok
				}
			}
			var desc roachpb.RangeDescriptor
			ns := mockNodeStore{}
			ns.nodes = append(ns.nodes, *test.node)
			for _, r := range test.slice {
				desc.InternalReplicas = append(desc.InternalReplicas, r.ReplicaDescriptor)
				ns.nodes = append(ns.nodes, *r.NodeDesc)
			}
			replicas, err := OrderReplicas(
				ctx,
				OrderByLatency,
				test.node,
				&ns,
				rpcutils.AllGoodHealthChecker{},
				latencyFn,
				&desc,
				roachpb.ReplicaDescriptor{}, // leaseholder
			)
			require.NoError(t, err)
			res := make([]roachpb.NodeID, len(replicas))
			for i, r := range replicas {
				res[i] = r.NodeID
			}
			require.Equal(t, test.expOrdered, res)
		})
	}
}

func nodeDesc(
	t *testing.T, nid roachpb.NodeID, sid roachpb.StoreID, locStrs []string,
) *roachpb.NodeDescriptor {
	return &roachpb.NodeDescriptor{
		NodeID:   nid,
		Locality: locality(t, locStrs),
		Address:  addr(nid, sid),
	}
}

func info(t *testing.T, nid roachpb.NodeID, sid roachpb.StoreID, locStrs []string) testReplica {
	return testReplica{
		ReplicaDescriptor: desc(nid, sid),
		NodeDesc:          nodeDesc(t, nid, sid, locStrs),
	}
}

// testReplica extends the Replica structure with the associated node
// descriptor.
type testReplica struct {
	roachpb.ReplicaDescriptor
	NodeDesc *roachpb.NodeDescriptor
}

func (i testReplica) locality() []roachpb.Tier {
	return i.NodeDesc.Locality.Tiers
}

func (i testReplica) addr() string {
	return i.NodeDesc.Address.String()
}

func desc(nid roachpb.NodeID, sid roachpb.StoreID) roachpb.ReplicaDescriptor {
	return roachpb.ReplicaDescriptor{NodeID: nid, StoreID: sid}
}

func addr(nid roachpb.NodeID, sid roachpb.StoreID) util.UnresolvedAddr {
	return util.MakeUnresolvedAddr("tcp", fmt.Sprintf("%d:%d", nid, sid))
}

func locality(t *testing.T, locStrs []string) roachpb.Locality {
	var locality roachpb.Locality
	for _, l := range locStrs {
		idx := strings.IndexByte(l, '=')
		if idx == -1 {
			t.Fatalf("locality %s not specified as <key>=<value>", l)
		}
		tier := roachpb.Tier{
			Key:   l[:idx],
			Value: l[idx+1:],
		}
		locality.Tiers = append(locality.Tiers, tier)
	}
	return locality
}

type mockNodeStore struct {
	nodes []roachpb.NodeDescriptor
}

var _ NodeDescStore = &mockNodeStore{}

// GetNodeDesc is part of the NodeDescStore interface.
func (ns *mockNodeStore) GetNodeDescriptor(nodeID roachpb.NodeID) (*roachpb.NodeDescriptor, error) {
	for _, nd := range ns.nodes {
		if nd.NodeID == nodeID {
			return &nd, nil
		}
	}
	return nil, errors.Errorf("unable to look up descriptor for n%d", nodeID)
}
func TestTransportMoveToFront(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	rd1 := roachpb.ReplicaDescriptor{NodeID: 1, StoreID: 1, ReplicaID: 1}
	rd2 := roachpb.ReplicaDescriptor{NodeID: 2, StoreID: 2, ReplicaID: 2}
	rd3 := roachpb.ReplicaDescriptor{NodeID: 3, StoreID: 3, ReplicaID: 3}
	gt := grpcTransport{replicas: []roachpb.ReplicaDescriptor{rd1, rd2, rd3}}

	verifyOrder := func(replicas []roachpb.ReplicaDescriptor) {
		file, line, _ := caller.Lookup(1)
		for i, r := range gt.replicas {
			if r != replicas[i] {
				t.Fatalf("%s:%d: expected order %+v; got mismatch at index %d: %+v",
					file, line, replicas, i, r)
			}
		}
	}

	verifyOrder([]roachpb.ReplicaDescriptor{rd1, rd2, rd3})

	// Move replica 2 to the front.
	gt.MoveToFront(rd2)
	verifyOrder([]roachpb.ReplicaDescriptor{rd2, rd1, rd3})

	// Now replica 3.
	gt.MoveToFront(rd3)
	verifyOrder([]roachpb.ReplicaDescriptor{rd3, rd1, rd2})

	// Advance the client index and move replica 3 back to front.
	gt.nextReplicaIdx++
	gt.MoveToFront(rd3)
	verifyOrder([]roachpb.ReplicaDescriptor{rd3, rd1, rd2})
	if gt.nextReplicaIdx != 0 {
		t.Fatalf("expected client index 0; got %d", gt.nextReplicaIdx)
	}

	// Advance the client index again and verify replica 3 can
	// be moved to front for a second retry.
	gt.nextReplicaIdx++
	gt.MoveToFront(rd3)
	verifyOrder([]roachpb.ReplicaDescriptor{rd3, rd1, rd2})
	if gt.nextReplicaIdx != 0 {
		t.Fatalf("expected client index 0; got %d", gt.nextReplicaIdx)
	}

	// Move replica 2 to the front.
	gt.MoveToFront(rd2)
	verifyOrder([]roachpb.ReplicaDescriptor{rd2, rd1, rd3})

	// Advance client index and move rd1 front; should be no change.
	gt.nextReplicaIdx++
	gt.MoveToFront(rd1)
	verifyOrder([]roachpb.ReplicaDescriptor{rd2, rd1, rd3})

	// Advance client index and and move rd1 to front. Should move
	// client index back for a retry.
	gt.nextReplicaIdx++
	gt.MoveToFront(rd1)
	verifyOrder([]roachpb.ReplicaDescriptor{rd2, rd1, rd3})
	if gt.nextReplicaIdx != 1 {
		t.Fatalf("expected client index 1; got %d", gt.nextReplicaIdx)
	}

	// Advance client index once more; verify second retry.
	gt.nextReplicaIdx++
	gt.MoveToFront(rd2)
	verifyOrder([]roachpb.ReplicaDescriptor{rd1, rd2, rd3})
	if gt.nextReplicaIdx != 1 {
		t.Fatalf("expected client index 1; got %d", gt.nextReplicaIdx)
	}
}

// TestSpanImport tests that the gRPC transport ingests trace information that
// came from gRPC responses (through the "snowball tracing" mechanism).
func TestSpanImport(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	metrics := makeDistSenderMetrics()
	gt := grpcTransport{
		opts: SendOptions{
			metrics: &metrics,
		},
	}
	server := mockInternalClient{}
	// Let's spice things up and simulate an error from the server.
	expectedErr := "my expected error"
	server.pErr = roachpb.NewErrorf(expectedErr /* nolint:fmtsafe */)

	recCtx, getRec, cancel := tracing.ContextWithRecordingSpan(ctx, "test")
	defer cancel()

	server.tr = opentracing.SpanFromContext(recCtx).Tracer().(*tracing.Tracer)

	br, err := gt.sendBatch(recCtx, roachpb.NodeID(1), &server, roachpb.BatchRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if !testutils.IsPError(br.Error, expectedErr) {
		t.Fatalf("expected err: %s, got: %q", expectedErr, br.Error)
	}
	expectedMsg := "mockInternalClient processing batch"
	if tracing.FindMsgInRecording(getRec(), expectedMsg) == -1 {
		t.Fatalf("didn't find expected message in trace: %s", expectedMsg)
	}
}

// mockInternalClient is an implementation of roachpb.InternalClient.
// It simulates aspects of how the Node normally handles tracing in gRPC calls.
type mockInternalClient struct {
	tr   *tracing.Tracer
	pErr *roachpb.Error
}

var _ roachpb.InternalClient = &mockInternalClient{}

// Batch is part of the roachpb.InternalClient interface.
func (m *mockInternalClient) Batch(
	ctx context.Context, in *roachpb.BatchRequest, opts ...grpc.CallOption,
) (*roachpb.BatchResponse, error) {
	sp := m.tr.StartRootSpan("mock", nil /* logTags */, tracing.RecordableSpan)
	defer sp.Finish()
	tracing.StartRecording(sp, tracing.SnowballRecording)
	ctx = opentracing.ContextWithSpan(ctx, sp)

	log.Eventf(ctx, "mockInternalClient processing batch")
	br := &roachpb.BatchResponse{}
	br.Error = m.pErr
	if rec := tracing.GetRecording(sp); rec != nil {
		br.CollectedSpans = append(br.CollectedSpans, rec...)
	}
	return br, nil
}

// RangeLookup implements the roachpb.InternalClient interface.
func (m *mockInternalClient) RangeLookup(
	ctx context.Context, rl *roachpb.RangeLookupRequest, _ ...grpc.CallOption,
) (*roachpb.RangeLookupResponse, error) {
	return nil, fmt.Errorf("unsupported RangeLookup call")
}

// RangeFeed is part of the roachpb.InternalClient interface.
func (m *mockInternalClient) RangeFeed(
	ctx context.Context, in *roachpb.RangeFeedRequest, opts ...grpc.CallOption,
) (roachpb.Internal_RangeFeedClient, error) {
	return nil, fmt.Errorf("unsupported RangeFeed call")
}

// GossipSubscription is part of the roachpb.InternalClient interface.
func (m *mockInternalClient) GossipSubscription(
	ctx context.Context, args *roachpb.GossipSubscriptionRequest, _ ...grpc.CallOption,
) (roachpb.Internal_GossipSubscriptionClient, error) {
	return nil, fmt.Errorf("unsupported GossipSubscripion call")
}

func (m *mockInternalClient) Join(
	context.Context, *roachpb.JoinNodeRequest, ...grpc.CallOption,
) (*roachpb.JoinNodeResponse, error) {
	return nil, fmt.Errorf("unsupported Join call")
}
