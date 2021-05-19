// Copyright 2015 The Cockroach Authors.
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
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/shuffle"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

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

func TestNewReplicaSlice(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	rd := &roachpb.RangeDescriptor{
		InternalReplicas: []roachpb.ReplicaDescriptor{
			{NodeID: 1, StoreID: 1, ReplicaID: 1},
			{NodeID: 2, StoreID: 2, ReplicaID: 2},
			{NodeID: 3, StoreID: 3, ReplicaID: 3},
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
	rs, err := NewReplicaSlice(ctx, ns, rd, nil, OnlyPotentialLeaseholders)
	require.NoError(t, err)
	require.Equal(t, 3, rs.Len())

	// Check that learners are not included.
	typLearner := roachpb.LEARNER
	rd.InternalReplicas[2].Type = &typLearner
	rs, err = NewReplicaSlice(ctx, ns, rd, nil, OnlyPotentialLeaseholders)
	require.NoError(t, err)
	require.Equal(t, 2, rs.Len())
	rs, err = NewReplicaSlice(ctx, ns, rd, nil, AllExtantReplicas)
	require.NoError(t, err)
	require.Equal(t, 2, rs.Len())

	// Check that non-voters are included iff we ask for them to be.
	typNonVoter := roachpb.NON_VOTER
	rd.InternalReplicas[2].Type = &typNonVoter
	rs, err = NewReplicaSlice(ctx, ns, rd, nil, AllExtantReplicas)
	require.NoError(t, err)
	require.Equal(t, 3, rs.Len())
	rs, err = NewReplicaSlice(ctx, ns, rd, nil, OnlyPotentialLeaseholders)
	require.NoError(t, err)
	require.Equal(t, 2, rs.Len())

	// Check that, if the leaseholder points to a learner, that learner is
	// included.
	leaseholder := &roachpb.ReplicaDescriptor{NodeID: 3, StoreID: 3, ReplicaID: 3}
	rs, err = NewReplicaSlice(ctx, ns, rd, leaseholder, OnlyPotentialLeaseholders)
	require.NoError(t, err)
	require.Equal(t, 3, rs.Len())
}

func getStores(rs ReplicaSlice) (r []roachpb.StoreID) {
	for i := range rs {
		r = append(r, rs[i].StoreID)
	}
	return
}

func createReplicaSlice() ReplicaSlice {
	rs := ReplicaSlice(nil)
	for i := 0; i < 5; i++ {
		rs = append(rs, ReplicaInfo{ReplicaDescriptor: roachpb.ReplicaDescriptor{StoreID: roachpb.StoreID(i + 1)}})
	}
	return rs
}

func TestReplicaSliceMoveToFront(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	rs := createReplicaSlice()
	rs.MoveToFront(0)
	exp := []roachpb.StoreID{1, 2, 3, 4, 5}
	if stores := getStores(rs); !reflect.DeepEqual(stores, exp) {
		t.Errorf("expected order %s, got %s", exp, stores)
	}
	rs.MoveToFront(2)
	exp = []roachpb.StoreID{3, 1, 2, 4, 5}
	if stores := getStores(rs); !reflect.DeepEqual(stores, exp) {
		t.Errorf("expected order %s, got %s", exp, stores)
	}
	rs.MoveToFront(4)
	exp = []roachpb.StoreID{5, 3, 1, 2, 4}
	if stores := getStores(rs); !reflect.DeepEqual(stores, exp) {
		t.Errorf("expected order %s, got %s", exp, stores)
	}
}

func desc(nid roachpb.NodeID, sid roachpb.StoreID) roachpb.ReplicaDescriptor {
	return roachpb.ReplicaDescriptor{NodeID: nid, StoreID: sid}
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

func nodeDesc(t *testing.T, nid roachpb.NodeID, locStrs []string) *roachpb.NodeDescriptor {
	return &roachpb.NodeDescriptor{
		NodeID:   nid,
		Locality: locality(t, locStrs),
		Address:  util.MakeUnresolvedAddr("tcp", fmt.Sprintf("%d:26257", nid)),
	}
}

func info(t *testing.T, nid roachpb.NodeID, sid roachpb.StoreID, locStrs []string) ReplicaInfo {
	return ReplicaInfo{
		ReplicaDescriptor: desc(nid, sid),
		NodeDesc:          nodeDesc(t, nid, locStrs),
	}
}

func TestReplicaSliceOptimizeReplicaOrder(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	testCases := []struct {
		name string
		node *roachpb.NodeDescriptor
		// map from node address (see nodeDesc()) to latency to that node.
		latencies map[string]time.Duration
		slice     ReplicaSlice
		// expOrder is the expected order in which the replicas sort. Replicas are
		// only identified by their node. If multiple replicas are on different
		// stores of the same node, the node only appears once in this list (as the
		// ordering between replicas on the same node is not deterministic).
		expOrdered []roachpb.NodeID
	}{
		{
			name: "order by locality matching",
			node: nodeDesc(t, 1, []string{"country=us", "region=west", "city=la"}),
			slice: ReplicaSlice{
				info(t, 1, 1, []string{"country=us", "region=west", "city=la"}),
				info(t, 2, 2, []string{"country=us", "region=west", "city=sf"}),
				info(t, 3, 3, []string{"country=uk", "city=london"}),
				info(t, 3, 33, []string{"country=uk", "city=london"}),
				info(t, 4, 4, []string{"country=us", "region=east", "city=ny"}),
			},
			expOrdered: []roachpb.NodeID{1, 2, 4, 3},
		},
		{
			name: "order by latency",
			node: nodeDesc(t, 1, []string{"country=us", "region=west", "city=la"}),
			latencies: map[string]time.Duration{
				"2:26257": time.Hour,
				"3:26257": time.Minute,
				"4:26257": time.Second,
			},
			slice: ReplicaSlice{
				info(t, 2, 2, []string{"country=us", "region=west", "city=sf"}),
				info(t, 4, 4, []string{"country=us", "region=east", "city=ny"}),
				info(t, 4, 44, []string{"country=us", "region=east", "city=ny"}),
				info(t, 3, 3, []string{"country=uk", "city=london"}),
			},
			expOrdered: []roachpb.NodeID{4, 3, 2},
		},
		{
			// Test that replicas on the local node sort first, regardless of factors
			// like their latency measurement (in production they won't have any
			// latency measurement).
			name: "local node comes first",
			node: nodeDesc(t, 1, nil),
			latencies: map[string]time.Duration{
				"1:26257": 10 * time.Hour,
				"2:26257": time.Hour,
				"3:26257": time.Minute,
				"4:26257": time.Second,
			},
			slice: ReplicaSlice{
				info(t, 1, 1, nil),
				info(t, 1, 2, nil),
				info(t, 2, 2, nil),
				info(t, 3, 3, nil),
				info(t, 4, 4, nil),
			},
			expOrdered: []roachpb.NodeID{1, 4, 3, 2},
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
			// Randomize the input order, as it's not supposed to matter.
			shuffle.Shuffle(test.slice)
			test.slice.OptimizeReplicaOrder(test.node, latencyFn)
			var sortedNodes []roachpb.NodeID
			sortedNodes = append(sortedNodes, test.slice[0].NodeID)
			for i := 1; i < len(test.slice); i++ {
				l := len(sortedNodes)
				if nid := test.slice[i].NodeID; nid != sortedNodes[l-1] {
					sortedNodes = append(sortedNodes, nid)
				}
			}
			if !reflect.DeepEqual(sortedNodes, test.expOrdered) {
				t.Errorf("expected node order %+v; got %+v", test.expOrdered, test.slice)
			}
		})
	}
}
