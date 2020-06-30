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
	rs, err := NewReplicaSlice(ctx, ns, rd, nil /* leaseholder */)
	require.NoError(t, err)
	require.Equal(t, 3, rs.Len())

	// Check that learners are not included.
	typLearner := roachpb.LEARNER
	rd.InternalReplicas[2].Type = &typLearner
	rs, err = NewReplicaSlice(ctx, ns, rd, nil /* leaseholder */)
	require.NoError(t, err)
	require.Equal(t, 2, rs.Len())

	// Check that, if the leasehoder points to a learner, that learner is
	// included.
	leaseholder := &roachpb.ReplicaDescriptor{NodeID: 3, StoreID: 3}
	rs, err = NewReplicaSlice(ctx, ns, rd, leaseholder)
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

func nodeDesc(
	t *testing.T, nid roachpb.NodeID, sid roachpb.StoreID, locStrs []string,
) *roachpb.NodeDescriptor {
	return &roachpb.NodeDescriptor{
		Locality: locality(t, locStrs),
		Address:  addr(nid, sid),
	}
}

func info(t *testing.T, nid roachpb.NodeID, sid roachpb.StoreID, locStrs []string) ReplicaInfo {
	return ReplicaInfo{
		ReplicaDescriptor: desc(nid, sid),
		NodeDesc:          nodeDesc(t, nid, sid, locStrs),
	}
}

func TestReplicaSliceOptimizeReplicaOrder(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testCases := []struct {
		name       string
		node       *roachpb.NodeDescriptor
		latencies  map[string]time.Duration
		slice      ReplicaSlice
		expOrdered ReplicaSlice
	}{
		{
			name: "order by locality matching",
			node: nodeDesc(t, 1, 1, []string{"country=us", "region=west", "city=la"}),
			slice: ReplicaSlice{
				info(t, 2, 2, []string{"country=us", "region=west", "city=sf"}),
				info(t, 3, 3, []string{"country=uk", "city=london"}),
				info(t, 4, 4, []string{"country=us", "region=east", "city=ny"}),
			},
			expOrdered: ReplicaSlice{
				info(t, 2, 2, []string{"country=us", "region=west", "city=sf"}),
				info(t, 4, 4, []string{"country=us", "region=east", "city=ny"}),
				info(t, 3, 3, []string{"country=uk", "city=london"}),
			},
		},
		{
			name: "order by locality matching, put node first",
			node: nodeDesc(t, 1, 1, []string{"country=us", "region=west", "city=la"}),
			slice: ReplicaSlice{
				info(t, 1, 1, []string{"country=us", "region=west", "city=la"}),
				info(t, 2, 2, []string{"country=us", "region=west", "city=sf"}),
				info(t, 3, 3, []string{"country=uk", "city=london"}),
				info(t, 4, 4, []string{"country=us", "region=east", "city=ny"}),
			},
			expOrdered: ReplicaSlice{
				info(t, 1, 1, []string{"country=us", "region=west", "city=la"}),
				info(t, 2, 2, []string{"country=us", "region=west", "city=sf"}),
				info(t, 4, 4, []string{"country=us", "region=east", "city=ny"}),
				info(t, 3, 3, []string{"country=uk", "city=london"}),
			},
		},
		{
			name: "order by latency",
			node: nodeDesc(t, 1, 1, []string{"country=us", "region=west", "city=la"}),
			latencies: map[string]time.Duration{
				"2:2": time.Hour,
				"3:3": time.Minute,
				"4:4": time.Second,
			},
			slice: ReplicaSlice{
				info(t, 2, 2, []string{"country=us", "region=west", "city=sf"}),
				info(t, 4, 4, []string{"country=us", "region=east", "city=ny"}),
				info(t, 3, 3, []string{"country=uk", "city=london"}),
			},
			expOrdered: ReplicaSlice{
				info(t, 4, 4, []string{"country=us", "region=east", "city=ny"}),
				info(t, 3, 3, []string{"country=uk", "city=london"}),
				info(t, 2, 2, []string{"country=us", "region=west", "city=sf"}),
			},
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
			test.slice.OptimizeReplicaOrder(test.node, latencyFn)
			if !reflect.DeepEqual(test.slice, test.expOrdered) {
				t.Errorf("expected order %+v; got %+v", test.expOrdered, test.slice)
			}
		})
	}
}
