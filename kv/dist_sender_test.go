// Copyright 2014 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Kathy Spradlin (kathyspradlin@gmail.com)
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package kv

import (
	"bytes"
	"errors"
	"fmt"
	"net"
	"reflect"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/gossip/simulation"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/testutils"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/gogo/protobuf/proto"
)

var testRangeDescriptor = roachpb.RangeDescriptor{
	RangeID:  1,
	StartKey: roachpb.RKey("a"),
	EndKey:   roachpb.RKey("z"),
	Replicas: []roachpb.ReplicaDescriptor{
		{
			NodeID:  1,
			StoreID: 1,
		},
	},
}

var testAddress = util.NewUnresolvedAddr("tcp", "node1:26257")

func makeTestGossip(t *testing.T) (*gossip.Gossip, func()) {
	n := simulation.NewNetwork(1)
	g := n.Nodes[0].Gossip

	if err := g.AddInfo(gossip.KeySentinel, nil, time.Hour); err != nil {
		t.Fatal(err)
	}
	if err := g.AddInfoProto(gossip.KeyFirstRangeDescriptor, &testRangeDescriptor, time.Hour); err != nil {
		t.Fatal(err)
	}
	nodeIDKey := gossip.MakeNodeIDKey(1)
	if err := g.AddInfoProto(nodeIDKey, &roachpb.NodeDescriptor{
		NodeID:  1,
		Address: *testAddress,
		Attrs:   roachpb.Attributes{Attrs: []string{"attr1", "attr2"}},
	}, time.Hour); err != nil {
		t.Fatal(err)
	}
	return g, n.Stop
}

// TestMoveLocalReplicaToFront verifies that optimizeReplicaOrder correctly
// move the local replica to the front.
func TestMoveLocalReplicaToFront(t *testing.T) {
	defer leaktest.AfterTest(t)
	testCase := []struct {
		slice         replicaSlice
		localNodeDesc roachpb.NodeDescriptor
	}{
		{
			// No attribute prefix
			slice: replicaSlice{
				replicaInfo{
					ReplicaDescriptor: roachpb.ReplicaDescriptor{NodeID: 2, StoreID: 2},
					NodeDesc:          &roachpb.NodeDescriptor{NodeID: 2},
				},
				replicaInfo{
					ReplicaDescriptor: roachpb.ReplicaDescriptor{NodeID: 3, StoreID: 3},
					NodeDesc:          &roachpb.NodeDescriptor{NodeID: 3},
				},
				replicaInfo{
					ReplicaDescriptor: roachpb.ReplicaDescriptor{NodeID: 1, StoreID: 1},
					NodeDesc:          &roachpb.NodeDescriptor{NodeID: 1},
				},
			},
			localNodeDesc: roachpb.NodeDescriptor{NodeID: 1},
		},
		{
			// Sort replicas by attribute
			slice: replicaSlice{
				replicaInfo{
					ReplicaDescriptor: roachpb.ReplicaDescriptor{NodeID: 2, StoreID: 2},
					NodeDesc:          &roachpb.NodeDescriptor{NodeID: 2, Attrs: roachpb.Attributes{Attrs: []string{"ad"}}},
				},
				replicaInfo{
					ReplicaDescriptor: roachpb.ReplicaDescriptor{NodeID: 3, StoreID: 3},
					NodeDesc:          &roachpb.NodeDescriptor{NodeID: 3, Attrs: roachpb.Attributes{Attrs: []string{"ab", "c"}}},
				},
				replicaInfo{
					ReplicaDescriptor: roachpb.ReplicaDescriptor{NodeID: 1, StoreID: 1},
					NodeDesc:          &roachpb.NodeDescriptor{NodeID: 1, Attrs: roachpb.Attributes{Attrs: []string{"ab"}}},
				},
			},
			localNodeDesc: roachpb.NodeDescriptor{NodeID: 1, Attrs: roachpb.Attributes{Attrs: []string{"ab"}}},
		},
	}
	for _, test := range testCase {
		ctx := &DistSenderContext{nodeDescriptor: &test.localNodeDesc}
		ds := NewDistSender(ctx, nil)
		ds.optimizeReplicaOrder(test.slice)
		if s := test.slice[0]; s.NodeID != ctx.nodeDescriptor.NodeID {
			t.Errorf("unexpected header, wanted nodeid = %d, got %d", ctx.nodeDescriptor.NodeID, s.NodeID)
		}
	}

}

// TestSendRPCOrder verifies that sendRPC correctly takes into account the
// leader, attributes and required consistency to determine where to send
// remote requests.
func TestSendRPCOrder(t *testing.T) {
	defer leaktest.AfterTest(t)
	g, s := makeTestGossip(t)
	defer s()
	rangeID := roachpb.RangeID(99)

	nodeAttrs := map[int32][]string{
		1: {}, // The local node, set in each test case.
		2: {"us", "west", "gpu"},
		3: {"eu", "dublin", "pdu2", "gpu"},
		4: {"us", "east", "gpu"},
		5: {"us", "east", "gpu", "flaky"},
	}

	// Gets filled below to identify the replica by its address.
	addrToNode := make(map[string]int32)
	makeVerifier := func(expOrder orderingPolicy,
		expAddrs []int32) func(SendOptions, []net.Addr) error {
		return func(o SendOptions, addrs []net.Addr) error {
			if o.Ordering != expOrder {
				return util.Errorf("unexpected ordering, wanted %v, got %v",
					expOrder, o.Ordering)
			}
			var actualAddrs []int32
			for i, a := range addrs {
				if len(expAddrs) <= i {
					return util.Errorf("got unexpected address: %s", a)
				}
				if expAddrs[i] == 0 {
					actualAddrs = append(actualAddrs, 0)
				} else {
					actualAddrs = append(actualAddrs, addrToNode[a.String()])
				}
			}
			if !reflect.DeepEqual(expAddrs, actualAddrs) {
				return util.Errorf("expected %d, but found %d", expAddrs, actualAddrs)
			}
			return nil
		}
	}

	testCases := []struct {
		args       roachpb.Request
		attrs      []string
		order      orderingPolicy
		expReplica []int32
		leader     int32 // 0 for not caching a leader.
		// Naming is somewhat off, as eventually consistent reads usually
		// do not have to go to the leader when a node has a read lease.
		// Would really want CONSENSUS here, but that is not implemented.
		// Likely a test setup here will never have a read lease, but good
		// to keep in mind.
		consistent bool
	}{
		// Inconsistent Scan without matching attributes.
		{
			args:       &roachpb.ScanRequest{},
			attrs:      []string{},
			order:      orderRandom,
			expReplica: []int32{1, 2, 3, 4, 5},
		},
		// Inconsistent Scan with matching attributes.
		// Should move the two nodes matching the attributes to the front and
		// go stable.
		{
			args:  &roachpb.ScanRequest{},
			attrs: nodeAttrs[5],
			order: orderStable,
			// Compare only the first two resulting addresses.
			expReplica: []int32{5, 4, 0, 0, 0},
		},

		// Scan without matching attributes that requires but does not find
		// a leader.
		{
			args:       &roachpb.ScanRequest{},
			attrs:      []string{},
			order:      orderRandom,
			expReplica: []int32{1, 2, 3, 4, 5},
			consistent: true,
		},
		// Put without matching attributes that requires but does not find leader.
		// Should go random and not change anything.
		{
			args:       &roachpb.PutRequest{},
			attrs:      []string{"nomatch"},
			order:      orderRandom,
			expReplica: []int32{1, 2, 3, 4, 5},
		},
		// Put with matching attributes but no leader.
		// Should move the two nodes matching the attributes to the front and
		// go stable.
		{
			args:  &roachpb.PutRequest{},
			attrs: append(nodeAttrs[5], "irrelevant"),
			// Compare only the first two resulting addresses.
			order:      orderStable,
			expReplica: []int32{5, 4, 0, 0, 0},
		},
		// Put with matching attributes that finds the leader (node 3).
		// Should address the leader and the two nodes matching the attributes
		// (the last and second to last) in that order.
		{
			args:  &roachpb.PutRequest{},
			attrs: append(nodeAttrs[5], "irrelevant"),
			// Compare only the first resulting addresses as we have a leader
			// and that means we're only trying to send there.
			order:      orderStable,
			expReplica: []int32{2, 5, 4, 0, 0},
			leader:     2,
		},
		// Inconsistent Get without matching attributes but leader (node 3). Should just
		// go random as the leader does not matter.
		{
			args:       &roachpb.GetRequest{},
			attrs:      []string{},
			order:      orderRandom,
			expReplica: []int32{1, 2, 3, 4, 5},
			leader:     2,
		},
	}

	descriptor := roachpb.RangeDescriptor{
		StartKey: roachpb.RKeyMin,
		EndKey:   roachpb.RKeyMax,
		RangeID:  rangeID,
		Replicas: nil,
	}

	// Stub to be changed in each test case.
	var verifyCall func(SendOptions, []net.Addr) error

	var testFn rpcSendFn = func(opts SendOptions, method string,
		addrs []net.Addr, getArgs func(addr net.Addr) *roachpb.BatchRequest, _ *rpc.Context) (proto.Message, error) {
		if err := verifyCall(opts, addrs); err != nil {
			return nil, err
		}
		return getArgs(addrs[0]).CreateReply(), nil
	}

	ctx := &DistSenderContext{
		RPCSend: testFn,
		RangeDescriptorDB: mockRangeDescriptorDB(func(roachpb.RKey, bool, bool) ([]roachpb.RangeDescriptor, *roachpb.Error) {
			return []roachpb.RangeDescriptor{descriptor}, nil
		}),
	}

	ds := NewDistSender(ctx, g)

	for n, tc := range testCases {
		verifyCall = makeVerifier(tc.order, tc.expReplica)
		descriptor.Replicas = nil // could do this once above, but more convenient here
		for i := int32(1); i <= 5; i++ {
			addr := util.MakeUnresolvedAddr("tcp", fmt.Sprintf("node%d", i))
			addrToNode[addr.String()] = i
			nd := &roachpb.NodeDescriptor{
				NodeID:  roachpb.NodeID(i),
				Address: util.MakeUnresolvedAddr(addr.Network(), addr.String()),
				Attrs: roachpb.Attributes{
					Attrs: nodeAttrs[i],
				},
			}
			if err := g.AddInfoProto(gossip.MakeNodeIDKey(roachpb.NodeID(i)), nd, time.Hour); err != nil {
				t.Fatal(err)
			}
			descriptor.Replicas = append(descriptor.Replicas, roachpb.ReplicaDescriptor{
				NodeID:  roachpb.NodeID(i),
				StoreID: roachpb.StoreID(i),
			})
		}

		{
			// The local node needs to get its attributes during sendRPC.
			nd := &roachpb.NodeDescriptor{
				NodeID: 6,
				Attrs: roachpb.Attributes{
					Attrs: tc.attrs,
				},
			}
			g.SetNodeID(nd.NodeID)
			if err := g.SetNodeDescriptor(nd); err != nil {
				t.Fatal(err)
			}
		}

		ds.leaderCache.Update(roachpb.RangeID(rangeID), roachpb.ReplicaDescriptor{})
		if tc.leader > 0 {
			ds.leaderCache.Update(roachpb.RangeID(rangeID), descriptor.Replicas[tc.leader-1])
		}

		args := tc.args
		args.Header().Key = roachpb.Key("a")
		if roachpb.IsRange(args) {
			args.Header().EndKey = roachpb.Key("b")
		}
		consistency := roachpb.CONSISTENT
		if !tc.consistent {
			consistency = roachpb.INCONSISTENT
		}
		// Kill the cached NodeDescriptor, enforcing a lookup from Gossip.
		ds.nodeDescriptor = nil
		if _, err := client.SendWrappedWith(ds, nil, roachpb.Header{
			RangeID:         rangeID, // Not used in this test, but why not.
			ReadConsistency: consistency,
		}, args); err != nil {
			t.Errorf("%d: %s", n, err)
		}
	}
}

type mockRangeDescriptorDB func(roachpb.RKey, bool, bool) ([]roachpb.RangeDescriptor, *roachpb.Error)

func (mdb mockRangeDescriptorDB) RangeLookup(key roachpb.RKey, _ *roachpb.RangeDescriptor, considerIntents, useReverseScan bool) ([]roachpb.RangeDescriptor, *roachpb.Error) {
	if bytes.HasPrefix(key, keys.Meta2Prefix) {
		return mdb(key[len(keys.Meta1Prefix):], considerIntents, useReverseScan)
	}
	if bytes.HasPrefix(key, keys.Meta1Prefix) {
		return mdb(keys.MakeKey(keys.Meta2Prefix, key[len(keys.Meta1Prefix):]), considerIntents, useReverseScan)
	}
	// First range.
	return mdb(nil, considerIntents, useReverseScan)
}
func (mdb mockRangeDescriptorDB) FirstRange() (*roachpb.RangeDescriptor, *roachpb.Error) {
	rs, err := mdb.RangeLookup(nil, nil, false /* considerIntents */, false /* useReverseScan */)
	if err != nil || len(rs) == 0 {
		return nil, err
	}
	return &rs[0], nil
}

func TestOwnNodeCertain(t *testing.T) {
	defer leaktest.AfterTest(t)
	g, s := makeTestGossip(t)
	defer s()
	const expNodeID = 42
	nd := &roachpb.NodeDescriptor{
		NodeID:  expNodeID,
		Address: util.MakeUnresolvedAddr("tcp", "foobar:1234"),
	}
	g.SetNodeID(nd.NodeID)
	if err := g.SetNodeDescriptor(nd); err != nil {
		t.Fatal(err)
	}
	if err := g.AddInfoProto(gossip.MakeNodeIDKey(expNodeID), nd, time.Hour); err != nil {
		t.Fatal(err)
	}

	var act roachpb.NodeList
	var testFn rpcSendFn = func(_ SendOptions, _ string, _ []net.Addr, getArgs func(addr net.Addr) *roachpb.BatchRequest, _ *rpc.Context) (proto.Message, error) {
		ba := getArgs(nil)
		for _, nodeID := range ba.Txn.CertainNodes.Nodes {
			act.Add(roachpb.NodeID(nodeID))
		}
		return ba.CreateReply(), nil
	}

	ctx := &DistSenderContext{
		RPCSend: testFn,
		RangeDescriptorDB: mockRangeDescriptorDB(func(_ roachpb.RKey, _, _ bool) ([]roachpb.RangeDescriptor, *roachpb.Error) {
			return []roachpb.RangeDescriptor{testRangeDescriptor}, nil
		}),
	}
	ds := NewDistSender(ctx, g)
	v := roachpb.MakeValueFromString("value")
	put := roachpb.NewPut(roachpb.Key("a"), v)
	if _, err := client.SendWrappedWith(ds, nil, roachpb.Header{
		Txn: &roachpb.Transaction{},
	}, put); err != nil {
		t.Fatalf("put encountered error: %s", err)
	}
	if expNodes := []roachpb.NodeID{expNodeID}; !reflect.DeepEqual(act.Nodes, expNodes) {
		t.Fatalf("got %v, expected %v", act.Nodes, expNodes)
	}

}

// TestRetryOnNotLeaderError verifies that the DistSender correctly updates the
// leader cache and retries when receiving a NotLeaderError.
func TestRetryOnNotLeaderError(t *testing.T) {
	defer leaktest.AfterTest(t)
	g, s := makeTestGossip(t)
	defer s()
	leader := roachpb.ReplicaDescriptor{
		NodeID:  99,
		StoreID: 999,
	}
	first := true

	var testFn rpcSendFn = func(_ SendOptions, method string, addrs []net.Addr, getArgs func(addr net.Addr) *roachpb.BatchRequest, _ *rpc.Context) (proto.Message, error) {
		if first {
			reply := &roachpb.BatchResponse{}
			reply.SetGoError(
				&roachpb.NotLeaderError{Leader: &leader, Replica: &roachpb.ReplicaDescriptor{}})
			first = false
			return reply, nil
		}
		return getArgs(nil).CreateReply(), nil
	}

	ctx := &DistSenderContext{
		RPCSend: testFn,
		RangeDescriptorDB: mockRangeDescriptorDB(func(_ roachpb.RKey, _, _ bool) ([]roachpb.RangeDescriptor, *roachpb.Error) {
			return []roachpb.RangeDescriptor{testRangeDescriptor}, nil
		}),
	}
	ds := NewDistSender(ctx, g)
	v := roachpb.MakeValueFromString("value")
	put := roachpb.NewPut(roachpb.Key("a"), v)
	if _, err := client.SendWrapped(ds, nil, put); err != nil {
		t.Errorf("put encountered error: %s", err)
	}
	if first {
		t.Errorf("The command did not retry")
	}
	if cur := ds.leaderCache.Lookup(1); cur.StoreID != leader.StoreID {
		t.Errorf("leader cache was not updated: expected %v, got %v",
			&leader, cur)
	}
}

// TestRetryOnDescriptorLookupError verifies that the DistSender retries a descriptor
// lookup on retryable errors.
func TestRetryOnDescriptorLookupError(t *testing.T) {
	defer leaktest.AfterTest(t)
	g, s := makeTestGossip(t)
	defer s()

	var testFn rpcSendFn = func(_ SendOptions, _ string, _ []net.Addr, getArgs func(addr net.Addr) *roachpb.BatchRequest, _ *rpc.Context) (proto.Message, error) {
		return getArgs(nil).CreateReply(), nil
	}

	pErrs := []*roachpb.Error{
		roachpb.NewError(errors.New("fatal boom")),
		roachpb.NewError(&roachpb.RangeKeyMismatchError{}), // retryable
		nil,
		nil,
	}

	ctx := &DistSenderContext{
		RPCSend: testFn,
		RangeDescriptorDB: mockRangeDescriptorDB(func(k roachpb.RKey, _, _ bool) ([]roachpb.RangeDescriptor, *roachpb.Error) {
			// Return next error and truncate the prefix of the errors array.
			var pErr *roachpb.Error
			if k != nil {
				pErr = pErrs[0]
				pErrs = pErrs[1:]
			}
			return []roachpb.RangeDescriptor{testRangeDescriptor}, pErr
		}),
	}
	ds := NewDistSender(ctx, g)
	put := roachpb.NewPut(roachpb.Key("a"), roachpb.MakeValueFromString("value"))
	// Fatal error on descriptor lookup, propagated to reply.
	if _, pErr := client.SendWrapped(ds, nil, put); pErr.String() != "fatal boom" {
		t.Errorf("unexpected error: %s", pErr)
	}
	// Retryable error on descriptor lookup, second attempt successful.
	if _, pErr := client.SendWrapped(ds, nil, put); pErr != nil {
		t.Errorf("unexpected error: %s", pErr)
	}
	if len(pErrs) != 0 {
		t.Fatalf("expected more descriptor lookups, leftover pErrs: %+v", pErrs)
	}
}

func TestEvictCacheOnError(t *testing.T) {
	defer leaktest.AfterTest(t)
	// if rpcError is true, the first attempt gets an RPC error, otherwise
	// the RPC call succeeds but there is an error in the RequestHeader.
	// Currently leader and cached range descriptor are treated equally.
	testCases := []struct{ rpcError, retryable, shouldClearLeader, shouldClearReplica bool }{
		{false, false, false, false}, // non-retryable replica error
		{false, true, false, false},  // retryable replica error
		{true, false, true, true},    // RPC error aka all nodes dead
		{true, true, false, false},   // retryable RPC error
	}

	for i, tc := range testCases {
		g, s := makeTestGossip(t)
		defer s()
		leader := roachpb.ReplicaDescriptor{
			NodeID:  99,
			StoreID: 999,
		}
		first := true

		var testFn rpcSendFn = func(_ SendOptions, _ string, _ []net.Addr, getArgs func(addr net.Addr) *roachpb.BatchRequest, _ *rpc.Context) (proto.Message, error) {
			if !first {
				return getArgs(nil).CreateReply(), nil
			}
			first = false
			if tc.rpcError {
				return nil, roachpb.NewSendError("boom", tc.retryable)
			}
			var err error
			if tc.retryable {
				err = &roachpb.RangeKeyMismatchError{}
			} else {
				err = errors.New("boom")
			}
			reply := &roachpb.BatchResponse{}
			reply.SetGoError(err)
			return reply, nil
		}

		ctx := &DistSenderContext{
			RPCSend: testFn,
			RangeDescriptorDB: mockRangeDescriptorDB(func(_ roachpb.RKey, _, _ bool) ([]roachpb.RangeDescriptor, *roachpb.Error) {
				return []roachpb.RangeDescriptor{testRangeDescriptor}, nil
			}),
		}
		ds := NewDistSender(ctx, g)
		ds.updateLeaderCache(1, leader)
		put := roachpb.NewPut(roachpb.Key("a"), roachpb.MakeValueFromString("value")).(*roachpb.PutRequest)

		if _, pErr := client.SendWrapped(ds, nil, put); pErr != nil && !testutils.IsPError(pErr, "boom") {
			t.Errorf("put encountered unexpected error: %s", pErr)
		}
		if cur := ds.leaderCache.Lookup(1); reflect.DeepEqual(cur, &roachpb.ReplicaDescriptor{}) && !tc.shouldClearLeader {
			t.Errorf("%d: leader cache eviction: shouldClearLeader=%t, but value is %v", i, tc.shouldClearLeader, cur)
		}
		_, cachedDesc := ds.rangeCache.getCachedRangeDescriptor(roachpb.RKey(put.Key), false /* !inclusive */)
		if cachedDesc == nil != tc.shouldClearReplica {
			t.Errorf("%d: unexpected second replica lookup behaviour: wanted=%t", i, tc.shouldClearReplica)
		}
	}
}

// TestRetryOnWrongReplicaError sets up a DistSender on a minimal gossip
// network and a mock of Send, and verifies that the DistSender correctly
// retries upon encountering a stale entry in its range descriptor cache.
func TestRetryOnWrongReplicaError(t *testing.T) {
	defer leaktest.AfterTest(t)
	g, s := makeTestGossip(t)
	defer s()
	// Updated below, after it has first been returned.
	badStartKey := roachpb.RKey("m")
	newRangeDescriptor := testRangeDescriptor
	goodStartKey := newRangeDescriptor.StartKey
	newRangeDescriptor.StartKey = badStartKey
	descStale := true

	var testFn rpcSendFn = func(_ SendOptions, method string, addrs []net.Addr, getArgs func(addr net.Addr) *roachpb.BatchRequest, _ *rpc.Context) (proto.Message, error) {
		ba := getArgs(testAddress)
		rs := keys.Range(*ba)
		if _, ok := ba.GetArg(roachpb.RangeLookup); ok {
			if !descStale && bytes.HasPrefix(rs.Key, keys.Meta2Prefix) {
				t.Errorf("unexpected extra lookup for non-stale replica descriptor at %s",
					rs.Key)
			}

			br := &roachpb.BatchResponse{}
			r := &roachpb.RangeLookupResponse{}
			r.Ranges = append(r.Ranges, newRangeDescriptor)
			br.Add(r)
			// If we just returned the stale descriptor, set up returning the
			// good one next time.
			if bytes.HasPrefix(rs.Key, keys.Meta2Prefix) {
				if newRangeDescriptor.StartKey.Equal(badStartKey) {
					newRangeDescriptor.StartKey = goodStartKey
				} else {
					descStale = false
				}
			}
			return br, nil
		}
		// When the Scan first turns up, update the descriptor for future
		// range descriptor lookups.
		if !newRangeDescriptor.StartKey.Equal(goodStartKey) {
			return nil, &roachpb.RangeKeyMismatchError{RequestStartKey: rs.Key.AsRawKey(),
				RequestEndKey: rs.EndKey.AsRawKey()}
		}
		return ba.CreateReply(), nil
	}

	ctx := &DistSenderContext{
		RPCSend: testFn,
	}
	ds := NewDistSender(ctx, g)
	scan := roachpb.NewScan(roachpb.Key("a"), roachpb.Key("d"), 0)
	if _, err := client.SendWrapped(ds, nil, scan); err != nil {
		t.Errorf("scan encountered error: %s", err)
	}
}

func TestGetFirstRangeDescriptor(t *testing.T) {
	defer leaktest.AfterTest(t)
	n := simulation.NewNetwork(3)
	ds := NewDistSender(nil, n.Nodes[0].Gossip)
	if _, err := ds.FirstRange(); err == nil {
		t.Errorf("expected not to find first range descriptor")
	}
	expectedDesc := &roachpb.RangeDescriptor{}
	expectedDesc.StartKey = roachpb.RKey("a")
	expectedDesc.EndKey = roachpb.RKey("c")

	// Add first RangeDescriptor to a node different from the node for
	// this dist sender and ensure that this dist sender has the
	// information within a given time.
	if err := n.Nodes[1].Gossip.AddInfoProto(gossip.KeyFirstRangeDescriptor, expectedDesc, time.Hour); err != nil {
		t.Fatal(err)
	}
	maxCycles := 10
	n.SimulateNetwork(func(cycle int, network *simulation.Network) bool {
		desc, err := ds.FirstRange()
		if err != nil {
			if cycle >= maxCycles {
				t.Errorf("could not get range descriptor after %d cycles", cycle)
				return false
			}
			return true
		}
		if !bytes.Equal(desc.StartKey, expectedDesc.StartKey) ||
			!bytes.Equal(desc.EndKey, expectedDesc.EndKey) {
			t.Errorf("expected first range descriptor %v, instead was %v",
				expectedDesc, desc)
		}
		return false
	})
	n.Stop()
}

// TestSendRPCRetry verifies that sendRPC failed on first address but succeed on
// second address, the second reply should be successfully returned back.
func TestSendRPCRetry(t *testing.T) {
	defer leaktest.AfterTest(t)
	g, s := makeTestGossip(t)
	defer s()
	g.SetNodeID(1)
	if err := g.SetNodeDescriptor(&roachpb.NodeDescriptor{NodeID: 1}); err != nil {
		t.Fatal(err)
	}
	// Fill RangeDescriptor with 2 replicas
	var descriptor = roachpb.RangeDescriptor{
		RangeID:  1,
		StartKey: roachpb.RKey("a"),
		EndKey:   roachpb.RKey("z"),
	}
	for i := 1; i <= 2; i++ {
		addr := util.MakeUnresolvedAddr("tcp", fmt.Sprintf("node%d", i))
		nd := &roachpb.NodeDescriptor{
			NodeID:  roachpb.NodeID(i),
			Address: util.MakeUnresolvedAddr(addr.Network(), addr.String()),
		}
		if err := g.AddInfoProto(gossip.MakeNodeIDKey(roachpb.NodeID(i)), nd, time.Hour); err != nil {
			t.Fatal(err)
		}

		descriptor.Replicas = append(descriptor.Replicas, roachpb.ReplicaDescriptor{
			NodeID:  roachpb.NodeID(i),
			StoreID: roachpb.StoreID(i),
		})
	}
	// Define our rpcSend stub which returns success on the second address.
	var testFn rpcSendFn = func(_ SendOptions, method string, addrs []net.Addr, getArgs func(addr net.Addr) *roachpb.BatchRequest, _ *rpc.Context) (proto.Message, error) {
		if method == "Node.Batch" {
			batchReply := &roachpb.BatchResponse{}
			reply := &roachpb.ScanResponse{}
			batchReply.Add(reply)
			reply.Rows = append([]roachpb.KeyValue{}, roachpb.KeyValue{Key: roachpb.Key("b"), Value: roachpb.Value{}})
			return batchReply, nil
		}
		return nil, util.Errorf("unexpected method %v", method)
	}
	ctx := &DistSenderContext{
		RPCSend: testFn,
		RangeDescriptorDB: mockRangeDescriptorDB(func(_ roachpb.RKey, _, _ bool) ([]roachpb.RangeDescriptor, *roachpb.Error) {
			return []roachpb.RangeDescriptor{descriptor}, nil
		}),
	}
	ds := NewDistSender(ctx, g)
	scan := roachpb.NewScan(roachpb.Key("a"), roachpb.Key("d"), 1)
	sr, err := client.SendWrapped(ds, nil, scan)
	if err != nil {
		t.Fatal(err)
	}
	if l := len(sr.(*roachpb.ScanResponse).Rows); l != 1 {
		t.Fatalf("expected 1 row; got %d", l)
	}
}

// TestGetNodeDescriptor checks that the Node descriptor automatically gets
// looked up from Gossip.
func TestGetNodeDescriptor(t *testing.T) {
	defer leaktest.AfterTest(t)
	g, s := makeTestGossip(t)
	defer s()
	ds := NewDistSender(&DistSenderContext{}, g)
	g.SetNodeID(5)
	if err := g.SetNodeDescriptor(&roachpb.NodeDescriptor{NodeID: 5}); err != nil {
		t.Fatal(err)
	}
	util.SucceedsWithin(t, time.Second, func() error {
		desc := ds.getNodeDescriptor()
		if desc != nil && desc.NodeID == 5 {
			return nil
		}
		return util.Errorf("wanted NodeID 5, got %v", desc)
	})
}

// TestMultiRangeMergeStaleDescriptor simulates the situation in which the
// DistSender executes a multi-range scan which encounters the stale descriptor
// of a range which has since incorporated its right neighbor by means of a
// merge. It is verified that the DistSender scans the correct keyrange exactly
// once.
func TestMultiRangeMergeStaleDescriptor(t *testing.T) {
	defer leaktest.AfterTest(t)
	g, s := makeTestGossip(t)
	defer s()
	// Assume we have two ranges, [a-b) and [b-KeyMax).
	merged := false
	// The stale first range descriptor which is unaware of the merge.
	var FirstRange = roachpb.RangeDescriptor{
		RangeID:  1,
		StartKey: roachpb.RKey("a"),
		EndKey:   roachpb.RKey("b"),
		Replicas: []roachpb.ReplicaDescriptor{
			{
				NodeID:  1,
				StoreID: 1,
			},
		},
	}
	// The merged descriptor, which will be looked up after having processed
	// the stale range [a,b).
	var mergedRange = roachpb.RangeDescriptor{
		RangeID:  1,
		StartKey: roachpb.RKey("a"),
		EndKey:   roachpb.RKeyMax,
		Replicas: []roachpb.ReplicaDescriptor{
			{
				NodeID:  1,
				StoreID: 1,
			},
		},
	}
	// Assume we have two key-value pairs, a=1 and c=2.
	existingKVs := []roachpb.KeyValue{
		{Key: roachpb.Key("a"), Value: roachpb.MakeValueFromString("1")},
		{Key: roachpb.Key("c"), Value: roachpb.MakeValueFromString("2")},
	}
	var testFn rpcSendFn = func(_ SendOptions, method string, addrs []net.Addr, getArgs func(addr net.Addr) *roachpb.BatchRequest, _ *rpc.Context) (proto.Message, error) {
		if method != "Node.Batch" {
			t.Fatalf("unexpected method:%s", method)
		}
		ba := getArgs(testAddress)
		rs := keys.Range(*ba)
		batchReply := &roachpb.BatchResponse{}
		reply := &roachpb.ScanResponse{}
		batchReply.Add(reply)
		results := []roachpb.KeyValue{}
		for _, curKV := range existingKVs {
			if rs.Key.Less(keys.Addr(curKV.Key).Next()) && keys.Addr(curKV.Key).Less(rs.EndKey) {
				results = append(results, curKV)
			}
		}
		reply.Rows = results
		return batchReply, nil
	}
	ctx := &DistSenderContext{
		RPCSend: testFn,
		RangeDescriptorDB: mockRangeDescriptorDB(func(key roachpb.RKey, _, _ bool) ([]roachpb.RangeDescriptor, *roachpb.Error) {
			if !merged {
				// Assume a range merge operation happened.
				merged = true
				return []roachpb.RangeDescriptor{FirstRange}, nil
			}
			return []roachpb.RangeDescriptor{mergedRange}, nil
		}),
	}
	ds := NewDistSender(ctx, g)
	scan := roachpb.NewScan(roachpb.Key("a"), roachpb.Key("d"), 10).(*roachpb.ScanRequest)
	// Set the Txn info to avoid an OpRequiresTxnError.
	reply, err := client.SendWrappedWith(ds, nil, roachpb.Header{
		Txn: &roachpb.Transaction{},
	}, scan)
	if err != nil {
		t.Fatalf("scan encountered error: %s", err)
	}
	sr := reply.(*roachpb.ScanResponse)
	if !reflect.DeepEqual(existingKVs, sr.Rows) {
		t.Fatalf("expect get %v, actual get %v", existingKVs, sr.Rows)
	}
}

// TestRangeLookupOptionOnReverseScan verifies that a lookup triggered by a
// ReverseScan request has the useReverseScan specified.
func TestRangeLookupOptionOnReverseScan(t *testing.T) {
	defer leaktest.AfterTest(t)
	g, s := makeTestGossip(t)
	defer s()

	var testFn rpcSendFn = func(_ SendOptions, method string, addrs []net.Addr, getArgs func(addr net.Addr) *roachpb.BatchRequest, _ *rpc.Context) (proto.Message, error) {
		return getArgs(nil).CreateReply(), nil
	}

	ctx := &DistSenderContext{
		RPCSend: testFn,
		RangeDescriptorDB: mockRangeDescriptorDB(func(k roachpb.RKey, considerIntents, useReverseScan bool) ([]roachpb.RangeDescriptor, *roachpb.Error) {
			if len(k) > 0 && !useReverseScan {
				t.Fatalf("expected UseReverseScan to be set")
			}
			return []roachpb.RangeDescriptor{testRangeDescriptor}, nil
		}),
	}
	ds := NewDistSender(ctx, g)
	rScan := &roachpb.ReverseScanRequest{
		Span: roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")},
	}
	if _, err := client.SendWrapped(ds, nil, rScan); err != nil {
		t.Fatal(err)
	}
}

// TestTruncateWithSpanAndDescriptor verifies that a batch request is truncated with a
// range span and the range of a descriptor found in cache.
func TestTruncateWithSpanAndDescriptor(t *testing.T) {
	defer leaktest.AfterTest(t)
	g, s := makeTestGossip(t)
	defer s()

	g.SetNodeID(1)
	if err := g.SetNodeDescriptor(&roachpb.NodeDescriptor{NodeID: 1}); err != nil {
		t.Fatal(err)
	}
	nd := &roachpb.NodeDescriptor{
		NodeID:  roachpb.NodeID(1),
		Address: util.MakeUnresolvedAddr(testAddress.Network(), testAddress.String()),
	}
	if err := g.AddInfoProto(gossip.MakeNodeIDKey(roachpb.NodeID(1)), nd, time.Hour); err != nil {
		t.Fatal(err)
	}

	// Fill mockRangeDescriptorDB with two descriptors. When a
	// range descriptor is looked up by key "b", return the second
	// descriptor whose range is ["a", "c") and partially overlaps
	// with the first descriptor's range.
	var descriptor1 = roachpb.RangeDescriptor{
		RangeID:  1,
		StartKey: roachpb.RKeyMin,
		EndKey:   roachpb.RKey("b"),
		Replicas: []roachpb.ReplicaDescriptor{
			{
				NodeID:  1,
				StoreID: 1,
			},
		},
	}
	var descriptor2 = roachpb.RangeDescriptor{
		RangeID:  2,
		StartKey: roachpb.RKey("a"),
		EndKey:   roachpb.RKey("c"),
		Replicas: []roachpb.ReplicaDescriptor{
			{
				NodeID:  1,
				StoreID: 1,
			},
		},
	}
	descDB := mockRangeDescriptorDB(func(key roachpb.RKey, _, _ bool) ([]roachpb.RangeDescriptor, *roachpb.Error) {
		desc := descriptor1
		if key.Equal(roachpb.RKey("b")) {
			desc = descriptor2
		}
		return []roachpb.RangeDescriptor{desc}, nil
	})

	// Define our rpcSend stub which checks the span of the batch
	// requests. The first request should be the point request on
	// "a". The second request should be on "b".
	first := true
	var testFn rpcSendFn = func(_ SendOptions, method string, addrs []net.Addr, getArgs func(addr net.Addr) *roachpb.BatchRequest, _ *rpc.Context) (proto.Message, error) {
		if method != "Node.Batch" {
			return nil, util.Errorf("unexpected method %v", method)
		}

		ba := getArgs(testAddress)
		rs := keys.Range(*ba)
		if first {
			if !(rs.Key.Equal(roachpb.RKey("a")) && rs.EndKey.Equal(roachpb.RKey("a").Next())) {
				t.Errorf("Unexpected span [%s,%s)", rs.Key, rs.EndKey)
			}
			first = false
		} else {
			if !(rs.Key.Equal(roachpb.RKey("b")) && rs.EndKey.Equal(roachpb.RKey("b").Next())) {
				t.Errorf("Unexpected span [%s,%s)", rs.Key, rs.EndKey)
			}
		}

		batchReply := &roachpb.BatchResponse{}
		reply := &roachpb.PutResponse{}
		batchReply.Add(reply)
		return batchReply, nil
	}

	ctx := &DistSenderContext{
		RPCSend:           testFn,
		RangeDescriptorDB: descDB,
	}
	ds := NewDistSender(ctx, g)

	// Send a batch request contains two puts. In the first
	// attempt, the range of the descriptor found in the cache is
	// ["a", "b"). The request is truncated to contain only the put
	// on "a".
	//
	// In the second attempt, The range of the descriptor found in
	// the cache is ["a", c"), but the put on "a" will not be
	// resent. The request is truncated to contain only the put on "b".
	ba := roachpb.BatchRequest{}
	ba.Txn = &roachpb.Transaction{Name: "test"}
	val := roachpb.MakeValueFromString("val")
	ba.Add(roachpb.NewPut(keys.RangeTreeNodeKey(roachpb.RKey("a")), val).(*roachpb.PutRequest))
	ba.Add(roachpb.NewPut(keys.RangeTreeNodeKey(roachpb.RKey("b")), val).(*roachpb.PutRequest))

	if _, pErr := ds.Send(context.Background(), ba); pErr != nil {
		t.Fatal(pErr)
	}
}

// TestSequenceUpdateOnMultiRangeQueryLoop reproduces #3206 and
// verifies that the sequence is updated in the DistSender
// multi-range-query loop.
//
// More specifically, the issue was that DistSender might send
// multiple batch requests to the same replica when it finds a
// post-split range descriptor in the cache while the split has not
// yet been fully completed. By giving a higher sequence to the second
// request, we can avoid an infinite txn restart error (otherwise
// caused by hitting the sequence cache).
func TestSequenceUpdateOnMultiRangeQueryLoop(t *testing.T) {
	defer leaktest.AfterTest(t)
	g, s := makeTestGossip(t)
	defer s()

	if err := g.SetNodeDescriptor(&roachpb.NodeDescriptor{NodeID: 1}); err != nil {
		t.Fatal(err)
	}
	nd := &roachpb.NodeDescriptor{
		NodeID:  roachpb.NodeID(1),
		Address: util.MakeUnresolvedAddr(testAddress.Network(), testAddress.String()),
	}
	if err := g.AddInfoProto(gossip.MakeNodeIDKey(roachpb.NodeID(1)), nd, time.Hour); err != nil {
		t.Fatal(err)

	}

	// Fill mockRangeDescriptorDB with two descriptors.
	var descriptor1 = roachpb.RangeDescriptor{
		RangeID:  1,
		StartKey: roachpb.RKeyMin,
		EndKey:   roachpb.RKey("b"),
		Replicas: []roachpb.ReplicaDescriptor{
			{
				NodeID:  1,
				StoreID: 1,
			},
		},
	}
	var descriptor2 = roachpb.RangeDescriptor{
		RangeID:  2,
		StartKey: roachpb.RKey("b"),
		EndKey:   roachpb.RKey("c"),
		Replicas: []roachpb.ReplicaDescriptor{
			{
				NodeID:  1,
				StoreID: 1,
			},
		},
	}
	descDB := mockRangeDescriptorDB(func(key roachpb.RKey, _, _ bool) ([]roachpb.RangeDescriptor, *roachpb.Error) {
		desc := descriptor1
		if key.Equal(roachpb.RKey("b")) {
			desc = descriptor2
		}
		return []roachpb.RangeDescriptor{desc}, nil
	})

	// Define our rpcSend stub which checks the span of the batch
	// requests. The first request should be the point request on
	// "a". The second request should be on "b". The sequence of the
	// second request will be incremented by one from that of the
	// first request.
	first := true
	var firstSequence uint32
	var testFn rpcSendFn = func(_ SendOptions, method string, addrs []net.Addr, getArgs func(addr net.Addr) *roachpb.BatchRequest, _ *rpc.Context) (proto.Message, error) {
		if method != "Node.Batch" {
			return nil, util.Errorf("unexpected method %v", method)
		}

		ba := getArgs(testAddress)
		rs := keys.Range(*ba)
		if first {
			if !(rs.Key.Equal(roachpb.RKey("a")) && rs.EndKey.Equal(roachpb.RKey("a").Next())) {
				t.Errorf("unexpected span [%s,%s)", rs.Key, rs.EndKey)
			}
			first = false
			firstSequence = ba.Txn.Sequence
		} else {
			if !(rs.Key.Equal(roachpb.RKey("b")) && rs.EndKey.Equal(roachpb.RKey("b").Next())) {
				t.Errorf("unexpected span [%s,%s)", rs.Key, rs.EndKey)
			}
			if ba.Txn.Sequence != firstSequence+1 {
				t.Errorf("unexpected sequence; expected %d, but got %d", firstSequence+1, ba.Txn.Sequence)
			}
		}
		return ba.CreateReply(), nil
	}

	ctx := &DistSenderContext{
		RPCSend:           testFn,
		RangeDescriptorDB: descDB,
	}
	ds := NewDistSender(ctx, g)

	// Send a batch request containing two puts.
	var ba roachpb.BatchRequest
	ba.Txn = &roachpb.Transaction{Name: "test"}
	val := roachpb.MakeValueFromString("val")
	ba.Add(roachpb.NewPut(roachpb.Key("a"), val).(*roachpb.PutRequest))
	ba.Add(roachpb.NewPut(roachpb.Key("b"), val).(*roachpb.PutRequest))

	if _, pErr := ds.Send(context.Background(), ba); pErr != nil {
		t.Fatal(pErr)
	}
}

// TestMultiRangeSplitEndTransaction verifies that when a chunk of batch looks
// like it's going to be dispatched to more than one range, it will be split
// up if it it contains EndTransaction.
func TestMultiRangeSplitEndTransaction(t *testing.T) {
	defer leaktest.AfterTest(t)
	g, s := makeTestGossip(t)
	defer s()

	testCases := []struct {
		put1, put2, et roachpb.Key
		exp            [][]roachpb.Method
	}{
		{
			// Everything hits the first range, so we get a 1PC txn.
			roachpb.Key("a1"), roachpb.Key("a2"), roachpb.Key("a3"),
			[][]roachpb.Method{{roachpb.Put, roachpb.Put, roachpb.EndTransaction}},
		},
		{
			// Only EndTransaction hits the second range.
			roachpb.Key("a1"), roachpb.Key("a2"), roachpb.Key("b"),
			[][]roachpb.Method{{roachpb.Put, roachpb.Put}, {roachpb.EndTransaction}},
		},
		{
			// One write hits the second range, so EndTransaction has to be split off.
			// In this case, going in the usual order without splitting off
			// would actually be fine, but it doesn't seem worth optimizing at
			// this point.
			roachpb.Key("a1"), roachpb.Key("b1"), roachpb.Key("a1"),
			[][]roachpb.Method{{roachpb.Put, roachpb.Noop}, {roachpb.Noop, roachpb.Put}, {roachpb.EndTransaction}},
		},
		{
			// Both writes go to the second range, but not EndTransaction.
			roachpb.Key("b1"), roachpb.Key("b2"), roachpb.Key("a1"),
			[][]roachpb.Method{{roachpb.Put, roachpb.Put}, {roachpb.EndTransaction}},
		},
	}

	if err := g.SetNodeDescriptor(&roachpb.NodeDescriptor{NodeID: 1}); err != nil {
		t.Fatal(err)
	}
	nd := &roachpb.NodeDescriptor{
		NodeID:  roachpb.NodeID(1),
		Address: util.MakeUnresolvedAddr(testAddress.Network(), testAddress.String()),
	}
	if err := g.AddInfoProto(gossip.MakeNodeIDKey(roachpb.NodeID(1)), nd, time.Hour); err != nil {
		t.Fatal(err)

	}

	// Fill mockRangeDescriptorDB with two descriptors.
	var descriptor1 = roachpb.RangeDescriptor{
		RangeID:  1,
		StartKey: roachpb.RKeyMin,
		EndKey:   roachpb.RKey("b"),
		Replicas: []roachpb.ReplicaDescriptor{
			{
				NodeID:  1,
				StoreID: 1,
			},
		},
	}
	var descriptor2 = roachpb.RangeDescriptor{
		RangeID:  2,
		StartKey: roachpb.RKey("b"),
		EndKey:   roachpb.RKeyMax,
		Replicas: []roachpb.ReplicaDescriptor{
			{
				NodeID:  1,
				StoreID: 1,
			},
		},
	}
	descDB := mockRangeDescriptorDB(func(key roachpb.RKey, _, _ bool) ([]roachpb.RangeDescriptor, *roachpb.Error) {
		desc := descriptor1
		if !key.Less(roachpb.RKey("b")) {
			desc = descriptor2
		}
		return []roachpb.RangeDescriptor{desc}, nil
	})

	for _, test := range testCases {
		var act [][]roachpb.Method
		var testFn rpcSendFn = func(_ SendOptions, method string, addrs []net.Addr, ga func(addr net.Addr) *roachpb.BatchRequest, _ *rpc.Context) (proto.Message, error) {
			ba := ga(testAddress)
			var cur []roachpb.Method
			for _, union := range ba.Requests {
				cur = append(cur, union.GetInner().Method())
			}
			act = append(act, cur)
			return ba.CreateReply(), nil
		}

		ctx := &DistSenderContext{
			RPCSend:           testFn,
			RangeDescriptorDB: descDB,
		}
		ds := NewDistSender(ctx, g)

		// Send a batch request containing two puts.
		var ba roachpb.BatchRequest
		ba.Txn = &roachpb.Transaction{Name: "test"}
		val := roachpb.MakeValueFromString("val")
		ba.Add(roachpb.NewPut(roachpb.Key(test.put1), val).(*roachpb.PutRequest))
		ba.Add(roachpb.NewPut(roachpb.Key(test.put2), val).(*roachpb.PutRequest))
		ba.Add(&roachpb.EndTransactionRequest{Span: roachpb.Span{Key: test.et}})

		if _, pErr := ds.Send(context.Background(), ba); pErr != nil {
			t.Fatal(pErr)
		}

		if !reflect.DeepEqual(test.exp, act) {
			t.Fatalf("expected %v, got %v", test.exp, act)
		}
	}
}
