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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
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

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/gossip/simulation"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/testutils"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/leaktest"
	gogoproto "github.com/gogo/protobuf/proto"
)

var testRangeDescriptor = proto.RangeDescriptor{
	RangeID:  1,
	StartKey: proto.Key("a"),
	EndKey:   proto.Key("z"),
	Replicas: []proto.Replica{
		{
			NodeID:  1,
			StoreID: 1,
		},
	},
}

var testAddress = util.MakeUnresolvedAddr("tcp", "node1:8080")

func makeTestGossip(t *testing.T) (*gossip.Gossip, func()) {
	n := simulation.NewNetwork(1, "tcp", gossip.TestInterval)
	g := n.Nodes[0].Gossip

	if err := g.AddInfo(gossip.KeySentinel, nil, time.Hour); err != nil {
		t.Fatal(err)
	}
	if err := g.AddInfoProto(gossip.KeyFirstRangeDescriptor, &testRangeDescriptor, time.Hour); err != nil {
		t.Fatal(err)
	}
	nodeIDKey := gossip.MakeNodeIDKey(1)
	if err := g.AddInfoProto(nodeIDKey, &proto.NodeDescriptor{
		NodeID:  1,
		Address: util.MakeUnresolvedAddr(testAddress.Network(), testAddress.String()),
		Attrs:   proto.Attributes{Attrs: []string{"attr1", "attr2"}},
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
		localNodeDesc proto.NodeDescriptor
	}{
		{
			// No attribute prefix
			slice: replicaSlice{
				replicaInfo{
					Replica:  proto.Replica{NodeID: 2, StoreID: 2},
					NodeDesc: &proto.NodeDescriptor{NodeID: 2},
				},
				replicaInfo{
					Replica:  proto.Replica{NodeID: 3, StoreID: 3},
					NodeDesc: &proto.NodeDescriptor{NodeID: 3},
				},
				replicaInfo{
					Replica:  proto.Replica{NodeID: 1, StoreID: 1},
					NodeDesc: &proto.NodeDescriptor{NodeID: 1},
				},
			},
			localNodeDesc: proto.NodeDescriptor{NodeID: 1},
		},
		{
			// Sort replicas by attribute
			slice: replicaSlice{
				replicaInfo{
					Replica:  proto.Replica{NodeID: 2, StoreID: 2},
					NodeDesc: &proto.NodeDescriptor{NodeID: 2, Attrs: proto.Attributes{Attrs: []string{"ad"}}},
				},
				replicaInfo{
					Replica:  proto.Replica{NodeID: 3, StoreID: 3},
					NodeDesc: &proto.NodeDescriptor{NodeID: 3, Attrs: proto.Attributes{Attrs: []string{"ab", "c"}}},
				},
				replicaInfo{
					Replica:  proto.Replica{NodeID: 1, StoreID: 1},
					NodeDesc: &proto.NodeDescriptor{NodeID: 1, Attrs: proto.Attributes{Attrs: []string{"ab"}}},
				},
			},
			localNodeDesc: proto.NodeDescriptor{NodeID: 1, Attrs: proto.Attributes{Attrs: []string{"ab"}}},
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
	rangeID := proto.RangeID(99)

	nodeAttrs := map[int32][]string{
		1: {}, // The local node, set in each test case.
		2: {"us", "west", "gpu"},
		3: {"eu", "dublin", "pdu2", "gpu"},
		4: {"us", "east", "gpu"},
		5: {"us", "east", "gpu", "flaky"},
	}

	// Gets filled below to identify the replica by its address.
	addrToNode := make(map[string]int32)
	makeVerifier := func(expOrder rpc.OrderingPolicy,
		expAddrs []int32) func(rpc.Options, []net.Addr) error {
		return func(o rpc.Options, addrs []net.Addr) error {
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
		args       proto.Request
		attrs      []string
		order      rpc.OrderingPolicy
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
			args:       &proto.ScanRequest{},
			attrs:      []string{},
			order:      rpc.OrderRandom,
			expReplica: []int32{1, 2, 3, 4, 5},
		},
		// Inconsistent Scan with matching attributes.
		// Should move the two nodes matching the attributes to the front and
		// go stable.
		{
			args:  &proto.ScanRequest{},
			attrs: nodeAttrs[5],
			order: rpc.OrderStable,
			// Compare only the first two resulting addresses.
			expReplica: []int32{5, 4, 0, 0, 0},
		},

		// Scan without matching attributes that requires but does not find
		// a leader.
		{
			args:       &proto.ScanRequest{},
			attrs:      []string{},
			order:      rpc.OrderRandom,
			expReplica: []int32{1, 2, 3, 4, 5},
			consistent: true,
		},
		// Put without matching attributes that requires but does not find leader.
		// Should go random and not change anything.
		{
			args:       &proto.PutRequest{},
			attrs:      []string{"nomatch"},
			order:      rpc.OrderRandom,
			expReplica: []int32{1, 2, 3, 4, 5},
		},
		// Put with matching attributes but no leader.
		// Should move the two nodes matching the attributes to the front and
		// go stable.
		{
			args:  &proto.PutRequest{},
			attrs: append(nodeAttrs[5], "irrelevant"),
			// Compare only the first two resulting addresses.
			order:      rpc.OrderStable,
			expReplica: []int32{5, 4, 0, 0, 0},
		},
		// Put with matching attributes that finds the leader (node 3).
		// Should address the leader and the two nodes matching the attributes
		// (the last and second to last) in that order.
		{
			args:  &proto.PutRequest{},
			attrs: append(nodeAttrs[5], "irrelevant"),
			// Compare only the first resulting addresses as we have a leader
			// and that means we're only trying to send there.
			order:      rpc.OrderStable,
			expReplica: []int32{2, 5, 4, 0, 0},
			leader:     2,
		},
		// Inconsistent Get without matching attributes but leader (node 3). Should just
		// go random as the leader does not matter.
		{
			args:       &proto.GetRequest{},
			attrs:      []string{},
			order:      rpc.OrderRandom,
			expReplica: []int32{1, 2, 3, 4, 5},
			leader:     2,
		},
	}

	descriptor := proto.RangeDescriptor{
		StartKey: proto.KeyMin,
		EndKey:   proto.KeyMax,
		RangeID:  rangeID,
		Replicas: nil,
	}

	// Stub to be changed in each test case.
	var verifyCall func(rpc.Options, []net.Addr) error

	var testFn rpcSendFn = func(opts rpc.Options, method string,
		addrs []net.Addr, _ func(addr net.Addr) gogoproto.Message,
		getReply func() gogoproto.Message, _ *rpc.Context) ([]gogoproto.Message, error) {
		if err := verifyCall(opts, addrs); err != nil {
			return nil, err
		}
		return []gogoproto.Message{getReply()}, nil
	}

	ctx := &DistSenderContext{
		RPCSend: testFn,
		RangeDescriptorDB: mockRangeDescriptorDB(func(proto.Key, lookupOptions) ([]proto.RangeDescriptor, error) {
			return []proto.RangeDescriptor{descriptor}, nil
		}),
	}

	ds := NewDistSender(ctx, g)

	for n, tc := range testCases {
		verifyCall = makeVerifier(tc.order, tc.expReplica)
		descriptor.Replicas = nil // could do this once above, but more convenient here
		for i := int32(1); i <= 5; i++ {
			addr := util.MakeUnresolvedAddr("tcp", fmt.Sprintf("node%d", i))
			addrToNode[addr.String()] = i
			nd := &proto.NodeDescriptor{
				NodeID:  proto.NodeID(i),
				Address: util.MakeUnresolvedAddr(addr.Network(), addr.String()),
				Attrs: proto.Attributes{
					Attrs: nodeAttrs[i],
				},
			}
			if err := g.AddInfoProto(gossip.MakeNodeIDKey(proto.NodeID(i)), nd, time.Hour); err != nil {
				t.Fatal(err)
			}
			descriptor.Replicas = append(descriptor.Replicas, proto.Replica{
				NodeID:  proto.NodeID(i),
				StoreID: proto.StoreID(i),
			})
		}

		{
			// The local node needs to get its attributes during sendRPC.
			nd := &proto.NodeDescriptor{
				NodeID: 6,
				Attrs: proto.Attributes{
					Attrs: tc.attrs,
				},
			}
			if err := g.SetNodeDescriptor(nd); err != nil {
				t.Fatal(err)
			}
		}

		ds.leaderCache.Update(proto.RangeID(rangeID), proto.Replica{})
		if tc.leader > 0 {
			ds.leaderCache.Update(proto.RangeID(rangeID), descriptor.Replicas[tc.leader-1])
		}

		args := tc.args
		args.Header().RangeID = rangeID // Not used in this test, but why not.
		args.Header().Key = proto.Key("a")
		if proto.IsRange(args) {
			args.Header().EndKey = proto.Key("b")
		}
		if !tc.consistent {
			args.Header().ReadConsistency = proto.INCONSISTENT
		}
		// Kill the cached NodeDescriptor, enforcing a lookup from Gossip.
		ds.nodeDescriptor = nil
		if _, err := client.SendCall(ds, args); err != nil {
			t.Errorf("%d: %s", n, err)
		}
	}
}

type mockRangeDescriptorDB func(proto.Key, lookupOptions) ([]proto.RangeDescriptor, error)

func (mdb mockRangeDescriptorDB) rangeLookup(key proto.Key, options lookupOptions, _ *proto.RangeDescriptor) ([]proto.RangeDescriptor, error) {
	if bytes.HasPrefix(key, keys.Meta1Prefix) {
		return mdb(key[len(keys.Meta1Prefix):], options)
	}
	// First range.
	return mdb(nil, options)
}
func (mdb mockRangeDescriptorDB) firstRange() (*proto.RangeDescriptor, error) {
	rs, err := mdb.rangeLookup(nil, lookupOptions{}, nil)
	if err != nil || len(rs) == 0 {
		return nil, err
	}
	return &rs[0], nil
}

// TestRetryOnNotLeaderError verifies that the DistSender correctly updates the
// leader cache and retries when receiving a NotLeaderError.
func TestRetryOnNotLeaderError(t *testing.T) {
	defer leaktest.AfterTest(t)
	g, s := makeTestGossip(t)
	defer s()
	leader := proto.Replica{
		NodeID:  99,
		StoreID: 999,
	}
	first := true

	var testFn rpcSendFn = func(_ rpc.Options, method string, addrs []net.Addr, getArgs func(addr net.Addr) gogoproto.Message, getReply func() gogoproto.Message, _ *rpc.Context) ([]gogoproto.Message, error) {
		if first {
			reply := getReply()
			reply.(proto.Response).Header().SetGoError(
				&proto.NotLeaderError{Leader: &leader, Replica: &proto.Replica{}})
			first = false
			return []gogoproto.Message{reply}, nil
		}
		return []gogoproto.Message{getReply()}, nil
	}

	ctx := &DistSenderContext{
		RPCSend: testFn,
		RangeDescriptorDB: mockRangeDescriptorDB(func(_ proto.Key, _ lookupOptions) ([]proto.RangeDescriptor, error) {
			return []proto.RangeDescriptor{testRangeDescriptor}, nil
		}),
	}
	ds := NewDistSender(ctx, g)
	put := proto.NewPut(proto.Key("a"), proto.Value{Bytes: []byte("value")})
	if _, err := client.SendCall(ds, put); err != nil {
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

	var testFn rpcSendFn = func(_ rpc.Options, _ string, _ []net.Addr, _ func(addr net.Addr) gogoproto.Message, getReply func() gogoproto.Message, _ *rpc.Context) ([]gogoproto.Message, error) {
		return []gogoproto.Message{getReply()}, nil
	}

	errors := []error{
		errors.New("fatal boom"),
		&proto.RangeKeyMismatchError{}, // retryable
		nil,
	}

	ctx := &DistSenderContext{
		RPCSend: testFn,
		RangeDescriptorDB: mockRangeDescriptorDB(func(k proto.Key, _ lookupOptions) ([]proto.RangeDescriptor, error) {
			// Return next error and truncate the prefix of the errors array.
			var err error
			if k != nil {
				err = errors[0]
				errors = errors[1:]
			}
			return []proto.RangeDescriptor{testRangeDescriptor}, err
		}),
	}
	ds := NewDistSender(ctx, g)
	put := proto.NewPut(proto.Key("a"), proto.Value{Bytes: []byte("value")})
	// Fatal error on descriptor lookup, propagated to reply.
	if _, err := client.SendCall(ds, put); err.Error() != "fatal boom" {
		t.Errorf("unexpected error: %s", err)
	}
	// Retryable error on descriptor lookup, second attempt successful.
	if _, err := client.SendCall(ds, put); err != nil {
		t.Errorf("unexpected error: %s", err)
	}
	if len(errors) != 0 {
		t.Fatalf("expected more descriptor lookups, leftover errors: %+v", errors)
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
		leader := proto.Replica{
			NodeID:  99,
			StoreID: 999,
		}
		first := true

		var testFn rpcSendFn = func(_ rpc.Options, _ string, _ []net.Addr, _ func(addr net.Addr) gogoproto.Message, getReply func() gogoproto.Message, _ *rpc.Context) ([]gogoproto.Message, error) {
			if !first {
				return []gogoproto.Message{getReply()}, nil
			}
			first = false
			if tc.rpcError {
				return nil, rpc.NewSendError("boom", tc.retryable)
			}
			var err error
			if tc.retryable {
				err = &proto.RangeKeyMismatchError{}
			} else {
				err = errors.New("boom")
			}
			reply := getReply()
			reply.(proto.Response).Header().SetGoError(err)
			return []gogoproto.Message{reply}, nil
		}

		ctx := &DistSenderContext{
			RPCSend: testFn,
			RangeDescriptorDB: mockRangeDescriptorDB(func(_ proto.Key, _ lookupOptions) ([]proto.RangeDescriptor, error) {
				return []proto.RangeDescriptor{testRangeDescriptor}, nil
			}),
		}
		ds := NewDistSender(ctx, g)
		ds.updateLeaderCache(1, leader)

		put := proto.NewPut(proto.Key("a"), proto.Value{Bytes: []byte("value")}).(*proto.PutRequest)

		if _, err := client.SendCall(ds, put); err != nil && !testutils.IsError(err, "boom") {
			t.Errorf("put encountered unexpected error: %s", err)
		}
		if cur := ds.leaderCache.Lookup(1); reflect.DeepEqual(cur, &proto.Replica{}) && !tc.shouldClearLeader {
			t.Errorf("%d: leader cache eviction: shouldClearLeader=%t, but value is %v", i, tc.shouldClearLeader, cur)
		}
		_, cachedDesc := ds.rangeCache.getCachedRangeDescriptor(put.Key, false /* !inclusive */)
		if cachedDesc == nil != tc.shouldClearReplica {
			t.Errorf("%d: unexpected second replica lookup behaviour: wanted=%t", i, tc.shouldClearReplica)
		}
	}
}

// TestRetryOnWrongReplicaError sets up a DistSender on a minimal gossip
// network and a mock of rpc.Send, and verifies that the DistSender correctly
// retries upon encountering a stale entry in its range descriptor cache.
func TestRetryOnWrongReplicaError(t *testing.T) {
	defer leaktest.AfterTest(t)
	g, s := makeTestGossip(t)
	defer s()
	// Updated below, after it has first been returned.
	badStartKey := proto.Key("m")
	newRangeDescriptor := testRangeDescriptor
	goodStartKey := newRangeDescriptor.StartKey
	newRangeDescriptor.StartKey = badStartKey
	descStale := true

	var testFn rpcSendFn = func(_ rpc.Options, method string, addrs []net.Addr, getArgs func(addr net.Addr) gogoproto.Message, getReply func() gogoproto.Message, _ *rpc.Context) ([]gogoproto.Message, error) {
		ba := getArgs(testAddress).(*proto.BatchRequest)
		if _, ok := ba.GetArg(proto.RangeLookup); ok {
			if !descStale && bytes.HasPrefix(ba.Key, keys.Meta2Prefix) {
				t.Errorf("unexpected extra lookup for non-stale replica descriptor at %s",
					ba.Key)
			}

			br := getReply().(*proto.BatchResponse)
			r := &proto.RangeLookupResponse{}
			r.Ranges = append(r.Ranges, newRangeDescriptor)
			br.Add(r)
			// If we just returned the stale descriptor, set up returning the
			// good one next time.
			if bytes.HasPrefix(ba.Key, keys.Meta2Prefix) {
				if newRangeDescriptor.StartKey.Equal(badStartKey) {
					newRangeDescriptor.StartKey = goodStartKey
				} else {
					descStale = false
				}
			}
			return []gogoproto.Message{br}, nil
		}
		// When the Scan first turns up, update the descriptor for future
		// range descriptor lookups.
		if !newRangeDescriptor.StartKey.Equal(goodStartKey) {
			return nil, &proto.RangeKeyMismatchError{RequestStartKey: ba.Key,
				RequestEndKey: ba.EndKey}
		}
		return []gogoproto.Message{ba.CreateReply().(*proto.BatchResponse)}, nil
	}

	ctx := &DistSenderContext{
		RPCSend: testFn,
	}
	ds := NewDistSender(ctx, g)
	scan := proto.NewScan(proto.Key("a"), proto.Key("d"), 0)
	if _, err := client.SendCall(ds, scan); err != nil {
		t.Errorf("scan encountered error: %s", err)
	}
}

func TestGetFirstRangeDescriptor(t *testing.T) {
	defer leaktest.AfterTest(t)
	n := simulation.NewNetwork(3, "tcp", gossip.TestInterval)
	ds := NewDistSender(nil, n.Nodes[0].Gossip)
	if _, err := ds.firstRange(); err == nil {
		t.Errorf("expected not to find first range descriptor")
	}
	expectedDesc := &proto.RangeDescriptor{}
	expectedDesc.StartKey = proto.Key("a")
	expectedDesc.EndKey = proto.Key("c")

	// Add first RangeDescriptor to a node different from the node for
	// this dist sender and ensure that this dist sender has the
	// information within a given time.
	if err := n.Nodes[1].Gossip.AddInfoProto(gossip.KeyFirstRangeDescriptor, expectedDesc, time.Hour); err != nil {
		t.Fatal(err)
	}
	maxCycles := 10
	n.SimulateNetwork(func(cycle int, network *simulation.Network) bool {
		desc, err := ds.firstRange()
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
	if err := g.SetNodeDescriptor(&proto.NodeDescriptor{NodeID: 1}); err != nil {
		t.Fatal(err)
	}
	// Fill RangeDescriptor with 2 replicas
	var descriptor = proto.RangeDescriptor{
		RangeID:  1,
		StartKey: proto.Key("a"),
		EndKey:   proto.Key("z"),
	}
	for i := 1; i <= 2; i++ {
		addr := util.MakeUnresolvedAddr("tcp", fmt.Sprintf("node%d", i))
		nd := &proto.NodeDescriptor{
			NodeID:  proto.NodeID(i),
			Address: util.MakeUnresolvedAddr(addr.Network(), addr.String()),
		}
		if err := g.AddInfoProto(gossip.MakeNodeIDKey(proto.NodeID(i)), nd, time.Hour); err != nil {
			t.Fatal(err)
		}

		descriptor.Replicas = append(descriptor.Replicas, proto.Replica{
			NodeID:  proto.NodeID(i),
			StoreID: proto.StoreID(i),
		})
	}
	// Define our rpcSend stub which returns success on the second address.
	var testFn rpcSendFn = func(_ rpc.Options, method string, addrs []net.Addr, getArgs func(addr net.Addr) gogoproto.Message, getReply func() gogoproto.Message, _ *rpc.Context) ([]gogoproto.Message, error) {
		if method == "Node.Batch" {
			// reply from first address failed
			_ = getReply()
			// reply from second address succeed
			batchReply := getReply().(*proto.BatchResponse)
			reply := &proto.ScanResponse{}
			batchReply.Add(reply)
			reply.Rows = append([]proto.KeyValue{}, proto.KeyValue{Key: proto.Key("b"), Value: proto.Value{}})
			return []gogoproto.Message{batchReply}, nil
		}
		return nil, util.Errorf("unexpected method %v", method)
	}
	ctx := &DistSenderContext{
		RPCSend: testFn,
		RangeDescriptorDB: mockRangeDescriptorDB(func(_ proto.Key, _ lookupOptions) ([]proto.RangeDescriptor, error) {
			return []proto.RangeDescriptor{descriptor}, nil
		}),
	}
	ds := NewDistSender(ctx, g)
	scan := proto.NewScan(proto.Key("a"), proto.Key("d"), 1)
	sr, err := client.SendCall(ds, scan)
	if err != nil {
		t.Fatal(err)
	}
	if l := len(sr.(*proto.ScanResponse).Rows); l != 1 {
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
	if err := g.SetNodeDescriptor(&proto.NodeDescriptor{NodeID: 5}); err != nil {
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
	var firstRange = proto.RangeDescriptor{
		RangeID:  1,
		StartKey: proto.Key("a"),
		EndKey:   proto.Key("b"),
		Replicas: []proto.Replica{
			{
				NodeID:  1,
				StoreID: 1,
			},
		},
	}
	// The merged descriptor, which will be looked up after having processed
	// the stale range [a,b).
	var mergedRange = proto.RangeDescriptor{
		RangeID:  1,
		StartKey: proto.Key("a"),
		EndKey:   proto.KeyMax,
		Replicas: []proto.Replica{
			{
				NodeID:  1,
				StoreID: 1,
			},
		},
	}
	// Assume we have two key-value pairs, a=1 and c=2.
	existingKVs := []proto.KeyValue{
		{Key: proto.Key("a"), Value: proto.Value{Bytes: []byte("1")}},
		{Key: proto.Key("c"), Value: proto.Value{Bytes: []byte("2")}},
	}
	var testFn rpcSendFn = func(_ rpc.Options, method string, addrs []net.Addr, getArgs func(addr net.Addr) gogoproto.Message, getReply func() gogoproto.Message, _ *rpc.Context) ([]gogoproto.Message, error) {
		if method != "Node.Batch" {
			t.Fatalf("unexpected method:%s", method)
		}
		header := getArgs(testAddress).(proto.Request).Header()
		batchReply := getReply().(*proto.BatchResponse)
		reply := &proto.ScanResponse{}
		batchReply.Add(reply)
		results := []proto.KeyValue{}
		for _, curKV := range existingKVs {
			if header.Key.Less(curKV.Key.Next()) && curKV.Key.Less(header.EndKey) {
				results = append(results, curKV)
			}
		}
		reply.Rows = results
		return []gogoproto.Message{batchReply}, nil
	}
	ctx := &DistSenderContext{
		RPCSend: testFn,
		RangeDescriptorDB: mockRangeDescriptorDB(func(key proto.Key, _ lookupOptions) ([]proto.RangeDescriptor, error) {
			if !merged {
				// Assume a range merge operation happened.
				merged = true
				return []proto.RangeDescriptor{firstRange}, nil
			}
			return []proto.RangeDescriptor{mergedRange}, nil
		}),
	}
	ds := NewDistSender(ctx, g)
	scan := proto.NewScan(proto.Key("a"), proto.Key("d"), 10).(*proto.ScanRequest)
	// Set the Txn info to avoid an OpRequiresTxnError.
	scan.Txn = &proto.Transaction{}
	reply, err := client.SendCall(ds, scan)
	if err != nil {
		t.Fatalf("scan encountered error: %s", err)
	}
	sr := reply.(*proto.ScanResponse)
	if !reflect.DeepEqual(existingKVs, sr.Rows) {
		t.Fatalf("expect get %v, actual get %v", existingKVs, sr.Rows)
	}
}

// TestRangeLookupOptionOnReverseScan verifies that a lookup triggered by a
// ReverseScan request has the `useReverseScan` option specified.
func TestRangeLookupOptionOnReverseScan(t *testing.T) {
	defer leaktest.AfterTest(t)
	g, s := makeTestGossip(t)
	defer s()

	var testFn rpcSendFn = func(_ rpc.Options, method string, addrs []net.Addr, getArgs func(addr net.Addr) gogoproto.Message, getReply func() gogoproto.Message, _ *rpc.Context) ([]gogoproto.Message, error) {
		return []gogoproto.Message{getReply()}, nil
	}

	ctx := &DistSenderContext{
		RPCSend: testFn,
		RangeDescriptorDB: mockRangeDescriptorDB(func(k proto.Key, opts lookupOptions) ([]proto.RangeDescriptor, error) {
			if len(k) > 0 && !opts.useReverseScan {
				t.Fatalf("expected useReverseScan to be set")
			}
			return []proto.RangeDescriptor{testRangeDescriptor}, nil
		}),
	}
	ds := NewDistSender(ctx, g)
	rScan := &proto.ReverseScanRequest{
		RequestHeader: proto.RequestHeader{Key: proto.Key("a"), EndKey: proto.Key("b")},
	}
	if _, err := client.SendCall(ds, rScan); err != nil {
		t.Fatal(err)
	}
}
