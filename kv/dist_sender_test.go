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
	"fmt"
	"net"
	"reflect"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/config"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/gossip/simulation"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/rpc"
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
	permConfig := &config.PermConfig{
		Read:  []string{""},
		Write: []string{""},
	}

	configMap, err := config.NewPrefixConfigMap([]*config.PrefixConfig{
		{proto.KeyMin, nil, permConfig},
	})
	if err != nil {
		t.Fatalf("failed to make prefix config map, err: %s", err.Error())
	}
	if err := g.AddInfo(gossip.KeySentinel, "cluster1", time.Hour); err != nil {
		t.Fatal(err)
	}
	if err := g.AddInfo(gossip.KeyConfigPermission, configMap, time.Hour); err != nil {
		t.Fatal(err)
	}
	if err := g.AddInfo(gossip.KeyFirstRangeDescriptor, testRangeDescriptor, time.Hour); err != nil {
		t.Fatal(err)
	}
	nodeIDKey := gossip.MakeNodeIDKey(1)
	if err := g.AddInfo(nodeIDKey, &proto.NodeDescriptor{
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
					NodeDesc: proto.NodeDescriptor{NodeID: 2},
				},
				replicaInfo{
					Replica:  proto.Replica{NodeID: 3, StoreID: 3},
					NodeDesc: proto.NodeDescriptor{NodeID: 3},
				},
				replicaInfo{
					Replica:  proto.Replica{NodeID: 1, StoreID: 1},
					NodeDesc: proto.NodeDescriptor{NodeID: 1},
				},
			},
			localNodeDesc: proto.NodeDescriptor{NodeID: 1},
		},
		{
			// Sort replicas by attribute
			slice: replicaSlice{
				replicaInfo{
					Replica:  proto.Replica{NodeID: 2, StoreID: 2},
					NodeDesc: proto.NodeDescriptor{NodeID: 2, Attrs: proto.Attributes{Attrs: []string{"ad"}}},
				},
				replicaInfo{
					Replica:  proto.Replica{NodeID: 3, StoreID: 3},
					NodeDesc: proto.NodeDescriptor{NodeID: 3, Attrs: proto.Attributes{Attrs: []string{"ab", "c"}}},
				},
				replicaInfo{
					Replica:  proto.Replica{NodeID: 1, StoreID: 1},
					NodeDesc: proto.NodeDescriptor{NodeID: 1, Attrs: proto.Attributes{Attrs: []string{"ab"}}},
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
		rpcSend: testFn,
		rangeDescriptorDB: mockRangeDescriptorDB(func(proto.Key, lookupOptions) ([]proto.RangeDescriptor, error) {
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
			if err := g.AddInfo(gossip.MakeNodeIDKey(proto.NodeID(i)), nd, time.Hour); err != nil {
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
		if !tc.consistent {
			args.Header().ReadConsistency = proto.INCONSISTENT
		}
		// Kill the cached NodeDescriptor, enforcing a lookup from Gossip.
		ds.nodeDescriptor = nil
		call := proto.Call{Args: args, Reply: args.CreateReply()}
		ds.Send(context.Background(), call)
		if err := call.Reply.Header().GoError(); err != nil {
			t.Errorf("%d: %s", n, err)
		}
	}
}

type mockRangeDescriptorDB func(proto.Key, lookupOptions) ([]proto.RangeDescriptor, error)

func (mdb mockRangeDescriptorDB) getRangeDescriptors(k proto.Key, lo lookupOptions) ([]proto.RangeDescriptor, error) {
	return mdb(k, lo)
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
		rpcSend: testFn,
		rangeDescriptorDB: mockRangeDescriptorDB(func(_ proto.Key, _ lookupOptions) ([]proto.RangeDescriptor, error) {
			return []proto.RangeDescriptor{testRangeDescriptor}, nil
		}),
	}
	ds := NewDistSender(ctx, g)
	call := proto.PutCall(proto.Key("a"), proto.Value{Bytes: []byte("value")})
	reply := call.Reply.(*proto.PutResponse)
	ds.Send(context.Background(), call)
	if err := reply.GoError(); err != nil {
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
		&proto.Error{
			Message:   "fatal boom",
			Retryable: false,
		},
		&proto.Error{
			Message:   "temporary boom",
			Retryable: true,
		},
		nil,
	}

	ctx := &DistSenderContext{
		rpcSend: testFn,
		rangeDescriptorDB: mockRangeDescriptorDB(func(_ proto.Key, _ lookupOptions) ([]proto.RangeDescriptor, error) {
			// Return next error and truncate the prefix of the errors array.
			err := errors[0]
			errors = errors[1:]
			return []proto.RangeDescriptor{testRangeDescriptor}, err
		}),
	}
	ds := NewDistSender(ctx, g)
	call := proto.PutCall(proto.Key("a"), proto.Value{Bytes: []byte("value")})
	reply := call.Reply.(*proto.PutResponse)
	// Fatal error on descriptor lookup, propagated to reply.
	ds.Send(context.Background(), call)
	if err := reply.Header().Error; err.GetMessage() != "fatal boom" {
		t.Errorf("unexpected error: %s", err)
	}
	// Retryable error on descriptor lookup, second attempt successful.
	ds.Send(context.Background(), call)
	if err := reply.GoError(); err != nil {
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
			err := &proto.Error{
				Message:   "boom",
				Retryable: tc.retryable,
			}
			if tc.rpcError {
				return nil, err
			}
			reply := getReply()
			reply.(proto.Response).Header().SetGoError(err)
			return []gogoproto.Message{reply}, nil
		}

		ctx := &DistSenderContext{
			rpcSend: testFn,
			rangeDescriptorDB: mockRangeDescriptorDB(func(_ proto.Key, _ lookupOptions) ([]proto.RangeDescriptor, error) {
				return []proto.RangeDescriptor{testRangeDescriptor}, nil
			}),
		}
		ds := NewDistSender(ctx, g)
		ds.updateLeaderCache(1, leader)

		call := proto.PutCall(proto.Key("a"), proto.Value{Bytes: []byte("value")})
		reply := call.Reply.(*proto.PutResponse)
		ds.Send(context.Background(), call)
		if err := reply.GoError(); err != nil && err.Error() != "boom" {
			t.Errorf("put encountered unexpected error: %s", err)
		}
		if cur := ds.leaderCache.Lookup(1); reflect.DeepEqual(cur, &proto.Replica{}) && !tc.shouldClearLeader {
			t.Errorf("%d: leader cache eviction: shouldClearLeader=%t, but value is %v", i, tc.shouldClearLeader, cur)
		}
		_, cachedDesc := ds.rangeCache.getCachedRangeDescriptor(call.Args.Header().Key, false /* !inclusive */)
		if cachedDesc == nil != tc.shouldClearReplica {
			t.Errorf("%d: unexpected second replica lookup behaviour: wanted=%t", i, tc.shouldClearReplica)
		}
	}
}

// TestRangeLookupOnPushTxnIgnoresIntents verifies that if a push txn
// request has range lookup set, the range lookup requests will have
// ignore intents set.
func TestRangeLookupOnPushTxnIgnoresIntents(t *testing.T) {
	defer leaktest.AfterTest(t)
	g, s := makeTestGossip(t)
	defer s()

	var testFn rpcSendFn = func(_ rpc.Options, method string, addrs []net.Addr, getArgs func(addr net.Addr) gogoproto.Message, getReply func() gogoproto.Message, _ *rpc.Context) ([]gogoproto.Message, error) {
		return []gogoproto.Message{getReply()}, nil
	}

	for _, rangeLookup := range []bool{true, false} {
		ctx := &DistSenderContext{
			rpcSend: testFn,
			rangeDescriptorDB: mockRangeDescriptorDB(func(_ proto.Key, opts lookupOptions) ([]proto.RangeDescriptor, error) {
				if opts.ignoreIntents != rangeLookup {
					t.Fatalf("expected ignore intents to be %t", rangeLookup)
				}
				return []proto.RangeDescriptor{testRangeDescriptor}, nil
			}),
		}
		ds := NewDistSender(ctx, g)
		call := proto.Call{
			Args: &proto.PushTxnRequest{
				RequestHeader: proto.RequestHeader{Key: proto.Key("a")},
				RangeLookup:   rangeLookup,
			},
			Reply: &proto.PushTxnResponse{},
		}
		ds.Send(context.Background(), call)
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
	newRangeDescriptor := testRangeDescriptor
	newEndKey := proto.Key("m")
	descStale := true

	var testFn rpcSendFn = func(_ rpc.Options, method string, addrs []net.Addr, getArgs func(addr net.Addr) gogoproto.Message, getReply func() gogoproto.Message, _ *rpc.Context) ([]gogoproto.Message, error) {
		header := getArgs(testAddress).(proto.Request).Header()
		if method == "Node.RangeLookup" {
			// If the non-broken descriptor has already been returned, that's
			// an error.
			if !descStale && bytes.HasPrefix(header.Key, keys.Meta2Prefix) {
				t.Errorf("unexpected extra lookup for non-stale replica descriptor at %s",
					header.Key)
			}

			r := getReply().(*proto.RangeLookupResponse)
			// The fresh descriptor is about to be returned.
			if bytes.HasPrefix(header.Key, keys.Meta2Prefix) &&
				newRangeDescriptor.StartKey.Equal(newEndKey) {
				descStale = false
			}
			r.Ranges = append(r.Ranges, newRangeDescriptor)
			return []gogoproto.Message{r}, nil
		}
		// When the Scan first turns up, update the descriptor for future
		// range descriptor lookups.
		if !newRangeDescriptor.StartKey.Equal(newEndKey) {
			newRangeDescriptor = *gogoproto.Clone(&testRangeDescriptor).(*proto.RangeDescriptor)
			newRangeDescriptor.StartKey = newEndKey
			return nil, &proto.RangeKeyMismatchError{RequestStartKey: header.Key,
				RequestEndKey: header.EndKey}
		}
		return []gogoproto.Message{getReply()}, nil
	}

	ctx := &DistSenderContext{
		rpcSend: testFn,
	}
	ds := NewDistSender(ctx, g)
	call := proto.ScanCall(proto.Key("a"), proto.Key("d"), 0)
	sr := call.Reply.(*proto.ScanResponse)
	ds.Send(context.Background(), call)
	if err := sr.GoError(); err != nil {
		t.Errorf("scan encountered error: %s", err)
	}
}

func TestGetFirstRangeDescriptor(t *testing.T) {
	defer leaktest.AfterTest(t)
	n := simulation.NewNetwork(3, "tcp", gossip.TestInterval)
	ds := NewDistSender(nil, n.Nodes[0].Gossip)
	if _, err := ds.getFirstRangeDescriptor(); err == nil {
		t.Errorf("expected not to find first range descriptor")
	}
	expectedDesc := &proto.RangeDescriptor{}
	expectedDesc.StartKey = proto.Key("a")
	expectedDesc.EndKey = proto.Key("c")

	// Add first RangeDescriptor to a node different from the node for
	// this dist sender and ensure that this dist sender has the
	// information within a given time.
	if err := n.Nodes[1].Gossip.AddInfo(gossip.KeyFirstRangeDescriptor, *expectedDesc, time.Hour); err != nil {
		t.Fatal(err)
	}
	maxCycles := 10
	n.SimulateNetwork(func(cycle int, network *simulation.Network) bool {
		desc, err := ds.getFirstRangeDescriptor()
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

// TestVerifyPermissions verifies permissions are checked for single
// zones and across multiple zones. It also verifies that permissions
// are checked hierarchically.
func TestVerifyPermissions(t *testing.T) {
	defer leaktest.AfterTest(t)
	n := simulation.NewNetwork(1, "tcp", gossip.TestInterval)
	ds := NewDistSender(nil, n.Nodes[0].Gossip)
	config1 := &config.PermConfig{
		Read:  []string{"read1", "readAll", "rw1", "rwAll"},
		Write: []string{"write1", "writeAll", "rw1", "rwAll"}}
	config2 := &config.PermConfig{
		Read:  []string{"read2", "readAll", "rw2", "rwAll"},
		Write: []string{"write2", "writeAll", "rw2", "rwAll"}}
	configs := []*config.PrefixConfig{
		{proto.KeyMin, nil, config1},
		{proto.Key("a"), nil, config2},
	}
	configMap, err := config.NewPrefixConfigMap(configs)
	if err != nil {
		t.Fatalf("failed to make prefix config map, err: %s", err.Error())
	}
	if err := ds.gossip.AddInfo(gossip.KeyConfigPermission, configMap, time.Hour); err != nil {
		t.Fatal(err)
	}

	allRequestTypes := []proto.Request{
		&proto.GetRequest{},
		&proto.PutRequest{},
		&proto.ConditionalPutRequest{},
		&proto.IncrementRequest{},
		&proto.DeleteRequest{},
		&proto.DeleteRangeRequest{},
		&proto.ScanRequest{},
		&proto.ReverseScanRequest{},
		&proto.EndTransactionRequest{},
		&proto.AdminSplitRequest{},
		&proto.AdminMergeRequest{},
		&proto.HeartbeatTxnRequest{},
		&proto.GCRequest{},
		&proto.PushTxnRequest{},
		&proto.RangeLookupRequest{},
		&proto.ResolveIntentRequest{},
		&proto.ResolveIntentRangeRequest{},
		&proto.MergeRequest{},
		&proto.TruncateLogRequest{},
		&proto.LeaderLeaseRequest{},
		&proto.BatchRequest{},
	}

	var readOnlyRequests []proto.Request
	var writeOnlyRequests []proto.Request
	var readWriteRequests []proto.Request

	for _, r := range allRequestTypes {
		if proto.IsRead(r) && !proto.IsWrite(r) {
			readOnlyRequests = append(readOnlyRequests, r)
		}
		if proto.IsWrite(r) && !proto.IsRead(r) {
			writeOnlyRequests = append(writeOnlyRequests, r)
		}
		if proto.IsRead(r) && proto.IsWrite(r) {
			readWriteRequests = append(readWriteRequests, r)
		}
	}

	testData := []struct {
		// Permission-based db methods from the storage package.
		requests         []proto.Request
		user             string
		startKey, endKey proto.Key
		hasPermission    bool
	}{
		// Test permissions within a single range
		{readOnlyRequests, "read1", proto.KeyMin, proto.KeyMin, true},
		{readOnlyRequests, "rw1", proto.KeyMin, proto.KeyMin, true},
		{readOnlyRequests, "write1", proto.KeyMin, proto.KeyMin, false},
		{readOnlyRequests, "random", proto.KeyMin, proto.KeyMin, false},
		{readWriteRequests, "rw1", proto.KeyMin, proto.KeyMin, true},
		{readWriteRequests, "read1", proto.KeyMin, proto.KeyMin, false},
		{readWriteRequests, "write1", proto.KeyMin, proto.KeyMin, false},
		{writeOnlyRequests, "write1", proto.KeyMin, proto.KeyMin, true},
		{writeOnlyRequests, "rw1", proto.KeyMin, proto.KeyMin, true},
		{writeOnlyRequests, "read1", proto.KeyMin, proto.KeyMin, false},
		{writeOnlyRequests, "random", proto.KeyMin, proto.KeyMin, false},
		// Test permissions hierarchically.
		{readOnlyRequests, "read1", proto.Key("a"), proto.Key("a1"), true},
		{readWriteRequests, "rw1", proto.Key("a"), proto.Key("a1"), true},
		{writeOnlyRequests, "write1", proto.Key("a"), proto.Key("a1"), true},
		// Test permissions across both ranges.
		{readOnlyRequests, "readAll", proto.KeyMin, proto.Key("b"), true},
		{readOnlyRequests, "read1", proto.KeyMin, proto.Key("b"), true},
		{readOnlyRequests, "read2", proto.KeyMin, proto.Key("b"), false},
		{readOnlyRequests, "random", proto.KeyMin, proto.Key("b"), false},
		{readWriteRequests, "rwAll", proto.KeyMin, proto.Key("b"), true},
		{readWriteRequests, "rw1", proto.KeyMin, proto.Key("b"), true},
		{readWriteRequests, "random", proto.KeyMin, proto.Key("b"), false},
		{writeOnlyRequests, "writeAll", proto.KeyMin, proto.Key("b"), true},
		{writeOnlyRequests, "write1", proto.KeyMin, proto.Key("b"), true},
		{writeOnlyRequests, "write2", proto.KeyMin, proto.Key("b"), false},
		{writeOnlyRequests, "random", proto.KeyMin, proto.Key("b"), false},
		// Test permissions within and around the boundaries of a range,
		// representatively using rw methods.
		{readWriteRequests, "rw2", proto.Key("a"), proto.Key("b"), true},
		{readWriteRequests, "rwAll", proto.Key("a"), proto.Key("b"), true},
		{readWriteRequests, "rw2", proto.Key("a"), proto.Key("a"), true},
		{readWriteRequests, "rw2", proto.Key("a"), proto.Key("a1"), true},
		{readWriteRequests, "rw2", proto.Key("a"), proto.Key("b1"), false},
		{readWriteRequests, "rw2", proto.Key("a3"), proto.Key("a4"), true},
		{readWriteRequests, "rw2", proto.Key("a3"), proto.Key("b1"), false},
	}

	for i, test := range testData {
		for _, r := range test.requests {
			*r.Header() = proto.RequestHeader{
				User:   test.user,
				Key:    test.startKey,
				EndKey: test.endKey,
			}
			err := ds.verifyPermissions(r)
			if err != nil && test.hasPermission {
				t.Errorf("test %d: user %s should have had permission to %s, err: %s",
					i, test.user, r.Method(), err.Error())
				break
			} else if err == nil && !test.hasPermission {
				t.Errorf("test %d: user %s should not have had permission to %s",
					i, test.user, r.Method())
				break
			}
		}
	}
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
		if err := g.AddInfo(gossip.MakeNodeIDKey(proto.NodeID(i)), nd, time.Hour); err != nil {
			t.Fatal(err)
		}

		descriptor.Replicas = append(descriptor.Replicas, proto.Replica{
			NodeID:  proto.NodeID(i),
			StoreID: proto.StoreID(i),
		})
	}
	// Define our rpcSend stub which returns success on the second address.
	var testFn rpcSendFn = func(_ rpc.Options, method string, addrs []net.Addr, getArgs func(addr net.Addr) gogoproto.Message, getReply func() gogoproto.Message, _ *rpc.Context) ([]gogoproto.Message, error) {
		if method == "Node.Scan" {
			// reply from first address failed
			_ = getReply()
			// reply from second address succeed
			reply := getReply()
			reply.(*proto.ScanResponse).Rows = append([]proto.KeyValue{}, proto.KeyValue{Key: proto.Key("b"), Value: proto.Value{}})
			return []gogoproto.Message{reply}, nil
		}
		return nil, util.Errorf("Not expected method %v", method)
	}
	ctx := &DistSenderContext{
		rpcSend: testFn,
		rangeDescriptorDB: mockRangeDescriptorDB(func(_ proto.Key, _ lookupOptions) ([]proto.RangeDescriptor, error) {
			return []proto.RangeDescriptor{descriptor}, nil
		}),
	}
	ds := NewDistSender(ctx, g)
	call := proto.ScanCall(proto.Key("a"), proto.Key("d"), 1)
	sr := call.Reply.(*proto.ScanResponse)
	ds.Send(context.Background(), call)
	if err := sr.GoError(); err != nil {
		t.Fatal(err)
	}
	if l := len(sr.Rows); l != 1 {
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
		if method != "Node.Scan" {
			t.Fatalf("unexpected method:%s", method)
		}
		header := getArgs(testAddress).(proto.Request).Header()
		reply := getReply().(*proto.ScanResponse)
		results := []proto.KeyValue{}
		for _, curKV := range existingKVs {
			if header.Key.Less(curKV.Key.Next()) && curKV.Key.Less(header.EndKey) {
				results = append(results, curKV)
			}
		}
		reply.Rows = results
		return []gogoproto.Message{reply}, nil
	}
	ctx := &DistSenderContext{
		rpcSend: testFn,
		rangeDescriptorDB: mockRangeDescriptorDB(func(key proto.Key, _ lookupOptions) ([]proto.RangeDescriptor, error) {
			if !merged {
				// Asume a range merge operation happened
				merged = true
				return []proto.RangeDescriptor{firstRange}, nil
			}
			return []proto.RangeDescriptor{mergedRange}, nil
		}),
	}
	ds := NewDistSender(ctx, g)
	call := proto.ScanCall(proto.Key("a"), proto.Key("d"), 10)
	// Set the Txn info to avoid an OpRequiresTxnError.
	call.Args.Header().Txn = &proto.Transaction{}
	reply := call.Reply.(*proto.ScanResponse)
	ds.Send(context.Background(), call)
	if err := reply.GoError(); err != nil {
		t.Fatalf("scan encountered error: %s", err)
	}
	if !reflect.DeepEqual(existingKVs, reply.Rows) {
		t.Fatalf("expect get %v, actual get %v", existingKVs, reply.Rows)
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
		rpcSend: testFn,
		rangeDescriptorDB: mockRangeDescriptorDB(func(_ proto.Key, opts lookupOptions) ([]proto.RangeDescriptor, error) {
			if !opts.useReverseScan {
				t.Fatalf("expected useReverseScan to be set")
			}
			return []proto.RangeDescriptor{testRangeDescriptor}, nil
		}),
	}
	ds := NewDistSender(ctx, g)
	call := proto.Call{
		Args: &proto.ReverseScanRequest{
			RequestHeader: proto.RequestHeader{EndKey: proto.Key("a")},
		},
		Reply: &proto.ReverseScanResponse{},
	}
	ds.Send(context.Background(), call)
}
