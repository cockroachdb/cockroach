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
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/gossip/simulation"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util"
	gogoproto "github.com/gogo/protobuf/proto"
)

var testRangeDescriptor = proto.RangeDescriptor{
	RaftID:   1,
	StartKey: proto.Key("a"),
	EndKey:   proto.Key("z"),
	Replicas: []proto.Replica{
		{
			NodeID:  1,
			StoreID: 1,
			Attrs:   proto.Attributes{Attrs: []string{"test"}},
		},
	},
}

var testAddress = util.MakeRawAddr("tcp", "node1:8080")

func makeTestGossip(t *testing.T) *gossip.Gossip {
	n := simulation.NewNetwork(1, "unix", gossip.TestInterval)
	g := n.Nodes[0].Gossip
	permConfig := &proto.PermConfig{
		Read:  []string{""},
		Write: []string{""},
	}

	configMap, err := storage.NewPrefixConfigMap([]*storage.PrefixConfig{
		{engine.KeyMin, nil, permConfig},
	})
	if err != nil {
		t.Fatalf("failed to make prefix config map, err: %s", err.Error())
	}
	g.AddInfo(gossip.KeySentinel, "cluster1", time.Hour)
	g.AddInfo(gossip.KeyConfigPermission, configMap, time.Hour)
	g.AddInfo(gossip.KeyFirstRangeDescriptor, testRangeDescriptor, time.Hour)
	nodeIDKey := gossip.MakeNodeIDKey(1)
	g.AddInfo(nodeIDKey, &storage.NodeDescriptor{
		NodeID:  1,
		Address: testAddress,
		Attrs:   proto.Attributes{Attrs: []string{"attr1", "attr2"}},
	}, time.Hour)
	return g
}

// TestSendRPCOrder verifies that sendRPC correctly takes into account the
// leader, attributes and required consistency to determine where to send
// remote requests.
func TestSendRPCOrder(t *testing.T) {
	g := makeTestGossip(t)
	g.NodeID = 1
	raftID := int64(99)

	nodeAttrs := map[int32][]string{
		1: []string{}, // The local node, set in each test case.
		2: []string{"us", "west", "gpu"},
		3: []string{"eu", "dublin", "pdu2", "gpu"},
		4: []string{"us", "east", "gpu"},
		5: []string{"us", "east", "gpu", "flaky"},
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
			for i, a := range addrs {
				if len(addrs) != len(expAddrs) || (expAddrs[i] > 0 &&
					addrToNode[a.String()] != int32(expAddrs[i])) {
					return util.Errorf("addresses changed: %s", addrs)
				}
			}
			return nil
		}
	}

	testCases := []struct {
		method string
		attrs  []string
		fn     func(rpc.Options, []net.Addr) error
		leader int32 // 0 for not caching a leader.
		// Naming is somewhat off, as eventually consistent reads usually
		// do not have to go to the leader when a node has a read lease.
		// Would really want CONSENSUS here, but that is not implemented.
		// Likely a test setup here will never have a read lease, but good
		// to keep in mind.
		consistent bool
	}{
		// Inconsistent Scan without matching attributes.
		{
			method: "Scan",
			attrs:  []string{},
			fn:     makeVerifier(rpc.OrderRandom, []int32{1, 2, 3, 4, 5}),
		},
		// Inconsistent Scan with matching attributes.
		// Should move the two nodes matching the attributes to the front and
		// go stable.
		{
			method: "Scan",
			attrs:  nodeAttrs[5],
			// Compare only the first two resulting addresses.
			fn: makeVerifier(rpc.OrderStable, []int32{5, 4, 0, 0, 0}),
		},

		// Scan without matching attributes that requires but does not find
		// a leader.
		{
			method:     "Scan",
			attrs:      []string{},
			fn:         makeVerifier(rpc.OrderRandom, []int32{1, 2, 3, 4, 5}),
			consistent: true,
		},
		// Put without matching attributes that requires but does not find leader.
		// Should go random and not change anything.
		{
			method: "Put",
			attrs:  []string{"nomatch"},
			fn:     makeVerifier(rpc.OrderRandom, []int32{1, 2, 3, 4, 5}),
		},
		// Put with matching attributes but no leader.
		// Should move the two nodes matching the attributes to the front and
		// go stable.
		{
			method: "Put",
			attrs:  append(nodeAttrs[5], "irrelevant"),
			// Compare only the first two resulting addresses.
			fn: makeVerifier(rpc.OrderStable, []int32{5, 4, 0, 0, 0}),
		},

		// Put with matching attributes that finds the leader (node 3).
		// Should address the leader and the two nodes matching the attributes
		// (the last and second to last) in that order.
		{
			method: "Put",
			attrs:  append(nodeAttrs[5], "irrelevant"),
			// Compare only the first three resulting addresses.
			fn:     makeVerifier(rpc.OrderStable, []int32{2, 5, 4, 0, 0}),
			leader: 2,
		},
	}

	// Stub to be changed in each test case.
	verifyCall := func(o rpc.Options, addrs []net.Addr) error {
		return util.Errorf("was not supposed to be invoked")
	}

	var testFn rpcSendFn = func(opts rpc.Options, method string,
		addrs []net.Addr, _ func(addr net.Addr) interface{},
		_ func() interface{}, _ *rpc.Context) ([]interface{}, error) {
		return nil, verifyCall(opts, addrs)
	}

	ctx := &DistSenderContext{
		rpcSend: testFn,
	}

	ds := NewDistSender(ctx, g)

	for n, tc := range testCases {
		verifyCall = tc.fn
		// We don't need to do all of it for each test case, but we need the
		// replica slice so might as well do it all.
		descriptor := proto.RangeDescriptor{
			RaftID:   raftID,
			Replicas: nil,
		}
		for i := int32(1); i <= 5; i++ {
			addr := util.MakeRawAddr("tcp", fmt.Sprintf("%d", i))
			addrToNode[addr.String()] = i
			nd := &storage.NodeDescriptor{
				NodeID:  proto.NodeID(i),
				Address: util.MakeRawAddr("tcp", fmt.Sprintf("%d", i)),
			}
			// First (= local) node needs to get its attributes during sendRPC.
			if i == 1 {
				nd.Attrs = proto.Attributes{Attrs: tc.attrs}
			}
			g.AddInfo(gossip.MakeNodeIDKey(proto.NodeID(i)), nd, time.Hour)

			descriptor.Replicas = append(descriptor.Replicas, proto.Replica{
				NodeID:  proto.NodeID(i),
				StoreID: proto.StoreID(i),
				Attrs:   proto.Attributes{Attrs: nodeAttrs[i]},
			})
		}
		ds.leaderCache.Update(proto.RaftID(raftID), nil)
		if tc.leader > 0 {
			ds.leaderCache.Update(proto.RaftID(raftID), &descriptor.Replicas[tc.leader-1])
		}

		// Always create the parameters for Scan, only the Header() is used
		// anyways so it doesn't matter.
		args := proto.ScanArgs(proto.Key("b"), proto.Key("y"), 0)
		args.Header().RaftID = raftID // Not used in this test, but why not.
		if !tc.consistent {
			args.Header().ReadConsistency = proto.INCONSISTENT
		}
		reply := &proto.ScanResponse{}
		if err := ds.sendRPC(&descriptor, tc.method, args, reply); err != nil {
			t.Errorf("%d: %s", n, err)
		}
	}
}

// TestRetryOnWrongReplicaError sets up a DistSender on a minimal gossip
// network and a mock of rpc.Send, and verifies that the DistSender correctly
// retries upon encountering a stale entry in its range descriptor cache.
func TestRetryOnWrongReplicaError(t *testing.T) {
	g := makeTestGossip(t)
	// Updated below, after it has first been returned.
	newRangeDescriptor := testRangeDescriptor
	newEndKey := proto.Key("m")
	descStale := true

	var testFn rpcSendFn = func(_ rpc.Options, method string, addrs []net.Addr, getArgs func(addr net.Addr) interface{}, getReply func() interface{}, _ *rpc.Context) ([]interface{}, error) {
		header := getArgs(testAddress).(proto.Request).Header()
		if method == "Node.InternalRangeLookup" {
			// If the non-broken descriptor has already been returned, that's
			// an error.
			if !descStale && bytes.HasPrefix(header.Key, engine.KeyMeta2Prefix) {
				t.Errorf("unexpected extra lookup for non-stale replica descriptor at %s",
					header.Key)
			}

			r := getReply().(*proto.InternalRangeLookupResponse)
			// The fresh descriptor is about to be returned.
			if bytes.HasPrefix(header.Key, engine.KeyMeta2Prefix) &&
				newRangeDescriptor.StartKey.Equal(newEndKey) {
				descStale = false
			}
			r.Ranges = append(r.Ranges, newRangeDescriptor)
			return nil, nil
		}
		// When the Scan first turns up, update the descriptor for future
		// range descriptor lookups.
		if !newRangeDescriptor.StartKey.Equal(newEndKey) {
			newRangeDescriptor = *gogoproto.Clone(&testRangeDescriptor).(*proto.RangeDescriptor)
			newRangeDescriptor.StartKey = newEndKey
			return nil, &proto.RangeKeyMismatchError{RequestStartKey: header.Key,
				RequestEndKey: header.EndKey}
		}
		return nil, nil
	}

	ctx := &DistSenderContext{
		rpcSend: testFn,
	}
	ds := NewDistSender(ctx, g)
	sa := proto.ScanArgs(proto.Key("a"), proto.Key("d"), 0)
	sr := &proto.ScanResponse{}
	ds.Send(&client.Call{Method: proto.Scan, Args: sa, Reply: sr})
	if err := sr.GoError(); err != nil {
		t.Errorf("scan encountered error: %s", err)
	}
}

func TestGetFirstRangeDescriptor(t *testing.T) {
	n := simulation.NewNetwork(3, "unix", gossip.TestInterval)
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
	n.Nodes[1].Gossip.AddInfo(
		gossip.KeyFirstRangeDescriptor, *expectedDesc, time.Hour)
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
	n := simulation.NewNetwork(1, "unix", gossip.TestInterval)
	ds := NewDistSender(nil, n.Nodes[0].Gossip)
	config1 := &proto.PermConfig{
		Read:  []string{"read1", "readAll", "rw1", "rwAll"},
		Write: []string{"write1", "writeAll", "rw1", "rwAll"}}
	config2 := &proto.PermConfig{
		Read:  []string{"read2", "readAll", "rw2", "rwAll"},
		Write: []string{"write2", "writeAll", "rw2", "rwAll"}}
	configs := []*storage.PrefixConfig{
		{engine.KeyMin, nil, config1},
		{proto.Key("a"), nil, config2},
	}
	configMap, err := storage.NewPrefixConfigMap(configs)
	if err != nil {
		t.Fatalf("failed to make prefix config map, err: %s", err.Error())
	}
	ds.gossip.AddInfo(gossip.KeyConfigPermission, configMap, time.Hour)

	readOnlyMethods := make([]string, 0, len(proto.ReadMethods))
	writeOnlyMethods := make([]string, 0, len(proto.WriteMethods))
	readWriteMethods := make([]string, 0, len(proto.ReadMethods)+len(proto.WriteMethods))
	for readM := range proto.ReadMethods {
		if proto.IsReadOnly(readM) {
			readOnlyMethods = append(readOnlyMethods, readM)
		} else {
			readWriteMethods = append(readWriteMethods, readM)
		}
	}
	for writeM := range proto.WriteMethods {
		if !proto.NeedReadPerm(writeM) {
			writeOnlyMethods = append(writeOnlyMethods, writeM)
		}
	}

	testData := []struct {
		// Permission-based db methods from the storage package.
		methods          []string
		user             string
		startKey, endKey proto.Key
		hasPermission    bool
	}{
		// Test permissions within a single range
		{readOnlyMethods, "read1", engine.KeyMin, engine.KeyMin, true},
		{readOnlyMethods, "rw1", engine.KeyMin, engine.KeyMin, true},
		{readOnlyMethods, "write1", engine.KeyMin, engine.KeyMin, false},
		{readOnlyMethods, "random", engine.KeyMin, engine.KeyMin, false},
		{readWriteMethods, "rw1", engine.KeyMin, engine.KeyMin, true},
		{readWriteMethods, "read1", engine.KeyMin, engine.KeyMin, false},
		{readWriteMethods, "write1", engine.KeyMin, engine.KeyMin, false},
		{writeOnlyMethods, "write1", engine.KeyMin, engine.KeyMin, true},
		{writeOnlyMethods, "rw1", engine.KeyMin, engine.KeyMin, true},
		{writeOnlyMethods, "read1", engine.KeyMin, engine.KeyMin, false},
		{writeOnlyMethods, "random", engine.KeyMin, engine.KeyMin, false},
		// Test permissions hierarchically.
		{readOnlyMethods, "read1", proto.Key("a"), proto.Key("a1"), true},
		{readWriteMethods, "rw1", proto.Key("a"), proto.Key("a1"), true},
		{writeOnlyMethods, "write1", proto.Key("a"), proto.Key("a1"), true},
		// Test permissions across both ranges.
		{readOnlyMethods, "readAll", engine.KeyMin, proto.Key("b"), true},
		{readOnlyMethods, "read1", engine.KeyMin, proto.Key("b"), true},
		{readOnlyMethods, "read2", engine.KeyMin, proto.Key("b"), false},
		{readOnlyMethods, "random", engine.KeyMin, proto.Key("b"), false},
		{readWriteMethods, "rwAll", engine.KeyMin, proto.Key("b"), true},
		{readWriteMethods, "rw1", engine.KeyMin, proto.Key("b"), true},
		{readWriteMethods, "random", engine.KeyMin, proto.Key("b"), false},
		{writeOnlyMethods, "writeAll", engine.KeyMin, proto.Key("b"), true},
		{writeOnlyMethods, "write1", engine.KeyMin, proto.Key("b"), true},
		{writeOnlyMethods, "write2", engine.KeyMin, proto.Key("b"), false},
		{writeOnlyMethods, "random", engine.KeyMin, proto.Key("b"), false},
		// Test permissions within and around the boundaries of a range,
		// representatively using rw methods.
		{readWriteMethods, "rw2", proto.Key("a"), proto.Key("b"), true},
		{readWriteMethods, "rwAll", proto.Key("a"), proto.Key("b"), true},
		{readWriteMethods, "rw2", proto.Key("a"), proto.Key("a"), true},
		{readWriteMethods, "rw2", proto.Key("a"), proto.Key("a1"), true},
		{readWriteMethods, "rw2", proto.Key("a"), proto.Key("b1"), false},
		{readWriteMethods, "rw2", proto.Key("a3"), proto.Key("a4"), true},
		{readWriteMethods, "rw2", proto.Key("a3"), proto.Key("b1"), false},
	}

	for i, test := range testData {
		for _, method := range test.methods {
			err := ds.verifyPermissions(
				method,
				&proto.RequestHeader{
					User: test.user, Key: test.startKey, EndKey: test.endKey})
			if err != nil && test.hasPermission {
				t.Errorf("test %d: user %s should have had permission to %s, err: %s",
					i, test.user, method, err.Error())
				break
			} else if err == nil && !test.hasPermission {
				t.Errorf("test %d: user %s should not have had permission to %s",
					i, test.user, method)
				break
			}
		}
	}
	n.Stop()
}
