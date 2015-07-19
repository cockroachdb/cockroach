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
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package server

import (
	"bytes"
	"fmt"
	"net"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/gossip/resolver"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/kv"
	"github.com/cockroachdb/cockroach/multiraft"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/server/status"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/stop"
)

// createTestNode creates an rpc server using the specified address,
// gossip instance, KV database and a node using the specified slice
// of engines. The server, clock and node are returned. If gossipBS is
// not nil, the gossip bootstrap address is set to gossipBS.
func createTestNode(addr net.Addr, engines []engine.Engine, gossipBS net.Addr, t *testing.T) (
	*rpc.Server, *hlc.Clock, *Node, *stop.Stopper) {
	var err error
	ctx := storage.StoreContext{}

	stopper := stop.NewStopper()
	ctx.Clock = hlc.NewClock(hlc.UnixNano)
	nodeRPCContext := rpc.NewContext(nodeTestBaseContext, ctx.Clock, stopper)
	ctx.ScanInterval = 10 * time.Hour
	rpcServer := rpc.NewServer(addr, nodeRPCContext)
	if err := rpcServer.Start(); err != nil {
		t.Fatal(err)
	}
	g := gossip.New(nodeRPCContext, testContext.GossipInterval, testContext.GossipBootstrapResolvers)
	if gossipBS != nil {
		// Handle possibility of a :0 port specification.
		if gossipBS == addr {
			gossipBS = rpcServer.Addr()
		}
		g.SetResolvers([]resolver.Resolver{resolver.NewResolverFromAddress(gossipBS)})
		g.Start(rpcServer, stopper)
	}
	ctx.Gossip = g
	sender := kv.NewDistSender(&kv.DistSenderContext{Clock: ctx.Clock}, g)
	if ctx.DB, err = client.Open("//root@", client.SenderOpt(sender)); err != nil {
		t.Fatal(err)
	}
	// TODO(bdarnell): arrange to have the transport closed.
	ctx.Transport = multiraft.NewLocalRPCTransport()
	ctx.EventFeed = &util.Feed{}
	node := NewNode(ctx)
	return rpcServer, ctx.Clock, node, stopper
}

// createAndStartTestNode creates a new test node and starts it. The server and node are returned.
func createAndStartTestNode(addr net.Addr, engines []engine.Engine, gossipBS net.Addr, t *testing.T) (
	*rpc.Server, *Node, *stop.Stopper) {
	rpcServer, _, node, stopper := createTestNode(addr, engines, gossipBS, t)
	if err := node.start(rpcServer, engines, proto.Attributes{}, stopper); err != nil {
		t.Fatal(err)
	}
	return rpcServer, node, stopper
}

func formatKeys(keys []proto.Key) string {
	var buf bytes.Buffer
	for i, key := range keys {
		buf.WriteString(fmt.Sprintf("%d: %s\n", i, key))
	}
	return buf.String()
}

// TestBootstrapCluster verifies the results of bootstrapping a
// cluster. Uses an in memory engine.
func TestBootstrapCluster(t *testing.T) {
	defer leaktest.AfterTest(t)
	stopper := stop.NewStopper()
	e := engine.NewInMem(proto.Attributes{}, 1<<20)
	localDB, err := BootstrapCluster("cluster-1", []engine.Engine{e}, stopper)
	if err != nil {
		t.Fatal(err)
	}
	defer stopper.Stop()

	// Scan the complete contents of the local database.
	rows, err := localDB.Scan(keys.LocalPrefix.PrefixEnd(), proto.KeyMax, 0)
	if err != nil {
		t.Fatal(err)
	}
	var keys []proto.Key
	for _, kv := range rows {
		keys = append(keys, kv.Key)
	}
	var expectedKeys = []proto.Key{
		proto.MakeKey(proto.Key("\x00\x00meta1"), proto.KeyMax),
		proto.MakeKey(proto.Key("\x00\x00meta2"), proto.KeyMax),
		proto.Key("\x00acct"),
		proto.Key("\x00desc-idgen"),
		proto.Key("\x00node-idgen"),
		proto.Key("\x00perm"),
		proto.Key("\x00range-tree-root"),
		proto.Key("\x00store-idgen"),
		proto.Key("\x00user"),
		proto.Key("\x00zone"),
	}
	if !reflect.DeepEqual(keys, expectedKeys) {
		t.Errorf("expected keys mismatch:\n%s\n  -- vs. -- \n\n%s",
			formatKeys(keys), formatKeys(expectedKeys))
	}

	// TODO(spencer): check values.
}

// TestBootstrapNewStore starts a cluster with two unbootstrapped
// stores and verifies both stores are added and started.
func TestBootstrapNewStore(t *testing.T) {
	defer leaktest.AfterTest(t)
	eagerStopper := stop.NewStopper()
	e := engine.NewInMem(proto.Attributes{}, 1<<20)
	if _, err := BootstrapCluster("cluster-1", []engine.Engine{e}, eagerStopper); err != nil {
		t.Fatal(err)
	}
	eagerStopper.Stop()

	// Start a new node with two new stores which will require bootstrapping.
	engines := []engine.Engine{
		e,
		engine.NewInMem(proto.Attributes{}, 1<<20),
		engine.NewInMem(proto.Attributes{}, 1<<20),
	}
	_, node, stopper := createAndStartTestNode(util.CreateTestAddr("tcp"), engines, nil, t)
	defer stopper.Stop()

	// Non-initialized stores (in this case the new in-memory-based
	// store) will be bootstrapped by the node upon start. This happens
	// in a goroutine, so we'll have to wait a bit (maximum 1s) until
	// we can find the new node.
	if err := util.IsTrueWithin(func() bool { return node.lSender.GetStoreCount() == 3 }, 1*time.Second); err != nil {
		t.Error(err)
	}

	// Check whether all stores are started properly.
	if err := node.lSender.VisitStores(func(s *storage.Store) error {
		if s.IsStarted() == false {
			return util.Errorf("fail to start store: %s", s)
		}
		return nil
	}); err != nil {
		t.Error(err)
	}
}

// TestNodeJoin verifies a new node is able to join a bootstrapped
// cluster consisting of one node.
func TestNodeJoin(t *testing.T) {
	defer leaktest.AfterTest(t)
	stopper := stop.NewStopper()
	e := engine.NewInMem(proto.Attributes{}, 1<<20)
	if _, err := BootstrapCluster("cluster-1", []engine.Engine{e}, stopper); err != nil {
		t.Fatal(err)
	}
	stopper.Stop()

	// Set an aggressive gossip interval to make sure information is exchanged tout de suite.
	testContext.GossipInterval = gossip.TestInterval
	// Start the bootstrap node.
	engines1 := []engine.Engine{e}
	addr1 := util.CreateTestAddr("tcp")
	server1, node1, stopper1 := createAndStartTestNode(addr1, engines1, addr1, t)
	defer stopper1.Stop()

	// Create a new node.
	engines2 := []engine.Engine{engine.NewInMem(proto.Attributes{}, 1<<20)}
	server2, node2, stopper2 := createAndStartTestNode(util.CreateTestAddr("tcp"), engines2, server1.Addr(), t)
	defer stopper2.Stop()

	// Verify new node is able to bootstrap its store.
	if err := util.IsTrueWithin(func() bool { return node2.lSender.GetStoreCount() == 1 }, 50*time.Millisecond); err != nil {
		t.Fatal(err)
	}

	// Verify node1 sees node2 via gossip and vice versa.
	node1Key := gossip.MakeNodeIDKey(node1.Descriptor.NodeID)
	node2Key := gossip.MakeNodeIDKey(node2.Descriptor.NodeID)
	if err := util.IsTrueWithin(func() bool {
		if val, err := node1.ctx.Gossip.GetInfo(node2Key); err != nil {
			return false
		} else if addr2 := val.(*proto.NodeDescriptor).Address.Address; addr2 != server2.Addr().String() {
			t.Errorf("addr2 gossip %s doesn't match addr2 address %s", addr2, server2.Addr().String())
		}
		if val, err := node2.ctx.Gossip.GetInfo(node1Key); err != nil {
			return false
		} else if addr1 := val.(*proto.NodeDescriptor).Address.Address; addr1 != server1.Addr().String() {
			t.Errorf("addr1 gossip %s doesn't match addr1 address %s", addr1, server1.Addr().String())
		}
		return true
	}, 50*time.Millisecond); err != nil {
		t.Error(err)
	}
}

// TestCorruptedClusterID verifies that a node fails to start when a
// store's cluster ID is empty.
func TestCorruptedClusterID(t *testing.T) {
	defer leaktest.AfterTest(t)
	eagerStopper := stop.NewStopper()
	e := engine.NewInMem(proto.Attributes{}, 1<<20)
	_, err := BootstrapCluster("cluster-1", []engine.Engine{e}, eagerStopper)
	if err != nil {
		t.Fatal(err)
	}
	eagerStopper.Stop()

	// Set the cluster ID to an empty string.
	sIdent := proto.StoreIdent{
		ClusterID: "",
		NodeID:    1,
		StoreID:   1,
	}
	if err := engine.MVCCPutProto(e, nil, keys.StoreIdentKey(), proto.ZeroTimestamp, nil, &sIdent); err != nil {
		t.Fatal(err)
	}

	engines := []engine.Engine{e}
	server, _, node, stopper := createTestNode(util.CreateTestAddr("tcp"), engines, nil, t)
	if err := node.start(server, engines, proto.Attributes{}, stopper); err == nil {
		t.Errorf("unexpected success")
	}
	stopper.Stop()
}

// compareNodeStatus ensures that the actual node status for the passed in
// node is updated correctly. It checks that the Node Descriptor, StoreIDs,
// RangeCount, StartedAt, ReplicatedRangeCount and are exactly correct and that
// the bytes and counts for Live, Key and Val are at least the expected value.
// And that UpdatedAt has increased.
// The latest actual stats are returned.
func compareNodeStatus(t *testing.T, ts *TestServer, expectedNodeStatus *status.NodeStatus, testNumber int) *status.NodeStatus {
	nodeStatusKey := keys.NodeStatusKey(int32(ts.node.Descriptor.NodeID))
	nodeStatus := &status.NodeStatus{}
	if err := ts.db.GetProto(nodeStatusKey, nodeStatus); err != nil {
		t.Fatalf("%v: failure getting node status: %s", testNumber, err)
	}

	// These values must be equal.
	if a, e := nodeStatus.RangeCount, expectedNodeStatus.RangeCount; a != e {
		t.Errorf("%d: RangeCount does not match expected.\nexpected: %d actual: %d", testNumber, e, a)
	}
	if a, e := nodeStatus.Desc, expectedNodeStatus.Desc; !reflect.DeepEqual(a, e) {
		t.Errorf("%d: Descriptor does not match expected.\nexpected: %s\nactual: %s", testNumber, e, a)
	}
	if a, e := nodeStatus.ReplicatedRangeCount, expectedNodeStatus.ReplicatedRangeCount; a != e {
		t.Errorf("%d: ReplicatedRangeCount does not match expected.\nexpected: %d actual: %d", testNumber, e, a)
	}

	// These values must >= to the older value.
	// If StartedAt is 0, we skip this test as we don't have the base value yet.
	if a, e := nodeStatus.StartedAt, expectedNodeStatus.StartedAt; e > 0 && e != a {
		t.Errorf("%d: StartedAt does not match expected.\nexpected: %d actual: %d", testNumber, e, a)
	}
	if a, e := nodeStatus.Stats.LiveBytes, expectedNodeStatus.Stats.LiveBytes; a < e {
		t.Errorf("%d: LiveBytes is not greater or equal to expected.\nexpected: %d actual: %d", testNumber, e, a)
	}
	if a, e := nodeStatus.Stats.KeyBytes, expectedNodeStatus.Stats.KeyBytes; a < e {
		t.Errorf("%d: KeyBytes is not greater or equal to expected.\nexpected: %d actual: %d", testNumber, e, a)
	}
	if a, e := nodeStatus.Stats.ValBytes, expectedNodeStatus.Stats.ValBytes; a < e {
		t.Errorf("%d: ValBytes is not greater or equal to expected.\nexpected: %d actual: %d", testNumber, e, a)
	}
	if a, e := nodeStatus.Stats.LiveCount, expectedNodeStatus.Stats.LiveCount; a < e {
		t.Errorf("%d: LiveCount is not greater or equal to expected.\nexpected: %d actual: %d", testNumber, e, a)
	}
	if a, e := nodeStatus.Stats.KeyCount, expectedNodeStatus.Stats.KeyCount; a < e {
		t.Errorf("%d: KeyCount is not greater or equal to expected.\nexpected: %d actual: %d", testNumber, e, a)
	}
	if a, e := nodeStatus.Stats.ValCount, expectedNodeStatus.Stats.ValCount; a < e {
		t.Errorf("%d: ValCount is not greater or equal to expected.\nexpected: %d actual: %d", testNumber, e, a)
	}
	if a, e := nodeStatus.UpdatedAt, expectedNodeStatus.UpdatedAt; a < e {
		t.Errorf("%d: UpdatedAt is not greater or equal to expected.\nexpected: %d actual: %d", testNumber, e, a)
	}

	// Compare the store ids.
	var actualStoreIDs, expectedStoreIDs sort.IntSlice
	for _, id := range nodeStatus.StoreIDs {
		actualStoreIDs = append(actualStoreIDs, int(id))
	}
	sort.Sort(actualStoreIDs)
	for _, id := range expectedNodeStatus.StoreIDs {
		expectedStoreIDs = append(expectedStoreIDs, int(id))
	}
	sort.Sort(expectedStoreIDs)

	if !reflect.DeepEqual(actualStoreIDs, expectedStoreIDs) {
		t.Errorf("%d: actual Store IDs don't match expected.\nexpected: %d actual: %d", testNumber, expectedStoreIDs, actualStoreIDs)
	}

	return nodeStatus
}

// compareStoreStatus ensures that the actual store status for the passed in
// store is updated correctly. It checks that the Desc.StoreID, Desc.Attrs,
// Desc.Node, Desc.Capacity.Capacity, NodeID, RangeCount, ReplicatedRangeCount
// are exactly correct and that the bytes and counts for Live, Key and Val are
// at least the expected value.
// The latest actual stats are returned.
func compareStoreStatus(t *testing.T, ts *TestServer, store *storage.Store, expectedStoreStatus *storage.StoreStatus, testNumber int) *storage.StoreStatus {
	// Retrieve store status from database.
	storeStatusKey := keys.StoreStatusKey(int32(store.Ident.StoreID))
	storeStatus := &storage.StoreStatus{}
	if err := ts.db.GetProto(storeStatusKey, storeStatus); err != nil {
		t.Fatalf("%v: failure getting store status: %s", testNumber, err)
	}

	// Values must match exactly.
	if a, e := storeStatus.Desc.StoreID, expectedStoreStatus.Desc.StoreID; a != e {
		t.Errorf("%d: actual Desc.StoreID does not match expected. expected: %d actual: %d", testNumber, e, a)
	}
	if a, e := storeStatus.Desc.Attrs, expectedStoreStatus.Desc.Attrs; !reflect.DeepEqual(a, e) {
		t.Errorf("%d: actual Desc.Attrs does not match expected.\nexpected: %s\nactual: %s", testNumber, e, a)
	}
	if a, e := storeStatus.Desc.Node, expectedStoreStatus.Desc.Node; !reflect.DeepEqual(a, e) {
		t.Errorf("%d: actual Desc.Attrs does not match expected.\nexpected: %s\nactual: %s", testNumber, e, a)
	}
	if a, e := storeStatus.Desc.Capacity.Capacity, expectedStoreStatus.Desc.Capacity.Capacity; a != e {
		t.Errorf("%d: actual Desc.Capacity.Capacity does not match expected.\nexpected: %d\nactual: %d", testNumber, e, a)
	}
	if a, e := storeStatus.NodeID, expectedStoreStatus.NodeID; a != e {
		t.Errorf("%d: actual node ID does not match expected.\nexpected: %d\nactual: %d", testNumber, e, a)
	}
	if a, e := storeStatus.RangeCount, expectedStoreStatus.RangeCount; a != e {
		t.Errorf("%d: actual RangeCount does not match expected.\nexpected: %d\nactual: %d", testNumber, e, a)
	}
	if a, e := storeStatus.ReplicatedRangeCount, expectedStoreStatus.ReplicatedRangeCount; a != e {
		t.Errorf("%d: actual ReplicatedRangeCount does not match expected.\nexpected: %d\nactual: %d", testNumber, e, a)
	}

	// Values should be >= to expected values.
	if a, e := storeStatus.Stats.LiveBytes, expectedStoreStatus.Stats.LiveBytes; a < e {
		t.Errorf("%d: actual Live Bytes is not greater or equal to expected.\nexpected: %d\nactual: %d", testNumber, e, a)
	}
	if a, e := storeStatus.Stats.KeyBytes, expectedStoreStatus.Stats.KeyBytes; a < e {
		t.Errorf("%d: actual Key Bytes is not greater or equal to expected.\nexpected: %d\nactual: %d", testNumber, e, a)
	}
	if a, e := storeStatus.Stats.ValBytes, expectedStoreStatus.Stats.ValBytes; a < e {
		t.Errorf("%d: actual Val Bytes is not greater or equal to expected.\nexpected: %d\nactual: %d", testNumber, e, a)
	}
	if a, e := storeStatus.Stats.LiveCount, expectedStoreStatus.Stats.LiveCount; a < e {
		t.Errorf("%d: actual Live Count is not greater or equal to expected.\nexpected: %d\nactual: %d", testNumber, e, a)
	}
	if a, e := storeStatus.Stats.KeyCount, expectedStoreStatus.Stats.KeyCount; a < e {
		t.Errorf("%d: actual Key Count is not greater or equal to expected.\nexpected: %d\nactual: %d", testNumber, e, a)
	}
	if a, e := storeStatus.Stats.ValCount, expectedStoreStatus.Stats.ValCount; a < e {
		t.Errorf("%d: actual Val Count is not greater or equal to expected.\nexpected: %d\nactual: %d", testNumber, e, a)
	}
	return storeStatus
}

// TestStatusSummaries verifies that status summaries are written correctly for
// both the Node and stores within the node.
func TestStatusSummaries(t *testing.T) {
	defer leaktest.AfterTest(t)
	ts := &TestServer{}
	ts.Ctx = NewTestContext()
	ts.StoresPerNode = 3
	if err := ts.Start(); err != nil {
		t.Fatal(err)
	}
	defer ts.Stop()

	var store *storage.Store
	// Retrieve the first store from the Node.
	if s, err := ts.node.lSender.GetStore(proto.StoreID(1)); err == nil {
		store = s
	} else {
		t.Fatal(err)
	}

	store.WaitForInit()
	// Perform a read from the range to ensure that the raft election has
	// completed.  We do not expect a response.
	if _, err := ts.db.Get("a"); err != nil {
		t.Fatal(err)
	}

	storeDesc, err := store.Descriptor()
	if err != nil {
		t.Fatal(err)
	}

	expectedNodeStatus := &status.NodeStatus{
		RangeCount:           1,
		StoreIDs:             []proto.StoreID{1, 2, 3},
		StartedAt:            0,
		UpdatedAt:            0,
		Desc:                 ts.node.Descriptor,
		LeaderRangeCount:     1,
		AvailableRangeCount:  1,
		ReplicatedRangeCount: 0,
		Stats: engine.MVCCStats{
			LiveBytes: 1,
			KeyBytes:  1,
			ValBytes:  1,
			LiveCount: 1,
			KeyCount:  1,
			ValCount:  1,
		},
	}
	expectedStoreStatus := &storage.StoreStatus{
		Desc:                 *storeDesc,
		NodeID:               1,
		RangeCount:           1,
		LeaderRangeCount:     1,
		AvailableRangeCount:  1,
		ReplicatedRangeCount: 0,
		Stats: engine.MVCCStats{
			LiveBytes: 1,
			KeyBytes:  1,
			ValBytes:  1,
			LiveCount: 1,
			KeyCount:  1,
			ValCount:  1,
		},
	}

	// Function to ensure that the event feed has been fully flushed.
	syncFeed := func() {
		syncEvent := status.NewTestSyncEvent(1)
		ts.EventFeed().Publish(syncEvent)
		if err := syncEvent.Sync(5 * time.Second); err != nil {
			t.Fatal(err)
		}
	}

	// Function to force summaries to be written synchronously, including all
	// data currently in the event pipeline. Only one of the stores has
	// replicas, so there are no concerns related to quorum writes; if there
	// were multiple replicas, more care would need to be taken in the initial
	// syncFeed().
	forceWriteStatus := func() {
		syncFeed()
		if err := ts.node.publishStoreStatuses(); err != nil {
			t.Fatalf("error publishing store statuses: %s", err)
		}
		syncFeed()
		if err := ts.writeSummaries(); err != nil {
			t.Fatalf("error writing summaries: %s", err)
		}
	}

	forceWriteStatus()
	oldNodeStats := compareNodeStatus(t, ts, expectedNodeStatus, 0)
	oldStoreStats := compareStoreStatus(t, ts, store, expectedStoreStatus, 0)

	// Write some values left and right of the proposed split key.
	content := proto.Key("test content")
	if err := ts.db.Put("a", content); err != nil {
		t.Fatal(err)
	}
	if err := ts.db.Put("c", content); err != nil {
		t.Fatal(err)
	}

	expectedNodeStatus = &status.NodeStatus{
		RangeCount:           1,
		StoreIDs:             []proto.StoreID{1, 2, 3},
		StartedAt:            oldNodeStats.StartedAt,
		UpdatedAt:            oldNodeStats.UpdatedAt,
		Desc:                 ts.node.Descriptor,
		LeaderRangeCount:     1,
		AvailableRangeCount:  1,
		ReplicatedRangeCount: 0,
		Stats: engine.MVCCStats{
			LiveBytes: 1,
			KeyBytes:  1,
			ValBytes:  1,
			LiveCount: oldNodeStats.Stats.LiveCount + 1,
			KeyCount:  oldNodeStats.Stats.KeyCount + 1,
			ValCount:  oldNodeStats.Stats.ValCount + 1,
		},
	}
	expectedStoreStatus = &storage.StoreStatus{
		Desc:                 oldStoreStats.Desc,
		NodeID:               1,
		RangeCount:           1,
		LeaderRangeCount:     1,
		AvailableRangeCount:  1,
		ReplicatedRangeCount: 0,
		Stats: engine.MVCCStats{
			LiveBytes: 1,
			KeyBytes:  1,
			ValBytes:  1,
			LiveCount: oldStoreStats.Stats.LiveCount + 1,
			KeyCount:  oldStoreStats.Stats.KeyCount + 1,
			ValCount:  oldStoreStats.Stats.ValCount + 1,
		},
	}

	forceWriteStatus()
	oldNodeStats = compareNodeStatus(t, ts, expectedNodeStatus, 1)
	oldStoreStats = compareStoreStatus(t, ts, store, expectedStoreStatus, 1)

	// Split the range.
	splitKey := proto.Key("b")
	rng := store.LookupRange(splitKey, nil)
	args := &proto.AdminSplitRequest{
		RequestHeader: proto.RequestHeader{
			Key:     proto.KeyMin,
			RaftID:  rng.Desc().RaftID,
			Replica: proto.Replica{StoreID: store.Ident.StoreID},
		},
		SplitKey: splitKey,
	}
	if replyI, err := ts.node.executeCmd(args); err != nil {
		t.Fatal(err)
	} else if reply := replyI.(*proto.AdminSplitResponse); reply.Error != nil {
		t.Fatal(reply.Error)
	}

	expectedNodeStatus = &status.NodeStatus{
		RangeCount:           2,
		StoreIDs:             []proto.StoreID{1, 2, 3},
		StartedAt:            oldNodeStats.StartedAt,
		UpdatedAt:            oldNodeStats.UpdatedAt,
		Desc:                 ts.node.Descriptor,
		LeaderRangeCount:     2,
		AvailableRangeCount:  2,
		ReplicatedRangeCount: 0,
		Stats: engine.MVCCStats{
			LiveBytes: 1,
			KeyBytes:  1,
			ValBytes:  1,
			LiveCount: oldNodeStats.Stats.LiveCount,
			KeyCount:  oldNodeStats.Stats.KeyCount,
			ValCount:  oldNodeStats.Stats.ValCount,
		},
	}
	expectedStoreStatus = &storage.StoreStatus{
		Desc:                 oldStoreStats.Desc,
		NodeID:               1,
		RangeCount:           2,
		LeaderRangeCount:     2,
		AvailableRangeCount:  2,
		ReplicatedRangeCount: 0,
		Stats: engine.MVCCStats{
			LiveBytes: 1,
			KeyBytes:  1,
			ValBytes:  1,
			LiveCount: oldStoreStats.Stats.LiveCount,
			KeyCount:  oldStoreStats.Stats.KeyCount,
			ValCount:  oldStoreStats.Stats.ValCount,
		},
	}
	forceWriteStatus()
	oldNodeStats = compareNodeStatus(t, ts, expectedNodeStatus, 2)
	oldStoreStats = compareStoreStatus(t, ts, store, expectedStoreStatus, 2)

	// Write some values left and right of the proposed split key.
	if err := ts.db.Put("aa", content); err != nil {
		t.Fatal(err)
	}
	if err := ts.db.Put("cc", content); err != nil {
		t.Fatal(err)
	}

	expectedNodeStatus = &status.NodeStatus{
		RangeCount:           2,
		StoreIDs:             []proto.StoreID{1, 2, 3},
		StartedAt:            oldNodeStats.StartedAt,
		UpdatedAt:            oldNodeStats.UpdatedAt,
		Desc:                 ts.node.Descriptor,
		LeaderRangeCount:     2,
		AvailableRangeCount:  2,
		ReplicatedRangeCount: 0,
		Stats: engine.MVCCStats{
			LiveBytes: 1,
			KeyBytes:  1,
			ValBytes:  1,
			LiveCount: oldNodeStats.Stats.LiveCount + 1,
			KeyCount:  oldNodeStats.Stats.KeyCount + 1,
			ValCount:  oldNodeStats.Stats.ValCount + 1,
		},
	}
	expectedStoreStatus = &storage.StoreStatus{
		Desc:                 oldStoreStats.Desc,
		NodeID:               1,
		RangeCount:           2,
		LeaderRangeCount:     2,
		AvailableRangeCount:  2,
		ReplicatedRangeCount: 0,
		Stats: engine.MVCCStats{
			LiveBytes: 1,
			KeyBytes:  1,
			ValBytes:  1,
			LiveCount: oldStoreStats.Stats.LiveCount + 1,
			KeyCount:  oldStoreStats.Stats.KeyCount + 1,
			ValCount:  oldStoreStats.Stats.ValCount + 1,
		},
	}
	forceWriteStatus()
	compareNodeStatus(t, ts, expectedNodeStatus, 3)
	compareStoreStatus(t, ts, store, expectedStoreStatus, 3)
}
