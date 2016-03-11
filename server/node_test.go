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

	"google.golang.org/grpc"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/gossip/resolver"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/kv"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/server/status"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/testutils"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/metric"
	"github.com/cockroachdb/cockroach/util/stop"
	"github.com/cockroachdb/cockroach/util/tracing"
	"github.com/cockroachdb/cockroach/util/uuid"
)

// createTestNode creates an rpc server using the specified address,
// gossip instance, KV database and a node using the specified slice
// of engines. The server, clock and node are returned. If gossipBS is
// not nil, the gossip bootstrap address is set to gossipBS.
func createTestNode(addr net.Addr, engines []engine.Engine, gossipBS net.Addr, t *testing.T) (
	*grpc.Server, net.Addr, *hlc.Clock, *Node, *stop.Stopper) {
	ctx := storage.StoreContext{}

	stopper := stop.NewStopper()
	ctx.Clock = hlc.NewClock(hlc.UnixNano)
	nodeRPCContext := rpc.NewContext(nodeTestBaseContext, ctx.Clock, stopper)
	ctx.ScanInterval = 10 * time.Hour
	ctx.ConsistencyCheckInterval = 10 * time.Hour
	grpcServer := rpc.NewServer(nodeRPCContext)
	ln, err := util.ListenAndServeGRPC(stopper, grpcServer, addr)
	if err != nil {
		t.Fatal(err)
	}
	g := gossip.New(nodeRPCContext, testContext.GossipBootstrapResolvers, stopper)
	if gossipBS != nil {
		// Handle possibility of a :0 port specification.
		if gossipBS.Network() == addr.Network() && gossipBS.String() == addr.String() {
			gossipBS = ln.Addr()
		}
		r, err := resolver.NewResolverFromAddress(gossipBS)
		if err != nil {
			t.Fatalf("bad gossip address %s: %s", gossipBS, err)
		}
		g.SetResolvers([]resolver.Resolver{r})
		g.Start(grpcServer, ln.Addr())
	}
	ctx.Gossip = g
	retryOpts := kv.GetDefaultDistSenderRetryOptions()
	retryOpts.Closer = stopper.ShouldDrain()
	distSender := kv.NewDistSender(&kv.DistSenderContext{
		Clock:           ctx.Clock,
		RPCContext:      nodeRPCContext,
		RPCRetryOptions: &retryOpts,
	}, g)
	tracer := tracing.NewTracer()
	sender := kv.NewTxnCoordSender(distSender, ctx.Clock, false, tracer, stopper,
		kv.NewTxnMetrics(metric.NewRegistry()))
	ctx.DB = client.NewDB(sender)
	ctx.Transport = storage.NewDummyRaftTransport()
	ctx.Tracer = tracer
	node := NewNode(ctx, status.NewMetricsRecorder(ctx.Clock), stopper, kv.NewTxnMetrics(metric.NewRegistry()))
	roachpb.RegisterInternalServer(grpcServer, node)
	return grpcServer, ln.Addr(), ctx.Clock, node, stopper
}

// createAndStartTestNode creates a new test node and starts it. The server and node are returned.
func createAndStartTestNode(addr net.Addr, engines []engine.Engine, gossipBS net.Addr, t *testing.T) (
	*grpc.Server, net.Addr, *Node, *stop.Stopper) {
	grpcServer, addr, _, node, stopper := createTestNode(addr, engines, gossipBS, t)
	if err := node.start(addr, engines, roachpb.Attributes{}); err != nil {
		t.Fatal(err)
	}
	return grpcServer, addr, node, stopper
}

func formatKeys(keys []roachpb.Key) string {
	var buf bytes.Buffer
	for i, key := range keys {
		fmt.Fprintf(&buf, "%d: %s\n", i, key)
	}
	return buf.String()
}

// keySlice implements sort.Interface.
type keySlice []roachpb.Key

func (s keySlice) Len() int           { return len(s) }
func (s keySlice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s keySlice) Less(i, j int) bool { return bytes.Compare(s[i], s[j]) < 0 }

// TestBootstrapCluster verifies the results of bootstrapping a
// cluster. Uses an in memory engine.
func TestBootstrapCluster(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop()
	e := engine.NewInMem(roachpb.Attributes{}, 1<<20, stopper)
	if _, err := bootstrapCluster([]engine.Engine{e}, kv.NewTxnMetrics(metric.NewRegistry())); err != nil {
		t.Fatal(err)
	}

	// Scan the complete contents of the local database directly from the engine.
	rows, _, err := engine.MVCCScan(e, keys.LocalMax, roachpb.KeyMax, 0, roachpb.MaxTimestamp, true, nil)
	if err != nil {
		t.Fatal(err)
	}
	var foundKeys keySlice
	for _, kv := range rows {
		foundKeys = append(foundKeys, kv.Key)
	}
	var expectedKeys = keySlice{
		testutils.MakeKey(roachpb.Key("\x02"), roachpb.KeyMax),
		testutils.MakeKey(roachpb.Key("\x03"), roachpb.KeyMax),
		roachpb.Key("\x04node-idgen"),
		roachpb.Key("\x04range-tree-root"),
		roachpb.Key("\x04store-idgen"),
	}
	// Add the initial keys for sql.
	for _, kv := range GetBootstrapSchema().GetInitialValues() {
		expectedKeys = append(expectedKeys, kv.Key)
	}
	// Resort the list. The sql values are not sorted.
	sort.Sort(expectedKeys)

	if !reflect.DeepEqual(foundKeys, expectedKeys) {
		t.Errorf("expected keys mismatch:\n%s\n  -- vs. -- \n\n%s",
			formatKeys(foundKeys), formatKeys(expectedKeys))
	}

	// TODO(spencer): check values.
}

// TestBootstrapNewStore starts a cluster with two unbootstrapped
// stores and verifies both stores are added and started.
func TestBootstrapNewStore(t *testing.T) {
	defer leaktest.AfterTest(t)()
	engineStopper := stop.NewStopper()
	defer engineStopper.Stop()
	e := engine.NewInMem(roachpb.Attributes{}, 1<<20, engineStopper)
	if _, err := bootstrapCluster([]engine.Engine{e}, kv.NewTxnMetrics(metric.NewRegistry())); err != nil {
		t.Fatal(err)
	}

	// Start a new node with two new stores which will require bootstrapping.
	engines := []engine.Engine{
		e,
		engine.NewInMem(roachpb.Attributes{}, 1<<20, engineStopper),
		engine.NewInMem(roachpb.Attributes{}, 1<<20, engineStopper),
	}
	_, _, node, stopper := createAndStartTestNode(util.TestAddr, engines, util.TestAddr, t)
	defer stopper.Stop()

	// Non-initialized stores (in this case the new in-memory-based
	// store) will be bootstrapped by the node upon start. This happens
	// in a goroutine, so we'll have to wait a bit until we can find the
	// new node.
	util.SucceedsSoon(t, func() error {
		if n := node.stores.GetStoreCount(); n != 3 {
			return util.Errorf("expected 3 stores but got %d", n)
		}
		return nil
	})

	// Check whether all stores are started properly.
	if err := node.stores.VisitStores(func(s *storage.Store) error {
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
	defer leaktest.AfterTest(t)()
	engineStopper := stop.NewStopper()
	defer engineStopper.Stop()
	e := engine.NewInMem(roachpb.Attributes{}, 1<<20, engineStopper)
	if _, err := bootstrapCluster([]engine.Engine{e}, kv.NewTxnMetrics(metric.NewRegistry())); err != nil {
		t.Fatal(err)
	}

	// Start the bootstrap node.
	engines1 := []engine.Engine{e}
	_, server1Addr, node1, stopper1 := createAndStartTestNode(util.TestAddr, engines1, util.TestAddr, t)
	defer stopper1.Stop()

	// Create a new node.
	engines2 := []engine.Engine{engine.NewInMem(roachpb.Attributes{}, 1<<20, engineStopper)}
	_, server2Addr, node2, stopper2 := createAndStartTestNode(util.TestAddr, engines2, server1Addr, t)
	defer stopper2.Stop()

	// Verify new node is able to bootstrap its store.
	util.SucceedsSoon(t, func() error {
		if sc := node2.stores.GetStoreCount(); sc != 1 {
			return util.Errorf("GetStoreCount() expected 1; got %d", sc)
		}
		return nil
	})

	// Verify node1 sees node2 via gossip and vice versa.
	node1Key := gossip.MakeNodeIDKey(node1.Descriptor.NodeID)
	node2Key := gossip.MakeNodeIDKey(node2.Descriptor.NodeID)
	util.SucceedsSoon(t, func() error {
		var nodeDesc1 roachpb.NodeDescriptor
		if err := node1.ctx.Gossip.GetInfoProto(node2Key, &nodeDesc1); err != nil {
			return err
		}
		if addr2Str, server2AddrStr := nodeDesc1.Address.String(), server2Addr.String(); addr2Str != server2AddrStr {
			return util.Errorf("addr2 gossip %s doesn't match addr2 address %s", addr2Str, server2AddrStr)
		}
		var nodeDesc2 roachpb.NodeDescriptor
		if err := node2.ctx.Gossip.GetInfoProto(node1Key, &nodeDesc2); err != nil {
			return err
		}
		if addr1Str, server1AddrStr := nodeDesc2.Address.String(), server1Addr.String(); addr1Str != server1AddrStr {
			return util.Errorf("addr1 gossip %s doesn't match addr1 address %s", addr1Str, server1AddrStr)
		}
		return nil
	})
}

// TestNodeJoinSelf verifies that an uninitialized node trying to join
// itself will fail.
func TestNodeJoinSelf(t *testing.T) {
	defer leaktest.AfterTest(t)()

	engineStopper := stop.NewStopper()
	defer engineStopper.Stop()
	engines := []engine.Engine{engine.NewInMem(roachpb.Attributes{}, 1<<20, engineStopper)}
	_, addr, _, node, stopper := createTestNode(util.TestAddr, engines, util.TestAddr, t)
	defer stopper.Stop()
	err := node.start(addr, engines, roachpb.Attributes{})
	if err != errCannotJoinSelf {
		t.Fatalf("expected err %s; got %s", errCannotJoinSelf, err)
	}
}

// TestCorruptedClusterID verifies that a node fails to start when a
// store's cluster ID is empty.
func TestCorruptedClusterID(t *testing.T) {
	defer leaktest.AfterTest(t)()
	engineStopper := stop.NewStopper()
	e := engine.NewInMem(roachpb.Attributes{}, 1<<20, engineStopper)
	defer engineStopper.Stop()
	if _, err := bootstrapCluster([]engine.Engine{e}, kv.NewTxnMetrics(metric.NewRegistry())); err != nil {
		t.Fatal(err)
	}

	// Set the cluster ID to the empty UUID.
	sIdent := roachpb.StoreIdent{
		ClusterID: *uuid.EmptyUUID,
		NodeID:    1,
		StoreID:   1,
	}
	if err := engine.MVCCPutProto(e, nil, keys.StoreIdentKey(), roachpb.ZeroTimestamp, nil, &sIdent); err != nil {
		t.Fatal(err)
	}

	engines := []engine.Engine{e}
	_, serverAddr, _, node, stopper := createTestNode(util.TestAddr, engines, nil, t)
	stopper.Stop()
	if err := node.start(serverAddr, engines, roachpb.Attributes{}); !testutils.IsError(err, "unidentified store") {
		t.Errorf("unexpected error %v", err)
	}
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
	defer leaktest.AfterTest(t)()
	ts := &TestServer{}
	ts.Ctx = NewTestContext()
	ts.StoresPerNode = 3
	if err := ts.Start(); err != nil {
		t.Fatal(err)
	}
	defer ts.Stop()

	// Retrieve the first store from the Node.
	s, err := ts.node.stores.GetStore(roachpb.StoreID(1))
	if err != nil {
		t.Fatal(err)
	}

	s.WaitForInit()

	content := "junk"
	leftKey := "a"

	// Scan over all keys to "wake up" all replicas (force a leader election).
	if _, err := ts.db.Scan(keys.MetaMax, keys.MaxKey, 0); err != nil {
		t.Fatal(err)
	}

	storeDesc, pErr := s.Descriptor()
	if pErr != nil {
		t.Fatal(pErr)
	}

	// Wait for full replication of initial ranges.
	initialRanges := int32(ExpectedInitialRangeCount())
	util.SucceedsSoon(t, func() error {
		for i := 1; i <= int(initialRanges); i++ {
			if s.RaftStatus(roachpb.RangeID(i)) == nil {
				return util.Errorf("Store %d replica %d is not present in raft", s.StoreID(), i)
			}
		}
		return nil
	})

	expectedNodeStatus := &status.NodeStatus{
		RangeCount:           initialRanges,
		StoreIDs:             []roachpb.StoreID{1, 2, 3},
		StartedAt:            0,
		UpdatedAt:            0,
		Desc:                 ts.node.Descriptor,
		LeaderRangeCount:     initialRanges,
		AvailableRangeCount:  initialRanges,
		ReplicatedRangeCount: initialRanges,
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
		RangeCount:           initialRanges,
		LeaderRangeCount:     initialRanges,
		AvailableRangeCount:  initialRanges,
		ReplicatedRangeCount: initialRanges,
		Stats: engine.MVCCStats{
			LiveBytes: 1,
			KeyBytes:  1,
			ValBytes:  1,
			LiveCount: 1,
			KeyCount:  1,
			ValCount:  1,
		},
	}

	// Function to force summaries to be written synchronously, including all
	// data currently in the event pipeline. Only one of the stores has
	// replicas, so there are no concerns related to quorum writes; if there
	// were multiple replicas, more care would need to be taken in the initial
	// syncFeed().
	forceWriteStatus := func() {
		if err := ts.node.computePeriodicMetrics(); err != nil {
			t.Fatalf("error publishing store statuses: %s", err)
		}

		if err := ts.WriteSummaries(); err != nil {
			t.Fatalf("error writing summaries: %s", err)
		}
	}

	forceWriteStatus()
	oldNodeStats := compareNodeStatus(t, ts, expectedNodeStatus, 0)
	oldStoreStats := compareStoreStatus(t, ts, s, expectedStoreStatus, 0)

	splitKey := "b"
	rightKey := "c"

	// Write some values left and right of the proposed split key. No
	// particular reason.
	if err := ts.db.Put(leftKey, content); err != nil {
		t.Fatal(err)
	}
	if err := ts.db.Put(rightKey, content); err != nil {
		t.Fatal(err)
	}

	expectedNodeStatus = &status.NodeStatus{
		RangeCount:           initialRanges,
		StoreIDs:             []roachpb.StoreID{1, 2, 3},
		StartedAt:            oldNodeStats.StartedAt,
		UpdatedAt:            oldNodeStats.UpdatedAt,
		Desc:                 ts.node.Descriptor,
		LeaderRangeCount:     initialRanges,
		AvailableRangeCount:  initialRanges,
		ReplicatedRangeCount: initialRanges,
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
		RangeCount:           initialRanges,
		LeaderRangeCount:     initialRanges,
		AvailableRangeCount:  initialRanges,
		ReplicatedRangeCount: initialRanges,
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
	oldStoreStats = compareStoreStatus(t, ts, s, expectedStoreStatus, 1)

	// Split the range.
	if err := ts.db.AdminSplit(splitKey); err != nil {
		t.Fatal(err)
	}

	// Write on both sides of the split to ensure that the raft machinery
	// is running.
	if err := ts.db.Put(leftKey, content); err != nil {
		t.Fatal(err)
	}
	if err := ts.db.Put(rightKey, content); err != nil {
		t.Fatal(err)
	}

	expectedNodeStatus = &status.NodeStatus{
		RangeCount:           initialRanges + 1,
		StoreIDs:             []roachpb.StoreID{1, 2, 3},
		StartedAt:            oldNodeStats.StartedAt,
		UpdatedAt:            oldNodeStats.UpdatedAt,
		Desc:                 ts.node.Descriptor,
		LeaderRangeCount:     initialRanges + 1,
		AvailableRangeCount:  initialRanges + 1,
		ReplicatedRangeCount: initialRanges + 1,
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
		RangeCount:           initialRanges + 1,
		LeaderRangeCount:     initialRanges + 1,
		AvailableRangeCount:  initialRanges + 1,
		ReplicatedRangeCount: initialRanges + 1,
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
	compareNodeStatus(t, ts, expectedNodeStatus, 3)
	compareStoreStatus(t, ts, s, expectedStoreStatus, 3)
}
