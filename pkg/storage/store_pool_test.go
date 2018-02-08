// Copyright 2015 The Cockroach Authors.
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

package storage

import (
	"context"
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/kr/pretty"
	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils/gossiputil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

var uniqueStore = []*roachpb.StoreDescriptor{
	{
		StoreID: 2,
		Attrs:   roachpb.Attributes{Attrs: []string{"ssd"}},
		Node: roachpb.NodeDescriptor{
			NodeID: 2,
			Attrs:  roachpb.Attributes{Attrs: []string{"a"}},
		},
		Capacity: roachpb.StoreCapacity{
			Capacity:  100,
			Available: 200,
		},
	},
}

type mockNodeLiveness struct {
	syncutil.Mutex
	defaultNodeStatus NodeLivenessStatus
	nodes             map[roachpb.NodeID]NodeLivenessStatus
}

func newMockNodeLiveness(defaultNodeStatus NodeLivenessStatus) *mockNodeLiveness {
	return &mockNodeLiveness{
		defaultNodeStatus: defaultNodeStatus,
		nodes:             map[roachpb.NodeID]NodeLivenessStatus{},
	}
}

func (m *mockNodeLiveness) setNodeStatus(nodeID roachpb.NodeID, status NodeLivenessStatus) {
	m.Lock()
	defer m.Unlock()
	m.nodes[nodeID] = status
}

func (m *mockNodeLiveness) nodeLivenessFunc(
	nodeID roachpb.NodeID, now time.Time, threshold time.Duration,
) NodeLivenessStatus {
	m.Lock()
	defer m.Unlock()
	if status, ok := m.nodes[nodeID]; ok {
		return status
	}
	return m.defaultNodeStatus
}

// createTestStorePool creates a stopper, gossip and storePool for use in
// tests. Stopper must be stopped by the caller.
func createTestStorePool(
	timeUntilStoreDeadValue time.Duration, deterministic bool, defaultNodeStatus NodeLivenessStatus,
) (*stop.Stopper, *gossip.Gossip, *hlc.ManualClock, *StorePool, *mockNodeLiveness) {
	stopper := stop.NewStopper()
	mc := hlc.NewManualClock(123)
	clock := hlc.NewClock(mc.UnixNano, time.Nanosecond)
	st := cluster.MakeTestingClusterSettings()
	rpcContext := rpc.NewContext(
		log.AmbientContext{Tracer: st.Tracer}, &base.Config{Insecure: true}, clock, stopper,
		&st.Version)
	server := rpc.NewServer(rpcContext) // never started
	g := gossip.NewTest(1, rpcContext, server, stopper, metric.NewRegistry())
	mnl := newMockNodeLiveness(defaultNodeStatus)

	TimeUntilStoreDead.Override(&st.SV, timeUntilStoreDeadValue)
	storePool := NewStorePool(
		log.AmbientContext{Tracer: st.Tracer},
		st,
		g,
		clock,
		mnl.nodeLivenessFunc,
		deterministic,
	)
	return stopper, g, mc, storePool, mnl
}

// TestStorePoolGossipUpdate ensures that the gossip callback in StorePool
// correctly updates a store's details.
func TestStorePoolGossipUpdate(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper, g, _, sp, _ := createTestStorePool(
		TestTimeUntilStoreDead, false /* deterministic */, NodeLivenessStatus_DEAD)
	defer stopper.Stop(context.TODO())
	sg := gossiputil.NewStoreGossiper(g)

	sp.detailsMu.RLock()
	if _, ok := sp.detailsMu.storeDetails[2]; ok {
		t.Fatalf("store 2 is already in the pool's store list")
	}
	sp.detailsMu.RUnlock()

	sg.GossipStores(uniqueStore, t)

	sp.detailsMu.RLock()
	if _, ok := sp.detailsMu.storeDetails[2]; !ok {
		t.Fatalf("store 2 isn't in the pool's store list")
	}
	sp.detailsMu.RUnlock()
}

// verifyStoreList ensures that the returned list of stores is correct.
func verifyStoreList(
	sp *StorePool,
	constraints []config.Constraints,
	rangeID roachpb.RangeID,
	expected []int,
	filter storeFilter,
	expectedAliveStoreCount int,
	expectedThrottledStoreCount int,
) error {
	var actual []int
	sl, aliveStoreCount, throttledStoreCount := sp.getStoreList(rangeID, filter)
	sl = sl.filter(constraints)
	if aliveStoreCount != expectedAliveStoreCount {
		return errors.Errorf("expected AliveStoreCount %d does not match actual %d",
			expectedAliveStoreCount, aliveStoreCount)
	}
	if throttledStoreCount != expectedThrottledStoreCount {
		return errors.Errorf("expected ThrottledStoreCount %d does not match actual %d",
			expectedThrottledStoreCount, throttledStoreCount)
	}
	for _, store := range sl.stores {
		actual = append(actual, int(store.StoreID))
	}
	sort.Ints(expected)
	sort.Ints(actual)
	if !reflect.DeepEqual(expected, actual) {
		return errors.Errorf("expected %+v stores, actual %+v", expected, actual)
	}
	return nil
}

// TestStorePoolGetStoreList ensures that the store list returns only stores
// that are live and match the attribute criteria.
func TestStorePoolGetStoreList(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// We're going to manually mark stores dead in this test.
	stopper, g, _, sp, mnl := createTestStorePool(
		TestTimeUntilStoreDead, false /* deterministic */, NodeLivenessStatus_DEAD)
	defer stopper.Stop(context.TODO())
	sg := gossiputil.NewStoreGossiper(g)
	constraints := []config.Constraints{
		{
			Constraints: []config.Constraint{
				{Type: config.Constraint_REQUIRED, Value: "ssd"},
				{Type: config.Constraint_REQUIRED, Value: "dc"},
			},
		},
	}
	required := []string{"ssd", "dc"}
	// Nothing yet.
	sl, _, _ := sp.getStoreList(roachpb.RangeID(0), storeFilterNone)
	sl = sl.filter(constraints)
	if len(sl.stores) != 0 {
		t.Errorf("expected no stores, instead %+v", sl.stores)
	}

	matchingStore := roachpb.StoreDescriptor{
		StoreID: 1,
		Node:    roachpb.NodeDescriptor{NodeID: 1},
		Attrs:   roachpb.Attributes{Attrs: required},
	}
	supersetStore := roachpb.StoreDescriptor{
		StoreID: 2,
		Node:    roachpb.NodeDescriptor{NodeID: 2},
		Attrs:   roachpb.Attributes{Attrs: append(required, "db")},
	}
	unmatchingStore := roachpb.StoreDescriptor{
		StoreID: 3,
		Node:    roachpb.NodeDescriptor{NodeID: 3},
		Attrs:   roachpb.Attributes{Attrs: []string{"ssd", "otherdc"}},
	}
	emptyStore := roachpb.StoreDescriptor{
		StoreID: 4,
		Node:    roachpb.NodeDescriptor{NodeID: 4},
		Attrs:   roachpb.Attributes{},
	}
	deadStore := roachpb.StoreDescriptor{
		StoreID: 5,
		Node:    roachpb.NodeDescriptor{NodeID: 5},
		Attrs:   roachpb.Attributes{Attrs: required},
	}
	declinedStore := roachpb.StoreDescriptor{
		StoreID: 6,
		Node:    roachpb.NodeDescriptor{NodeID: 6},
		Attrs:   roachpb.Attributes{Attrs: required},
	}
	corruptReplicaStore := roachpb.StoreDescriptor{
		StoreID: 7,
		Node:    roachpb.NodeDescriptor{NodeID: 7},
		Attrs:   roachpb.Attributes{Attrs: required},
	}

	corruptedRangeID := roachpb.RangeID(1)

	// Gossip and mark all alive initially.
	sg.GossipStores([]*roachpb.StoreDescriptor{
		&matchingStore,
		&supersetStore,
		&unmatchingStore,
		&emptyStore,
		&deadStore,
		&declinedStore,
		&corruptReplicaStore,
	}, t)
	for i := 1; i <= 7; i++ {
		mnl.setNodeStatus(roachpb.NodeID(i), NodeLivenessStatus_LIVE)
	}

	// Add some corrupt replicas that should not affect getStoreList().
	sp.detailsMu.Lock()
	sp.detailsMu.storeDetails[matchingStore.StoreID].deadReplicas[roachpb.RangeID(10)] =
		[]roachpb.ReplicaDescriptor{{
			StoreID: matchingStore.StoreID,
			NodeID:  matchingStore.Node.NodeID,
		}}
	sp.detailsMu.storeDetails[matchingStore.StoreID].deadReplicas[roachpb.RangeID(11)] =
		[]roachpb.ReplicaDescriptor{{
			StoreID: matchingStore.StoreID,
			NodeID:  matchingStore.Node.NodeID,
		}}
	sp.detailsMu.storeDetails[corruptReplicaStore.StoreID].deadReplicas[roachpb.RangeID(10)] =
		[]roachpb.ReplicaDescriptor{{
			StoreID: corruptReplicaStore.StoreID,
			NodeID:  corruptReplicaStore.Node.NodeID,
		}}
	sp.detailsMu.Unlock()

	if err := verifyStoreList(
		sp,
		constraints,
		corruptedRangeID,
		[]int{
			int(matchingStore.StoreID),
			int(supersetStore.StoreID),
			int(deadStore.StoreID),
			int(declinedStore.StoreID),
			int(corruptReplicaStore.StoreID),
		},
		storeFilterNone,
		/* expectedAliveStoreCount */ 7,
		/* expectedThrottledStoreCount */ 0,
	); err != nil {
		t.Error(err)
	}

	// Set deadStore as dead.
	mnl.setNodeStatus(deadStore.Node.NodeID, NodeLivenessStatus_DEAD)
	sp.detailsMu.Lock()
	// Set declinedStore as throttled.
	sp.detailsMu.storeDetails[declinedStore.StoreID].throttledUntil = sp.clock.Now().GoTime().Add(time.Hour)
	// Add a corrupt replica to corruptReplicaStore.
	sp.detailsMu.storeDetails[corruptReplicaStore.StoreID].deadReplicas[roachpb.RangeID(1)] =
		[]roachpb.ReplicaDescriptor{{
			StoreID: corruptReplicaStore.StoreID,
			NodeID:  corruptReplicaStore.Node.NodeID,
		}}
	sp.detailsMu.Unlock()

	if err := verifyStoreList(
		sp,
		constraints,
		corruptedRangeID,
		[]int{
			int(matchingStore.StoreID),
			int(supersetStore.StoreID),
			int(declinedStore.StoreID),
		},
		storeFilterNone,
		/* expectedAliveStoreCount */ 6,
		/* expectedThrottledStoreCount */ 1,
	); err != nil {
		t.Error(err)
	}

	if err := verifyStoreList(
		sp,
		constraints,
		corruptedRangeID,
		[]int{
			int(matchingStore.StoreID),
			int(supersetStore.StoreID),
		},
		storeFilterThrottled,
		/* expectedAliveStoreCount */ 6,
		/* expectedThrottledStoreCount */ 1,
	); err != nil {
		t.Error(err)
	}
}

// TestStoreListFilter ensures that the store list constraint filtering works
// properly.
func TestStoreListFilter(t *testing.T) {
	defer leaktest.AfterTest(t)()

	constraints := []config.Constraints{
		{
			Constraints: []config.Constraint{
				{Type: config.Constraint_REQUIRED, Key: "region", Value: "us-west"},
				{Type: config.Constraint_REQUIRED, Value: "MustMatch"},
				{Type: config.Constraint_DEPRECATED_POSITIVE, Value: "MatchingOptional"},
				{Type: config.Constraint_PROHIBITED, Value: "MustNotMatch"},
			},
		},
	}

	stores := []struct {
		attributes []string
		locality   []roachpb.Tier
		expected   bool
	}{
		{
			expected: false,
		},
		{
			attributes: []string{"MustMatch"},
			expected:   false,
		},
		{
			locality: []roachpb.Tier{{Key: "region", Value: "us-west"}},
			expected: false,
		},
		{
			attributes: []string{"MustMatch"},
			locality:   []roachpb.Tier{{Key: "region", Value: "us-west"}},
			expected:   true,
		},
		{
			attributes: []string{"a", "MustMatch"},
			locality:   []roachpb.Tier{{Key: "a", Value: "b"}, {Key: "region", Value: "us-west"}},
			expected:   true,
		},
		{
			attributes: []string{"a", "b", "MustMatch", "c"},
			locality:   []roachpb.Tier{{Key: "region", Value: "us-west"}, {Key: "c", Value: "d"}},
			expected:   true,
		},
		{
			attributes: []string{"MustMatch", "MustNotMatch"},
			locality:   []roachpb.Tier{{Key: "region", Value: "us-west"}},
			expected:   false,
		},
		{
			attributes: []string{"MustMatch"},
			locality:   []roachpb.Tier{{Key: "region", Value: "us-west"}, {Key: "MustNotMatch", Value: "b"}},
			expected:   true,
		},
		{
			attributes: []string{"MustMatch"},
			locality:   []roachpb.Tier{{Key: "region", Value: "us-west"}, {Key: "a", Value: "MustNotMatch"}},
			expected:   true,
		},
	}

	var sl StoreList
	var expected []roachpb.StoreDescriptor
	for i, s := range stores {
		storeDesc := roachpb.StoreDescriptor{
			StoreID: roachpb.StoreID(i + 1),
			Node: roachpb.NodeDescriptor{
				Locality: roachpb.Locality{
					Tiers: s.locality,
				},
			},
		}
		// Randomly stick the attributes in either the node or the store to get
		// code coverage of both locations.
		if rand.Intn(2) == 0 {
			storeDesc.Attrs.Attrs = s.attributes
		} else {
			storeDesc.Node.Attrs.Attrs = s.attributes
		}
		sl.stores = append(sl.stores, storeDesc)
		if s.expected {
			expected = append(expected, storeDesc)
		}
	}

	filtered := sl.filter(constraints)
	if !reflect.DeepEqual(expected, filtered.stores) {
		t.Errorf("did not get expected stores %s", pretty.Diff(expected, filtered.stores))
	}
}

func TestStorePoolUpdateLocalStore(t *testing.T) {
	defer leaktest.AfterTest(t)()
	manual := hlc.NewManualClock(123)
	clock := hlc.NewClock(manual.UnixNano, time.Nanosecond)
	// We're going to manually mark stores dead in this test.
	stopper, g, _, sp, _ := createTestStorePool(
		TestTimeUntilStoreDead, false /* deterministic */, NodeLivenessStatus_DEAD)
	defer stopper.Stop(context.TODO())
	sg := gossiputil.NewStoreGossiper(g)
	stores := []*roachpb.StoreDescriptor{
		{
			StoreID: 1,
			Node:    roachpb.NodeDescriptor{NodeID: 1},
			Capacity: roachpb.StoreCapacity{
				Capacity:        100,
				Available:       50,
				RangeCount:      5,
				LogicalBytes:    30,
				WritesPerSecond: 30,
			},
		},
		{
			StoreID: 2,
			Node:    roachpb.NodeDescriptor{NodeID: 2},
			Capacity: roachpb.StoreCapacity{
				Capacity:        100,
				Available:       55,
				RangeCount:      4,
				LogicalBytes:    25,
				WritesPerSecond: 25,
			},
		},
	}
	sg.GossipStores(stores, t)

	replica := &Replica{RangeID: 1}
	replica.mu.Lock()
	replica.mu.state.Stats = &enginepb.MVCCStats{
		KeyBytes: 2,
		ValBytes: 4,
	}
	replica.mu.Unlock()
	rs := newReplicaStats(clock, nil)
	for _, store := range stores {
		rs.record(store.Node.NodeID)
	}
	manual.Increment(int64(MinStatsDuration + time.Second))
	replica.writeStats = rs

	rangeDesc := &roachpb.RangeDescriptor{
		RangeID: replica.RangeID,
	}

	rangeInfo := rangeInfoForRepl(replica, rangeDesc)

	sp.updateLocalStoreAfterRebalance(roachpb.StoreID(1), rangeInfo, roachpb.ADD_REPLICA)
	desc, ok := sp.getStoreDescriptor(roachpb.StoreID(1))
	if !ok {
		t.Fatalf("couldn't find StoreDescriptor for Store ID %d", 1)
	}
	QPS, _ := replica.writeStats.avgQPS()
	if expectedBytes, expectedQPS := int64(36), 30+QPS; desc.Capacity.LogicalBytes != expectedBytes || desc.Capacity.WritesPerSecond != expectedQPS {
		t.Fatalf("expected Logical bytes %d, but got %d, expected WritesPerSecond %f, but got %f",
			expectedBytes, desc.Capacity.LogicalBytes, expectedQPS, desc.Capacity.WritesPerSecond)
	}

	sp.updateLocalStoreAfterRebalance(roachpb.StoreID(2), rangeInfo, roachpb.REMOVE_REPLICA)
	desc, ok = sp.getStoreDescriptor(roachpb.StoreID(2))
	if !ok {
		t.Fatalf("couldn't find StoreDescriptor for Store ID %d", 2)
	}
	if expectedBytes, expectedQPS := int64(19), 25-QPS; desc.Capacity.LogicalBytes != expectedBytes || desc.Capacity.WritesPerSecond != expectedQPS {
		t.Fatalf("expected Logical bytes %d, but got %d, expected WritesPerSecond %f, but got %f",
			expectedBytes, desc.Capacity.LogicalBytes, expectedQPS, desc.Capacity.WritesPerSecond)
	}
}

// TestStorePoolUpdateLocalStoreBeforeGossip verifies that an attempt to update
// the local copy of store before that store has been gossiped will be a no-op.
func TestStorePoolUpdateLocalStoreBeforeGossip(t *testing.T) {
	defer leaktest.AfterTest(t)()
	manual := hlc.NewManualClock(123)
	clock := hlc.NewClock(manual.UnixNano, time.Nanosecond)
	stopper, _, _, sp, _ := createTestStorePool(
		TestTimeUntilStoreDead, false /* deterministic */, NodeLivenessStatus_DEAD)
	defer stopper.Stop(context.TODO())

	// Create store.
	node := roachpb.NodeDescriptor{NodeID: roachpb.NodeID(1)}
	eng := engine.NewInMem(roachpb.Attributes{}, 1<<20)
	stopper.AddCloser(eng)
	cfg := TestStoreConfig(clock)
	cfg.Transport = NewDummyRaftTransport(cfg.Settings)
	store := NewStore(cfg, eng, &node)

	// Create replica.
	rg := roachpb.RangeDescriptor{
		RangeID:  1,
		StartKey: roachpb.RKey([]byte("a")),
		EndKey:   roachpb.RKey([]byte("b")),
	}
	replica, err := NewReplica(&rg, store, roachpb.ReplicaID(0))
	if err != nil {
		t.Fatalf("make replica error : %s", err)
	}

	rangeInfo := rangeInfoForRepl(replica, &rg)

	// Update StorePool, which should be a no-op.
	storeID := roachpb.StoreID(1)
	if _, ok := sp.getStoreDescriptor(storeID); ok {
		t.Fatalf("StoreDescriptor not gossiped, should not be found")
	}
	sp.updateLocalStoreAfterRebalance(storeID, rangeInfo, roachpb.ADD_REPLICA)
	if _, ok := sp.getStoreDescriptor(storeID); ok {
		t.Fatalf("StoreDescriptor still not gossiped, should not be found")
	}
}

func TestStorePoolGetStoreDetails(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper, g, _, sp, _ := createTestStorePool(
		TestTimeUntilStoreDead, false /* deterministic */, NodeLivenessStatus_DEAD)
	defer stopper.Stop(context.TODO())
	sg := gossiputil.NewStoreGossiper(g)
	sg.GossipStores(uniqueStore, t)

	sp.detailsMu.Lock()
	defer sp.detailsMu.Unlock()
	if detail := sp.getStoreDetailLocked(roachpb.StoreID(1)); detail.desc != nil {
		t.Errorf("unexpected fetched store ID 1: %+v", detail.desc)
	}
	if detail := sp.getStoreDetailLocked(roachpb.StoreID(2)); detail.desc == nil {
		t.Errorf("failed to fetch store ID 2")
	}
}

func TestStorePoolFindDeadReplicas(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper, g, _, sp, mnl := createTestStorePool(
		TestTimeUntilStoreDead, false /* deterministic */, NodeLivenessStatus_DEAD)
	defer stopper.Stop(context.TODO())
	sg := gossiputil.NewStoreGossiper(g)

	stores := []*roachpb.StoreDescriptor{
		{
			StoreID: 1,
			Node:    roachpb.NodeDescriptor{NodeID: 1},
		},
		{
			StoreID: 2,
			Node:    roachpb.NodeDescriptor{NodeID: 2},
		},
		{
			StoreID: 3,
			Node:    roachpb.NodeDescriptor{NodeID: 3},
		},
		{
			StoreID: 4,
			Node:    roachpb.NodeDescriptor{NodeID: 4},
		},
		{
			StoreID: 5,
			Node:    roachpb.NodeDescriptor{NodeID: 5},
		},
	}

	replicas := []roachpb.ReplicaDescriptor{
		{
			NodeID:    1,
			StoreID:   1,
			ReplicaID: 1,
		},
		{
			NodeID:    2,
			StoreID:   2,
			ReplicaID: 2,
		},
		{
			NodeID:    3,
			StoreID:   3,
			ReplicaID: 4,
		},
		{
			NodeID:    4,
			StoreID:   4,
			ReplicaID: 4,
		},
		{
			NodeID:    5,
			StoreID:   5,
			ReplicaID: 5,
		},
	}

	sg.GossipStores(stores, t)
	for i := 1; i <= 5; i++ {
		mnl.setNodeStatus(roachpb.NodeID(i), NodeLivenessStatus_LIVE)
	}

	liveReplicas, deadReplicas := sp.liveAndDeadReplicas(0, replicas)
	if len(liveReplicas) != 5 {
		t.Fatalf("expected five live replicas, found %d (%v)", len(liveReplicas), liveReplicas)
	}
	if len(deadReplicas) > 0 {
		t.Fatalf("expected no dead replicas initially, found %d (%v)", len(deadReplicas), deadReplicas)
	}
	// Mark nodes 4 & 5 as dead.
	mnl.setNodeStatus(4, NodeLivenessStatus_DEAD)
	mnl.setNodeStatus(5, NodeLivenessStatus_DEAD)

	liveReplicas, deadReplicas = sp.liveAndDeadReplicas(0, replicas)
	if a, e := liveReplicas, replicas[:3]; !reflect.DeepEqual(a, e) {
		t.Fatalf("expected live replicas %+v; got %+v", e, a)
	}
	if a, e := deadReplicas, replicas[3:]; !reflect.DeepEqual(a, e) {
		t.Fatalf("expected dead replicas %+v; got %+v", e, a)
	}

	// Mark node 4 as merely unavailable.
	mnl.setNodeStatus(4, NodeLivenessStatus_UNAVAILABLE)

	liveReplicas, deadReplicas = sp.liveAndDeadReplicas(0, replicas)
	if a, e := liveReplicas, replicas[:3]; !reflect.DeepEqual(a, e) {
		t.Fatalf("expected live replicas %+v; got %+v", e, a)
	}
	if a, e := deadReplicas, replicas[4:]; !reflect.DeepEqual(a, e) {
		t.Fatalf("expected dead replicas %+v; got %+v", e, a)
	}
}

// TestStorePoolDefaultState verifies that the default state of a
// store is neither alive nor dead. This is a regression test for a
// bug in which a call to deadReplicas involving an unknown store
// would have the side effect of marking that store as alive and
// eligible for return by getStoreList. It is therefore significant
// that the two methods are tested in the same test, and in this
// order.
func TestStorePoolDefaultState(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper, _, _, sp, _ := createTestStorePool(
		TestTimeUntilStoreDead, false /* deterministic */, NodeLivenessStatus_DEAD)
	defer stopper.Stop(context.TODO())

	liveReplicas, deadReplicas := sp.liveAndDeadReplicas(0, []roachpb.ReplicaDescriptor{{StoreID: 1}})
	if len(liveReplicas) != 0 || len(deadReplicas) != 0 {
		t.Errorf("expected 0 live and 0 dead replicas; got %v and %v", liveReplicas, deadReplicas)
	}

	sl, alive, throttled := sp.getStoreList(roachpb.RangeID(0), storeFilterNone)
	if len(sl.stores) > 0 {
		t.Errorf("expected no live stores; got list of %v", sl)
	}
	if alive != 0 {
		t.Errorf("expected no live stores; got a live count of %d", alive)
	}
	if throttled != 0 {
		t.Errorf("expected no live stores; got a throttled count of %d", throttled)
	}
}

func TestStorePoolThrottle(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper, g, _, sp, _ := createTestStorePool(
		TestTimeUntilStoreDead, false /* deterministic */, NodeLivenessStatus_DEAD)
	defer stopper.Stop(context.TODO())

	sg := gossiputil.NewStoreGossiper(g)
	sg.GossipStores(uniqueStore, t)

	{
		expected := sp.clock.Now().GoTime().Add(declinedReservationsTimeout.Get(&sp.st.SV))
		sp.throttle(throttleDeclined, 1)

		sp.detailsMu.Lock()
		detail := sp.getStoreDetailLocked(1)
		sp.detailsMu.Unlock()
		if !detail.throttledUntil.Equal(expected) {
			t.Errorf("expected store to have been throttled to %v, found %v",
				expected, detail.throttledUntil)
		}
	}

	{
		expected := sp.clock.Now().GoTime().Add(failedReservationsTimeout.Get(&sp.st.SV))
		sp.throttle(throttleFailed, 1)

		sp.detailsMu.Lock()
		detail := sp.getStoreDetailLocked(1)
		sp.detailsMu.Unlock()
		if !detail.throttledUntil.Equal(expected) {
			t.Errorf("expected store to have been throttled to %v, found %v",
				expected, detail.throttledUntil)
		}
	}
}

func TestGetLocalities(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper, g, _, sp, _ := createTestStorePool(
		TestTimeUntilStoreDead, false /* deterministic */, NodeLivenessStatus_DEAD)
	defer stopper.Stop(context.TODO())
	sg := gossiputil.NewStoreGossiper(g)

	// Creates a node with a locality with the number of tiers passed in. The
	// NodeID is the same as the tier count.
	createLocality := func(tierCount int) roachpb.Locality {
		var locality roachpb.Locality
		for i := 1; i <= tierCount; i++ {
			value := fmt.Sprintf("%d", i)
			locality.Tiers = append(locality.Tiers, roachpb.Tier{
				Key:   value,
				Value: value,
			})
		}
		return locality
	}
	createDescWithLocality := func(tierCount int) roachpb.NodeDescriptor {
		return roachpb.NodeDescriptor{
			NodeID:   roachpb.NodeID(tierCount),
			Locality: createLocality(tierCount),
		}
	}

	stores := []*roachpb.StoreDescriptor{
		{
			StoreID: 1,
			Node:    createDescWithLocality(1),
		},
		{
			StoreID: 2,
			Node:    createDescWithLocality(2),
		},
		{
			StoreID: 3,
			Node:    createDescWithLocality(3),
		},
		{
			StoreID: 4,
			Node:    createDescWithLocality(2),
		},
	}

	sg.GossipStores(stores, t)

	var existingReplicas []roachpb.ReplicaDescriptor
	for _, store := range stores {
		existingReplicas = append(existingReplicas, roachpb.ReplicaDescriptor{NodeID: store.Node.NodeID})
	}

	localities := sp.getLocalities(existingReplicas)
	for _, store := range stores {
		nodeID := store.Node.NodeID
		locality, ok := localities[nodeID]
		if !ok {
			t.Fatalf("could not find locality for node %d", nodeID)
		}
		if e, a := int(nodeID), len(locality.Tiers); e != a {
			t.Fatalf("for node %d, expected %d tiers, only got %d", nodeID, e, a)
		}
		if e, a := createLocality(int(nodeID)).String(), sp.getNodeLocalityString(nodeID); e != a {
			t.Fatalf("for getNodeLocalityString(%d), expected %q, got %q", nodeID, e, a)
		}
	}
}

func TestStorePoolDecommissioningReplicas(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper, g, _, sp, mnl := createTestStorePool(
		TestTimeUntilStoreDead, false /* deterministic */, NodeLivenessStatus_DEAD)
	defer stopper.Stop(context.TODO())
	sg := gossiputil.NewStoreGossiper(g)

	stores := []*roachpb.StoreDescriptor{
		{
			StoreID: 1,
			Node:    roachpb.NodeDescriptor{NodeID: 1},
		},
		{
			StoreID: 2,
			Node:    roachpb.NodeDescriptor{NodeID: 2},
		},
		{
			StoreID: 3,
			Node:    roachpb.NodeDescriptor{NodeID: 3},
		},
		{
			StoreID: 4,
			Node:    roachpb.NodeDescriptor{NodeID: 4},
		},
		{
			StoreID: 5,
			Node:    roachpb.NodeDescriptor{NodeID: 5},
		},
	}

	replicas := []roachpb.ReplicaDescriptor{
		{
			NodeID:    1,
			StoreID:   1,
			ReplicaID: 1,
		},
		{
			NodeID:    2,
			StoreID:   2,
			ReplicaID: 2,
		},
		{
			NodeID:    3,
			StoreID:   3,
			ReplicaID: 4,
		},
		{
			NodeID:    4,
			StoreID:   4,
			ReplicaID: 4,
		},
		{
			NodeID:    5,
			StoreID:   5,
			ReplicaID: 5,
		},
	}

	sg.GossipStores(stores, t)
	for i := 1; i <= 5; i++ {
		mnl.setNodeStatus(roachpb.NodeID(i), NodeLivenessStatus_LIVE)
	}

	liveReplicas, deadReplicas := sp.liveAndDeadReplicas(0, replicas)
	if len(liveReplicas) != 5 {
		t.Fatalf("expected five live replicas, found %d (%v)", len(liveReplicas), liveReplicas)
	}
	if len(deadReplicas) > 0 {
		t.Fatalf("expected no dead replicas initially, found %d (%v)", len(deadReplicas), deadReplicas)
	}
	// Mark node 4 as decommissioning.
	mnl.setNodeStatus(4, NodeLivenessStatus_DECOMMISSIONING)
	// Mark node 5 as dead.
	mnl.setNodeStatus(5, NodeLivenessStatus_DEAD)

	liveReplicas, deadReplicas = sp.liveAndDeadReplicas(0, replicas)
	// Decommissioning replicas are considered live.
	if a, e := liveReplicas, replicas[:4]; !reflect.DeepEqual(a, e) {
		t.Fatalf("expected live replicas %+v; got %+v", e, a)
	}
	if a, e := deadReplicas, replicas[4:]; !reflect.DeepEqual(a, e) {
		t.Fatalf("expected dead replicas %+v; got %+v", e, a)
	}

	decommissioningReplicas := sp.decommissioningReplicas(0, replicas)
	if a, e := decommissioningReplicas, replicas[3:4]; !reflect.DeepEqual(a, e) {
		t.Fatalf("expected decommissioning replicas %+v; got %+v", e, a)
	}
}
