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
//
// Author: Bram Gruneir (bram+code@cockroachlabs.com)

package storage

import (
	"fmt"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
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
	defaultNodeStatus nodeStatus
	nodes             map[roachpb.NodeID]nodeStatus
}

func newMockNodeLiveness(defaultNodeStatus nodeStatus) *mockNodeLiveness {
	return &mockNodeLiveness{
		defaultNodeStatus: defaultNodeStatus,
		nodes:             map[roachpb.NodeID]nodeStatus{},
	}
}

func (m *mockNodeLiveness) setNodeStatus(nodeID roachpb.NodeID, status nodeStatus) {
	m.Lock()
	defer m.Unlock()
	m.nodes[nodeID] = status
}

func (m *mockNodeLiveness) nodeLivenessFunc(
	nodeID roachpb.NodeID, now time.Time, threshold time.Duration,
) nodeStatus {
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
	timeUntilStoreDead time.Duration, deterministic bool, defaultNodeStatus nodeStatus,
) (*stop.Stopper, *gossip.Gossip, *hlc.ManualClock, *StorePool, *mockNodeLiveness) {
	stopper := stop.NewStopper()
	mc := hlc.NewManualClock(123)
	clock := hlc.NewClock(mc.UnixNano, time.Nanosecond)
	rpcContext := rpc.NewContext(log.AmbientContext{}, &base.Config{Insecure: true}, clock, stopper)
	server := rpc.NewServer(rpcContext) // never started
	g := gossip.NewTest(1, rpcContext, server, stopper, metric.NewRegistry())
	mnl := newMockNodeLiveness(defaultNodeStatus)
	storePool := NewStorePool(
		log.AmbientContext{},
		g,
		clock,
		mnl.nodeLivenessFunc,
		timeUntilStoreDead,
		deterministic,
	)
	return stopper, g, mc, storePool, mnl
}

// TestStorePoolGossipUpdate ensures that the gossip callback in StorePool
// correctly updates a store's details.
func TestStorePoolGossipUpdate(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper, g, _, sp, _ := createTestStorePool(
		TestTimeUntilStoreDead, false /* deterministic */, nodeStatusDead)
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
	constraints config.Constraints,
	rangeID roachpb.RangeID,
	expected []int,
	expectedAliveStoreCount int,
	expectedThrottledStoreCount int,
) error {
	var actual []int
	sl, aliveStoreCount, throttledStoreCount := sp.getStoreList(rangeID)
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
		TestTimeUntilStoreDead, false /* deterministic */, nodeStatusDead)
	defer stopper.Stop(context.TODO())
	sg := gossiputil.NewStoreGossiper(g)
	constraints := config.Constraints{Constraints: []config.Constraint{{Value: "ssd"}, {Value: "dc"}}}
	required := []string{"ssd", "dc"}
	// Nothing yet.
	sl, _, _ := sp.getStoreList(roachpb.RangeID(0))
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
		mnl.setNodeStatus(roachpb.NodeID(i), nodeStatusLive)
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
		/* expectedAliveStoreCount */ 7,
		/* expectedThrottledStoreCount */ 0,
	); err != nil {
		t.Error(err)
	}

	// Set deadStore as dead.
	mnl.setNodeStatus(deadStore.Node.NodeID, nodeStatusDead)
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
		},
		/* expectedAliveStoreCount */ 6,
		/* expectedThrottledStoreCount */ 1,
	); err != nil {
		t.Error(err)
	}
}

func TestStorePoolGetStoreDetails(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper, g, _, sp, _ := createTestStorePool(
		TestTimeUntilStoreDead, false /* deterministic */, nodeStatusDead)
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
		TestTimeUntilStoreDead, false /* deterministic */, nodeStatusDead)
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
		mnl.setNodeStatus(roachpb.NodeID(i), nodeStatusLive)
	}

	liveReplicas, deadReplicas := sp.liveAndDeadReplicas(0, replicas)
	if len(liveReplicas) != 5 {
		t.Fatalf("expected five live replicas, found %d (%v)", len(liveReplicas), liveReplicas)
	}
	if len(deadReplicas) > 0 {
		t.Fatalf("expected no dead replicas initially, found %d (%v)", len(deadReplicas), deadReplicas)
	}
	// Mark nodes 4 & 5 as dead.
	mnl.setNodeStatus(4, nodeStatusDead)
	mnl.setNodeStatus(5, nodeStatusDead)

	liveReplicas, deadReplicas = sp.liveAndDeadReplicas(0, replicas)
	if a, e := liveReplicas, replicas[:3]; !reflect.DeepEqual(a, e) {
		t.Fatalf("expected live replicas %+v; got %+v", e, a)
	}
	if a, e := deadReplicas, replicas[3:]; !reflect.DeepEqual(a, e) {
		t.Fatalf("expected dead replicas %+v; got %+v", e, a)
	}

	// Mark node 4 as merely unavailable.
	mnl.setNodeStatus(4, nodeStatusUnknown)

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
		TestTimeUntilStoreDead, false /* deterministic */, nodeStatusDead)
	defer stopper.Stop(context.TODO())

	liveReplicas, deadReplicas := sp.liveAndDeadReplicas(0, []roachpb.ReplicaDescriptor{{StoreID: 1}})
	if len(liveReplicas) != 0 || len(deadReplicas) != 0 {
		t.Errorf("expected 0 live and 0 dead replicas; got %v and %v", liveReplicas, deadReplicas)
	}

	sl, alive, throttled := sp.getStoreList(roachpb.RangeID(0))
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
		TestTimeUntilStoreDead, false /* deterministic */, nodeStatusDead)
	defer stopper.Stop(context.TODO())

	sg := gossiputil.NewStoreGossiper(g)
	sg.GossipStores(uniqueStore, t)

	{
		expected := sp.clock.Now().GoTime().Add(sp.declinedReservationsTimeout)
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
		expected := sp.clock.Now().GoTime().Add(sp.failedReservationsTimeout)
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
		TestTimeUntilStoreDead, false /* deterministic */, nodeStatusDead)
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
