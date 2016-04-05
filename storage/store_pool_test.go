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
	"errors"
	"fmt"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/testutils/gossiputil"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/stop"
	"github.com/cockroachdb/cockroach/util/timeutil"
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

// createTestStorePool creates a stopper, gossip and storePool for use in
// tests. Stopper must be stopped by the caller.
func createTestStorePool(timeUntilStoreDead time.Duration) (*stop.Stopper, *gossip.Gossip, *hlc.ManualClock, *StorePool) {
	stopper := stop.NewStopper()
	mc := hlc.NewManualClock(0)
	clock := hlc.NewClock(mc.UnixNano)
	rpcContext := rpc.NewContext(nil, clock, stopper)
	g := gossip.New(rpcContext, nil, stopper)
	// Have to call g.SetNodeID before call g.AddInfo
	g.SetNodeID(roachpb.NodeID(1))
	storePool := NewStorePool(g, clock, timeUntilStoreDead, stopper)
	return stopper, g, mc, storePool
}

// TestStorePoolGossipUpdate ensures that the gossip callback in StorePool
// correctly updates a store's details.
func TestStorePoolGossipUpdate(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper, g, _, sp := createTestStorePool(TestTimeUntilStoreDead)
	defer stopper.Stop()
	sg := gossiputil.NewStoreGossiper(g)

	sp.mu.RLock()
	if _, ok := sp.stores[2]; ok {
		t.Fatalf("store 2 is already in the pool's store list")
	}
	sp.mu.RUnlock()

	sg.GossipStores(uniqueStore, t)

	sp.mu.RLock()
	if _, ok := sp.stores[2]; !ok {
		t.Fatalf("store 2 isn't in the pool's store list")
	}
	if e, a := 1, sp.queue.Len(); e > a {
		t.Fatalf("wrong number of stores in the queue expected at least:%d actual:%d", e, a)
	}
	sp.mu.RUnlock()
}

// waitUntilDead will block until the specified store is marked as dead.
func waitUntilDead(t *testing.T, mc *hlc.ManualClock, sp *StorePool, storeID roachpb.StoreID) {
	lastTime := timeutil.Now()
	util.SucceedsSoon(t, func() error {
		curTime := timeutil.Now()
		mc.Increment(curTime.UnixNano() - lastTime.UnixNano())
		lastTime = curTime

		sp.mu.RLock()
		defer sp.mu.RUnlock()
		store, ok := sp.stores[storeID]
		if !ok {
			t.Fatalf("store %s isn't in the pool's store list", storeID)
		}
		exitcode := store.dead

		if exitcode {
			return nil
		}
		return errors.New("store not marked as dead yet")
	})
}

// TestStorePoolDies ensures that a store is marked as dead after it
// times out and that it will be revived after a new update is received.
func TestStorePoolDies(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper, g, mc, sp := createTestStorePool(TestTimeUntilStoreDead)
	defer stopper.Stop()
	sg := gossiputil.NewStoreGossiper(g)
	sg.GossipStores(uniqueStore, t)

	{
		sp.mu.RLock()
		store2, ok := sp.stores[2]
		if !ok {
			t.Fatalf("store 2 isn't in the pool's store list")
		}
		if store2.dead {
			t.Errorf("store 2 is dead before it times out")
		}
		if e, a := 0, store2.timesDied; e != a {
			t.Errorf("store 2 has been counted dead %d times, expected %d", a, e)
		}
		if store2.index == -1 {
			t.Errorf("store 2 is mot the queue, it should be")
		}
		if e, a := 1, sp.queue.Len(); e > a {
			t.Errorf("wrong number of stores in the queue expected to be at least:%d actual:%d", e, a)
		}
		sp.mu.RUnlock()
	}

	// Timeout store 2.
	waitUntilDead(t, mc, sp, 2)
	{
		sp.mu.RLock()
		store2, ok := sp.stores[2]
		if !ok {
			t.Fatalf("store 2 isn't in the pool's store list")
		}
		if e, a := 1, store2.timesDied; e != a {
			t.Errorf("store 2 has been counted dead %d times, expected %d", a, e)
		}
		if store2.index != -1 {
			t.Errorf("store 2 is in the queue, it shouldn't be")
		}
		sp.mu.RUnlock()
	}

	sg.GossipStores(uniqueStore, t)

	{
		sp.mu.RLock()
		store2, ok := sp.stores[2]
		if !ok {
			t.Fatalf("store 2 isn't in the pool's store list")
		}
		if store2.dead {
			t.Errorf("store 2 is dead still, it should be alive")
		}
		if e, a := 1, store2.timesDied; e != a {
			t.Errorf("store 2 has been counted dead %d times, expected %d", a, e)
		}
		if store2.index == -1 {
			t.Errorf("store 2 is mot the queue, it should be")
		}
		sp.mu.RUnlock()
	}

	// Timeout store 2 again.
	waitUntilDead(t, mc, sp, 2)
	{
		sp.mu.RLock()
		store2, ok := sp.stores[2]
		if !ok {
			t.Fatalf("store 2 isn't in the pool's store list")
		}
		if e, a := 2, store2.timesDied; e != a {
			t.Errorf("store 2 has been counted dead %d times, expected %d", a, e)
		}
		if store2.index != -1 {
			t.Errorf("store 2 is in the queue, it shouldn't be")
		}
		sp.mu.RUnlock()
	}
}

// verifyStoreList ensures that the returned list of stores is correct.
func verifyStoreList(sp *StorePool, requiredAttrs []string, expected []int, expectedAliveStoreCount int) error {
	var actual []int
	sl, aliveStoreCount := sp.getStoreList(roachpb.Attributes{Attrs: requiredAttrs}, false)
	if aliveStoreCount != expectedAliveStoreCount {
		return fmt.Errorf("expected AliveStoreCount %d does not match actual %d", expectedAliveStoreCount,
			aliveStoreCount)
	}
	for _, store := range sl.stores {
		actual = append(actual, int(store.StoreID))
	}
	sort.Ints(expected)
	sort.Ints(actual)
	if !reflect.DeepEqual(expected, actual) {
		return fmt.Errorf("expected %+v stores, actual %+v", expected, actual)
	}
	return nil
}

// TestStorePoolGetStoreList ensures that the store list returns only stores
// that are alive and match the attribute criteria.
func TestStorePoolGetStoreList(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// We're going to manually mark stores dead in this test.
	stopper, g, _, sp := createTestStorePool(TestTimeUntilStoreDeadOff)
	defer stopper.Stop()
	sg := gossiputil.NewStoreGossiper(g)
	required := []string{"ssd", "dc"}
	// Nothing yet.
	if sl, _ := sp.getStoreList(roachpb.Attributes{Attrs: required}, false); len(sl.stores) != 0 {
		t.Errorf("expected no stores, instead %+v", sl.stores)
	}

	matchingStore := roachpb.StoreDescriptor{
		StoreID: 1,
		Node:    roachpb.NodeDescriptor{NodeID: 1},
		Attrs:   roachpb.Attributes{Attrs: required},
	}
	supersetStore := roachpb.StoreDescriptor{
		StoreID: 2,
		Node:    roachpb.NodeDescriptor{NodeID: 1},
		Attrs:   roachpb.Attributes{Attrs: append(required, "db")},
	}
	unmatchingStore := roachpb.StoreDescriptor{
		StoreID: 3,
		Node:    roachpb.NodeDescriptor{NodeID: 1},
		Attrs:   roachpb.Attributes{Attrs: []string{"ssd", "otherdc"}},
	}
	emptyStore := roachpb.StoreDescriptor{
		StoreID: 4,
		Node:    roachpb.NodeDescriptor{NodeID: 1},
		Attrs:   roachpb.Attributes{},
	}
	deadStore := roachpb.StoreDescriptor{
		StoreID: 5,
		Node:    roachpb.NodeDescriptor{NodeID: 1},
		Attrs:   roachpb.Attributes{Attrs: required},
	}

	// Mark all alive initially.
	sg.GossipStores([]*roachpb.StoreDescriptor{
		&matchingStore,
		&supersetStore,
		&unmatchingStore,
		&emptyStore,
		&deadStore,
	}, t)

	if err := verifyStoreList(sp, required, []int{
		int(matchingStore.StoreID),
		int(supersetStore.StoreID),
		int(deadStore.StoreID),
	}, 5); err != nil {
		t.Error(err)
	}

	// Mark one store dead.
	sp.mu.Lock()
	sp.stores[deadStore.StoreID].markDead(sp.clock.Now())
	sp.mu.Unlock()

	if err := verifyStoreList(sp, required, []int{
		int(matchingStore.StoreID),
		int(supersetStore.StoreID),
	}, 4); err != nil {
		t.Error(err)
	}
}

func TestStorePoolGetStoreDetails(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper, g, _, sp := createTestStorePool(TestTimeUntilStoreDeadOff)
	defer stopper.Stop()
	sg := gossiputil.NewStoreGossiper(g)
	sg.GossipStores(uniqueStore, t)

	sp.mu.Lock()
	defer sp.mu.Unlock()
	if detail := sp.getStoreDetailLocked(roachpb.StoreID(1)); detail.dead {
		t.Errorf("Present storeDetail came back as dead, expected it to be alive. %+v", detail)
	}

	if detail := sp.getStoreDetailLocked(roachpb.StoreID(2)); detail.dead {
		t.Errorf("Absent storeDetail came back as dead, expected it to be alive. %+v", detail)
	}
}

func TestStorePoolFindDeadReplicas(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper, g, mc, sp := createTestStorePool(TestTimeUntilStoreDead)
	defer stopper.Stop()
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
			StoreID:   5,
			ReplicaID: 4,
		},
		{
			NodeID:    5,
			StoreID:   5,
			ReplicaID: 5,
		},
	}

	sg.GossipStores(stores, t)

	deadReplicas := sp.deadReplicas(replicas)
	if len(deadReplicas) > 0 {
		t.Fatalf("expected no dead replicas initially, found %d (%v)", len(deadReplicas), deadReplicas)
	}
	// Timeout all stores, but specifically store 5.
	waitUntilDead(t, mc, sp, 5)

	// Resurrect all stores except for 4 and 5.
	sg.GossipStores(stores[:3], t)

	deadReplicas = sp.deadReplicas(replicas)
	if a, e := deadReplicas, replicas[3:]; !reflect.DeepEqual(a, e) {
		t.Fatalf("findDeadReplicas did not return expected values; got \n%v, expected \n%v", a, e)
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
	stopper, _, _, sp := createTestStorePool(TestTimeUntilStoreDeadOff)
	defer stopper.Stop()

	if dead := sp.deadReplicas([]roachpb.ReplicaDescriptor{{StoreID: 1}}); len(dead) > 0 {
		t.Errorf("expected 0 dead replicas; got %v", dead)
	}

	if sl, c := sp.getStoreList(roachpb.Attributes{}, true); len(sl.stores) > 0 || c != 0 {
		t.Errorf("expected 0 live stores; got list %v and total count %d", sl, c)
	}
}
