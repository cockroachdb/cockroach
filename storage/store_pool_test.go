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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
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

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/stop"
)

var uniqueStore = []*proto.StoreDescriptor{
	{
		StoreID: 2,
		Attrs:   proto.Attributes{Attrs: []string{"ssd"}},
		Node: proto.NodeDescriptor{
			NodeID: 2,
			Attrs:  proto.Attributes{Attrs: []string{"a"}},
		},
		Capacity: proto.StoreCapacity{
			Capacity:  100,
			Available: 200,
		},
	},
}

// createTestStorePool creates a stopper, gossip and storePool for use in
// tests. Stopper must be stopped by the caller.
func createTestStorePool(timeUntilStoreDead time.Duration) (*stop.Stopper, *gossip.Gossip, *StorePool) {
	stopper := stop.NewStopper()
	rpcContext := rpc.NewContext(&base.Context{}, hlc.NewClock(hlc.UnixNano), stopper)
	g := gossip.New(rpcContext, gossip.TestInterval, gossip.TestBootstrap)
	storePool := NewStorePool(g, timeUntilStoreDead, stopper)
	return stopper, g, storePool
}

// TestStorePoolGossipUpdate ensures that the gossip callback in StorePool
// correctly updates a store's details.
func TestStorePoolGossipUpdate(t *testing.T) {
	defer leaktest.AfterTest(t)
	stopper, g, sp := createTestStorePool(testTimeUntilStoreDead)
	defer stopper.Stop()
	sg := newStoreGossiper(g)

	sp.mu.RLock()
	if _, ok := sp.stores[2]; ok {
		t.Fatalf("store 2 is already in the pool's store list")
	}
	sp.mu.RUnlock()

	sg.gossipStores(uniqueStore, t)

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
func waitUntilDead(t *testing.T, sp *StorePool, storeID proto.StoreID) {
	util.SucceedsWithin(t, 10*testTimeUntilStoreDead, func() error {
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

// TestStorePoolGossipUpdate ensures that a store is marked as dead after it
// times out and that it will be revived after a new update is received.
func TestStorePoolDies(t *testing.T) {
	defer leaktest.AfterTest(t)
	stopper, g, sp := createTestStorePool(testTimeUntilStoreDead)
	defer stopper.Stop()
	sg := newStoreGossiper(g)
	sg.gossipStores(uniqueStore, t)

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
	waitUntilDead(t, sp, 2)
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

	sg.gossipStores(uniqueStore, t)

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
	waitUntilDead(t, sp, 2)
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
func verifyStoreList(sp *StorePool, requiredAttrs []string, expected []int) error {
	var actual []int
	sl := sp.getStoreList(proto.Attributes{Attrs: requiredAttrs}, false)
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
	defer leaktest.AfterTest(t)
	stopper, g, sp := createTestStorePool(testTimeUntilStoreDead)
	defer stopper.Stop()
	sg := newStoreGossiper(g)
	required := []string{"ssd", "dc"}
	// Nothing yet.
	if sl := sp.getStoreList(proto.Attributes{Attrs: required}, false); len(sl.stores) != 0 {
		t.Errorf("expected no stores, instead %+v", sl.stores)
	}

	matchingStore := proto.StoreDescriptor{
		StoreID: 1,
		Node:    proto.NodeDescriptor{NodeID: 1},
		Attrs:   proto.Attributes{Attrs: required},
	}
	supersetStore := proto.StoreDescriptor{
		StoreID: 2,
		Node:    proto.NodeDescriptor{NodeID: 1},
		Attrs:   proto.Attributes{Attrs: append(required, "db")},
	}
	unmatchingStore := proto.StoreDescriptor{
		StoreID: 3,
		Node:    proto.NodeDescriptor{NodeID: 1},
		Attrs:   proto.Attributes{Attrs: []string{"ssd", "otherdc"}},
	}
	emptyStore := proto.StoreDescriptor{
		StoreID: 4,
		Node:    proto.NodeDescriptor{NodeID: 1},
		Attrs:   proto.Attributes{},
	}
	deadStore := proto.StoreDescriptor{
		StoreID: 5,
		Node:    proto.NodeDescriptor{NodeID: 1},
		Attrs:   proto.Attributes{Attrs: required},
	}

	sg.gossipStores([]*proto.StoreDescriptor{
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
	}); err != nil {
		t.Error(err)
	}

	// Timeout all stores, but specifically store 5.
	waitUntilDead(t, sp, 5)

	// Resurrect all stores except for 5.
	sg.gossipStores([]*proto.StoreDescriptor{
		&matchingStore,
		&supersetStore,
		&unmatchingStore,
		&emptyStore,
	}, t)

	if err := verifyStoreList(sp, required, []int{
		int(matchingStore.StoreID),
		int(supersetStore.StoreID),
	}); err != nil {
		t.Error(err)
	}
}

func TestStorePoolGetStoreDetails(t *testing.T) {
	defer leaktest.AfterTest(t)
	stopper, g, sp := createTestStorePool(TestTimeUntilStoreDeadOff)
	defer stopper.Stop()
	sg := newStoreGossiper(g)
	sg.gossipStores(uniqueStore, t)

	if detail := sp.getStoreDetail(proto.StoreID(1)); detail.dead {
		t.Errorf("Present storeDetail came back as dead, expected it to be alive. %+v", detail)
	}

	if detail := sp.getStoreDetail(proto.StoreID(2)); detail.dead {
		t.Errorf("Absent storeDetail came back as dead, expected it to be alive. %+v", detail)
	}
}
