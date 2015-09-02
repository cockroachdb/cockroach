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
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/stop"
)

// createTestReplicaForRepair creates a new range descriptor with a collection
// of replicas and then adds this descriptor to a new Replica struct. This
// Replica is just a shell designed for use in the repair queue's needsRepair
// function and is not intended for any other use.
func createTestReplicaForRepair(storeIDs []proto.StoreID) *Replica {
	desc := &proto.RangeDescriptor{
		RangeID:  1,
		StartKey: proto.KeyMin,
		EndKey:   proto.KeyMax,
		Replicas: createReplicaSets(storeIDs), // Found in replica_test
	}
	repl := &Replica{}
	repl.setDescWithoutProcessUpdate(desc)
	return repl
}

// mockStorePool sets up a collection of a alive and dead stores in the
// store pool for testing purposes.
func mockStorePool(storePool *StorePool, aliveStoreIDs, deadStoreIDs []proto.StoreID) {
	storePool.mu.Lock()
	defer storePool.mu.Unlock()

	storePool.stores = make(map[proto.StoreID]*storeDetail)
	for _, storeID := range aliveStoreIDs {
		storePool.stores[storeID] = &storeDetail{
			dead: false,
			desc: proto.StoreDescriptor{StoreID: storeID},
		}
	}
	for _, storeID := range deadStoreIDs {
		storePool.stores[storeID] = &storeDetail{
			dead: true,
			desc: proto.StoreDescriptor{StoreID: storeID},
		}
	}
}

// TestRepairQueueNeedsRepair verifies that the needsRepair method correctly
// identifies when a range requires repair.
func TestRepairQueueNeedsRepair(t *testing.T) {
	defer leaktest.AfterTest(t)

	stopper := stop.NewStopper()
	defer stopper.Stop()
	clock := hlc.NewClock(hlc.UnixNano)
	rpcContext := rpc.NewContext(&base.Context{}, clock, stopper)
	g := gossip.New(rpcContext, gossip.TestInterval, gossip.TestBootstrap)
	storePool := NewStorePool(g, TestTimeUntilStoreDeadOff, stopper)
	a := makeAllocator(storePool)
	replicateQ := makeReplicateQueue(g, a, clock)
	repairQ := makeRepairQueue(storePool, &replicateQ, clock)

	testCases := []struct {
		replica        *Replica
		storePoolAlive []proto.StoreID
		storePoolDead  []proto.StoreID
		eNeeds         bool
		ePriority      int
		eReplicas      []proto.Replica
	}{
		// Test 0: three stores, all alive, nothing to do
		{
			replica:        createTestReplicaForRepair([]proto.StoreID{1, 2, 3}),
			storePoolAlive: []proto.StoreID{1, 2, 3},
			storePoolDead:  []proto.StoreID{},
		},
		// Test 1: three stores, one dead store, repair the store
		{
			replica:        createTestReplicaForRepair([]proto.StoreID{1, 2, 3}),
			storePoolAlive: []proto.StoreID{1, 2},
			storePoolDead:  []proto.StoreID{3},
			eNeeds:         true,
			ePriority:      0,
			eReplicas:      createReplicaSets([]proto.StoreID{3}),
		},
		// Test 2: three stores, two dead stores, can't repair due to not having quorum
		{
			replica:        createTestReplicaForRepair([]proto.StoreID{1, 2, 3}),
			storePoolAlive: []proto.StoreID{1},
			storePoolDead:  []proto.StoreID{2, 3},
			ePriority:      1,
			eReplicas:      createReplicaSets([]proto.StoreID{2, 3}),
		},
		// Test 3: five store, two dead stores, both should be repaired
		{
			replica:        createTestReplicaForRepair([]proto.StoreID{1, 2, 3, 4, 5}),
			storePoolAlive: []proto.StoreID{1, 2, 3},
			storePoolDead:  []proto.StoreID{4, 5},
			eNeeds:         true,
			ePriority:      0,
			eReplicas:      createReplicaSets([]proto.StoreID{4, 5}),
		},
		// Test 4: five store, one dead stores, repair the store
		{
			replica:        createTestReplicaForRepair([]proto.StoreID{1, 2, 3, 4, 5}),
			storePoolAlive: []proto.StoreID{1, 2, 3, 4},
			storePoolDead:  []proto.StoreID{5},
			eNeeds:         true,
			ePriority:      -1,
			eReplicas:      createReplicaSets([]proto.StoreID{5}),
		},
		// Test 5: three stores, all alive, but only 1 is in the store pool, nothing to do
		{
			replica:        createTestReplicaForRepair([]proto.StoreID{1, 2, 3}),
			storePoolAlive: []proto.StoreID{1},
			storePoolDead:  []proto.StoreID{},
		},
	}

	for i, testCase := range testCases {
		mockStorePool(storePool, testCase.storePoolAlive, testCase.storePoolDead)
		aNeeds, aPriority, aReplicas := repairQ.needsRepair(testCase.replica)
		if testCase.eNeeds != aNeeds {
			t.Errorf("%d: Expected `needs` does not match actual. Expected: %t, Actual: %t", i, testCase.eNeeds, aNeeds)
		}
		if testCase.ePriority != int(aPriority) {
			t.Errorf("%d: Expected `priority` does not match actual. Expected: %d, Actual: %f", i, testCase.ePriority, aPriority)
		}
		if !reflect.DeepEqual(testCase.eReplicas, aReplicas) {
			t.Errorf("%d: Expected `replicas` does not match actual. Expected:\n%v\nActual:\n%v\n", i, testCase.eReplicas, aReplicas)
		}
	}
}
