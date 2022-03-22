// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver

import (
	"context"
	"fmt"
	"math"
	"os"
	"reflect"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/constraint"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/gossiputil"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/olekukonko/tablewriter"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/tracker"
)

const firstRangeID = roachpb.RangeID(1)

var simpleSpanConfig = roachpb.SpanConfig{
	NumReplicas: 1,
	Constraints: []roachpb.ConstraintsConjunction{
		{
			Constraints: []roachpb.Constraint{
				{Value: "a", Type: roachpb.Constraint_REQUIRED},
				{Value: "ssd", Type: roachpb.Constraint_REQUIRED},
			},
		},
	},
}

var multiDCConfigSSD = roachpb.SpanConfig{
	NumReplicas: 2,
	Constraints: []roachpb.ConstraintsConjunction{
		{Constraints: []roachpb.Constraint{{Value: "ssd", Type: roachpb.Constraint_REQUIRED}}},
	},
}

var multiDCConfigConstrainToA = roachpb.SpanConfig{
	NumReplicas: 2,
	Constraints: []roachpb.ConstraintsConjunction{
		{Constraints: []roachpb.Constraint{{Value: "a", Type: roachpb.Constraint_REQUIRED}}},
	},
}

var multiDCConfigUnsatisfiableVoterConstraints = roachpb.SpanConfig{
	NumReplicas: 2,
	VoterConstraints: []roachpb.ConstraintsConjunction{
		{Constraints: []roachpb.Constraint{{Value: "doesNotExist", Type: roachpb.Constraint_REQUIRED}}},
	},
}

// multiDCConfigVoterAndNonVoter prescribes that one voting replica be placed in
// DC "b" and one non-voting replica be placed in DC "a".
var multiDCConfigVoterAndNonVoter = roachpb.SpanConfig{
	NumReplicas: 2,
	Constraints: []roachpb.ConstraintsConjunction{
		// Constrain the non-voter to "a".
		{Constraints: []roachpb.Constraint{{Value: "a", Type: roachpb.Constraint_REQUIRED}}, NumReplicas: 1},
	},
	VoterConstraints: []roachpb.ConstraintsConjunction{
		// Constrain the voter to "b".
		{Constraints: []roachpb.Constraint{{Value: "b", Type: roachpb.Constraint_REQUIRED}}},
	},
}

// emptySpanConfig returns the empty span configuration.
func emptySpanConfig() roachpb.SpanConfig {
	return roachpb.SpanConfig{}
}

var singleStore = []*roachpb.StoreDescriptor{
	{
		StoreID: 1,
		Attrs:   roachpb.Attributes{Attrs: []string{"ssd"}},
		Node: roachpb.NodeDescriptor{
			NodeID: 1,
			Attrs:  roachpb.Attributes{Attrs: []string{"a"}},
		},
		Capacity: roachpb.StoreCapacity{
			Capacity:     200,
			Available:    100,
			LogicalBytes: 100,
		},
	},
}

var sameDCStores = []*roachpb.StoreDescriptor{
	{
		StoreID: 1,
		Attrs:   roachpb.Attributes{Attrs: []string{"ssd"}},
		Node: roachpb.NodeDescriptor{
			NodeID: 1,
			Attrs:  roachpb.Attributes{Attrs: []string{"a"}},
		},
		Capacity: roachpb.StoreCapacity{
			Capacity:     200,
			Available:    100,
			LogicalBytes: 100,
		},
	},
	{
		StoreID: 2,
		Attrs:   roachpb.Attributes{Attrs: []string{"ssd"}},
		Node: roachpb.NodeDescriptor{
			NodeID: 2,
			Attrs:  roachpb.Attributes{Attrs: []string{"a"}},
		},
		Capacity: roachpb.StoreCapacity{
			Capacity:     200,
			Available:    100,
			LogicalBytes: 100,
		},
	},
	{
		StoreID: 3,
		Attrs:   roachpb.Attributes{Attrs: []string{"hdd"}},
		Node: roachpb.NodeDescriptor{
			NodeID: 3,
			Attrs:  roachpb.Attributes{Attrs: []string{"a"}},
		},
		Capacity: roachpb.StoreCapacity{
			Capacity:     200,
			Available:    100,
			LogicalBytes: 100,
		},
	},
	{
		StoreID: 4,
		Attrs:   roachpb.Attributes{Attrs: []string{"hdd"}},
		Node: roachpb.NodeDescriptor{
			NodeID: 4,
			Attrs:  roachpb.Attributes{Attrs: []string{"a"}},
		},
		Capacity: roachpb.StoreCapacity{
			Capacity:     200,
			Available:    100,
			LogicalBytes: 100,
		},
	},
	{
		StoreID: 5,
		Attrs:   roachpb.Attributes{Attrs: []string{"mem"}},
		Node: roachpb.NodeDescriptor{
			NodeID: 5,
			Attrs:  roachpb.Attributes{Attrs: []string{"a"}},
		},
		Capacity: roachpb.StoreCapacity{
			Capacity:     200,
			Available:    100,
			LogicalBytes: 100,
		},
	},
}

var multiDCStores = []*roachpb.StoreDescriptor{
	{
		StoreID: 1,
		Attrs:   roachpb.Attributes{Attrs: []string{"ssd"}},
		Node: roachpb.NodeDescriptor{
			NodeID: 1,
			Attrs:  roachpb.Attributes{Attrs: []string{"a"}},
		},
		Capacity: roachpb.StoreCapacity{
			Capacity:     200,
			Available:    100,
			LogicalBytes: 100,
		},
	},
	{
		StoreID: 2,
		Attrs:   roachpb.Attributes{Attrs: []string{"ssd"}},
		Node: roachpb.NodeDescriptor{
			NodeID: 2,
			Attrs:  roachpb.Attributes{Attrs: []string{"b"}},
		},
		Capacity: roachpb.StoreCapacity{
			Capacity:     200,
			Available:    100,
			LogicalBytes: 100,
		},
	},
}

var multiDiversityDCStores = []*roachpb.StoreDescriptor{
	{
		StoreID: 1,
		Attrs:   roachpb.Attributes{Attrs: []string{"ssd"}},
		Node: roachpb.NodeDescriptor{
			NodeID: 1,
			Attrs:  roachpb.Attributes{Attrs: []string{"odd"}},
			Locality: roachpb.Locality{
				Tiers: []roachpb.Tier{
					{Key: "datacenter", Value: "a"},
				},
			},
		},
	},
	{
		StoreID: 2,
		Attrs:   roachpb.Attributes{Attrs: []string{"hdd"}},
		Node: roachpb.NodeDescriptor{
			NodeID: 2,
			Attrs:  roachpb.Attributes{Attrs: []string{"even"}},
			Locality: roachpb.Locality{
				Tiers: []roachpb.Tier{
					{Key: "datacenter", Value: "a"},
				},
			},
		},
	},
	{
		StoreID: 3,
		Attrs:   roachpb.Attributes{Attrs: []string{"ssd"}},
		Node: roachpb.NodeDescriptor{
			NodeID: 3,
			Attrs:  roachpb.Attributes{Attrs: []string{"odd"}},
			Locality: roachpb.Locality{
				Tiers: []roachpb.Tier{
					{Key: "datacenter", Value: "b"},
				},
			},
		},
	},
	{
		StoreID: 4,
		Attrs:   roachpb.Attributes{Attrs: []string{"hdd"}},
		Node: roachpb.NodeDescriptor{
			NodeID: 4,
			Attrs:  roachpb.Attributes{Attrs: []string{"even"}},
			Locality: roachpb.Locality{
				Tiers: []roachpb.Tier{
					{Key: "datacenter", Value: "b"},
				},
			},
		},
	},
	{
		StoreID: 5,
		Attrs:   roachpb.Attributes{Attrs: []string{"ssd"}},
		Node: roachpb.NodeDescriptor{
			NodeID: 5,
			Attrs:  roachpb.Attributes{Attrs: []string{"odd"}},
			Locality: roachpb.Locality{
				Tiers: []roachpb.Tier{
					{Key: "datacenter", Value: "c"},
				},
			},
		},
	},
	{
		StoreID: 6,
		Attrs:   roachpb.Attributes{Attrs: []string{"hdd"}},
		Node: roachpb.NodeDescriptor{
			NodeID: 6,
			Attrs:  roachpb.Attributes{Attrs: []string{"even"}},
			Locality: roachpb.Locality{
				Tiers: []roachpb.Tier{
					{Key: "datacenter", Value: "c"},
				},
			},
		},
	},
	{
		StoreID: 7,
		Attrs:   roachpb.Attributes{Attrs: []string{"ssd"}},
		Node: roachpb.NodeDescriptor{
			NodeID: 7,
			Attrs:  roachpb.Attributes{Attrs: []string{"odd"}},
			Locality: roachpb.Locality{
				Tiers: []roachpb.Tier{
					{Key: "datacenter", Value: "d"},
				},
			},
		},
	},
	{
		StoreID: 8,
		Attrs:   roachpb.Attributes{Attrs: []string{"hdd"}},
		Node: roachpb.NodeDescriptor{
			NodeID: 8,
			Attrs:  roachpb.Attributes{Attrs: []string{"even"}},
			Locality: roachpb.Locality{
				Tiers: []roachpb.Tier{
					{Key: "datacenter", Value: "d"},
				},
			},
		},
	},
}

var oneStoreWithFullDisk = []*roachpb.StoreDescriptor{
	{
		StoreID:  1,
		Node:     roachpb.NodeDescriptor{NodeID: 1},
		Capacity: roachpb.StoreCapacity{Capacity: 200, Available: 5, RangeCount: 600},
	},
	{
		StoreID:  2,
		Node:     roachpb.NodeDescriptor{NodeID: 2},
		Capacity: roachpb.StoreCapacity{Capacity: 200, Available: 100, RangeCount: 600},
	},
	{
		StoreID:  3,
		Node:     roachpb.NodeDescriptor{NodeID: 3},
		Capacity: roachpb.StoreCapacity{Capacity: 200, Available: 100, RangeCount: 600},
	},
}

var oneStoreWithTooManyRanges = []*roachpb.StoreDescriptor{
	{
		StoreID:  1,
		Node:     roachpb.NodeDescriptor{NodeID: 1},
		Capacity: roachpb.StoreCapacity{Capacity: 200, Available: 5, RangeCount: 600},
	},
	{
		StoreID:  2,
		Node:     roachpb.NodeDescriptor{NodeID: 2},
		Capacity: roachpb.StoreCapacity{Capacity: 200, Available: 100, RangeCount: 200},
	},
	{
		StoreID:  3,
		Node:     roachpb.NodeDescriptor{NodeID: 3},
		Capacity: roachpb.StoreCapacity{Capacity: 200, Available: 100, RangeCount: 200},
	},
}

func replicas(storeIDs ...roachpb.StoreID) []roachpb.ReplicaDescriptor {
	res := make([]roachpb.ReplicaDescriptor, len(storeIDs))
	for i, storeID := range storeIDs {
		res[i].NodeID = roachpb.NodeID(storeID)
		res[i].StoreID = storeID
		res[i].ReplicaID = roachpb.ReplicaID(i + 1)
	}
	return res
}

// createTestAllocator creates a stopper, gossip, store pool and allocator for
// use in tests. Stopper must be stopped by the caller.
func createTestAllocator(
	ctx context.Context, numNodes int, deterministic bool,
) (*stop.Stopper, *gossip.Gossip, *StorePool, Allocator, *hlc.ManualClock) {
	return createTestAllocatorWithKnobs(ctx, numNodes, deterministic, nil /* knobs */)
}

// createTestAllocatorWithKnobs is like `createTestAllocator`, but allows the
// caller to pass in custom AllocatorTestingKnobs. Stopper must be stopped by
// the caller.
func createTestAllocatorWithKnobs(
	ctx context.Context, numNodes int, deterministic bool, knobs *AllocatorTestingKnobs,
) (*stop.Stopper, *gossip.Gossip, *StorePool, Allocator, *hlc.ManualClock) {
	stopper, g, manual, storePool, _ := createTestStorePool(ctx,
		TestTimeUntilStoreDeadOff, deterministic,
		func() int { return numNodes },
		livenesspb.NodeLivenessStatus_LIVE)
	a := MakeAllocator(
		storePool, func(string) (time.Duration, bool) {
			return 0, true
		},
		knobs,
	)
	return stopper, g, storePool, a, manual
}

// checkReplExists checks whether the given `repl` exists on any of the
// `stores`.
func checkReplExists(repl roachpb.ReplicationTarget, stores []roachpb.StoreID) (found bool) {
	for _, storeID := range stores {
		if repl.StoreID == storeID {
			found = true
			break
		}
	}
	return found
}

// mockStorePool sets up a collection of a alive and dead stores in the store
// pool for testing purposes.
func mockStorePool(
	storePool *StorePool,
	aliveStoreIDs []roachpb.StoreID,
	unavailableStoreIDs []roachpb.StoreID,
	deadStoreIDs []roachpb.StoreID,
	decommissioningStoreIDs []roachpb.StoreID,
	decommissionedStoreIDs []roachpb.StoreID,
	suspectedStoreIDs []roachpb.StoreID,
) {
	storePool.detailsMu.Lock()
	defer storePool.detailsMu.Unlock()

	liveNodeSet := map[roachpb.NodeID]livenesspb.NodeLivenessStatus{}
	storePool.detailsMu.storeDetails = map[roachpb.StoreID]*storeDetail{}
	for _, storeID := range aliveStoreIDs {
		liveNodeSet[roachpb.NodeID(storeID)] = livenesspb.NodeLivenessStatus_LIVE
		detail := storePool.getStoreDetailLocked(storeID)
		detail.desc = &roachpb.StoreDescriptor{
			StoreID: storeID,
			Node:    roachpb.NodeDescriptor{NodeID: roachpb.NodeID(storeID)},
		}
	}
	for _, storeID := range unavailableStoreIDs {
		liveNodeSet[roachpb.NodeID(storeID)] = livenesspb.NodeLivenessStatus_UNAVAILABLE
		detail := storePool.getStoreDetailLocked(storeID)
		detail.desc = &roachpb.StoreDescriptor{
			StoreID: storeID,
			Node:    roachpb.NodeDescriptor{NodeID: roachpb.NodeID(storeID)},
		}
	}
	for _, storeID := range deadStoreIDs {
		liveNodeSet[roachpb.NodeID(storeID)] = livenesspb.NodeLivenessStatus_DEAD
		detail := storePool.getStoreDetailLocked(storeID)
		detail.desc = &roachpb.StoreDescriptor{
			StoreID: storeID,
			Node:    roachpb.NodeDescriptor{NodeID: roachpb.NodeID(storeID)},
		}
	}
	for _, storeID := range decommissioningStoreIDs {
		liveNodeSet[roachpb.NodeID(storeID)] = livenesspb.NodeLivenessStatus_DECOMMISSIONING
		detail := storePool.getStoreDetailLocked(storeID)
		detail.desc = &roachpb.StoreDescriptor{
			StoreID: storeID,
			Node:    roachpb.NodeDescriptor{NodeID: roachpb.NodeID(storeID)},
		}
	}
	for _, storeID := range decommissionedStoreIDs {
		liveNodeSet[roachpb.NodeID(storeID)] = livenesspb.NodeLivenessStatus_DECOMMISSIONED
		detail := storePool.getStoreDetailLocked(storeID)
		detail.desc = &roachpb.StoreDescriptor{
			StoreID: storeID,
			Node:    roachpb.NodeDescriptor{NodeID: roachpb.NodeID(storeID)},
		}
	}

	for _, storeID := range suspectedStoreIDs {
		liveNodeSet[roachpb.NodeID(storeID)] = livenesspb.NodeLivenessStatus_LIVE
		detail := storePool.getStoreDetailLocked(storeID)
		detail.lastAvailable = storePool.clock.Now().GoTime()
		detail.lastUnavailable = storePool.clock.Now().GoTime()
		detail.desc = &roachpb.StoreDescriptor{
			StoreID: storeID,
			Node:    roachpb.NodeDescriptor{NodeID: roachpb.NodeID(storeID)},
		}
	}

	// Set the node liveness function using the set we constructed.
	storePool.nodeLivenessFn =
		func(nodeID roachpb.NodeID, now time.Time, threshold time.Duration) livenesspb.NodeLivenessStatus {
			if status, ok := liveNodeSet[nodeID]; ok {
				return status
			}
			return livenesspb.NodeLivenessStatus_UNAVAILABLE
		}
}

func TestAllocatorSimpleRetrieval(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper, g, _, a, _ := createTestAllocator(ctx, 1, false /* deterministic */)
	defer stopper.Stop(ctx)
	gossiputil.NewStoreGossiper(g).GossipStores(singleStore, t)
	result, _, err := a.AllocateVoter(
		ctx,
		simpleSpanConfig,
		nil /* existingVoters */, nil, /* existingNonVoters */
	)
	if err != nil {
		t.Fatalf("Unable to perform allocation: %+v", err)
	}
	if result.NodeID != 1 || result.StoreID != 1 {
		t.Errorf("expected NodeID 1 and StoreID 1: %+v", result)
	}
}

func TestAllocatorNoAvailableDisks(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper, _, _, a, _ := createTestAllocator(ctx, 1, false /* deterministic */)
	defer stopper.Stop(ctx)
	result, _, err := a.AllocateVoter(
		ctx,
		simpleSpanConfig,
		nil /* existingVoters */, nil, /* existingNonVoters */
	)
	if !roachpb.Empty(result) {
		t.Errorf("expected nil result: %+v", result)
	}
	if err == nil {
		t.Errorf("allocation succeeded despite there being no available disks: %v", result)
	}
}

func TestAllocatorTwoDatacenters(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper, g, _, a, _ := createTestAllocator(ctx, 1, false /* deterministic */)
	defer stopper.Stop(ctx)
	gossiputil.NewStoreGossiper(g).GossipStores(multiDCStores, t)
	result1, _, err := a.AllocateVoter(
		ctx,
		multiDCConfigSSD,
		nil /* existingVoters */, nil, /* existingNonVoters */
	)
	if err != nil {
		t.Fatalf("Unable to perform allocation: %+v", err)
	}
	result2, _, err := a.AllocateVoter(
		ctx,
		multiDCConfigSSD,
		[]roachpb.ReplicaDescriptor{{
			NodeID:  result1.NodeID,
			StoreID: result1.StoreID,
		}}, nil, /* existingNonVoters */
	)
	if err != nil {
		t.Fatalf("Unable to perform allocation: %+v", err)
	}
	ids := []int{int(result1.NodeID), int(result2.NodeID)}
	sort.Ints(ids)
	if expected := []int{1, 2}; !reflect.DeepEqual(ids, expected) {
		t.Errorf("Expected nodes %+v: %+v vs %+v", expected, result1.NodeID, result2.NodeID)
	}
	// Verify that no result is forthcoming if we already have a replica.
	result3, _, err := a.AllocateVoter(
		ctx,
		multiDCConfigSSD,
		[]roachpb.ReplicaDescriptor{
			{
				NodeID:  result1.NodeID,
				StoreID: result1.StoreID,
			},
			{
				NodeID:  result2.NodeID,
				StoreID: result2.StoreID,
			},
		}, nil, /* existingNonVoters */
	)
	if err == nil {
		t.Errorf("expected error on allocation without available stores: %+v", result3)
	}
}

func TestAllocatorExistingReplica(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper, g, _, a, _ := createTestAllocator(ctx, 1, false /* deterministic */)
	defer stopper.Stop(ctx)
	gossiputil.NewStoreGossiper(g).GossipStores(sameDCStores, t)
	result, _, err := a.AllocateVoter(
		ctx,
		roachpb.SpanConfig{
			NumReplicas: 0,
			Constraints: []roachpb.ConstraintsConjunction{
				{
					Constraints: []roachpb.Constraint{
						{Value: "a", Type: roachpb.Constraint_REQUIRED},
						{Value: "hdd", Type: roachpb.Constraint_REQUIRED},
					},
				},
			},
		},
		[]roachpb.ReplicaDescriptor{
			{
				NodeID:  2,
				StoreID: 2,
			},
		}, nil, /* existingNonVoters */
	)
	if err != nil {
		t.Fatalf("Unable to perform allocation: %+v", err)
	}
	if !(result.StoreID == 3 || result.StoreID == 4) {
		t.Errorf("expected result to have store ID 3 or 4: %+v", result)
	}
}

func TestAllocatorMultipleStoresPerNode(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	stores := []*roachpb.StoreDescriptor{
		{
			StoreID:  1,
			Node:     roachpb.NodeDescriptor{NodeID: 1},
			Capacity: roachpb.StoreCapacity{Capacity: 200, Available: 100, RangeCount: 600},
		},
		{
			StoreID:  2,
			Node:     roachpb.NodeDescriptor{NodeID: 1},
			Capacity: roachpb.StoreCapacity{Capacity: 200, Available: 100, RangeCount: 500},
		},
		{
			StoreID:  3,
			Node:     roachpb.NodeDescriptor{NodeID: 2},
			Capacity: roachpb.StoreCapacity{Capacity: 200, Available: 100, RangeCount: 400},
		},
		{
			StoreID:  4,
			Node:     roachpb.NodeDescriptor{NodeID: 2},
			Capacity: roachpb.StoreCapacity{Capacity: 200, Available: 100, RangeCount: 300},
		},
		{
			StoreID:  5,
			Node:     roachpb.NodeDescriptor{NodeID: 3},
			Capacity: roachpb.StoreCapacity{Capacity: 200, Available: 100, RangeCount: 200},
		},
		{
			StoreID:  6,
			Node:     roachpb.NodeDescriptor{NodeID: 3},
			Capacity: roachpb.StoreCapacity{Capacity: 200, Available: 100, RangeCount: 100},
		},
	}

	ctx := context.Background()
	stopper, g, _, a, _ := createTestAllocator(ctx, 10, false /* deterministic */)
	defer stopper.Stop(ctx)
	gossiputil.NewStoreGossiper(g).GossipStores(stores, t)

	testCases := []struct {
		existing              []roachpb.ReplicaDescriptor
		expectTargetAllocate  bool
		expectTargetRebalance bool
	}{
		{
			existing: []roachpb.ReplicaDescriptor{
				{NodeID: 1, StoreID: 1},
			},
			expectTargetAllocate:  true,
			expectTargetRebalance: true,
		},
		{
			existing: []roachpb.ReplicaDescriptor{
				{NodeID: 1, StoreID: 2},
				{NodeID: 2, StoreID: 3},
			},
			expectTargetAllocate:  true,
			expectTargetRebalance: true,
		},
		{
			existing: []roachpb.ReplicaDescriptor{
				{NodeID: 1, StoreID: 2},
				{NodeID: 3, StoreID: 6},
			},
			expectTargetAllocate:  true,
			expectTargetRebalance: true,
		},
		{
			existing: []roachpb.ReplicaDescriptor{
				{NodeID: 1, StoreID: 1},
				{NodeID: 2, StoreID: 3},
				{NodeID: 3, StoreID: 5},
			},
			expectTargetAllocate:  false,
			expectTargetRebalance: true,
		},
		{
			existing: []roachpb.ReplicaDescriptor{
				{NodeID: 1, StoreID: 2},
				{NodeID: 2, StoreID: 4},
				{NodeID: 3, StoreID: 6},
			},
			expectTargetAllocate:  false,
			expectTargetRebalance: false,
		},
	}

	for _, tc := range testCases {
		{
			result, _, err := a.AllocateVoter(
				ctx, emptySpanConfig(), tc.existing, nil,
			)
			if e, a := tc.expectTargetAllocate, !roachpb.Empty(result); e != a {
				t.Errorf(
					"AllocateVoter(%v) got target %v, err %v; expectTarget=%v",
					tc.existing, result, err, tc.expectTargetAllocate,
				)
			}
		}

		{
			var rangeUsageInfo RangeUsageInfo
			target, _, details, ok := a.RebalanceVoter(
				ctx,
				emptySpanConfig(),
				nil,
				tc.existing,
				nil,
				rangeUsageInfo,
				storeFilterThrottled,
				a.scorerOptions(),
			)
			if e, a := tc.expectTargetRebalance, ok; e != a {
				t.Errorf(
					"RebalanceVoter(%v) got target %v, details %v; expectTarget=%v",
					tc.existing, target, details, tc.expectTargetRebalance,
				)
			}
		}
	}
}

func TestAllocatorMultipleStoresPerNodeLopsided(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	store1 := roachpb.StoreDescriptor{
		StoreID:  1,
		Node:     roachpb.NodeDescriptor{NodeID: 1},
		Capacity: roachpb.StoreCapacity{Capacity: 200, Available: 100, RangeCount: 40},
	}
	store2 := roachpb.StoreDescriptor{
		StoreID:  2,
		Node:     roachpb.NodeDescriptor{NodeID: 1},
		Capacity: roachpb.StoreCapacity{Capacity: 200, Available: 100, RangeCount: 0},
	}

	// We start out with 40 ranges on 3 nodes and 3 stores, we then add a new store
	// on Node 1 and try to rebalance all the ranges. What we want to see happen
	// is an equilibrium where 20 ranges move from Store 1 to Store 2.
	stores := []*roachpb.StoreDescriptor{
		&store1,
		&store2,
		{
			StoreID:  3,
			Node:     roachpb.NodeDescriptor{NodeID: 2},
			Capacity: roachpb.StoreCapacity{Capacity: 200, Available: 100, RangeCount: 40},
		},
		{
			StoreID:  4,
			Node:     roachpb.NodeDescriptor{NodeID: 3},
			Capacity: roachpb.StoreCapacity{Capacity: 200, Available: 100, RangeCount: 40},
		},
	}

	ranges := make([]roachpb.RangeDescriptor, 40)
	for i := 0; i < 40; i++ {
		ranges[i] = roachpb.RangeDescriptor{
			InternalReplicas: []roachpb.ReplicaDescriptor{
				{NodeID: 1, StoreID: 1},
				{NodeID: 2, StoreID: 3},
				{NodeID: 3, StoreID: 4},
			},
		}
	}

	ctx := context.Background()
	stopper, g, _, a, _ := createTestAllocator(ctx, 10, false /* deterministic */)
	defer stopper.Stop(ctx)
	storeGossiper := gossiputil.NewStoreGossiper(g)
	storeGossiper.GossipStores(stores, t)

	// We run through all the ranges once to get the cluster to balance.
	// After that we should not be seeing replicas move.
	var rangeUsageInfo RangeUsageInfo
	for i := 1; i < 40; i++ {
		add, remove, _, ok := a.RebalanceVoter(
			ctx,
			emptySpanConfig(),
			nil,
			ranges[i].InternalReplicas,
			nil,
			rangeUsageInfo,
			storeFilterThrottled,
			a.scorerOptions(),
		)
		if ok {
			// Update the descriptor.
			newReplicas := make([]roachpb.ReplicaDescriptor, 0, len(ranges[i].InternalReplicas))
			for _, repl := range ranges[i].InternalReplicas {
				if remove.StoreID != repl.StoreID {
					newReplicas = append(newReplicas, repl)
				}
			}
			newReplicas = append(newReplicas, roachpb.ReplicaDescriptor{
				StoreID: add.StoreID,
				NodeID:  add.NodeID,
			})
			ranges[i].InternalReplicas = newReplicas

			for _, store := range stores {
				if store.StoreID == add.StoreID {
					store.Capacity.RangeCount = store.Capacity.RangeCount + 1
				} else if store.StoreID == remove.StoreID {
					store.Capacity.RangeCount = store.Capacity.RangeCount - 1
				}
			}
			storeGossiper.GossipStores(stores, t)
		}
	}

	// Verify that the stores are reasonably balanced.
	require.True(t, math.Abs(float64(
		store1.Capacity.RangeCount-store2.Capacity.RangeCount)) <= minRangeRebalanceThreshold*2)
	// We dont expect any range wanting to move since the system should have
	// reached a stable state at this point.
	for i := 1; i < 40; i++ {
		_, _, _, ok := a.RebalanceVoter(
			ctx,
			emptySpanConfig(),
			nil,
			ranges[i].InternalReplicas,
			nil,
			rangeUsageInfo,
			storeFilterThrottled,
			a.scorerOptions(),
		)
		require.False(t, ok)
	}
}

// TestAllocatorRebalance verifies that rebalance targets are chosen
// randomly from amongst stores under the maxFractionUsedThreshold.
func TestAllocatorRebalanceBasedOnRangeCount(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	stores := []*roachpb.StoreDescriptor{
		{
			StoreID:  1,
			Node:     roachpb.NodeDescriptor{NodeID: 1},
			Capacity: roachpb.StoreCapacity{Capacity: 100, Available: 100, RangeCount: 1},
		},
		{
			StoreID:  2,
			Node:     roachpb.NodeDescriptor{NodeID: 2},
			Capacity: roachpb.StoreCapacity{Capacity: 100, Available: 50, RangeCount: 1},
		},
		{
			StoreID: 3,
			Node:    roachpb.NodeDescriptor{NodeID: 3},
			Capacity: roachpb.StoreCapacity{
				Capacity:   100,
				Available:  100 - int64(100*maxFractionUsedThreshold),
				RangeCount: 5,
			},
		},
		{
			// This store must not be rebalanced to, because it's too full.
			StoreID: 4,
			Node:    roachpb.NodeDescriptor{NodeID: 4},
			Capacity: roachpb.StoreCapacity{
				Capacity:   100,
				Available:  (100 - int64(100*maxFractionUsedThreshold)) / 2,
				RangeCount: 10,
			},
		},
		{
			// This store will not be rebalanced to, because it already has more
			// replicas than the mean range count.
			StoreID: 5,
			Node:    roachpb.NodeDescriptor{NodeID: 5},
			Capacity: roachpb.StoreCapacity{
				Capacity:   100,
				Available:  100,
				RangeCount: 10,
			},
		},
	}

	ctx := context.Background()
	stopper, g, _, a, _ := createTestAllocator(ctx, 10, false /* deterministic */)
	defer stopper.Stop(ctx)

	gossiputil.NewStoreGossiper(g).GossipStores(stores, t)

	// Every rebalance target must be either store 1 or 2.
	for i := 0; i < 10; i++ {
		var rangeUsageInfo RangeUsageInfo
		target, _, _, ok := a.RebalanceVoter(
			ctx,
			emptySpanConfig(),
			nil,
			[]roachpb.ReplicaDescriptor{{NodeID: 3, StoreID: 3}},
			nil,
			rangeUsageInfo,
			storeFilterThrottled,
			a.scorerOptions(),
		)
		if !ok {
			i-- // loop until we find 10 candidates
			continue
		}
		// We might not get a rebalance target if the random nodes selected as
		// candidates are not suitable targets.
		if target.StoreID != 1 && target.StoreID != 2 {
			t.Errorf("%d: expected store 1 or 2; got %d", i, target.StoreID)
		}
	}

	sl, _, _ := a.storePool.getStoreList(storeFilterThrottled)
	eqClass := equivalenceClass{
		candidateSL: sl,
	}
	// Verify shouldRebalanceBasedOnThresholds results.
	for i, store := range stores {
		desc, ok := a.storePool.getStoreDescriptor(store.StoreID)
		if !ok {
			t.Fatalf("%d: unable to get store %d descriptor", i, store.StoreID)
		}
		eqClass.existing = desc
		result := a.scorerOptions().shouldRebalanceBasedOnThresholds(ctx, eqClass)
		if expResult := (i >= 2); expResult != result {
			t.Errorf("%d: expected rebalance %t; got %t; desc %+v; sl: %+v", i, expResult, result, desc, sl)
		}
	}
}

// TestAllocatorRebalanceTarget could help us to verify whether we'll rebalance
// to a target that we'll immediately remove.
func TestAllocatorRebalanceTarget(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	manual := hlc.NewManualClock(123)
	clock := hlc.NewClock(manual.UnixNano, time.Nanosecond)
	ctx := context.Background()
	stopper, g, _, a, _ := createTestAllocator(ctx, 5, false /* deterministic */)
	defer stopper.Stop(ctx)
	// We make 5 stores in this test -- 3 in the same datacenter, and 1 each in
	// 2 other datacenters. All of our replicas are distributed within these 3
	// datacenters. Originally, the stores that are all alone in their datacenter
	// are fuller than the other stores. If we didn't simulate RemoveVoter in
	// RebalanceVoter, we would try to choose store 2 or 3 as the target store
	// to make a rebalance. However, we would immediately remove the replica on
	// store 1 or 2 to retain the locality diversity.
	stores := []*roachpb.StoreDescriptor{
		{
			StoreID: 1,
			Node: roachpb.NodeDescriptor{
				NodeID: 1,
				Locality: roachpb.Locality{
					Tiers: []roachpb.Tier{
						{Key: "datacenter", Value: "a"},
					},
				},
			},
			Capacity: roachpb.StoreCapacity{
				RangeCount: 50,
			},
		},
		{
			StoreID: 2,
			Node: roachpb.NodeDescriptor{
				NodeID: 2,
				Locality: roachpb.Locality{
					Tiers: []roachpb.Tier{
						{Key: "datacenter", Value: "a"},
					},
				},
			},
			Capacity: roachpb.StoreCapacity{
				RangeCount: 55,
			},
		},
		{
			StoreID: 3,
			Node: roachpb.NodeDescriptor{
				NodeID: 3,
				Locality: roachpb.Locality{
					Tiers: []roachpb.Tier{
						{Key: "datacenter", Value: "a"},
					},
				},
			},
			Capacity: roachpb.StoreCapacity{
				RangeCount: 55,
			},
		},
		{
			StoreID: 4,
			Node: roachpb.NodeDescriptor{
				NodeID: 4,
				Locality: roachpb.Locality{
					Tiers: []roachpb.Tier{
						{Key: "datacenter", Value: "b"},
					},
				},
			},
			Capacity: roachpb.StoreCapacity{
				RangeCount: 100,
			},
		},
		{
			StoreID: 5,
			Node: roachpb.NodeDescriptor{
				NodeID: 5,
				Locality: roachpb.Locality{
					Tiers: []roachpb.Tier{
						{Key: "datacenter", Value: "c"},
					},
				},
			},
			Capacity: roachpb.StoreCapacity{
				RangeCount: 100,
			},
		},
	}
	sg := gossiputil.NewStoreGossiper(g)
	sg.GossipStores(stores, t)

	replicas := []roachpb.ReplicaDescriptor{
		{NodeID: 1, StoreID: 1, ReplicaID: 1},
		{NodeID: 4, StoreID: 4, ReplicaID: 4},
		{NodeID: 5, StoreID: 5, ReplicaID: 5},
	}
	repl := &Replica{RangeID: firstRangeID}

	repl.mu.Lock()
	repl.mu.state.Stats = &enginepb.MVCCStats{}
	repl.mu.Unlock()

	repl.leaseholderStats = newReplicaStats(clock, nil)
	repl.writeStats = newReplicaStats(clock, nil)

	var rangeUsageInfo RangeUsageInfo

	status := &raft.Status{
		Progress: make(map[uint64]tracker.Progress),
	}
	status.Commit = 10
	for _, replica := range replicas {
		status.Progress[uint64(replica.ReplicaID)] = tracker.Progress{
			Match: 10,
			State: tracker.StateReplicate,
		}
	}
	for i := 0; i < 10; i++ {
		result, _, details, ok := a.RebalanceVoter(
			ctx,
			emptySpanConfig(),
			status,
			replicas,
			nil,
			rangeUsageInfo,
			storeFilterThrottled,
			a.scorerOptions(),
		)
		if ok {
			t.Fatalf("expected no rebalance, but got target s%d; details: %s", result.StoreID, details)
		}
	}

	// Set up a second round of testing where the other two stores in the big
	// locality actually have fewer replicas, but enough that it still isn't worth
	// rebalancing to them. We create a situation where the replacement candidates
	// for s1 (i.e. s2 and s3) have an average of 48 replicas each (leading to an
	// overfullness threshold of 51, which is greater than the replica count of
	// s1).
	stores[1].Capacity.RangeCount = 48
	stores[2].Capacity.RangeCount = 48
	sg.GossipStores(stores, t)
	for i := 0; i < 10; i++ {
		target, _, details, ok := a.RebalanceVoter(
			ctx,
			emptySpanConfig(),
			status,
			replicas,
			nil,
			rangeUsageInfo,
			storeFilterThrottled,
			a.scorerOptions(),
		)
		if ok {
			t.Fatalf("expected no rebalance, but got target s%d; details: %s", target.StoreID, details)
		}
	}

	// Make sure rebalancing does happen if we drop just a little further down.
	stores[1].Capacity.RangeCount = 44
	sg.GossipStores(stores, t)
	for i := 0; i < 10; i++ {
		target, origin, details, ok := a.RebalanceVoter(
			ctx,
			emptySpanConfig(),
			status,
			replicas,
			nil,
			rangeUsageInfo,
			storeFilterThrottled,
			a.scorerOptions(),
		)
		expTo := stores[1].StoreID
		expFrom := stores[0].StoreID
		if !ok || target.StoreID != expTo || origin.StoreID != expFrom {
			t.Fatalf("%d: expected rebalance from either of %v to s%d, but got %v->%v; details: %s",
				i, expFrom, expTo, origin, target, details)
		}
	}
}

func TestAllocatorRebalanceDeadNodes(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper, _, sp, a, _ := createTestAllocator(ctx, 8, false /* deterministic */)
	defer stopper.Stop(ctx)

	mockStorePool(
		sp,
		[]roachpb.StoreID{1, 2, 3, 4, 5, 6},
		nil,
		[]roachpb.StoreID{7, 8},
		nil,
		nil,
		nil,
	)

	ranges := func(rangeCount int32) roachpb.StoreCapacity {
		return roachpb.StoreCapacity{
			Capacity:     1000,
			Available:    1000,
			LogicalBytes: 0,
			RangeCount:   rangeCount,
		}
	}

	// Initialize 8 stores: where store 6 is the target for rebalancing.
	sp.detailsMu.Lock()
	sp.getStoreDetailLocked(1).desc.Capacity = ranges(100)
	sp.getStoreDetailLocked(2).desc.Capacity = ranges(100)
	sp.getStoreDetailLocked(3).desc.Capacity = ranges(100)
	sp.getStoreDetailLocked(4).desc.Capacity = ranges(100)
	sp.getStoreDetailLocked(5).desc.Capacity = ranges(100)
	sp.getStoreDetailLocked(6).desc.Capacity = ranges(0)
	sp.getStoreDetailLocked(7).desc.Capacity = ranges(100)
	sp.getStoreDetailLocked(8).desc.Capacity = ranges(100)
	sp.detailsMu.Unlock()

	// Each test case should describe a repair situation which has a lower
	// priority than the previous test case.
	testCases := []struct {
		existing []roachpb.ReplicaDescriptor
		expected roachpb.StoreID
	}{
		// 3/3 live -> 3/4 live: ok
		{replicas(1, 2, 3), 6},
		// 4/4 live -> 4/5 live: ok
		{replicas(1, 2, 3, 4), 6},
		// 3/4 live -> 3/5 live: ok
		{replicas(1, 2, 3, 7), 6},
		// 5/5 live -> 5/6 live: ok
		{replicas(1, 2, 3, 4, 5), 6},
		// 4/5 live -> 4/6 live: ok
		{replicas(1, 2, 3, 4, 7), 6},
	}

	for _, c := range testCases {
		t.Run("", func(t *testing.T) {
			var rangeUsageInfo RangeUsageInfo
			target, _, _, ok := a.RebalanceVoter(
				ctx,
				emptySpanConfig(),
				nil,
				c.existing,
				nil,
				rangeUsageInfo,
				storeFilterThrottled,
				a.scorerOptions(),
			)
			if c.expected > 0 {
				if !ok {
					t.Fatalf("expected %d, but found nil", c.expected)
				} else if c.expected != target.StoreID {
					t.Fatalf("expected %d, but found %d", c.expected, target.StoreID)
				}
			} else if ok {
				t.Fatalf("expected nil, but found %d", target.StoreID)
			}
		})
	}
}

// TestAllocatorRebalanceThrashing tests that the rebalancer does not thrash
// when replica counts are balanced, within the appropriate thresholds, across
// stores.
func TestAllocatorRebalanceThrashing(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	type testStore struct {
		rangeCount          int32
		shouldRebalanceFrom bool
	}

	// Returns a slice of stores with the specified mean. The first replica will
	// have a range count that's above the target range count for the rebalancer,
	// so it should be rebalanced from.
	oneStoreAboveRebalanceTarget := func(mean int32, numStores int) func(*cluster.Settings) []testStore {
		return func(st *cluster.Settings) []testStore {
			stores := make([]testStore, numStores)
			for i := range stores {
				stores[i].rangeCount = mean
			}
			surplus := int32(math.Ceil(float64(mean)*rangeRebalanceThreshold.Get(&st.SV) + 1))
			stores[0].rangeCount += surplus
			stores[0].shouldRebalanceFrom = true
			for i := 1; i < len(stores); i++ {
				stores[i].rangeCount -= int32(math.Ceil(float64(surplus) / float64(len(stores)-1)))
			}
			return stores
		}
	}

	// Returns a slice of stores with the specified mean such that the first store
	// has few enough replicas to make it a rebalance target.
	oneUnderusedStore := func(mean int32, numStores int) func(*cluster.Settings) []testStore {
		return func(st *cluster.Settings) []testStore {
			stores := make([]testStore, numStores)
			for i := range stores {
				stores[i].rangeCount = mean
			}
			// Subtract enough ranges from the first store to make it a suitable
			// rebalance target. To maintain the specified mean, we then add that delta
			// back to the rest of the replicas.
			deficit := int32(math.Ceil(float64(mean)*rangeRebalanceThreshold.Get(&st.SV) + 1))
			stores[0].rangeCount -= deficit
			for i := 1; i < len(stores); i++ {
				stores[i].rangeCount += int32(math.Ceil(float64(deficit) / float64(len(stores)-1)))
				stores[i].shouldRebalanceFrom = true
			}
			return stores
		}
	}

	// Each test case defines the range counts for the test stores and whether we
	// should rebalance from the store.
	testCases := []struct {
		name    string
		cluster func(*cluster.Settings) []testStore
	}{
		// An evenly balanced cluster should not rebalance.
		{"balanced", func(*cluster.Settings) []testStore {
			return []testStore{{5, false}, {5, false}, {5, false}, {5, false}}
		}},
		// Adding an empty node to a 3-node cluster triggers rebalancing from
		// existing nodes.
		{"empty-node", func(*cluster.Settings) []testStore {
			return []testStore{{100, true}, {100, true}, {100, true}, {0, false}}
		}},
		// A cluster where all range counts are within rangeRebalanceThreshold should
		// not rebalance. This assumes rangeRebalanceThreshold > 2%.
		{"within-threshold", func(*cluster.Settings) []testStore {
			return []testStore{{98, false}, {99, false}, {101, false}, {102, false}}
		}},
		{"5-stores-mean-100-one-above", oneStoreAboveRebalanceTarget(100, 5)},
		{"5-stores-mean-1000-one-above", oneStoreAboveRebalanceTarget(1000, 5)},
		{"5-stores-mean-10000-one-above", oneStoreAboveRebalanceTarget(10000, 5)},

		{"5-stores-mean-1000-one-underused", oneUnderusedStore(1000, 5)},
		{"10-stores-mean-1000-one-underused", oneUnderusedStore(1000, 10)},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Deterministic is required when stressing as test case 8 may rebalance
			// to different configurations.
			ctx := context.Background()
			stopper, g, _, a, _ := createTestAllocator(ctx, 1, true /* deterministic */)
			defer stopper.Stop(ctx)

			st := a.storePool.st
			cluster := tc.cluster(st)

			// It doesn't make sense to test sets of stores containing fewer than 4
			// stores, because 4 stores is the minimum number of stores needed to
			// trigger rebalancing with the default replication factor of 3. Also, the
			// above local functions need a minimum number of stores to properly create
			// the desired distribution of range counts.
			const minStores = 4
			if numStores := len(cluster); numStores < minStores {
				t.Fatalf("numStores %d < min %d", numStores, minStores)
			}

			// Create stores with the range counts from the test case and gossip them.
			var stores []*roachpb.StoreDescriptor
			for j, store := range cluster {
				stores = append(stores, &roachpb.StoreDescriptor{
					StoreID:  roachpb.StoreID(j + 1),
					Node:     roachpb.NodeDescriptor{NodeID: roachpb.NodeID(j + 1)},
					Capacity: roachpb.StoreCapacity{Capacity: 1, Available: 1, RangeCount: store.rangeCount},
				})
			}
			gossiputil.NewStoreGossiper(g).GossipStores(stores, t)

			// Ensure gossiped store descriptor changes have propagated.
			testutils.SucceedsSoon(t, func() error {
				sl, _, _ := a.storePool.getStoreList(storeFilterThrottled)
				for j, s := range sl.stores {
					if a, e := s.Capacity.RangeCount, cluster[j].rangeCount; a != e {
						return errors.Errorf("range count for %d = %d != expected %d", j, a, e)
					}
				}
				return nil
			})
			sl, _, _ := a.storePool.getStoreList(storeFilterThrottled)
			eqClass := equivalenceClass{
				candidateSL: sl,
			}
			// Verify shouldRebalanceBasedOnThresholds returns the expected value.
			for j, store := range stores {
				desc, ok := a.storePool.getStoreDescriptor(store.StoreID)
				if !ok {
					t.Fatalf("[store %d]: unable to get store %d descriptor", j, store.StoreID)
				}
				eqClass.existing = desc
				if a, e := a.scorerOptions().shouldRebalanceBasedOnThresholds(
					context.Background(), eqClass,
				), cluster[j].shouldRebalanceFrom; a != e {
					t.Errorf(
						"[store %d]: shouldRebalanceBasedOnThresholds %t != expected %t", store.StoreID, a, e,
					)
				}
			}
		})
	}
}

// TestAllocatorRebalanceByQPS tests that the allocator rebalances replicas
// based on QPS if there are underfull or overfull stores in the cluster.
func TestAllocatorRebalanceByQPS(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	allStoresEqual := []*roachpb.StoreDescriptor{
		{
			StoreID:  1,
			Node:     roachpb.NodeDescriptor{NodeID: 1},
			Capacity: roachpb.StoreCapacity{RangeCount: 1000, QueriesPerSecond: 1000},
		},
		{
			StoreID:  2,
			Node:     roachpb.NodeDescriptor{NodeID: 2},
			Capacity: roachpb.StoreCapacity{RangeCount: 1000, QueriesPerSecond: 1000},
		},
		{
			StoreID:  3,
			Node:     roachpb.NodeDescriptor{NodeID: 3},
			Capacity: roachpb.StoreCapacity{RangeCount: 1000, QueriesPerSecond: 1000},
		},
		{
			StoreID:  4,
			Node:     roachpb.NodeDescriptor{NodeID: 4},
			Capacity: roachpb.StoreCapacity{RangeCount: 1000, QueriesPerSecond: 1000},
		},
	}

	allStoresAroundTheMean := []*roachpb.StoreDescriptor{
		{
			StoreID:  1,
			Node:     roachpb.NodeDescriptor{NodeID: 1},
			Capacity: roachpb.StoreCapacity{RangeCount: 1000, QueriesPerSecond: 1100},
		},
		{
			StoreID:  2,
			Node:     roachpb.NodeDescriptor{NodeID: 2},
			Capacity: roachpb.StoreCapacity{RangeCount: 1000, QueriesPerSecond: 1000},
		},
		{
			StoreID:  3,
			Node:     roachpb.NodeDescriptor{NodeID: 3},
			Capacity: roachpb.StoreCapacity{RangeCount: 1000, QueriesPerSecond: 900},
		},
		{
			StoreID:  4,
			Node:     roachpb.NodeDescriptor{NodeID: 4},
			Capacity: roachpb.StoreCapacity{RangeCount: 1000, QueriesPerSecond: 1000},
		},
	}

	oneOverfullAndOneUnderfull := []*roachpb.StoreDescriptor{
		{
			StoreID:  1,
			Node:     roachpb.NodeDescriptor{NodeID: 1},
			Capacity: roachpb.StoreCapacity{RangeCount: 1000, QueriesPerSecond: 1300},
		},
		{
			StoreID:  2,
			Node:     roachpb.NodeDescriptor{NodeID: 2},
			Capacity: roachpb.StoreCapacity{RangeCount: 1000, QueriesPerSecond: 1000},
		},
		{
			StoreID:  3,
			Node:     roachpb.NodeDescriptor{NodeID: 3},
			Capacity: roachpb.StoreCapacity{RangeCount: 1000, QueriesPerSecond: 1000},
		},
		{
			StoreID:  4,
			Node:     roachpb.NodeDescriptor{NodeID: 4},
			Capacity: roachpb.StoreCapacity{RangeCount: 1000, QueriesPerSecond: 700},
		},
	}

	type testCase struct {
		testStores                              []*roachpb.StoreDescriptor
		expectRebalance                         bool
		expectedAddTarget, expectedRemoveTarget roachpb.StoreID
	}
	tests := []testCase{
		{
			// We don't expect any QPS based rebalancing when all stores are serving
			// the same QPS.
			testStores:      allStoresEqual,
			expectRebalance: false,
		},
		{
			// We don't expect any QPS based rebalancing when all stores are "close
			// enough" to the mean.
			testStores:      allStoresAroundTheMean,
			expectRebalance: false,
		},
		{
			// When one store is overfull and another is underfull, we expect a QPS
			// based rebalance from the overfull store to the underfull store.
			testStores:           oneOverfullAndOneUnderfull,
			expectRebalance:      true,
			expectedRemoveTarget: roachpb.StoreID(1),
			expectedAddTarget:    roachpb.StoreID(4),
		},
	}

	for _, subtest := range tests {
		ctx := context.Background()
		stopper, g, _, a, _ := createTestAllocator(ctx, 10, false /* deterministic */)
		defer stopper.Stop(ctx)
		gossiputil.NewStoreGossiper(g).GossipStores(subtest.testStores, t)
		var rangeUsageInfo RangeUsageInfo
		options := &qpsScorerOptions{
			qpsPerReplica:         100,
			qpsRebalanceThreshold: 0.2,
		}
		add, remove, _, ok := a.RebalanceVoter(
			ctx,
			emptySpanConfig(),
			nil,
			[]roachpb.ReplicaDescriptor{{StoreID: subtest.testStores[0].StoreID}},
			nil,
			rangeUsageInfo,
			storeFilterThrottled,
			options,
		)
		if subtest.expectRebalance {
			require.True(t, ok)
			require.Equal(t, subtest.expectedAddTarget, add.StoreID)
			require.Equal(t, subtest.expectedRemoveTarget, remove.StoreID)
			// Verify shouldRebalanceBasedOnThresholds results.
			if desc, descOk := a.storePool.getStoreDescriptor(remove.StoreID); descOk {
				sl, _, _ := a.storePool.getStoreList(storeFilterThrottled)
				eqClass := equivalenceClass{
					existing:    desc,
					candidateSL: sl,
				}
				result := options.shouldRebalanceBasedOnThresholds(ctx, eqClass)
				require.True(t, result)
			} else {
				t.Fatalf("unable to get store %d descriptor", remove.StoreID)
			}
		} else {
			require.False(t, ok)
		}
	}
}

func TestAllocatorRemoveBasedOnQPS(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	region := func(regionName string) roachpb.Locality {
		return roachpb.Locality{
			Tiers: []roachpb.Tier{
				{Key: "region", Value: regionName},
			},
		}
	}
	twoOverfullStores := []*roachpb.StoreDescriptor{
		{
			StoreID:  1,
			Node:     roachpb.NodeDescriptor{NodeID: 1, Locality: region("a")},
			Capacity: roachpb.StoreCapacity{RangeCount: 1000, QueriesPerSecond: 1200},
		},
		{
			StoreID:  2,
			Node:     roachpb.NodeDescriptor{NodeID: 2, Locality: region("a")},
			Capacity: roachpb.StoreCapacity{RangeCount: 1000, QueriesPerSecond: 1400},
		},
		{
			StoreID:  3,
			Node:     roachpb.NodeDescriptor{NodeID: 3, Locality: region("b")},
			Capacity: roachpb.StoreCapacity{RangeCount: 1000, QueriesPerSecond: 1000},
		},
		{
			StoreID:  4,
			Node:     roachpb.NodeDescriptor{NodeID: 4, Locality: region("c")},
			Capacity: roachpb.StoreCapacity{RangeCount: 1000, QueriesPerSecond: 1000},
		},
		{
			StoreID:  5,
			Node:     roachpb.NodeDescriptor{NodeID: 5, Locality: region("c")},
			Capacity: roachpb.StoreCapacity{RangeCount: 1000, QueriesPerSecond: 800},
		},
	}

	type testCase struct {
		testStores           []*roachpb.StoreDescriptor
		existingRepls        []roachpb.ReplicaDescriptor
		expectedRemoveTarget roachpb.StoreID
	}
	tests := []testCase{
		{
			// Expect store 1 to be removed since it is fielding the most QPS out of
			// all the existing replicas.
			testStores:           twoOverfullStores,
			existingRepls:        replicas(1, 3, 4),
			expectedRemoveTarget: roachpb.StoreID(1),
		},
		{
			// Expect store 2 to be removed since it is serving more QPS than the only
			// other store that's "comparable" to it (store 1).
			testStores:           twoOverfullStores,
			existingRepls:        replicas(1, 2, 3, 4, 5),
			expectedRemoveTarget: roachpb.StoreID(2),
		},
		{
			// Expect store 4 to be removed because it is serving more QPS than store
			// 5, which is its only comparable store.
			testStores:           twoOverfullStores,
			existingRepls:        replicas(2, 3, 4, 5),
			expectedRemoveTarget: roachpb.StoreID(4),
		},
	}

	for _, subtest := range tests {
		ctx := context.Background()
		stopper, g, _, a, _ := createTestAllocator(ctx, 10, false /* deterministic */)
		defer stopper.Stop(ctx)
		gossiputil.NewStoreGossiper(g).GossipStores(subtest.testStores, t)
		options := &qpsScorerOptions{
			qpsRebalanceThreshold: 0.1,
		}
		remove, _, err := a.RemoveVoter(
			ctx,
			emptySpanConfig(),
			subtest.existingRepls,
			subtest.existingRepls,
			nil,
			options,
		)
		require.NoError(t, err)
		require.Equal(t, subtest.expectedRemoveTarget, remove.StoreID)
	}
}

// TestAllocatorRebalanceByCount verifies that rebalance targets are
// chosen by range counts in the event that available capacities
// exceed the maxAvailCapacityThreshold.
func TestAllocatorRebalanceByCount(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Setup the stores so that only one is below the standard deviation threshold.
	stores := []*roachpb.StoreDescriptor{
		{
			StoreID:  1,
			Node:     roachpb.NodeDescriptor{NodeID: 1},
			Capacity: roachpb.StoreCapacity{Capacity: 100, Available: 100, RangeCount: 10},
		},
		{
			StoreID:  2,
			Node:     roachpb.NodeDescriptor{NodeID: 2},
			Capacity: roachpb.StoreCapacity{Capacity: 100, Available: 99, RangeCount: 10},
		},
		{
			StoreID:  3,
			Node:     roachpb.NodeDescriptor{NodeID: 3},
			Capacity: roachpb.StoreCapacity{Capacity: 100, Available: 98, RangeCount: 10},
		},
		{
			StoreID:  4,
			Node:     roachpb.NodeDescriptor{NodeID: 4},
			Capacity: roachpb.StoreCapacity{Capacity: 100, Available: 98, RangeCount: 2},
		},
	}

	ctx := context.Background()
	stopper, g, _, a, _ := createTestAllocator(ctx, 10, false /* deterministic */)
	defer stopper.Stop(ctx)

	gossiputil.NewStoreGossiper(g).GossipStores(stores, t)

	// Every rebalance target must be store 4 (or nil for case of missing the only option).
	for i := 0; i < 10; i++ {
		var rangeUsageInfo RangeUsageInfo
		result, _, _, ok := a.RebalanceVoter(
			ctx,
			emptySpanConfig(),
			nil,
			[]roachpb.ReplicaDescriptor{{StoreID: stores[0].StoreID}},
			nil,
			rangeUsageInfo,
			storeFilterThrottled,
			a.scorerOptions(),
		)
		if ok && result.StoreID != 4 {
			t.Errorf("expected store 4; got %d", result.StoreID)
		}
	}

	// Verify shouldRebalanceBasedOnThresholds results.
	for i, store := range stores {
		desc, ok := a.storePool.getStoreDescriptor(store.StoreID)
		if !ok {
			t.Fatalf("%d: unable to get store %d descriptor", i, store.StoreID)
		}
		sl, _, _ := a.storePool.getStoreList(storeFilterThrottled)
		eqClass := equivalenceClass{
			existing:    desc,
			candidateSL: sl,
		}
		result := a.scorerOptions().shouldRebalanceBasedOnThresholds(ctx, eqClass)
		if expResult := (i < 3); expResult != result {
			t.Errorf("%d: expected rebalance %t; got %t", i, expResult, result)
		}
	}
}

// mockRepl satisfies the interface for the `leaseRepl` passed into
// `Allocator.TransferLeaseTarget()` for these tests.
type mockRepl struct {
	replicationFactor     int32
	storeID               roachpb.StoreID
	replsInNeedOfSnapshot map[roachpb.ReplicaID]struct{}
}

func (r *mockRepl) RaftStatus() *raft.Status {
	raftStatus := &raft.Status{
		Progress: make(map[uint64]tracker.Progress),
	}
	for i := int32(1); i <= r.replicationFactor; i++ {
		state := tracker.StateReplicate
		if _, ok := r.replsInNeedOfSnapshot[roachpb.ReplicaID(i)]; ok {
			state = tracker.StateSnapshot
		}
		raftStatus.Progress[uint64(i)] = tracker.Progress{State: state}
	}
	return raftStatus
}

func (r *mockRepl) StoreID() roachpb.StoreID {
	return r.storeID
}

func (r *mockRepl) GetRangeID() roachpb.RangeID {
	return roachpb.RangeID(0)
}

func (r *mockRepl) markReplAsNeedingSnapshot(id roachpb.ReplicaID) {
	if r.replsInNeedOfSnapshot == nil {
		r.replsInNeedOfSnapshot = make(map[roachpb.ReplicaID]struct{})
	}
	r.replsInNeedOfSnapshot[id] = struct{}{}
}

func TestAllocatorTransferLeaseTarget(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	stopper, g, _, a, _ := createTestAllocator(ctx, 10, true /* deterministic */)
	defer stopper.Stop(ctx)

	// 3 stores where the lease count for each store is equal to 10x the store
	// ID.
	var stores []*roachpb.StoreDescriptor
	for i := 1; i <= 3; i++ {
		stores = append(stores, &roachpb.StoreDescriptor{
			StoreID:  roachpb.StoreID(i),
			Node:     roachpb.NodeDescriptor{NodeID: roachpb.NodeID(i)},
			Capacity: roachpb.StoreCapacity{LeaseCount: int32(10 * i)},
		})
	}
	sg := gossiputil.NewStoreGossiper(g)
	sg.GossipStores(stores, t)

	existing := []roachpb.ReplicaDescriptor{
		{StoreID: 1, ReplicaID: 1},
		{StoreID: 2, ReplicaID: 2},
		{StoreID: 3, ReplicaID: 3},
	}

	// TODO(peter): Add test cases for non-empty constraints.
	testCases := []struct {
		existing    []roachpb.ReplicaDescriptor
		leaseholder roachpb.StoreID
		check       bool
		expected    roachpb.StoreID
	}{
		// No existing lease holder, nothing to do.
		{existing: existing, leaseholder: 0, check: true, expected: 0},
		// Store 1 is not a lease transfer source.
		{existing: existing, leaseholder: 1, check: true, expected: 0},
		{existing: existing, leaseholder: 1, check: false, expected: 2},
		// Store 2 is not a lease transfer source.
		{existing: existing, leaseholder: 2, check: true, expected: 0},
		{existing: existing, leaseholder: 2, check: false, expected: 1},
		// Store 3 is a lease transfer source.
		{existing: existing, leaseholder: 3, check: true, expected: 1},
		{existing: existing, leaseholder: 3, check: false, expected: 1},
	}
	for _, c := range testCases {
		t.Run("", func(t *testing.T) {
			target := a.TransferLeaseTarget(
				ctx,
				emptySpanConfig(),
				c.existing,
				&mockRepl{
					replicationFactor: 3,
					storeID:           c.leaseholder,
				},
				nil,   /* stats */
				false, /* forceDecisionWithoutStats */
				transferLeaseOptions{
					checkTransferLeaseSource: c.check,
					checkCandidateFullness:   true,
				},
			)
			if c.expected != target.StoreID {
				t.Fatalf("expected %d, but found %d", c.expected, target.StoreID)
			}
		})
	}
}

func TestAllocatorTransferLeaseToReplicasNeedingSnapshot(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	existing := []roachpb.ReplicaDescriptor{
		{StoreID: 1, NodeID: 1, ReplicaID: 1},
		{StoreID: 2, NodeID: 2, ReplicaID: 2},
		{StoreID: 3, NodeID: 3, ReplicaID: 3},
		{StoreID: 4, NodeID: 4, ReplicaID: 4},
	}
	ctx := context.Background()
	stopper, g, _, a, _ := createTestAllocator(ctx, 10, true /* deterministic */)
	defer stopper.Stop(ctx)

	// 4 stores where the lease count for each store is equal to 10x the store
	// ID.
	var stores []*roachpb.StoreDescriptor
	for i := 1; i <= 4; i++ {
		stores = append(stores, &roachpb.StoreDescriptor{
			StoreID:  roachpb.StoreID(i),
			Node:     roachpb.NodeDescriptor{NodeID: roachpb.NodeID(i)},
			Capacity: roachpb.StoreCapacity{LeaseCount: int32(10 * i)},
		})
	}
	sg := gossiputil.NewStoreGossiper(g)
	sg.GossipStores(stores, t)

	testCases := []struct {
		existing          []roachpb.ReplicaDescriptor
		replsNeedingSnaps []roachpb.ReplicaID
		leaseholder       roachpb.StoreID
		checkSource       bool
		transferTarget    roachpb.StoreID
	}{
		{
			existing:          existing,
			replsNeedingSnaps: []roachpb.ReplicaID{1},
			leaseholder:       3,
			checkSource:       true,
			transferTarget:    0,
		},
		{
			existing:          existing,
			replsNeedingSnaps: []roachpb.ReplicaID{1},
			leaseholder:       3,
			checkSource:       false,
			transferTarget:    2,
		},
		{
			existing:          existing,
			replsNeedingSnaps: []roachpb.ReplicaID{1},
			leaseholder:       4,
			checkSource:       true,
			transferTarget:    2,
		},
		{
			existing:          existing,
			replsNeedingSnaps: []roachpb.ReplicaID{1},
			leaseholder:       4,
			checkSource:       false,
			transferTarget:    2,
		},
		{
			existing:          existing,
			replsNeedingSnaps: []roachpb.ReplicaID{1, 2},
			leaseholder:       4,
			checkSource:       false,
			transferTarget:    3,
		},
		{
			existing:          existing,
			replsNeedingSnaps: []roachpb.ReplicaID{1, 2},
			leaseholder:       4,
			checkSource:       true,
			transferTarget:    0,
		},
		{
			existing:          existing,
			replsNeedingSnaps: []roachpb.ReplicaID{1, 2, 3},
			leaseholder:       4,
			checkSource:       true,
			transferTarget:    0,
		},
	}

	for _, c := range testCases {
		repl := &mockRepl{
			replicationFactor: 4,
			storeID:           c.leaseholder,
		}
		for _, r := range c.replsNeedingSnaps {
			repl.markReplAsNeedingSnapshot(r)
		}
		t.Run("", func(t *testing.T) {
			target := a.TransferLeaseTarget(
				ctx,
				emptySpanConfig(),
				c.existing,
				repl,
				nil,
				false, /* alwaysAllowDecisionWithoutStats */
				transferLeaseOptions{
					checkTransferLeaseSource: c.checkSource,
					checkCandidateFullness:   true,
				},
			)
			if c.transferTarget != target.StoreID {
				t.Fatalf("expected %d, but found %d", c.transferTarget, target.StoreID)
			}
		})
	}
}

func TestAllocatorTransferLeaseTargetConstraints(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	stopper, g, _, a, _ := createTestAllocator(ctx, 10, true /* deterministic */)
	defer stopper.Stop(ctx)

	// 6 stores with the following setup
	// 1 | locality=dc=1 | lease_count=10
	// 2 | locality=dc=0 | lease_count=0
	// 3 | locality=dc=1 | lease_count=30
	// 4 | locality=dc=0 | lease_count=0
	// 5 | locality=dc=1 | lease_count=50
	// 6 | locality=dc=0 | lease_count=0
	var stores []*roachpb.StoreDescriptor
	for i := 1; i <= 6; i++ {
		stores = append(stores, &roachpb.StoreDescriptor{
			StoreID: roachpb.StoreID(i),
			Node: roachpb.NodeDescriptor{
				NodeID: roachpb.NodeID(i),
				Locality: roachpb.Locality{
					Tiers: []roachpb.Tier{
						{Key: "dc", Value: strconv.FormatInt(int64(i%2), 10)},
					},
				},
			},
			Capacity: roachpb.StoreCapacity{LeaseCount: int32(100 * i * (i % 2))},
		})
	}
	sg := gossiputil.NewStoreGossiper(g)
	sg.GossipStores(stores, t)

	existing := replicas(1, 3, 5)

	constraint := func(value string) []roachpb.ConstraintsConjunction {
		return []roachpb.ConstraintsConjunction{
			{
				Constraints: []roachpb.Constraint{
					{Key: "dc", Value: value, Type: roachpb.Constraint_REQUIRED},
				},
			},
		}
	}

	constraints := func(value string) roachpb.SpanConfig {
		return roachpb.SpanConfig{
			NumReplicas: 1,
			Constraints: constraint(value),
		}
	}

	voterConstraints := func(value string) roachpb.SpanConfig {
		return roachpb.SpanConfig{
			NumReplicas:      1,
			VoterConstraints: constraint(value),
		}
	}

	testCases := []struct {
		existing    []roachpb.ReplicaDescriptor
		leaseholder roachpb.StoreID
		expected    roachpb.StoreID
		conf        roachpb.SpanConfig
	}{
		{existing: existing, leaseholder: 5, expected: 1, conf: constraints("1")},
		{existing: existing, leaseholder: 5, expected: 1, conf: voterConstraints("1")},
		{existing: existing, leaseholder: 5, expected: 0, conf: constraints("0")},
		{existing: existing, leaseholder: 5, expected: 0, conf: voterConstraints("0")},
		{existing: existing, leaseholder: 5, expected: 1, conf: emptySpanConfig()},
	}
	for _, c := range testCases {
		t.Run("", func(t *testing.T) {
			target := a.TransferLeaseTarget(
				context.Background(),
				c.conf,
				c.existing,
				&mockRepl{
					replicationFactor: 3,
					storeID:           c.leaseholder,
				},
				nil,   /* stats */
				false, /* forceDecisionWithoutStats */
				transferLeaseOptions{
					checkTransferLeaseSource: true,
					checkCandidateFullness:   true,
				},
			)
			if c.expected != target.StoreID {
				t.Fatalf("expected %d, but found %d", c.expected, target.StoreID)
			}
		})
	}
}

// TestAllocatorTransferLeaseTargetDraining verifies that the allocator will
// not choose to transfer leases to a store that is draining.
func TestAllocatorTransferLeaseTargetDraining(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	stopper, g, _, storePool, nl := createTestStorePool(ctx,
		TestTimeUntilStoreDeadOff, true, /* deterministic */
		func() int { return 10 }, /* nodeCount */
		livenesspb.NodeLivenessStatus_LIVE)
	a := MakeAllocator(
		storePool, func(string) (time.Duration, bool) {
			return 0, true
		}, nil, /* knobs */
	)
	defer stopper.Stop(ctx)

	// 3 stores where the lease count for each store is equal to 100x the store
	// ID. We'll be draining the store with the fewest leases on it.
	var stores []*roachpb.StoreDescriptor
	for i := 1; i <= 3; i++ {
		stores = append(stores, &roachpb.StoreDescriptor{
			StoreID: roachpb.StoreID(i),
			Attrs:   roachpb.Attributes{Attrs: []string{fmt.Sprintf("s%d", i)}},
			Node: roachpb.NodeDescriptor{
				NodeID: roachpb.NodeID(i),
				Attrs:  roachpb.Attributes{Attrs: []string{fmt.Sprintf("n%d", i)}},
				Locality: roachpb.Locality{
					Tiers: []roachpb.Tier{
						{Key: "dc", Value: strconv.Itoa(i)},
						{Key: "region", Value: strconv.Itoa(i % 2)},
					},
				},
			},
			Capacity: roachpb.StoreCapacity{LeaseCount: int32(100 * i)},
		})
	}
	sg := gossiputil.NewStoreGossiper(g)
	sg.GossipStores(stores, t)

	nl.setNodeStatus(1, livenesspb.NodeLivenessStatus_DRAINING)
	preferDC1 := []roachpb.LeasePreference{
		{Constraints: []roachpb.Constraint{{Key: "dc", Value: "1", Type: roachpb.Constraint_REQUIRED}}},
	}

	// This means odd nodes.
	preferRegion1 := []roachpb.LeasePreference{
		{Constraints: []roachpb.Constraint{{Key: "region", Value: "1", Type: roachpb.Constraint_REQUIRED}}},
	}

	existing := []roachpb.ReplicaDescriptor{
		{StoreID: 1, ReplicaID: 1},
		{StoreID: 2, ReplicaID: 2},
		{StoreID: 3, ReplicaID: 3},
	}

	testCases := []struct {
		existing    []roachpb.ReplicaDescriptor
		leaseholder roachpb.StoreID
		check       bool
		expected    roachpb.StoreID
		conf        roachpb.SpanConfig
	}{
		// No existing lease holder, nothing to do.
		{existing: existing, leaseholder: 0, check: true, expected: 0, conf: emptySpanConfig()},
		// Store 1 is draining, so it will try to transfer its lease if
		// checkTransferLeaseSource is false. This behavior isn't relied upon,
		// though; leases are manually transferred when draining.
		{existing: existing, leaseholder: 1, check: true, expected: 0, conf: emptySpanConfig()},
		{existing: existing, leaseholder: 1, check: false, expected: 2, conf: emptySpanConfig()},
		// Store 2 is not a lease transfer source.
		{existing: existing, leaseholder: 2, check: true, expected: 0, conf: emptySpanConfig()},
		{existing: existing, leaseholder: 2, check: false, expected: 3, conf: emptySpanConfig()},
		// Store 3 is a lease transfer source, but won't transfer to
		// node 1 because it's draining.
		{existing: existing, leaseholder: 3, check: true, expected: 2, conf: emptySpanConfig()},
		{existing: existing, leaseholder: 3, check: false, expected: 2, conf: emptySpanConfig()},
		// Verify that lease preferences dont impact draining
		{existing: existing, leaseholder: 2, check: true, expected: 0, conf: roachpb.SpanConfig{LeasePreferences: preferDC1}},
		{existing: existing, leaseholder: 2, check: false, expected: 0, conf: roachpb.SpanConfig{LeasePreferences: preferDC1}},
		{existing: existing, leaseholder: 2, check: true, expected: 3, conf: roachpb.SpanConfig{LeasePreferences: preferRegion1}},
		{existing: existing, leaseholder: 2, check: false, expected: 3, conf: roachpb.SpanConfig{LeasePreferences: preferRegion1}},
	}
	for _, c := range testCases {
		t.Run("", func(t *testing.T) {
			target := a.TransferLeaseTarget(
				ctx,
				c.conf,
				c.existing,
				&mockRepl{
					replicationFactor: 3,
					storeID:           c.leaseholder,
				},
				nil,   /* stats */
				false, /* forceDecisionWithoutStats */
				transferLeaseOptions{
					checkTransferLeaseSource: c.check,
					checkCandidateFullness:   true,
				},
			)
			if c.expected != target.StoreID {
				t.Fatalf("expected %d, but found %d", c.expected, target.StoreID)
			}
		})
	}
}

func TestAllocatorRebalanceDifferentLocalitySizes(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper, g, _, a, _ := createTestAllocator(ctx, 10, false /* deterministic */)
	defer stopper.Stop(ctx)

	// Set up 8 stores -- 2 in each of the first 2 localities, and 4 in the third.
	// Because of the desire for diversity, the nodes in the small localities end
	// up being fuller than the nodes in the large locality. In the past this has
	// caused an over-eagerness to rebalance to nodes in the large locality, and
	// not enough willingness to rebalance within the small localities. This test
	// verifies that we compare fairly amongst stores that will givve the store
	// an optimal diversity score, not considering the fullness of those that
	// will make for worse diversity.
	stores := []*roachpb.StoreDescriptor{
		{
			StoreID: 1,
			Node: roachpb.NodeDescriptor{
				NodeID: roachpb.NodeID(1),
				Locality: roachpb.Locality{
					Tiers: []roachpb.Tier{{Key: "locale", Value: "1"}},
				},
			},
			Capacity: testStoreCapacitySetup(50, 50),
		},
		{
			StoreID: 2,
			Node: roachpb.NodeDescriptor{
				NodeID: roachpb.NodeID(2),
				Locality: roachpb.Locality{
					Tiers: []roachpb.Tier{{Key: "locale", Value: "1"}},
				},
			},
			Capacity: testStoreCapacitySetup(40, 60),
		},
		{
			StoreID: 3,
			Node: roachpb.NodeDescriptor{
				NodeID: roachpb.NodeID(3),
				Locality: roachpb.Locality{
					Tiers: []roachpb.Tier{{Key: "locale", Value: "2"}},
				},
			},
			Capacity: testStoreCapacitySetup(50, 50),
		},
		{
			StoreID: 4,
			Node: roachpb.NodeDescriptor{
				NodeID: roachpb.NodeID(4),
				Locality: roachpb.Locality{
					Tiers: []roachpb.Tier{{Key: "locale", Value: "2"}},
				},
			},
			Capacity: testStoreCapacitySetup(40, 60),
		},
		{
			StoreID: 5,
			Node: roachpb.NodeDescriptor{
				NodeID: roachpb.NodeID(5),
				Locality: roachpb.Locality{
					Tiers: []roachpb.Tier{{Key: "locale", Value: "3"}},
				},
			},
			Capacity: testStoreCapacitySetup(90, 10),
		},
		{
			StoreID: 6,
			Node: roachpb.NodeDescriptor{
				NodeID: roachpb.NodeID(6),
				Locality: roachpb.Locality{
					Tiers: []roachpb.Tier{{Key: "locale", Value: "3"}},
				},
			},
			Capacity: testStoreCapacitySetup(80, 20),
		},
		{
			StoreID: 7,
			Node: roachpb.NodeDescriptor{
				NodeID: roachpb.NodeID(7),
				Locality: roachpb.Locality{
					Tiers: []roachpb.Tier{{Key: "locale", Value: "3"}},
				},
			},
			Capacity: testStoreCapacitySetup(80, 20),
		},
		{
			StoreID: 8,
			Node: roachpb.NodeDescriptor{
				NodeID: roachpb.NodeID(8),
				Locality: roachpb.Locality{
					Tiers: []roachpb.Tier{{Key: "locale", Value: "3"}},
				},
			},
			Capacity: testStoreCapacitySetup(80, 20),
		},
	}

	sg := gossiputil.NewStoreGossiper(g)
	sg.GossipStores(stores, t)

	testCases := []struct {
		existing []roachpb.ReplicaDescriptor
		expected roachpb.StoreID // 0 if no rebalance is expected
	}{
		{replicas(1, 3, 5), 0},
		{replicas(2, 3, 5), 1},
		{replicas(1, 4, 5), 3},
		{replicas(1, 3, 6), 5},
		{replicas(1, 5, 6), 3},
		{replicas(2, 5, 6), 3},
		{replicas(3, 5, 6), 1},
		{replicas(4, 5, 6), 1},
	}

	for i, tc := range testCases {
		var rangeUsageInfo RangeUsageInfo
		result, _, details, ok := a.RebalanceVoter(
			ctx,
			emptySpanConfig(),
			nil,
			tc.existing,
			nil,
			rangeUsageInfo,
			storeFilterThrottled,
			a.scorerOptions(),
		)
		var resultID roachpb.StoreID
		if ok {
			resultID = result.StoreID
		}
		if resultID != tc.expected {
			t.Errorf("%d: RebalanceVoter(%v) expected s%d; got %v: %s", i, tc.existing, tc.expected, result, details)
		}
	}

	// Add a couple less full nodes in a fourth locality, then run a few more tests:
	stores = append(stores, &roachpb.StoreDescriptor{
		StoreID: 9,
		Node: roachpb.NodeDescriptor{
			NodeID: roachpb.NodeID(9),
			Locality: roachpb.Locality{
				Tiers: []roachpb.Tier{{Key: "locale", Value: "4"}},
			},
		},
		Capacity: testStoreCapacitySetup(70, 30),
	})
	stores = append(stores, &roachpb.StoreDescriptor{
		StoreID: 10,
		Node: roachpb.NodeDescriptor{
			NodeID: roachpb.NodeID(10),
			Locality: roachpb.Locality{
				Tiers: []roachpb.Tier{{Key: "locale", Value: "4"}},
			},
		},
		Capacity: testStoreCapacitySetup(60, 40),
	})

	sg.GossipStores(stores, t)

	testCases2 := []struct {
		existing []roachpb.ReplicaDescriptor
		expected []roachpb.StoreID
	}{
		{replicas(1, 3, 5), []roachpb.StoreID{9}},
		{replicas(2, 3, 5), []roachpb.StoreID{9}},
		{replicas(1, 4, 5), []roachpb.StoreID{9}},
		{replicas(1, 3, 6), []roachpb.StoreID{9}},
		{replicas(1, 5, 6), []roachpb.StoreID{9}},
		{replicas(2, 5, 6), []roachpb.StoreID{9}},
		{replicas(3, 5, 6), []roachpb.StoreID{9}},
		{replicas(4, 5, 6), []roachpb.StoreID{9}},
		{replicas(5, 6, 7), []roachpb.StoreID{9}},
		{replicas(1, 5, 9), nil},
		{replicas(3, 5, 9), nil},
		{replicas(1, 3, 9), []roachpb.StoreID{5, 6, 7, 8}},
		{replicas(1, 3, 10), []roachpb.StoreID{5, 6, 7, 8}},
		// This last case is a bit more interesting - the difference in range count
		// between s10 an s9 is significant enough to motivate a rebalance if they
		// were the only two valid options, but they're both considered underful
		// relative to the other equally valid placement options (s3 and s4), so
		// the system doesn't consider it helpful to rebalance between them. It'd
		// prefer to move replicas onto both s9 and s10 from other stores.
		{replicas(1, 5, 10), nil},
	}

	for i, tc := range testCases2 {
		log.Infof(ctx, "case #%d", i)
		var rangeUsageInfo RangeUsageInfo
		result, _, details, ok := a.RebalanceVoter(
			ctx,
			emptySpanConfig(),
			nil,
			tc.existing,
			nil,
			rangeUsageInfo,
			storeFilterThrottled,
			a.scorerOptions(),
		)
		var gotExpected bool
		if !ok {
			gotExpected = (tc.expected == nil)
		} else {
			for _, expectedStoreID := range tc.expected {
				if result.StoreID == expectedStoreID {
					gotExpected = true
					break
				}
			}
		}
		if !gotExpected {
			t.Errorf("%d: RebalanceVoter(%v) expected store in %v; got %v: %s",
				i, tc.existing, tc.expected, result, details)
		}
	}
}

func TestAllocatorShouldTransferLease(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	stopper, g, _, a, _ := createTestAllocator(ctx, 10, true /* deterministic */)
	defer stopper.Stop(ctx)

	// 4 stores where the lease count for each store is equal to 10x the store
	// ID.
	var stores []*roachpb.StoreDescriptor
	for i := 1; i <= 4; i++ {
		stores = append(stores, &roachpb.StoreDescriptor{
			StoreID:  roachpb.StoreID(i),
			Node:     roachpb.NodeDescriptor{NodeID: roachpb.NodeID(i)},
			Capacity: roachpb.StoreCapacity{LeaseCount: int32(10 * i)},
		})
	}
	sg := gossiputil.NewStoreGossiper(g)
	sg.GossipStores(stores, t)

	testCases := []struct {
		leaseholder roachpb.StoreID
		existing    []roachpb.ReplicaDescriptor
		expected    bool
	}{
		{leaseholder: 1, existing: nil, expected: false},
		{leaseholder: 2, existing: nil, expected: false},
		{leaseholder: 3, existing: nil, expected: false},
		{leaseholder: 4, existing: nil, expected: false},
		{leaseholder: 3, existing: replicas(1), expected: true},
		{leaseholder: 3, existing: replicas(1, 2), expected: true},
		{leaseholder: 3, existing: replicas(2), expected: false},
		{leaseholder: 3, existing: replicas(3), expected: false},
		{leaseholder: 3, existing: replicas(4), expected: false},
		{leaseholder: 4, existing: replicas(1), expected: true},
		{leaseholder: 4, existing: replicas(2), expected: true},
		{leaseholder: 4, existing: replicas(3), expected: true},
		{leaseholder: 4, existing: replicas(1, 2, 3), expected: true},
	}
	for _, c := range testCases {
		t.Run("", func(t *testing.T) {
			result := a.ShouldTransferLease(
				ctx,
				emptySpanConfig(),
				c.existing,
				c.leaseholder,
				nil, /* replicaStats */
			)
			if c.expected != result {
				t.Fatalf("expected %v, but found %v", c.expected, result)
			}
		})
	}
}

func TestAllocatorShouldTransferLeaseDraining(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	stopper, g, _, storePool, nl := createTestStorePool(ctx,
		TestTimeUntilStoreDeadOff, true, /* deterministic */
		func() int { return 10 }, /* nodeCount */
		livenesspb.NodeLivenessStatus_LIVE)
	a := MakeAllocator(
		storePool, func(string) (time.Duration, bool) {
			return 0, true
		}, nil, /* knobs */
	)
	defer stopper.Stop(context.Background())

	// 4 stores where the lease count for each store is equal to 10x the store
	// ID.
	var stores []*roachpb.StoreDescriptor
	for i := 1; i <= 4; i++ {
		stores = append(stores, &roachpb.StoreDescriptor{
			StoreID:  roachpb.StoreID(i),
			Node:     roachpb.NodeDescriptor{NodeID: roachpb.NodeID(i)},
			Capacity: roachpb.StoreCapacity{LeaseCount: int32(10 * i)},
		})
	}
	sg := gossiputil.NewStoreGossiper(g)
	sg.GossipStores(stores, t)

	// UNAVAILABLE is the node liveness status used for a node that's draining.
	nl.setNodeStatus(1, livenesspb.NodeLivenessStatus_UNAVAILABLE)

	testCases := []struct {
		leaseholder roachpb.StoreID
		existing    []roachpb.ReplicaDescriptor
		expected    bool
	}{
		{leaseholder: 1, existing: nil, expected: false},
		{leaseholder: 2, existing: nil, expected: false},
		{leaseholder: 3, existing: nil, expected: false},
		{leaseholder: 4, existing: nil, expected: false},
		{leaseholder: 2, existing: replicas(1), expected: false},
		{leaseholder: 3, existing: replicas(1), expected: false},
		{leaseholder: 3, existing: replicas(1, 2), expected: false},
		{leaseholder: 3, existing: replicas(1, 2, 4), expected: false},
		{leaseholder: 4, existing: replicas(1), expected: false},
		{leaseholder: 4, existing: replicas(1, 2), expected: true},
		{leaseholder: 4, existing: replicas(1, 3), expected: true},
		{leaseholder: 4, existing: replicas(1, 2, 3), expected: true},
	}
	for _, c := range testCases {
		t.Run("", func(t *testing.T) {
			result := a.ShouldTransferLease(
				ctx,
				emptySpanConfig(),
				c.existing,
				c.leaseholder,
				nil, /* replicaStats */
			)
			if c.expected != result {
				t.Fatalf("expected %v, but found %v", c.expected, result)
			}
		})
	}
}

func TestAllocatorShouldTransferSuspected(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	stopper, g, clock, storePool, nl := createTestStorePool(ctx,
		TestTimeUntilStoreDeadOff, true, /* deterministic */
		func() int { return 10 }, /* nodeCount */
		livenesspb.NodeLivenessStatus_LIVE)
	a := MakeAllocator(
		storePool, func(string) (time.Duration, bool) {
			return 0, true
		}, nil, /* knobs */
	)
	defer stopper.Stop(context.Background())

	var stores []*roachpb.StoreDescriptor
	// Structure the capacity so we only get the desire to move when store 1 is around.
	capacity := []int32{0, 20, 20}
	for i := 1; i <= 3; i++ {
		stores = append(stores, &roachpb.StoreDescriptor{
			StoreID:  roachpb.StoreID(i),
			Node:     roachpb.NodeDescriptor{NodeID: roachpb.NodeID(i)},
			Capacity: roachpb.StoreCapacity{LeaseCount: capacity[i-1]},
		})
	}
	sg := gossiputil.NewStoreGossiper(g)
	sg.GossipStores(stores, t)

	assertShouldTransferLease := func(expected bool) {
		t.Helper()
		result := a.ShouldTransferLease(
			ctx,
			emptySpanConfig(),
			replicas(1, 2, 3),
			2,
			nil, /* replicaStats */
		)
		require.Equal(t, expected, result)
	}
	timeAfterStoreSuspect := TimeAfterStoreSuspect.Get(&storePool.st.SV)
	// Based on capacity node 1 is desirable.
	assertShouldTransferLease(true)
	// Flip node 1 to unavailable, there should be no lease transfer now.
	nl.setNodeStatus(1, livenesspb.NodeLivenessStatus_UNAVAILABLE)
	assertShouldTransferLease(false)
	// Set node back to live, but it's still suspected so not lease transfer expected.
	nl.setNodeStatus(1, livenesspb.NodeLivenessStatus_LIVE)
	assertShouldTransferLease(false)
	// Wait out the suspected store timeout, verify that lease transfers are back.
	clock.Increment(timeAfterStoreSuspect.Nanoseconds() + time.Millisecond.Nanoseconds())
	assertShouldTransferLease(true)
}

func TestAllocatorLeasePreferences(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	stopper, g, _, a, _ := createTestAllocator(ctx, 10, true /* deterministic */)
	defer stopper.Stop(ctx)

	// 4 stores with distinct localities, store attributes, and node attributes
	// where the lease count for each store is equal to 100x the store ID.
	var stores []*roachpb.StoreDescriptor
	for i := 1; i <= 4; i++ {
		stores = append(stores, &roachpb.StoreDescriptor{
			StoreID: roachpb.StoreID(i),
			Attrs:   roachpb.Attributes{Attrs: []string{fmt.Sprintf("s%d", i)}},
			Node: roachpb.NodeDescriptor{
				NodeID: roachpb.NodeID(i),
				Attrs:  roachpb.Attributes{Attrs: []string{fmt.Sprintf("n%d", i)}},
				Locality: roachpb.Locality{
					Tiers: []roachpb.Tier{
						{Key: "dc", Value: strconv.Itoa(i)},
					},
				},
			},
			Capacity: roachpb.StoreCapacity{LeaseCount: int32(100 * i)},
		})
	}
	sg := gossiputil.NewStoreGossiper(g)
	sg.GossipStores(stores, t)

	preferDC1 := []roachpb.LeasePreference{
		{Constraints: []roachpb.Constraint{{Key: "dc", Value: "1", Type: roachpb.Constraint_REQUIRED}}},
	}
	preferDC4Then3Then2 := []roachpb.LeasePreference{
		{Constraints: []roachpb.Constraint{{Key: "dc", Value: "4", Type: roachpb.Constraint_REQUIRED}}},
		{Constraints: []roachpb.Constraint{{Key: "dc", Value: "3", Type: roachpb.Constraint_REQUIRED}}},
		{Constraints: []roachpb.Constraint{{Key: "dc", Value: "2", Type: roachpb.Constraint_REQUIRED}}},
	}
	preferN2ThenS3 := []roachpb.LeasePreference{
		{Constraints: []roachpb.Constraint{{Value: "n2", Type: roachpb.Constraint_REQUIRED}}},
		{Constraints: []roachpb.Constraint{{Value: "s3", Type: roachpb.Constraint_REQUIRED}}},
	}
	preferNotS1ThenNotN2 := []roachpb.LeasePreference{
		{Constraints: []roachpb.Constraint{{Value: "s1", Type: roachpb.Constraint_PROHIBITED}}},
		{Constraints: []roachpb.Constraint{{Value: "n2", Type: roachpb.Constraint_PROHIBITED}}},
	}
	preferNotS1AndNotN2 := []roachpb.LeasePreference{
		{
			Constraints: []roachpb.Constraint{
				{Value: "s1", Type: roachpb.Constraint_PROHIBITED},
				{Value: "n2", Type: roachpb.Constraint_PROHIBITED},
			},
		},
	}
	preferMatchesNothing := []roachpb.LeasePreference{
		{Constraints: []roachpb.Constraint{{Key: "dc", Value: "5", Type: roachpb.Constraint_REQUIRED}}},
		{Constraints: []roachpb.Constraint{{Value: "n6", Type: roachpb.Constraint_REQUIRED}}},
	}

	testCases := []struct {
		leaseholder        roachpb.StoreID
		existing           []roachpb.ReplicaDescriptor
		preferences        []roachpb.LeasePreference
		expectedCheckTrue  roachpb.StoreID /* checkTransferLeaseSource = true */
		expectedCheckFalse roachpb.StoreID /* checkTransferLeaseSource = false */
	}{
		{1, nil, preferDC1, 0, 0},
		{1, replicas(1, 2, 3, 4), preferDC1, 0, 2},
		{1, replicas(2, 3, 4), preferDC1, 0, 2},
		{2, replicas(1, 2, 3, 4), preferDC1, 1, 1},
		{2, replicas(2, 3, 4), preferDC1, 0, 3},
		{4, replicas(2, 3, 4), preferDC1, 2, 2},
		{1, nil, preferDC4Then3Then2, 0, 0},
		{1, replicas(1, 2, 3, 4), preferDC4Then3Then2, 4, 4},
		{1, replicas(1, 2, 3), preferDC4Then3Then2, 3, 3},
		{1, replicas(1, 2), preferDC4Then3Then2, 2, 2},
		{3, replicas(1, 2, 3, 4), preferDC4Then3Then2, 4, 4},
		{3, replicas(1, 2, 3), preferDC4Then3Then2, 0, 2},
		{3, replicas(1, 3), preferDC4Then3Then2, 0, 1},
		{4, replicas(1, 2, 3, 4), preferDC4Then3Then2, 0, 3},
		{4, replicas(1, 2, 4), preferDC4Then3Then2, 0, 2},
		{4, replicas(1, 4), preferDC4Then3Then2, 0, 1},
		{1, replicas(1, 2, 3, 4), preferN2ThenS3, 2, 2},
		{1, replicas(1, 3, 4), preferN2ThenS3, 3, 3},
		{1, replicas(1, 4), preferN2ThenS3, 0, 4},
		{2, replicas(1, 2, 3, 4), preferN2ThenS3, 0, 3},
		{2, replicas(1, 2, 4), preferN2ThenS3, 0, 1},
		{3, replicas(1, 2, 3, 4), preferN2ThenS3, 2, 2},
		{3, replicas(1, 3, 4), preferN2ThenS3, 0, 1},
		{4, replicas(1, 4), preferN2ThenS3, 1, 1},
		{1, replicas(1, 2, 3, 4), preferNotS1ThenNotN2, 2, 2},
		{1, replicas(1, 3, 4), preferNotS1ThenNotN2, 3, 3},
		{1, replicas(1, 2), preferNotS1ThenNotN2, 2, 2},
		{1, replicas(1), preferNotS1ThenNotN2, 0, 0},
		{2, replicas(1, 2, 3, 4), preferNotS1ThenNotN2, 0, 3},
		{2, replicas(2, 3, 4), preferNotS1ThenNotN2, 0, 3},
		{2, replicas(1, 2, 3), preferNotS1ThenNotN2, 0, 3},
		{2, replicas(1, 2, 4), preferNotS1ThenNotN2, 0, 4},
		{4, replicas(1, 2, 3, 4), preferNotS1ThenNotN2, 2, 2},
		{4, replicas(1, 4), preferNotS1ThenNotN2, 0, 1},
		{1, replicas(1, 2, 3, 4), preferNotS1AndNotN2, 3, 3},
		{1, replicas(1, 2), preferNotS1AndNotN2, 0, 2},
		{2, replicas(1, 2, 3, 4), preferNotS1AndNotN2, 3, 3},
		{2, replicas(2, 3, 4), preferNotS1AndNotN2, 3, 3},
		{2, replicas(1, 2, 3), preferNotS1AndNotN2, 3, 3},
		{2, replicas(1, 2, 4), preferNotS1AndNotN2, 4, 4},
		{3, replicas(1, 3), preferNotS1AndNotN2, 0, 1},
		{4, replicas(1, 4), preferNotS1AndNotN2, 0, 1},
		{1, replicas(1, 2, 3, 4), preferMatchesNothing, 0, 2},
		{2, replicas(1, 2, 3, 4), preferMatchesNothing, 0, 1},
		{3, replicas(1, 3, 4), preferMatchesNothing, 1, 1},
		{4, replicas(1, 3, 4), preferMatchesNothing, 1, 1},
		{4, replicas(2, 3, 4), preferMatchesNothing, 2, 2},
	}

	for _, c := range testCases {
		t.Run("", func(t *testing.T) {
			conf := roachpb.SpanConfig{LeasePreferences: c.preferences}
			result := a.ShouldTransferLease(
				ctx,
				conf,
				c.existing,
				c.leaseholder,
				nil, /* replicaStats */
			)
			expectTransfer := c.expectedCheckTrue != 0
			if expectTransfer != result {
				t.Errorf("expected %v, but found %v", expectTransfer, result)
			}
			target := a.TransferLeaseTarget(
				ctx,
				conf,
				c.existing,
				&mockRepl{
					replicationFactor: 5,
					storeID:           c.leaseholder,
				},
				nil,   /* stats */
				false, /* forceDecisionWithoutStats */
				transferLeaseOptions{
					checkTransferLeaseSource: true,
					checkCandidateFullness:   true,
				},
			)
			if c.expectedCheckTrue != target.StoreID {
				t.Errorf("expected s%d for check=true, but found %v", c.expectedCheckTrue, target)
			}
			target = a.TransferLeaseTarget(
				ctx,
				conf,
				c.existing,
				&mockRepl{
					replicationFactor: 5,
					storeID:           c.leaseholder,
				},
				nil,   /* stats */
				false, /* forceDecisionWithoutStats */
				transferLeaseOptions{
					checkTransferLeaseSource: false,
					checkCandidateFullness:   true,
				},
			)
			if c.expectedCheckFalse != target.StoreID {
				t.Errorf("expected s%d for check=false, but found %v", c.expectedCheckFalse, target)
			}
		})
	}
}

func TestAllocatorLeasePreferencesMultipleStoresPerLocality(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	stopper, g, _, a, _ := createTestAllocator(ctx, 10, true /* deterministic */)
	defer stopper.Stop(ctx)

	// 6 stores, 2 in each of 3 distinct localities.
	var stores []*roachpb.StoreDescriptor
	for i := 1; i <= 6; i++ {
		var region, zone string
		if i <= 2 {
			region = "us-east1"
			zone = "us-east1-a"
		} else if i <= 4 {
			region = "us-east1"
			zone = "us-east1-b"
		} else {
			region = "us-west1"
			zone = "us-west1-a"
		}
		stores = append(stores, &roachpb.StoreDescriptor{
			StoreID: roachpb.StoreID(i),
			Node: roachpb.NodeDescriptor{
				NodeID: roachpb.NodeID(i),
				Locality: roachpb.Locality{
					Tiers: []roachpb.Tier{
						{Key: "region", Value: region},
						{Key: "zone", Value: zone},
					},
				},
			},
			Capacity: roachpb.StoreCapacity{LeaseCount: int32(100 * i)},
		})
	}
	sg := gossiputil.NewStoreGossiper(g)
	sg.GossipStores(stores, t)

	preferEast := []roachpb.LeasePreference{
		{Constraints: []roachpb.Constraint{{Key: "region", Value: "us-east1", Type: roachpb.Constraint_REQUIRED}}},
	}
	preferNotEast := []roachpb.LeasePreference{
		{Constraints: []roachpb.Constraint{{Key: "region", Value: "us-east1", Type: roachpb.Constraint_PROHIBITED}}},
	}

	testCases := []struct {
		leaseholder        roachpb.StoreID
		existing           []roachpb.ReplicaDescriptor
		preferences        []roachpb.LeasePreference
		expectedCheckTrue  roachpb.StoreID /* checkTransferLeaseSource = true */
		expectedCheckFalse roachpb.StoreID /* checkTransferLeaseSource = false */
	}{
		{1, replicas(1, 3, 5), preferEast, 0, 3},
		{1, replicas(1, 2, 3), preferEast, 0, 2},
		{3, replicas(1, 3, 5), preferEast, 0, 1},
		{5, replicas(1, 4, 5), preferEast, 1, 1},
		{5, replicas(3, 4, 5), preferEast, 3, 3},
		{1, replicas(1, 5, 6), preferEast, 0, 5},
		{1, replicas(1, 3, 5), preferNotEast, 5, 5},
		{1, replicas(1, 5, 6), preferNotEast, 5, 5},
		{3, replicas(1, 3, 5), preferNotEast, 5, 5},
		{5, replicas(1, 5, 6), preferNotEast, 0, 6},
	}

	for _, c := range testCases {
		t.Run("", func(t *testing.T) {
			conf := roachpb.SpanConfig{LeasePreferences: c.preferences}
			target := a.TransferLeaseTarget(
				ctx,
				conf,
				c.existing,
				&mockRepl{
					replicationFactor: 6,
					storeID:           c.leaseholder,
				},
				nil,   /* stats */
				false, /* forceDecisionWithoutStats */
				transferLeaseOptions{
					checkTransferLeaseSource: true,
					checkCandidateFullness:   true,
				},
			)
			if c.expectedCheckTrue != target.StoreID {
				t.Errorf("expected s%d for check=true, but found %v", c.expectedCheckTrue, target)
			}
			target = a.TransferLeaseTarget(
				ctx,
				conf,
				c.existing,
				&mockRepl{
					replicationFactor: 6,
					storeID:           c.leaseholder,
				},
				nil,   /* stats */
				false, /* forceDecisionWithoutStats */
				transferLeaseOptions{
					checkTransferLeaseSource: false,
					checkCandidateFullness:   true,
				},
			)
			if c.expectedCheckFalse != target.StoreID {
				t.Errorf("expected s%d for check=false, but found %v", c.expectedCheckFalse, target)
			}
		})
	}
}

// TestAllocatorRemoveBasedOnDiversity tests that replicas that are removed on
// the basis of diversity are such that the resulting diversity score of the
// range (after their removal) is the highest. Additionally, it also ensures
// that voting replica removals only consider the set of existing voters when
// computing the diversity score, whereas non-voting replica removal considers
// all existing replicas for its diversity calculation.
func TestAllocatorRemoveBasedOnDiversity(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper, g, _, a, _ := createTestAllocator(ctx, 10, false /* deterministic */)
	defer stopper.Stop(ctx)
	sg := gossiputil.NewStoreGossiper(g)
	sg.GossipStores(multiDiversityDCStores, t)

	// Given a set of existing replicas for a range, pick out the ones that should
	// be removed purely on the basis of locality diversity.
	testCases := []struct {
		existingVoters, existingNonVoters     []roachpb.ReplicaDescriptor
		expVoterRemovals, expNonVoterRemovals []roachpb.StoreID
	}{
		// NB: the `existingNonVoters` in these subtests are such that they would be
		// expected to alter the diversity scores if they were not disregarded
		// during voter removal.
		{
			existingVoters:    replicas(1, 2, 3, 5),
			existingNonVoters: replicas(6, 7),
			// 1 and 2 are in the same datacenter.
			expVoterRemovals:    []roachpb.StoreID{1, 2},
			expNonVoterRemovals: []roachpb.StoreID{6},
		},
		{
			existingVoters:      replicas(1, 2, 3),
			existingNonVoters:   replicas(4, 6, 7),
			expVoterRemovals:    []roachpb.StoreID{1, 2},
			expNonVoterRemovals: []roachpb.StoreID{4},
		},
		{
			existingVoters:      replicas(1, 3, 4, 5),
			existingNonVoters:   replicas(2),
			expVoterRemovals:    []roachpb.StoreID{3, 4},
			expNonVoterRemovals: []roachpb.StoreID{2},
		},
		{
			existingVoters:      replicas(1, 3, 5, 6),
			existingNonVoters:   replicas(2, 7, 8),
			expVoterRemovals:    []roachpb.StoreID{5, 6},
			expNonVoterRemovals: []roachpb.StoreID{2, 7, 8},
		},
		{
			existingVoters:      replicas(3, 4, 7, 8),
			existingNonVoters:   replicas(2, 5, 6),
			expVoterRemovals:    []roachpb.StoreID{3, 4, 7, 8},
			expNonVoterRemovals: []roachpb.StoreID{5, 6},
		},
	}
	for _, c := range testCases {
		targetVoter, details, err := a.RemoveVoter(
			ctx,
			emptySpanConfig(),
			c.existingVoters, /* voterCandidates */
			c.existingVoters,
			c.existingNonVoters,
			a.scorerOptions(),
		)
		require.NoError(t, err)

		require.Truef(
			t,
			checkReplExists(targetVoter, c.expVoterRemovals),
			"expected RemoveVoter(%v) in %v, but got %d; details: %s",
			c.existingVoters, c.expVoterRemovals, targetVoter.StoreID, details,
		)
		// Ensure that we get the same set of results if we didn't have any
		// non-voting replicas. If non-voters were to have an impact on voters'
		// diversity score calculations, we would fail here.
		targetVoter, _, err = a.RemoveVoter(
			ctx,
			emptySpanConfig(),
			c.existingVoters,
			c.existingVoters,
			nil,
			a.scorerOptions(),
		)
		require.NoError(t, err)
		require.Truef(t, checkReplExists(targetVoter, c.expVoterRemovals),
			"voter target for removal differs from expectation when non-voters are present;"+
				" expected %v, got %d", c.expVoterRemovals, targetVoter.StoreID)

		targetNonVoter, _, err := a.RemoveNonVoter(
			ctx,
			emptySpanConfig(),
			c.existingNonVoters, /* nonVoterCandidates */
			c.existingVoters,
			c.existingNonVoters,
			a.scorerOptions(),
		)
		require.NoError(t, err)
		require.True(t, checkReplExists(targetNonVoter, c.expNonVoterRemovals))
	}
}

// TestAllocatorConstraintsAndVoterConstraints tests that allocation of voting
// replicas respects both the `constraints` and the `voter_constraints` and the
// allocation of non-voting replicas respects just the `constraints`.
func TestAllocatorConstraintsAndVoterConstraints(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testCases := []struct {
		name                                          string
		existingVoters, existingNonVoters             []roachpb.ReplicaDescriptor
		stores                                        []*roachpb.StoreDescriptor
		conf                                          roachpb.SpanConfig
		expectedVoters, expectedNonVoters             []roachpb.StoreID
		shouldVoterAllocFail, shouldNonVoterAllocFail bool
		expError                                      string
	}{
		{
			name:              "one store satisfies constraints for each type of replica",
			stores:            multiDCStores,
			conf:              multiDCConfigVoterAndNonVoter,
			expectedVoters:    []roachpb.StoreID{2},
			expectedNonVoters: []roachpb.StoreID{1},
		},
		{
			name:                    "only voter can satisfy constraints",
			stores:                  multiDCStores,
			conf:                    multiDCConfigConstrainToA,
			expectedVoters:          []roachpb.StoreID{1},
			shouldNonVoterAllocFail: true,
		},
		{
			name:                 "only non_voter can satisfy constraints",
			stores:               multiDCStores,
			conf:                 multiDCConfigUnsatisfiableVoterConstraints,
			shouldVoterAllocFail: true,
			expectedNonVoters:    []roachpb.StoreID{1, 2},
		},
	}

	check := func(target roachpb.StoreID, stores []roachpb.StoreID) bool {
		for _, s := range stores {
			if s == target {
				return true
			}
		}
		return false
	}

	for i, test := range testCases {
		t.Run(fmt.Sprintf("%d:%s", i+1, test.name), func(t *testing.T) {
			ctx := context.Background()
			stopper, g, _, a, _ := createTestAllocator(ctx, 10, false /* deterministic */)
			defer stopper.Stop(ctx)
			sg := gossiputil.NewStoreGossiper(g)
			sg.GossipStores(test.stores, t)

			// Allocate the voting replica first, before the non-voter. This is the
			// order in which we'd expect the allocator to repair a given range. See
			// TestAllocatorComputeAction.
			voterTarget, _, err := a.AllocateVoter(ctx, test.conf, test.existingVoters, test.existingNonVoters)
			if test.shouldVoterAllocFail {
				require.Errorf(t, err, "expected voter allocation to fail; got %v as a valid target instead", voterTarget)
			} else {
				require.NoError(t, err)
				require.True(t, check(voterTarget.StoreID, test.expectedVoters))
				test.existingVoters = append(test.existingVoters, replicas(voterTarget.StoreID)...)
			}

			nonVoterTarget, _, err := a.AllocateNonVoter(ctx, test.conf, test.existingVoters, test.existingNonVoters)
			if test.shouldNonVoterAllocFail {
				require.Errorf(t, err, "expected non-voter allocation to fail; got %v as a valid target instead", nonVoterTarget)
			} else {
				require.True(t, check(nonVoterTarget.StoreID, test.expectedNonVoters))
				require.NoError(t, err)
			}
		})
	}
}

func TestAllocatorAllocateTargetLocality(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper, g, _, a, _ := createTestAllocator(ctx, 10, false /* deterministic */)
	defer stopper.Stop(ctx)
	sg := gossiputil.NewStoreGossiper(g)
	sg.GossipStores(multiDiversityDCStores, t)

	// Given a set of existing replicas for a range, rank which of the remaining
	// stores from multiDiversityDCStores would be the best addition to the range
	// purely on the basis of locality diversity.
	testCases := []struct {
		existing []roachpb.StoreID
		expected []roachpb.StoreID
	}{
		{
			[]roachpb.StoreID{1, 2, 3},
			[]roachpb.StoreID{5, 6, 7, 8},
		},
		{
			[]roachpb.StoreID{1, 3, 4},
			[]roachpb.StoreID{5, 6, 7, 8},
		},
		{
			[]roachpb.StoreID{3, 4, 5},
			[]roachpb.StoreID{1, 2, 7, 8},
		},
		{
			[]roachpb.StoreID{1, 7, 8},
			[]roachpb.StoreID{3, 4, 5, 6},
		},
		{
			[]roachpb.StoreID{5, 7, 8},
			[]roachpb.StoreID{1, 2, 3, 4},
		},
		{
			[]roachpb.StoreID{1, 3, 5},
			[]roachpb.StoreID{7, 8},
		},
		{
			[]roachpb.StoreID{1, 3, 7},
			[]roachpb.StoreID{5, 6},
		},
		{
			[]roachpb.StoreID{1, 5, 7},
			[]roachpb.StoreID{3, 4},
		},
		{
			[]roachpb.StoreID{3, 5, 7},
			[]roachpb.StoreID{1, 2},
		},
	}

	for _, c := range testCases {
		existingRepls := make([]roachpb.ReplicaDescriptor, len(c.existing))
		for i, storeID := range c.existing {
			existingRepls[i] = roachpb.ReplicaDescriptor{
				NodeID:  roachpb.NodeID(storeID),
				StoreID: storeID,
			}
		}
		targetStore, details, err := a.AllocateVoter(ctx, emptySpanConfig(), existingRepls, nil)
		if err != nil {
			t.Fatal(err)
		}
		var found bool
		for _, storeID := range c.expected {
			if targetStore.StoreID == storeID {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("expected AllocateVoter(%v) in %v, but got %d; details: %s", c.existing, c.expected, targetStore.StoreID, details)
		}
	}
}

func TestAllocatorRebalanceTargetLocality(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper, g, _, a, _ := createTestAllocator(ctx, 10, false /* deterministic */)
	defer stopper.Stop(ctx)

	stores := []*roachpb.StoreDescriptor{
		{
			StoreID:  1,
			Node:     multiDiversityDCStores[0].Node,
			Capacity: roachpb.StoreCapacity{RangeCount: 10},
		},
		{
			StoreID:  2,
			Node:     multiDiversityDCStores[1].Node,
			Capacity: roachpb.StoreCapacity{RangeCount: 20},
		},
		{
			StoreID:  3,
			Node:     multiDiversityDCStores[2].Node,
			Capacity: roachpb.StoreCapacity{RangeCount: 10},
		},
		{
			StoreID:  4,
			Node:     multiDiversityDCStores[3].Node,
			Capacity: roachpb.StoreCapacity{RangeCount: 20},
		},
		{
			StoreID:  5,
			Node:     multiDiversityDCStores[4].Node,
			Capacity: roachpb.StoreCapacity{RangeCount: 10},
		},
		{
			StoreID:  6,
			Node:     multiDiversityDCStores[5].Node,
			Capacity: roachpb.StoreCapacity{RangeCount: 20},
		},
	}
	sg := gossiputil.NewStoreGossiper(g)
	sg.GossipStores(stores, t)

	testCases := []struct {
		existing []roachpb.StoreID
		expected []roachpb.StoreID
	}{
		{
			[]roachpb.StoreID{1, 2, 3},
			[]roachpb.StoreID{5},
		},
		{
			[]roachpb.StoreID{1, 3, 4},
			[]roachpb.StoreID{5},
		},
		{
			[]roachpb.StoreID{1, 3, 6},
			[]roachpb.StoreID{5},
		},
		{
			[]roachpb.StoreID{1, 2, 5},
			[]roachpb.StoreID{3},
		},
		{
			[]roachpb.StoreID{1, 2, 6},
			[]roachpb.StoreID{3},
		},
		{
			[]roachpb.StoreID{1, 4, 5},
			[]roachpb.StoreID{3},
		},
		{
			[]roachpb.StoreID{1, 4, 6},
			[]roachpb.StoreID{3, 5},
		},
		{
			[]roachpb.StoreID{3, 4, 5},
			[]roachpb.StoreID{1},
		},
		{
			[]roachpb.StoreID{3, 4, 6},
			[]roachpb.StoreID{1},
		},
		{
			[]roachpb.StoreID{4, 5, 6},
			[]roachpb.StoreID{1},
		},
		{
			[]roachpb.StoreID{2, 4, 6},
			[]roachpb.StoreID{1, 3, 5},
		},
	}

	for i, c := range testCases {
		existingRepls := make([]roachpb.ReplicaDescriptor, len(c.existing))
		for i, storeID := range c.existing {
			existingRepls[i] = roachpb.ReplicaDescriptor{
				NodeID:  roachpb.NodeID(storeID),
				StoreID: storeID,
			}
		}
		var rangeUsageInfo RangeUsageInfo
		target, _, details, ok := a.RebalanceVoter(
			ctx,
			emptySpanConfig(),
			nil,
			existingRepls,
			nil,
			rangeUsageInfo,
			storeFilterThrottled,
			a.scorerOptions(),
		)
		if !ok {
			t.Fatalf("%d: RebalanceVoter(%v) returned no target store; details: %s", i, c.existing, details)
		}
		var found bool
		for _, storeID := range c.expected {
			if target.StoreID == storeID {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("%d: expected RebalanceVoter(%v) in %v, but got %d; details: %s",
				i, c.existing, c.expected, target.StoreID, details)
		}
	}
}

var (
	threeSpecificLocalities = []roachpb.ConstraintsConjunction{
		{
			Constraints: []roachpb.Constraint{
				{Key: "datacenter", Value: "a", Type: roachpb.Constraint_REQUIRED},
			},
			NumReplicas: 1,
		},
		{
			Constraints: []roachpb.Constraint{
				{Key: "datacenter", Value: "b", Type: roachpb.Constraint_REQUIRED},
			},
			NumReplicas: 1,
		},
		{
			Constraints: []roachpb.Constraint{
				{Key: "datacenter", Value: "c", Type: roachpb.Constraint_REQUIRED},
			},
			NumReplicas: 1,
		},
	}

	twoAndOneLocalities = []roachpb.ConstraintsConjunction{
		{
			Constraints: []roachpb.Constraint{
				{Key: "datacenter", Value: "a", Type: roachpb.Constraint_REQUIRED},
			},
			NumReplicas: 2,
		},
		{
			Constraints: []roachpb.Constraint{
				{Key: "datacenter", Value: "b", Type: roachpb.Constraint_REQUIRED},
			},
			NumReplicas: 1,
		},
	}

	threeInOneLocality = []roachpb.ConstraintsConjunction{
		{
			Constraints: []roachpb.Constraint{
				{Key: "datacenter", Value: "a", Type: roachpb.Constraint_REQUIRED},
			},
			NumReplicas: 3,
		},
	}

	twoAndOneNodeAttrs = []roachpb.ConstraintsConjunction{
		{
			Constraints: []roachpb.Constraint{
				{Value: "ssd", Type: roachpb.Constraint_REQUIRED},
			},
			NumReplicas: 2,
		},
		{
			Constraints: []roachpb.Constraint{
				{Value: "hdd", Type: roachpb.Constraint_REQUIRED},
			},
			NumReplicas: 1,
		},
	}

	twoAndOneStoreAttrs = []roachpb.ConstraintsConjunction{
		{
			Constraints: []roachpb.Constraint{
				{Value: "odd", Type: roachpb.Constraint_REQUIRED},
			},
			NumReplicas: 2,
		},
		{
			Constraints: []roachpb.Constraint{
				{Value: "even", Type: roachpb.Constraint_REQUIRED},
			},
			NumReplicas: 1,
		},
	}

	mixLocalityAndAttrs = []roachpb.ConstraintsConjunction{
		{
			Constraints: []roachpb.Constraint{
				{Key: "datacenter", Value: "a", Type: roachpb.Constraint_REQUIRED},
				{Value: "ssd", Type: roachpb.Constraint_REQUIRED},
			},
			NumReplicas: 1,
		},
		{
			Constraints: []roachpb.Constraint{
				{Key: "datacenter", Value: "b", Type: roachpb.Constraint_REQUIRED},
				{Value: "odd", Type: roachpb.Constraint_REQUIRED},
			},
			NumReplicas: 1,
		},
		{
			Constraints: []roachpb.Constraint{
				{Value: "even", Type: roachpb.Constraint_REQUIRED},
			},
			NumReplicas: 1,
		},
	}

	twoSpecificLocalities = []roachpb.ConstraintsConjunction{
		{
			Constraints: []roachpb.Constraint{
				{Key: "datacenter", Value: "a", Type: roachpb.Constraint_REQUIRED},
			},
			NumReplicas: 1,
		},
		{
			Constraints: []roachpb.Constraint{
				{Key: "datacenter", Value: "b", Type: roachpb.Constraint_REQUIRED},
			},
			NumReplicas: 1,
		},
	}
)

// TestAllocateCandidatesExcludeNonReadyNodes checks that non-ready
// (e.g. draining) nodes, as per a store pool's
// isNodeValidForRoutineReplicaTransfer(), are excluded from the list
// of candidates for an allocation.
func TestAllocateCandidatesExcludeNonReadyNodes(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// NB: These stores are ordered from least likely to most likely to receive a
	// replica.
	stores := []*roachpb.StoreDescriptor{
		{
			StoreID:  1,
			Node:     roachpb.NodeDescriptor{NodeID: 1},
			Capacity: roachpb.StoreCapacity{Capacity: 200, Available: 100, RangeCount: 600},
		},
		{
			StoreID:  2,
			Node:     roachpb.NodeDescriptor{NodeID: 2},
			Capacity: roachpb.StoreCapacity{Capacity: 200, Available: 100, RangeCount: 450},
		},
		{
			StoreID:  3,
			Node:     roachpb.NodeDescriptor{NodeID: 3},
			Capacity: roachpb.StoreCapacity{Capacity: 200, Available: 100, RangeCount: 300},
		},
		{
			StoreID:  4,
			Node:     roachpb.NodeDescriptor{NodeID: 4},
			Capacity: roachpb.StoreCapacity{Capacity: 200, Available: 100, RangeCount: 150},
		},
	}

	ctx := context.Background()
	stopper, g, _, a, _ := createTestAllocator(ctx, 10, false /* deterministic */)
	defer stopper.Stop(ctx)
	sg := gossiputil.NewStoreGossiper(g)
	sg.GossipStores(stores, t)
	sl, _, _ := a.storePool.getStoreList(storeFilterThrottled)

	testCases := []struct {
		existing roachpb.StoreID
		excluded []roachpb.StoreID
		expected []roachpb.StoreID
	}{
		{
			existing: 1,
			excluded: []roachpb.StoreID{2},
			expected: []roachpb.StoreID{3, 4},
		},
		{
			existing: 1,
			excluded: []roachpb.StoreID{2, 3},
			expected: []roachpb.StoreID{4},
		},
		{
			existing: 1,
			excluded: []roachpb.StoreID{2, 3, 4},
			expected: []roachpb.StoreID{},
		},
		{
			existing: 1,
			excluded: []roachpb.StoreID{2, 4},
			expected: []roachpb.StoreID{3},
		},
	}

	for testIdx, tc := range testCases {
		existingRepls := []roachpb.ReplicaDescriptor{
			{NodeID: roachpb.NodeID(tc.existing), StoreID: tc.existing},
		}
		// No constraints.
		conf := roachpb.SpanConfig{}
		analyzed := constraint.AnalyzeConstraints(
			ctx, a.storePool.getStoreDescriptor, existingRepls, conf.NumReplicas,
			conf.Constraints)
		allocationConstraintsChecker := voterConstraintsCheckerForAllocation(analyzed, constraint.EmptyAnalyzedConstraints)
		removalConstraintsChecker := voterConstraintsCheckerForRemoval(analyzed, constraint.EmptyAnalyzedConstraints)
		rebalanceConstraintsChecker := voterConstraintsCheckerForRebalance(analyzed, constraint.EmptyAnalyzedConstraints)

		a.storePool.isStoreReadyForRoutineReplicaTransfer = func(_ context.Context, storeID roachpb.StoreID) bool {
			for _, s := range tc.excluded {
				if s == storeID {
					return false
				}
			}
			return true
		}

		t.Run(fmt.Sprintf("%d/allocate", testIdx), func(t *testing.T) {
			candidates := rankedCandidateListForAllocation(
				ctx,
				sl,
				allocationConstraintsChecker,
				existingRepls,
				a.storePool.getLocalitiesByStore(existingRepls),
				a.storePool.isStoreReadyForRoutineReplicaTransfer,
				false, /* allowMultipleReplsPerNode */
				a.scorerOptions(),
			)

			if !expectedStoreIDsMatch(tc.expected, candidates) {
				t.Errorf("expected rankedCandidateListForAllocation(%v) = %v, but got %v",
					tc.existing, tc.expected, candidates)
			}
		})

		t.Run(fmt.Sprintf("%d/rebalance", testIdx), func(t *testing.T) {
			rebalanceOpts := rankedCandidateListForRebalancing(
				ctx,
				sl,
				removalConstraintsChecker,
				rebalanceConstraintsChecker,
				existingRepls,
				nil,
				a.storePool.getLocalitiesByStore(existingRepls),
				a.storePool.isStoreReadyForRoutineReplicaTransfer,
				a.scorerOptions(),
			)
			if len(tc.expected) > 0 {
				require.Len(t, rebalanceOpts, 1)
				candidateStores := make([]roachpb.StoreID, len(rebalanceOpts[0].candidates))
				for i, cand := range rebalanceOpts[0].candidates {
					candidateStores[i] = cand.store.StoreID
				}
				require.ElementsMatch(t, tc.expected, candidateStores)
				require.Equal(t, tc.existing, rebalanceOpts[0].existing.store.StoreID)
			} else {
				require.Len(t, rebalanceOpts, 0)
			}
		})
	}
}

// TestAllocatorNonVoterAllocationExcludesVoterNodes checks that when allocating
// non-voting replicas, stores that have any existing replica (voting or
// non-voting) are excluded from the list of candidates.
func TestAllocatorNonVoterAllocationExcludesVoterNodes(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testCases := []struct {
		name                              string
		existingVoters, existingNonVoters []roachpb.ReplicaDescriptor
		stores                            []*roachpb.StoreDescriptor
		conf                              roachpb.SpanConfig
		expected                          roachpb.StoreID
		shouldFail                        bool
		expError                          string
	}{
		{
			name:              "voters only",
			existingNonVoters: replicas(1, 2, 3, 4),
			stores:            sameDCStores,
			conf:              emptySpanConfig(),
			// Expect that that the store that doesn't have any replicas would be
			// the one to receive a new non-voter.
			expected: roachpb.StoreID(5),
		},
		{
			name:              "non-voters only",
			existingNonVoters: replicas(1, 2, 3, 4),
			stores:            sameDCStores,
			conf:              emptySpanConfig(),
			expected:          roachpb.StoreID(5),
		},
		{
			name:              "mixed",
			existingVoters:    replicas(1, 2),
			existingNonVoters: replicas(3, 4),
			stores:            sameDCStores,
			conf:              emptySpanConfig(),
			expected:          roachpb.StoreID(5),
		},
		{
			name: "only valid store has a voter",
			// Place a voter on the only store that would meet the constraints of
			// `multiDCConfigConstrainToA`.
			existingVoters: replicas(1),
			stores:         multiDCStores,
			conf:           multiDCConfigConstrainToA,
			shouldFail:     true,
			expError:       "0 of 2 live stores are able to take a new replica for the range",
		},
		{
			name: "only valid store has a non_voter",
			// Place a non-voter on the only store that would meet the constraints of
			// `multiDCConfigConstrainToA`.
			existingNonVoters: replicas(1),
			stores:            multiDCStores,
			conf:              multiDCConfigConstrainToA,
			shouldFail:        true,
			expError:          "0 of 2 live stores are able to take a new replica for the range",
		},
	}

	for i, test := range testCases {
		t.Run(fmt.Sprintf("%d:%s", i+1, test.name), func(t *testing.T) {
			ctx := context.Background()
			stopper, g, _, a, _ := createTestAllocator(ctx, 10, false /* deterministic */)
			defer stopper.Stop(ctx)
			sg := gossiputil.NewStoreGossiper(g)
			sg.GossipStores(test.stores, t)

			result, _, err := a.AllocateNonVoter(ctx, test.conf, test.existingVoters, test.existingNonVoters)
			if test.shouldFail {
				require.Error(t, err)
				require.Regexp(t, test.expError, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.expected, result.StoreID)
			}
		})
	}
}

func TestAllocateCandidatesNumReplicasConstraints(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper, g, _, a, _ := createTestAllocator(ctx, 10, false /* deterministic */)
	defer stopper.Stop(ctx)
	sg := gossiputil.NewStoreGossiper(g)
	sg.GossipStores(multiDiversityDCStores, t)
	sl, _, _ := a.storePool.getStoreList(storeFilterThrottled)

	// Given a set of existing replicas for a range, rank which of the remaining
	// stores from multiDiversityDCStores would be the best addition to the range
	// purely on the basis of constraint satisfaction and locality diversity.
	testCases := []struct {
		constraints []roachpb.ConstraintsConjunction
		existing    []roachpb.StoreID
		expected    []roachpb.StoreID
	}{
		{
			threeSpecificLocalities,
			[]roachpb.StoreID{},
			[]roachpb.StoreID{1, 2, 3, 4, 5, 6},
		},
		{
			threeSpecificLocalities,
			[]roachpb.StoreID{7, 8},
			[]roachpb.StoreID{1, 2, 3, 4, 5, 6},
		},
		{
			threeSpecificLocalities,
			[]roachpb.StoreID{1},
			[]roachpb.StoreID{3, 4, 5, 6},
		},
		{
			threeSpecificLocalities,
			[]roachpb.StoreID{3, 5},
			[]roachpb.StoreID{1, 2},
		},
		{
			threeSpecificLocalities,
			[]roachpb.StoreID{3, 4},
			[]roachpb.StoreID{1, 2, 5, 6},
		},
		{
			threeSpecificLocalities,
			[]roachpb.StoreID{1, 3, 5},
			[]roachpb.StoreID{2, 4, 6},
		},
		{
			twoAndOneLocalities,
			[]roachpb.StoreID{},
			[]roachpb.StoreID{1, 2, 3, 4},
		},
		{
			twoAndOneLocalities,
			[]roachpb.StoreID{1},
			[]roachpb.StoreID{3, 4}, // 2 isn't included because its diversity is worse
		},
		{
			twoAndOneLocalities,
			[]roachpb.StoreID{1, 2},
			[]roachpb.StoreID{3, 4},
		},
		{
			twoAndOneLocalities,
			[]roachpb.StoreID{1, 2, 3},
			[]roachpb.StoreID{4},
		},
		{
			twoAndOneLocalities,
			[]roachpb.StoreID{3},
			[]roachpb.StoreID{1, 2},
		},
		{
			twoAndOneLocalities,
			[]roachpb.StoreID{5},
			[]roachpb.StoreID{1, 2, 3, 4},
		},
		{
			threeInOneLocality,
			[]roachpb.StoreID{},
			[]roachpb.StoreID{1, 2},
		},
		{
			threeInOneLocality,
			[]roachpb.StoreID{3, 4, 5},
			[]roachpb.StoreID{1, 2},
		},
		{
			threeInOneLocality,
			[]roachpb.StoreID{1},
			[]roachpb.StoreID{2},
		},
		{
			threeInOneLocality,
			[]roachpb.StoreID{1, 2},
			[]roachpb.StoreID{},
		},
		{
			twoAndOneNodeAttrs,
			[]roachpb.StoreID{},
			[]roachpb.StoreID{1, 2, 3, 4, 5, 6, 7, 8},
		},
		{
			twoAndOneNodeAttrs,
			[]roachpb.StoreID{2},
			[]roachpb.StoreID{3, 5, 7},
		},
		{
			twoAndOneNodeAttrs,
			[]roachpb.StoreID{1},
			[]roachpb.StoreID{3, 4, 5, 6, 7, 8},
		},
		{
			twoAndOneNodeAttrs,
			[]roachpb.StoreID{1, 2},
			[]roachpb.StoreID{3, 5, 7},
		},
		{
			twoAndOneNodeAttrs,
			[]roachpb.StoreID{1, 3},
			[]roachpb.StoreID{6, 8},
		},
		{
			twoAndOneNodeAttrs,
			[]roachpb.StoreID{1, 3, 6},
			[]roachpb.StoreID{7, 8},
		},
		{
			twoAndOneStoreAttrs,
			[]roachpb.StoreID{},
			[]roachpb.StoreID{1, 2, 3, 4, 5, 6, 7, 8},
		},
		{
			twoAndOneStoreAttrs,
			[]roachpb.StoreID{2},
			[]roachpb.StoreID{3, 5, 7},
		},
		{
			twoAndOneStoreAttrs,
			[]roachpb.StoreID{1},
			[]roachpb.StoreID{3, 4, 5, 6, 7, 8},
		},
		{
			twoAndOneStoreAttrs,
			[]roachpb.StoreID{1, 2},
			[]roachpb.StoreID{3, 5, 7},
		},
		{
			twoAndOneStoreAttrs,
			[]roachpb.StoreID{1, 3},
			[]roachpb.StoreID{6, 8},
		},
		{
			twoAndOneStoreAttrs,
			[]roachpb.StoreID{1, 3, 6},
			[]roachpb.StoreID{7, 8},
		},
		{
			mixLocalityAndAttrs,
			[]roachpb.StoreID{},
			[]roachpb.StoreID{1, 2, 3, 4, 6, 8},
		},
		{
			mixLocalityAndAttrs,
			[]roachpb.StoreID{1},
			[]roachpb.StoreID{3, 4, 6, 8},
		},
		{
			mixLocalityAndAttrs,
			[]roachpb.StoreID{2},
			[]roachpb.StoreID{3},
		},
		{
			mixLocalityAndAttrs,
			[]roachpb.StoreID{3},
			[]roachpb.StoreID{1, 2, 6, 8},
		},
		{
			mixLocalityAndAttrs,
			[]roachpb.StoreID{2, 3},
			[]roachpb.StoreID{1},
		},
		{
			mixLocalityAndAttrs,
			[]roachpb.StoreID{1, 2},
			[]roachpb.StoreID{3},
		},
		{
			mixLocalityAndAttrs,
			[]roachpb.StoreID{1, 3},
			[]roachpb.StoreID{6, 8},
		},
		{
			mixLocalityAndAttrs,
			[]roachpb.StoreID{1, 2, 3},
			[]roachpb.StoreID{6, 8},
		},
	}

	for testIdx, tc := range testCases {
		existingRepls := make([]roachpb.ReplicaDescriptor, len(tc.existing))
		for i, storeID := range tc.existing {
			existingRepls[i] = roachpb.ReplicaDescriptor{
				NodeID:  roachpb.NodeID(storeID),
				StoreID: storeID,
			}
		}
		conf := roachpb.SpanConfig{Constraints: tc.constraints}
		analyzed := constraint.AnalyzeConstraints(
			ctx, a.storePool.getStoreDescriptor, existingRepls, conf.NumReplicas,
			conf.Constraints)
		checkFn := voterConstraintsCheckerForAllocation(analyzed, constraint.EmptyAnalyzedConstraints)

		candidates := rankedCandidateListForAllocation(
			ctx,
			sl,
			checkFn,
			existingRepls,
			a.storePool.getLocalitiesByStore(existingRepls),
			func(context.Context, roachpb.StoreID) bool { return true },
			false, /* allowMultipleReplsPerNode */
			a.scorerOptions(),
		)
		best := candidates.best()
		match := true
		if len(tc.expected) != len(best) {
			match = false
		} else {
			sort.Slice(best, func(i, j int) bool {
				return best[i].store.StoreID < best[j].store.StoreID
			})
			for i := range tc.expected {
				if tc.expected[i] != best[i].store.StoreID {
					match = false
					break
				}
			}
		}
		if !match {
			t.Errorf("%d: expected rankedCandidateListForAllocation(%v) = %v, but got %v",
				testIdx, tc.existing, tc.expected, candidates)
		}
	}
}

func TestRemoveCandidatesNumReplicasConstraints(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper, g, _, a, _ := createTestAllocator(ctx, 10, false /* deterministic */)
	defer stopper.Stop(ctx)
	sg := gossiputil.NewStoreGossiper(g)
	sg.GossipStores(multiDiversityDCStores, t)

	// Given a set of existing replicas for a range, rank which of the remaining
	// stores would be best to remove if we had to remove one purely on the basis
	// of constraint-matching and locality diversity.
	testCases := []struct {
		constraints []roachpb.ConstraintsConjunction
		existing    []roachpb.StoreID
		expected    []roachpb.StoreID
	}{
		{
			threeSpecificLocalities,
			[]roachpb.StoreID{1},
			[]roachpb.StoreID{1},
		},
		{
			threeSpecificLocalities,
			[]roachpb.StoreID{1, 2},
			[]roachpb.StoreID{1, 2},
		},
		{
			threeSpecificLocalities,
			[]roachpb.StoreID{1, 3},
			[]roachpb.StoreID{1, 3},
		},
		{
			threeSpecificLocalities,
			[]roachpb.StoreID{1, 2, 3},
			[]roachpb.StoreID{1, 2},
		},
		{
			threeSpecificLocalities,
			[]roachpb.StoreID{1, 3, 5},
			[]roachpb.StoreID{1, 3, 5},
		},
		{
			threeSpecificLocalities,
			[]roachpb.StoreID{1, 3, 7},
			[]roachpb.StoreID{7},
		},
		{
			twoAndOneLocalities,
			[]roachpb.StoreID{1, 3},
			[]roachpb.StoreID{1, 3},
		},
		{
			twoAndOneLocalities,
			[]roachpb.StoreID{1, 3, 5},
			[]roachpb.StoreID{5},
		},
		{
			twoAndOneLocalities,
			[]roachpb.StoreID{1, 3, 4},
			[]roachpb.StoreID{3, 4},
		},
		{
			twoAndOneLocalities,
			[]roachpb.StoreID{1, 2, 3},
			[]roachpb.StoreID{1, 2},
		},
		{
			threeInOneLocality,
			[]roachpb.StoreID{1, 3},
			[]roachpb.StoreID{3},
		},
		{
			threeInOneLocality,
			[]roachpb.StoreID{2, 3},
			[]roachpb.StoreID{3},
		},
		{
			threeInOneLocality,
			[]roachpb.StoreID{1, 2, 3},
			[]roachpb.StoreID{3},
		},
		{
			threeInOneLocality,
			[]roachpb.StoreID{3, 5, 7},
			[]roachpb.StoreID{3, 5, 7},
		},
		{
			threeInOneLocality,
			[]roachpb.StoreID{1, 2, 3, 5, 7},
			[]roachpb.StoreID{3, 5, 7},
		},
		{
			twoAndOneNodeAttrs,
			[]roachpb.StoreID{1, 3},
			[]roachpb.StoreID{1, 3},
		},
		{
			twoAndOneNodeAttrs,
			[]roachpb.StoreID{1, 2, 3},
			[]roachpb.StoreID{1, 2},
		},
		{
			twoAndOneNodeAttrs,
			[]roachpb.StoreID{1, 3, 6},
			[]roachpb.StoreID{1, 3, 6},
		},
		{
			twoAndOneNodeAttrs,
			[]roachpb.StoreID{1, 4, 6},
			[]roachpb.StoreID{4, 6},
		},
		{
			twoAndOneNodeAttrs,
			[]roachpb.StoreID{1, 2, 6},
			[]roachpb.StoreID{2},
		},
		{
			twoAndOneStoreAttrs,
			[]roachpb.StoreID{1, 2, 3},
			[]roachpb.StoreID{1, 2},
		},
		{
			twoAndOneStoreAttrs,
			[]roachpb.StoreID{1, 3, 6},
			[]roachpb.StoreID{1, 3, 6},
		},
		{
			twoAndOneStoreAttrs,
			[]roachpb.StoreID{1, 4, 6},
			[]roachpb.StoreID{4, 6},
		},
		{
			twoAndOneStoreAttrs,
			[]roachpb.StoreID{1, 2, 6},
			[]roachpb.StoreID{2},
		},
		{
			mixLocalityAndAttrs,
			[]roachpb.StoreID{1, 3, 6},
			[]roachpb.StoreID{1, 3, 6},
		},
		{
			mixLocalityAndAttrs,
			[]roachpb.StoreID{1, 2, 3},
			[]roachpb.StoreID{1, 2},
		},
		{
			mixLocalityAndAttrs,
			[]roachpb.StoreID{2, 3, 6},
			[]roachpb.StoreID{2, 6},
		},
		{
			mixLocalityAndAttrs,
			[]roachpb.StoreID{2, 3, 4},
			[]roachpb.StoreID{4},
		},
		{
			mixLocalityAndAttrs,
			[]roachpb.StoreID{5, 7},
			[]roachpb.StoreID{5, 7},
		},
		{
			// TODO(a-robinson): Should we prefer just 5 here for diversity reasons?
			// We'd have to rework our handling of invalid stores in a handful of
			// places, including in `candidateList.worst()`, to consider traits beyond
			// just invalidity.
			mixLocalityAndAttrs,
			[]roachpb.StoreID{5, 6, 7},
			[]roachpb.StoreID{5, 7},
		},
		{
			mixLocalityAndAttrs,
			[]roachpb.StoreID{1, 5, 7},
			[]roachpb.StoreID{5, 7},
		},
		{
			mixLocalityAndAttrs,
			[]roachpb.StoreID{1, 6, 8},
			[]roachpb.StoreID{6, 8},
		},
	}

	for testIdx, tc := range testCases {
		sl, _, _ := a.storePool.getStoreListFromIDs(tc.existing, storeFilterNone)
		existingRepls := make([]roachpb.ReplicaDescriptor, len(tc.existing))
		for i, storeID := range tc.existing {
			existingRepls[i] = roachpb.ReplicaDescriptor{
				NodeID:  roachpb.NodeID(storeID),
				StoreID: storeID,
			}
		}
		analyzed := constraint.AnalyzeConstraints(ctx, a.storePool.getStoreDescriptor, existingRepls,
			0 /* numReplicas */, tc.constraints)

		// Check behavior in a span config where `voter_constraints` are empty.
		checkFn := voterConstraintsCheckerForRemoval(analyzed, constraint.EmptyAnalyzedConstraints)
		candidates := candidateListForRemoval(sl,
			checkFn,
			a.storePool.getLocalitiesByStore(existingRepls),
			a.scorerOptions())
		if !expectedStoreIDsMatch(tc.expected, candidates.worst()) {
			t.Errorf("%d (with `constraints`): expected candidateListForRemoval(%v)"+
				" = %v, but got %v\n for candidates %v", testIdx, tc.existing, tc.expected,
				candidates.worst(), candidates)
		}

		// Check that we'd see the same result if the same constraints were
		// specified as `voter_constraints`.
		checkFn = voterConstraintsCheckerForRemoval(constraint.EmptyAnalyzedConstraints, analyzed)
		candidates = candidateListForRemoval(sl,
			checkFn,
			a.storePool.getLocalitiesByStore(existingRepls),
			a.scorerOptions())
		if !expectedStoreIDsMatch(tc.expected, candidates.worst()) {
			t.Errorf("%d (with `voter_constraints`): expected candidateListForRemoval(%v)"+
				" = %v, but got %v\n for candidates %v", testIdx, tc.existing, tc.expected,
				candidates.worst(), candidates)
		}
	}
}

func expectedStoreIDsMatch(expected []roachpb.StoreID, results candidateList) bool {
	if len(expected) != len(results) {
		return false
	}
	sort.Slice(results, func(i, j int) bool {
		return results[i].store.StoreID < results[j].store.StoreID
	})
	for i := range expected {
		if expected[i] != results[i].store.StoreID {
			return false
		}
	}
	return true
}

// TestAllocatorRebalanceNonVoters tests that non-voting replicas rebalance "as
// expected". In particular, it checks the following things:
//
// 1. Non-voter rebalancing obeys the allocator's capacity based heuristics.
// 2. Non-voter rebalancing tries to ensure constraints conformance.
func TestAllocatorRebalanceNonVoters(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	type testCase struct {
		name                                      string
		stores                                    []*roachpb.StoreDescriptor
		conf                                      roachpb.SpanConfig
		existingVoters, existingNonVoters         []roachpb.ReplicaDescriptor
		expectNoAction                            bool
		expectedRemoveTargets, expectedAddTargets []roachpb.StoreID
	}
	tests := []testCase{
		{
			name:              "no-op",
			stores:            multiDiversityDCStores,
			conf:              emptySpanConfig(),
			existingVoters:    replicas(1),
			existingNonVoters: replicas(3),
			expectNoAction:    true,
		},
		// Test that rebalancing based on just the diversity scores works as
		// expected. In particular, we expect non-voter rebalancing to compute
		// diversity scores based on the entire existing replica set, and not just
		// the set of non-voting replicas.
		{
			name:                  "diversity among non-voters",
			stores:                multiDiversityDCStores,
			conf:                  emptySpanConfig(),
			existingVoters:        replicas(1, 2),
			existingNonVoters:     replicas(3, 4, 6),
			expectedRemoveTargets: []roachpb.StoreID{3, 4},
			expectedAddTargets:    []roachpb.StoreID{7, 8},
		},
		{
			name:                  "diversity among all existing replicas",
			stores:                multiDiversityDCStores,
			conf:                  emptySpanConfig(),
			existingVoters:        replicas(1),
			existingNonVoters:     replicas(2, 4, 6),
			expectedRemoveTargets: []roachpb.StoreID{2},
			expectedAddTargets:    []roachpb.StoreID{7, 8},
		},
		// Test that non-voting replicas obey the capacity / load based heuristics
		// for rebalancing.
		{
			name: "move off of nodes with full disk",
			// NB: Store 1 has a 97.5% full disk.
			stores:                oneStoreWithFullDisk,
			conf:                  emptySpanConfig(),
			existingVoters:        replicas(3),
			existingNonVoters:     replicas(1),
			expectedRemoveTargets: []roachpb.StoreID{1},
			expectedAddTargets:    []roachpb.StoreID{2},
		},
		{
			name: "move off of nodes with too many ranges",
			// NB: Store 1 has 3x the number of ranges as the other stores.
			stores:                oneStoreWithTooManyRanges,
			conf:                  emptySpanConfig(),
			existingVoters:        replicas(3),
			existingNonVoters:     replicas(1),
			expectedRemoveTargets: []roachpb.StoreID{1},
			expectedAddTargets:    []roachpb.StoreID{2},
		},
		// Test that `constraints` cause non-voters to move around in order to
		// sustain constraints conformance.
		{
			name:   "already on a store that satisfies constraints for non_voters",
			stores: multiDCStores,
			// Constrain a voter to store 2 and a non_voter to store 1.
			conf:              multiDCConfigVoterAndNonVoter,
			existingVoters:    replicas(2),
			existingNonVoters: replicas(1),
			expectNoAction:    true,
		},
		{
			name:   "need to rebalance to conform to constraints",
			stores: multiDCStores,
			// Constrain a non_voter to store 1.
			conf:                  multiDCConfigVoterAndNonVoter,
			existingVoters:        nil,
			existingNonVoters:     replicas(2),
			expectedRemoveTargets: []roachpb.StoreID{2},
			expectedAddTargets:    []roachpb.StoreID{1},
		},
		{
			// Test that non-voting replica rebalancing does not consider stores that
			// have voters as valid candidates, even if those stores satisfy
			// constraints.
			name:              "need to rebalance, but cannot because a voter already exists",
			stores:            multiDCStores,
			conf:              multiDCConfigVoterAndNonVoter,
			existingVoters:    replicas(1),
			existingNonVoters: replicas(2),
			expectNoAction:    true,
		},
	}

	var rangeUsageInfo RangeUsageInfo
	chk := func(target roachpb.ReplicationTarget, expectedCandidates []roachpb.StoreID) bool {
		for _, candidate := range expectedCandidates {
			if target.StoreID == candidate {
				return true
			}
		}
		return false
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("%d_%s", i+1, test.name), func(t *testing.T) {
			stopper, g, _, a, _ := createTestAllocator(ctx, 10, false /* deterministic */)
			defer stopper.Stop(ctx)
			sg := gossiputil.NewStoreGossiper(g)
			sg.GossipStores(test.stores, t)
			add, remove, _, ok := a.RebalanceNonVoter(
				ctx,
				test.conf,
				nil,
				test.existingVoters,
				test.existingNonVoters,
				rangeUsageInfo,
				storeFilterThrottled,
				a.scorerOptions(),
			)
			if test.expectNoAction {
				require.True(t, !ok)
			} else {
				require.Truef(t, ok, "no action taken on range")
				require.Truef(t,
					chk(add, test.expectedAddTargets),
					"the addition target %+v from RebalanceNonVoter doesn't match expectation",
					add)
				require.Truef(t,
					chk(remove, test.expectedRemoveTargets),
					"the removal target %+v from RebalanceNonVoter doesn't match expectation",
					remove)
			}
		})
	}
}

// TestVotersCanRebalanceToNonVoterStores ensures that rebalancing of voting
// replicas considers stores that have non-voters as feasible candidates.
func TestVotersCanRebalanceToNonVoterStores(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	stopper, g, _, a, _ := createTestAllocator(ctx, 10, false /* deterministic */)
	defer stopper.Stop(ctx)
	sg := gossiputil.NewStoreGossiper(g)
	sg.GossipStores(multiDiversityDCStores, t)

	conf := roachpb.SpanConfig{
		NumReplicas: 4,
		NumVoters:   2,
		// We constrain 2 voting replicas to datacenter "a" (stores 1 and 2) but
		// place non voting replicas there. In order to achieve constraints
		// conformance, each of the voters must want to move to one of these stores.
		VoterConstraints: []roachpb.ConstraintsConjunction{
			{
				NumReplicas: 2,
				Constraints: []roachpb.Constraint{
					{Type: roachpb.Constraint_REQUIRED, Key: "datacenter", Value: "a"},
				},
			},
		},
	}

	var rangeUsageInfo RangeUsageInfo
	existingNonVoters := replicas(1, 2)
	existingVoters := replicas(3, 4)
	add, remove, _, ok := a.RebalanceVoter(
		ctx,
		conf,
		nil,
		existingVoters,
		existingNonVoters,
		rangeUsageInfo,
		storeFilterThrottled,
		a.scorerOptions(),
	)

	require.Truef(t, ok, "no action taken")
	if !(add.StoreID == roachpb.StoreID(1) || add.StoreID == roachpb.StoreID(2)) {
		t.Fatalf("received unexpected addition target %s from RebalanceVoter", add)
	}
	if !(remove.StoreID == roachpb.StoreID(3) || remove.StoreID == roachpb.StoreID(4)) {
		t.Fatalf("received unexpected removal target %s from RebalanceVoter", remove)
	}
}

// TestNonVotersCannotRebalanceToVoterStores ensures that non-voting replicas
// cannot rebalance to stores that already have a voting replica for the range.
func TestNonVotersCannotRebalanceToVoterStores(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx, finishAndGetRecording := tracing.ContextWithRecordingSpan(
		context.Background(), tracing.NewTracer(), "test",
	)

	stopper, g, _, a, _ := createTestAllocator(ctx, 2, false /* deterministic */)
	defer stopper.Stop(ctx)
	sg := gossiputil.NewStoreGossiper(g)

	// Create 2 stores. Store 2 has a voting replica and store 1 has a non-voting
	// replica. Make it such that store 1 has a full disk so the allocator will
	// want to rebalance it away. However, the only possible candidate is store 2
	// which already has a voting replica. Thus, this rebalance attempt should
	// fail.
	stores := []*roachpb.StoreDescriptor{
		{
			StoreID: 1,
			Capacity: roachpb.StoreCapacity{
				Capacity:  100,
				Available: 0,
			},
		},
		{
			StoreID: 2,
			Capacity: roachpb.StoreCapacity{
				Capacity:  100,
				Available: 100,
			},
		},
	}
	existingNonVoters := replicas(1)
	existingVoters := replicas(2)

	sg.GossipStores(stores, t)
	var rangeUsageInfo RangeUsageInfo
	add, remove, _, ok := a.RebalanceNonVoter(
		ctx,
		emptySpanConfig(),
		nil,
		existingVoters,
		existingNonVoters,
		rangeUsageInfo,
		storeFilterThrottled,
		a.scorerOptions(),
	)

	require.Falsef(
		t, ok, "expected no action; got rebalance from s%d to s%d", remove.StoreID, add.StoreID,
	)
	trace := finishAndGetRecording().String()
	require.Regexpf(
		t,
		"it already has a voter",
		trace,
		"expected the voter store to be explicitly ignored; got %s",
		trace,
	)
}

func TestRebalanceCandidatesNumReplicasConstraints(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper, g, _, a, _ := createTestAllocator(ctx, 10, false /* deterministic */)
	defer stopper.Stop(ctx)
	sg := gossiputil.NewStoreGossiper(g)
	sg.GossipStores(multiDiversityDCStores, t)
	sl, _, _ := a.storePool.getStoreList(storeFilterThrottled)

	// Given a set of existing replicas for a range, rank which of the remaining
	// stores would be best to remove if we had to remove one purely on the basis
	// of constraint-matching and locality diversity.
	type rebalanceStoreIDs struct {
		existing   roachpb.StoreID
		candidates []roachpb.StoreID
	}
	testCases := []struct {
		constraints  []roachpb.ConstraintsConjunction
		numReplicas  int32
		existing     []roachpb.StoreID
		expected     []rebalanceStoreIDs
		validTargets []roachpb.StoreID
	}{
		{
			constraints:  threeSpecificLocalities,
			existing:     []roachpb.StoreID{1},
			expected:     []rebalanceStoreIDs{}, // a store must be an improvement to justify rebalancing
			validTargets: []roachpb.StoreID{},
		},
		{
			constraints:  threeSpecificLocalities,
			existing:     []roachpb.StoreID{1, 3},
			expected:     []rebalanceStoreIDs{},
			validTargets: []roachpb.StoreID{},
		},
		{
			constraints:  threeSpecificLocalities,
			existing:     []roachpb.StoreID{1, 3, 5},
			expected:     []rebalanceStoreIDs{},
			validTargets: []roachpb.StoreID{},
		},
		{
			constraints: threeSpecificLocalities,
			existing:    []roachpb.StoreID{1, 2},
			expected: []rebalanceStoreIDs{
				{
					existing:   1,
					candidates: []roachpb.StoreID{3, 4, 5, 6},
				},
				{
					existing:   2,
					candidates: []roachpb.StoreID{3, 4, 5, 6},
				},
			},
			validTargets: []roachpb.StoreID{3, 4, 5, 6},
		},
		{
			constraints: threeSpecificLocalities,
			existing:    []roachpb.StoreID{1, 2, 3},
			expected: []rebalanceStoreIDs{
				{
					existing:   1,
					candidates: []roachpb.StoreID{5, 6},
				},
				{
					existing:   2,
					candidates: []roachpb.StoreID{5, 6},
				},
			},
			validTargets: []roachpb.StoreID{5, 6},
		},
		{
			constraints: threeSpecificLocalities,
			existing:    []roachpb.StoreID{1, 3, 7},
			expected: []rebalanceStoreIDs{
				{
					existing:   7,
					candidates: []roachpb.StoreID{5, 6},
				},
			},
			validTargets: []roachpb.StoreID{5, 6},
		},
		{
			constraints: threeSpecificLocalities,
			existing:    []roachpb.StoreID{1, 2, 7},
			expected: []rebalanceStoreIDs{
				{
					existing:   1,
					candidates: []roachpb.StoreID{3, 4, 5, 6},
				},
				{
					existing:   2,
					candidates: []roachpb.StoreID{3, 4, 5, 6},
				},
				{
					existing:   7,
					candidates: []roachpb.StoreID{3, 4, 5, 6},
				},
			},
			validTargets: []roachpb.StoreID{3, 4, 5, 6},
		},
		{
			constraints: threeSpecificLocalities,
			existing:    []roachpb.StoreID{1, 7, 8},
			expected: []rebalanceStoreIDs{
				{
					existing:   7,
					candidates: []roachpb.StoreID{3, 4, 5, 6},
				},
				{
					existing:   8,
					candidates: []roachpb.StoreID{3, 4, 5, 6},
				},
			},
			validTargets: []roachpb.StoreID{3, 4, 5, 6},
		},
		{
			constraints:  twoAndOneLocalities,
			existing:     []roachpb.StoreID{1, 2, 3},
			expected:     []rebalanceStoreIDs{},
			validTargets: []roachpb.StoreID{},
		},
		{
			constraints: twoAndOneLocalities,
			existing:    []roachpb.StoreID{2, 3, 4},
			expected: []rebalanceStoreIDs{
				{
					existing:   3,
					candidates: []roachpb.StoreID{1},
				},
				{
					existing:   4,
					candidates: []roachpb.StoreID{1},
				},
			},
			validTargets: []roachpb.StoreID{1},
		},
		{
			constraints: twoAndOneLocalities,
			existing:    []roachpb.StoreID{1, 2, 5},
			expected: []rebalanceStoreIDs{
				{
					existing:   1,
					candidates: []roachpb.StoreID{3, 4},
				},
				{
					existing:   2,
					candidates: []roachpb.StoreID{3, 4},
				},
				{
					existing:   5,
					candidates: []roachpb.StoreID{3, 4},
				},
			},
			validTargets: []roachpb.StoreID{3, 4},
		},
		{
			constraints: twoAndOneLocalities,
			existing:    []roachpb.StoreID{1, 3, 5},
			expected: []rebalanceStoreIDs{
				{
					existing:   5,
					candidates: []roachpb.StoreID{2},
				},
			},
			validTargets: []roachpb.StoreID{2},
		},
		{
			constraints: twoAndOneLocalities,
			existing:    []roachpb.StoreID{1, 5, 6},
			expected: []rebalanceStoreIDs{
				{
					existing:   5,
					candidates: []roachpb.StoreID{3, 4},
				},
				{
					existing:   6,
					candidates: []roachpb.StoreID{3, 4},
				},
			},
			validTargets: []roachpb.StoreID{3, 4},
		},
		{
			constraints: twoAndOneLocalities,
			existing:    []roachpb.StoreID{3, 5, 6},
			expected: []rebalanceStoreIDs{
				{
					existing:   5,
					candidates: []roachpb.StoreID{1, 2},
				},
				{
					existing:   6,
					candidates: []roachpb.StoreID{1, 2},
				},
			},
			validTargets: []roachpb.StoreID{1, 2},
		},
		{
			constraints: twoAndOneLocalities,
			existing:    []roachpb.StoreID{1, 3, 4},
			expected: []rebalanceStoreIDs{
				{
					existing:   3,
					candidates: []roachpb.StoreID{2},
				},
				{
					existing:   4,
					candidates: []roachpb.StoreID{2},
				},
			},
			validTargets: []roachpb.StoreID{2},
		},
		{
			constraints:  threeInOneLocality,
			existing:     []roachpb.StoreID{1, 2, 3},
			expected:     []rebalanceStoreIDs{},
			validTargets: []roachpb.StoreID{},
		},
		{
			constraints: threeInOneLocality,
			existing:    []roachpb.StoreID{1, 3, 4},
			expected: []rebalanceStoreIDs{
				{
					existing:   3,
					candidates: []roachpb.StoreID{2},
				},
				{
					existing:   4,
					candidates: []roachpb.StoreID{2},
				},
			},
			validTargets: []roachpb.StoreID{2},
		},
		{
			constraints: threeInOneLocality,
			existing:    []roachpb.StoreID{3, 4, 5},
			expected: []rebalanceStoreIDs{
				{
					existing:   3,
					candidates: []roachpb.StoreID{1, 2},
				},
				{
					existing:   4,
					candidates: []roachpb.StoreID{1, 2},
				},
				{
					existing:   5,
					candidates: []roachpb.StoreID{1, 2},
				},
			},
			validTargets: []roachpb.StoreID{1, 2},
		},
		{
			constraints:  twoAndOneNodeAttrs,
			existing:     []roachpb.StoreID{1, 4, 5},
			expected:     []rebalanceStoreIDs{},
			validTargets: []roachpb.StoreID{},
		},
		{
			constraints:  twoAndOneNodeAttrs,
			existing:     []roachpb.StoreID{3, 6, 7},
			expected:     []rebalanceStoreIDs{},
			validTargets: []roachpb.StoreID{},
		},
		{
			constraints: twoAndOneNodeAttrs,
			existing:    []roachpb.StoreID{1, 2, 3},
			expected: []rebalanceStoreIDs{
				{
					existing:   1,
					candidates: []roachpb.StoreID{5, 7},
				},
				{
					existing:   2,
					candidates: []roachpb.StoreID{6, 8},
				},
			},
			validTargets: []roachpb.StoreID{5, 6, 7, 8},
		},
		{
			constraints: twoAndOneNodeAttrs,
			existing:    []roachpb.StoreID{2, 3, 4},
			expected: []rebalanceStoreIDs{
				{
					existing:   2,
					candidates: []roachpb.StoreID{1, 5, 7},
				},
				{
					existing:   3,
					candidates: []roachpb.StoreID{5, 7},
				},
				{
					existing:   4,
					candidates: []roachpb.StoreID{5, 7},
				},
			},
			validTargets: []roachpb.StoreID{5, 7},
		},
		{
			constraints: twoAndOneNodeAttrs,
			existing:    []roachpb.StoreID{2, 4, 5},
			expected: []rebalanceStoreIDs{
				{
					existing:   2,
					candidates: []roachpb.StoreID{1, 7},
				},
				{
					existing:   4,
					candidates: []roachpb.StoreID{3, 7},
				},
			},
			validTargets: []roachpb.StoreID{1, 3, 7},
		},
		{
			constraints: twoAndOneNodeAttrs,
			existing:    []roachpb.StoreID{1, 3, 5},
			expected: []rebalanceStoreIDs{
				{
					existing:   1,
					candidates: []roachpb.StoreID{2, 8},
				},
				{
					existing:   3,
					candidates: []roachpb.StoreID{4, 8},
				},
				{
					existing:   5,
					candidates: []roachpb.StoreID{6, 8},
				},
			},
			validTargets: []roachpb.StoreID{2, 4, 6, 8},
		},
		{
			constraints: twoAndOneNodeAttrs,
			existing:    []roachpb.StoreID{2, 4, 6},
			expected: []rebalanceStoreIDs{
				{
					existing:   2,
					candidates: []roachpb.StoreID{1, 7},
				},
				{
					existing:   4,
					candidates: []roachpb.StoreID{3, 7},
				},
				{
					existing:   6,
					candidates: []roachpb.StoreID{5, 7},
				},
			},
			validTargets: []roachpb.StoreID{1, 3, 5, 7},
		},
		{
			constraints:  twoAndOneStoreAttrs,
			existing:     []roachpb.StoreID{1, 4, 5},
			expected:     []rebalanceStoreIDs{},
			validTargets: []roachpb.StoreID{},
		},
		{
			constraints:  twoAndOneStoreAttrs,
			existing:     []roachpb.StoreID{3, 6, 7},
			expected:     []rebalanceStoreIDs{},
			validTargets: []roachpb.StoreID{},
		},
		{
			constraints: twoAndOneStoreAttrs,
			existing:    []roachpb.StoreID{1, 2, 3},
			expected: []rebalanceStoreIDs{
				{
					existing:   1,
					candidates: []roachpb.StoreID{5, 7},
				},
				{
					existing:   2,
					candidates: []roachpb.StoreID{6, 8},
				},
			},
			validTargets: []roachpb.StoreID{5, 6, 7, 8},
		},
		{
			constraints: twoAndOneStoreAttrs,
			existing:    []roachpb.StoreID{2, 3, 4},
			expected: []rebalanceStoreIDs{
				{
					existing:   2,
					candidates: []roachpb.StoreID{1, 5, 7},
				},
				{
					existing:   3,
					candidates: []roachpb.StoreID{5, 7},
				},
				{
					existing:   4,
					candidates: []roachpb.StoreID{5, 7},
				},
			},
			validTargets: []roachpb.StoreID{5, 7},
		},
		{
			constraints: twoAndOneStoreAttrs,
			existing:    []roachpb.StoreID{2, 4, 5},
			expected: []rebalanceStoreIDs{
				{
					existing:   2,
					candidates: []roachpb.StoreID{1, 7},
				},
				{
					existing:   4,
					candidates: []roachpb.StoreID{3, 7},
				},
			},
			validTargets: []roachpb.StoreID{1, 3, 7},
		},
		{
			constraints: twoAndOneStoreAttrs,
			existing:    []roachpb.StoreID{1, 3, 5},
			expected: []rebalanceStoreIDs{
				{
					existing:   1,
					candidates: []roachpb.StoreID{2, 8},
				},
				{
					existing:   3,
					candidates: []roachpb.StoreID{4, 8},
				},
				{
					existing:   5,
					candidates: []roachpb.StoreID{6, 8},
				},
			},
			validTargets: []roachpb.StoreID{2, 4, 6, 8},
		},
		{
			constraints: twoAndOneStoreAttrs,
			existing:    []roachpb.StoreID{2, 4, 6},
			expected: []rebalanceStoreIDs{
				{
					existing:   2,
					candidates: []roachpb.StoreID{1, 7},
				},
				{
					existing:   4,
					candidates: []roachpb.StoreID{3, 7},
				},
				{
					existing:   6,
					candidates: []roachpb.StoreID{5, 7},
				},
			},
			validTargets: []roachpb.StoreID{1, 3, 5, 7},
		},
		{
			constraints:  mixLocalityAndAttrs,
			existing:     []roachpb.StoreID{1, 3, 6},
			expected:     []rebalanceStoreIDs{},
			validTargets: []roachpb.StoreID{},
		},
		{
			constraints:  mixLocalityAndAttrs,
			existing:     []roachpb.StoreID{1, 3, 8},
			expected:     []rebalanceStoreIDs{},
			validTargets: []roachpb.StoreID{},
		},
		{
			constraints: mixLocalityAndAttrs,
			existing:    []roachpb.StoreID{1, 5, 8},
			expected: []rebalanceStoreIDs{
				{
					existing:   5,
					candidates: []roachpb.StoreID{3},
				},
			},
			validTargets: []roachpb.StoreID{3},
		},
		{
			constraints: mixLocalityAndAttrs,
			existing:    []roachpb.StoreID{1, 5, 6},
			expected: []rebalanceStoreIDs{
				{
					existing:   5,
					candidates: []roachpb.StoreID{3},
				},
				{
					existing:   6,
					candidates: []roachpb.StoreID{3, 4, 8},
				},
			},
			validTargets: []roachpb.StoreID{3},
		},
		{
			constraints: mixLocalityAndAttrs,
			existing:    []roachpb.StoreID{1, 3, 5},
			expected: []rebalanceStoreIDs{
				{
					existing:   5,
					candidates: []roachpb.StoreID{6, 8},
				},
			},
			validTargets: []roachpb.StoreID{6, 8},
		},
		{
			constraints: mixLocalityAndAttrs,
			existing:    []roachpb.StoreID{1, 2, 3},
			expected: []rebalanceStoreIDs{
				{
					existing:   2,
					candidates: []roachpb.StoreID{6, 8},
				},
			},
			validTargets: []roachpb.StoreID{6, 8},
		},
		{
			constraints: mixLocalityAndAttrs,
			existing:    []roachpb.StoreID{1, 3, 4},
			expected: []rebalanceStoreIDs{
				{
					existing:   4,
					candidates: []roachpb.StoreID{6, 8},
				},
			},
			validTargets: []roachpb.StoreID{6, 8},
		},
		{
			constraints: mixLocalityAndAttrs,
			existing:    []roachpb.StoreID{2, 3, 4},
			expected: []rebalanceStoreIDs{
				{
					existing:   2,
					candidates: []roachpb.StoreID{1},
				},
				{
					existing:   4,
					candidates: []roachpb.StoreID{1},
				},
			},
			validTargets: []roachpb.StoreID{1},
		},
		{
			constraints: mixLocalityAndAttrs,
			existing:    []roachpb.StoreID{5, 6, 7},
			expected: []rebalanceStoreIDs{
				{
					existing:   5,
					candidates: []roachpb.StoreID{1, 3},
				},
				{
					existing:   6,
					candidates: []roachpb.StoreID{1, 2, 3, 4},
				},
				{
					existing:   7,
					candidates: []roachpb.StoreID{1, 3},
				},
			},
			validTargets: []roachpb.StoreID{1, 3},
		},
		{
			constraints: mixLocalityAndAttrs,
			existing:    []roachpb.StoreID{6, 7, 8},
			expected: []rebalanceStoreIDs{
				{
					existing:   6,
					candidates: []roachpb.StoreID{1, 3},
				},
				{
					existing:   7,
					candidates: []roachpb.StoreID{1, 3},
				},
				{
					existing:   8,
					candidates: []roachpb.StoreID{1, 3},
				},
			},
			validTargets: []roachpb.StoreID{1, 3},
		},
		{
			constraints: mixLocalityAndAttrs,
			existing:    []roachpb.StoreID{1, 6, 8},
			expected: []rebalanceStoreIDs{
				{
					existing:   6,
					candidates: []roachpb.StoreID{3},
				},
				{
					existing:   8,
					candidates: []roachpb.StoreID{3},
				},
			},
			validTargets: []roachpb.StoreID{3},
		},
		{
			constraints: mixLocalityAndAttrs,
			existing:    []roachpb.StoreID{1, 5, 7},
			expected: []rebalanceStoreIDs{
				{
					existing:   5,
					candidates: []roachpb.StoreID{3, 4, 6},
				},
				{
					existing:   7,
					candidates: []roachpb.StoreID{3, 4, 8},
				},
			},
			validTargets: []roachpb.StoreID{3, 4, 6, 8},
		},
		{
			constraints:  twoSpecificLocalities,
			numReplicas:  3,
			existing:     []roachpb.StoreID{1, 3, 5},
			expected:     []rebalanceStoreIDs{},
			validTargets: []roachpb.StoreID{},
		},
		{
			constraints:  twoSpecificLocalities,
			numReplicas:  3,
			existing:     []roachpb.StoreID{1, 3, 7},
			expected:     []rebalanceStoreIDs{},
			validTargets: []roachpb.StoreID{},
		},
		{
			constraints:  twoSpecificLocalities,
			numReplicas:  3,
			existing:     []roachpb.StoreID{2, 4, 8},
			expected:     []rebalanceStoreIDs{},
			validTargets: []roachpb.StoreID{},
		},
		{
			constraints: twoSpecificLocalities,
			numReplicas: 3,
			existing:    []roachpb.StoreID{1, 2, 3},
			expected: []rebalanceStoreIDs{
				{
					existing:   1,
					candidates: []roachpb.StoreID{5, 6, 7, 8},
				},
				{
					existing:   2,
					candidates: []roachpb.StoreID{5, 6, 7, 8},
				},
			},
			validTargets: []roachpb.StoreID{5, 6, 7, 8},
		},
		{
			constraints: twoSpecificLocalities,
			numReplicas: 3,
			existing:    []roachpb.StoreID{2, 3, 4},
			expected: []rebalanceStoreIDs{
				{
					existing:   3,
					candidates: []roachpb.StoreID{5, 6, 7, 8},
				},
				{
					existing:   4,
					candidates: []roachpb.StoreID{5, 6, 7, 8},
				},
			},
			validTargets: []roachpb.StoreID{5, 6, 7, 8},
		},
		{
			constraints: twoSpecificLocalities,
			numReplicas: 3,
			existing:    []roachpb.StoreID{1, 2, 5},
			expected: []rebalanceStoreIDs{
				{
					existing:   1,
					candidates: []roachpb.StoreID{3, 4},
				},
				{
					existing:   2,
					candidates: []roachpb.StoreID{3, 4},
				},
				{
					existing:   5,
					candidates: []roachpb.StoreID{3, 4},
				},
			},
			validTargets: []roachpb.StoreID{3, 4},
		},
		{
			constraints: twoSpecificLocalities,
			numReplicas: 3,
			existing:    []roachpb.StoreID{3, 4, 5},
			expected: []rebalanceStoreIDs{
				{
					existing:   3,
					candidates: []roachpb.StoreID{1, 2},
				},
				{
					existing:   4,
					candidates: []roachpb.StoreID{1, 2},
				},
				{
					existing:   5,
					candidates: []roachpb.StoreID{1, 2},
				},
			},
			validTargets: []roachpb.StoreID{1, 2},
		},
		{
			constraints: twoSpecificLocalities,
			numReplicas: 3,
			existing:    []roachpb.StoreID{1, 5, 7},
			expected: []rebalanceStoreIDs{
				{
					existing:   5,
					candidates: []roachpb.StoreID{3, 4},
				},
				{
					existing:   7,
					candidates: []roachpb.StoreID{3, 4},
				},
			},
			validTargets: []roachpb.StoreID{3, 4},
		},
		{
			constraints: twoSpecificLocalities,
			numReplicas: 3,
			existing:    []roachpb.StoreID{1, 5, 6},
			expected: []rebalanceStoreIDs{
				{
					existing:   5,
					candidates: []roachpb.StoreID{3, 4},
				},
				{
					existing:   6,
					candidates: []roachpb.StoreID{3, 4},
				},
			},
			validTargets: []roachpb.StoreID{3, 4},
		},
		{
			constraints: twoSpecificLocalities,
			numReplicas: 3,
			existing:    []roachpb.StoreID{5, 6, 7},
			expected: []rebalanceStoreIDs{
				{
					existing:   5,
					candidates: []roachpb.StoreID{1, 2, 3, 4},
				},
				{
					existing:   6,
					candidates: []roachpb.StoreID{1, 2, 3, 4},
				},
				{
					existing:   7,
					candidates: []roachpb.StoreID{1, 2, 3, 4},
				},
			},
			validTargets: []roachpb.StoreID{1, 2, 3, 4},
		},
	}

	for testIdx, tc := range testCases {
		existingRepls := make([]roachpb.ReplicaDescriptor, len(tc.existing))
		for i, storeID := range tc.existing {
			existingRepls[i] = roachpb.ReplicaDescriptor{
				NodeID:  roachpb.NodeID(storeID),
				StoreID: storeID,
			}
		}
		var rangeUsageInfo RangeUsageInfo
		conf := roachpb.SpanConfig{
			Constraints: tc.constraints,
			NumReplicas: tc.numReplicas,
		}
		analyzed := constraint.AnalyzeConstraints(
			ctx, a.storePool.getStoreDescriptor, existingRepls,
			conf.NumReplicas, conf.Constraints)
		removalConstraintsChecker := voterConstraintsCheckerForRemoval(
			analyzed,
			constraint.EmptyAnalyzedConstraints,
		)
		rebalanceConstraintsChecker := voterConstraintsCheckerForRebalance(
			analyzed,
			constraint.EmptyAnalyzedConstraints,
		)

		results := rankedCandidateListForRebalancing(
			ctx,
			sl,
			removalConstraintsChecker,
			rebalanceConstraintsChecker,
			existingRepls,
			nil,
			a.storePool.getLocalitiesByStore(existingRepls),
			func(context.Context, roachpb.StoreID) bool { return true },
			a.scorerOptions(),
		)
		match := true
		if len(tc.expected) != len(results) {
			match = false
		} else {
			sort.Slice(results, func(i, j int) bool {
				return results[i].existing.store.StoreID < results[j].existing.store.StoreID
			})
			for i := range tc.expected {
				if tc.expected[i].existing != results[i].existing.store.StoreID ||
					!expectedStoreIDsMatch(tc.expected[i].candidates, results[i].candidates) {
					match = false
					break
				}
			}
		}
		if !match {
			t.Errorf("%d: expected rankedCandidateListForRebalancing(%v) = %v, but got %v",
				testIdx, tc.existing, tc.expected, results)
		} else {
			// Also verify that RebalanceVoter picks out one of the best options as
			// the final rebalance choice.
			target, _, details, ok := a.RebalanceVoter(
				ctx,
				conf,
				nil,
				existingRepls,
				nil,
				rangeUsageInfo,
				storeFilterThrottled,
				a.scorerOptions(),
			)
			var found bool
			if !ok && len(tc.validTargets) == 0 {
				found = true
			}
			for _, storeID := range tc.validTargets {
				if storeID == target.StoreID {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("%d: expected RebalanceVoter(%v) to be in %v, but got %v; details: %s",
					testIdx, tc.existing, tc.validTargets, target, details)
			}
		}
	}
}

// Test out the load-based lease transfer algorithm against a variety of
// request distributions and inter-node latencies.
func TestAllocatorTransferLeaseTargetLoadBased(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper, g, _, storePool, _ := createTestStorePool(ctx,
		TestTimeUntilStoreDeadOff, true, /* deterministic */
		func() int { return 10 }, /* nodeCount */
		livenesspb.NodeLivenessStatus_LIVE)
	defer stopper.Stop(ctx)

	// 3 stores where the lease count for each store is equal to 10x the store ID.
	var stores []*roachpb.StoreDescriptor
	for i := 1; i <= 3; i++ {
		stores = append(stores, &roachpb.StoreDescriptor{
			StoreID: roachpb.StoreID(i),
			Node: roachpb.NodeDescriptor{
				NodeID:  roachpb.NodeID(i),
				Address: util.MakeUnresolvedAddr("tcp", strconv.Itoa(i)),
				Locality: roachpb.Locality{
					Tiers: []roachpb.Tier{
						{Key: "l", Value: strconv.Itoa(i)},
					},
				},
			},
			Capacity: roachpb.StoreCapacity{LeaseCount: int32(10 * i)},
		})
	}
	sg := gossiputil.NewStoreGossiper(g)
	sg.GossipStores(stores, t)

	// Nodes need to have descriptors in gossip for the load-based algorithm to
	// consider transferring a lease to them.
	for _, store := range stores {
		if err := g.SetNodeDescriptor(&store.Node); err != nil {
			t.Fatal(err)
		}
	}

	localities := map[roachpb.NodeID]string{
		1: "l=1",
		2: "l=2",
		3: "l=3",
	}
	localityFn := func(nodeID roachpb.NodeID) string {
		return localities[nodeID]
	}
	manual := hlc.NewManualClock(123)
	clock := hlc.NewClock(manual.UnixNano, time.Nanosecond)

	// Set up four different load distributions. Record a bunch of requests to
	// the unknown node 99 in evenlyBalanced to verify that requests from
	// unknown localities don't affect the algorithm.
	evenlyBalanced := newReplicaStats(clock, localityFn)
	evenlyBalanced.recordCount(1, 1)
	evenlyBalanced.recordCount(1, 2)
	evenlyBalanced.recordCount(1, 3)
	imbalanced1 := newReplicaStats(clock, localityFn)
	imbalanced2 := newReplicaStats(clock, localityFn)
	imbalanced3 := newReplicaStats(clock, localityFn)
	for i := 0; i < 100*int(MinLeaseTransferStatsDuration.Seconds()); i++ {
		evenlyBalanced.recordCount(1, 99)
		imbalanced1.recordCount(1, 1)
		imbalanced2.recordCount(1, 2)
		imbalanced3.recordCount(1, 3)
	}

	manual.Increment(int64(MinLeaseTransferStatsDuration))

	noLatency := map[string]time.Duration{}
	highLatency := map[string]time.Duration{
		stores[0].Node.Address.String(): 50 * time.Millisecond,
		stores[1].Node.Address.String(): 50 * time.Millisecond,
		stores[2].Node.Address.String(): 50 * time.Millisecond,
	}

	existing := []roachpb.ReplicaDescriptor{
		{NodeID: 1, StoreID: 1, ReplicaID: 1},
		{NodeID: 2, StoreID: 2, ReplicaID: 2},
		{NodeID: 3, StoreID: 3, ReplicaID: 3},
	}

	testCases := []struct {
		leaseholder roachpb.StoreID
		latency     map[string]time.Duration
		stats       *replicaStats
		check       bool
		expected    roachpb.StoreID
	}{
		// No existing lease holder, nothing to do.
		{leaseholder: 0, latency: noLatency, stats: evenlyBalanced, check: true, expected: 0},
		{leaseholder: 1, latency: noLatency, stats: evenlyBalanced, check: true, expected: 0},
		{leaseholder: 1, latency: noLatency, stats: evenlyBalanced, check: false, expected: 2},
		{leaseholder: 2, latency: noLatency, stats: evenlyBalanced, check: true, expected: 1},
		{leaseholder: 2, latency: noLatency, stats: evenlyBalanced, check: false, expected: 1},
		{leaseholder: 3, latency: noLatency, stats: evenlyBalanced, check: true, expected: 1},
		{leaseholder: 3, latency: noLatency, stats: evenlyBalanced, check: false, expected: 1},
		{leaseholder: 0, latency: noLatency, stats: imbalanced1, check: true, expected: 0},
		{leaseholder: 1, latency: noLatency, stats: imbalanced1, check: true, expected: 0},
		{leaseholder: 1, latency: noLatency, stats: imbalanced1, check: false, expected: 2},
		{leaseholder: 2, latency: noLatency, stats: imbalanced1, check: true, expected: 1},
		{leaseholder: 2, latency: noLatency, stats: imbalanced1, check: false, expected: 1},
		{leaseholder: 3, latency: noLatency, stats: imbalanced1, check: true, expected: 1},
		{leaseholder: 3, latency: noLatency, stats: imbalanced1, check: false, expected: 1},
		{leaseholder: 0, latency: noLatency, stats: imbalanced2, check: true, expected: 0},
		{leaseholder: 1, latency: noLatency, stats: imbalanced2, check: true, expected: 0},
		{leaseholder: 1, latency: noLatency, stats: imbalanced2, check: false, expected: 2},
		{leaseholder: 2, latency: noLatency, stats: imbalanced2, check: true, expected: 1},
		{leaseholder: 2, latency: noLatency, stats: imbalanced2, check: false, expected: 1},
		{leaseholder: 3, latency: noLatency, stats: imbalanced2, check: true, expected: 1},
		{leaseholder: 3, latency: noLatency, stats: imbalanced2, check: false, expected: 1},
		{leaseholder: 0, latency: noLatency, stats: imbalanced3, check: true, expected: 0},
		{leaseholder: 1, latency: noLatency, stats: imbalanced3, check: true, expected: 0},
		{leaseholder: 1, latency: noLatency, stats: imbalanced3, check: false, expected: 2},
		{leaseholder: 2, latency: noLatency, stats: imbalanced3, check: true, expected: 1},
		{leaseholder: 2, latency: noLatency, stats: imbalanced3, check: false, expected: 1},
		{leaseholder: 3, latency: noLatency, stats: imbalanced3, check: true, expected: 1},
		{leaseholder: 3, latency: noLatency, stats: imbalanced3, check: false, expected: 1},
		{leaseholder: 0, latency: highLatency, stats: evenlyBalanced, check: true, expected: 0},
		{leaseholder: 1, latency: highLatency, stats: evenlyBalanced, check: true, expected: 0},
		{leaseholder: 1, latency: highLatency, stats: evenlyBalanced, check: false, expected: 2},
		{leaseholder: 2, latency: highLatency, stats: evenlyBalanced, check: true, expected: 1},
		{leaseholder: 2, latency: highLatency, stats: evenlyBalanced, check: false, expected: 1},
		{leaseholder: 3, latency: highLatency, stats: evenlyBalanced, check: true, expected: 1},
		{leaseholder: 3, latency: highLatency, stats: evenlyBalanced, check: false, expected: 1},
		{leaseholder: 0, latency: highLatency, stats: imbalanced1, check: true, expected: 0},
		{leaseholder: 1, latency: highLatency, stats: imbalanced1, check: true, expected: 0},
		{leaseholder: 1, latency: highLatency, stats: imbalanced1, check: false, expected: 2},
		{leaseholder: 2, latency: highLatency, stats: imbalanced1, check: true, expected: 1},
		{leaseholder: 2, latency: highLatency, stats: imbalanced1, check: false, expected: 1},
		{leaseholder: 3, latency: highLatency, stats: imbalanced1, check: true, expected: 1},
		{leaseholder: 3, latency: highLatency, stats: imbalanced1, check: false, expected: 1},
		{leaseholder: 0, latency: highLatency, stats: imbalanced2, check: true, expected: 0},
		{leaseholder: 1, latency: highLatency, stats: imbalanced2, check: true, expected: 2},
		{leaseholder: 1, latency: highLatency, stats: imbalanced2, check: false, expected: 2},
		{leaseholder: 2, latency: highLatency, stats: imbalanced2, check: true, expected: 0},
		{leaseholder: 2, latency: highLatency, stats: imbalanced2, check: false, expected: 1},
		{leaseholder: 3, latency: highLatency, stats: imbalanced2, check: true, expected: 2},
		{leaseholder: 3, latency: highLatency, stats: imbalanced2, check: false, expected: 2},
		{leaseholder: 0, latency: highLatency, stats: imbalanced3, check: true, expected: 0},
		{leaseholder: 1, latency: highLatency, stats: imbalanced3, check: true, expected: 3},
		{leaseholder: 1, latency: highLatency, stats: imbalanced3, check: false, expected: 3},
		{leaseholder: 2, latency: highLatency, stats: imbalanced3, check: true, expected: 3},
		{leaseholder: 2, latency: highLatency, stats: imbalanced3, check: false, expected: 3},
		{leaseholder: 3, latency: highLatency, stats: imbalanced3, check: true, expected: 0},
		{leaseholder: 3, latency: highLatency, stats: imbalanced3, check: false, expected: 1},
	}

	for _, c := range testCases {
		t.Run("", func(t *testing.T) {
			a := MakeAllocator(
				storePool, func(addr string) (time.Duration, bool) {
					return c.latency[addr], true
				}, nil, /* knobs */
			)
			target := a.TransferLeaseTarget(
				ctx,
				emptySpanConfig(),
				existing,
				&mockRepl{
					replicationFactor: 3,
					storeID:           c.leaseholder,
				},
				c.stats,
				false,
				transferLeaseOptions{
					checkTransferLeaseSource: c.check,
					checkCandidateFullness:   true,
					dryRun:                   false,
				},
			)
			if c.expected != target.StoreID {
				t.Errorf("expected %d, got %d", c.expected, target.StoreID)
			}
		})
	}
}

func TestLoadBasedLeaseRebalanceScore(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	enableLoadBasedLeaseRebalancing.Override(ctx, &st.SV, true)

	remoteStore := roachpb.StoreDescriptor{
		Node: roachpb.NodeDescriptor{
			NodeID: 2,
		},
	}
	sourceStore := roachpb.StoreDescriptor{
		Node: roachpb.NodeDescriptor{
			NodeID: 1,
		},
	}

	testCases := []struct {
		remoteWeight  float64
		remoteLatency time.Duration
		remoteLeases  int32
		sourceWeight  float64
		sourceLeases  int32
		meanLeases    float64
		expected      int32
	}{
		// Evenly balanced leases stay balanced if requests are even
		{1, 0 * time.Millisecond, 10, 1, 10, 10, -2},
		{1, 0 * time.Millisecond, 100, 1, 100, 100, -21},
		{1, 0 * time.Millisecond, 1000, 1, 1000, 1000, -200},
		{1, 10 * time.Millisecond, 10, 1, 10, 10, -2},
		{1, 10 * time.Millisecond, 100, 1, 100, 100, -21},
		{1, 10 * time.Millisecond, 1000, 1, 1000, 1000, -200},
		{1, 50 * time.Millisecond, 10, 1, 10, 10, -2},
		{1, 50 * time.Millisecond, 100, 1, 100, 100, -21},
		{1, 50 * time.Millisecond, 1000, 1, 1000, 1000, -200},
		{1000, 0 * time.Millisecond, 10, 1000, 10, 10, -2},
		{1000, 0 * time.Millisecond, 100, 1000, 100, 100, -21},
		{1000, 0 * time.Millisecond, 1000, 1000, 1000, 1000, -200},
		{1000, 10 * time.Millisecond, 10, 1000, 10, 10, -2},
		{1000, 10 * time.Millisecond, 100, 1000, 100, 100, -21},
		{1000, 10 * time.Millisecond, 1000, 1000, 1000, 1000, -200},
		{1000, 50 * time.Millisecond, 10, 1000, 10, 10, -2},
		{1000, 50 * time.Millisecond, 100, 1000, 100, 100, -21},
		{1000, 50 * time.Millisecond, 1000, 1000, 1000, 1000, -200},
		// No latency favors lease balance despite request imbalance
		{10, 0 * time.Millisecond, 100, 1, 100, 100, -21},
		{100, 0 * time.Millisecond, 100, 1, 100, 100, -21},
		{1000, 0 * time.Millisecond, 100, 1, 100, 100, -21},
		{10000, 0 * time.Millisecond, 100, 1, 100, 100, -21},
		// Adding some latency changes that (perhaps a bit too much?)
		{10, 1 * time.Millisecond, 100, 1, 100, 100, -8},
		{100, 1 * time.Millisecond, 100, 1, 100, 100, 6},
		{1000, 1 * time.Millisecond, 100, 1, 100, 100, 20},
		{10000, 1 * time.Millisecond, 100, 1, 100, 100, 34},
		{10, 10 * time.Millisecond, 100, 1, 100, 100, 26},
		{100, 10 * time.Millisecond, 100, 1, 100, 100, 74},
		{1000, 10 * time.Millisecond, 100, 1, 100, 100, 122},
		{10000, 10 * time.Millisecond, 100, 1, 100, 100, 170},
		// Moving from very unbalanced to more balanced
		{1, 1 * time.Millisecond, 0, 1, 500, 200, 459},
		{1, 1 * time.Millisecond, 0, 10, 500, 200, 432},
		{1, 1 * time.Millisecond, 0, 100, 500, 200, 404},
		{1, 10 * time.Millisecond, 0, 1, 500, 200, 459},
		{1, 10 * time.Millisecond, 0, 10, 500, 200, 364},
		{1, 10 * time.Millisecond, 0, 100, 500, 200, 268},
		{1, 50 * time.Millisecond, 0, 1, 500, 200, 459},
		{1, 50 * time.Millisecond, 0, 10, 500, 200, 302},
		{1, 50 * time.Millisecond, 0, 100, 500, 200, 144},
		{1, 1 * time.Millisecond, 50, 1, 500, 250, 400},
		{1, 1 * time.Millisecond, 50, 10, 500, 250, 364},
		{1, 1 * time.Millisecond, 50, 100, 500, 250, 330},
		{1, 10 * time.Millisecond, 50, 1, 500, 250, 400},
		{1, 10 * time.Millisecond, 50, 10, 500, 250, 280},
		{1, 10 * time.Millisecond, 50, 100, 500, 250, 160},
		{1, 50 * time.Millisecond, 50, 1, 500, 250, 400},
		{1, 50 * time.Millisecond, 50, 10, 500, 250, 202},
		{1, 50 * time.Millisecond, 50, 100, 500, 250, 6},
		// Miscellaneous cases with uneven balance
		{10, 1 * time.Millisecond, 100, 1, 50, 67, -56},
		{1, 1 * time.Millisecond, 50, 10, 100, 67, 26},
		{10, 10 * time.Millisecond, 100, 1, 50, 67, -32},
		{1, 10 * time.Millisecond, 50, 10, 100, 67, 4},
		{10, 1 * time.Millisecond, 100, 1, 50, 80, -56},
		{1, 1 * time.Millisecond, 50, 10, 100, 80, 22},
		{10, 10 * time.Millisecond, 100, 1, 50, 80, -28},
		{1, 10 * time.Millisecond, 50, 10, 100, 80, -6},
	}

	for _, c := range testCases {
		remoteStore.Capacity.LeaseCount = c.remoteLeases
		sourceStore.Capacity.LeaseCount = c.sourceLeases
		score, _ := loadBasedLeaseRebalanceScore(
			ctx,
			st,
			c.remoteWeight,
			c.remoteLatency,
			remoteStore,
			c.sourceWeight,
			sourceStore,
			c.meanLeases,
		)
		if c.expected != score {
			t.Errorf("%+v: expected %d, got %d", c, c.expected, score)
		}
	}
}

// TestAllocatorRemoveTargetBasedOnCapacity verifies that the replica chosen by
// RemoveVoter is the one with the lowest capacity.
func TestAllocatorRemoveTargetBasedOnCapacity(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// List of replicas that will be passed to RemoveVoter
	replicas := []roachpb.ReplicaDescriptor{
		{
			StoreID:   1,
			NodeID:    1,
			ReplicaID: 1,
		},
		{
			StoreID:   2,
			NodeID:    2,
			ReplicaID: 2,
		},
		{
			StoreID:   3,
			NodeID:    3,
			ReplicaID: 3,
		},
		{
			StoreID:   4,
			NodeID:    4,
			ReplicaID: 4,
		},
		{
			StoreID:   5,
			NodeID:    5,
			ReplicaID: 5,
		},
	}

	// Setup the stores so that store 3 is the worst candidate and store 2 is
	// the 2nd worst.
	stores := []*roachpb.StoreDescriptor{
		{
			StoreID:  1,
			Node:     roachpb.NodeDescriptor{NodeID: 1},
			Capacity: roachpb.StoreCapacity{Capacity: 100, Available: 100, RangeCount: 10},
		},
		{
			StoreID:  2,
			Node:     roachpb.NodeDescriptor{NodeID: 2},
			Capacity: roachpb.StoreCapacity{Capacity: 100, Available: 65, RangeCount: 14},
		},
		{
			StoreID:  3,
			Node:     roachpb.NodeDescriptor{NodeID: 3},
			Capacity: roachpb.StoreCapacity{Capacity: 100, Available: 60, RangeCount: 15},
		},
		{
			StoreID:  4,
			Node:     roachpb.NodeDescriptor{NodeID: 4},
			Capacity: roachpb.StoreCapacity{Capacity: 100, Available: 65, RangeCount: 10},
		},
		{
			StoreID:  5,
			Node:     roachpb.NodeDescriptor{NodeID: 5},
			Capacity: roachpb.StoreCapacity{Capacity: 100, Available: 65, RangeCount: 13},
		},
	}

	ctx := context.Background()
	stopper, g, _, a, _ := createTestAllocator(ctx, 10, false /* deterministic */)
	defer stopper.Stop(ctx)
	sg := gossiputil.NewStoreGossiper(g)
	sg.GossipStores(stores, t)

	// Repeat this test 10 times, it should always be either store 2 or 3.
	for i := 0; i < 10; i++ {
		targetRepl, _, err := a.RemoveVoter(
			ctx,
			emptySpanConfig(),
			replicas,
			replicas,
			nil,
			a.scorerOptions(),
		)
		if err != nil {
			t.Fatal(err)
		}
		if a, e1, e2 := targetRepl, replicas[1], replicas[2]; a.StoreID != e1.StoreID && a.StoreID != e2.StoreID {
			t.Fatalf("%d: RemoveVoter did not select either expected replica; expected %v or %v, got %v",
				i, e1, e2, a)
		}
	}
}

func TestAllocatorComputeAction(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Each test case should describe a repair situation which has a lower
	// priority than the previous test case.
	testCases := []struct {
		conf           roachpb.SpanConfig
		desc           roachpb.RangeDescriptor
		expectedAction AllocatorAction
	}{
		// Need three replicas, have three, one is on a dead store.
		{
			conf: roachpb.SpanConfig{
				NumReplicas:   3,
				RangeMaxBytes: 64000,
			},
			desc: roachpb.RangeDescriptor{
				InternalReplicas: []roachpb.ReplicaDescriptor{
					{
						StoreID:   1,
						NodeID:    1,
						ReplicaID: 1,
					},
					{
						StoreID:   2,
						NodeID:    2,
						ReplicaID: 2,
					},
					{
						StoreID:   6,
						NodeID:    6,
						ReplicaID: 6,
					},
				},
			},
			expectedAction: AllocatorReplaceDeadVoter,
		},
		// Need five replicas, one is on a dead store.
		{
			conf: roachpb.SpanConfig{
				NumReplicas:   5,
				RangeMaxBytes: 64000,
			},
			desc: roachpb.RangeDescriptor{
				InternalReplicas: []roachpb.ReplicaDescriptor{
					{
						StoreID:   1,
						NodeID:    1,
						ReplicaID: 1,
					},
					{
						StoreID:   2,
						NodeID:    2,
						ReplicaID: 2,
					},
					{
						StoreID:   3,
						NodeID:    3,
						ReplicaID: 3,
					},
					{
						StoreID:   4,
						NodeID:    4,
						ReplicaID: 4,
					},
					{
						StoreID:   6,
						NodeID:    6,
						ReplicaID: 6,
					},
				},
			},
			expectedAction: AllocatorReplaceDeadVoter,
		},
		// Need 1 non-voter but a voter is on a dead store.
		{
			conf: roachpb.SpanConfig{
				NumReplicas:   5,
				NumVoters:     3,
				RangeMaxBytes: 64000,
			},
			desc: roachpb.RangeDescriptor{
				InternalReplicas: []roachpb.ReplicaDescriptor{
					{
						StoreID:   1,
						NodeID:    1,
						ReplicaID: 1,
					},
					{
						StoreID:   2,
						NodeID:    2,
						ReplicaID: 2,
					},
					{
						StoreID:   6,
						NodeID:    6,
						ReplicaID: 6,
					},
					{
						StoreID:   4,
						NodeID:    4,
						ReplicaID: 4,
						Type:      roachpb.ReplicaTypeNonVoter(),
					},
				},
			},
			expectedAction: AllocatorReplaceDeadVoter,
		},
		// Need 3 replicas, have 2, but one of them is dead so we don't have quorum.
		{
			conf: roachpb.SpanConfig{
				NumReplicas:   3,
				RangeMaxBytes: 64000,
			},
			desc: roachpb.RangeDescriptor{
				InternalReplicas: []roachpb.ReplicaDescriptor{
					{
						StoreID:   1,
						NodeID:    1,
						ReplicaID: 1,
					},
					{
						StoreID:   6,
						NodeID:    6,
						ReplicaID: 6,
					},
				},
			},
			// TODO(aayush): This test should be returning an
			// AllocatorRangeUnavailable.
			expectedAction: AllocatorAddVoter,
		},

		// Need three replicas, have two.
		{
			conf: roachpb.SpanConfig{
				NumReplicas:   3,
				RangeMaxBytes: 64000,
			},
			desc: roachpb.RangeDescriptor{
				InternalReplicas: []roachpb.ReplicaDescriptor{
					{
						StoreID:   1,
						NodeID:    1,
						ReplicaID: 1,
					},
					{
						StoreID:   2,
						NodeID:    2,
						ReplicaID: 2,
					},
				},
			},
			expectedAction: AllocatorAddVoter,
		},
		// Need a voter and a non-voter.
		{
			conf: roachpb.SpanConfig{
				NumReplicas:   5,
				NumVoters:     3,
				RangeMaxBytes: 64000,
			},
			desc: roachpb.RangeDescriptor{
				InternalReplicas: []roachpb.ReplicaDescriptor{
					{
						StoreID:   1,
						NodeID:    1,
						ReplicaID: 1,
					},
					{
						StoreID:   2,
						NodeID:    2,
						ReplicaID: 2,
					},
					{
						StoreID:   4,
						NodeID:    4,
						ReplicaID: 4,
						Type:      roachpb.ReplicaTypeNonVoter(),
					},
				},
			},
			expectedAction: AllocatorAddVoter,
		},
		// Need five replicas, have four, one is on a dead store.
		{
			conf: roachpb.SpanConfig{
				NumReplicas:   5,
				RangeMaxBytes: 64000,
			},
			desc: roachpb.RangeDescriptor{
				InternalReplicas: []roachpb.ReplicaDescriptor{
					{
						StoreID:   1,
						NodeID:    1,
						ReplicaID: 1,
					},
					{
						StoreID:   2,
						NodeID:    2,
						ReplicaID: 2,
					},
					{
						StoreID:   3,
						NodeID:    3,
						ReplicaID: 3,
					},
					{
						StoreID:   6,
						NodeID:    6,
						ReplicaID: 6,
					},
				},
			},
			expectedAction: AllocatorAddVoter,
		},
		// Need five replicas, have four.
		{
			conf: roachpb.SpanConfig{
				NumReplicas:   5,
				RangeMaxBytes: 64000,
			},
			desc: roachpb.RangeDescriptor{
				InternalReplicas: []roachpb.ReplicaDescriptor{
					{
						StoreID:   1,
						NodeID:    1,
						ReplicaID: 1,
					},
					{
						StoreID:   2,
						NodeID:    2,
						ReplicaID: 2,
					},
					{
						StoreID:   3,
						NodeID:    3,
						ReplicaID: 3,
					},
					{
						StoreID:   4,
						NodeID:    4,
						ReplicaID: 4,
					},
				},
			},
			expectedAction: AllocatorAddVoter,
		},
		// Need three replicas, have four, one is on a dead store.
		{
			conf: roachpb.SpanConfig{
				NumReplicas:   3,
				RangeMaxBytes: 64000,
			},
			desc: roachpb.RangeDescriptor{
				InternalReplicas: []roachpb.ReplicaDescriptor{
					{
						StoreID:   1,
						NodeID:    1,
						ReplicaID: 1,
					},
					{
						StoreID:   2,
						NodeID:    2,
						ReplicaID: 2,
					},
					{
						StoreID:   3,
						NodeID:    3,
						ReplicaID: 3,
					},
					{
						StoreID:   6,
						NodeID:    6,
						ReplicaID: 6,
					},
				},
			},
			expectedAction: AllocatorRemoveDeadVoter,
		},
		// Need five replicas, have six, one is on a dead store.
		{
			conf: roachpb.SpanConfig{
				NumReplicas:   5,
				RangeMaxBytes: 64000,
			},
			desc: roachpb.RangeDescriptor{
				InternalReplicas: []roachpb.ReplicaDescriptor{
					{
						StoreID:   1,
						NodeID:    1,
						ReplicaID: 1,
					},
					{
						StoreID:   2,
						NodeID:    2,
						ReplicaID: 2,
					},
					{
						StoreID:   3,
						NodeID:    3,
						ReplicaID: 3,
					},
					{
						StoreID:   4,
						NodeID:    4,
						ReplicaID: 4,
					},
					{
						StoreID:   5,
						NodeID:    5,
						ReplicaID: 5,
					},
					{
						StoreID:   6,
						NodeID:    6,
						ReplicaID: 6,
					},
				},
			},
			expectedAction: AllocatorRemoveDeadVoter,
		},
		// Need three replicas, have five, one is on a dead store.
		{
			conf: roachpb.SpanConfig{
				NumReplicas:   3,
				RangeMaxBytes: 64000,
			},
			desc: roachpb.RangeDescriptor{
				InternalReplicas: []roachpb.ReplicaDescriptor{
					{
						StoreID:   1,
						NodeID:    1,
						ReplicaID: 1,
					},
					{
						StoreID:   2,
						NodeID:    2,
						ReplicaID: 2,
					},
					{
						StoreID:   3,
						NodeID:    3,
						ReplicaID: 3,
					},
					{
						StoreID:   4,
						NodeID:    4,
						ReplicaID: 4,
					},
					{
						StoreID:   6,
						NodeID:    6,
						ReplicaID: 6,
					},
				},
			},
			expectedAction: AllocatorRemoveDeadVoter,
		},
		// Need three replicas, have four.
		{
			conf: roachpb.SpanConfig{
				NumReplicas:   3,
				RangeMaxBytes: 64000,
			},
			desc: roachpb.RangeDescriptor{
				InternalReplicas: []roachpb.ReplicaDescriptor{
					{
						StoreID:   1,
						NodeID:    1,
						ReplicaID: 1,
					},
					{
						StoreID:   2,
						NodeID:    2,
						ReplicaID: 2,
					},
					{
						StoreID:   3,
						NodeID:    3,
						ReplicaID: 3,
					},
					{
						StoreID:   4,
						NodeID:    4,
						ReplicaID: 4,
					},
				},
			},
			expectedAction: AllocatorRemoveVoter,
		},
		// Need three replicas, have five.
		{
			conf: roachpb.SpanConfig{
				NumReplicas:   3,
				RangeMaxBytes: 64000,
			},
			desc: roachpb.RangeDescriptor{
				InternalReplicas: []roachpb.ReplicaDescriptor{
					{
						StoreID:   1,
						NodeID:    1,
						ReplicaID: 1,
					},
					{
						StoreID:   2,
						NodeID:    2,
						ReplicaID: 2,
					},
					{
						StoreID:   3,
						NodeID:    3,
						ReplicaID: 3,
					},
					{
						StoreID:   4,
						NodeID:    4,
						ReplicaID: 4,
					},
					{
						StoreID:   5,
						NodeID:    5,
						ReplicaID: 5,
					},
				},
			},
			expectedAction: AllocatorRemoveVoter,
		},
		// Need 2 non-voting replicas, have 2 but one of them is on a dead node.
		{
			conf: roachpb.SpanConfig{
				NumReplicas:   5,
				NumVoters:     3,
				RangeMaxBytes: 64000,
			},
			desc: roachpb.RangeDescriptor{
				InternalReplicas: []roachpb.ReplicaDescriptor{
					{
						StoreID:   1,
						NodeID:    1,
						ReplicaID: 1,
					},
					{
						StoreID:   2,
						NodeID:    2,
						ReplicaID: 2,
					},
					{
						StoreID:   3,
						NodeID:    3,
						ReplicaID: 3,
					},
					{
						StoreID:   4,
						NodeID:    4,
						ReplicaID: 4,
						Type:      roachpb.ReplicaTypeNonVoter(),
					},
					{
						StoreID:   6,
						NodeID:    6,
						ReplicaID: 6,
						Type:      roachpb.ReplicaTypeNonVoter(),
					},
				},
			},
			expectedAction: AllocatorReplaceDeadNonVoter,
		},
		// Need 2 non-voting replicas, have none.
		{
			conf: roachpb.SpanConfig{
				NumReplicas:   5,
				NumVoters:     3,
				RangeMaxBytes: 64000,
			},
			desc: roachpb.RangeDescriptor{
				InternalReplicas: []roachpb.ReplicaDescriptor{
					{
						StoreID:   1,
						NodeID:    1,
						ReplicaID: 1,
					},
					{
						StoreID:   2,
						NodeID:    2,
						ReplicaID: 2,
					},
					{
						StoreID:   3,
						NodeID:    3,
						ReplicaID: 3,
					},
				},
			},
			expectedAction: AllocatorAddNonVoter,
		},
		// Need 2 non-voting replicas, have 1 but its on a dead node.
		{
			conf: roachpb.SpanConfig{
				NumReplicas:   3,
				NumVoters:     1,
				RangeMaxBytes: 64000,
			},
			desc: roachpb.RangeDescriptor{
				InternalReplicas: []roachpb.ReplicaDescriptor{
					{
						StoreID:   1,
						NodeID:    1,
						ReplicaID: 1,
					},
					{
						StoreID:   6,
						NodeID:    6,
						ReplicaID: 6,
						Type:      roachpb.ReplicaTypeNonVoter(),
					},
				},
			},
			expectedAction: AllocatorAddNonVoter,
		},
		{
			conf: roachpb.SpanConfig{
				NumReplicas:   2,
				NumVoters:     1,
				RangeMaxBytes: 64000,
			},
			desc: roachpb.RangeDescriptor{
				InternalReplicas: []roachpb.ReplicaDescriptor{
					{
						StoreID:   1,
						NodeID:    1,
						ReplicaID: 1,
					},
					{
						StoreID:   2,
						NodeID:    2,
						ReplicaID: 2,
						Type:      roachpb.ReplicaTypeNonVoter(),
					},
					{
						StoreID:   6,
						NodeID:    6,
						ReplicaID: 6,
						Type:      roachpb.ReplicaTypeNonVoter(),
					},
				},
			},
			expectedAction: AllocatorRemoveDeadNonVoter,
		},
		// Need 1 non-voting replicas, have 2.
		{
			conf: roachpb.SpanConfig{
				NumReplicas:   2,
				NumVoters:     1,
				RangeMaxBytes: 64000,
			},
			desc: roachpb.RangeDescriptor{
				InternalReplicas: []roachpb.ReplicaDescriptor{
					{
						StoreID:   1,
						NodeID:    1,
						ReplicaID: 1,
					},
					{
						StoreID:   2,
						NodeID:    2,
						ReplicaID: 2,
						Type:      roachpb.ReplicaTypeNonVoter(),
					},
					{
						StoreID:   3,
						NodeID:    3,
						ReplicaID: 3,
						Type:      roachpb.ReplicaTypeNonVoter(),
					},
				},
			},
			expectedAction: AllocatorRemoveNonVoter,
		},
		// Need three replicas, two are on dead stores. Should
		// be a noop because there aren't enough live replicas for
		// a quorum.
		{
			conf: roachpb.SpanConfig{
				NumReplicas:   3,
				RangeMaxBytes: 64000,
			},
			desc: roachpb.RangeDescriptor{
				InternalReplicas: []roachpb.ReplicaDescriptor{
					{
						StoreID:   1,
						NodeID:    1,
						ReplicaID: 1,
					},
					{
						StoreID:   7,
						NodeID:    7,
						ReplicaID: 7,
					},
					{
						StoreID:   6,
						NodeID:    6,
						ReplicaID: 6,
					},
				},
			},
			expectedAction: AllocatorRangeUnavailable,
		},
		// Need three replicas, have three, none of the replicas in the store pool.
		{
			conf: roachpb.SpanConfig{
				NumReplicas:   3,
				RangeMaxBytes: 64000,
			},
			desc: roachpb.RangeDescriptor{
				InternalReplicas: []roachpb.ReplicaDescriptor{
					{
						StoreID:   10,
						NodeID:    10,
						ReplicaID: 10,
					},
					{
						StoreID:   20,
						NodeID:    20,
						ReplicaID: 20,
					},
					{
						StoreID:   30,
						NodeID:    30,
						ReplicaID: 30,
					},
				},
			},
			expectedAction: AllocatorRangeUnavailable,
		},
		// Need three replicas, have three.
		{
			conf: roachpb.SpanConfig{
				NumReplicas:   3,
				RangeMaxBytes: 64000,
			},
			desc: roachpb.RangeDescriptor{
				InternalReplicas: []roachpb.ReplicaDescriptor{
					{
						StoreID:   1,
						NodeID:    1,
						ReplicaID: 1,
					},
					{
						StoreID:   2,
						NodeID:    2,
						ReplicaID: 2,
					},
					{
						StoreID:   3,
						NodeID:    3,
						ReplicaID: 3,
					},
				},
			},
			expectedAction: AllocatorConsiderRebalance,
		},
	}

	ctx := context.Background()
	stopper, _, sp, a, _ := createTestAllocator(ctx, 10, false /* deterministic */)
	defer stopper.Stop(ctx)

	// Set up eight stores. Stores six and seven are marked as dead. Replica eight
	// is dead.
	mockStorePool(sp,
		[]roachpb.StoreID{1, 2, 3, 4, 5, 8},
		nil,
		[]roachpb.StoreID{6, 7},
		nil,
		nil,
		nil,
	)

	lastPriority := float64(999999999)
	for i, tcase := range testCases {
		action, priority := a.ComputeAction(ctx, tcase.conf, &tcase.desc)
		if tcase.expectedAction != action {
			t.Errorf("Test case %d expected action %q, got action %q",
				i, allocatorActionNames[tcase.expectedAction], allocatorActionNames[action])
			continue
		}
		if tcase.expectedAction != AllocatorNoop && priority > lastPriority {
			t.Errorf("Test cases should have descending priority. Case %d had priority %f, previous case had priority %f", i, priority, lastPriority)
		}
		lastPriority = priority
	}
}

func TestAllocatorComputeActionRemoveDead(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	conf := roachpb.SpanConfig{NumReplicas: 3}
	threeReplDesc := roachpb.RangeDescriptor{
		InternalReplicas: []roachpb.ReplicaDescriptor{
			{
				StoreID:   1,
				NodeID:    1,
				ReplicaID: 1,
			},
			{
				StoreID:   2,
				NodeID:    2,
				ReplicaID: 2,
			},
			{
				StoreID:   3,
				NodeID:    3,
				ReplicaID: 3,
			},
		},
	}
	fourReplDesc := threeReplDesc
	fourReplDesc.InternalReplicas = append(fourReplDesc.InternalReplicas, roachpb.ReplicaDescriptor{
		StoreID:   4,
		NodeID:    4,
		ReplicaID: 4,
	})

	// Each test case should describe a repair situation which has a lower
	// priority than the previous test case.
	testCases := []struct {
		desc           roachpb.RangeDescriptor
		live           []roachpb.StoreID
		dead           []roachpb.StoreID
		expectedAction AllocatorAction
	}{
		// Needs three replicas, one is dead, and there's no replacement. Since
		// there's no replacement we can't do anything, but an action is still
		// emitted.
		{
			desc:           threeReplDesc,
			live:           []roachpb.StoreID{1, 2},
			dead:           []roachpb.StoreID{3},
			expectedAction: AllocatorReplaceDeadVoter,
		},
		// Needs three replicas, one is dead, but there is a replacement.
		{
			desc:           threeReplDesc,
			live:           []roachpb.StoreID{1, 2, 4},
			dead:           []roachpb.StoreID{3},
			expectedAction: AllocatorReplaceDeadVoter,
		},
		// Needs three replicas, two are dead (i.e. the range lacks a quorum).
		{
			desc:           threeReplDesc,
			live:           []roachpb.StoreID{1, 4},
			dead:           []roachpb.StoreID{2, 3},
			expectedAction: AllocatorRangeUnavailable,
		},
		// Needs three replicas, has four, one is dead.
		{
			desc:           fourReplDesc,
			live:           []roachpb.StoreID{1, 2, 4},
			dead:           []roachpb.StoreID{3},
			expectedAction: AllocatorRemoveDeadVoter,
		},
		// Needs three replicas, has four, two are dead (i.e. the range lacks a quorum).
		{
			desc:           fourReplDesc,
			live:           []roachpb.StoreID{1, 4},
			dead:           []roachpb.StoreID{2, 3},
			expectedAction: AllocatorRangeUnavailable,
		},
	}

	ctx := context.Background()
	stopper, _, sp, a, _ := createTestAllocator(ctx, 10, false /* deterministic */)
	defer stopper.Stop(ctx)

	for i, tcase := range testCases {
		mockStorePool(sp, tcase.live, nil, tcase.dead, nil, nil, nil)
		action, _ := a.ComputeAction(ctx, conf, &tcase.desc)
		if tcase.expectedAction != action {
			t.Errorf("Test case %d expected action %d, got action %d", i, tcase.expectedAction, action)
		}
	}
}

func TestAllocatorComputeActionSuspect(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	conf := roachpb.SpanConfig{NumReplicas: 3}
	threeReplDesc := roachpb.RangeDescriptor{
		InternalReplicas: []roachpb.ReplicaDescriptor{
			{
				StoreID:   1,
				NodeID:    1,
				ReplicaID: 1,
			},
			{
				StoreID:   2,
				NodeID:    2,
				ReplicaID: 2,
			},
			{
				StoreID:   3,
				NodeID:    3,
				ReplicaID: 3,
			},
		},
	}

	testCases := []struct {
		desc           roachpb.RangeDescriptor
		live           []roachpb.StoreID
		suspect        []roachpb.StoreID
		expectedAction AllocatorAction
	}{
		{
			desc:           threeReplDesc,
			live:           []roachpb.StoreID{1, 2, 3},
			suspect:        nil,
			expectedAction: AllocatorConsiderRebalance,
		},
		{
			desc:           threeReplDesc,
			live:           []roachpb.StoreID{1, 2},
			suspect:        []roachpb.StoreID{3},
			expectedAction: AllocatorConsiderRebalance,
		},
		{
			desc:           threeReplDesc,
			live:           []roachpb.StoreID{1, 2, 4},
			suspect:        []roachpb.StoreID{3},
			expectedAction: AllocatorConsiderRebalance,
		},
		{
			// When trying to determine whether a range can achieve quorum, we count
			// suspect nodes as live because they _currently_ have a "live" node
			// liveness record.
			desc:           threeReplDesc,
			live:           []roachpb.StoreID{1, 4},
			suspect:        []roachpb.StoreID{2, 3},
			expectedAction: AllocatorConsiderRebalance,
		},
	}

	ctx := context.Background()
	stopper, _, sp, a, _ := createTestAllocator(ctx, 10, false /* deterministic */)
	defer stopper.Stop(ctx)

	for i, tcase := range testCases {
		mockStorePool(sp, tcase.live, nil, nil, nil, nil, tcase.suspect)
		action, _ := a.ComputeAction(ctx, conf, &tcase.desc)
		if tcase.expectedAction != action {
			t.Errorf("Test case %d expected action %d, got action %d", i, tcase.expectedAction, action)
		}
	}
}

func TestAllocatorComputeActionDecommission(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testCases := []struct {
		conf            roachpb.SpanConfig
		desc            roachpb.RangeDescriptor
		expectedAction  AllocatorAction
		live            []roachpb.StoreID
		dead            []roachpb.StoreID
		decommissioning []roachpb.StoreID
		decommissioned  []roachpb.StoreID
	}{
		// Has three replicas, but one is in decommissioning status. We can't
		// replace it (nor add a new replica) since there isn't a live target,
		// but that's still the action being emitted.
		{
			conf: roachpb.SpanConfig{NumReplicas: 3},
			desc: roachpb.RangeDescriptor{
				InternalReplicas: []roachpb.ReplicaDescriptor{
					{
						StoreID:   1,
						NodeID:    1,
						ReplicaID: 1,
					},
					{
						StoreID:   2,
						NodeID:    2,
						ReplicaID: 2,
					},
					{
						StoreID:   3,
						NodeID:    3,
						ReplicaID: 3,
					},
				},
			},
			expectedAction:  AllocatorReplaceDecommissioningVoter,
			live:            []roachpb.StoreID{1, 2},
			dead:            nil,
			decommissioning: []roachpb.StoreID{3},
		},
		// Has three replicas, one is in decommissioning status, and one is on a
		// dead node. Replacing the dead replica is more important.
		{
			conf: roachpb.SpanConfig{NumReplicas: 3},
			desc: roachpb.RangeDescriptor{
				InternalReplicas: []roachpb.ReplicaDescriptor{
					{
						StoreID:   1,
						NodeID:    1,
						ReplicaID: 1,
					},
					{
						StoreID:   2,
						NodeID:    2,
						ReplicaID: 2,
					},
					{
						StoreID:   3,
						NodeID:    3,
						ReplicaID: 3,
					},
				},
			},
			expectedAction:  AllocatorReplaceDeadVoter,
			live:            []roachpb.StoreID{1},
			dead:            []roachpb.StoreID{2},
			decommissioning: []roachpb.StoreID{3},
		},
		// Needs three replicas, has four, where one is decommissioning and one is
		// dead.
		{
			conf: roachpb.SpanConfig{NumReplicas: 3},
			desc: roachpb.RangeDescriptor{
				InternalReplicas: []roachpb.ReplicaDescriptor{
					{
						StoreID:   1,
						NodeID:    1,
						ReplicaID: 1,
					},
					{
						StoreID:   2,
						NodeID:    2,
						ReplicaID: 2,
					},
					{
						StoreID:   3,
						NodeID:    3,
						ReplicaID: 3,
					},
					{
						StoreID:   4,
						NodeID:    4,
						ReplicaID: 4,
					},
				},
			},
			expectedAction:  AllocatorRemoveDeadVoter,
			live:            []roachpb.StoreID{1, 4},
			dead:            []roachpb.StoreID{2},
			decommissioning: []roachpb.StoreID{3},
		},
		// Needs three replicas, has four, where one is decommissioning and one is
		// decommissioned.
		{
			conf: roachpb.SpanConfig{NumReplicas: 3},
			desc: roachpb.RangeDescriptor{
				InternalReplicas: []roachpb.ReplicaDescriptor{
					{
						StoreID:   1,
						NodeID:    1,
						ReplicaID: 1,
					},
					{
						StoreID:   2,
						NodeID:    2,
						ReplicaID: 2,
					},
					{
						StoreID:   3,
						NodeID:    3,
						ReplicaID: 3,
					},
					{
						StoreID:   4,
						NodeID:    4,
						ReplicaID: 4,
					},
				},
			},
			expectedAction:  AllocatorRemoveDeadVoter,
			live:            []roachpb.StoreID{1, 4},
			dead:            nil,
			decommissioning: []roachpb.StoreID{3},
			decommissioned:  []roachpb.StoreID{2},
		},
		// Needs three replicas, has three, all decommissioning
		{
			conf: roachpb.SpanConfig{NumReplicas: 3},
			desc: roachpb.RangeDescriptor{
				InternalReplicas: []roachpb.ReplicaDescriptor{
					{
						StoreID:   1,
						NodeID:    1,
						ReplicaID: 1,
					},
					{
						StoreID:   2,
						NodeID:    2,
						ReplicaID: 2,
					},
					{
						StoreID:   3,
						NodeID:    3,
						ReplicaID: 3,
					},
				},
			},
			expectedAction:  AllocatorReplaceDecommissioningVoter,
			live:            nil,
			dead:            nil,
			decommissioning: []roachpb.StoreID{1, 2, 3},
		},
		// Needs 3. Has 1 live, 3 decommissioning.
		{
			conf: roachpb.SpanConfig{NumReplicas: 3},
			desc: roachpb.RangeDescriptor{
				InternalReplicas: []roachpb.ReplicaDescriptor{
					{
						StoreID:   1,
						NodeID:    1,
						ReplicaID: 1,
					},
					{
						StoreID:   2,
						NodeID:    2,
						ReplicaID: 2,
					},
					{
						StoreID:   3,
						NodeID:    3,
						ReplicaID: 3,
					},
					{
						StoreID:   4,
						NodeID:    4,
						ReplicaID: 4,
					},
				},
			},
			expectedAction:  AllocatorRemoveDecommissioningVoter,
			live:            []roachpb.StoreID{4},
			dead:            nil,
			decommissioning: []roachpb.StoreID{1, 2, 3},
		},
		{
			conf: roachpb.SpanConfig{
				NumVoters:   1,
				NumReplicas: 3,
			},
			desc: roachpb.RangeDescriptor{
				InternalReplicas: []roachpb.ReplicaDescriptor{
					{
						StoreID:   1,
						NodeID:    1,
						ReplicaID: 1,
					},
					{
						StoreID:   4,
						NodeID:    4,
						ReplicaID: 4,
						Type:      roachpb.ReplicaTypeNonVoter(),
					},
					{
						StoreID:   6,
						NodeID:    6,
						ReplicaID: 6,
						Type:      roachpb.ReplicaTypeNonVoter(),
					},
					{
						StoreID:   7,
						NodeID:    7,
						ReplicaID: 7,
						Type:      roachpb.ReplicaTypeNonVoter(),
					},
				},
			},
			expectedAction:  AllocatorRemoveDecommissioningNonVoter,
			live:            []roachpb.StoreID{1, 4, 6},
			dead:            nil,
			decommissioning: []roachpb.StoreID{7},
		},
		{
			conf: roachpb.SpanConfig{
				NumVoters:   1,
				NumReplicas: 3,
			},
			desc: roachpb.RangeDescriptor{
				InternalReplicas: []roachpb.ReplicaDescriptor{
					{
						StoreID:   1,
						NodeID:    1,
						ReplicaID: 1,
					},
					{
						StoreID:   4,
						NodeID:    4,
						ReplicaID: 4,
						Type:      roachpb.ReplicaTypeNonVoter(),
					},
					{
						StoreID:   6,
						NodeID:    6,
						ReplicaID: 6,
						Type:      roachpb.ReplicaTypeNonVoter(),
					},
				},
			},
			expectedAction:  AllocatorReplaceDecommissioningNonVoter,
			live:            []roachpb.StoreID{1, 2, 3, 4, 6},
			dead:            nil,
			decommissioning: []roachpb.StoreID{4},
		},
	}

	ctx := context.Background()
	stopper, _, sp, a, _ := createTestAllocator(ctx, 10, false /* deterministic */)
	defer stopper.Stop(ctx)

	for i, tcase := range testCases {
		mockStorePool(sp, tcase.live, nil, tcase.dead, tcase.decommissioning, tcase.decommissioned, nil)
		action, _ := a.ComputeAction(ctx, tcase.conf, &tcase.desc)
		if tcase.expectedAction != action {
			t.Errorf("Test case %d expected action %s, got action %s", i, tcase.expectedAction, action)
			continue
		}
	}
}

func TestAllocatorRemoveLearner(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	conf := roachpb.SpanConfig{NumReplicas: 3}
	learnerType := roachpb.LEARNER
	rangeWithLearnerDesc := roachpb.RangeDescriptor{
		InternalReplicas: []roachpb.ReplicaDescriptor{
			{
				StoreID:   1,
				NodeID:    1,
				ReplicaID: 1,
			},
			{
				StoreID:   2,
				NodeID:    2,
				ReplicaID: 2,
				Type:      &learnerType,
			},
		},
	}

	// Removing a learner is prioritized over adding a new replica to an under
	// replicated range.
	ctx := context.Background()
	stopper, _, sp, a, _ := createTestAllocator(ctx, 10, false /* deterministic */)
	defer stopper.Stop(ctx)
	live, dead := []roachpb.StoreID{1, 2}, []roachpb.StoreID{3}
	mockStorePool(sp, live, nil, dead, nil, nil, nil)
	action, _ := a.ComputeAction(ctx, conf, &rangeWithLearnerDesc)
	require.Equal(t, AllocatorRemoveLearner, action)
}

func TestAllocatorComputeActionDynamicNumReplicas(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// In this test, the configured span config has a replication factor of five
	// set. We are checking that the effective replication factor is rounded down
	// to the number of stores which are not decommissioned or decommissioning.
	testCases := []struct {
		storeList           []roachpb.StoreID
		expectedNumReplicas int
		expectedAction      AllocatorAction
		live                []roachpb.StoreID
		unavailable         []roachpb.StoreID
		dead                []roachpb.StoreID
		decommissioning     []roachpb.StoreID
	}{
		{
			// Four known stores, three of them are decommissioning, so effective
			// replication factor would be 1 if we hadn't decided that we'll never
			// drop past 3, so 3 it is.
			storeList:           []roachpb.StoreID{1, 2, 3, 4},
			expectedNumReplicas: 3,
			expectedAction:      AllocatorRemoveDecommissioningVoter,
			live:                []roachpb.StoreID{4},
			unavailable:         nil,
			dead:                nil,
			decommissioning:     []roachpb.StoreID{1, 2, 3},
		},
		{
			// Ditto.
			storeList:           []roachpb.StoreID{1, 2, 3},
			expectedNumReplicas: 3,
			expectedAction:      AllocatorReplaceDecommissioningVoter,
			live:                []roachpb.StoreID{4, 5},
			unavailable:         nil,
			dead:                nil,
			decommissioning:     []roachpb.StoreID{1, 2, 3},
		},
		{
			// Four live stores and one dead one, so the effective replication
			// factor would be even (four), in which case we drop down one more
			// to three. Then the right thing becomes removing the dead replica
			// from the range at hand, rather than trying to replace it.
			storeList:           []roachpb.StoreID{1, 2, 3, 4},
			expectedNumReplicas: 3,
			expectedAction:      AllocatorRemoveDeadVoter,
			live:                []roachpb.StoreID{1, 2, 3, 5},
			unavailable:         nil,
			dead:                []roachpb.StoreID{4},
			decommissioning:     nil,
		},
		{
			// Two replicas, one on a dead store, but we have four live nodes
			// in the system which amounts to an effective replication factor
			// of three (avoiding the even number). Adding a replica is more
			// important than replacing the dead one.
			storeList:           []roachpb.StoreID{1, 4},
			expectedNumReplicas: 3,
			expectedAction:      AllocatorAddVoter,
			live:                []roachpb.StoreID{1, 2, 3, 5},
			unavailable:         nil,
			dead:                []roachpb.StoreID{4},
			decommissioning:     nil,
		},
		{
			// Similar to above, but nothing to do.
			storeList:           []roachpb.StoreID{1, 2, 3},
			expectedNumReplicas: 3,
			expectedAction:      AllocatorConsiderRebalance,
			live:                []roachpb.StoreID{1, 2, 3, 4},
			unavailable:         nil,
			dead:                nil,
			decommissioning:     nil,
		},
		{
			// Effective replication factor can't dip below three (unless the
			// span config explicitly asks for that, which it does not), so three
			// it is and we are under-replicaed.
			storeList:           []roachpb.StoreID{1, 2},
			expectedNumReplicas: 3,
			expectedAction:      AllocatorAddVoter,
			live:                []roachpb.StoreID{1, 2},
			unavailable:         nil,
			dead:                nil,
			decommissioning:     nil,
		},
		{
			// Three and happy.
			storeList:           []roachpb.StoreID{1, 2, 3},
			expectedNumReplicas: 3,
			expectedAction:      AllocatorConsiderRebalance,
			live:                []roachpb.StoreID{1, 2, 3},
			unavailable:         nil,
			dead:                nil,
			decommissioning:     nil,
		},
		{
			// Three again, on account of avoiding the even four.
			storeList:           []roachpb.StoreID{1, 2, 3, 4},
			expectedNumReplicas: 3,
			expectedAction:      AllocatorRemoveVoter,
			live:                []roachpb.StoreID{1, 2, 3, 4},
			unavailable:         nil,
			dead:                nil,
			decommissioning:     nil,
		},
		{
			// The usual case in which there are enough nodes to accommodate the
			// span config.
			storeList:           []roachpb.StoreID{1, 2, 3, 4, 5},
			expectedNumReplicas: 5,
			expectedAction:      AllocatorConsiderRebalance,
			live:                []roachpb.StoreID{1, 2, 3, 4, 5},
			unavailable:         nil,
			dead:                nil,
			decommissioning:     nil,
		},
		{
			// No dead or decommissioning node and enough nodes around, so
			// sticking with the span config.
			storeList:           []roachpb.StoreID{1, 2, 3, 4, 5},
			expectedNumReplicas: 5,
			expectedAction:      AllocatorConsiderRebalance,
			live:                []roachpb.StoreID{1, 2, 3, 4},
			unavailable:         []roachpb.StoreID{5},
			dead:                nil,
			decommissioning:     nil,
		},
		{
			// Ditto.
			storeList:           []roachpb.StoreID{1, 2, 3, 4, 5},
			expectedNumReplicas: 5,
			expectedAction:      AllocatorConsiderRebalance,
			live:                []roachpb.StoreID{1, 2, 3},
			unavailable:         []roachpb.StoreID{4, 5},
			dead:                nil,
			decommissioning:     nil,
		},
		{
			// Ditto, but we've lost quorum.
			storeList:           []roachpb.StoreID{1, 2, 3, 4, 5},
			expectedNumReplicas: 5,
			expectedAction:      AllocatorRangeUnavailable,
			live:                []roachpb.StoreID{1, 2},
			unavailable:         []roachpb.StoreID{3, 4, 5},
			dead:                nil,
			decommissioning:     nil,
		},
		{
			// Ditto (dead nodes don't reduce NumReplicas, only decommissioning
			// or decommissioned do, and both correspond to the 'decommissioning'
			// slice in these tests).
			storeList:           []roachpb.StoreID{1, 2, 3, 4, 5},
			expectedNumReplicas: 5,
			expectedAction:      AllocatorReplaceDeadVoter,
			live:                []roachpb.StoreID{1, 2, 3},
			unavailable:         []roachpb.StoreID{4},
			dead:                []roachpb.StoreID{5},
			decommissioning:     nil,
		},
		{
			// Avoiding four, so getting three, and since there is no dead store
			// the most important thing is removing a decommissioning replica.
			storeList:           []roachpb.StoreID{1, 2, 3, 4, 5},
			expectedNumReplicas: 3,
			expectedAction:      AllocatorRemoveDecommissioningVoter,
			live:                []roachpb.StoreID{1, 2, 3},
			unavailable:         []roachpb.StoreID{4},
			dead:                nil,
			decommissioning:     []roachpb.StoreID{5},
		},
	}

	var numNodes int
	ctx := context.Background()
	stopper, _, _, sp, _ := createTestStorePool(ctx,
		TestTimeUntilStoreDeadOff, false, /* deterministic */
		func() int { return numNodes },
		livenesspb.NodeLivenessStatus_LIVE)
	a := MakeAllocator(
		sp, func(string) (time.Duration, bool) {
			return 0, true
		}, nil, /* knobs */
	)

	defer stopper.Stop(ctx)
	conf := roachpb.SpanConfig{NumReplicas: 5}

	for _, prefixKey := range []roachpb.RKey{
		roachpb.RKey(keys.NodeLivenessPrefix),
		roachpb.RKey(keys.SystemPrefix),
	} {
		for _, c := range testCases {
			t.Run(prefixKey.String(), func(t *testing.T) {
				numNodes = len(c.storeList) - len(c.decommissioning)
				mockStorePool(sp, c.live, c.unavailable, c.dead,
					c.decommissioning, nil, nil)
				desc := makeDescriptor(c.storeList)
				desc.EndKey = prefixKey

				clusterNodes := a.storePool.ClusterNodeCount()
				effectiveNumReplicas := GetNeededVoters(conf.NumReplicas, clusterNodes)
				require.Equal(t, c.expectedNumReplicas, effectiveNumReplicas, "clusterNodes=%d", clusterNodes)

				action, _ := a.ComputeAction(ctx, conf, &desc)
				require.Equal(t, c.expectedAction.String(), action.String())
			})
		}
	}
}

func TestAllocatorGetNeededReplicas(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testCases := []struct {
		numReplicas int32
		availNodes  int
		expected    int
	}{
		// If conf.NumReplicas <= 3, GetNeededVoters should always return conf.NumReplicas.
		{1, 0, 1},
		{1, 1, 1},
		{2, 0, 2},
		{2, 1, 2},
		{2, 2, 2},
		{3, 0, 3},
		{3, 1, 3},
		{3, 3, 3},
		// Things get more involved when conf.NumReplicas > 3.
		{4, 1, 3},
		{4, 2, 3},
		{4, 3, 3},
		{4, 4, 4},
		{5, 1, 3},
		{5, 2, 3},
		{5, 3, 3},
		{5, 4, 3},
		{5, 5, 5},
		{6, 1, 3},
		{6, 2, 3},
		{6, 3, 3},
		{6, 4, 3},
		{6, 5, 5},
		{6, 6, 6},
		{7, 1, 3},
		{7, 2, 3},
		{7, 3, 3},
		{7, 4, 3},
		{7, 5, 5},
		{7, 6, 5},
		{7, 7, 7},
	}

	for _, tc := range testCases {
		if e, a := tc.expected, GetNeededVoters(tc.numReplicas, tc.availNodes); e != a {
			t.Errorf(
				"GetNeededVoters(conf.NumReplicas=%d, availNodes=%d) got %d; want %d",
				tc.numReplicas, tc.availNodes, a, e)
		}
	}
}

func makeDescriptor(storeList []roachpb.StoreID) roachpb.RangeDescriptor {
	desc := roachpb.RangeDescriptor{
		EndKey: roachpb.RKey(keys.SystemPrefix),
	}

	desc.InternalReplicas = make([]roachpb.ReplicaDescriptor, len(storeList))

	for i, node := range storeList {
		desc.InternalReplicas[i] = roachpb.ReplicaDescriptor{
			StoreID:   node,
			NodeID:    roachpb.NodeID(node),
			ReplicaID: roachpb.ReplicaID(node),
		}
	}

	return desc
}

// TestAllocatorComputeActionNoStorePool verifies that
// ComputeAction returns AllocatorNoop when storePool is nil.
func TestAllocatorComputeActionNoStorePool(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	a := MakeAllocator(nil /* storePool */, nil /* nodeLatencyFn */, nil /* knobs */)
	action, priority := a.ComputeAction(context.Background(), roachpb.SpanConfig{}, nil)
	if action != AllocatorNoop {
		t.Errorf("expected AllocatorNoop, but got %v", action)
	}
	if priority != 0 {
		t.Errorf("expected priority 0, but got %f", priority)
	}
}

// TestAllocatorError ensures that the correctly formatted error message is
// returned from an allocatorError.
func TestAllocatorError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	constraint := []roachpb.ConstraintsConjunction{
		{Constraints: []roachpb.Constraint{{Value: "one", Type: roachpb.Constraint_REQUIRED}}},
	}
	constraints := []roachpb.ConstraintsConjunction{
		{
			Constraints: []roachpb.Constraint{
				{Value: "one", Type: roachpb.Constraint_REQUIRED},
				{Value: "two", Type: roachpb.Constraint_REQUIRED},
			},
		},
	}

	testCases := []struct {
		ae       allocatorError
		expected string
	}{
		{allocatorError{constraints: nil, existingVoterCount: 1, aliveStores: 1},
			"0 of 1 live stores are able to take a new replica for the range" +
				" (1 already has a voter, 0 already have a non-voter); likely not enough nodes in cluster",
		},
		{allocatorError{constraints: nil, existingVoterCount: 1, aliveStores: 2, throttledStores: 1},
			"0 of 2 live stores are able to take a new replica for the range" +
				" (1 throttled, 1 already has a voter, 0 already have a non-voter)"},
		{allocatorError{constraints: constraint, existingVoterCount: 1, aliveStores: 1},
			"0 of 1 live stores are able to take a new replica for the range" +
				" (1 already has a voter, 0 already have a non-voter);" +
				" replicas must match constraints [{+one}];" +
				" voting replicas must match voter_constraints []",
		},
		{allocatorError{constraints: constraint, existingVoterCount: 1, aliveStores: 2},
			"0 of 2 live stores are able to take a new replica for the range" +
				" (1 already has a voter, 0 already have a non-voter);" +
				" replicas must match constraints [{+one}];" +
				" voting replicas must match voter_constraints []"},
		{allocatorError{constraints: constraints, existingVoterCount: 1, aliveStores: 1},
			"0 of 1 live stores are able to take a new replica for the range" +
				" (1 already has a voter, 0 already have a non-voter);" +
				" replicas must match constraints [{+one,+two}];" +
				" voting replicas must match voter_constraints []"},
		{allocatorError{constraints: constraints, existingVoterCount: 1, aliveStores: 2},
			"0 of 2 live stores are able to take a new replica for the range" +
				" (1 already has a voter, 0 already have a non-voter);" +
				" replicas must match constraints [{+one,+two}];" +
				" voting replicas must match voter_constraints []"},
		{allocatorError{constraints: constraint, existingVoterCount: 1, aliveStores: 2, throttledStores: 1},
			"0 of 2 live stores are able to take a new replica for the range" +
				" (1 throttled, 1 already has a voter, 0 already have a non-voter);" +
				" replicas must match constraints [{+one}];" +
				" voting replicas must match voter_constraints []",
		},
	}

	for i, testCase := range testCases {
		assert.EqualErrorf(t, &testCase.ae, testCase.expected, "test case: %d", i)
	}
}

// TestAllocatorThrottled ensures that when a store is throttled, the replica
// will not be sent to purgatory.
func TestAllocatorThrottled(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper, g, _, a, _ := createTestAllocator(ctx, 10, false /* deterministic */)
	defer stopper.Stop(ctx)

	// First test to make sure we would send the replica to purgatory.
	_, _, err := a.AllocateVoter(ctx, simpleSpanConfig, []roachpb.ReplicaDescriptor{}, nil)
	if !errors.HasInterface(err, (*purgatoryError)(nil)) {
		t.Fatalf("expected a purgatory error, got: %+v", err)
	}

	// Second, test the normal case in which we can allocate to the store.
	gossiputil.NewStoreGossiper(g).GossipStores(singleStore, t)
	result, _, err := a.AllocateVoter(ctx, simpleSpanConfig, []roachpb.ReplicaDescriptor{}, nil)
	if err != nil {
		t.Fatalf("unable to perform allocation: %+v", err)
	}
	if result.NodeID != 1 || result.StoreID != 1 {
		t.Errorf("expected NodeID 1 and StoreID 1: %+v", result)
	}

	// Finally, set that store to be throttled and ensure we don't send the
	// replica to purgatory.
	a.storePool.detailsMu.Lock()
	storeDetail, ok := a.storePool.detailsMu.storeDetails[singleStore[0].StoreID]
	if !ok {
		t.Fatalf("store:%d was not found in the store pool", singleStore[0].StoreID)
	}
	storeDetail.throttledUntil = timeutil.Now().Add(24 * time.Hour)
	a.storePool.detailsMu.Unlock()
	_, _, err = a.AllocateVoter(ctx, simpleSpanConfig, []roachpb.ReplicaDescriptor{}, nil)
	if errors.HasInterface(err, (*purgatoryError)(nil)) {
		t.Fatalf("expected a non purgatory error, got: %+v", err)
	}
}

func TestFilterBehindReplicas(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	testCases := []struct {
		commit   uint64
		leader   uint64
		progress []uint64
		expected []uint64
	}{
		{0, 99, []uint64{0}, nil},
		{1, 99, []uint64{1}, []uint64{1}},
		{2, 99, []uint64{2}, []uint64{2}},
		{1, 99, []uint64{0, 1}, []uint64{1}},
		{1, 99, []uint64{1, 2}, []uint64{1, 2}},
		{2, 99, []uint64{3, 2}, []uint64{3, 2}},
		{1, 99, []uint64{0, 0, 1}, []uint64{1}},
		{1, 99, []uint64{0, 1, 2}, []uint64{1, 2}},
		{2, 99, []uint64{1, 2, 3}, []uint64{2, 3}},
		{3, 99, []uint64{4, 3, 2}, []uint64{4, 3}},
		{1, 99, []uint64{1, 1, 1}, []uint64{1, 1, 1}},
		{1, 99, []uint64{1, 1, 2}, []uint64{1, 1, 2}},
		{2, 99, []uint64{1, 2, 2}, []uint64{2, 2}},
		{2, 99, []uint64{0, 1, 2, 3}, []uint64{2, 3}},
		{2, 99, []uint64{1, 2, 3, 4}, []uint64{2, 3, 4}},
		{3, 99, []uint64{5, 4, 3, 2}, []uint64{5, 4, 3}},
		{3, 99, []uint64{1, 2, 3, 4, 5}, []uint64{3, 4, 5}},
		{4, 99, []uint64{6, 5, 4, 3, 2}, []uint64{6, 5, 4}},
		{4, 99, []uint64{6, 5, 4, 3, 2}, []uint64{6, 5, 4}},
		{0, 1, []uint64{0}, []uint64{0}},
		{0, 1, []uint64{0, 0, 0}, []uint64{0}},
		{1, 1, []uint64{2, 0, 1}, []uint64{2, 1}},
		{1, 2, []uint64{0, 2, 1}, []uint64{2, 1}},
	}
	for _, c := range testCases {
		t.Run("", func(t *testing.T) {
			status := &raft.Status{
				Progress: make(map[uint64]tracker.Progress),
			}
			status.Lead = c.leader
			status.Commit = c.commit
			var replicas []roachpb.ReplicaDescriptor
			for j, v := range c.progress {
				p := tracker.Progress{
					Match: v,
					State: tracker.StateReplicate,
				}
				if v == 0 {
					p.State = tracker.StateProbe
				}
				replicaID := uint64(j + 1)
				status.Progress[replicaID] = p
				replicas = append(replicas, roachpb.ReplicaDescriptor{
					ReplicaID: roachpb.ReplicaID(replicaID),
					StoreID:   roachpb.StoreID(v),
				})
			}
			candidates := filterBehindReplicas(ctx, status, replicas)
			var ids []uint64
			for _, c := range candidates {
				ids = append(ids, uint64(c.StoreID))
			}
			if !reflect.DeepEqual(c.expected, ids) {
				t.Fatalf("expected %d, but got %d", c.expected, ids)
			}
		})
	}
}

func TestFilterUnremovableReplicas(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	testCases := []struct {
		commit            uint64
		progress          []uint64
		brandNewReplicaID roachpb.ReplicaID
		expected          []uint64
	}{
		{0, []uint64{0}, 0, nil},
		{1, []uint64{1}, 0, nil},
		{1, []uint64{0, 1}, 0, nil},
		{1, []uint64{1, 2}, 0, []uint64{1, 2}},
		{1, []uint64{1, 2, 3}, 0, []uint64{1, 2, 3}},
		{2, []uint64{1, 2, 3}, 0, []uint64{1}},
		{3, []uint64{1, 2, 3}, 0, nil},
		{1, []uint64{1, 2, 3, 4}, 0, []uint64{1, 2, 3, 4}},
		{2, []uint64{1, 2, 3, 4}, 0, []uint64{1, 2, 3, 4}},
		{3, []uint64{1, 2, 3, 4}, 0, nil},
		{2, []uint64{1, 2, 3, 4, 5}, 0, []uint64{1, 2, 3, 4, 5}},
		{3, []uint64{1, 2, 3, 4, 5}, 0, []uint64{1, 2}},
		{1, []uint64{1, 0}, 2, nil},
		{1, []uint64{2, 1}, 2, []uint64{2}},
		{1, []uint64{1, 0}, 1, nil},
		{1, []uint64{2, 1}, 1, []uint64{1}},
		{3, []uint64{3, 2, 1}, 3, nil},
		{3, []uint64{3, 2, 0}, 3, nil},
		{2, []uint64{4, 3, 2, 1}, 4, []uint64{4, 3, 2}},
		{2, []uint64{4, 3, 2, 0}, 3, []uint64{4, 3, 0}},
		{2, []uint64{4, 3, 2, 0}, 4, []uint64{4, 3, 2}},
		{3, []uint64{4, 3, 2, 1}, 0, nil},
		{3, []uint64{4, 3, 2, 1}, 4, nil},
	}
	for _, c := range testCases {
		t.Run("", func(t *testing.T) {
			status := &raft.Status{
				Progress: make(map[uint64]tracker.Progress),
			}
			// Use an invalid replica ID for the leader. TestFilterBehindReplicas covers
			// valid replica IDs.
			status.Lead = 99
			status.Commit = c.commit
			var replicas []roachpb.ReplicaDescriptor
			for j, v := range c.progress {
				p := tracker.Progress{
					Match: v,
					State: tracker.StateReplicate,
				}
				if v == 0 {
					p.State = tracker.StateProbe
				}
				replicaID := uint64(j + 1)
				status.Progress[replicaID] = p
				replicas = append(replicas, roachpb.ReplicaDescriptor{
					ReplicaID: roachpb.ReplicaID(replicaID),
					StoreID:   roachpb.StoreID(v),
				})
			}

			candidates := filterUnremovableReplicas(ctx, status, replicas, c.brandNewReplicaID)
			var ids []uint64
			for _, c := range candidates {
				ids = append(ids, uint64(c.StoreID))
			}
			if !reflect.DeepEqual(c.expected, ids) {
				t.Fatalf("expected %d, but got %d", c.expected, ids)
			}
		})
	}
}

func TestSimulateFilterUnremovableReplicas(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	testCases := []struct {
		commit            uint64
		progress          []uint64
		brandNewReplicaID roachpb.ReplicaID
		expected          []uint64
	}{
		{1, []uint64{1, 0}, 2, []uint64{1}},
		{1, []uint64{1, 0}, 1, nil},
		{3, []uint64{3, 2, 1}, 3, []uint64{2}},
		{3, []uint64{3, 2, 0}, 3, []uint64{2}},
		{3, []uint64{4, 3, 2, 1}, 4, []uint64{4, 3, 2}},
		{3, []uint64{4, 3, 2, 0}, 3, []uint64{4, 3, 0}},
		{3, []uint64{4, 3, 2, 0}, 4, []uint64{4, 3, 2}},
	}
	for _, c := range testCases {
		t.Run("", func(t *testing.T) {
			status := &raft.Status{
				Progress: make(map[uint64]tracker.Progress),
			}
			// Use an invalid replica ID for the leader. TestFilterBehindReplicas covers
			// valid replica IDs.
			status.Lead = 99
			status.Commit = c.commit
			var replicas []roachpb.ReplicaDescriptor
			for j, v := range c.progress {
				p := tracker.Progress{
					Match: v,
					State: tracker.StateReplicate,
				}
				if v == 0 {
					p.State = tracker.StateProbe
				}
				replicaID := uint64(j + 1)
				status.Progress[replicaID] = p
				replicas = append(replicas, roachpb.ReplicaDescriptor{
					ReplicaID: roachpb.ReplicaID(replicaID),
					StoreID:   roachpb.StoreID(v),
				})
			}

			candidates := simulateFilterUnremovableReplicas(ctx, status, replicas, c.brandNewReplicaID)
			var ids []uint64
			for _, c := range candidates {
				ids = append(ids, uint64(c.StoreID))
			}
			if !reflect.DeepEqual(c.expected, ids) {
				t.Fatalf("expected %d, but got %d", c.expected, ids)
			}
		})
	}
}

// TestAllocatorRebalanceWithScatter tests that when `scatter` is set to true,
// the allocator will produce rebalance opportunities even when it normally
// wouldn't.
func TestAllocatorRebalanceWithScatter(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper, g, _, a, _ := createTestAllocator(ctx, 10 /* numNodes */, true /* deterministic */)
	defer stopper.Stop(ctx)

	stores := []*roachpb.StoreDescriptor{
		{
			StoreID: 1,
			Node: roachpb.NodeDescriptor{
				NodeID: 1,
			},
			Capacity: roachpb.StoreCapacity{
				RangeCount: 1000,
			},
		},
		{
			StoreID: 2,
			Node: roachpb.NodeDescriptor{
				NodeID: 2,
			},
			Capacity: roachpb.StoreCapacity{
				RangeCount: 1000,
			},
		},
		{
			StoreID: 3,
			Node: roachpb.NodeDescriptor{
				NodeID: 3,
			},
			Capacity: roachpb.StoreCapacity{
				RangeCount: 1000,
			},
		},
	}

	gossiputil.NewStoreGossiper(g).GossipStores(stores, t)

	var rangeUsageInfo RangeUsageInfo

	// Ensure that we wouldn't normally rebalance when all stores have the same
	// replica count.
	_, _, _, ok := a.RebalanceVoter(
		ctx,
		emptySpanConfig(),
		nil,
		replicas(1),
		nil,
		rangeUsageInfo,
		storeFilterThrottled,
		a.scorerOptions(),
	)
	require.False(t, ok)

	// Ensure that we would produce a rebalance target when running with scatter.
	_, _, _, ok = a.RebalanceVoter(
		ctx,
		emptySpanConfig(),
		nil,
		replicas(1),
		nil,
		rangeUsageInfo,
		storeFilterThrottled,
		a.scorerOptionsForScatter(),
	)
	require.True(t, ok)
}

// TestAllocatorRebalanceAway verifies that when a replica is on a node with a
// bad span config, the replica will be rebalanced off of it.
func TestAllocatorRebalanceAway(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	localityUS := roachpb.Locality{Tiers: []roachpb.Tier{{Key: "datacenter", Value: "us"}}}
	localityEUR := roachpb.Locality{Tiers: []roachpb.Tier{{Key: "datacenter", Value: "eur"}}}
	capacityEmpty := roachpb.StoreCapacity{Capacity: 100, Available: 99, RangeCount: 1}
	stores := []*roachpb.StoreDescriptor{
		{
			StoreID: 1,
			Node: roachpb.NodeDescriptor{
				NodeID:   1,
				Locality: localityUS,
			},
			Capacity: capacityEmpty,
		},
		{
			StoreID: 2,
			Node: roachpb.NodeDescriptor{
				NodeID:   2,
				Locality: localityUS,
			},
			Capacity: capacityEmpty,
		},
		{
			StoreID: 3,
			Node: roachpb.NodeDescriptor{
				NodeID:   3,
				Locality: localityEUR,
			},
			Capacity: capacityEmpty,
		},
		{
			StoreID: 4,
			Node: roachpb.NodeDescriptor{
				NodeID:   4,
				Locality: localityUS,
			},
			Capacity: capacityEmpty,
		},
		{
			StoreID: 5,
			Node: roachpb.NodeDescriptor{
				NodeID:   5,
				Locality: localityEUR,
			},
			Capacity: capacityEmpty,
		},
	}

	existingReplicas := []roachpb.ReplicaDescriptor{
		{StoreID: stores[0].StoreID, NodeID: stores[0].Node.NodeID},
		{StoreID: stores[1].StoreID, NodeID: stores[1].Node.NodeID},
		{StoreID: stores[2].StoreID, NodeID: stores[2].Node.NodeID},
	}
	testCases := []struct {
		constraint roachpb.Constraint
		expected   *roachpb.StoreID
	}{
		{
			constraint: roachpb.Constraint{Key: "datacenter", Value: "us", Type: roachpb.Constraint_REQUIRED},
			expected:   &stores[3].StoreID,
		},
		{
			constraint: roachpb.Constraint{Key: "datacenter", Value: "eur", Type: roachpb.Constraint_PROHIBITED},
			expected:   &stores[3].StoreID,
		},
		{
			constraint: roachpb.Constraint{Key: "datacenter", Value: "eur", Type: roachpb.Constraint_REQUIRED},
			expected:   &stores[4].StoreID,
		},
		{
			constraint: roachpb.Constraint{Key: "datacenter", Value: "us", Type: roachpb.Constraint_PROHIBITED},
			expected:   &stores[4].StoreID,
		},
		{
			constraint: roachpb.Constraint{Key: "datacenter", Value: "other", Type: roachpb.Constraint_REQUIRED},
			expected:   nil,
		},
		{
			constraint: roachpb.Constraint{Key: "datacenter", Value: "other", Type: roachpb.Constraint_PROHIBITED},
			expected:   nil,
		},
	}

	ctx := context.Background()
	stopper, g, _, a, _ := createTestAllocator(ctx, 10, false /* deterministic */)
	defer stopper.Stop(ctx)
	gossiputil.NewStoreGossiper(g).GossipStores(stores, t)

	for _, tc := range testCases {
		t.Run(tc.constraint.String(), func(t *testing.T) {
			constraints := roachpb.ConstraintsConjunction{
				Constraints: []roachpb.Constraint{
					tc.constraint,
				},
			}

			var rangeUsageInfo RangeUsageInfo
			actual, _, _, ok := a.RebalanceVoter(
				ctx,
				roachpb.SpanConfig{Constraints: []roachpb.ConstraintsConjunction{constraints}},
				nil,
				existingReplicas,
				nil,
				rangeUsageInfo,
				storeFilterThrottled,
				a.scorerOptions(),
			)

			if tc.expected == nil && ok {
				t.Errorf("rebalancing to the incorrect store, expected nil, got %d", actual.StoreID)
			} else if tc.expected != nil && !ok {
				t.Errorf("rebalancing to the incorrect store, expected %d, got nil", *tc.expected)
			} else if !(tc.expected == nil && !ok) && *tc.expected != actual.StoreID {
				t.Errorf("rebalancing to the incorrect store, expected %d, got %d", tc.expected, actual.StoreID)
			}
		})
	}
}

type testStore struct {
	roachpb.StoreDescriptor
	immediateCompaction bool
}

func (ts *testStore) add(bytes int64, qps float64) {
	ts.Capacity.RangeCount++
	ts.Capacity.Available -= bytes
	ts.Capacity.Used += bytes
	ts.Capacity.LogicalBytes += bytes
	ts.Capacity.QueriesPerSecond += qps
}

func (ts *testStore) rebalance(ots *testStore, bytes int64, qps float64) {
	if ts.Capacity.RangeCount == 0 || (ts.Capacity.Capacity-ts.Capacity.Available) < bytes {
		return
	}
	// Mimic a real Store's behavior of not considering target stores that are
	// almost out of disk. (In a real allocator this is, for example, in
	// rankedCandidateListFor{Allocation,Rebalancing}).
	if !maxCapacityCheck(ots.StoreDescriptor) {
		log.Infof(
			context.Background(),
			"s%d too full to accept snapshot from s%d: %v", ots.StoreID, ts.StoreID, ots.Capacity,
		)
		return
	}
	log.Infof(context.Background(), "s%d accepting snapshot from s%d", ots.StoreID, ts.StoreID)
	ts.Capacity.RangeCount--
	ts.Capacity.QueriesPerSecond -= qps
	if ts.immediateCompaction {
		ts.Capacity.Available += bytes
		ts.Capacity.Used -= bytes
	}
	ts.Capacity.LogicalBytes -= bytes
	ots.Capacity.RangeCount++
	ots.Capacity.QueriesPerSecond += qps
	ots.Capacity.Available -= bytes
	ots.Capacity.Used += bytes
	ots.Capacity.LogicalBytes += bytes
}

func (ts *testStore) compact() {
	ts.Capacity.Used = ts.Capacity.LogicalBytes
	ts.Capacity.Available = ts.Capacity.Capacity - ts.Capacity.Used
}

func TestAllocatorFullDisks(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	st := cluster.MakeTestingClusterSettings()
	tr := tracing.NewTracer()
	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)

	// Model a set of stores in a cluster doing rebalancing, with ranges being
	// randomly added occasionally.
	rpcContext := rpc.NewContext(ctx, rpc.ContextOptions{
		TenantID: roachpb.SystemTenantID,
		Config:   &base.Config{Insecure: true},
		Clock:    clock,
		Stopper:  stopper,
		Settings: st,
	})
	server := rpc.NewServer(rpcContext) // never started
	g := gossip.NewTest(1, rpcContext, server, stopper, metric.NewRegistry(), zonepb.DefaultZoneConfigRef())

	TimeUntilStoreDead.Override(ctx, &st.SV, TestTimeUntilStoreDeadOff)

	const generations = 100
	const nodes = 20
	const capacity = (1 << 30) + 1
	const rangeSize = 16 << 20

	mockNodeLiveness := newMockNodeLiveness(livenesspb.NodeLivenessStatus_LIVE)
	sp := NewStorePool(
		log.MakeTestingAmbientContext(tr),
		st,
		g,
		clock,
		func() int {
			return nodes
		},
		mockNodeLiveness.nodeLivenessFunc,
		false, /* deterministic */
	)
	alloc := MakeAllocator(
		sp, func(string) (time.Duration, bool) {
			return 0, false
		}, nil, /* knobs */
	)

	var wg sync.WaitGroup
	g.RegisterCallback(gossip.MakePrefixPattern(gossip.KeyStorePrefix),
		func(_ string, _ roachpb.Value) { wg.Done() },
		// Redundant callbacks are required by this test.
		gossip.Redundant)

	rangesPerNode := int(math.Floor(capacity * rebalanceToMaxFractionUsedThreshold / rangeSize))
	rangesToAdd := rangesPerNode * nodes

	// Initialize testStores.
	var testStores [nodes]testStore
	for i := 0; i < len(testStores); i++ {
		// Don't immediately reclaim disk space from removed ranges. This mimics
		// range deletions don't immediately reclaim disk space in rocksdb.
		testStores[i].immediateCompaction = false
		testStores[i].StoreID = roachpb.StoreID(i)
		testStores[i].Node = roachpb.NodeDescriptor{NodeID: roachpb.NodeID(i)}
		testStores[i].Capacity = roachpb.StoreCapacity{Capacity: capacity, Available: capacity}
	}
	// Initialize the cluster with a single range.
	testStores[0].add(rangeSize, 0)
	rangesAdded := 1

	for i := 0; i < generations; i++ {
		// First loop through test stores and randomly add data.
		for j := 0; j < len(testStores); j++ {
			if mockNodeLiveness.nodeLivenessFunc(roachpb.NodeID(j), time.Time{}, 0) == livenesspb.NodeLivenessStatus_DEAD {
				continue
			}
			ts := &testStores[j]
			// Add [0,3) ranges to the node, simulating splits and data growth.
			toAdd := alloc.randGen.Intn(3)
			for k := 0; k < toAdd; k++ {
				if rangesAdded < rangesToAdd {
					ts.add(rangeSize, 0)
					rangesAdded++
				}
			}
			if ts.Capacity.Available <= 0 {
				t.Errorf("testStore %d ran out of space during generation %d (rangesAdded=%d/%d): %+v",
					j, i, rangesAdded, rangesToAdd, ts.Capacity)
				mockNodeLiveness.setNodeStatus(roachpb.NodeID(j), livenesspb.NodeLivenessStatus_DEAD)
			}
			wg.Add(1)
			if err := g.AddInfoProto(gossip.MakeStoreKey(roachpb.StoreID(j)), &ts.StoreDescriptor, 0); err != nil {
				t.Fatal(err)
			}
		}
		wg.Wait()

		// Loop through each store a number of times and maybe rebalance.
		for j := 0; j < 10; j++ {
			for k := 0; k < len(testStores); k++ {
				if mockNodeLiveness.nodeLivenessFunc(roachpb.NodeID(k), time.Time{}, 0) == livenesspb.NodeLivenessStatus_DEAD {
					continue
				}
				ts := &testStores[k]
				// Rebalance until there's no more rebalancing to do.
				if ts.Capacity.RangeCount > 0 {
					var rangeUsageInfo RangeUsageInfo
					target, _, details, ok := alloc.RebalanceVoter(
						ctx,
						emptySpanConfig(),
						nil,
						[]roachpb.ReplicaDescriptor{{NodeID: ts.Node.NodeID, StoreID: ts.StoreID}},
						nil,
						rangeUsageInfo,
						storeFilterThrottled,
						alloc.scorerOptions(),
					)
					if ok {
						if log.V(1) {
							log.Infof(ctx, "rebalancing to %v; details: %s", target, details)
						}
						testStores[k].rebalance(&testStores[int(target.StoreID)], rangeSize, 0 /* qps */)
					}
				}
				// Gossip occasionally, as real Stores do when replicas move around.
				if j%3 == 2 {
					wg.Add(1)
					if err := g.AddInfoProto(gossip.MakeStoreKey(roachpb.StoreID(j)), &ts.StoreDescriptor, 0); err != nil {
						t.Fatal(err)
					}
				}
			}
		}

		// Simulate rocksdb compactions freeing up disk space.
		for j := 0; j < len(testStores); j++ {
			if mockNodeLiveness.nodeLivenessFunc(roachpb.NodeID(j), time.Time{}, 0) != livenesspb.NodeLivenessStatus_DEAD {
				ts := &testStores[j]
				if ts.Capacity.Available <= 0 {
					t.Errorf("testStore %d ran out of space during generation %d: %+v", j, i, ts.Capacity)
					mockNodeLiveness.setNodeStatus(roachpb.NodeID(j), livenesspb.NodeLivenessStatus_DEAD)
				} else {
					ts.compact()
				}
			}
		}
	}
}

func Example_rangeCountRebalancing() {
	testStores := make([]testStore, 20)
	rebalanceFn := func(ctx context.Context, ts *testStore, testStores []testStore, alloc *Allocator) {
		var rangeUsageInfo RangeUsageInfo
		target, _, details, ok := alloc.RebalanceVoter(
			ctx,
			emptySpanConfig(),
			nil,
			[]roachpb.ReplicaDescriptor{{NodeID: ts.Node.NodeID, StoreID: ts.StoreID}},
			nil,
			rangeUsageInfo,
			storeFilterThrottled,
			alloc.scorerOptions(),
		)
		if ok {
			log.Infof(ctx, "rebalancing to %v; details: %s", target, details)
			ts.rebalance(&testStores[int(target.StoreID)], alloc.randGen.Int63n(1<<20), 0 /* qps */)
		}
	}

	generation := 0
	const printEvery = 2
	printFn := func(testStores []testStore, table *tablewriter.Table) {
		if generation%printEvery == 0 {
			var totalBytes int64
			for j := 0; j < len(testStores); j++ {
				totalBytes += testStores[j].Capacity.Capacity - testStores[j].Capacity.Available
			}
			row := make([]string, len(testStores)+1)
			row[0] = fmt.Sprintf("%d", generation)
			for j := 0; j < len(testStores); j++ {
				ts := testStores[j]
				bytes := ts.Capacity.Capacity - ts.Capacity.Available
				row[j+1] = fmt.Sprintf("%3d %3d%%", ts.Capacity.RangeCount, (100*bytes)/totalBytes)
			}
			table.Append(row)
		}
		generation++
	}

	exampleRebalancing(testStores, rebalanceFn, printFn)

	// Output:
	// +-----+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+
	// | gen | store 0  | store 1  | store 2  | store 3  | store 4  | store 5  | store 6  | store 7  | store 8  | store 9  | store 10 | store 11 | store 12 | store 13 | store 14 | store 15 | store 16 | store 17 | store 18 | store 19 |
	// +-----+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+
	// |   0 |   2 100% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |
	// |   2 |   3  75% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   2  24% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |
	// |   4 |   3  18% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   1  14% |   0   0% |   0   0% |   3  35% |   2  31% |   0   0% |   1   0% |   0   0% |   0   0% |   0   0% |   0   0% |
	// |   6 |   3   7% |   0   0% |   0   0% |   1   5% |   0   0% |   2   8% |   0   0% |   0   0% |   0   0% |   3  17% |   0   0% |   2   9% |   3  21% |   3  20% |   2   1% |   3   8% |   0   0% |   0   0% |   0   0% |   0   0% |
	// |   8 |   4   4% |   0   0% |   1   0% |   3   6% |   1   1% |   4   9% |   0   0% |   2   4% |   0   0% |   4  14% |   0   0% |   4   8% |   4  11% |   4  14% |   4   8% |   4  10% |   0   0% |   0   0% |   0   0% |   1   4% |
	// |  10 |   5   3% |   1   2% |   3   2% |   4   5% |   3   5% |   5   6% |   0   0% |   3   5% |   3   2% |   5  11% |   3   2% |   5   7% |   5  10% |   5  11% |   5   5% |   5   9% |   1   1% |   1   0% |   1   1% |   3   6% |
	// |  12 |   6   2% |   4   5% |   5   2% |   5   2% |   5   4% |   6   4% |   4   4% |   5   5% |   5   4% |   6   8% |   5   3% |   6   5% |   6   8% |   6   6% |   6   4% |   6   7% |   4   2% |   6   6% |   4   3% |   5   6% |
	// |  14 |   8   3% |   6   5% |   7   2% |   7   3% |   7   3% |   8   4% |   6   5% |   7   6% |   7   4% |   8   7% |   7   3% |   8   5% |   8   7% |   8   7% |   8   4% |   8   6% |   6   3% |   8   5% |   6   3% |   7   5% |
	// |  16 |  10   4% |   8   5% |   9   3% |   9   3% |   9   3% |  10   4% |   8   4% |   9   5% |   9   4% |  10   7% |   9   4% |  10   5% |  10   6% |  10   6% |  10   3% |  10   6% |   8   3% |  10   5% |   8   4% |   9   5% |
	// |  18 |  12   4% |  10   5% |  11   4% |  11   4% |  11   4% |  12   4% |  10   4% |  11   5% |  11   4% |  12   6% |  11   4% |  12   5% |  12   6% |  12   6% |  12   4% |  12   5% |  10   4% |  12   4% |  10   4% |  11   4% |
	// |  20 |  14   4% |  12   5% |  13   4% |  13   4% |  13   4% |  14   4% |  12   4% |  13   5% |  13   4% |  14   5% |  13   4% |  14   5% |  14   6% |  14   6% |  14   4% |  14   5% |  12   4% |  14   4% |  12   4% |  13   5% |
	// |  22 |  16   4% |  14   5% |  15   4% |  15   4% |  15   4% |  16   5% |  14   4% |  15   5% |  15   4% |  16   5% |  15   4% |  16   4% |  16   6% |  16   5% |  16   4% |  16   4% |  14   4% |  16   4% |  14   5% |  15   5% |
	// |  24 |  18   4% |  16   5% |  17   3% |  17   4% |  17   4% |  18   5% |  16   4% |  17   5% |  17   4% |  18   5% |  17   4% |  18   4% |  18   5% |  18   5% |  18   4% |  18   5% |  16   4% |  18   4% |  16   5% |  17   5% |
	// |  26 |  20   4% |  18   5% |  19   3% |  19   4% |  19   4% |  20   5% |  18   4% |  19   5% |  19   4% |  20   5% |  19   4% |  20   4% |  20   5% |  20   5% |  20   4% |  20   4% |  18   5% |  20   4% |  18   5% |  19   4% |
	// |  28 |  22   4% |  20   5% |  21   3% |  21   4% |  21   4% |  22   6% |  20   4% |  21   5% |  21   4% |  22   5% |  21   4% |  22   4% |  22   5% |  22   5% |  22   4% |  22   4% |  20   4% |  22   4% |  20   5% |  21   4% |
	// |  30 |  24   4% |  22   5% |  23   3% |  23   4% |  23   4% |  24   6% |  22   4% |  23   5% |  23   5% |  24   5% |  23   4% |  24   4% |  24   5% |  24   5% |  24   4% |  24   4% |  22   4% |  24   4% |  22   5% |  23   4% |
	// |  32 |  26   4% |  24   5% |  25   3% |  25   4% |  25   4% |  26   5% |  24   4% |  25   5% |  25   5% |  26   5% |  25   4% |  26   4% |  26   5% |  26   5% |  26   4% |  26   5% |  24   4% |  26   5% |  24   4% |  25   5% |
	// |  34 |  28   4% |  26   5% |  27   3% |  27   4% |  27   4% |  28   5% |  26   4% |  27   5% |  27   5% |  28   5% |  27   4% |  28   4% |  28   5% |  28   5% |  28   4% |  28   5% |  26   4% |  28   5% |  26   5% |  27   4% |
	// |  36 |  30   4% |  28   5% |  29   3% |  29   4% |  29   4% |  30   5% |  28   4% |  29   5% |  29   5% |  30   4% |  29   4% |  30   4% |  30   5% |  30   5% |  30   4% |  30   5% |  28   4% |  30   5% |  28   4% |  29   4% |
	// |  38 |  32   4% |  30   5% |  31   4% |  31   4% |  31   4% |  32   5% |  30   4% |  31   5% |  31   5% |  32   4% |  31   4% |  32   4% |  32   5% |  32   5% |  32   4% |  32   5% |  30   4% |  32   5% |  30   4% |  31   4% |
	// |  40 |  34   4% |  32   5% |  33   4% |  33   4% |  33   4% |  34   5% |  32   4% |  33   5% |  33   5% |  34   4% |  33   4% |  34   5% |  34   5% |  34   5% |  34   4% |  34   5% |  32   4% |  34   5% |  32   4% |  33   4% |
	// |  42 |  36   4% |  34   5% |  35   4% |  35   4% |  35   4% |  36   5% |  34   4% |  35   5% |  35   5% |  36   4% |  35   4% |  36   5% |  36   5% |  36   5% |  36   4% |  36   5% |  34   5% |  36   5% |  34   4% |  35   5% |
	// |  44 |  38   4% |  36   5% |  37   4% |  37   4% |  37   4% |  38   5% |  36   4% |  37   5% |  37   5% |  38   4% |  37   4% |  38   5% |  38   5% |  38   5% |  38   4% |  38   5% |  36   5% |  38   5% |  36   4% |  37   5% |
	// |  46 |  40   4% |  38   5% |  39   4% |  39   4% |  39   4% |  40   5% |  38   4% |  39   5% |  39   5% |  40   4% |  39   4% |  40   5% |  40   5% |  40   5% |  40   4% |  40   5% |  38   5% |  40   5% |  38   4% |  39   5% |
	// |  48 |  42   4% |  40   5% |  41   4% |  41   4% |  41   4% |  42   5% |  40   4% |  41   5% |  41   5% |  42   4% |  41   4% |  42   5% |  42   5% |  42   5% |  42   4% |  42   5% |  40   5% |  42   5% |  40   4% |  41   5% |
	// |  50 |  44   4% |  42   5% |  43   3% |  43   4% |  43   4% |  44   5% |  42   4% |  43   5% |  43   5% |  44   4% |  43   4% |  44   5% |  44   5% |  44   5% |  44   4% |  44   5% |  42   5% |  44   5% |  42   4% |  43   5% |
	// |  52 |  46   4% |  44   5% |  45   4% |  45   4% |  45   4% |  46   5% |  44   4% |  45   5% |  45   5% |  46   4% |  45   4% |  46   5% |  46   5% |  46   5% |  46   4% |  46   5% |  44   5% |  46   5% |  44   4% |  45   5% |
	// |  54 |  48   4% |  46   5% |  47   4% |  47   4% |  47   4% |  48   5% |  46   4% |  47   5% |  47   5% |  48   4% |  47   4% |  48   5% |  48   5% |  48   5% |  48   5% |  48   5% |  46   5% |  48   5% |  46   4% |  47   5% |
	// |  56 |  50   4% |  48   5% |  49   4% |  49   4% |  49   4% |  50   5% |  48   4% |  49   5% |  49   5% |  50   4% |  49   4% |  50   5% |  50   5% |  50   5% |  50   4% |  50   4% |  48   5% |  50   5% |  48   4% |  49   5% |
	// |  58 |  52   4% |  50   5% |  51   4% |  51   4% |  51   4% |  52   5% |  50   4% |  51   5% |  51   5% |  52   4% |  51   4% |  52   5% |  52   5% |  52   5% |  52   4% |  52   4% |  50   5% |  52   5% |  50   4% |  51   5% |
	// |  60 |  54   4% |  52   5% |  53   4% |  53   4% |  53   4% |  54   5% |  52   4% |  53   5% |  53   5% |  54   4% |  53   4% |  54   5% |  54   5% |  54   5% |  54   4% |  54   5% |  52   5% |  54   5% |  52   4% |  53   5% |
	// |  62 |  56   4% |  54   5% |  55   4% |  55   4% |  55   4% |  56   5% |  54   4% |  55   5% |  55   5% |  56   5% |  55   4% |  56   5% |  56   5% |  56   5% |  56   5% |  56   5% |  54   5% |  56   5% |  54   4% |  55   5% |
	// |  64 |  58   4% |  56   5% |  57   4% |  57   4% |  57   4% |  58   5% |  56   4% |  57   4% |  57   5% |  58   5% |  57   4% |  58   5% |  58   5% |  58   5% |  58   5% |  58   5% |  56   5% |  58   5% |  56   4% |  57   5% |
	// |  66 |  60   4% |  58   5% |  59   4% |  59   4% |  59   4% |  60   5% |  58   4% |  59   5% |  59   5% |  60   5% |  59   4% |  60   5% |  60   5% |  60   5% |  60   5% |  60   5% |  58   5% |  60   5% |  58   4% |  59   5% |
	// |  68 |  62   4% |  60   5% |  61   4% |  61   4% |  61   4% |  62   5% |  60   4% |  61   4% |  61   5% |  62   5% |  61   4% |  62   5% |  62   5% |  62   5% |  62   5% |  62   5% |  60   5% |  62   5% |  60   4% |  61   5% |
	// |  70 |  64   4% |  62   5% |  63   4% |  63   4% |  63   4% |  64   5% |  62   4% |  63   5% |  63   5% |  64   4% |  63   4% |  64   5% |  64   5% |  64   5% |  64   4% |  64   4% |  62   5% |  64   5% |  62   4% |  63   5% |
	// |  72 |  66   4% |  64   5% |  65   4% |  65   4% |  65   4% |  66   5% |  64   4% |  65   5% |  65   5% |  66   5% |  65   4% |  66   5% |  66   5% |  66   5% |  66   4% |  66   5% |  64   5% |  66   5% |  64   4% |  65   5% |
	// |  74 |  68   4% |  66   5% |  67   4% |  67   4% |  67   4% |  68   5% |  66   4% |  67   5% |  67   5% |  68   4% |  67   4% |  68   5% |  68   5% |  68   5% |  68   4% |  68   4% |  66   5% |  68   5% |  66   4% |  67   5% |
	// |  76 |  70   4% |  68   5% |  69   4% |  69   4% |  69   4% |  70   5% |  68   4% |  69   5% |  69   5% |  70   4% |  69   4% |  70   5% |  70   5% |  70   5% |  70   4% |  70   5% |  68   5% |  70   5% |  68   4% |  69   5% |
	// |  78 |  72   4% |  70   5% |  71   4% |  71   4% |  71   4% |  72   5% |  70   4% |  71   5% |  71   5% |  72   4% |  71   4% |  72   5% |  72   5% |  72   5% |  72   4% |  72   4% |  70   4% |  72   5% |  70   4% |  71   5% |
	// |  80 |  74   4% |  72   5% |  73   4% |  73   4% |  73   4% |  74   5% |  72   4% |  73   5% |  73   5% |  74   4% |  73   4% |  74   5% |  74   5% |  74   5% |  74   4% |  74   5% |  72   5% |  74   5% |  72   4% |  73   4% |
	// |  82 |  76   4% |  74   5% |  75   4% |  75   4% |  75   4% |  76   5% |  74   4% |  75   5% |  75   5% |  76   4% |  75   4% |  76   5% |  76   5% |  76   5% |  76   4% |  76   4% |  74   5% |  76   4% |  74   4% |  75   5% |
	// |  84 |  78   4% |  76   5% |  77   4% |  77   4% |  77   4% |  78   5% |  76   4% |  77   5% |  77   5% |  78   4% |  77   4% |  78   5% |  78   5% |  78   5% |  78   5% |  78   4% |  76   5% |  78   4% |  76   4% |  77   4% |
	// |  86 |  80   4% |  78   5% |  79   4% |  79   4% |  79   4% |  80   5% |  78   4% |  79   5% |  79   5% |  80   4% |  79   4% |  80   5% |  80   5% |  80   5% |  80   5% |  80   5% |  78   5% |  80   4% |  78   4% |  79   4% |
	// |  88 |  82   4% |  80   5% |  81   4% |  81   4% |  81   4% |  82   5% |  80   5% |  81   5% |  81   5% |  82   4% |  81   4% |  82   5% |  82   5% |  82   5% |  82   5% |  82   5% |  80   5% |  82   5% |  80   4% |  81   4% |
	// |  90 |  84   4% |  82   5% |  83   4% |  83   4% |  83   4% |  84   5% |  82   5% |  83   5% |  83   5% |  84   4% |  83   4% |  84   5% |  84   5% |  84   5% |  84   4% |  84   5% |  82   5% |  84   5% |  82   4% |  83   4% |
	// |  92 |  86   4% |  84   5% |  85   4% |  85   4% |  85   4% |  86   5% |  84   5% |  85   5% |  85   5% |  86   4% |  85   4% |  86   5% |  86   5% |  86   5% |  86   4% |  86   5% |  84   5% |  86   4% |  84   4% |  85   5% |
	// |  94 |  88   4% |  86   5% |  87   4% |  87   4% |  87   4% |  88   5% |  86   5% |  87   5% |  87   5% |  88   4% |  87   4% |  88   5% |  88   5% |  88   5% |  88   4% |  88   5% |  86   5% |  88   4% |  86   4% |  87   5% |
	// |  96 |  90   4% |  88   5% |  89   4% |  89   4% |  89   4% |  90   5% |  88   5% |  89   5% |  89   5% |  90   5% |  89   4% |  90   5% |  90   5% |  90   5% |  90   4% |  90   5% |  88   5% |  90   4% |  88   4% |  89   5% |
	// |  98 |  92   4% |  90   5% |  91   4% |  91   4% |  91   4% |  92   5% |  90   5% |  91   5% |  91   5% |  92   5% |  91   4% |  92   5% |  92   5% |  92   5% |  92   4% |  92   5% |  90   5% |  92   4% |  90   4% |  91   5% |
	// +-----+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+
	// Total bytes=969478392, ranges=1845
}

func qpsBasedRebalanceFn(
	ctx context.Context, candidate *testStore, testStores []testStore, alloc *Allocator,
) {
	avgQPS := candidate.Capacity.QueriesPerSecond / float64(candidate.Capacity.RangeCount)
	jitteredQPS := avgQPS * (1 + alloc.randGen.Float64())
	opts := &qpsScorerOptions{
		qpsPerReplica:         jitteredQPS,
		qpsRebalanceThreshold: 0.2,
	}
	var rangeUsageInfo RangeUsageInfo
	add, remove, details, ok := alloc.RebalanceVoter(
		ctx,
		emptySpanConfig(),
		nil,
		[]roachpb.ReplicaDescriptor{{NodeID: candidate.Node.NodeID, StoreID: candidate.StoreID}},
		nil,
		rangeUsageInfo,
		storeFilterThrottled,
		opts,
	)
	if ok {
		log.Infof(ctx, "rebalancing from %v to %v; details: %s", remove, add, details)
		candidate.rebalance(&testStores[int(add.StoreID)], alloc.randGen.Int63n(1<<20), jitteredQPS)
	}
}

func Example_qpsRebalancingSingleRegion() {
	generation := 0
	const printEvery = 2
	printFn := func(testStores []testStore, table *tablewriter.Table) {
		if generation%printEvery == 0 {
			row := make([]string, len(testStores)+1)
			row[0] = fmt.Sprintf("%d", generation)
			for j := 0; j < len(testStores); j++ {
				ts := testStores[j]
				row[j+1] = fmt.Sprintf("%d %0.2f", ts.Capacity.RangeCount, ts.Capacity.QueriesPerSecond)
			}
			table.Append(row)
		}
		generation++
	}

	testStores := make([]testStore, 10)
	exampleRebalancing(testStores, qpsBasedRebalanceFn, printFn)

	// Output:
	// +-----+------------+------------+------------+------------+------------+------------+------------+------------+------------+------------+
	// | gen |  store 0   |  store 1   |  store 2   |  store 3   |  store 4   |  store 5   |  store 6   |  store 7   |  store 8   |  store 9   |
	// +-----+------------+------------+------------+------------+------------+------------+------------+------------+------------+------------+
	// |   0 | 2 29943.92 |     0 0.00 |     0 0.00 |     0 0.00 |     0 0.00 |     0 0.00 |     0 0.00 |     0 0.00 |     0 0.00 |     0 0.00 |
	// |   2 | 3 17950.57 | 2 11993.35 |     0 0.00 |     0 0.00 |     0 0.00 |     0 0.00 |     0 0.00 |     0 0.00 |     0 0.00 |     0 0.00 |
	// |   4 |  3 7473.51 |  3 6220.99 |  2 7930.67 |  2 8318.76 |     0 0.00 |     0 0.00 |     0 0.00 |     0 0.00 |     0 0.00 |     0 0.00 |
	// |   6 |  3 3411.46 |  4 3478.11 |  3 4735.76 |  3 4775.10 |  2 2598.82 | 4 10944.68 |     0 0.00 |     0 0.00 |     0 0.00 |     0 0.00 |
	// |   8 |  5 3411.46 |  6 3478.11 |  4 3392.44 |  4 2972.12 |  4 2598.82 |  4 4107.39 |  3 4636.29 |  2 5347.30 |     0 0.00 |     0 0.00 |
	// |  10 |  7 3411.46 |  8 3478.11 |  6 3392.44 |  6 2972.12 |  6 2598.82 |  5 2743.76 |  4 2814.28 |  3 2757.89 |  3 3205.33 |  1 2569.71 |
	// |  12 |  9 3411.46 | 10 3478.11 |  8 3392.44 |  8 2972.12 |  8 2598.82 |  7 2743.76 |  6 2814.28 |  5 2757.89 |  5 3205.33 |  3 2569.71 |
	// |  14 | 11 3411.46 | 12 3478.11 | 10 3392.44 | 10 2972.12 | 10 2598.82 |  9 2743.76 |  8 2814.28 |  7 2757.89 |  7 3205.33 |  5 2569.71 |
	// |  16 | 13 3411.46 | 14 3478.11 | 12 3392.44 | 12 2972.12 | 12 2598.82 | 11 2743.76 | 10 2814.28 |  9 2757.89 |  9 3205.33 |  7 2569.71 |
	// |  18 | 15 3411.46 | 16 3478.11 | 14 3392.44 | 14 2972.12 | 14 2598.82 | 13 2743.76 | 12 2814.28 | 11 2757.89 | 11 3205.33 |  9 2569.71 |
	// |  20 | 17 3411.46 | 18 3478.11 | 16 3392.44 | 16 2972.12 | 16 2598.82 | 15 2743.76 | 14 2814.28 | 13 2757.89 | 13 3205.33 | 11 2569.71 |
	// |  22 | 19 3411.46 | 20 3478.11 | 18 3392.44 | 18 2972.12 | 18 2598.82 | 17 2743.76 | 16 2814.28 | 15 2757.89 | 15 3205.33 | 13 2569.71 |
	// |  24 | 21 3411.46 | 22 3478.11 | 20 3392.44 | 20 2972.12 | 20 2598.82 | 19 2743.76 | 18 2814.28 | 17 2757.89 | 17 3205.33 | 15 2569.71 |
	// |  26 | 23 3411.46 | 24 3478.11 | 22 3392.44 | 22 2972.12 | 22 2598.82 | 21 2743.76 | 20 2814.28 | 19 2757.89 | 19 3205.33 | 17 2569.71 |
	// |  28 | 25 3411.46 | 26 3478.11 | 24 3392.44 | 24 2972.12 | 24 2598.82 | 23 2743.76 | 22 2814.28 | 21 2757.89 | 21 3205.33 | 19 2569.71 |
	// |  30 | 27 3411.46 | 28 3478.11 | 26 3392.44 | 26 2972.12 | 26 2598.82 | 25 2743.76 | 24 2814.28 | 23 2757.89 | 23 3205.33 | 21 2569.71 |
	// |  32 | 29 3411.46 | 30 3478.11 | 28 3392.44 | 28 2972.12 | 28 2598.82 | 27 2743.76 | 26 2814.28 | 25 2757.89 | 25 3205.33 | 23 2569.71 |
	// |  34 | 31 3411.46 | 32 3478.11 | 30 3392.44 | 30 2972.12 | 30 2598.82 | 29 2743.76 | 28 2814.28 | 27 2757.89 | 27 3205.33 | 25 2569.71 |
	// |  36 | 33 3411.46 | 34 3478.11 | 32 3392.44 | 32 2972.12 | 32 2598.82 | 31 2743.76 | 30 2814.28 | 29 2757.89 | 29 3205.33 | 27 2569.71 |
	// |  38 | 35 3411.46 | 36 3478.11 | 34 3392.44 | 34 2972.12 | 34 2598.82 | 33 2743.76 | 32 2814.28 | 31 2757.89 | 31 3205.33 | 29 2569.71 |
	// |  40 | 37 3411.46 | 38 3478.11 | 36 3392.44 | 36 2972.12 | 36 2598.82 | 35 2743.76 | 34 2814.28 | 33 2757.89 | 33 3205.33 | 31 2569.71 |
	// |  42 | 39 3411.46 | 40 3478.11 | 38 3392.44 | 38 2972.12 | 38 2598.82 | 37 2743.76 | 36 2814.28 | 35 2757.89 | 35 3205.33 | 33 2569.71 |
	// |  44 | 41 3411.46 | 42 3478.11 | 40 3392.44 | 40 2972.12 | 40 2598.82 | 39 2743.76 | 38 2814.28 | 37 2757.89 | 37 3205.33 | 35 2569.71 |
	// |  46 | 43 3411.46 | 44 3478.11 | 42 3392.44 | 42 2972.12 | 42 2598.82 | 41 2743.76 | 40 2814.28 | 39 2757.89 | 39 3205.33 | 37 2569.71 |
	// |  48 | 45 3411.46 | 46 3478.11 | 44 3392.44 | 44 2972.12 | 44 2598.82 | 43 2743.76 | 42 2814.28 | 41 2757.89 | 41 3205.33 | 39 2569.71 |
	// |  50 | 47 3411.46 | 48 3478.11 | 46 3392.44 | 46 2972.12 | 46 2598.82 | 45 2743.76 | 44 2814.28 | 43 2757.89 | 43 3205.33 | 41 2569.71 |
	// |  52 | 49 3411.46 | 50 3478.11 | 48 3392.44 | 48 2972.12 | 48 2598.82 | 47 2743.76 | 46 2814.28 | 45 2757.89 | 45 3205.33 | 43 2569.71 |
	// |  54 | 51 3411.46 | 52 3478.11 | 50 3392.44 | 50 2972.12 | 50 2598.82 | 49 2743.76 | 48 2814.28 | 47 2757.89 | 47 3205.33 | 45 2569.71 |
	// |  56 | 53 3411.46 | 54 3478.11 | 52 3392.44 | 52 2972.12 | 52 2598.82 | 51 2743.76 | 50 2814.28 | 49 2757.89 | 49 3205.33 | 47 2569.71 |
	// |  58 | 55 3411.46 | 56 3478.11 | 54 3392.44 | 54 2972.12 | 54 2598.82 | 53 2743.76 | 52 2814.28 | 51 2757.89 | 51 3205.33 | 49 2569.71 |
	// |  60 | 57 3411.46 | 58 3478.11 | 56 3392.44 | 56 2972.12 | 56 2598.82 | 55 2743.76 | 54 2814.28 | 53 2757.89 | 53 3205.33 | 51 2569.71 |
	// |  62 | 59 3411.46 | 60 3478.11 | 58 3392.44 | 58 2972.12 | 58 2598.82 | 57 2743.76 | 56 2814.28 | 55 2757.89 | 55 3205.33 | 53 2569.71 |
	// |  64 | 61 3411.46 | 62 3478.11 | 60 3392.44 | 60 2972.12 | 60 2598.82 | 59 2743.76 | 58 2814.28 | 57 2757.89 | 57 3205.33 | 55 2569.71 |
	// |  66 | 63 3411.46 | 64 3478.11 | 62 3392.44 | 62 2972.12 | 62 2598.82 | 61 2743.76 | 60 2814.28 | 59 2757.89 | 59 3205.33 | 57 2569.71 |
	// |  68 | 65 3411.46 | 66 3478.11 | 64 3392.44 | 64 2972.12 | 64 2598.82 | 63 2743.76 | 62 2814.28 | 61 2757.89 | 61 3205.33 | 59 2569.71 |
	// |  70 | 67 3411.46 | 68 3478.11 | 66 3392.44 | 66 2972.12 | 66 2598.82 | 65 2743.76 | 64 2814.28 | 63 2757.89 | 63 3205.33 | 61 2569.71 |
	// |  72 | 69 3411.46 | 70 3478.11 | 68 3392.44 | 68 2972.12 | 68 2598.82 | 67 2743.76 | 66 2814.28 | 65 2757.89 | 65 3205.33 | 63 2569.71 |
	// |  74 | 71 3411.46 | 72 3478.11 | 70 3392.44 | 70 2972.12 | 70 2598.82 | 69 2743.76 | 68 2814.28 | 67 2757.89 | 67 3205.33 | 65 2569.71 |
	// |  76 | 73 3411.46 | 74 3478.11 | 72 3392.44 | 72 2972.12 | 72 2598.82 | 71 2743.76 | 70 2814.28 | 69 2757.89 | 69 3205.33 | 67 2569.71 |
	// |  78 | 75 3411.46 | 76 3478.11 | 74 3392.44 | 74 2972.12 | 74 2598.82 | 73 2743.76 | 72 2814.28 | 71 2757.89 | 71 3205.33 | 69 2569.71 |
	// |  80 | 77 3411.46 | 78 3478.11 | 76 3392.44 | 76 2972.12 | 76 2598.82 | 75 2743.76 | 74 2814.28 | 73 2757.89 | 73 3205.33 | 71 2569.71 |
	// |  82 | 79 3411.46 | 80 3478.11 | 78 3392.44 | 78 2972.12 | 78 2598.82 | 77 2743.76 | 76 2814.28 | 75 2757.89 | 75 3205.33 | 73 2569.71 |
	// |  84 | 81 3411.46 | 82 3478.11 | 80 3392.44 | 80 2972.12 | 80 2598.82 | 79 2743.76 | 78 2814.28 | 77 2757.89 | 77 3205.33 | 75 2569.71 |
	// |  86 | 83 3411.46 | 84 3478.11 | 82 3392.44 | 82 2972.12 | 82 2598.82 | 81 2743.76 | 80 2814.28 | 79 2757.89 | 79 3205.33 | 77 2569.71 |
	// |  88 | 85 3411.46 | 86 3478.11 | 84 3392.44 | 84 2972.12 | 84 2598.82 | 83 2743.76 | 82 2814.28 | 81 2757.89 | 81 3205.33 | 79 2569.71 |
	// |  90 | 87 3411.46 | 88 3478.11 | 86 3392.44 | 86 2972.12 | 86 2598.82 | 85 2743.76 | 84 2814.28 | 83 2757.89 | 83 3205.33 | 81 2569.71 |
	// |  92 | 89 3411.46 | 90 3478.11 | 88 3392.44 | 88 2972.12 | 88 2598.82 | 87 2743.76 | 86 2814.28 | 85 2757.89 | 85 3205.33 | 83 2569.71 |
	// |  94 | 91 3411.46 | 92 3478.11 | 90 3392.44 | 90 2972.12 | 90 2598.82 | 89 2743.76 | 88 2814.28 | 87 2757.89 | 87 3205.33 | 85 2569.71 |
	// |  96 | 93 3411.46 | 94 3478.11 | 92 3392.44 | 92 2972.12 | 92 2598.82 | 91 2743.76 | 90 2814.28 | 89 2757.89 | 89 3205.33 | 87 2569.71 |
	// |  98 | 95 3411.46 | 96 3478.11 | 94 3392.44 | 94 2972.12 | 94 2598.82 | 93 2743.76 | 92 2814.28 | 91 2757.89 | 91 3205.33 | 89 2569.71 |
	// +-----+------------+------------+------------+------------+------------+------------+------------+------------+------------+------------+
	// Total bytes=477376096, ranges=939
}

func Example_qpsRebalancingMultiRegion() {
	generation := 0
	const printEvery = 2
	printFn := func(testStores []testStore, table *tablewriter.Table) {
		if generation%printEvery == 0 {
			row := make([]string, len(testStores)+1)
			row[0] = fmt.Sprintf("%d", generation)
			for j := 0; j < len(testStores); j++ {
				ts := testStores[j]
				row[j+1] = fmt.Sprintf("%d %0.2f", ts.Capacity.RangeCount, ts.Capacity.QueriesPerSecond)
			}
			table.Append(row)
		}
		generation++
	}

	const numStores = 15
	testStores := make([]testStore, numStores)
	for i := 0; i < 8; i++ {
		testStores[i].Node.Locality = roachpb.Locality{
			Tiers: []roachpb.Tier{{Key: "region", Value: "A"}},
		}
	}
	for i := 8; i < 13; i++ {
		testStores[i].Node.Locality = roachpb.Locality{
			Tiers: []roachpb.Tier{{Key: "region", Value: "B"}},
		}
	}
	for i := 13; i < numStores; i++ {
		testStores[i].Node.Locality = roachpb.Locality{
			Tiers: []roachpb.Tier{{Key: "region", Value: "C"}},
		}
	}
	exampleRebalancing(testStores, qpsBasedRebalanceFn, printFn)

	// Output:
	// +-----+------------+------------+------------+------------+------------+------------+------------+------------+------------+------------+------------+------------+------------+------------+------------+
	// | gen |  store 0   |  store 1   |  store 2   |  store 3   |  store 4   |  store 5   |  store 6   |  store 7   |  store 8   |  store 9   |  store 10  |  store 11  |  store 12  |  store 13  |  store 14  |
	// +-----+------------+------------+------------+------------+------------+------------+------------+------------+------------+------------+------------+------------+------------+------------+------------+
	// |   0 | 2 29943.92 |     0 0.00 |     0 0.00 |     0 0.00 |     0 0.00 |     0 0.00 |     0 0.00 |     0 0.00 |     0 0.00 |     0 0.00 |     0 0.00 |     0 0.00 |     0 0.00 |     0 0.00 |     0 0.00 |
	// |   2 | 3 17950.57 | 2 11993.35 |     0 0.00 |     0 0.00 |     0 0.00 |     0 0.00 |     0 0.00 |     0 0.00 |     0 0.00 |     0 0.00 |     0 0.00 |     0 0.00 |     0 0.00 |     0 0.00 |     0 0.00 |
	// |   4 |  3 7473.51 |  3 6220.99 |  2 7930.67 |  2 8318.76 |     0 0.00 |     0 0.00 |     0 0.00 |     0 0.00 |     0 0.00 |     0 0.00 |     0 0.00 |     0 0.00 |     0 0.00 |     0 0.00 |     0 0.00 |
	// |   6 |  3 3411.46 |  3 1788.05 |  3 5620.53 |  3 5486.53 |  3 4928.13 |  4 8709.23 |     0 0.00 |     0 0.00 |     0 0.00 |     0 0.00 |     0 0.00 |     0 0.00 |     0 0.00 |     0 0.00 |     0 0.00 |
	// |   8 |  4 1954.26 |  5 1788.05 |  3 2121.87 |  3 2138.62 |  3 1269.93 |  4 3938.09 |  5 7465.02 |  5 9268.08 |     0 0.00 |     0 0.00 |     0 0.00 |     0 0.00 |     0 0.00 |     0 0.00 |     0 0.00 |
	// |  10 |  6 1954.26 |  7 1788.05 |  5 2121.87 |  5 2138.62 |  5 1269.93 |  4 1738.27 |  5 3800.32 |  5 5472.52 |  3 3550.29 |  4 6109.78 |     0 0.00 |     0 0.00 |     0 0.00 |     0 0.00 |     0 0.00 |
	// |  12 |  8 1954.26 |  9 1788.05 |  7 2121.87 |  7 2138.62 |  7 1269.93 |  6 1738.27 |  5 2038.40 |  5 3055.35 |  4 2027.79 |  4 2656.52 |  4 3698.73 |  4 5456.13 |     0 0.00 |     0 0.00 |     0 0.00 |
	// |  14 | 10 1954.26 | 11 1788.05 |  9 2121.87 |  9 2138.62 |  9 1269.93 |  8 1738.27 |  7 2038.40 |  6 2169.10 |  6 2027.79 |  5 1608.05 |  5 2380.65 |  4 3152.09 |  4 3451.23 |  2 2105.61 |     0 0.00 |
	// |  16 | 12 1954.26 | 13 1788.05 | 11 2121.87 | 11 2138.62 | 11 1269.93 | 10 1738.27 |  9 2038.40 |  8 2169.10 |  8 2027.79 |  7 1608.05 |  7 2380.65 |  5 2041.69 |  5 2161.29 |  4 2105.61 |  3 2400.34 |
	// |  18 | 14 1954.26 | 15 1788.05 | 13 2121.87 | 13 2138.62 | 14 1881.00 | 12 1738.27 | 11 2038.40 | 10 2169.10 | 10 2027.79 |  9 1608.05 |  9 2380.65 |  7 2041.69 |  7 2161.29 |  6 2105.61 |  4 1789.27 |
	// |  20 | 16 1954.26 | 17 1788.05 | 15 2121.87 | 15 2138.62 | 16 1881.00 | 14 1738.27 | 13 2038.40 | 12 2169.10 | 12 2027.79 | 11 1608.05 | 11 2380.65 |  9 2041.69 |  9 2161.29 |  8 2105.61 |  6 1789.27 |
	// |  22 | 18 1954.26 | 19 1788.05 | 17 2121.87 | 17 2138.62 | 18 1881.00 | 16 1738.27 | 15 2038.40 | 14 2169.10 | 14 2027.79 | 13 1608.05 | 13 2380.65 | 11 2041.69 | 11 2161.29 | 10 2105.61 |  8 1789.27 |
	// |  24 | 20 1954.26 | 21 1788.05 | 19 2121.87 | 19 2138.62 | 20 1881.00 | 18 1738.27 | 17 2038.40 | 16 2169.10 | 16 2027.79 | 15 1608.05 | 15 2380.65 | 13 2041.69 | 13 2161.29 | 12 2105.61 | 10 1789.27 |
	// |  26 | 22 1954.26 | 23 1788.05 | 21 2121.87 | 21 2138.62 | 22 1881.00 | 20 1738.27 | 19 2038.40 | 18 2169.10 | 18 2027.79 | 17 1608.05 | 17 2380.65 | 15 2041.69 | 15 2161.29 | 14 2105.61 | 12 1789.27 |
	// |  28 | 24 1954.26 | 25 1788.05 | 23 2121.87 | 23 2138.62 | 24 1881.00 | 22 1738.27 | 21 2038.40 | 20 2169.10 | 20 2027.79 | 19 1608.05 | 19 2380.65 | 17 2041.69 | 17 2161.29 | 16 2105.61 | 14 1789.27 |
	// |  30 | 26 1954.26 | 27 1788.05 | 25 2121.87 | 25 2138.62 | 26 1881.00 | 24 1738.27 | 23 2038.40 | 22 2169.10 | 22 2027.79 | 21 1608.05 | 21 2380.65 | 19 2041.69 | 19 2161.29 | 18 2105.61 | 16 1789.27 |
	// |  32 | 28 1954.26 | 29 1788.05 | 27 2121.87 | 27 2138.62 | 28 1881.00 | 26 1738.27 | 25 2038.40 | 24 2169.10 | 24 2027.79 | 23 1608.05 | 23 2380.65 | 21 2041.69 | 21 2161.29 | 20 2105.61 | 18 1789.27 |
	// |  34 | 30 1954.26 | 31 1788.05 | 29 2121.87 | 29 2138.62 | 30 1881.00 | 28 1738.27 | 27 2038.40 | 26 2169.10 | 26 2027.79 | 25 1608.05 | 25 2380.65 | 23 2041.69 | 23 2161.29 | 22 2105.61 | 20 1789.27 |
	// |  36 | 32 1954.26 | 33 1788.05 | 31 2121.87 | 31 2138.62 | 32 1881.00 | 30 1738.27 | 29 2038.40 | 28 2169.10 | 28 2027.79 | 27 1608.05 | 27 2380.65 | 25 2041.69 | 25 2161.29 | 24 2105.61 | 22 1789.27 |
	// |  38 | 34 1954.26 | 35 1788.05 | 33 2121.87 | 33 2138.62 | 34 1881.00 | 32 1738.27 | 31 2038.40 | 30 2169.10 | 30 2027.79 | 29 1608.05 | 29 2380.65 | 27 2041.69 | 27 2161.29 | 26 2105.61 | 24 1789.27 |
	// |  40 | 36 1954.26 | 37 1788.05 | 35 2121.87 | 35 2138.62 | 36 1881.00 | 34 1738.27 | 33 2038.40 | 32 2169.10 | 32 2027.79 | 31 1608.05 | 31 2380.65 | 29 2041.69 | 29 2161.29 | 28 2105.61 | 26 1789.27 |
	// |  42 | 38 1954.26 | 39 1788.05 | 37 2121.87 | 37 2138.62 | 38 1881.00 | 36 1738.27 | 35 2038.40 | 34 2169.10 | 34 2027.79 | 33 1608.05 | 33 2380.65 | 31 2041.69 | 31 2161.29 | 30 2105.61 | 28 1789.27 |
	// |  44 | 40 1954.26 | 41 1788.05 | 39 2121.87 | 39 2138.62 | 40 1881.00 | 38 1738.27 | 37 2038.40 | 36 2169.10 | 36 2027.79 | 35 1608.05 | 35 2380.65 | 33 2041.69 | 33 2161.29 | 32 2105.61 | 30 1789.27 |
	// |  46 | 42 1954.26 | 43 1788.05 | 41 2121.87 | 41 2138.62 | 42 1881.00 | 40 1738.27 | 39 2038.40 | 38 2169.10 | 38 2027.79 | 37 1608.05 | 37 2380.65 | 35 2041.69 | 35 2161.29 | 34 2105.61 | 32 1789.27 |
	// |  48 | 44 1954.26 | 45 1788.05 | 43 2121.87 | 43 2138.62 | 44 1881.00 | 42 1738.27 | 41 2038.40 | 40 2169.10 | 40 2027.79 | 39 1608.05 | 39 2380.65 | 37 2041.69 | 37 2161.29 | 36 2105.61 | 34 1789.27 |
	// |  50 | 46 1954.26 | 47 1788.05 | 45 2121.87 | 45 2138.62 | 46 1881.00 | 44 1738.27 | 43 2038.40 | 42 2169.10 | 42 2027.79 | 41 1608.05 | 41 2380.65 | 39 2041.69 | 39 2161.29 | 38 2105.61 | 36 1789.27 |
	// |  52 | 48 1954.26 | 49 1788.05 | 47 2121.87 | 47 2138.62 | 48 1881.00 | 46 1738.27 | 45 2038.40 | 44 2169.10 | 44 2027.79 | 43 1608.05 | 43 2380.65 | 41 2041.69 | 41 2161.29 | 40 2105.61 | 38 1789.27 |
	// |  54 | 50 1954.26 | 51 1788.05 | 49 2121.87 | 49 2138.62 | 50 1881.00 | 48 1738.27 | 47 2038.40 | 46 2169.10 | 46 2027.79 | 45 1608.05 | 45 2380.65 | 43 2041.69 | 43 2161.29 | 42 2105.61 | 40 1789.27 |
	// |  56 | 52 1954.26 | 53 1788.05 | 51 2121.87 | 51 2138.62 | 52 1881.00 | 50 1738.27 | 49 2038.40 | 48 2169.10 | 48 2027.79 | 47 1608.05 | 47 2380.65 | 45 2041.69 | 45 2161.29 | 44 2105.61 | 42 1789.27 |
	// |  58 | 54 1954.26 | 55 1788.05 | 53 2121.87 | 53 2138.62 | 54 1881.00 | 52 1738.27 | 51 2038.40 | 50 2169.10 | 50 2027.79 | 49 1608.05 | 49 2380.65 | 47 2041.69 | 47 2161.29 | 46 2105.61 | 44 1789.27 |
	// |  60 | 56 1954.26 | 57 1788.05 | 55 2121.87 | 55 2138.62 | 56 1881.00 | 54 1738.27 | 53 2038.40 | 52 2169.10 | 52 2027.79 | 51 1608.05 | 51 2380.65 | 49 2041.69 | 49 2161.29 | 48 2105.61 | 46 1789.27 |
	// |  62 | 58 1954.26 | 59 1788.05 | 57 2121.87 | 57 2138.62 | 58 1881.00 | 56 1738.27 | 55 2038.40 | 54 2169.10 | 54 2027.79 | 53 1608.05 | 53 2380.65 | 51 2041.69 | 51 2161.29 | 50 2105.61 | 48 1789.27 |
	// |  64 | 60 1954.26 | 61 1788.05 | 59 2121.87 | 59 2138.62 | 60 1881.00 | 58 1738.27 | 57 2038.40 | 56 2169.10 | 56 2027.79 | 55 1608.05 | 55 2380.65 | 53 2041.69 | 53 2161.29 | 52 2105.61 | 50 1789.27 |
	// |  66 | 62 1954.26 | 63 1788.05 | 61 2121.87 | 61 2138.62 | 62 1881.00 | 60 1738.27 | 59 2038.40 | 58 2169.10 | 58 2027.79 | 57 1608.05 | 57 2380.65 | 55 2041.69 | 55 2161.29 | 54 2105.61 | 52 1789.27 |
	// |  68 | 64 1954.26 | 65 1788.05 | 63 2121.87 | 63 2138.62 | 64 1881.00 | 62 1738.27 | 61 2038.40 | 60 2169.10 | 60 2027.79 | 59 1608.05 | 59 2380.65 | 57 2041.69 | 57 2161.29 | 56 2105.61 | 54 1789.27 |
	// |  70 | 66 1954.26 | 67 1788.05 | 65 2121.87 | 65 2138.62 | 66 1881.00 | 64 1738.27 | 63 2038.40 | 62 2169.10 | 62 2027.79 | 61 1608.05 | 61 2380.65 | 59 2041.69 | 59 2161.29 | 58 2105.61 | 56 1789.27 |
	// |  72 | 68 1954.26 | 69 1788.05 | 67 2121.87 | 67 2138.62 | 68 1881.00 | 66 1738.27 | 65 2038.40 | 64 2169.10 | 64 2027.79 | 63 1608.05 | 63 2380.65 | 61 2041.69 | 61 2161.29 | 60 2105.61 | 58 1789.27 |
	// |  74 | 70 1954.26 | 71 1788.05 | 69 2121.87 | 69 2138.62 | 70 1881.00 | 68 1738.27 | 67 2038.40 | 66 2169.10 | 66 2027.79 | 65 1608.05 | 65 2380.65 | 63 2041.69 | 63 2161.29 | 62 2105.61 | 60 1789.27 |
	// |  76 | 72 1954.26 | 73 1788.05 | 71 2121.87 | 71 2138.62 | 72 1881.00 | 70 1738.27 | 69 2038.40 | 68 2169.10 | 68 2027.79 | 67 1608.05 | 67 2380.65 | 65 2041.69 | 65 2161.29 | 64 2105.61 | 62 1789.27 |
	// |  78 | 74 1954.26 | 75 1788.05 | 73 2121.87 | 73 2138.62 | 74 1881.00 | 72 1738.27 | 71 2038.40 | 70 2169.10 | 70 2027.79 | 69 1608.05 | 69 2380.65 | 67 2041.69 | 67 2161.29 | 66 2105.61 | 64 1789.27 |
	// |  80 | 76 1954.26 | 77 1788.05 | 75 2121.87 | 75 2138.62 | 76 1881.00 | 74 1738.27 | 73 2038.40 | 72 2169.10 | 72 2027.79 | 71 1608.05 | 71 2380.65 | 69 2041.69 | 69 2161.29 | 68 2105.61 | 66 1789.27 |
	// |  82 | 78 1954.26 | 79 1788.05 | 77 2121.87 | 77 2138.62 | 78 1881.00 | 76 1738.27 | 75 2038.40 | 74 2169.10 | 74 2027.79 | 73 1608.05 | 73 2380.65 | 71 2041.69 | 71 2161.29 | 70 2105.61 | 68 1789.27 |
	// |  84 | 80 1954.26 | 81 1788.05 | 79 2121.87 | 79 2138.62 | 80 1881.00 | 78 1738.27 | 77 2038.40 | 76 2169.10 | 76 2027.79 | 75 1608.05 | 75 2380.65 | 73 2041.69 | 73 2161.29 | 72 2105.61 | 70 1789.27 |
	// |  86 | 82 1954.26 | 83 1788.05 | 81 2121.87 | 81 2138.62 | 82 1881.00 | 80 1738.27 | 79 2038.40 | 78 2169.10 | 78 2027.79 | 77 1608.05 | 77 2380.65 | 75 2041.69 | 75 2161.29 | 74 2105.61 | 72 1789.27 |
	// |  88 | 84 1954.26 | 85 1788.05 | 83 2121.87 | 83 2138.62 | 84 1881.00 | 82 1738.27 | 81 2038.40 | 80 2169.10 | 80 2027.79 | 79 1608.05 | 79 2380.65 | 77 2041.69 | 77 2161.29 | 76 2105.61 | 74 1789.27 |
	// |  90 | 86 1954.26 | 87 1788.05 | 85 2121.87 | 85 2138.62 | 86 1881.00 | 84 1738.27 | 83 2038.40 | 82 2169.10 | 82 2027.79 | 81 1608.05 | 81 2380.65 | 79 2041.69 | 79 2161.29 | 78 2105.61 | 76 1789.27 |
	// |  92 | 88 1954.26 | 89 1788.05 | 87 2121.87 | 87 2138.62 | 88 1881.00 | 86 1738.27 | 85 2038.40 | 84 2169.10 | 84 2027.79 | 83 1608.05 | 83 2380.65 | 81 2041.69 | 81 2161.29 | 80 2105.61 | 78 1789.27 |
	// |  94 | 90 1954.26 | 91 1788.05 | 89 2121.87 | 89 2138.62 | 90 1881.00 | 88 1738.27 | 87 2038.40 | 86 2169.10 | 86 2027.79 | 85 1608.05 | 85 2380.65 | 83 2041.69 | 83 2161.29 | 82 2105.61 | 80 1789.27 |
	// |  96 | 92 1954.26 | 93 1788.05 | 91 2121.87 | 91 2138.62 | 92 1881.00 | 90 1738.27 | 89 2038.40 | 88 2169.10 | 88 2027.79 | 87 1608.05 | 87 2380.65 | 85 2041.69 | 85 2161.29 | 84 2105.61 | 82 1789.27 |
	// |  98 | 94 1954.26 | 95 1788.05 | 93 2121.87 | 93 2138.62 | 94 1881.00 | 92 1738.27 | 91 2038.40 | 90 2169.10 | 90 2027.79 | 89 1608.05 | 89 2380.65 | 87 2041.69 | 87 2161.29 | 86 2105.61 | 84 1789.27 |
	// +-----+------------+------------+------------+------------+------------+------------+------------+------------+------------+------------+------------+------------+------------+------------+------------+
	// Total bytes=709837749, ranges=1369
}

func exampleRebalancing(
	testStores []testStore,
	rebalanceFn func(context.Context, *testStore, []testStore, *Allocator),
	printFn func([]testStore, *tablewriter.Table),
) {
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	ambientCtx := log.MakeTestingAmbientContext(stopper.Tracer())
	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)

	// Model a set of stores in a cluster,
	// adding / rebalancing ranges of random sizes.
	rpcContext := rpc.NewContext(ctx, rpc.ContextOptions{
		TenantID: roachpb.SystemTenantID,
		Config:   &base.Config{Insecure: true},
		Clock:    clock,
		Stopper:  stopper,
		Settings: st,
	})
	server := rpc.NewServer(rpcContext) // never started
	g := gossip.NewTest(1, rpcContext, server, stopper, metric.NewRegistry(), zonepb.DefaultZoneConfigRef())

	TimeUntilStoreDead.Override(ctx, &st.SV, TestTimeUntilStoreDeadOff)

	const nodes = 20

	// Deterministic must be set as this test is comparing the exact output
	// after each rebalance.
	sp := NewStorePool(
		ambientCtx,
		st,
		g,
		clock,
		func() int {
			return nodes
		},
		newMockNodeLiveness(livenesspb.NodeLivenessStatus_LIVE).nodeLivenessFunc,
		/* deterministic */ true,
	)
	alloc := MakeAllocator(
		sp, func(string) (time.Duration, bool) {
			return 0, false
		}, nil, /* knobs */
	)

	var wg sync.WaitGroup
	g.RegisterCallback(gossip.MakePrefixPattern(gossip.KeyStorePrefix),
		func(_ string, _ roachpb.Value) { wg.Done() },
		// Redundant callbacks are required by this test.
		gossip.Redundant)

	// Initialize testStores.
	initTestStores(
		testStores,
		alloc.randGen.Int63n(1<<20), /* firstRangeSize */
		alloc.randGen.Float64()*1e5, /* firstRangeQPS */
	)

	table := tablewriter.NewWriter(os.Stdout)
	table.SetAutoFormatHeaders(false)
	table.SetAlignment(tablewriter.ALIGN_RIGHT)

	header := make([]string, len(testStores)+1)
	header[0] = "gen"
	for i := 0; i < len(testStores); i++ {
		header[i+1] = fmt.Sprintf("store %d", i)
	}
	table.SetHeader(header)

	const generations = 100
	for i := 0; i < generations; i++ {
		// First loop through test stores and add data.
		wg.Add(len(testStores))
		for j := 0; j < len(testStores); j++ {
			// Add a pretend range to the testStore if there's already one.
			if testStores[j].Capacity.RangeCount > 0 {
				testStores[j].add(alloc.randGen.Int63n(1<<20), 0)
			}
			if err := g.AddInfoProto(
				gossip.MakeStoreKey(roachpb.StoreID(j)),
				&testStores[j].StoreDescriptor,
				0,
			); err != nil {
				panic(err)
			}
		}
		wg.Wait()

		// Next loop through test stores and maybe rebalance.
		for j := 0; j < len(testStores); j++ {
			ts := &testStores[j]
			if ts.Capacity.RangeCount == 0 {
				continue
			}
			rebalanceFn(ctx, ts, testStores, &alloc)
		}

		printFn(testStores, table)
	}

	var totBytes int64
	var totRanges int32
	for i := 0; i < len(testStores); i++ {
		totBytes += testStores[i].Capacity.Capacity - testStores[i].Capacity.Available
		totRanges += testStores[i].Capacity.RangeCount
	}
	table.Render()
	fmt.Printf("Total bytes=%d, ranges=%d\n", totBytes, totRanges)
}

func initTestStores(testStores []testStore, firstRangeSize int64, firstStoreQPS float64) {
	for i := 0; i < len(testStores); i++ {
		testStores[i].immediateCompaction = true
		testStores[i].StoreID = roachpb.StoreID(i)
		testStores[i].Node = roachpb.NodeDescriptor{NodeID: roachpb.NodeID(i)}
		testStores[i].Capacity = roachpb.StoreCapacity{
			Capacity:  1 << 30,
			Available: 1 << 30,
		}
	}

	// Initialize the cluster with a single range.
	testStores[0].add(firstRangeSize, firstStoreQPS)
}
