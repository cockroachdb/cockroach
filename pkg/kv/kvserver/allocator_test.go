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
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/proto"
	"github.com/olekukonko/tablewriter"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/tracker"
)

const firstRangeID = roachpb.RangeID(1)

var simpleZoneConfig = zonepb.ZoneConfig{
	NumReplicas: proto.Int32(1),
	Constraints: []zonepb.ConstraintsConjunction{
		{
			Constraints: []zonepb.Constraint{
				{Value: "a", Type: zonepb.Constraint_REQUIRED},
				{Value: "ssd", Type: zonepb.Constraint_REQUIRED},
			},
		},
	},
}

var multiDCConfigSSD = zonepb.ZoneConfig{
	NumReplicas: proto.Int32(2),
	Constraints: []zonepb.ConstraintsConjunction{
		{Constraints: []zonepb.Constraint{{Value: "ssd", Type: zonepb.Constraint_REQUIRED}}},
	},
}

var multiDCConfigConstrainToA = zonepb.ZoneConfig{
	NumReplicas: proto.Int32(2),
	Constraints: []zonepb.ConstraintsConjunction{
		{Constraints: []zonepb.Constraint{{Value: "a", Type: zonepb.Constraint_REQUIRED}}},
	},
}

var multiDCConfigUnsatisfiableVoterConstraints = zonepb.ZoneConfig{
	NumReplicas: proto.Int32(2),
	VoterConstraints: []zonepb.ConstraintsConjunction{
		{Constraints: []zonepb.Constraint{{Value: "doesNotExist", Type: zonepb.Constraint_REQUIRED}}},
	},
}

// multiDCConfigVoterAndNonVoter prescribes that one voting replica be placed in
// DC "b" and one non-voting replica be placed in DC "a".
var multiDCConfigVoterAndNonVoter = zonepb.ZoneConfig{
	NumReplicas: proto.Int32(2),
	Constraints: []zonepb.ConstraintsConjunction{
		// Constrain the non-voter to "a".
		{Constraints: []zonepb.Constraint{{Value: "a", Type: zonepb.Constraint_REQUIRED}}, NumReplicas: 1},
	},
	VoterConstraints: []zonepb.ConstraintsConjunction{
		// Constrain the voter to "b".
		{Constraints: []zonepb.Constraint{{Value: "b", Type: zonepb.Constraint_REQUIRED}}},
	},
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
	numNodes int, deterministic bool,
) (*stop.Stopper, *gossip.Gossip, *StorePool, Allocator, *hlc.ManualClock) {
	stopper, g, manual, storePool, _ := createTestStorePool(
		TestTimeUntilStoreDeadOff, deterministic,
		func() int { return numNodes },
		livenesspb.NodeLivenessStatus_LIVE)
	a := MakeAllocator(storePool, func(string) (time.Duration, bool) {
		return 0, true
	})
	return stopper, g, storePool, a, manual
}

// checkReplExists checks whether the given `repl` exists on any of the
// `stores`.
func checkReplExists(repl roachpb.ReplicaDescriptor, stores []roachpb.StoreID) (found bool) {
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

	stopper, g, _, a, _ := createTestAllocator(1, false /* deterministic */)
	defer stopper.Stop(context.Background())
	gossiputil.NewStoreGossiper(g).GossipStores(singleStore, t)
	result, _, err := a.AllocateVoter(
		context.Background(),
		&simpleZoneConfig,
		nil /* existingVoters */, nil, /* existingNonVoters */
	)
	if err != nil {
		t.Fatalf("Unable to perform allocation: %+v", err)
	}
	if result.Node.NodeID != 1 || result.StoreID != 1 {
		t.Errorf("expected NodeID 1 and StoreID 1: %+v", result)
	}
}

func TestAllocatorNoAvailableDisks(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	stopper, _, _, a, _ := createTestAllocator(1, false /* deterministic */)
	defer stopper.Stop(context.Background())
	result, _, err := a.AllocateVoter(
		context.Background(),
		&simpleZoneConfig,
		nil /* existingVoters */, nil, /* existingNonVoters */
	)
	if result != nil {
		t.Errorf("expected nil result: %+v", result)
	}
	if err == nil {
		t.Errorf("allocation succeeded despite there being no available disks: %v", result)
	}
}

func TestAllocatorTwoDatacenters(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	stopper, g, _, a, _ := createTestAllocator(1, false /* deterministic */)
	defer stopper.Stop(context.Background())
	gossiputil.NewStoreGossiper(g).GossipStores(multiDCStores, t)
	ctx := context.Background()
	result1, _, err := a.AllocateVoter(
		ctx,
		&multiDCConfigSSD,
		nil /* existingVoters */, nil, /* existingNonVoters */
	)
	if err != nil {
		t.Fatalf("Unable to perform allocation: %+v", err)
	}
	result2, _, err := a.AllocateVoter(
		ctx,
		&multiDCConfigSSD,
		[]roachpb.ReplicaDescriptor{{
			NodeID:  result1.Node.NodeID,
			StoreID: result1.StoreID,
		}}, nil, /* existingNonVoters */
	)
	if err != nil {
		t.Fatalf("Unable to perform allocation: %+v", err)
	}
	ids := []int{int(result1.Node.NodeID), int(result2.Node.NodeID)}
	sort.Ints(ids)
	if expected := []int{1, 2}; !reflect.DeepEqual(ids, expected) {
		t.Errorf("Expected nodes %+v: %+v vs %+v", expected, result1.Node, result2.Node)
	}
	// Verify that no result is forthcoming if we already have a replica.
	result3, _, err := a.AllocateVoter(
		ctx,
		&multiDCConfigSSD,
		[]roachpb.ReplicaDescriptor{
			{
				NodeID:  result1.Node.NodeID,
				StoreID: result1.StoreID,
			},
			{
				NodeID:  result2.Node.NodeID,
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

	stopper, g, _, a, _ := createTestAllocator(1, false /* deterministic */)
	defer stopper.Stop(context.Background())
	gossiputil.NewStoreGossiper(g).GossipStores(sameDCStores, t)
	result, _, err := a.AllocateVoter(
		context.Background(),
		&zonepb.ZoneConfig{
			NumReplicas: proto.Int32(0),
			Constraints: []zonepb.ConstraintsConjunction{
				{
					Constraints: []zonepb.Constraint{
						{Value: "a", Type: zonepb.Constraint_REQUIRED},
						{Value: "hdd", Type: zonepb.Constraint_REQUIRED},
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

	stopper, g, _, a, _ := createTestAllocator(10, false /* deterministic */)
	defer stopper.Stop(context.Background())
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
				context.Background(), zonepb.EmptyCompleteZoneConfig(), tc.existing, nil,
			)
			if e, a := tc.expectTargetAllocate, result != nil; e != a {
				t.Errorf(
					"AllocateVoter(%v) got target %v, err %v; expectTarget=%v",
					tc.existing, result, err, tc.expectTargetAllocate,
				)
			}
		}

		{
			var rangeUsageInfo RangeUsageInfo
			target, _, details, ok := a.RebalanceVoter(
				context.Background(),
				zonepb.EmptyCompleteZoneConfig(),
				nil,
				tc.existing,
				nil,
				rangeUsageInfo,
				storeFilterThrottled,
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

	stopper, g, _, a, _ := createTestAllocator(10, false /* deterministic */)
	defer stopper.Stop(context.Background())
	storeGossiper := gossiputil.NewStoreGossiper(g)
	storeGossiper.GossipStores(stores, t)

	// We run through all the ranges once to get the cluster to balance.
	// After that we should not be seeing replicas move.
	var rangeUsageInfo RangeUsageInfo
	for i := 1; i < 40; i++ {
		add, remove, _, ok := a.RebalanceVoter(context.Background(), zonepb.EmptyCompleteZoneConfig(), nil, ranges[i].InternalReplicas, nil, rangeUsageInfo, storeFilterThrottled)
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
		_, _, _, ok := a.RebalanceVoter(context.Background(), zonepb.EmptyCompleteZoneConfig(), nil, ranges[i].InternalReplicas, nil, rangeUsageInfo, storeFilterThrottled)
		require.False(t, ok)
	}
}

// TestAllocatorRebalance verifies that rebalance targets are chosen
// randomly from amongst stores under the maxFractionUsedThreshold.
func TestAllocatorRebalance(t *testing.T) {
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

	stopper, g, _, a, _ := createTestAllocator(10, false /* deterministic */)
	defer stopper.Stop(context.Background())

	gossiputil.NewStoreGossiper(g).GossipStores(stores, t)
	ctx := context.Background()

	// Every rebalance target must be either store 1 or 2.
	for i := 0; i < 10; i++ {
		var rangeUsageInfo RangeUsageInfo
		target, _, _, ok := a.RebalanceVoter(ctx, zonepb.EmptyCompleteZoneConfig(), nil, []roachpb.ReplicaDescriptor{{NodeID: 3, StoreID: 3}}, nil, rangeUsageInfo, storeFilterThrottled)
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

	// Verify shouldRebalanceBasedOnRangeCount results.
	for i, store := range stores {
		desc, ok := a.storePool.getStoreDescriptor(store.StoreID)
		if !ok {
			t.Fatalf("%d: unable to get store %d descriptor", i, store.StoreID)
		}
		sl, _, _ := a.storePool.getStoreList(storeFilterThrottled)
		result := shouldRebalanceBasedOnRangeCount(ctx, desc, sl, a.scorerOptions())
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
	stopper, g, _, a, _ := createTestAllocator(5, false /* deterministic */)
	defer stopper.Stop(context.Background())
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
		result, _, details, ok := a.RebalanceVoter(context.Background(), zonepb.EmptyCompleteZoneConfig(), status, replicas, nil, rangeUsageInfo, storeFilterThrottled)
		if ok {
			t.Fatalf("expected no rebalance, but got target s%d; details: %s", result.StoreID, details)
		}
	}

	// Set up a second round of testing where the other two stores in the big
	// locality actually have fewer replicas, but enough that it still isn't
	// worth rebalancing to them.
	stores[1].Capacity.RangeCount = 46
	stores[2].Capacity.RangeCount = 46
	sg.GossipStores(stores, t)
	for i := 0; i < 10; i++ {
		target, _, details, ok := a.RebalanceVoter(context.Background(), zonepb.EmptyCompleteZoneConfig(), status, replicas, nil, rangeUsageInfo, storeFilterThrottled)
		if ok {
			t.Fatalf("expected no rebalance, but got target s%d; details: %s", target.StoreID, details)
		}
	}

	// Make sure rebalancing does happen if we drop just a little further down.
	stores[1].Capacity.RangeCount = 44
	sg.GossipStores(stores, t)
	for i := 0; i < 10; i++ {
		target, origin, details, ok := a.RebalanceVoter(context.Background(), zonepb.EmptyCompleteZoneConfig(), status, replicas, nil, rangeUsageInfo, storeFilterThrottled)
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

	stopper, _, sp, a, _ := createTestAllocator(8, false /* deterministic */)
	ctx := context.Background()
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
			target, _, _, ok := a.RebalanceVoter(ctx, zonepb.EmptyCompleteZoneConfig(), nil, c.existing, nil, rangeUsageInfo, storeFilterThrottled)
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
			stopper, g, _, a, _ := createTestAllocator(1, true /* deterministic */)
			defer stopper.Stop(context.Background())

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

			// Verify shouldRebalanceBasedOnRangeCount returns the expected value.
			for j, store := range stores {
				desc, ok := a.storePool.getStoreDescriptor(store.StoreID)
				if !ok {
					t.Fatalf("[store %d]: unable to get store %d descriptor", j, store.StoreID)
				}
				if a, e := shouldRebalanceBasedOnRangeCount(context.Background(), desc, sl, a.scorerOptions()), cluster[j].shouldRebalanceFrom; a != e {
					t.Errorf("[store %d]: shouldRebalanceBasedOnRangeCount %t != expected %t", store.StoreID, a, e)
				}
			}
		})
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

	stopper, g, _, a, _ := createTestAllocator(10, false /* deterministic */)
	defer stopper.Stop(context.Background())

	gossiputil.NewStoreGossiper(g).GossipStores(stores, t)
	ctx := context.Background()

	// Every rebalance target must be store 4 (or nil for case of missing the only option).
	for i := 0; i < 10; i++ {
		var rangeUsageInfo RangeUsageInfo
		result, _, _, ok := a.RebalanceVoter(ctx, zonepb.EmptyCompleteZoneConfig(), nil, []roachpb.ReplicaDescriptor{{StoreID: stores[0].StoreID}}, nil, rangeUsageInfo, storeFilterThrottled)
		if ok && result.StoreID != 4 {
			t.Errorf("expected store 4; got %d", result.StoreID)
		}
	}

	// Verify shouldRebalanceBasedOnRangeCount results.
	for i, store := range stores {
		desc, ok := a.storePool.getStoreDescriptor(store.StoreID)
		if !ok {
			t.Fatalf("%d: unable to get store %d descriptor", i, store.StoreID)
		}
		sl, _, _ := a.storePool.getStoreList(storeFilterThrottled)
		result := shouldRebalanceBasedOnRangeCount(ctx, desc, sl, a.scorerOptions())
		if expResult := (i < 3); expResult != result {
			t.Errorf("%d: expected rebalance %t; got %t", i, expResult, result)
		}
	}
}

func TestAllocatorTransferLeaseTarget(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	stopper, g, _, a, _ := createTestAllocator(10, true /* deterministic */)
	defer stopper.Stop(context.Background())

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
		{StoreID: 1},
		{StoreID: 2},
		{StoreID: 3},
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
				context.Background(),
				zonepb.EmptyCompleteZoneConfig(),
				c.existing,
				c.leaseholder,
				nil, /* replicaStats */
				c.check,
				true,  /* checkCandidateFullness */
				false, /* alwaysAllowDecisionWithoutStats */
			)
			if c.expected != target.StoreID {
				t.Fatalf("expected %d, but found %d", c.expected, target.StoreID)
			}
		})
	}
}

func TestAllocatorTransferLeaseTargetConstraints(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	stopper, g, _, a, _ := createTestAllocator(10, true /* deterministic */)
	defer stopper.Stop(context.Background())

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

	constraint := func(value string) []zonepb.ConstraintsConjunction {
		return []zonepb.ConstraintsConjunction{
			{
				Constraints: []zonepb.Constraint{
					{Key: "dc", Value: value, Type: zonepb.Constraint_REQUIRED},
				},
			},
		}
	}

	constraints := func(value string) *zonepb.ZoneConfig {
		return &zonepb.ZoneConfig{
			NumReplicas: proto.Int32(1),
			Constraints: constraint(value),
		}
	}

	voterConstraints := func(value string) *zonepb.ZoneConfig {
		return &zonepb.ZoneConfig{
			NumReplicas:      proto.Int32(1),
			VoterConstraints: constraint(value),
		}
	}

	testCases := []struct {
		existing    []roachpb.ReplicaDescriptor
		leaseholder roachpb.StoreID
		expected    roachpb.StoreID
		zone        *zonepb.ZoneConfig
	}{
		{existing: existing, leaseholder: 5, expected: 1, zone: constraints("1")},
		{existing: existing, leaseholder: 5, expected: 1, zone: voterConstraints("1")},
		{existing: existing, leaseholder: 5, expected: 0, zone: constraints("0")},
		{existing: existing, leaseholder: 5, expected: 0, zone: voterConstraints("0")},
		{existing: existing, leaseholder: 5, expected: 1, zone: zonepb.EmptyCompleteZoneConfig()},
	}
	for _, c := range testCases {
		t.Run("", func(t *testing.T) {
			target := a.TransferLeaseTarget(
				context.Background(),
				c.zone,
				c.existing,
				c.leaseholder,
				nil, /* replicaStats */
				true,
				true,  /* checkCandidateFullness */
				false, /* alwaysAllowDecisionWithoutStats */
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
	stopper, g, _, storePool, nl := createTestStorePool(
		TestTimeUntilStoreDeadOff, true, /* deterministic */
		func() int { return 10 }, /* nodeCount */
		livenesspb.NodeLivenessStatus_LIVE)
	a := MakeAllocator(storePool, func(string) (time.Duration, bool) {
		return 0, true
	})
	defer stopper.Stop(context.Background())

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
	preferDC1 := []zonepb.LeasePreference{
		{Constraints: []zonepb.Constraint{{Key: "dc", Value: "1", Type: zonepb.Constraint_REQUIRED}}},
	}
	//This means odd nodes.
	preferRegion1 := []zonepb.LeasePreference{
		{Constraints: []zonepb.Constraint{{Key: "region", Value: "1", Type: zonepb.Constraint_REQUIRED}}},
	}

	existing := []roachpb.ReplicaDescriptor{
		{StoreID: 1},
		{StoreID: 2},
		{StoreID: 3},
	}

	testCases := []struct {
		existing    []roachpb.ReplicaDescriptor
		leaseholder roachpb.StoreID
		check       bool
		expected    roachpb.StoreID
		zone        *zonepb.ZoneConfig
	}{
		// No existing lease holder, nothing to do.
		{existing: existing, leaseholder: 0, check: true, expected: 0, zone: zonepb.EmptyCompleteZoneConfig()},
		// Store 1 is draining, so it will try to transfer its lease if
		// checkTransferLeaseSource is false. This behavior isn't relied upon,
		// though; leases are manually transferred when draining.
		{existing: existing, leaseholder: 1, check: true, expected: 0, zone: zonepb.EmptyCompleteZoneConfig()},
		{existing: existing, leaseholder: 1, check: false, expected: 2, zone: zonepb.EmptyCompleteZoneConfig()},
		// Store 2 is not a lease transfer source.
		{existing: existing, leaseholder: 2, check: true, expected: 0, zone: zonepb.EmptyCompleteZoneConfig()},
		{existing: existing, leaseholder: 2, check: false, expected: 3, zone: zonepb.EmptyCompleteZoneConfig()},
		// Store 3 is a lease transfer source, but won't transfer to
		// node 1 because it's draining.
		{existing: existing, leaseholder: 3, check: true, expected: 2, zone: zonepb.EmptyCompleteZoneConfig()},
		{existing: existing, leaseholder: 3, check: false, expected: 2, zone: zonepb.EmptyCompleteZoneConfig()},
		// Verify that lease preferences dont impact draining
		{existing: existing, leaseholder: 2, check: true, expected: 0, zone: &zonepb.ZoneConfig{LeasePreferences: preferDC1}},
		{existing: existing, leaseholder: 2, check: false, expected: 0, zone: &zonepb.ZoneConfig{LeasePreferences: preferDC1}},
		{existing: existing, leaseholder: 2, check: true, expected: 3, zone: &zonepb.ZoneConfig{LeasePreferences: preferRegion1}},
		{existing: existing, leaseholder: 2, check: false, expected: 3, zone: &zonepb.ZoneConfig{LeasePreferences: preferRegion1}},
	}
	for _, c := range testCases {
		t.Run("", func(t *testing.T) {
			target := a.TransferLeaseTarget(
				context.Background(),
				c.zone,
				c.existing,
				c.leaseholder,
				nil, /* replicaStats */
				c.check,
				true,  /* checkCandidateFullness */
				false, /* alwaysAllowDecisionWithoutStats */
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

	stopper, g, _, a, _ := createTestAllocator(10, false /* deterministic */)
	ctx := context.Background()
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
		result, _, details, ok := a.RebalanceVoter(ctx, zonepb.EmptyCompleteZoneConfig(), nil, tc.existing, nil, rangeUsageInfo, storeFilterThrottled)
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
		result, _, details, ok := a.RebalanceVoter(ctx, zonepb.EmptyCompleteZoneConfig(), nil, tc.existing, nil, rangeUsageInfo, storeFilterThrottled)
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
	stopper, g, _, a, _ := createTestAllocator(10, true /* deterministic */)
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
				context.Background(),
				zonepb.EmptyCompleteZoneConfig(),
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
	stopper, g, _, storePool, nl := createTestStorePool(
		TestTimeUntilStoreDeadOff, true, /* deterministic */
		func() int { return 10 }, /* nodeCount */
		livenesspb.NodeLivenessStatus_LIVE)
	a := MakeAllocator(storePool, func(string) (time.Duration, bool) {
		return 0, true
	})
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
				context.Background(),
				zonepb.EmptyCompleteZoneConfig(),
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
	stopper, g, clock, storePool, nl := createTestStorePool(
		TestTimeUntilStoreDeadOff, true, /* deterministic */
		func() int { return 10 }, /* nodeCount */
		livenesspb.NodeLivenessStatus_LIVE)
	a := MakeAllocator(storePool, func(string) (time.Duration, bool) {
		return 0, true
	})
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
			context.Background(),
			zonepb.EmptyCompleteZoneConfig(),
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
	stopper, g, _, a, _ := createTestAllocator(10, true /* deterministic */)
	defer stopper.Stop(context.Background())

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

	preferDC1 := []zonepb.LeasePreference{
		{Constraints: []zonepb.Constraint{{Key: "dc", Value: "1", Type: zonepb.Constraint_REQUIRED}}},
	}
	preferDC4Then3Then2 := []zonepb.LeasePreference{
		{Constraints: []zonepb.Constraint{{Key: "dc", Value: "4", Type: zonepb.Constraint_REQUIRED}}},
		{Constraints: []zonepb.Constraint{{Key: "dc", Value: "3", Type: zonepb.Constraint_REQUIRED}}},
		{Constraints: []zonepb.Constraint{{Key: "dc", Value: "2", Type: zonepb.Constraint_REQUIRED}}},
	}
	preferN2ThenS3 := []zonepb.LeasePreference{
		{Constraints: []zonepb.Constraint{{Value: "n2", Type: zonepb.Constraint_REQUIRED}}},
		{Constraints: []zonepb.Constraint{{Value: "s3", Type: zonepb.Constraint_REQUIRED}}},
	}
	preferNotS1ThenNotN2 := []zonepb.LeasePreference{
		{Constraints: []zonepb.Constraint{{Value: "s1", Type: zonepb.Constraint_PROHIBITED}}},
		{Constraints: []zonepb.Constraint{{Value: "n2", Type: zonepb.Constraint_PROHIBITED}}},
	}
	preferNotS1AndNotN2 := []zonepb.LeasePreference{
		{
			Constraints: []zonepb.Constraint{
				{Value: "s1", Type: zonepb.Constraint_PROHIBITED},
				{Value: "n2", Type: zonepb.Constraint_PROHIBITED},
			},
		},
	}
	preferMatchesNothing := []zonepb.LeasePreference{
		{Constraints: []zonepb.Constraint{{Key: "dc", Value: "5", Type: zonepb.Constraint_REQUIRED}}},
		{Constraints: []zonepb.Constraint{{Value: "n6", Type: zonepb.Constraint_REQUIRED}}},
	}

	testCases := []struct {
		leaseholder        roachpb.StoreID
		existing           []roachpb.ReplicaDescriptor
		preferences        []zonepb.LeasePreference
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
			zone := &zonepb.ZoneConfig{NumReplicas: proto.Int32(0), LeasePreferences: c.preferences}
			result := a.ShouldTransferLease(
				context.Background(),
				zone,
				c.existing,
				c.leaseholder,
				nil, /* replicaStats */
			)
			expectTransfer := c.expectedCheckTrue != 0
			if expectTransfer != result {
				t.Errorf("expected %v, but found %v", expectTransfer, result)
			}
			target := a.TransferLeaseTarget(
				context.Background(),
				zone,
				c.existing,
				c.leaseholder,
				nil,   /* replicaStats */
				true,  /* checkTransferLeaseSource */
				true,  /* checkCandidateFullness */
				false, /* alwaysAllowDecisionWithoutStats */
			)
			if c.expectedCheckTrue != target.StoreID {
				t.Errorf("expected s%d for check=true, but found %v", c.expectedCheckTrue, target)
			}
			target = a.TransferLeaseTarget(
				context.Background(),
				zone,
				c.existing,
				c.leaseholder,
				nil,   /* replicaStats */
				false, /* checkTransferLeaseSource */
				true,  /* checkCandidateFullness */
				false, /* alwaysAllowDecisionWithoutStats */
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
	stopper, g, _, a, _ := createTestAllocator(10, true /* deterministic */)
	defer stopper.Stop(context.Background())

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

	preferEast := []zonepb.LeasePreference{
		{Constraints: []zonepb.Constraint{{Key: "region", Value: "us-east1", Type: zonepb.Constraint_REQUIRED}}},
	}
	preferNotEast := []zonepb.LeasePreference{
		{Constraints: []zonepb.Constraint{{Key: "region", Value: "us-east1", Type: zonepb.Constraint_PROHIBITED}}},
	}

	testCases := []struct {
		leaseholder        roachpb.StoreID
		existing           []roachpb.ReplicaDescriptor
		preferences        []zonepb.LeasePreference
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
			zone := &zonepb.ZoneConfig{NumReplicas: proto.Int32(0), LeasePreferences: c.preferences}
			target := a.TransferLeaseTarget(
				context.Background(),
				zone,
				c.existing,
				c.leaseholder,
				nil,   /* replicaStats */
				true,  /* checkTransferLeaseSource */
				true,  /* checkCandidateFullness */
				false, /* alwaysAllowDecisionWithoutStats */
			)
			if c.expectedCheckTrue != target.StoreID {
				t.Errorf("expected s%d for check=true, but found %v", c.expectedCheckTrue, target)
			}
			target = a.TransferLeaseTarget(
				context.Background(),
				zone,
				c.existing,
				c.leaseholder,
				nil,   /* replicaStats */
				false, /* checkTransferLeaseSource */
				true,  /* checkCandidateFullness */
				false, /* alwaysAllowDecisionWithoutStats */
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

	stopper, g, _, a, _ := createTestAllocator(10, false /* deterministic */)
	defer stopper.Stop(context.Background())
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
			context.Background(),
			zonepb.EmptyCompleteZoneConfig(),
			c.existingVoters, /* voterCandidates */
			c.existingVoters,
			c.existingNonVoters,
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
			context.Background(),
			zonepb.EmptyCompleteZoneConfig(),
			c.existingVoters, /* voterCandidates */
			c.existingVoters,
			nil, /* existingNonVoters */
		)
		require.NoError(t, err)
		require.Truef(t, checkReplExists(targetVoter, c.expVoterRemovals),
			"voter target for removal differs from expectation when non-voters are present;"+
				" expected %v, got %d", c.expVoterRemovals, targetVoter.StoreID)

		targetNonVoter, _, err := a.RemoveNonVoter(
			context.Background(),
			zonepb.EmptyCompleteZoneConfig(),
			c.existingNonVoters, /* nonVoterCandidates */
			c.existingVoters,
			c.existingNonVoters,
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
		zone                                          *zonepb.ZoneConfig
		expectedVoters, expectedNonVoters             []roachpb.StoreID
		shouldVoterAllocFail, shouldNonVoterAllocFail bool
		expError                                      string
	}{
		{
			name:              "one store satisfies constraints for each type of replica",
			stores:            multiDCStores,
			zone:              &multiDCConfigVoterAndNonVoter,
			expectedVoters:    []roachpb.StoreID{2},
			expectedNonVoters: []roachpb.StoreID{1},
		},
		{
			name:                    "only voter can satisfy constraints",
			stores:                  multiDCStores,
			zone:                    &multiDCConfigConstrainToA,
			expectedVoters:          []roachpb.StoreID{1},
			shouldNonVoterAllocFail: true,
		},
		{
			name:                 "only non_voter can satisfy constraints",
			stores:               multiDCStores,
			zone:                 &multiDCConfigUnsatisfiableVoterConstraints,
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
			stopper, g, _, a, _ := createTestAllocator(10, false /* deterministic */)
			defer stopper.Stop(ctx)
			sg := gossiputil.NewStoreGossiper(g)
			sg.GossipStores(test.stores, t)

			// Allocate the voting replica first, before the non-voter. This is the
			// order in which we'd expect the allocator to repair a given range. See
			// TestAllocatorComputeAction.
			voterTarget, _, err := a.AllocateVoter(ctx, test.zone, test.existingVoters, test.existingNonVoters)
			if test.shouldVoterAllocFail {
				require.Errorf(t, err, "expected voter allocation to fail; got %v as a valid target instead", voterTarget)
			} else {
				require.NoError(t, err)
				require.True(t, check(voterTarget.StoreID, test.expectedVoters))
				test.existingVoters = append(test.existingVoters, replicas(voterTarget.StoreID)...)
			}

			nonVoterTarget, _, err := a.AllocateNonVoter(ctx, test.zone, test.existingVoters, test.existingNonVoters)
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

	stopper, g, _, a, _ := createTestAllocator(10, false /* deterministic */)
	defer stopper.Stop(context.Background())
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
		targetStore, details, err := a.AllocateVoter(context.Background(), zonepb.EmptyCompleteZoneConfig(), existingRepls, nil)
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

	stopper, g, _, a, _ := createTestAllocator(10, false /* deterministic */)
	defer stopper.Stop(context.Background())

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
		target, _, details, ok := a.RebalanceVoter(context.Background(), zonepb.EmptyCompleteZoneConfig(), nil, existingRepls, nil, rangeUsageInfo, storeFilterThrottled)
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
	threeSpecificLocalities = []zonepb.ConstraintsConjunction{
		{
			Constraints: []zonepb.Constraint{
				{Key: "datacenter", Value: "a", Type: zonepb.Constraint_REQUIRED},
			},
			NumReplicas: 1,
		},
		{
			Constraints: []zonepb.Constraint{
				{Key: "datacenter", Value: "b", Type: zonepb.Constraint_REQUIRED},
			},
			NumReplicas: 1,
		},
		{
			Constraints: []zonepb.Constraint{
				{Key: "datacenter", Value: "c", Type: zonepb.Constraint_REQUIRED},
			},
			NumReplicas: 1,
		},
	}

	twoAndOneLocalities = []zonepb.ConstraintsConjunction{
		{
			Constraints: []zonepb.Constraint{
				{Key: "datacenter", Value: "a", Type: zonepb.Constraint_REQUIRED},
			},
			NumReplicas: 2,
		},
		{
			Constraints: []zonepb.Constraint{
				{Key: "datacenter", Value: "b", Type: zonepb.Constraint_REQUIRED},
			},
			NumReplicas: 1,
		},
	}

	threeInOneLocality = []zonepb.ConstraintsConjunction{
		{
			Constraints: []zonepb.Constraint{
				{Key: "datacenter", Value: "a", Type: zonepb.Constraint_REQUIRED},
			},
			NumReplicas: 3,
		},
	}

	twoAndOneNodeAttrs = []zonepb.ConstraintsConjunction{
		{
			Constraints: []zonepb.Constraint{
				{Value: "ssd", Type: zonepb.Constraint_REQUIRED},
			},
			NumReplicas: 2,
		},
		{
			Constraints: []zonepb.Constraint{
				{Value: "hdd", Type: zonepb.Constraint_REQUIRED},
			},
			NumReplicas: 1,
		},
	}

	twoAndOneStoreAttrs = []zonepb.ConstraintsConjunction{
		{
			Constraints: []zonepb.Constraint{
				{Value: "odd", Type: zonepb.Constraint_REQUIRED},
			},
			NumReplicas: 2,
		},
		{
			Constraints: []zonepb.Constraint{
				{Value: "even", Type: zonepb.Constraint_REQUIRED},
			},
			NumReplicas: 1,
		},
	}

	mixLocalityAndAttrs = []zonepb.ConstraintsConjunction{
		{
			Constraints: []zonepb.Constraint{
				{Key: "datacenter", Value: "a", Type: zonepb.Constraint_REQUIRED},
				{Value: "ssd", Type: zonepb.Constraint_REQUIRED},
			},
			NumReplicas: 1,
		},
		{
			Constraints: []zonepb.Constraint{
				{Key: "datacenter", Value: "b", Type: zonepb.Constraint_REQUIRED},
				{Value: "odd", Type: zonepb.Constraint_REQUIRED},
			},
			NumReplicas: 1,
		},
		{
			Constraints: []zonepb.Constraint{
				{Value: "even", Type: zonepb.Constraint_REQUIRED},
			},
			NumReplicas: 1,
		},
	}

	twoSpecificLocalities = []zonepb.ConstraintsConjunction{
		{
			Constraints: []zonepb.Constraint{
				{Key: "datacenter", Value: "a", Type: zonepb.Constraint_REQUIRED},
			},
			NumReplicas: 1,
		},
		{
			Constraints: []zonepb.Constraint{
				{Key: "datacenter", Value: "b", Type: zonepb.Constraint_REQUIRED},
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

	stopper, g, _, a, _ := createTestAllocator(10, false /* deterministic */)
	defer stopper.Stop(context.Background())
	sg := gossiputil.NewStoreGossiper(g)
	sg.GossipStores(stores, t)
	sl, _, _ := a.storePool.getStoreList(storeFilterThrottled)

	testCases := []struct {
		existing []roachpb.StoreID
		excluded []roachpb.StoreID
		expected []roachpb.StoreID
	}{
		{
			[]roachpb.StoreID{1},
			[]roachpb.StoreID{2},
			[]roachpb.StoreID{3, 4},
		},
		{
			[]roachpb.StoreID{1},
			[]roachpb.StoreID{2, 3},
			[]roachpb.StoreID{4},
		},
		{
			[]roachpb.StoreID{1},
			[]roachpb.StoreID{2, 3, 4},
			[]roachpb.StoreID{},
		},
		{
			[]roachpb.StoreID{1},
			[]roachpb.StoreID{2, 4},
			[]roachpb.StoreID{3},
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
		// No constraints.
		zone := &zonepb.ZoneConfig{NumReplicas: proto.Int32(0), Constraints: nil}
		analyzed := constraint.AnalyzeConstraints(
			context.Background(), a.storePool.getStoreDescriptor, existingRepls, *zone.NumReplicas,
			zone.Constraints)
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
				context.Background(),
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
				context.Background(),
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
				existingStores := make([]roachpb.StoreID, len(rebalanceOpts[0].existingCandidates))
				for i, cand := range rebalanceOpts[0].existingCandidates {
					existingStores[i] = cand.store.StoreID
				}
				require.ElementsMatch(t, tc.existing, existingStores)
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
		zone                              *zonepb.ZoneConfig
		expected                          roachpb.StoreID
		shouldFail                        bool
		expError                          string
	}{
		{
			name:              "voters only",
			existingNonVoters: replicas(1, 2, 3, 4),
			stores:            sameDCStores,
			zone:              zonepb.EmptyCompleteZoneConfig(),
			// Expect that that the store that doesn't have any replicas would be
			// the one to receive a new non-voter.
			expected: roachpb.StoreID(5),
		},
		{
			name:              "non-voters only",
			existingNonVoters: replicas(1, 2, 3, 4),
			stores:            sameDCStores,
			zone:              zonepb.EmptyCompleteZoneConfig(),
			expected:          roachpb.StoreID(5),
		},
		{
			name:              "mixed",
			existingVoters:    replicas(1, 2),
			existingNonVoters: replicas(3, 4),
			stores:            sameDCStores,
			zone:              zonepb.EmptyCompleteZoneConfig(),
			expected:          roachpb.StoreID(5),
		},
		{
			name: "only valid store has a voter",
			// Place a voter on the only store that would meet the constraints of
			// `multiDCConfigConstrainToA`.
			existingVoters: replicas(1),
			stores:         multiDCStores,
			zone:           &multiDCConfigConstrainToA,
			shouldFail:     true,
			expError:       "0 of 2 live stores are able to take a new replica for the range",
		},
		{
			name: "only valid store has a non_voter",
			// Place a non-voter on the only store that would meet the constraints of
			// `multiDCConfigConstrainToA`.
			existingNonVoters: replicas(1),
			stores:            multiDCStores,
			zone:              &multiDCConfigConstrainToA,
			shouldFail:        true,
			expError:          "0 of 2 live stores are able to take a new replica for the range",
		},
	}

	for i, test := range testCases {
		t.Run(fmt.Sprintf("%d:%s", i+1, test.name), func(t *testing.T) {
			ctx := context.Background()
			stopper, g, _, a, _ := createTestAllocator(10, false /* deterministic */)
			defer stopper.Stop(ctx)
			sg := gossiputil.NewStoreGossiper(g)
			sg.GossipStores(test.stores, t)

			result, _, err := a.AllocateNonVoter(ctx, test.zone, test.existingVoters, test.existingNonVoters)
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

	stopper, g, _, a, _ := createTestAllocator(10, false /* deterministic */)
	defer stopper.Stop(context.Background())
	sg := gossiputil.NewStoreGossiper(g)
	sg.GossipStores(multiDiversityDCStores, t)
	sl, _, _ := a.storePool.getStoreList(storeFilterThrottled)

	// Given a set of existing replicas for a range, rank which of the remaining
	// stores from multiDiversityDCStores would be the best addition to the range
	// purely on the basis of constraint satisfaction and locality diversity.
	testCases := []struct {
		constraints []zonepb.ConstraintsConjunction
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
		zone := &zonepb.ZoneConfig{NumReplicas: proto.Int32(0), Constraints: tc.constraints}
		analyzed := constraint.AnalyzeConstraints(
			context.Background(), a.storePool.getStoreDescriptor, existingRepls, *zone.NumReplicas,
			zone.Constraints)
		checkFn := voterConstraintsCheckerForAllocation(analyzed, constraint.EmptyAnalyzedConstraints)

		candidates := rankedCandidateListForAllocation(
			context.Background(),
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

	stopper, g, _, a, _ := createTestAllocator(10, false /* deterministic */)
	defer stopper.Stop(context.Background())
	sg := gossiputil.NewStoreGossiper(g)
	sg.GossipStores(multiDiversityDCStores, t)

	// Given a set of existing replicas for a range, rank which of the remaining
	// stores would be best to remove if we had to remove one purely on the basis
	// of constraint-matching and locality diversity.
	testCases := []struct {
		constraints []zonepb.ConstraintsConjunction
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
		ctx := context.Background()
		analyzed := constraint.AnalyzeConstraints(ctx, a.storePool.getStoreDescriptor, existingRepls,
			0 /* numReplicas */, tc.constraints)

		// Check behavior in a zone config where `voter_constraints` are empty.
		checkFn := voterConstraintsCheckerForRemoval(analyzed, constraint.EmptyAnalyzedConstraints)
		candidates := rankedCandidateListForRemoval(sl,
			checkFn,
			a.storePool.getLocalitiesByStore(existingRepls),
			a.scorerOptions())
		if !expectedStoreIDsMatch(tc.expected, candidates.worst()) {
			t.Errorf("%d (with `constraints`): expected rankedCandidateListForRemoval(%v)"+
				" = %v, but got %v\n for candidates %v", testIdx, tc.existing, tc.expected,
				candidates.worst(), candidates)
		}

		// Check that we'd see the same result if the same constraints were
		// specified as `voter_constraints`.
		checkFn = voterConstraintsCheckerForRemoval(constraint.EmptyAnalyzedConstraints, analyzed)
		candidates = rankedCandidateListForRemoval(sl,
			checkFn,
			a.storePool.getLocalitiesByStore(existingRepls),
			a.scorerOptions())
		if !expectedStoreIDsMatch(tc.expected, candidates.worst()) {
			t.Errorf("%d (with `voter_constraints`): expected rankedCandidateListForRemoval(%v)"+
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
		zone                                      *zonepb.ZoneConfig
		existingVoters, existingNonVoters         []roachpb.ReplicaDescriptor
		expectNoAction                            bool
		expectedRemoveTargets, expectedAddTargets []roachpb.StoreID
	}
	tests := []testCase{
		{
			name:              "no-op",
			stores:            multiDiversityDCStores,
			zone:              zonepb.EmptyCompleteZoneConfig(),
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
			zone:                  zonepb.EmptyCompleteZoneConfig(),
			existingVoters:        replicas(1, 2),
			existingNonVoters:     replicas(3, 4, 6),
			expectedRemoveTargets: []roachpb.StoreID{3, 4},
			expectedAddTargets:    []roachpb.StoreID{7, 8},
		},
		{
			name:                  "diversity among all existing replicas",
			stores:                multiDiversityDCStores,
			zone:                  zonepb.EmptyCompleteZoneConfig(),
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
			zone:                  zonepb.EmptyCompleteZoneConfig(),
			existingVoters:        replicas(3),
			existingNonVoters:     replicas(1),
			expectedRemoveTargets: []roachpb.StoreID{1},
			expectedAddTargets:    []roachpb.StoreID{2},
		},
		{
			name: "move off of nodes with too many ranges",
			// NB: Store 1 has 3x the number of ranges as the other stores.
			stores:                oneStoreWithTooManyRanges,
			zone:                  zonepb.EmptyCompleteZoneConfig(),
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
			zone:              &multiDCConfigVoterAndNonVoter,
			existingVoters:    replicas(2),
			existingNonVoters: replicas(1),
			expectNoAction:    true,
		},
		{
			name:   "need to rebalance to conform to constraints",
			stores: multiDCStores,
			// Constrain a non_voter to store 1.
			zone:                  &multiDCConfigVoterAndNonVoter,
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
			zone:              &multiDCConfigVoterAndNonVoter,
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
			stopper, g, _, a, _ := createTestAllocator(10, false /* deterministic */)
			defer stopper.Stop(ctx)
			sg := gossiputil.NewStoreGossiper(g)
			sg.GossipStores(test.stores, t)
			add, remove, _, ok := a.RebalanceNonVoter(ctx,
				test.zone,
				nil,
				test.existingVoters,
				test.existingNonVoters,
				rangeUsageInfo,
				storeFilterThrottled)
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
	stopper, g, _, a, _ := createTestAllocator(10, false /* deterministic */)
	defer stopper.Stop(context.Background())
	sg := gossiputil.NewStoreGossiper(g)
	sg.GossipStores(multiDiversityDCStores, t)

	zone := zonepb.ZoneConfig{
		NumReplicas: proto.Int32(4),
		NumVoters:   proto.Int32(2),
		// We constrain 2 voting replicas to datacenter "a" (stores 1 and 2) but
		// place non voting replicas there. In order to achieve constraints
		// conformance, each of the voters must want to move to one of these stores.
		VoterConstraints: []zonepb.ConstraintsConjunction{
			{
				NumReplicas: 2,
				Constraints: []zonepb.Constraint{
					{Type: zonepb.Constraint_REQUIRED, Key: "datacenter", Value: "a"},
				},
			},
		},
	}

	var rangeUsageInfo RangeUsageInfo
	existingNonVoters := replicas(1, 2)
	existingVoters := replicas(3, 4)
	add, remove, _, ok := a.RebalanceVoter(
		ctx,
		&zone,
		nil,
		existingVoters,
		existingNonVoters,
		rangeUsageInfo,
		storeFilterThrottled,
	)

	require.Truef(t, ok, "no action taken")
	if !(add.StoreID == roachpb.StoreID(1) || add.StoreID == roachpb.StoreID(2)) {
		t.Fatalf("received unexpected addition target %s from RebalanceVoter", add)
	}
	if !(remove.StoreID == roachpb.StoreID(3) || remove.StoreID == roachpb.StoreID(4)) {
		t.Fatalf("received unexpected removal target %s from RebalanceVoter", remove)
	}
}

func TestRebalanceCandidatesNumReplicasConstraints(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	stopper, g, _, a, _ := createTestAllocator(10, false /* deterministic */)
	defer stopper.Stop(context.Background())
	sg := gossiputil.NewStoreGossiper(g)
	sg.GossipStores(multiDiversityDCStores, t)
	sl, _, _ := a.storePool.getStoreList(storeFilterThrottled)

	// Given a set of existing replicas for a range, rank which of the remaining
	// stores would be best to remove if we had to remove one purely on the basis
	// of constraint-matching and locality diversity.
	type rebalanceStoreIDs struct {
		existing   []roachpb.StoreID
		candidates []roachpb.StoreID
	}
	testCases := []struct {
		constraints     []zonepb.ConstraintsConjunction
		zoneNumReplicas int32
		existing        []roachpb.StoreID
		expected        []rebalanceStoreIDs
		validTargets    []roachpb.StoreID
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
					existing:   []roachpb.StoreID{1},
					candidates: []roachpb.StoreID{3, 4, 5, 6},
				},
				{
					existing:   []roachpb.StoreID{2},
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
					existing:   []roachpb.StoreID{1},
					candidates: []roachpb.StoreID{5, 6},
				},
				{
					existing:   []roachpb.StoreID{2},
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
					existing:   []roachpb.StoreID{7},
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
					existing:   []roachpb.StoreID{1},
					candidates: []roachpb.StoreID{3, 4, 5, 6},
				},
				{
					existing:   []roachpb.StoreID{2},
					candidates: []roachpb.StoreID{3, 4, 5, 6},
				},
				{
					existing:   []roachpb.StoreID{7},
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
					existing:   []roachpb.StoreID{7},
					candidates: []roachpb.StoreID{3, 4, 5, 6},
				},
				{
					existing:   []roachpb.StoreID{8},
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
					existing:   []roachpb.StoreID{3},
					candidates: []roachpb.StoreID{1},
				},
				{
					existing:   []roachpb.StoreID{4},
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
					existing:   []roachpb.StoreID{1},
					candidates: []roachpb.StoreID{3, 4},
				},
				{
					existing:   []roachpb.StoreID{2},
					candidates: []roachpb.StoreID{3, 4},
				},
				{
					existing:   []roachpb.StoreID{5},
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
					existing:   []roachpb.StoreID{5},
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
					existing:   []roachpb.StoreID{5},
					candidates: []roachpb.StoreID{3, 4},
				},
				{
					existing:   []roachpb.StoreID{6},
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
					existing:   []roachpb.StoreID{5},
					candidates: []roachpb.StoreID{1, 2},
				},
				{
					existing:   []roachpb.StoreID{6},
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
					existing:   []roachpb.StoreID{3},
					candidates: []roachpb.StoreID{2},
				},
				{
					existing:   []roachpb.StoreID{4},
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
					existing:   []roachpb.StoreID{3},
					candidates: []roachpb.StoreID{2},
				},
				{
					existing:   []roachpb.StoreID{4},
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
					existing:   []roachpb.StoreID{3},
					candidates: []roachpb.StoreID{1, 2},
				},
				{
					existing:   []roachpb.StoreID{4},
					candidates: []roachpb.StoreID{1, 2},
				},
				{
					existing:   []roachpb.StoreID{5},
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
					existing:   []roachpb.StoreID{1},
					candidates: []roachpb.StoreID{5, 7},
				},
				{
					existing:   []roachpb.StoreID{2},
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
					existing:   []roachpb.StoreID{2},
					candidates: []roachpb.StoreID{1, 5, 7},
				},
				{
					existing:   []roachpb.StoreID{3},
					candidates: []roachpb.StoreID{5, 7},
				},
				{
					existing:   []roachpb.StoreID{4},
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
					existing:   []roachpb.StoreID{2},
					candidates: []roachpb.StoreID{1, 7},
				},
				{
					existing:   []roachpb.StoreID{4},
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
					existing:   []roachpb.StoreID{1},
					candidates: []roachpb.StoreID{2, 8},
				},
				{
					existing:   []roachpb.StoreID{3},
					candidates: []roachpb.StoreID{4, 8},
				},
				{
					existing:   []roachpb.StoreID{5},
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
					existing:   []roachpb.StoreID{2},
					candidates: []roachpb.StoreID{1, 7},
				},
				{
					existing:   []roachpb.StoreID{4},
					candidates: []roachpb.StoreID{3, 7},
				},
				{
					existing:   []roachpb.StoreID{6},
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
					existing:   []roachpb.StoreID{1},
					candidates: []roachpb.StoreID{5, 7},
				},
				{
					existing:   []roachpb.StoreID{2},
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
					existing:   []roachpb.StoreID{2},
					candidates: []roachpb.StoreID{1, 5, 7},
				},
				{
					existing:   []roachpb.StoreID{3},
					candidates: []roachpb.StoreID{5, 7},
				},
				{
					existing:   []roachpb.StoreID{4},
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
					existing:   []roachpb.StoreID{2},
					candidates: []roachpb.StoreID{1, 7},
				},
				{
					existing:   []roachpb.StoreID{4},
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
					existing:   []roachpb.StoreID{1},
					candidates: []roachpb.StoreID{2, 8},
				},
				{
					existing:   []roachpb.StoreID{3},
					candidates: []roachpb.StoreID{4, 8},
				},
				{
					existing:   []roachpb.StoreID{5},
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
					existing:   []roachpb.StoreID{2},
					candidates: []roachpb.StoreID{1, 7},
				},
				{
					existing:   []roachpb.StoreID{4},
					candidates: []roachpb.StoreID{3, 7},
				},
				{
					existing:   []roachpb.StoreID{6},
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
					existing:   []roachpb.StoreID{5},
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
					existing:   []roachpb.StoreID{5},
					candidates: []roachpb.StoreID{3},
				},
				{
					existing:   []roachpb.StoreID{6},
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
					existing:   []roachpb.StoreID{5},
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
					existing:   []roachpb.StoreID{2},
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
					existing:   []roachpb.StoreID{4},
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
					existing:   []roachpb.StoreID{2},
					candidates: []roachpb.StoreID{1},
				},
				{
					existing:   []roachpb.StoreID{4},
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
					existing:   []roachpb.StoreID{5},
					candidates: []roachpb.StoreID{1, 3},
				},
				{
					existing:   []roachpb.StoreID{6},
					candidates: []roachpb.StoreID{1, 2, 3, 4},
				},
				{
					existing:   []roachpb.StoreID{7},
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
					existing:   []roachpb.StoreID{6},
					candidates: []roachpb.StoreID{1, 3},
				},
				{
					existing:   []roachpb.StoreID{7},
					candidates: []roachpb.StoreID{1, 3},
				},
				{
					existing:   []roachpb.StoreID{8},
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
					existing:   []roachpb.StoreID{6},
					candidates: []roachpb.StoreID{3},
				},
				{
					existing:   []roachpb.StoreID{8},
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
					existing:   []roachpb.StoreID{5},
					candidates: []roachpb.StoreID{3, 4, 6},
				},
				{
					existing:   []roachpb.StoreID{7},
					candidates: []roachpb.StoreID{3, 4, 8},
				},
			},
			validTargets: []roachpb.StoreID{3, 4, 6, 8},
		},
		{
			constraints:     twoSpecificLocalities,
			zoneNumReplicas: 3,
			existing:        []roachpb.StoreID{1, 3, 5},
			expected:        []rebalanceStoreIDs{},
			validTargets:    []roachpb.StoreID{},
		},
		{
			constraints:     twoSpecificLocalities,
			zoneNumReplicas: 3,
			existing:        []roachpb.StoreID{1, 3, 7},
			expected:        []rebalanceStoreIDs{},
			validTargets:    []roachpb.StoreID{},
		},
		{
			constraints:     twoSpecificLocalities,
			zoneNumReplicas: 3,
			existing:        []roachpb.StoreID{2, 4, 8},
			expected:        []rebalanceStoreIDs{},
			validTargets:    []roachpb.StoreID{},
		},
		{
			constraints:     twoSpecificLocalities,
			zoneNumReplicas: 3,
			existing:        []roachpb.StoreID{1, 2, 3},
			expected: []rebalanceStoreIDs{
				{
					existing:   []roachpb.StoreID{1},
					candidates: []roachpb.StoreID{5, 6, 7, 8},
				},
				{
					existing:   []roachpb.StoreID{2},
					candidates: []roachpb.StoreID{5, 6, 7, 8},
				},
			},
			validTargets: []roachpb.StoreID{5, 6, 7, 8},
		},
		{
			constraints:     twoSpecificLocalities,
			zoneNumReplicas: 3,
			existing:        []roachpb.StoreID{2, 3, 4},
			expected: []rebalanceStoreIDs{
				{
					existing:   []roachpb.StoreID{3},
					candidates: []roachpb.StoreID{5, 6, 7, 8},
				},
				{
					existing:   []roachpb.StoreID{4},
					candidates: []roachpb.StoreID{5, 6, 7, 8},
				},
			},
			validTargets: []roachpb.StoreID{5, 6, 7, 8},
		},
		{
			constraints:     twoSpecificLocalities,
			zoneNumReplicas: 3,
			existing:        []roachpb.StoreID{1, 2, 5},
			expected: []rebalanceStoreIDs{
				{
					existing:   []roachpb.StoreID{1},
					candidates: []roachpb.StoreID{3, 4},
				},
				{
					existing:   []roachpb.StoreID{2},
					candidates: []roachpb.StoreID{3, 4},
				},
				{
					existing:   []roachpb.StoreID{5},
					candidates: []roachpb.StoreID{3, 4},
				},
			},
			validTargets: []roachpb.StoreID{3, 4},
		},
		{
			constraints:     twoSpecificLocalities,
			zoneNumReplicas: 3,
			existing:        []roachpb.StoreID{3, 4, 5},
			expected: []rebalanceStoreIDs{
				{
					existing:   []roachpb.StoreID{3},
					candidates: []roachpb.StoreID{1, 2},
				},
				{
					existing:   []roachpb.StoreID{4},
					candidates: []roachpb.StoreID{1, 2},
				},
				{
					existing:   []roachpb.StoreID{5},
					candidates: []roachpb.StoreID{1, 2},
				},
			},
			validTargets: []roachpb.StoreID{1, 2},
		},
		{
			constraints:     twoSpecificLocalities,
			zoneNumReplicas: 3,
			existing:        []roachpb.StoreID{1, 5, 7},
			expected: []rebalanceStoreIDs{
				{
					existing:   []roachpb.StoreID{5},
					candidates: []roachpb.StoreID{3, 4},
				},
				{
					existing:   []roachpb.StoreID{7},
					candidates: []roachpb.StoreID{3, 4},
				},
			},
			validTargets: []roachpb.StoreID{3, 4},
		},
		{
			constraints:     twoSpecificLocalities,
			zoneNumReplicas: 3,
			existing:        []roachpb.StoreID{1, 5, 6},
			expected: []rebalanceStoreIDs{
				{
					existing:   []roachpb.StoreID{5},
					candidates: []roachpb.StoreID{3, 4},
				},
				{
					existing:   []roachpb.StoreID{6},
					candidates: []roachpb.StoreID{3, 4},
				},
			},
			validTargets: []roachpb.StoreID{3, 4},
		},
		{
			constraints:     twoSpecificLocalities,
			zoneNumReplicas: 3,
			existing:        []roachpb.StoreID{5, 6, 7},
			expected: []rebalanceStoreIDs{
				{
					existing:   []roachpb.StoreID{5},
					candidates: []roachpb.StoreID{1, 2, 3, 4},
				},
				{
					existing:   []roachpb.StoreID{6},
					candidates: []roachpb.StoreID{1, 2, 3, 4},
				},
				{
					existing:   []roachpb.StoreID{7},
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
		zone := &zonepb.ZoneConfig{
			Constraints: tc.constraints,
			NumReplicas: proto.Int32(tc.zoneNumReplicas),
		}
		analyzed := constraint.AnalyzeConstraints(
			context.Background(), a.storePool.getStoreDescriptor, existingRepls,
			*zone.NumReplicas, zone.Constraints)
		removalConstraintsChecker := voterConstraintsCheckerForRemoval(
			analyzed,
			constraint.EmptyAnalyzedConstraints,
		)
		rebalanceConstraintsChecker := voterConstraintsCheckerForRebalance(
			analyzed,
			constraint.EmptyAnalyzedConstraints,
		)

		results := rankedCandidateListForRebalancing(
			context.Background(),
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
				return results[i].existingCandidates[0].store.StoreID < results[j].existingCandidates[0].store.StoreID
			})
			for i := range tc.expected {
				if !expectedStoreIDsMatch(tc.expected[i].existing, results[i].existingCandidates) ||
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
			target, _, details, ok := a.RebalanceVoter(context.Background(), zone, nil, existingRepls, nil, rangeUsageInfo, storeFilterThrottled)
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

	stopper, g, _, storePool, _ := createTestStorePool(
		TestTimeUntilStoreDeadOff, true, /* deterministic */
		func() int { return 10 }, /* nodeCount */
		livenesspb.NodeLivenessStatus_LIVE)
	defer stopper.Stop(context.Background())

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
	evenlyBalanced.record(1)
	evenlyBalanced.record(2)
	evenlyBalanced.record(3)
	imbalanced1 := newReplicaStats(clock, localityFn)
	imbalanced2 := newReplicaStats(clock, localityFn)
	imbalanced3 := newReplicaStats(clock, localityFn)
	for i := 0; i < 100*int(MinLeaseTransferStatsDuration.Seconds()); i++ {
		evenlyBalanced.record(99)
		imbalanced1.record(1)
		imbalanced2.record(2)
		imbalanced3.record(3)
	}

	manual.Increment(int64(MinLeaseTransferStatsDuration))

	noLatency := map[string]time.Duration{}
	highLatency := map[string]time.Duration{
		stores[0].Node.Address.String(): 50 * time.Millisecond,
		stores[1].Node.Address.String(): 50 * time.Millisecond,
		stores[2].Node.Address.String(): 50 * time.Millisecond,
	}

	existing := []roachpb.ReplicaDescriptor{
		{NodeID: 1, StoreID: 1},
		{NodeID: 2, StoreID: 2},
		{NodeID: 3, StoreID: 3},
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
			a := MakeAllocator(storePool, func(addr string) (time.Duration, bool) {
				return c.latency[addr], true
			})
			target := a.TransferLeaseTarget(
				context.Background(),
				zonepb.EmptyCompleteZoneConfig(),
				existing,
				c.leaseholder,
				c.stats,
				c.check,
				true,  /* checkCandidateFullness */
				false, /* alwaysAllowDecisionWithoutStats */
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
			context.Background(),
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
	stopper, g, _, a, _ := createTestAllocator(10, false /* deterministic */)
	defer stopper.Stop(ctx)
	sg := gossiputil.NewStoreGossiper(g)
	sg.GossipStores(stores, t)

	// Repeat this test 10 times, it should always be either store 2 or 3.
	for i := 0; i < 10; i++ {
		targetRepl, _, err := a.RemoveVoter(ctx, zonepb.EmptyCompleteZoneConfig(), replicas, replicas,
			nil)
		if err != nil {
			t.Fatal(err)
		}
		if a, e1, e2 := targetRepl, replicas[1], replicas[2]; a != e1 && a != e2 {
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
		zone           zonepb.ZoneConfig
		desc           roachpb.RangeDescriptor
		expectedAction AllocatorAction
	}{
		// Need three replicas, have three, one is on a dead store.
		{
			zone: zonepb.ZoneConfig{
				NumReplicas:   proto.Int32(3),
				Constraints:   []zonepb.ConstraintsConjunction{{Constraints: []zonepb.Constraint{{Value: "us-east", Type: zonepb.Constraint_DEPRECATED_POSITIVE}}}},
				RangeMinBytes: proto.Int64(0),
				RangeMaxBytes: proto.Int64(64000),
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
			zone: zonepb.ZoneConfig{
				NumReplicas:   proto.Int32(5),
				Constraints:   []zonepb.ConstraintsConjunction{{Constraints: []zonepb.Constraint{{Value: "us-east", Type: zonepb.Constraint_DEPRECATED_POSITIVE}}}},
				RangeMinBytes: proto.Int64(0),
				RangeMaxBytes: proto.Int64(64000),
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
			zone: zonepb.ZoneConfig{
				NumReplicas:   proto.Int32(5),
				NumVoters:     proto.Int32(3),
				RangeMinBytes: proto.Int64(0),
				RangeMaxBytes: proto.Int64(64000),
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
			zone: zonepb.ZoneConfig{
				NumReplicas:   proto.Int32(3),
				Constraints:   []zonepb.ConstraintsConjunction{{Constraints: []zonepb.Constraint{{Value: "us-east", Type: zonepb.Constraint_DEPRECATED_POSITIVE}}}},
				RangeMinBytes: proto.Int64(0),
				RangeMaxBytes: proto.Int64(64000),
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
			zone: zonepb.ZoneConfig{
				NumReplicas:   proto.Int32(3),
				Constraints:   []zonepb.ConstraintsConjunction{{Constraints: []zonepb.Constraint{{Value: "us-east", Type: zonepb.Constraint_DEPRECATED_POSITIVE}}}},
				RangeMinBytes: proto.Int64(0),
				RangeMaxBytes: proto.Int64(64000),
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
			zone: zonepb.ZoneConfig{
				NumReplicas:   proto.Int32(5),
				NumVoters:     proto.Int32(3),
				RangeMinBytes: proto.Int64(0),
				RangeMaxBytes: proto.Int64(64000),
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
			zone: zonepb.ZoneConfig{
				NumReplicas:   proto.Int32(5),
				Constraints:   []zonepb.ConstraintsConjunction{{Constraints: []zonepb.Constraint{{Value: "us-east", Type: zonepb.Constraint_DEPRECATED_POSITIVE}}}},
				RangeMinBytes: proto.Int64(0),
				RangeMaxBytes: proto.Int64(64000),
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
			zone: zonepb.ZoneConfig{
				NumReplicas:   proto.Int32(5),
				Constraints:   []zonepb.ConstraintsConjunction{{Constraints: []zonepb.Constraint{{Value: "us-east", Type: zonepb.Constraint_DEPRECATED_POSITIVE}}}},
				RangeMinBytes: proto.Int64(0),
				RangeMaxBytes: proto.Int64(64000),
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
			zone: zonepb.ZoneConfig{
				NumReplicas:   proto.Int32(3),
				Constraints:   []zonepb.ConstraintsConjunction{{Constraints: []zonepb.Constraint{{Value: "us-east", Type: zonepb.Constraint_DEPRECATED_POSITIVE}}}},
				RangeMinBytes: proto.Int64(0),
				RangeMaxBytes: proto.Int64(64000),
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
			zone: zonepb.ZoneConfig{
				NumReplicas:   proto.Int32(5),
				Constraints:   []zonepb.ConstraintsConjunction{{Constraints: []zonepb.Constraint{{Value: "us-east", Type: zonepb.Constraint_DEPRECATED_POSITIVE}}}},
				RangeMinBytes: proto.Int64(0),
				RangeMaxBytes: proto.Int64(64000),
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
			zone: zonepb.ZoneConfig{
				NumReplicas:   proto.Int32(3),
				Constraints:   []zonepb.ConstraintsConjunction{{Constraints: []zonepb.Constraint{{Value: "us-east", Type: zonepb.Constraint_DEPRECATED_POSITIVE}}}},
				RangeMinBytes: proto.Int64(0),
				RangeMaxBytes: proto.Int64(64000),
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
			zone: zonepb.ZoneConfig{
				NumReplicas:   proto.Int32(3),
				Constraints:   []zonepb.ConstraintsConjunction{{Constraints: []zonepb.Constraint{{Value: "us-east", Type: zonepb.Constraint_DEPRECATED_POSITIVE}}}},
				RangeMinBytes: proto.Int64(0),
				RangeMaxBytes: proto.Int64(64000),
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
			zone: zonepb.ZoneConfig{
				NumReplicas:   proto.Int32(3),
				Constraints:   []zonepb.ConstraintsConjunction{{Constraints: []zonepb.Constraint{{Value: "us-east", Type: zonepb.Constraint_DEPRECATED_POSITIVE}}}},
				RangeMinBytes: proto.Int64(0),
				RangeMaxBytes: proto.Int64(64000),
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
			zone: zonepb.ZoneConfig{
				NumReplicas:   proto.Int32(5),
				NumVoters:     proto.Int32(3),
				RangeMinBytes: proto.Int64(0),
				RangeMaxBytes: proto.Int64(64000),
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
			zone: zonepb.ZoneConfig{
				NumReplicas:   proto.Int32(5),
				NumVoters:     proto.Int32(3),
				RangeMinBytes: proto.Int64(0),
				RangeMaxBytes: proto.Int64(64000),
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
			zone: zonepb.ZoneConfig{
				NumReplicas:   proto.Int32(3),
				NumVoters:     proto.Int32(1),
				RangeMinBytes: proto.Int64(0),
				RangeMaxBytes: proto.Int64(64000),
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
			zone: zonepb.ZoneConfig{
				NumReplicas:   proto.Int32(2),
				NumVoters:     proto.Int32(1),
				RangeMinBytes: proto.Int64(0),
				RangeMaxBytes: proto.Int64(64000),
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
			zone: zonepb.ZoneConfig{
				NumReplicas:   proto.Int32(2),
				NumVoters:     proto.Int32(1),
				RangeMinBytes: proto.Int64(0),
				RangeMaxBytes: proto.Int64(64000),
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
			zone: zonepb.ZoneConfig{
				NumReplicas:   proto.Int32(3),
				Constraints:   []zonepb.ConstraintsConjunction{{Constraints: []zonepb.Constraint{{Value: "us-east", Type: zonepb.Constraint_DEPRECATED_POSITIVE}}}},
				RangeMinBytes: proto.Int64(0),
				RangeMaxBytes: proto.Int64(64000),
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
			zone: zonepb.ZoneConfig{
				NumReplicas:   proto.Int32(3),
				Constraints:   []zonepb.ConstraintsConjunction{{Constraints: []zonepb.Constraint{{Value: "us-east", Type: zonepb.Constraint_DEPRECATED_POSITIVE}}}},
				RangeMinBytes: proto.Int64(0),
				RangeMaxBytes: proto.Int64(64000),
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
			zone: zonepb.ZoneConfig{
				NumReplicas:   proto.Int32(3),
				Constraints:   []zonepb.ConstraintsConjunction{{Constraints: []zonepb.Constraint{{Value: "us-east", Type: zonepb.Constraint_DEPRECATED_POSITIVE}}}},
				RangeMinBytes: proto.Int64(0),
				RangeMaxBytes: proto.Int64(64000),
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

	stopper, _, sp, a, _ := createTestAllocator(10, false /* deterministic */)
	ctx := context.Background()
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
		action, priority := a.ComputeAction(ctx, &tcase.zone, &tcase.desc)
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

	zone := zonepb.ZoneConfig{
		NumReplicas: proto.Int32(3),
	}
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

	stopper, _, sp, a, _ := createTestAllocator(10, false /* deterministic */)
	ctx := context.Background()
	defer stopper.Stop(ctx)

	for i, tcase := range testCases {
		mockStorePool(sp, tcase.live, nil, tcase.dead, nil, nil, nil)
		action, _ := a.ComputeAction(ctx, &zone, &tcase.desc)
		if tcase.expectedAction != action {
			t.Errorf("Test case %d expected action %d, got action %d", i, tcase.expectedAction, action)
		}
	}
}

func TestAllocatorComputeActionSuspect(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	zone := zonepb.ZoneConfig{
		NumReplicas: proto.Int32(3),
	}
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
		// Needs three replicas, two are suspect (i.e. the range lacks a quorum).
		{
			desc:           threeReplDesc,
			live:           []roachpb.StoreID{1, 4},
			suspect:        []roachpb.StoreID{2, 3},
			expectedAction: AllocatorRangeUnavailable,
		},
	}

	stopper, _, sp, a, _ := createTestAllocator(10, false /* deterministic */)
	ctx := context.Background()
	defer stopper.Stop(ctx)

	for i, tcase := range testCases {
		mockStorePool(sp, tcase.live, nil, nil, nil, nil, tcase.suspect)
		action, _ := a.ComputeAction(ctx, &zone, &tcase.desc)
		if tcase.expectedAction != action {
			t.Errorf("Test case %d expected action %d, got action %d", i, tcase.expectedAction, action)
		}
	}
}

func TestAllocatorComputeActionDecommission(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testCases := []struct {
		zone            zonepb.ZoneConfig
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
			zone: zonepb.ZoneConfig{
				NumReplicas: proto.Int32(3),
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
			expectedAction:  AllocatorReplaceDecommissioningVoter,
			live:            []roachpb.StoreID{1, 2},
			dead:            nil,
			decommissioning: []roachpb.StoreID{3},
		},
		// Has three replicas, one is in decommissioning status, and one is on a
		// dead node. Replacing the dead replica is more important.
		{
			zone: zonepb.ZoneConfig{
				NumReplicas: proto.Int32(3),
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
			expectedAction:  AllocatorReplaceDeadVoter,
			live:            []roachpb.StoreID{1},
			dead:            []roachpb.StoreID{2},
			decommissioning: []roachpb.StoreID{3},
		},
		// Needs three replicas, has four, where one is decommissioning and one is
		// dead.
		{
			zone: zonepb.ZoneConfig{
				NumReplicas: proto.Int32(3),
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
			expectedAction:  AllocatorRemoveDeadVoter,
			live:            []roachpb.StoreID{1, 4},
			dead:            []roachpb.StoreID{2},
			decommissioning: []roachpb.StoreID{3},
		},
		// Needs three replicas, has four, where one is decommissioning and one is
		// decommissioned.
		{
			zone: zonepb.ZoneConfig{
				NumReplicas: proto.Int32(3),
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
			expectedAction:  AllocatorRemoveDeadVoter,
			live:            []roachpb.StoreID{1, 4},
			dead:            nil,
			decommissioning: []roachpb.StoreID{3},
			decommissioned:  []roachpb.StoreID{2},
		},
		// Needs three replicas, has three, all decommissioning
		{
			zone: zonepb.ZoneConfig{
				NumReplicas: proto.Int32(3),
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
			expectedAction:  AllocatorReplaceDecommissioningVoter,
			live:            nil,
			dead:            nil,
			decommissioning: []roachpb.StoreID{1, 2, 3},
		},
		// Needs 3. Has 1 live, 3 decommissioning.
		{
			zone: zonepb.ZoneConfig{
				NumReplicas: proto.Int32(3),
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
			expectedAction:  AllocatorRemoveDecommissioningVoter,
			live:            []roachpb.StoreID{4},
			dead:            nil,
			decommissioning: []roachpb.StoreID{1, 2, 3},
		},
		{
			zone: zonepb.ZoneConfig{
				NumVoters:   proto.Int32(1),
				NumReplicas: proto.Int32(3),
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
			zone: zonepb.ZoneConfig{
				NumVoters:   proto.Int32(1),
				NumReplicas: proto.Int32(3),
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

	stopper, _, sp, a, _ := createTestAllocator(10, false /* deterministic */)
	ctx := context.Background()
	defer stopper.Stop(ctx)

	for i, tcase := range testCases {
		mockStorePool(sp, tcase.live, nil, tcase.dead, tcase.decommissioning, tcase.decommissioned, nil)
		action, _ := a.ComputeAction(ctx, &tcase.zone, &tcase.desc)
		if tcase.expectedAction != action {
			t.Errorf("Test case %d expected action %s, got action %s", i, tcase.expectedAction, action)
			continue
		}
	}
}

func TestAllocatorRemoveLearner(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	zone := zonepb.ZoneConfig{
		NumReplicas: proto.Int32(3),
	}
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
	stopper, _, sp, a, _ := createTestAllocator(10, false /* deterministic */)
	ctx := context.Background()
	defer stopper.Stop(ctx)
	live, dead := []roachpb.StoreID{1, 2}, []roachpb.StoreID{3}
	mockStorePool(sp, live, nil, dead, nil, nil, nil)
	action, _ := a.ComputeAction(ctx, &zone, &rangeWithLearnerDesc)
	require.Equal(t, AllocatorRemoveLearner, action)
}

func TestAllocatorComputeActionDynamicNumReplicas(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// In this test, the configured zone config has a replication factor of five
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
			// zone config explicitly asks for that, which it does not), so three
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
			// zone config.
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
			// sticking with the zone config.
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
	stopper, _, _, sp, _ := createTestStorePool(
		TestTimeUntilStoreDeadOff, false, /* deterministic */
		func() int { return numNodes },
		livenesspb.NodeLivenessStatus_LIVE)
	a := MakeAllocator(sp, func(string) (time.Duration, bool) {
		return 0, true
	})

	ctx := context.Background()
	defer stopper.Stop(ctx)
	zone := &zonepb.ZoneConfig{
		NumReplicas: proto.Int32(5),
	}

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
				effectiveNumReplicas := GetNeededVoters(*zone.NumReplicas, clusterNodes)
				require.Equal(t, c.expectedNumReplicas, effectiveNumReplicas, "clusterNodes=%d", clusterNodes)

				action, _ := a.ComputeAction(ctx, zone, &desc)
				require.Equal(t, c.expectedAction.String(), action.String())
			})
		}
	}
}

func TestAllocatorGetNeededReplicas(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testCases := []struct {
		zoneRepls  int32
		availNodes int
		expected   int
	}{
		// If zone.NumReplicas <= 3, GetNeededVoters should always return zone.NumReplicas.
		{1, 0, 1},
		{1, 1, 1},
		{2, 0, 2},
		{2, 1, 2},
		{2, 2, 2},
		{3, 0, 3},
		{3, 1, 3},
		{3, 3, 3},
		// Things get more involved when zone.NumReplicas > 3.
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
		if e, a := tc.expected, GetNeededVoters(tc.zoneRepls, tc.availNodes); e != a {
			t.Errorf(
				"GetNeededVoters(zone.NumReplicas=%d, availNodes=%d) got %d; want %d",
				tc.zoneRepls, tc.availNodes, a, e)
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

	a := MakeAllocator(nil /* storePool */, nil /* rpcContext */)
	action, priority := a.ComputeAction(context.Background(), &zonepb.ZoneConfig{NumReplicas: proto.Int32(0)}, nil)
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

	constraint := []zonepb.ConstraintsConjunction{
		{Constraints: []zonepb.Constraint{{Value: "one", Type: zonepb.Constraint_REQUIRED}}},
	}
	constraints := []zonepb.ConstraintsConjunction{
		{
			Constraints: []zonepb.Constraint{
				{Value: "one", Type: zonepb.Constraint_REQUIRED},
				{Value: "two", Type: zonepb.Constraint_REQUIRED},
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

	stopper, g, _, a, _ := createTestAllocator(10, false /* deterministic */)
	ctx := context.Background()
	defer stopper.Stop(ctx)

	// First test to make sure we would send the replica to purgatory.
	_, _, err := a.AllocateVoter(ctx, &simpleZoneConfig, []roachpb.ReplicaDescriptor{}, nil)
	if !errors.HasInterface(err, (*purgatoryError)(nil)) {
		t.Fatalf("expected a purgatory error, got: %+v", err)
	}

	// Second, test the normal case in which we can allocate to the store.
	gossiputil.NewStoreGossiper(g).GossipStores(singleStore, t)
	result, _, err := a.AllocateVoter(ctx, &simpleZoneConfig, []roachpb.ReplicaDescriptor{}, nil)
	if err != nil {
		t.Fatalf("unable to perform allocation: %+v", err)
	}
	if result.Node.NodeID != 1 || result.StoreID != 1 {
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
	_, _, err = a.AllocateVoter(ctx, &simpleZoneConfig, []roachpb.ReplicaDescriptor{}, nil)
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

// TestAllocatorRebalanceAway verifies that when a replica is on a node with a
// bad zone config, the replica will be rebalanced off of it.
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
		constraint zonepb.Constraint
		expected   *roachpb.StoreID
	}{
		{
			constraint: zonepb.Constraint{Key: "datacenter", Value: "us", Type: zonepb.Constraint_REQUIRED},
			expected:   &stores[3].StoreID,
		},
		{
			constraint: zonepb.Constraint{Key: "datacenter", Value: "eur", Type: zonepb.Constraint_PROHIBITED},
			expected:   &stores[3].StoreID,
		},
		{
			constraint: zonepb.Constraint{Key: "datacenter", Value: "eur", Type: zonepb.Constraint_REQUIRED},
			expected:   &stores[4].StoreID,
		},
		{
			constraint: zonepb.Constraint{Key: "datacenter", Value: "us", Type: zonepb.Constraint_PROHIBITED},
			expected:   &stores[4].StoreID,
		},
		{
			constraint: zonepb.Constraint{Key: "datacenter", Value: "other", Type: zonepb.Constraint_REQUIRED},
			expected:   nil,
		},
		{
			constraint: zonepb.Constraint{Key: "datacenter", Value: "other", Type: zonepb.Constraint_PROHIBITED},
			expected:   nil,
		},
		{
			constraint: zonepb.Constraint{Key: "datacenter", Value: "other", Type: zonepb.Constraint_DEPRECATED_POSITIVE},
			expected:   nil,
		},
		{
			constraint: zonepb.Constraint{Key: "datacenter", Value: "us", Type: zonepb.Constraint_DEPRECATED_POSITIVE},
			expected:   nil,
		},
		{
			constraint: zonepb.Constraint{Key: "datacenter", Value: "eur", Type: zonepb.Constraint_DEPRECATED_POSITIVE},
			expected:   nil,
		},
	}

	stopper, g, _, a, _ := createTestAllocator(10, false /* deterministic */)
	defer stopper.Stop(context.Background())
	gossiputil.NewStoreGossiper(g).GossipStores(stores, t)
	ctx := context.Background()

	for _, tc := range testCases {
		t.Run(tc.constraint.String(), func(t *testing.T) {
			constraints := zonepb.ConstraintsConjunction{
				Constraints: []zonepb.Constraint{
					tc.constraint,
				},
			}

			var rangeUsageInfo RangeUsageInfo
			actual, _, _, ok := a.RebalanceVoter(ctx, &zonepb.ZoneConfig{NumReplicas: proto.Int32(0), Constraints: []zonepb.ConstraintsConjunction{constraints}}, nil, existingReplicas, nil, rangeUsageInfo, storeFilterThrottled)

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

func (ts *testStore) add(bytes int64) {
	ts.Capacity.RangeCount++
	ts.Capacity.Available -= bytes
	ts.Capacity.Used += bytes
	ts.Capacity.LogicalBytes += bytes
}

func (ts *testStore) rebalance(ots *testStore, bytes int64) {
	if ts.Capacity.RangeCount == 0 || (ts.Capacity.Capacity-ts.Capacity.Available) < bytes {
		return
	}
	// Mimic a real Store's behavior of rejecting preemptive snapshots when full.
	if !maxCapacityCheck(ots.StoreDescriptor) {
		log.Infof(context.Background(),
			"s%d too full to accept snapshot from s%d: %v", ots.StoreID, ts.StoreID, ots.Capacity)
		return
	}
	log.Infof(context.Background(), "s%d accepting snapshot from s%d", ots.StoreID, ts.StoreID)
	ts.Capacity.RangeCount--
	if ts.immediateCompaction {
		ts.Capacity.Available += bytes
		ts.Capacity.Used -= bytes
	}
	ts.Capacity.LogicalBytes -= bytes
	ots.Capacity.RangeCount++
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
	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)

	// Model a set of stores in a cluster doing rebalancing, with ranges being
	// randomly added occasionally.
	rpcContext := rpc.NewContext(rpc.ContextOptions{
		TenantID:   roachpb.SystemTenantID,
		AmbientCtx: log.AmbientContext{Tracer: st.Tracer},
		Config:     &base.Config{Insecure: true},
		Clock:      clock,
		Stopper:    stopper,
		Settings:   st,
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
		log.AmbientContext{Tracer: st.Tracer},
		st,
		g,
		clock,
		func() int {
			return nodes
		},
		mockNodeLiveness.nodeLivenessFunc,
		false, /* deterministic */
	)
	alloc := MakeAllocator(sp, func(string) (time.Duration, bool) {
		return 0, false
	})

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
	testStores[0].add(rangeSize)
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
					ts.add(rangeSize)
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
					target, _, details, ok := alloc.RebalanceVoter(ctx, zonepb.EmptyCompleteZoneConfig(), nil, []roachpb.ReplicaDescriptor{{NodeID: ts.Node.NodeID, StoreID: ts.StoreID}}, nil, rangeUsageInfo, storeFilterThrottled)
					if ok {
						if log.V(1) {
							log.Infof(ctx, "rebalancing to %v; details: %s", target, details)
						}
						testStores[k].rebalance(&testStores[int(target.StoreID)], rangeSize)
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

func Example_rebalancing() {
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)

	// Model a set of stores in a cluster,
	// adding / rebalancing ranges of random sizes.
	rpcContext := rpc.NewContext(rpc.ContextOptions{
		TenantID:   roachpb.SystemTenantID,
		AmbientCtx: log.AmbientContext{Tracer: st.Tracer},
		Config:     &base.Config{Insecure: true},
		Clock:      clock,
		Stopper:    stopper,
		Settings:   st,
	})
	server := rpc.NewServer(rpcContext) // never started
	g := gossip.NewTest(1, rpcContext, server, stopper, metric.NewRegistry(), zonepb.DefaultZoneConfigRef())

	TimeUntilStoreDead.Override(ctx, &st.SV, TestTimeUntilStoreDeadOff)

	const generations = 100
	const nodes = 20
	const printGenerations = generations / 2

	// Deterministic must be set as this test is comparing the exact output
	// after each rebalance.
	sp := NewStorePool(
		log.AmbientContext{Tracer: st.Tracer},
		st,
		g,
		clock,
		func() int {
			return nodes
		},
		newMockNodeLiveness(livenesspb.NodeLivenessStatus_LIVE).nodeLivenessFunc,
		/* deterministic */ true,
	)
	alloc := MakeAllocator(sp, func(string) (time.Duration, bool) {
		return 0, false
	})

	var wg sync.WaitGroup
	g.RegisterCallback(gossip.MakePrefixPattern(gossip.KeyStorePrefix),
		func(_ string, _ roachpb.Value) { wg.Done() },
		// Redundant callbacks are required by this test.
		gossip.Redundant)

	// Initialize testStores.
	var testStores [nodes]testStore
	for i := 0; i < len(testStores); i++ {
		testStores[i].immediateCompaction = true
		testStores[i].StoreID = roachpb.StoreID(i)
		testStores[i].Node = roachpb.NodeDescriptor{NodeID: roachpb.NodeID(i)}
		testStores[i].Capacity = roachpb.StoreCapacity{Capacity: 1 << 30, Available: 1 << 30}
	}
	// Initialize the cluster with a single range.
	testStores[0].add(alloc.randGen.Int63n(1 << 20))

	table := tablewriter.NewWriter(os.Stdout)
	table.SetAutoFormatHeaders(false)
	table.SetAlignment(tablewriter.ALIGN_RIGHT)

	header := make([]string, len(testStores)+1)
	header[0] = "gen"
	for i := 0; i < len(testStores); i++ {
		header[i+1] = fmt.Sprintf("store %d", i)
	}
	table.SetHeader(header)

	for i := 0; i < generations; i++ {
		// First loop through test stores and add data.
		wg.Add(len(testStores))
		for j := 0; j < len(testStores); j++ {
			// Add a pretend range to the testStore if there's already one.
			if testStores[j].Capacity.RangeCount > 0 {
				testStores[j].add(alloc.randGen.Int63n(1 << 20))
			}
			if err := g.AddInfoProto(gossip.MakeStoreKey(roachpb.StoreID(j)), &testStores[j].StoreDescriptor, 0); err != nil {
				panic(err)
			}
		}
		wg.Wait()

		// Next loop through test stores and maybe rebalance.
		for j := 0; j < len(testStores); j++ {
			ts := &testStores[j]
			var rangeUsageInfo RangeUsageInfo
			target, _, details, ok := alloc.RebalanceVoter(ctx, zonepb.EmptyCompleteZoneConfig(), nil, []roachpb.ReplicaDescriptor{{NodeID: ts.Node.NodeID, StoreID: ts.StoreID}}, nil, rangeUsageInfo, storeFilterThrottled)
			if ok {
				log.Infof(ctx, "rebalancing to %v; details: %s", target, details)
				testStores[j].rebalance(&testStores[int(target.StoreID)], alloc.randGen.Int63n(1<<20))
			}
		}

		if i%(generations/printGenerations) == 0 {
			var totalBytes int64
			for j := 0; j < len(testStores); j++ {
				totalBytes += testStores[j].Capacity.Capacity - testStores[j].Capacity.Available
			}
			row := make([]string, len(testStores)+1)
			row[0] = fmt.Sprintf("%d", i)
			for j := 0; j < len(testStores); j++ {
				ts := testStores[j]
				bytes := ts.Capacity.Capacity - ts.Capacity.Available
				row[j+1] = fmt.Sprintf("%3d %3d%%", ts.Capacity.RangeCount, (100*bytes)/totalBytes)
			}
			table.Append(row)
		}
	}

	var totBytes int64
	var totRanges int32
	for i := 0; i < len(testStores); i++ {
		totBytes += testStores[i].Capacity.Capacity - testStores[i].Capacity.Available
		totRanges += testStores[i].Capacity.RangeCount
	}
	table.Render()
	fmt.Printf("Total bytes=%d, ranges=%d\n", totBytes, totRanges)

	// Output:
	// +-----+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+
	// | gen | store 0  | store 1  | store 2  | store 3  | store 4  | store 5  | store 6  | store 7  | store 8  | store 9  | store 10 | store 11 | store 12 | store 13 | store 14 | store 15 | store 16 | store 17 | store 18 | store 19 |
	// +-----+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+
	// |   0 |   2 100% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |
	// |   2 |   4 100% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |
	// |   4 |   6 100% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |
	// |   6 |   8 100% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |
	// |   8 |  10 100% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |
	// |  10 |  10  68% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   1  11% |   0   0% |   2  20% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |
	// |  12 |  10  34% |   1   2% |   0   0% |   2  18% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   3  14% |   0   0% |   3  27% |   0   0% |   0   0% |   1   2% |   0   0% |   0   0% |   0   0% |
	// |  14 |  10  22% |   3   7% |   0   0% |   4  18% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   1   3% |   0   0% |   4  11% |   2   1% |   4  22% |   0   0% |   0   0% |   3   8% |   1   0% |   1   4% |   0   0% |
	// |  16 |  10  12% |   5  10% |   0   0% |   5  13% |   0   0% |   2   4% |   1   3% |   2   1% |   0   0% |   3   6% |   1   0% |   5   8% |   4   5% |   5  12% |   0   0% |   0   0% |   5   9% |   3   4% |   3   8% |   0   0% |
	// |  18 |  10   5% |   6   7% |   3   1% |   6   9% |   3   4% |   4   6% |   3   5% |   4   2% |   2   1% |   4   5% |   3   2% |   6   7% |   5   3% |   6  10% |   3   2% |   4   3% |   6   6% |   4   5% |   4   6% |   2   1% |
	// |  20 |  10   4% |   8   6% |   5   2% |   8   8% |   5   4% |   6   6% |   5   5% |   6   3% |   5   3% |   6   6% |   5   2% |   8   6% |   7   4% |   8   9% |   5   3% |   6   4% |   8   6% |   6   3% |   6   5% |   5   3% |
	// |  22 |  11   4% |  10   6% |   7   2% |  10   7% |   7   4% |   8   6% |   7   5% |   8   4% |   7   3% |   8   5% |   8   2% |  10   6% |   9   4% |  10   8% |   7   3% |   8   3% |  10   6% |   8   3% |   8   5% |   7   4% |
	// |  24 |  13   5% |  12   6% |   9   2% |  12   6% |   9   4% |  10   5% |   9   5% |  10   4% |   9   4% |  10   5% |  10   3% |  12   6% |  11   4% |  12   7% |   9   3% |  10   3% |  12   6% |  10   3% |  10   5% |   9   4% |
	// |  26 |  15   5% |  14   6% |  11   3% |  14   6% |  11   4% |  12   5% |  11   4% |  12   4% |  11   4% |  12   5% |  12   3% |  14   6% |  13   4% |  14   6% |  11   4% |  12   3% |  14   6% |  12   3% |  12   5% |  11   4% |
	// |  28 |  17   5% |  16   5% |  13   4% |  16   6% |  13   4% |  14   5% |  13   4% |  14   4% |  13   4% |  14   5% |  14   3% |  16   5% |  15   5% |  16   6% |  13   4% |  14   3% |  16   5% |  14   4% |  14   4% |  13   4% |
	// |  30 |  19   5% |  18   6% |  15   3% |  18   6% |  15   4% |  16   5% |  15   4% |  16   4% |  15   4% |  16   5% |  16   3% |  18   5% |  17   5% |  18   5% |  15   4% |  16   3% |  18   6% |  16   4% |  16   5% |  15   4% |
	// |  32 |  21   4% |  20   5% |  17   4% |  20   6% |  17   4% |  18   6% |  17   5% |  18   4% |  17   4% |  18   5% |  18   3% |  20   5% |  19   4% |  20   6% |  17   4% |  18   3% |  20   6% |  18   4% |  18   4% |  17   4% |
	// |  34 |  23   4% |  22   6% |  19   4% |  22   6% |  19   4% |  20   5% |  19   5% |  20   4% |  19   4% |  20   4% |  20   4% |  22   5% |  21   4% |  22   5% |  19   4% |  20   3% |  22   6% |  20   3% |  20   4% |  19   5% |
	// |  36 |  25   4% |  24   5% |  21   4% |  24   7% |  21   4% |  22   5% |  21   5% |  22   4% |  21   4% |  22   4% |  22   4% |  24   5% |  23   4% |  24   5% |  21   4% |  22   4% |  24   6% |  22   4% |  22   4% |  21   5% |
	// |  38 |  27   4% |  26   5% |  23   4% |  26   6% |  23   4% |  24   5% |  23   5% |  24   4% |  23   4% |  24   4% |  24   4% |  26   5% |  25   5% |  26   5% |  23   4% |  24   4% |  26   6% |  24   4% |  24   5% |  23   5% |
	// |  40 |  29   4% |  28   5% |  25   4% |  28   6% |  25   4% |  26   5% |  25   5% |  26   4% |  25   4% |  26   4% |  26   4% |  28   5% |  27   4% |  28   5% |  25   4% |  26   4% |  28   5% |  26   4% |  26   4% |  25   5% |
	// |  42 |  31   4% |  30   5% |  27   4% |  30   6% |  27   4% |  28   5% |  27   5% |  28   4% |  27   4% |  28   4% |  28   4% |  30   5% |  29   4% |  30   5% |  27   4% |  28   4% |  30   5% |  28   4% |  28   4% |  27   5% |
	// |  44 |  33   4% |  32   5% |  29   4% |  32   6% |  29   4% |  30   5% |  29   5% |  30   3% |  29   4% |  30   4% |  30   4% |  32   5% |  31   4% |  32   6% |  29   4% |  30   4% |  32   5% |  30   4% |  30   4% |  29   5% |
	// |  46 |  35   4% |  34   5% |  31   4% |  34   6% |  31   4% |  32   5% |  31   5% |  32   3% |  31   4% |  32   5% |  32   4% |  34   5% |  33   4% |  34   6% |  31   4% |  32   4% |  34   5% |  32   4% |  32   4% |  31   5% |
	// |  48 |  37   4% |  36   4% |  33   4% |  36   5% |  33   4% |  34   5% |  33   5% |  34   3% |  33   4% |  34   5% |  34   4% |  36   5% |  35   4% |  36   6% |  33   4% |  34   4% |  36   5% |  34   4% |  34   5% |  33   5% |
	// |  50 |  39   4% |  38   4% |  35   4% |  38   5% |  35   4% |  36   5% |  35   5% |  36   3% |  35   4% |  36   5% |  36   4% |  38   5% |  37   4% |  38   5% |  35   4% |  36   4% |  38   5% |  36   4% |  36   5% |  35   5% |
	// |  52 |  41   4% |  40   5% |  37   4% |  40   5% |  37   4% |  38   5% |  37   5% |  38   3% |  37   4% |  38   5% |  38   4% |  40   5% |  39   4% |  40   5% |  37   5% |  38   4% |  40   5% |  38   4% |  38   5% |  37   5% |
	// |  54 |  43   4% |  42   5% |  39   4% |  42   5% |  39   4% |  40   5% |  39   5% |  40   3% |  39   4% |  40   5% |  40   4% |  42   5% |  41   4% |  42   5% |  39   5% |  40   4% |  42   5% |  40   4% |  40   5% |  39   5% |
	// |  56 |  45   4% |  44   5% |  41   4% |  44   5% |  41   4% |  42   5% |  41   5% |  42   4% |  41   4% |  42   5% |  42   4% |  44   5% |  43   4% |  44   5% |  41   5% |  42   4% |  44   5% |  42   4% |  42   5% |  41   5% |
	// |  58 |  47   4% |  46   5% |  43   4% |  46   5% |  43   4% |  44   5% |  43   5% |  44   4% |  43   4% |  44   5% |  44   4% |  46   5% |  45   5% |  46   5% |  43   5% |  44   4% |  46   5% |  44   4% |  44   5% |  43   5% |
	// |  60 |  49   4% |  48   5% |  45   3% |  48   5% |  45   4% |  46   5% |  45   5% |  46   4% |  45   4% |  46   5% |  46   4% |  48   5% |  47   5% |  48   5% |  45   5% |  46   4% |  48   5% |  46   5% |  46   4% |  45   5% |
	// |  62 |  51   4% |  50   5% |  47   4% |  50   5% |  47   4% |  48   5% |  47   5% |  48   4% |  47   4% |  48   5% |  48   5% |  50   5% |  49   5% |  50   5% |  47   5% |  48   4% |  50   5% |  48   5% |  48   4% |  47   5% |
	// |  64 |  53   4% |  52   5% |  49   3% |  52   5% |  49   4% |  50   5% |  49   5% |  50   4% |  49   4% |  50   5% |  50   5% |  52   5% |  51   5% |  52   5% |  49   5% |  50   4% |  52   5% |  50   4% |  50   4% |  49   5% |
	// |  66 |  55   4% |  54   5% |  51   4% |  54   5% |  51   4% |  52   5% |  51   5% |  52   4% |  51   4% |  52   5% |  52   5% |  54   5% |  53   5% |  54   5% |  51   5% |  52   5% |  54   5% |  52   4% |  52   4% |  51   5% |
	// |  68 |  57   4% |  56   5% |  53   4% |  56   5% |  53   4% |  54   5% |  53   5% |  54   4% |  53   4% |  54   5% |  54   5% |  56   5% |  55   5% |  56   5% |  53   5% |  54   5% |  56   5% |  54   4% |  54   4% |  53   5% |
	// |  70 |  59   4% |  58   5% |  55   4% |  58   5% |  55   4% |  56   5% |  55   5% |  56   4% |  55   4% |  56   5% |  56   4% |  58   5% |  57   5% |  58   5% |  55   5% |  56   5% |  58   5% |  56   4% |  56   4% |  55   5% |
	// |  72 |  61   4% |  60   5% |  57   4% |  60   5% |  57   4% |  58   5% |  57   5% |  58   4% |  57   4% |  58   5% |  58   4% |  60   5% |  59   5% |  60   5% |  57   4% |  58   5% |  60   5% |  58   5% |  58   4% |  57   5% |
	// |  74 |  63   4% |  62   5% |  59   4% |  62   5% |  59   4% |  60   5% |  59   5% |  60   4% |  59   4% |  60   5% |  60   4% |  62   5% |  61   5% |  62   5% |  59   4% |  60   5% |  62   5% |  60   5% |  60   4% |  59   5% |
	// |  76 |  65   4% |  64   5% |  61   4% |  64   5% |  61   4% |  62   5% |  61   5% |  62   4% |  61   4% |  62   5% |  62   4% |  64   5% |  63   5% |  64   5% |  61   5% |  62   5% |  64   4% |  62   5% |  62   4% |  61   5% |
	// |  78 |  67   4% |  66   5% |  63   4% |  66   5% |  63   4% |  64   5% |  63   5% |  64   4% |  63   4% |  64   5% |  64   4% |  66   5% |  65   5% |  66   5% |  63   4% |  64   5% |  66   5% |  64   5% |  64   4% |  63   5% |
	// |  80 |  69   4% |  68   5% |  65   4% |  68   5% |  65   4% |  66   5% |  65   5% |  66   4% |  65   4% |  66   5% |  66   4% |  68   5% |  67   4% |  68   5% |  65   4% |  66   5% |  68   5% |  66   5% |  66   4% |  65   5% |
	// |  82 |  71   4% |  70   5% |  67   4% |  70   5% |  67   4% |  68   5% |  67   5% |  68   4% |  67   4% |  68   5% |  68   4% |  70   5% |  69   5% |  70   5% |  67   4% |  68   4% |  70   5% |  68   4% |  68   4% |  67   5% |
	// |  84 |  73   4% |  72   5% |  69   4% |  72   5% |  69   4% |  70   5% |  69   5% |  70   4% |  69   4% |  70   5% |  70   4% |  72   5% |  71   4% |  72   5% |  69   4% |  70   4% |  72   5% |  70   4% |  70   4% |  69   5% |
	// |  86 |  75   4% |  74   5% |  71   4% |  74   5% |  71   4% |  72   5% |  71   5% |  72   4% |  71   4% |  72   5% |  72   4% |  74   5% |  73   4% |  74   5% |  71   4% |  72   4% |  74   5% |  72   4% |  72   4% |  71   5% |
	// |  88 |  77   4% |  76   4% |  73   4% |  76   5% |  73   4% |  74   5% |  73   5% |  74   4% |  73   4% |  74   5% |  74   4% |  76   5% |  75   4% |  76   5% |  73   4% |  74   4% |  76   5% |  74   4% |  74   4% |  73   5% |
	// |  90 |  79   4% |  78   4% |  75   4% |  78   5% |  75   4% |  76   5% |  75   5% |  76   4% |  75   4% |  76   5% |  76   4% |  78   5% |  77   5% |  78   5% |  75   5% |  76   4% |  78   5% |  76   4% |  76   4% |  75   5% |
	// |  92 |  81   4% |  80   4% |  77   4% |  80   5% |  77   4% |  78   5% |  77   5% |  78   4% |  77   4% |  78   5% |  78   4% |  80   5% |  79   5% |  80   5% |  77   5% |  78   4% |  80   5% |  78   4% |  78   4% |  77   5% |
	// |  94 |  83   4% |  82   4% |  79   4% |  82   5% |  79   4% |  80   5% |  79   5% |  80   4% |  79   4% |  80   5% |  80   4% |  82   5% |  81   5% |  82   5% |  79   4% |  80   4% |  82   5% |  80   4% |  80   4% |  79   5% |
	// |  96 |  85   4% |  84   4% |  81   4% |  84   5% |  81   5% |  82   5% |  81   5% |  82   4% |  81   4% |  82   5% |  82   4% |  84   5% |  83   5% |  84   5% |  81   5% |  82   4% |  84   5% |  82   4% |  82   4% |  81   5% |
	// |  98 |  87   4% |  86   4% |  83   4% |  86   5% |  83   5% |  84   5% |  83   5% |  84   4% |  83   4% |  84   5% |  84   4% |  86   5% |  85   5% |  86   5% |  83   5% |  84   4% |  86   5% |  84   4% |  84   4% |  83   5% |
	// +-----+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+
	// Total bytes=894061338, ranges=1708
}
