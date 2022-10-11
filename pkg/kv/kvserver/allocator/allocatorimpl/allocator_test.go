// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package allocatorimpl

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
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/storepool"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/constraint"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/replicastats"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
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

var oneStoreHighReadAmp = []*roachpb.StoreDescriptor{
	{
		StoreID:  1,
		Node:     roachpb.NodeDescriptor{NodeID: 1},
		Capacity: roachpb.StoreCapacity{Capacity: 200, Available: 200, RangeCount: 600, L0Sublevels: MaxL0SublevelThreshold - 5},
	},
	{
		StoreID:  2,
		Node:     roachpb.NodeDescriptor{NodeID: 2},
		Capacity: roachpb.StoreCapacity{Capacity: 200, Available: 200, RangeCount: 1800, L0Sublevels: MaxL0SublevelThreshold - 5},
	},
	{
		StoreID:  3,
		Node:     roachpb.NodeDescriptor{NodeID: 3},
		Capacity: roachpb.StoreCapacity{Capacity: 200, Available: 200, RangeCount: 600, L0Sublevels: MaxL0SublevelThreshold + 5},
	},
	{
		StoreID:  4,
		Node:     roachpb.NodeDescriptor{NodeID: 4},
		Capacity: roachpb.StoreCapacity{Capacity: 200, Available: 200, RangeCount: 1200, L0Sublevels: MaxL0SublevelThreshold - 5},
	},
}

var allStoresHighReadAmp = []*roachpb.StoreDescriptor{
	{
		StoreID:  1,
		Node:     roachpb.NodeDescriptor{NodeID: 1},
		Capacity: roachpb.StoreCapacity{Capacity: 200, Available: 200, RangeCount: 1200, L0Sublevels: MaxL0SublevelThreshold + 1},
	},
	{
		StoreID:  2,
		Node:     roachpb.NodeDescriptor{NodeID: 2},
		Capacity: roachpb.StoreCapacity{Capacity: 200, Available: 200, RangeCount: 800, L0Sublevels: MaxL0SublevelThreshold + 1},
	},
	{
		StoreID:  3,
		Node:     roachpb.NodeDescriptor{NodeID: 3},
		Capacity: roachpb.StoreCapacity{Capacity: 200, Available: 200, RangeCount: 600, L0Sublevels: MaxL0SublevelThreshold + 1},
	},
}

var allStoresHighReadAmpSkewed = []*roachpb.StoreDescriptor{
	{
		StoreID:  1,
		Node:     roachpb.NodeDescriptor{NodeID: 1},
		Capacity: roachpb.StoreCapacity{Capacity: 200, Available: 200, RangeCount: 1200, L0Sublevels: MaxL0SublevelThreshold + 1},
	},
	{
		StoreID:  2,
		Node:     roachpb.NodeDescriptor{NodeID: 2},
		Capacity: roachpb.StoreCapacity{Capacity: 200, Available: 200, RangeCount: 800, L0Sublevels: MaxL0SublevelThreshold + 50},
	},
	{
		StoreID:  3,
		Node:     roachpb.NodeDescriptor{NodeID: 3},
		Capacity: roachpb.StoreCapacity{Capacity: 200, Available: 200, RangeCount: 600, L0Sublevels: MaxL0SublevelThreshold + 55},
	},
}

var threeStoresHighReadAmpAscRangeCount = []*roachpb.StoreDescriptor{
	{
		StoreID:  1,
		Node:     roachpb.NodeDescriptor{NodeID: 1},
		Capacity: roachpb.StoreCapacity{Capacity: 200, Available: 200, RangeCount: 100, L0Sublevels: MaxL0SublevelThreshold + 10},
	},
	{
		StoreID:  2,
		Node:     roachpb.NodeDescriptor{NodeID: 2},
		Capacity: roachpb.StoreCapacity{Capacity: 200, Available: 200, RangeCount: 400, L0Sublevels: MaxL0SublevelThreshold + 10},
	},
	{
		StoreID:  3,
		Node:     roachpb.NodeDescriptor{NodeID: 3},
		Capacity: roachpb.StoreCapacity{Capacity: 200, Available: 200, RangeCount: 1600, L0Sublevels: MaxL0SublevelThreshold + 10},
	},
	{
		StoreID:  4,
		Node:     roachpb.NodeDescriptor{NodeID: 4},
		Capacity: roachpb.StoreCapacity{Capacity: 200, Available: 200, RangeCount: 6400, L0Sublevels: MaxL0SublevelThreshold - 10},
	},
	{
		StoreID:  5,
		Node:     roachpb.NodeDescriptor{NodeID: 5},
		Capacity: roachpb.StoreCapacity{Capacity: 200, Available: 200, RangeCount: 25000, L0Sublevels: MaxL0SublevelThreshold - 10},
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
	storePool *storepool.StorePool,
	aliveStoreIDs []roachpb.StoreID,
	unavailableStoreIDs []roachpb.StoreID,
	deadStoreIDs []roachpb.StoreID,
	decommissioningStoreIDs []roachpb.StoreID,
	decommissionedStoreIDs []roachpb.StoreID,
	suspectedStoreIDs []roachpb.StoreID,
) {
	storePool.DetailsMu.Lock()
	defer storePool.DetailsMu.Unlock()

	liveNodeSet := map[roachpb.NodeID]livenesspb.NodeLivenessStatus{}
	storePool.DetailsMu.StoreDetails = map[roachpb.StoreID]*storepool.StoreDetail{}
	for _, storeID := range aliveStoreIDs {
		liveNodeSet[roachpb.NodeID(storeID)] = livenesspb.NodeLivenessStatus_LIVE
		detail := storePool.GetStoreDetailLocked(storeID)
		detail.Desc = &roachpb.StoreDescriptor{
			StoreID: storeID,
			Node:    roachpb.NodeDescriptor{NodeID: roachpb.NodeID(storeID)},
		}
	}
	for _, storeID := range unavailableStoreIDs {
		liveNodeSet[roachpb.NodeID(storeID)] = livenesspb.NodeLivenessStatus_UNAVAILABLE
		detail := storePool.GetStoreDetailLocked(storeID)
		detail.Desc = &roachpb.StoreDescriptor{
			StoreID: storeID,
			Node:    roachpb.NodeDescriptor{NodeID: roachpb.NodeID(storeID)},
		}
	}
	for _, storeID := range deadStoreIDs {
		liveNodeSet[roachpb.NodeID(storeID)] = livenesspb.NodeLivenessStatus_DEAD
		detail := storePool.GetStoreDetailLocked(storeID)
		detail.Desc = &roachpb.StoreDescriptor{
			StoreID: storeID,
			Node:    roachpb.NodeDescriptor{NodeID: roachpb.NodeID(storeID)},
		}
	}
	for _, storeID := range decommissioningStoreIDs {
		liveNodeSet[roachpb.NodeID(storeID)] = livenesspb.NodeLivenessStatus_DECOMMISSIONING
		detail := storePool.GetStoreDetailLocked(storeID)
		detail.Desc = &roachpb.StoreDescriptor{
			StoreID: storeID,
			Node:    roachpb.NodeDescriptor{NodeID: roachpb.NodeID(storeID)},
		}
	}
	for _, storeID := range decommissionedStoreIDs {
		liveNodeSet[roachpb.NodeID(storeID)] = livenesspb.NodeLivenessStatus_DECOMMISSIONED
		detail := storePool.GetStoreDetailLocked(storeID)
		detail.Desc = &roachpb.StoreDescriptor{
			StoreID: storeID,
			Node:    roachpb.NodeDescriptor{NodeID: roachpb.NodeID(storeID)},
		}
	}

	for _, storeID := range suspectedStoreIDs {
		liveNodeSet[roachpb.NodeID(storeID)] = livenesspb.NodeLivenessStatus_LIVE
		detail := storePool.GetStoreDetailLocked(storeID)
		detail.LastAvailable = storePool.Clock.Now().GoTime()
		detail.LastUnavailable = storePool.Clock.Now().GoTime()
		detail.Desc = &roachpb.StoreDescriptor{
			StoreID: storeID,
			Node:    roachpb.NodeDescriptor{NodeID: roachpb.NodeID(storeID)},
		}
	}

	// Set the node liveness function using the set we constructed.
	storePool.NodeLivenessFn =
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
	stopper, g, _, a, _ := CreateTestAllocator(ctx, 1, false /* deterministic */)
	defer stopper.Stop(ctx)
	gossiputil.NewStoreGossiper(g).GossipStores(singleStore, t)
	result, _, err := a.AllocateVoter(
		ctx,
		simpleSpanConfig,
		nil /* existingVoters */, nil, /* existingNonVoters */
		Dead,
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
	stopper, _, _, a, _ := CreateTestAllocator(ctx, 1, false /* deterministic */)
	defer stopper.Stop(ctx)
	result, _, err := a.AllocateVoter(
		ctx,
		simpleSpanConfig,
		nil /* existingVoters */, nil, /* existingNonVoters */
		Dead,
	)
	if !roachpb.Empty(result) {
		t.Errorf("expected nil result: %+v", result)
	}
	if err == nil {
		t.Errorf("allocation succeeded despite there being no available disks: %v", result)
	}
}

func TestAllocatorReadAmpCheck(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	type testCase struct {
		name   string
		stores []*roachpb.StoreDescriptor
		conf   roachpb.SpanConfig
		// The expected store to add when replicas are alive. The allocator should
		// pick one of the best stores, with low range count.
		expectedTargetIfAlive roachpb.StoreID
		// The expected store to add when a replica is dead or decommissioning. The
		// allocator should pick a store that is good enough, ignoring the range
		// count.
		expectedTargetIfDead roachpb.StoreID
		enforcement          StoreHealthEnforcement
	}
	tests := []testCase{
		{
			name:   "ignore read amp on allocation when StoreHealthNoAction enforcement",
			stores: allStoresHighReadAmp,
			conf:   emptySpanConfig(),
			// NB: All stores have high read amp, this should be ignored and
			// allocate to the store with the lowest range count.
			expectedTargetIfAlive: roachpb.StoreID(3),
			// Recovery of a dead node can pick any valid store, not necessarily the
			// one with the lowest range count.
			expectedTargetIfDead: roachpb.StoreID(2),
			enforcement:          StoreHealthNoAction,
		},
		{
			name: "ignore read amp on allocation when storeHealthLogOnly enforcement",
			// NB: All stores have high read amp, this should be ignored and
			// allocate to the store with the lowest range count.
			stores:                allStoresHighReadAmp,
			conf:                  emptySpanConfig(),
			expectedTargetIfAlive: roachpb.StoreID(3),
			// Recovery of a dead node can pick any valid store, not necessarily the
			// one with the lowest range count.
			expectedTargetIfDead: roachpb.StoreID(2),
			enforcement:          StoreHealthLogOnly,
		},
		{
			name: "ignore read amp on allocation when StoreHealthBlockRebalanceTo enforcement",
			// NB: All stores have high read amp, this should be ignored and
			// allocate to the store with the lowest range count.
			stores:                allStoresHighReadAmp,
			conf:                  emptySpanConfig(),
			expectedTargetIfAlive: roachpb.StoreID(3),
			// Recovery of a dead node can pick any valid store, not necessarily the
			// one with the lowest range count.
			expectedTargetIfDead: roachpb.StoreID(2),
			enforcement:          StoreHealthBlockRebalanceTo,
		},
		{
			name: "don't allocate to stores when all have high read amp and StoreHealthBlockAll",
			// NB: All stores have high read amp (limit + 1), none are above the watermark, select the lowest range count.
			stores:                allStoresHighReadAmp,
			conf:                  emptySpanConfig(),
			expectedTargetIfAlive: roachpb.StoreID(3),
			// Recovery of a dead node can pick any valid store, not necessarily the
			// one with the lowest range count.
			expectedTargetIfDead: roachpb.StoreID(2),
			enforcement:          StoreHealthBlockAll,
		},
		{
			name: "allocate to store below the mean when all have high read amp and StoreHealthBlockAll",
			// NB: All stores have high read amp, however store 1 is below the watermark mean read amp.
			stores:                allStoresHighReadAmpSkewed,
			conf:                  emptySpanConfig(),
			expectedTargetIfAlive: roachpb.StoreID(1),
			expectedTargetIfDead:  roachpb.StoreID(1),
			enforcement:           StoreHealthBlockAll,
		},
		{
			name: "allocate to lowest range count store without high read amp when StoreHealthBlockAll enforcement",
			// NB: Store 1, 2 and 3 have high read amp and are above the watermark, the lowest range count (4)
			// should be selected.
			stores:                threeStoresHighReadAmpAscRangeCount,
			conf:                  emptySpanConfig(),
			expectedTargetIfAlive: roachpb.StoreID(4),
			expectedTargetIfDead:  roachpb.StoreID(4),
			enforcement:           StoreHealthBlockAll,
		},
	}

	chk := func(target roachpb.ReplicationTarget, expectedTarget roachpb.StoreID) bool {
		return target.StoreID == expectedTarget
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("%d_%s", i+1, test.name), func(t *testing.T) {
			stopper, g, _, a, _ := CreateTestAllocator(ctx, 10, true /* deterministic */)
			defer stopper.Stop(ctx)
			sg := gossiputil.NewStoreGossiper(g)
			sg.GossipStores(test.stores, t)

			// Enable read disk health checking in candidate exclusion.
			l0SublevelsThresholdEnforce.Override(ctx, &a.StorePool.St.SV, int64(test.enforcement))

			// Allocate a voter where all replicas are alive (e.g. up-replicating a valid range).
			add, _, err := a.AllocateVoter(
				ctx,
				test.conf,
				nil,
				nil,
				Alive,
			)
			require.NoError(t, err)
			require.Truef(t,
				chk(add, test.expectedTargetIfAlive),
				"the addition target %+v from AllocateVoter doesn't match expectation",
				add)

			// Allocate a voter where we have a dead (or decommissioning) replica.
			add, _, err = a.AllocateVoter(
				ctx,
				test.conf,
				nil,
				nil,
				// Dead and Decommissioning should behave the same here, use either.
				func() ReplicaStatus {
					if i%2 == 0 {
						return Dead
					}
					return Decommissioning
				}(),
			)
			require.NoError(t, err)
			require.Truef(t,
				chk(add, test.expectedTargetIfDead),
				"the addition target %+v from AllocateVoter doesn't match expectation",
				add)
		})
	}
}

func TestAllocatorTwoDatacenters(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper, g, _, a, _ := CreateTestAllocator(ctx, 1, false /* deterministic */)
	defer stopper.Stop(ctx)
	gossiputil.NewStoreGossiper(g).GossipStores(multiDCStores, t)
	result1, _, err := a.AllocateVoter(
		ctx,
		multiDCConfigSSD,
		nil /* existingVoters */, nil, /* existingNonVoters */
		Dead,
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
		Dead,
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
		Dead,
	)
	if err == nil {
		t.Errorf("expected error on allocation without available stores: %+v", result3)
	}
}

func TestAllocatorExistingReplica(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper, g, _, a, _ := CreateTestAllocator(ctx, 1, false /* deterministic */)
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
		Dead,
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
	stopper, g, _, a, _ := CreateTestAllocator(ctx, 10, false /* deterministic */)
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
				Dead,
			)
			if e, a := tc.expectTargetAllocate, !roachpb.Empty(result); e != a {
				t.Errorf(
					"AllocateVoter(%v) got target %v, err %v; expectTarget=%v",
					tc.existing, result, err, tc.expectTargetAllocate,
				)
			}
		}

		{
			var rangeUsageInfo allocator.RangeUsageInfo
			target, _, details, ok := a.RebalanceVoter(
				ctx,
				emptySpanConfig(),
				nil,
				tc.existing,
				nil,
				rangeUsageInfo,
				storepool.StoreFilterThrottled,
				a.ScorerOptions(ctx),
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
	stopper, g, _, a, _ := CreateTestAllocator(ctx, 10, false /* deterministic */)
	defer stopper.Stop(ctx)
	storeGossiper := gossiputil.NewStoreGossiper(g)
	storeGossiper.GossipStores(stores, t)

	// We run through all the ranges once to get the cluster to balance.
	// After that we should not be seeing replicas move.
	var rangeUsageInfo allocator.RangeUsageInfo
	for i := 1; i < 40; i++ {
		add, remove, _, ok := a.RebalanceVoter(
			ctx,
			emptySpanConfig(),
			nil,
			ranges[i].InternalReplicas,
			nil,
			rangeUsageInfo,
			storepool.StoreFilterThrottled,
			a.ScorerOptions(ctx),
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
			storepool.StoreFilterThrottled,
			a.ScorerOptions(ctx),
		)
		require.False(t, ok)
	}
}

// TestAllocatorRebalance verifies that rebalance targets are chosen
// randomly from amongst stores under the MaxFractionUsedThreshold.
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
				Available:  100 - int64(100*float64(allocator.MaxFractionUsedThreshold)),
				RangeCount: 5,
			},
		},
		{
			// This store must not be rebalanced to, because it's too full.
			StoreID: 4,
			Node:    roachpb.NodeDescriptor{NodeID: 4},
			Capacity: roachpb.StoreCapacity{
				Capacity:   100,
				Available:  (100 - int64(100*float64(allocator.MaxFractionUsedThreshold))) / 2,
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
	stopper, g, _, a, _ := CreateTestAllocator(ctx, 10, false /* deterministic */)
	defer stopper.Stop(ctx)

	gossiputil.NewStoreGossiper(g).GossipStores(stores, t)

	// Every rebalance target must be either store 1 or 2.
	for i := 0; i < 10; i++ {
		var rangeUsageInfo allocator.RangeUsageInfo
		target, _, _, ok := a.RebalanceVoter(
			ctx,
			emptySpanConfig(),
			nil,
			[]roachpb.ReplicaDescriptor{{NodeID: 3, StoreID: 3}},
			nil,
			rangeUsageInfo,
			storepool.StoreFilterThrottled,
			a.ScorerOptions(ctx),
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

	sl, _, _ := a.StorePool.GetStoreList(storepool.StoreFilterThrottled)
	eqClass := equivalenceClass{
		candidateSL: sl,
	}
	// Verify shouldRebalanceBasedOnThresholds results.
	for i, store := range stores {
		desc, ok := a.StorePool.GetStoreDescriptor(store.StoreID)
		if !ok {
			t.Fatalf("%d: unable to get store %d descriptor", i, store.StoreID)
		}
		eqClass.existing = desc
		result := a.ScorerOptions(ctx).shouldRebalanceBasedOnThresholds(
			ctx,
			eqClass,
			a.Metrics,
		)
		if expResult := (i >= 2); expResult != result {
			t.Errorf("%d: expected rebalance %t; got %t; desc %+v; sl: %+v", i, expResult, result, desc, sl)
		}
	}
}

func TestAllocatorRebalanceDeadNodes(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper, _, sp, a, _ := CreateTestAllocator(ctx, 8, false /* deterministic */)
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
	sp.DetailsMu.Lock()
	sp.GetStoreDetailLocked(1).Desc.Capacity = ranges(100)
	sp.GetStoreDetailLocked(2).Desc.Capacity = ranges(100)
	sp.GetStoreDetailLocked(3).Desc.Capacity = ranges(100)
	sp.GetStoreDetailLocked(4).Desc.Capacity = ranges(100)
	sp.GetStoreDetailLocked(5).Desc.Capacity = ranges(100)
	sp.GetStoreDetailLocked(6).Desc.Capacity = ranges(0)
	sp.GetStoreDetailLocked(7).Desc.Capacity = ranges(100)
	sp.GetStoreDetailLocked(8).Desc.Capacity = ranges(100)
	sp.DetailsMu.Unlock()

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
			var rangeUsageInfo allocator.RangeUsageInfo
			target, _, _, ok := a.RebalanceVoter(
				ctx,
				emptySpanConfig(),
				nil,
				c.existing,
				nil,
				rangeUsageInfo,
				storepool.StoreFilterThrottled,
				a.ScorerOptions(ctx),
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
			surplus := int32(math.Ceil(float64(mean)*RangeRebalanceThreshold.Get(&st.SV) + 1))
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
			deficit := int32(math.Ceil(float64(mean)*RangeRebalanceThreshold.Get(&st.SV) + 1))
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
			stopper, g, _, a, _ := CreateTestAllocator(ctx, 1, true /* deterministic */)
			defer stopper.Stop(ctx)

			st := a.StorePool.St
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
				sl, _, _ := a.StorePool.GetStoreList(storepool.StoreFilterThrottled)
				for j, s := range sl.Stores {
					if a, e := s.Capacity.RangeCount, cluster[j].rangeCount; a != e {
						return errors.Errorf("range count for %d = %d != expected %d", j, a, e)
					}
				}
				return nil
			})
			sl, _, _ := a.StorePool.GetStoreList(storepool.StoreFilterThrottled)
			eqClass := equivalenceClass{
				candidateSL: sl,
			}
			// Verify shouldRebalanceBasedOnThresholds returns the expected value.
			for j, store := range stores {
				desc, ok := a.StorePool.GetStoreDescriptor(store.StoreID)
				if !ok {
					t.Fatalf("[store %d]: unable to get store %d descriptor", j, store.StoreID)
				}
				eqClass.existing = desc
				if a, e := a.ScorerOptions(ctx).shouldRebalanceBasedOnThresholds(
					context.Background(),
					eqClass,
					a.Metrics,
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
		stopper, g, _, a, _ := CreateTestAllocator(ctx, 10, false /* deterministic */)
		defer stopper.Stop(ctx)
		gossiputil.NewStoreGossiper(g).GossipStores(subtest.testStores, t)
		var rangeUsageInfo allocator.RangeUsageInfo
		options := &QPSScorerOptions{
			StoreHealthOptions:    StoreHealthOptions{EnforcementLevel: StoreHealthNoAction},
			QPSPerReplica:         100,
			QPSRebalanceThreshold: 0.2,
		}
		add, remove, _, ok := a.RebalanceVoter(
			ctx,
			emptySpanConfig(),
			nil,
			[]roachpb.ReplicaDescriptor{{StoreID: subtest.testStores[0].StoreID}},
			nil,
			rangeUsageInfo,
			storepool.StoreFilterThrottled,
			options,
		)
		if subtest.expectRebalance {
			require.True(t, ok)
			require.Equal(t, subtest.expectedAddTarget, add.StoreID)
			require.Equal(t, subtest.expectedRemoveTarget, remove.StoreID)
			// Verify shouldRebalanceBasedOnThresholds results.
			if desc, descOk := a.StorePool.GetStoreDescriptor(remove.StoreID); descOk {
				sl, _, _ := a.StorePool.GetStoreList(storepool.StoreFilterThrottled)
				eqClass := equivalenceClass{
					existing:    desc,
					candidateSL: sl,
				}
				result := options.shouldRebalanceBasedOnThresholds(
					ctx,
					eqClass,
					a.Metrics,
				)
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
		stopper, g, _, a, _ := CreateTestAllocator(ctx, 10, false /* deterministic */)
		defer stopper.Stop(ctx)
		gossiputil.NewStoreGossiper(g).GossipStores(subtest.testStores, t)
		options := &QPSScorerOptions{
			StoreHealthOptions:    StoreHealthOptions{EnforcementLevel: StoreHealthNoAction},
			QPSRebalanceThreshold: 0.1,
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
	stopper, g, _, a, _ := CreateTestAllocator(ctx, 10, false /* deterministic */)
	defer stopper.Stop(ctx)

	gossiputil.NewStoreGossiper(g).GossipStores(stores, t)

	// Every rebalance target must be store 4 (or nil for case of missing the only option).
	for i := 0; i < 10; i++ {
		var rangeUsageInfo allocator.RangeUsageInfo
		result, _, _, ok := a.RebalanceVoter(
			ctx,
			emptySpanConfig(),
			nil,
			[]roachpb.ReplicaDescriptor{{StoreID: stores[0].StoreID}},
			nil,
			rangeUsageInfo,
			storepool.StoreFilterThrottled,
			a.ScorerOptions(ctx),
		)
		if ok && result.StoreID != 4 {
			t.Errorf("expected store 4; got %d", result.StoreID)
		}
	}

	// Verify shouldRebalanceBasedOnThresholds results.
	for i, store := range stores {
		desc, ok := a.StorePool.GetStoreDescriptor(store.StoreID)
		if !ok {
			t.Fatalf("%d: unable to get store %d descriptor", i, store.StoreID)
		}
		sl, _, _ := a.StorePool.GetStoreList(storepool.StoreFilterThrottled)
		eqClass := equivalenceClass{
			existing:    desc,
			candidateSL: sl,
		}
		result := a.ScorerOptions(ctx).shouldRebalanceBasedOnThresholds(
			ctx,
			eqClass,
			a.Metrics,
		)
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
	raftStatus.RaftState = raft.StateLeader
	for i := int32(1); i <= r.replicationFactor; i++ {
		state := tracker.StateReplicate
		if _, ok := r.replsInNeedOfSnapshot[roachpb.ReplicaID(i)]; ok {
			state = tracker.StateSnapshot
		}
		raftStatus.Progress[uint64(i)] = tracker.Progress{State: state}
	}
	return raftStatus
}

func (r *mockRepl) GetFirstIndex() uint64 {
	return 0
}

func (r *mockRepl) StoreID() roachpb.StoreID {
	return r.storeID
}
func (r *mockRepl) Desc() *roachpb.RangeDescriptor {
	return &roachpb.RangeDescriptor{}
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
	stopper, g, _, a, _ := CreateTestAllocator(ctx, 10, true /* deterministic */)
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
		existing         []roachpb.ReplicaDescriptor
		leaseholder      roachpb.StoreID
		excludeLeaseRepl bool
		expected         roachpb.StoreID
	}{
		// No existing lease holder, nothing to do.
		{existing: existing, leaseholder: 0, excludeLeaseRepl: false, expected: 0},
		// Store 1 is not a lease transfer source.
		{existing: existing, leaseholder: 1, excludeLeaseRepl: false, expected: 0},
		{existing: existing, leaseholder: 1, excludeLeaseRepl: true, expected: 2},
		// Store 2 is not a lease transfer source.
		{existing: existing, leaseholder: 2, excludeLeaseRepl: false, expected: 0},
		{existing: existing, leaseholder: 2, excludeLeaseRepl: true, expected: 1},
		// Store 3 is a lease transfer source.
		{existing: existing, leaseholder: 3, excludeLeaseRepl: false, expected: 1},
		{existing: existing, leaseholder: 3, excludeLeaseRepl: true, expected: 1},
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
				allocator.TransferLeaseOptions{
					ExcludeLeaseRepl:       c.excludeLeaseRepl,
					CheckCandidateFullness: true,
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
	stopper, g, _, a, _ := CreateTestAllocator(ctx, 10, true /* deterministic */)
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
		excludeLeaseRepl  bool
		transferTarget    roachpb.StoreID
	}{
		{
			existing:          existing,
			replsNeedingSnaps: []roachpb.ReplicaID{1},
			leaseholder:       3,
			excludeLeaseRepl:  false,
			transferTarget:    0,
		},
		{
			existing:          existing,
			replsNeedingSnaps: []roachpb.ReplicaID{1},
			leaseholder:       3,
			excludeLeaseRepl:  true,
			transferTarget:    2,
		},
		{
			existing:          existing,
			replsNeedingSnaps: []roachpb.ReplicaID{1},
			leaseholder:       4,
			excludeLeaseRepl:  false,
			transferTarget:    2,
		},
		{
			existing:          existing,
			replsNeedingSnaps: []roachpb.ReplicaID{1},
			leaseholder:       4,
			excludeLeaseRepl:  true,
			transferTarget:    2,
		},
		{
			existing:          existing,
			replsNeedingSnaps: []roachpb.ReplicaID{1, 2},
			leaseholder:       4,
			excludeLeaseRepl:  true,
			transferTarget:    3,
		},
		{
			existing:          existing,
			replsNeedingSnaps: []roachpb.ReplicaID{1, 2},
			leaseholder:       4,
			excludeLeaseRepl:  false,
			transferTarget:    0,
		},
		{
			existing:          existing,
			replsNeedingSnaps: []roachpb.ReplicaID{1, 2, 3},
			leaseholder:       4,
			excludeLeaseRepl:  false,
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
				allocator.TransferLeaseOptions{
					ExcludeLeaseRepl:       c.excludeLeaseRepl,
					CheckCandidateFullness: true,
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
	stopper, g, _, a, _ := CreateTestAllocator(ctx, 10, true /* deterministic */)
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
				allocator.TransferLeaseOptions{
					ExcludeLeaseRepl:       false,
					CheckCandidateFullness: true,
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
	stopper, g, _, storePool, nl := storepool.CreateTestStorePool(ctx,
		storepool.TestTimeUntilStoreDeadOff, true, /* deterministic */
		func() int { return 10 }, /* nodeCount */
		livenesspb.NodeLivenessStatus_LIVE)
	a := MakeAllocator(storePool, func(string) (time.Duration, bool) {
		return 0, true
	}, nil)
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

	nl.SetNodeStatus(1, livenesspb.NodeLivenessStatus_DRAINING)
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
		existing         []roachpb.ReplicaDescriptor
		leaseholder      roachpb.StoreID
		excludeLeaseRepl bool
		expected         roachpb.StoreID
		conf             roachpb.SpanConfig
	}{
		// No existing lease holder, nothing to do.
		{existing: existing, leaseholder: 0, excludeLeaseRepl: false, expected: 0, conf: emptySpanConfig()},
		// Store 1 is draining, so it will try to transfer its lease if
		// excludeLeaseRepl is false. This behavior isn't relied upon,
		// though; leases are manually transferred when draining.
		{existing: existing, leaseholder: 1, excludeLeaseRepl: false, expected: 0, conf: emptySpanConfig()},
		{existing: existing, leaseholder: 1, excludeLeaseRepl: true, expected: 2, conf: emptySpanConfig()},
		// Store 2 is not a lease transfer source.
		{existing: existing, leaseholder: 2, excludeLeaseRepl: false, expected: 0, conf: emptySpanConfig()},
		{existing: existing, leaseholder: 2, excludeLeaseRepl: true, expected: 3, conf: emptySpanConfig()},
		// Store 3 is a lease transfer source, but won't transfer to
		// node 1 because it's draining.
		{existing: existing, leaseholder: 3, excludeLeaseRepl: false, expected: 2, conf: emptySpanConfig()},
		{existing: existing, leaseholder: 3, excludeLeaseRepl: true, expected: 2, conf: emptySpanConfig()},
		// Verify that lease preferences dont impact draining.
		// If the store that is within the lease preferences (store 1) is draining,
		// we'd like the lease to stay on the next best store (which is store 2).
		{existing: existing, leaseholder: 2, excludeLeaseRepl: false, expected: 0, conf: roachpb.SpanConfig{LeasePreferences: preferDC1}},
		// If the current lease on store 2 needs to be shed (indicated by
		// excludeLeaseRepl = false), and store 1 is draining, then store 3
		// is the only reasonable lease transfer target.
		{existing: existing, leaseholder: 2, excludeLeaseRepl: true, expected: 3, conf: roachpb.SpanConfig{LeasePreferences: preferDC1}},
		{existing: existing, leaseholder: 2, excludeLeaseRepl: false, expected: 3, conf: roachpb.SpanConfig{LeasePreferences: preferRegion1}},
		{existing: existing, leaseholder: 2, excludeLeaseRepl: true, expected: 3, conf: roachpb.SpanConfig{LeasePreferences: preferRegion1}},
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
				allocator.TransferLeaseOptions{
					ExcludeLeaseRepl:       c.excludeLeaseRepl,
					CheckCandidateFullness: true,
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
	stopper, g, _, a, _ := CreateTestAllocator(ctx, 10, false /* deterministic */)
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
		var rangeUsageInfo allocator.RangeUsageInfo
		result, _, details, ok := a.RebalanceVoter(
			ctx,
			emptySpanConfig(),
			nil,
			tc.existing,
			nil,
			rangeUsageInfo,
			storepool.StoreFilterThrottled,
			a.ScorerOptions(ctx),
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
		var rangeUsageInfo allocator.RangeUsageInfo
		result, _, details, ok := a.RebalanceVoter(
			ctx,
			emptySpanConfig(),
			nil,
			tc.existing,
			nil,
			rangeUsageInfo,
			storepool.StoreFilterThrottled,
			a.ScorerOptions(ctx),
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
	stopper, g, _, a, _ := CreateTestAllocator(ctx, 10, true /* deterministic */)
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
				&mockRepl{
					storeID:           c.leaseholder,
					replicationFactor: int32(len(c.existing)),
				},
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
	stopper, g, _, storePool, nl := storepool.CreateTestStorePool(ctx,
		storepool.TestTimeUntilStoreDeadOff, true, /* deterministic */
		func() int { return 10 }, /* nodeCount */
		livenesspb.NodeLivenessStatus_LIVE)
	a := MakeAllocator(storePool, func(string) (time.Duration, bool) {
		return 0, true
	}, nil)
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
	nl.SetNodeStatus(1, livenesspb.NodeLivenessStatus_UNAVAILABLE)

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
				&mockRepl{
					storeID:           c.leaseholder,
					replicationFactor: int32(len(c.existing)),
				},
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
	stopper, g, clock, storePool, nl := storepool.CreateTestStorePool(ctx,
		storepool.TestTimeUntilStoreDeadOff, true, /* deterministic */
		func() int { return 10 }, /* nodeCount */
		livenesspb.NodeLivenessStatus_LIVE)
	a := MakeAllocator(storePool, func(string) (time.Duration, bool) {
		return 0, true
	}, nil)
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
			&mockRepl{storeID: 2, replicationFactor: 3},
			nil, /* replicaStats */
		)
		require.Equal(t, expected, result)
	}
	timeAfterStoreSuspect := storepool.TimeAfterStoreSuspect.Get(&storePool.St.SV)
	// Based on capacity node 1 is desirable.
	assertShouldTransferLease(true)
	// Flip node 1 to unavailable, there should be no lease transfer now.
	nl.SetNodeStatus(1, livenesspb.NodeLivenessStatus_UNAVAILABLE)
	assertShouldTransferLease(false)
	// Set node back to live, but it's still suspected so not lease transfer expected.
	nl.SetNodeStatus(1, livenesspb.NodeLivenessStatus_LIVE)
	assertShouldTransferLease(false)
	// Wait out the suspected store timeout, verify that lease transfers are back.
	clock.Advance(timeAfterStoreSuspect + time.Millisecond)
	assertShouldTransferLease(true)
}

func TestAllocatorLeasePreferences(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	stopper, g, _, a, _ := CreateTestAllocator(ctx, 10, true /* deterministic */)
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
		leaseholder            roachpb.StoreID
		existing               []roachpb.ReplicaDescriptor
		preferences            []roachpb.LeasePreference
		expectAllowLeaseRepl   roachpb.StoreID /* excludeLeaseRepl = false */
		expectExcludeLeaseRepl roachpb.StoreID /* excludeLeaseRepl = true */
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
				&mockRepl{
					storeID:           c.leaseholder,
					replicationFactor: int32(len(c.existing)),
				},
				nil, /* replicaStats */
			)
			expectTransfer := c.expectAllowLeaseRepl != 0
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
				allocator.TransferLeaseOptions{
					ExcludeLeaseRepl:       false,
					CheckCandidateFullness: true,
				},
			)
			if c.expectAllowLeaseRepl != target.StoreID {
				t.Errorf("expected s%d for excludeLeaseRepl=false, but found %v", c.expectAllowLeaseRepl, target)
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
				allocator.TransferLeaseOptions{
					ExcludeLeaseRepl:       true,
					CheckCandidateFullness: true,
				},
			)
			if c.expectExcludeLeaseRepl != target.StoreID {
				t.Errorf("expected s%d for excludeLeaseRepl=true, but found %v", c.expectExcludeLeaseRepl, target)
			}
		})
	}
}

func TestAllocatorLeasePreferencesMultipleStoresPerLocality(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	stopper, g, _, a, _ := CreateTestAllocator(ctx, 10, true /* deterministic */)
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
		leaseholder              roachpb.StoreID
		existing                 []roachpb.ReplicaDescriptor
		preferences              []roachpb.LeasePreference
		expectedAllowLeaseRepl   roachpb.StoreID /* excludeLeaseRepl = false */
		expectedExcludeLeaseRepl roachpb.StoreID /* excludeLeaseRepl = true */
	}{
		{1, replicas(1, 3, 5), preferEast, 0, 3},
		// When `excludeLeaseRepl` = false, we'd expect either store 2 or 3
		// to be produced by `TransferLeaseTarget` (since both of them have
		// less-than-mean leases). In this case, the rng should produce 3.
		{1, replicas(1, 2, 3), preferEast, 0, 3},
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
				allocator.TransferLeaseOptions{
					ExcludeLeaseRepl:       false,
					CheckCandidateFullness: true,
				},
			)
			if c.expectedAllowLeaseRepl != target.StoreID {
				t.Errorf("expected s%d for excludeLeaseRepl=false, but found %v", c.expectedAllowLeaseRepl, target)
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
				allocator.TransferLeaseOptions{
					ExcludeLeaseRepl:       true,
					CheckCandidateFullness: true,
				},
			)
			if c.expectedExcludeLeaseRepl != target.StoreID {
				t.Errorf("expected s%d for excludeLeaseRepl=true, but found %v", c.expectedExcludeLeaseRepl, target)
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
	stopper, g, _, a, _ := CreateTestAllocator(ctx, 10, false /* deterministic */)
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
			a.ScorerOptions(ctx),
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
			a.ScorerOptions(ctx),
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
			a.ScorerOptions(ctx),
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
			stopper, g, _, a, _ := CreateTestAllocator(ctx, 10, false /* deterministic */)
			defer stopper.Stop(ctx)
			sg := gossiputil.NewStoreGossiper(g)
			sg.GossipStores(test.stores, t)

			// Allocate the voting replica first, before the non-voter. This is the
			// order in which we'd expect the allocator to repair a given range. See
			// TestAllocatorComputeAction.
			voterTarget, _, err := a.AllocateVoter(ctx, test.conf, test.existingVoters, test.existingNonVoters, Dead)
			if test.shouldVoterAllocFail {
				require.Errorf(t, err, "expected voter allocation to fail; got %v as a valid target instead", voterTarget)
			} else {
				require.NoError(t, err)
				require.True(t, check(voterTarget.StoreID, test.expectedVoters))
				test.existingVoters = append(test.existingVoters, replicas(voterTarget.StoreID)...)
			}

			nonVoterTarget, _, err := a.AllocateNonVoter(ctx, test.conf, test.existingVoters, test.existingNonVoters, Dead)
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
	stopper, g, _, a, _ := CreateTestAllocator(ctx, 10, false /* deterministic */)
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
		targetStore, details, err := a.AllocateVoter(ctx, emptySpanConfig(), existingRepls, nil, Dead)
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
	stopper, g, _, a, _ := CreateTestAllocator(ctx, 10, false /* deterministic */)
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
		var rangeUsageInfo allocator.RangeUsageInfo
		target, _, details, ok := a.RebalanceVoter(
			ctx,
			emptySpanConfig(),
			nil,
			existingRepls,
			nil,
			rangeUsageInfo,
			storepool.StoreFilterThrottled,
			a.ScorerOptions(ctx),
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
	stopper, g, _, a, _ := CreateTestAllocator(ctx, 10, false /* deterministic */)
	defer stopper.Stop(ctx)
	sg := gossiputil.NewStoreGossiper(g)
	sg.GossipStores(stores, t)
	sl, _, _ := a.StorePool.GetStoreList(storepool.StoreFilterThrottled)

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
			ctx, a.StorePool.GetStoreDescriptor, existingRepls, conf.NumReplicas,
			conf.Constraints)
		allocationConstraintsChecker := voterConstraintsCheckerForAllocation(analyzed, constraint.EmptyAnalyzedConstraints)
		removalConstraintsChecker := voterConstraintsCheckerForRemoval(analyzed, constraint.EmptyAnalyzedConstraints)
		rebalanceConstraintsChecker := voterConstraintsCheckerForRebalance(analyzed, constraint.EmptyAnalyzedConstraints)

		a.StorePool.IsStoreReadyForRoutineReplicaTransfer = func(_ context.Context, storeID roachpb.StoreID) bool {
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
				a.StorePool.GetLocalitiesByStore(existingRepls),
				a.StorePool.IsStoreReadyForRoutineReplicaTransfer,
				false, /* allowMultipleReplsPerNode */
				a.ScorerOptions(ctx),
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
				a.StorePool.GetLocalitiesByStore(existingRepls),
				a.StorePool.IsStoreReadyForRoutineReplicaTransfer,
				a.ScorerOptions(ctx),
				a.Metrics,
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
			stopper, g, _, a, _ := CreateTestAllocator(ctx, 10, false /* deterministic */)
			defer stopper.Stop(ctx)
			sg := gossiputil.NewStoreGossiper(g)
			sg.GossipStores(test.stores, t)

			result, _, err := a.AllocateNonVoter(ctx, test.conf, test.existingVoters, test.existingNonVoters, Dead)
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
	stopper, g, _, a, _ := CreateTestAllocator(ctx, 10, false /* deterministic */)
	defer stopper.Stop(ctx)
	sg := gossiputil.NewStoreGossiper(g)
	sg.GossipStores(multiDiversityDCStores, t)
	sl, _, _ := a.StorePool.GetStoreList(storepool.StoreFilterThrottled)

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
			ctx, a.StorePool.GetStoreDescriptor, existingRepls, conf.NumReplicas,
			conf.Constraints)
		checkFn := voterConstraintsCheckerForAllocation(analyzed, constraint.EmptyAnalyzedConstraints)

		candidates := rankedCandidateListForAllocation(
			ctx,
			sl,
			checkFn,
			existingRepls,
			a.StorePool.GetLocalitiesByStore(existingRepls),
			func(context.Context, roachpb.StoreID) bool { return true },
			false, /* allowMultipleReplsPerNode */
			a.ScorerOptions(ctx),
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
	stopper, g, _, a, _ := CreateTestAllocator(ctx, 10, false /* deterministic */)
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
		sl, _, _ := a.StorePool.GetStoreListFromIDs(tc.existing, storepool.StoreFilterNone)
		existingRepls := make([]roachpb.ReplicaDescriptor, len(tc.existing))
		for i, storeID := range tc.existing {
			existingRepls[i] = roachpb.ReplicaDescriptor{
				NodeID:  roachpb.NodeID(storeID),
				StoreID: storeID,
			}
		}
		analyzed := constraint.AnalyzeConstraints(ctx, a.StorePool.GetStoreDescriptor, existingRepls,
			0 /* numReplicas */, tc.constraints)

		// Check behavior in a span config where `voter_constraints` are empty.
		checkFn := voterConstraintsCheckerForRemoval(analyzed, constraint.EmptyAnalyzedConstraints)
		candidates := candidateListForRemoval(ctx,
			sl,
			checkFn,
			a.StorePool.GetLocalitiesByStore(existingRepls),
			a.ScorerOptions(ctx))
		if !expectedStoreIDsMatch(tc.expected, candidates.worst()) {
			t.Errorf("%d (with `constraints`): expected candidateListForRemoval(%v)"+
				" = %v, but got %v\n for candidates %v", testIdx, tc.existing, tc.expected,
				candidates.worst(), candidates)
		}

		// Check that we'd see the same result if the same constraints were
		// specified as `voter_constraints`.
		checkFn = voterConstraintsCheckerForRemoval(constraint.EmptyAnalyzedConstraints, analyzed)
		candidates = candidateListForRemoval(ctx,
			sl,
			checkFn,
			a.StorePool.GetLocalitiesByStore(existingRepls),
			a.ScorerOptions(ctx))
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

	var rangeUsageInfo allocator.RangeUsageInfo
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
			stopper, g, _, a, _ := CreateTestAllocator(ctx, 10, false /* deterministic */)
			defer stopper.Stop(ctx)
			sg := gossiputil.NewStoreGossiper(g)
			sg.GossipStores(test.stores, t)
			// Enable read disk health checking in candidate exclusion.
			add, remove, _, ok := a.RebalanceNonVoter(
				ctx,
				test.conf,
				nil,
				test.existingVoters,
				test.existingNonVoters,
				rangeUsageInfo,
				storepool.StoreFilterThrottled,
				a.ScorerOptions(ctx),
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

// TestAllocatorRebalanceReadAmpCheck ensures that rebalancing voters:
// (1) Respects storeHealthEnforcement setting, by ignoring L0 Sublevels in
//
//	rebalancing decisions when disabled or set to log only.
//
// (2) Considers L0 sublevels when set to rebalanceOnly or allocate in
//
//	conjunction with the mean.
//
// (3) Does not attempt to rebalance off of the store when read amplification
//
//	is high, as this setting is only used for filtering candidates.
func TestAllocatorRebalanceReadAmpCheck(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	type testCase struct {
		name                                      string
		stores                                    []*roachpb.StoreDescriptor
		conf                                      roachpb.SpanConfig
		existingVoters                            []roachpb.ReplicaDescriptor
		expectNoAction                            bool
		expectedRemoveTargets, expectedAddTargets []roachpb.StoreID
		enforcement                               StoreHealthEnforcement
	}
	tests := []testCase{
		{
			name: "don't move off of nodes with high read amp when StoreHealthBlockRebalanceTo",
			// NB: Store 1,2, 4 have okay read amp. Store 3 has high read amp.
			// We expect high read amplifaction to only be considered for
			// exlcuding targets, not for triggering rebalancing.
			stores:         threeStoresHighReadAmpAscRangeCount,
			conf:           emptySpanConfig(),
			existingVoters: replicas(3, 1),
			expectNoAction: true,
			enforcement:    StoreHealthBlockRebalanceTo,
		},
		{
			name: "don't move off of nodes with high read amp when StoreHealthBlockAll",
			// NB: Store 1,2, 4 have okay read amp. Store 3 has high read amp.
			// We expect high read amplifaction to only be considered for
			// exlcuding targets, not for triggering rebalancing.
			stores:         threeStoresHighReadAmpAscRangeCount,
			conf:           emptySpanConfig(),
			existingVoters: replicas(3, 1),
			expectNoAction: true,
			enforcement:    StoreHealthBlockAll,
		},
		{
			name: "don't take action when enforcement is not StoreHealthNoAction",
			// NB: Store 3 has L0Sublevels > threshold. Store 2 has 3 x higher
			// ranges as other stores. Should move to candidate to 4, however
			// enforcement for rebalancing is not enabled so will pick
			// candidate 3 which has a lower range count.
			stores:                oneStoreHighReadAmp,
			conf:                  emptySpanConfig(),
			existingVoters:        replicas(1, 2),
			expectedRemoveTargets: []roachpb.StoreID{2},
			expectedAddTargets:    []roachpb.StoreID{3},
			enforcement:           StoreHealthNoAction,
		},
		{
			name: "don't rebalance to nodes with high read amp when StoreHealthBlockRebalanceTo enforcement",
			// NB: Store 3 has L0Sublevels > threshold. Store 2 has 3 x higher
			// ranges as other stores. Should move to candidate to 4, which
			// doesn't have high read amp.
			stores:                oneStoreHighReadAmp,
			conf:                  emptySpanConfig(),
			existingVoters:        replicas(1, 2),
			expectedRemoveTargets: []roachpb.StoreID{2},
			expectedAddTargets:    []roachpb.StoreID{4},
			enforcement:           StoreHealthBlockRebalanceTo,
		},
		{
			name: "don't rebalance to nodes with high read amp when StoreHealthBlockAll enforcement",
			// NB: Store 3 has L0Sublevels > threshold. Store 2 has 3 x higher
			// ranges as other stores. Should move to candidate to 4, which
			// doesn't have high read amp.
			stores:                oneStoreHighReadAmp,
			conf:                  emptySpanConfig(),
			existingVoters:        replicas(1, 2),
			expectedRemoveTargets: []roachpb.StoreID{2},
			expectedAddTargets:    []roachpb.StoreID{4},
			enforcement:           StoreHealthBlockAll,
		},
	}

	var rangeUsageInfo allocator.RangeUsageInfo
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
			stopper, g, _, a, _ := CreateTestAllocator(ctx, 10, true /* deterministic */)
			defer stopper.Stop(ctx)
			sg := gossiputil.NewStoreGossiper(g)
			sg.GossipStores(test.stores, t)
			// Enable read disk health checking in candidate exclusion.
			options := a.ScorerOptions(ctx)
			options.StoreHealthOptions = StoreHealthOptions{EnforcementLevel: test.enforcement, L0SublevelThreshold: 20}
			add, remove, _, ok := a.RebalanceVoter(
				ctx,
				test.conf,
				nil,
				test.existingVoters,
				[]roachpb.ReplicaDescriptor{},
				rangeUsageInfo,
				storepool.StoreFilterThrottled,
				options,
			)
			if test.expectNoAction {
				require.True(t, !ok)
			} else {
				require.Truef(t, ok, "no action taken on range")
				require.Truef(t,
					chk(add, test.expectedAddTargets),
					"the addition target %+v from RebalanceVoter doesn't match expectation",
					add)
				require.Truef(t,
					chk(remove, test.expectedRemoveTargets),
					"the removal target %+v from RebalanceVoter doesn't match expectation",
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
	stopper, g, _, a, _ := CreateTestAllocator(ctx, 10, false /* deterministic */)
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

	var rangeUsageInfo allocator.RangeUsageInfo
	existingNonVoters := replicas(1, 2)
	existingVoters := replicas(3, 4)
	add, remove, _, ok := a.RebalanceVoter(
		ctx,
		conf,
		nil,
		existingVoters,
		existingNonVoters,
		rangeUsageInfo,
		storepool.StoreFilterThrottled,
		a.ScorerOptions(ctx),
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

	stopper, g, _, a, _ := CreateTestAllocator(ctx, 2, false /* deterministic */)
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
	var rangeUsageInfo allocator.RangeUsageInfo
	add, remove, _, ok := a.RebalanceNonVoter(
		ctx,
		emptySpanConfig(),
		nil,
		existingVoters,
		existingNonVoters,
		rangeUsageInfo,
		storepool.StoreFilterThrottled,
		a.ScorerOptions(ctx),
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
	stopper, g, _, a, _ := CreateTestAllocator(ctx, 10, false /* deterministic */)
	defer stopper.Stop(ctx)
	sg := gossiputil.NewStoreGossiper(g)
	sg.GossipStores(multiDiversityDCStores, t)
	sl, _, _ := a.StorePool.GetStoreList(storepool.StoreFilterThrottled)

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
		var rangeUsageInfo allocator.RangeUsageInfo
		conf := roachpb.SpanConfig{
			Constraints: tc.constraints,
			NumReplicas: tc.numReplicas,
		}
		analyzed := constraint.AnalyzeConstraints(
			ctx, a.StorePool.GetStoreDescriptor, existingRepls,
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
			a.StorePool.GetLocalitiesByStore(existingRepls),
			func(context.Context, roachpb.StoreID) bool { return true },
			a.ScorerOptions(ctx),
			a.Metrics,
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
				storepool.StoreFilterThrottled,
				a.ScorerOptions(ctx),
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
	stopper, g, _, storePool, _ := storepool.CreateTestStorePool(ctx,
		storepool.TestTimeUntilStoreDeadOff, true, /* deterministic */
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
	manual := timeutil.NewManualTime(timeutil.Unix(0, 123))
	clock := hlc.NewClock(manual, time.Nanosecond /* maxOffset */)

	// Set up four different load distributions. Record a bunch of requests to
	// the unknown node 99 in evenlyBalanced to verify that requests from
	// unknown localities don't affect the algorithm.
	evenlyBalanced := replicastats.NewReplicaStats(clock, localityFn)
	evenlyBalanced.RecordCount(1, 1)
	evenlyBalanced.RecordCount(1, 2)
	evenlyBalanced.RecordCount(1, 3)
	imbalanced1 := replicastats.NewReplicaStats(clock, localityFn)
	imbalanced2 := replicastats.NewReplicaStats(clock, localityFn)
	imbalanced3 := replicastats.NewReplicaStats(clock, localityFn)
	for i := 0; i < 100*int(MinLeaseTransferStatsDuration.Seconds()); i++ {
		evenlyBalanced.RecordCount(1, 99)
		imbalanced1.RecordCount(1, 1)
		imbalanced2.RecordCount(1, 2)
		imbalanced3.RecordCount(1, 3)
	}

	manual.Advance(MinLeaseTransferStatsDuration)

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
		leaseholder      roachpb.StoreID
		latency          map[string]time.Duration
		stats            *replicastats.ReplicaStats
		excludeLeaseRepl bool
		expected         roachpb.StoreID
	}{
		// No existing lease holder, nothing to do.
		{leaseholder: 0, latency: noLatency, stats: evenlyBalanced, excludeLeaseRepl: false, expected: 0},
		{leaseholder: 1, latency: noLatency, stats: evenlyBalanced, excludeLeaseRepl: false, expected: 0},
		{leaseholder: 1, latency: noLatency, stats: evenlyBalanced, excludeLeaseRepl: true, expected: 2},
		{leaseholder: 2, latency: noLatency, stats: evenlyBalanced, excludeLeaseRepl: false, expected: 1},
		{leaseholder: 2, latency: noLatency, stats: evenlyBalanced, excludeLeaseRepl: true, expected: 1},
		{leaseholder: 3, latency: noLatency, stats: evenlyBalanced, excludeLeaseRepl: false, expected: 1},
		{leaseholder: 3, latency: noLatency, stats: evenlyBalanced, excludeLeaseRepl: true, expected: 1},
		{leaseholder: 0, latency: noLatency, stats: imbalanced1, excludeLeaseRepl: false, expected: 0},
		{leaseholder: 1, latency: noLatency, stats: imbalanced1, excludeLeaseRepl: false, expected: 0},
		{leaseholder: 1, latency: noLatency, stats: imbalanced1, excludeLeaseRepl: true, expected: 2},
		{leaseholder: 2, latency: noLatency, stats: imbalanced1, excludeLeaseRepl: false, expected: 1},
		{leaseholder: 2, latency: noLatency, stats: imbalanced1, excludeLeaseRepl: true, expected: 1},
		{leaseholder: 3, latency: noLatency, stats: imbalanced1, excludeLeaseRepl: false, expected: 1},
		{leaseholder: 3, latency: noLatency, stats: imbalanced1, excludeLeaseRepl: true, expected: 1},
		{leaseholder: 0, latency: noLatency, stats: imbalanced2, excludeLeaseRepl: false, expected: 0},
		{leaseholder: 1, latency: noLatency, stats: imbalanced2, excludeLeaseRepl: false, expected: 0},
		{leaseholder: 1, latency: noLatency, stats: imbalanced2, excludeLeaseRepl: true, expected: 2},
		{leaseholder: 2, latency: noLatency, stats: imbalanced2, excludeLeaseRepl: false, expected: 1},
		{leaseholder: 2, latency: noLatency, stats: imbalanced2, excludeLeaseRepl: true, expected: 1},
		{leaseholder: 3, latency: noLatency, stats: imbalanced2, excludeLeaseRepl: false, expected: 1},
		{leaseholder: 3, latency: noLatency, stats: imbalanced2, excludeLeaseRepl: true, expected: 1},
		{leaseholder: 0, latency: noLatency, stats: imbalanced3, excludeLeaseRepl: false, expected: 0},
		{leaseholder: 1, latency: noLatency, stats: imbalanced3, excludeLeaseRepl: false, expected: 0},
		{leaseholder: 1, latency: noLatency, stats: imbalanced3, excludeLeaseRepl: true, expected: 2},
		{leaseholder: 2, latency: noLatency, stats: imbalanced3, excludeLeaseRepl: false, expected: 1},
		{leaseholder: 2, latency: noLatency, stats: imbalanced3, excludeLeaseRepl: true, expected: 1},
		{leaseholder: 3, latency: noLatency, stats: imbalanced3, excludeLeaseRepl: false, expected: 1},
		{leaseholder: 3, latency: noLatency, stats: imbalanced3, excludeLeaseRepl: true, expected: 1},
		{leaseholder: 0, latency: highLatency, stats: evenlyBalanced, excludeLeaseRepl: false, expected: 0},
		{leaseholder: 1, latency: highLatency, stats: evenlyBalanced, excludeLeaseRepl: false, expected: 0},
		{leaseholder: 1, latency: highLatency, stats: evenlyBalanced, excludeLeaseRepl: true, expected: 2},
		{leaseholder: 2, latency: highLatency, stats: evenlyBalanced, excludeLeaseRepl: false, expected: 1},
		{leaseholder: 2, latency: highLatency, stats: evenlyBalanced, excludeLeaseRepl: true, expected: 1},
		{leaseholder: 3, latency: highLatency, stats: evenlyBalanced, excludeLeaseRepl: false, expected: 1},
		{leaseholder: 3, latency: highLatency, stats: evenlyBalanced, excludeLeaseRepl: true, expected: 1},
		{leaseholder: 0, latency: highLatency, stats: imbalanced1, excludeLeaseRepl: false, expected: 0},
		{leaseholder: 1, latency: highLatency, stats: imbalanced1, excludeLeaseRepl: false, expected: 0},
		{leaseholder: 1, latency: highLatency, stats: imbalanced1, excludeLeaseRepl: true, expected: 2},
		{leaseholder: 2, latency: highLatency, stats: imbalanced1, excludeLeaseRepl: false, expected: 1},
		{leaseholder: 2, latency: highLatency, stats: imbalanced1, excludeLeaseRepl: true, expected: 1},
		{leaseholder: 3, latency: highLatency, stats: imbalanced1, excludeLeaseRepl: false, expected: 1},
		{leaseholder: 3, latency: highLatency, stats: imbalanced1, excludeLeaseRepl: true, expected: 1},
		{leaseholder: 0, latency: highLatency, stats: imbalanced2, excludeLeaseRepl: false, expected: 0},
		{leaseholder: 1, latency: highLatency, stats: imbalanced2, excludeLeaseRepl: false, expected: 2},
		{leaseholder: 1, latency: highLatency, stats: imbalanced2, excludeLeaseRepl: true, expected: 2},
		{leaseholder: 2, latency: highLatency, stats: imbalanced2, excludeLeaseRepl: false, expected: 0},
		{leaseholder: 2, latency: highLatency, stats: imbalanced2, excludeLeaseRepl: true, expected: 1},
		{leaseholder: 3, latency: highLatency, stats: imbalanced2, excludeLeaseRepl: false, expected: 2},
		{leaseholder: 3, latency: highLatency, stats: imbalanced2, excludeLeaseRepl: true, expected: 2},
		{leaseholder: 0, latency: highLatency, stats: imbalanced3, excludeLeaseRepl: false, expected: 0},
		{leaseholder: 1, latency: highLatency, stats: imbalanced3, excludeLeaseRepl: true, expected: 3},
		{leaseholder: 1, latency: highLatency, stats: imbalanced3, excludeLeaseRepl: true, expected: 3},
		{leaseholder: 2, latency: highLatency, stats: imbalanced3, excludeLeaseRepl: false, expected: 3},
		{leaseholder: 2, latency: highLatency, stats: imbalanced3, excludeLeaseRepl: true, expected: 3},
		{leaseholder: 3, latency: highLatency, stats: imbalanced3, excludeLeaseRepl: false, expected: 0},
		{leaseholder: 3, latency: highLatency, stats: imbalanced3, excludeLeaseRepl: true, expected: 1},
	}

	for _, c := range testCases {
		t.Run("", func(t *testing.T) {
			a := MakeAllocator(storePool, func(addr string) (time.Duration, bool) {
				return c.latency[addr], true
			}, nil)
			target := a.TransferLeaseTarget(
				ctx,
				emptySpanConfig(),
				existing,
				&mockRepl{
					replicationFactor: 3,
					storeID:           c.leaseholder,
				},
				c.stats.SnapshotRatedSummary(),
				false,
				allocator.TransferLeaseOptions{
					ExcludeLeaseRepl:       c.excludeLeaseRepl,
					CheckCandidateFullness: true,
					DryRun:                 false,
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
	stopper, g, _, a, _ := CreateTestAllocator(ctx, 10, false /* deterministic */)
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
			a.ScorerOptions(ctx),
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
						Type:      roachpb.NON_VOTER,
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
						Type:      roachpb.NON_VOTER,
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
						Type:      roachpb.NON_VOTER,
					},
					{
						StoreID:   6,
						NodeID:    6,
						ReplicaID: 6,
						Type:      roachpb.NON_VOTER,
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
						Type:      roachpb.NON_VOTER,
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
						Type:      roachpb.NON_VOTER,
					},
					{
						StoreID:   6,
						NodeID:    6,
						ReplicaID: 6,
						Type:      roachpb.NON_VOTER,
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
						Type:      roachpb.NON_VOTER,
					},
					{
						StoreID:   3,
						NodeID:    3,
						ReplicaID: 3,
						Type:      roachpb.NON_VOTER,
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
	stopper, _, sp, a, _ := CreateTestAllocator(ctx, 10, false /* deterministic */)
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
	stopper, _, sp, a, _ := CreateTestAllocator(ctx, 10, false /* deterministic */)
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
	stopper, _, sp, a, _ := CreateTestAllocator(ctx, 10, false /* deterministic */)
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
						Type:      roachpb.NON_VOTER,
					},
					{
						StoreID:   6,
						NodeID:    6,
						ReplicaID: 6,
						Type:      roachpb.NON_VOTER,
					},
					{
						StoreID:   7,
						NodeID:    7,
						ReplicaID: 7,
						Type:      roachpb.NON_VOTER,
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
						Type:      roachpb.NON_VOTER,
					},
					{
						StoreID:   6,
						NodeID:    6,
						ReplicaID: 6,
						Type:      roachpb.NON_VOTER,
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
	stopper, _, sp, a, _ := CreateTestAllocator(ctx, 10, false /* deterministic */)
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
				Type:      roachpb.LEARNER,
			},
		},
	}

	// Removing a learner is prioritized over adding a new replica to an under
	// replicated range.
	ctx := context.Background()
	stopper, _, sp, a, _ := CreateTestAllocator(ctx, 10, false /* deterministic */)
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
	stopper, _, _, sp, _ := storepool.CreateTestStorePool(ctx,
		storepool.TestTimeUntilStoreDeadOff, false, /* deterministic */
		func() int { return numNodes },
		livenesspb.NodeLivenessStatus_LIVE)
	a := MakeAllocator(sp, func(string) (time.Duration, bool) {
		return 0, true
	}, nil)

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

				clusterNodes := a.StorePool.ClusterNodeCount()
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

	a := MakeAllocator(nil, nil, nil)
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
			status.RaftState = raft.StateLeader
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
			candidates := FilterBehindReplicas(ctx, status, replicas)
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
			status.RaftState = raft.StateLeader
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

			candidates := FilterUnremovableReplicas(ctx, status, replicas, c.brandNewReplicaID)
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
			status.RaftState = raft.StateLeader
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

// TestAllocatorRebalanceDeterminism tests that calls to RebalanceVoter are
// deterministic.
func TestAllocatorRebalanceDeterminism(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	stores := []*roachpb.StoreDescriptor{
		{
			StoreID:    1,
			Attrs:      roachpb.Attributes{},
			Node:       roachpb.NodeDescriptor{NodeID: 1},
			Capacity:   roachpb.StoreCapacity{LeaseCount: 934, RangeCount: 934},
			Properties: roachpb.StoreProperties{},
		},
		{
			StoreID:  2,
			Node:     roachpb.NodeDescriptor{NodeID: 3},
			Capacity: roachpb.StoreCapacity{LeaseCount: 0, RangeCount: 933},
		},
		{
			StoreID:  3,
			Node:     roachpb.NodeDescriptor{NodeID: 3},
			Capacity: roachpb.StoreCapacity{LeaseCount: 0, RangeCount: 934},
		},
		{
			StoreID:  4,
			Node:     roachpb.NodeDescriptor{NodeID: 4},
			Capacity: roachpb.StoreCapacity{LeaseCount: 118, RangeCount: 349},
		},
		{
			StoreID:  5,
			Node:     roachpb.NodeDescriptor{NodeID: 5},
			Capacity: roachpb.StoreCapacity{LeaseCount: 115, RangeCount: 351},
		},
		{
			StoreID:  6,
			Node:     roachpb.NodeDescriptor{NodeID: 6},
			Capacity: roachpb.StoreCapacity{LeaseCount: 118, RangeCount: 349},
		},
		{
			StoreID:  7,
			Node:     roachpb.NodeDescriptor{NodeID: 7},
			Capacity: roachpb.StoreCapacity{LeaseCount: 105, RangeCount: 350},
		},
	}

	runner := func() func() (roachpb.ReplicationTarget, roachpb.ReplicationTarget) {
		ctx := context.Background()
		stopper, g, _, a, _ := CreateTestAllocator(ctx, 7 /* numNodes */, true /* deterministic */)
		defer stopper.Stop(ctx)
		gossiputil.NewStoreGossiper(g).GossipStores(stores, t)
		return func() (roachpb.ReplicationTarget, roachpb.ReplicationTarget) {
			var rangeUsageInfo allocator.RangeUsageInfo
			// Ensure that we wouldn't normally rebalance when all stores have the same
			// replica count.
			add, remove, _, _ := a.RebalanceVoter(
				ctx,
				roachpb.TestingDefaultSpanConfig(),
				nil,
				replicas(1, 2, 5),
				nil,
				rangeUsageInfo,
				storepool.StoreFilterThrottled,
				a.ScorerOptions(ctx),
			)
			return add, remove
		}
	}

	ra, rb := runner(), runner()
	for i := 0; i < 100000; i++ {
		fmt.Println()
		addA, removeA := ra()
		addB, removeB := rb()
		fmt.Println()

		require.Equal(t, addA, addB, "%d iters", i)
		require.Equal(t, removeA, removeB, "%d iters", i)
	}
}

// TestAllocatorRebalanceWithScatter tests that when `scatter` is set to true,
// the allocator will produce rebalance opportunities even when it normally
// wouldn't.
func TestAllocatorRebalanceWithScatter(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper, g, _, a, _ := CreateTestAllocator(ctx, 10 /* numNodes */, true /* deterministic */)
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

	var rangeUsageInfo allocator.RangeUsageInfo

	// Ensure that we wouldn't normally rebalance when all stores have the same
	// replica count.
	_, _, _, ok := a.RebalanceVoter(
		ctx,
		emptySpanConfig(),
		nil,
		replicas(1),
		nil,
		rangeUsageInfo,
		storepool.StoreFilterThrottled,
		a.ScorerOptions(ctx),
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
		storepool.StoreFilterThrottled,
		a.ScorerOptionsForScatter(ctx),
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
	stopper, g, _, a, _ := CreateTestAllocator(ctx, 10, false /* deterministic */)
	defer stopper.Stop(ctx)
	gossiputil.NewStoreGossiper(g).GossipStores(stores, t)

	for _, tc := range testCases {
		t.Run(tc.constraint.String(), func(t *testing.T) {
			constraints := roachpb.ConstraintsConjunction{
				Constraints: []roachpb.Constraint{
					tc.constraint,
				},
			}

			var rangeUsageInfo allocator.RangeUsageInfo
			actual, _, _, ok := a.RebalanceVoter(
				ctx,
				roachpb.SpanConfig{Constraints: []roachpb.ConstraintsConjunction{constraints}},
				nil,
				existingReplicas,
				nil,
				rangeUsageInfo,
				storepool.StoreFilterThrottled,
				a.ScorerOptions(ctx),
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
	if !allocator.MaxCapacityCheck(ots.StoreDescriptor) {
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
	clock := hlc.NewClockWithSystemTimeSource(time.Nanosecond /* maxOffset */)

	// Model a set of stores in a cluster doing rebalancing, with ranges being
	// randomly added occasionally.
	rpcContext := rpc.NewContext(ctx, rpc.ContextOptions{
		TenantID:  roachpb.SystemTenantID,
		Config:    &base.Config{Insecure: true},
		Clock:     clock.WallClock(),
		MaxOffset: clock.MaxOffset(),
		Stopper:   stopper,
		Settings:  st,
	})
	server := rpc.NewServer(rpcContext) // never started
	g := gossip.NewTest(1, rpcContext, server, stopper, metric.NewRegistry(), zonepb.DefaultZoneConfigRef())

	storepool.TimeUntilStoreDead.Override(ctx, &st.SV, storepool.TestTimeUntilStoreDeadOff)

	const generations = 100
	const nodes = 20
	const capacity = (1 << 30) + 1
	const rangeSize = 16 << 20

	mockNodeLiveness := storepool.NewMockNodeLiveness(livenesspb.NodeLivenessStatus_LIVE)
	sp := storepool.NewStorePool(
		log.MakeTestingAmbientContext(tr),
		st,
		g,
		clock,
		func() int {
			return nodes
		},
		mockNodeLiveness.NodeLivenessFunc,
		false, /* deterministic */
	)
	alloc := MakeAllocator(sp, func(string) (time.Duration, bool) {
		return 0, false
	}, nil)

	var wg sync.WaitGroup
	g.RegisterCallback(gossip.MakePrefixPattern(gossip.KeyStoreDescPrefix),
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
			if mockNodeLiveness.NodeLivenessFunc(roachpb.NodeID(j), time.Time{}, 0) == livenesspb.NodeLivenessStatus_DEAD {
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
				mockNodeLiveness.SetNodeStatus(roachpb.NodeID(j), livenesspb.NodeLivenessStatus_DEAD)
			}
			wg.Add(1)
			if err := g.AddInfoProto(gossip.MakeStoreDescKey(roachpb.StoreID(j)), &ts.StoreDescriptor, 0); err != nil {
				t.Fatal(err)
			}
		}
		wg.Wait()

		// Loop through each store a number of times and maybe rebalance.
		for j := 0; j < 10; j++ {
			for k := 0; k < len(testStores); k++ {
				if mockNodeLiveness.NodeLivenessFunc(roachpb.NodeID(k), time.Time{}, 0) == livenesspb.NodeLivenessStatus_DEAD {
					continue
				}
				ts := &testStores[k]
				// Rebalance until there's no more rebalancing to do.
				if ts.Capacity.RangeCount > 0 {
					var rangeUsageInfo allocator.RangeUsageInfo
					target, _, details, ok := alloc.RebalanceVoter(
						ctx,
						emptySpanConfig(),
						nil,
						[]roachpb.ReplicaDescriptor{{NodeID: ts.Node.NodeID, StoreID: ts.StoreID}},
						nil,
						rangeUsageInfo,
						storepool.StoreFilterThrottled,
						alloc.ScorerOptions(ctx),
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
					if err := g.AddInfoProto(gossip.MakeStoreDescKey(roachpb.StoreID(j)), &ts.StoreDescriptor, 0); err != nil {
						t.Fatal(err)
					}
				}
			}
		}

		// Simulate rocksdb compactions freeing up disk space.
		for j := 0; j < len(testStores); j++ {
			if mockNodeLiveness.NodeLivenessFunc(roachpb.NodeID(j), time.Time{}, 0) != livenesspb.NodeLivenessStatus_DEAD {
				ts := &testStores[j]
				if ts.Capacity.Available <= 0 {
					t.Errorf("testStore %d ran out of space during generation %d: %+v", j, i, ts.Capacity)
					mockNodeLiveness.SetNodeStatus(roachpb.NodeID(j), livenesspb.NodeLivenessStatus_DEAD)
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
		var rangeUsageInfo allocator.RangeUsageInfo
		target, _, details, ok := alloc.RebalanceVoter(
			ctx,
			emptySpanConfig(),
			nil,
			[]roachpb.ReplicaDescriptor{{NodeID: ts.Node.NodeID, StoreID: ts.StoreID}},
			nil,
			rangeUsageInfo,
			storepool.StoreFilterThrottled,
			alloc.ScorerOptions(ctx),
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
	opts := &QPSScorerOptions{
		StoreHealthOptions:    StoreHealthOptions{EnforcementLevel: StoreHealthNoAction},
		QPSPerReplica:         jitteredQPS,
		QPSRebalanceThreshold: 0.2,
	}
	var rangeUsageInfo allocator.RangeUsageInfo
	add, remove, details, ok := alloc.RebalanceVoter(
		ctx,
		emptySpanConfig(),
		nil,
		[]roachpb.ReplicaDescriptor{{NodeID: candidate.Node.NodeID, StoreID: candidate.StoreID}},
		nil,
		rangeUsageInfo,
		storepool.StoreFilterThrottled,
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
	// |   0 | 1 13326.98 | 1 16616.94 |     0 0.00 |     0 0.00 |     0 0.00 |     0 0.00 |     0 0.00 |     0 0.00 |     0 0.00 |     0 0.00 |
	// |   2 |  2 3955.65 |  2 6238.15 |  1 1500.16 | 2 18249.96 |     0 0.00 |     0 0.00 |     0 0.00 |     0 0.00 |     0 0.00 |     0 0.00 |
	// |   4 |  3 2011.65 |  3 2823.93 |  3 1500.16 | 3 11773.06 |  2 1886.72 |  2 9948.41 |     0 0.00 |     0 0.00 |     0 0.00 |     0 0.00 |
	// |   6 |  5 2011.65 |  5 2823.93 |  5 1500.16 |  3 3866.48 |  4 1886.72 |  2 3672.19 |  2 5461.94 |  3 8720.85 |     0 0.00 |     0 0.00 |
	// |   8 |  7 2011.65 |  7 2823.93 |  7 1500.16 |  4 2580.55 |  6 1886.72 |  3 1526.99 |  3 2376.77 |  4 5101.76 |  4 7951.01 |  1 2184.39 |
	// |  10 |  9 2011.65 |  9 2823.93 | 10 5059.90 |  6 2580.55 |  8 1886.72 |  8 5128.17 |  5 2376.77 |  4 2730.05 |  4 3161.80 |  3 2184.39 |
	// |  12 | 11 2011.65 | 11 2823.93 | 11 4567.11 |  8 2580.55 | 12 3116.29 |  9 4391.38 |  7 2376.77 |  6 2730.05 |  6 3161.80 |  5 2184.39 |
	// |  14 | 13 2011.65 | 13 2823.93 | 13 4567.11 | 10 2580.55 | 14 3116.29 | 11 4391.38 |  9 2376.77 |  8 2730.05 |  8 3161.80 |  7 2184.39 |
	// |  16 | 15 2011.65 | 15 2823.93 | 15 4567.11 | 12 2580.55 | 16 3116.29 | 13 4391.38 | 11 2376.77 | 10 2730.05 | 10 3161.80 |  9 2184.39 |
	// |  18 | 17 2011.65 | 17 2823.93 | 17 4567.11 | 14 2580.55 | 18 3116.29 | 15 4391.38 | 13 2376.77 | 12 2730.05 | 12 3161.80 | 11 2184.39 |
	// |  20 | 19 2011.65 | 19 2823.93 | 19 4567.11 | 16 2580.55 | 20 3116.29 | 17 4391.38 | 15 2376.77 | 14 2730.05 | 14 3161.80 | 13 2184.39 |
	// |  22 | 21 2011.65 | 21 2823.93 | 21 4567.11 | 18 2580.55 | 22 3116.29 | 19 4391.38 | 17 2376.77 | 16 2730.05 | 16 3161.80 | 15 2184.39 |
	// |  24 | 23 2011.65 | 23 2823.93 | 23 4567.11 | 20 2580.55 | 24 3116.29 | 21 4391.38 | 19 2376.77 | 18 2730.05 | 18 3161.80 | 17 2184.39 |
	// |  26 | 25 2011.65 | 25 2823.93 | 25 4567.11 | 22 2580.55 | 26 3116.29 | 23 4391.38 | 21 2376.77 | 20 2730.05 | 20 3161.80 | 19 2184.39 |
	// |  28 | 27 2011.65 | 27 2823.93 | 27 4567.11 | 24 2580.55 | 28 3116.29 | 25 4391.38 | 23 2376.77 | 22 2730.05 | 22 3161.80 | 21 2184.39 |
	// |  30 | 29 2011.65 | 29 2823.93 | 29 4567.11 | 26 2580.55 | 30 3116.29 | 27 4391.38 | 25 2376.77 | 24 2730.05 | 24 3161.80 | 23 2184.39 |
	// |  32 | 31 2011.65 | 31 2823.93 | 31 4567.11 | 28 2580.55 | 32 3116.29 | 29 4391.38 | 27 2376.77 | 26 2730.05 | 26 3161.80 | 25 2184.39 |
	// |  34 | 33 2011.65 | 33 2823.93 | 33 4567.11 | 30 2580.55 | 34 3116.29 | 31 4391.38 | 29 2376.77 | 28 2730.05 | 28 3161.80 | 27 2184.39 |
	// |  36 | 35 2011.65 | 35 2823.93 | 35 4567.11 | 32 2580.55 | 36 3116.29 | 33 4391.38 | 31 2376.77 | 30 2730.05 | 30 3161.80 | 29 2184.39 |
	// |  38 | 37 2011.65 | 37 2823.93 | 37 4567.11 | 34 2580.55 | 38 3116.29 | 35 4391.38 | 33 2376.77 | 32 2730.05 | 32 3161.80 | 31 2184.39 |
	// |  40 | 39 2011.65 | 39 2823.93 | 39 4567.11 | 36 2580.55 | 40 3116.29 | 37 4391.38 | 35 2376.77 | 34 2730.05 | 34 3161.80 | 33 2184.39 |
	// |  42 | 41 2011.65 | 41 2823.93 | 41 4567.11 | 38 2580.55 | 42 3116.29 | 39 4391.38 | 37 2376.77 | 36 2730.05 | 36 3161.80 | 35 2184.39 |
	// |  44 | 43 2011.65 | 43 2823.93 | 43 4567.11 | 40 2580.55 | 44 3116.29 | 41 4391.38 | 39 2376.77 | 38 2730.05 | 38 3161.80 | 37 2184.39 |
	// |  46 | 45 2011.65 | 45 2823.93 | 45 4567.11 | 42 2580.55 | 46 3116.29 | 43 4391.38 | 41 2376.77 | 40 2730.05 | 40 3161.80 | 39 2184.39 |
	// |  48 | 47 2011.65 | 47 2823.93 | 47 4567.11 | 44 2580.55 | 48 3116.29 | 45 4391.38 | 43 2376.77 | 42 2730.05 | 42 3161.80 | 41 2184.39 |
	// |  50 | 49 2011.65 | 49 2823.93 | 49 4567.11 | 46 2580.55 | 50 3116.29 | 47 4391.38 | 45 2376.77 | 44 2730.05 | 44 3161.80 | 43 2184.39 |
	// |  52 | 51 2011.65 | 51 2823.93 | 51 4567.11 | 48 2580.55 | 52 3116.29 | 49 4391.38 | 47 2376.77 | 46 2730.05 | 46 3161.80 | 45 2184.39 |
	// |  54 | 53 2011.65 | 53 2823.93 | 53 4567.11 | 50 2580.55 | 54 3116.29 | 51 4391.38 | 49 2376.77 | 48 2730.05 | 48 3161.80 | 47 2184.39 |
	// |  56 | 55 2011.65 | 55 2823.93 | 55 4567.11 | 52 2580.55 | 56 3116.29 | 53 4391.38 | 51 2376.77 | 50 2730.05 | 50 3161.80 | 49 2184.39 |
	// |  58 | 57 2011.65 | 57 2823.93 | 57 4567.11 | 54 2580.55 | 58 3116.29 | 55 4391.38 | 53 2376.77 | 52 2730.05 | 52 3161.80 | 51 2184.39 |
	// |  60 | 59 2011.65 | 59 2823.93 | 59 4567.11 | 56 2580.55 | 60 3116.29 | 57 4391.38 | 55 2376.77 | 54 2730.05 | 54 3161.80 | 53 2184.39 |
	// |  62 | 61 2011.65 | 61 2823.93 | 61 4567.11 | 58 2580.55 | 62 3116.29 | 59 4391.38 | 57 2376.77 | 56 2730.05 | 56 3161.80 | 55 2184.39 |
	// |  64 | 63 2011.65 | 63 2823.93 | 63 4567.11 | 60 2580.55 | 64 3116.29 | 61 4391.38 | 59 2376.77 | 58 2730.05 | 58 3161.80 | 57 2184.39 |
	// |  66 | 65 2011.65 | 65 2823.93 | 65 4567.11 | 62 2580.55 | 66 3116.29 | 63 4391.38 | 61 2376.77 | 60 2730.05 | 60 3161.80 | 59 2184.39 |
	// |  68 | 67 2011.65 | 67 2823.93 | 67 4567.11 | 64 2580.55 | 68 3116.29 | 65 4391.38 | 63 2376.77 | 62 2730.05 | 62 3161.80 | 61 2184.39 |
	// |  70 | 69 2011.65 | 69 2823.93 | 69 4567.11 | 66 2580.55 | 70 3116.29 | 67 4391.38 | 65 2376.77 | 64 2730.05 | 64 3161.80 | 63 2184.39 |
	// |  72 | 71 2011.65 | 71 2823.93 | 71 4567.11 | 68 2580.55 | 72 3116.29 | 69 4391.38 | 67 2376.77 | 66 2730.05 | 66 3161.80 | 65 2184.39 |
	// |  74 | 73 2011.65 | 73 2823.93 | 73 4567.11 | 70 2580.55 | 74 3116.29 | 71 4391.38 | 69 2376.77 | 68 2730.05 | 68 3161.80 | 67 2184.39 |
	// |  76 | 75 2011.65 | 75 2823.93 | 75 4567.11 | 72 2580.55 | 76 3116.29 | 73 4391.38 | 71 2376.77 | 70 2730.05 | 70 3161.80 | 69 2184.39 |
	// |  78 | 77 2011.65 | 77 2823.93 | 77 4567.11 | 74 2580.55 | 78 3116.29 | 75 4391.38 | 73 2376.77 | 72 2730.05 | 72 3161.80 | 71 2184.39 |
	// |  80 | 79 2011.65 | 79 2823.93 | 79 4567.11 | 76 2580.55 | 80 3116.29 | 77 4391.38 | 75 2376.77 | 74 2730.05 | 74 3161.80 | 73 2184.39 |
	// |  82 | 81 2011.65 | 81 2823.93 | 81 4567.11 | 78 2580.55 | 82 3116.29 | 79 4391.38 | 77 2376.77 | 76 2730.05 | 76 3161.80 | 75 2184.39 |
	// |  84 | 83 2011.65 | 83 2823.93 | 83 4567.11 | 80 2580.55 | 84 3116.29 | 81 4391.38 | 79 2376.77 | 78 2730.05 | 78 3161.80 | 77 2184.39 |
	// |  86 | 85 2011.65 | 85 2823.93 | 85 4567.11 | 82 2580.55 | 86 3116.29 | 83 4391.38 | 81 2376.77 | 80 2730.05 | 80 3161.80 | 79 2184.39 |
	// |  88 | 87 2011.65 | 87 2823.93 | 87 4567.11 | 84 2580.55 | 88 3116.29 | 85 4391.38 | 83 2376.77 | 82 2730.05 | 82 3161.80 | 81 2184.39 |
	// |  90 | 89 2011.65 | 89 2823.93 | 89 4567.11 | 86 2580.55 | 90 3116.29 | 87 4391.38 | 85 2376.77 | 84 2730.05 | 84 3161.80 | 83 2184.39 |
	// |  92 | 91 2011.65 | 91 2823.93 | 91 4567.11 | 88 2580.55 | 92 3116.29 | 89 4391.38 | 87 2376.77 | 86 2730.05 | 86 3161.80 | 85 2184.39 |
	// |  94 | 93 2011.65 | 93 2823.93 | 93 4567.11 | 90 2580.55 | 94 3116.29 | 91 4391.38 | 89 2376.77 | 88 2730.05 | 88 3161.80 | 87 2184.39 |
	// |  96 | 95 2011.65 | 95 2823.93 | 95 4567.11 | 92 2580.55 | 96 3116.29 | 93 4391.38 | 91 2376.77 | 90 2730.05 | 90 3161.80 | 89 2184.39 |
	// |  98 | 97 2011.65 | 97 2823.93 | 97 4567.11 | 94 2580.55 | 98 3116.29 | 95 4391.38 | 93 2376.77 | 92 2730.05 | 92 3161.80 | 91 2184.39 |
	// +-----+------------+------------+------------+------------+------------+------------+------------+------------+------------+------------+
	// Total bytes=489775145, ranges=956
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
	// |   0 | 1 13326.98 | 1 16616.94 |     0 0.00 |     0 0.00 |     0 0.00 |     0 0.00 |     0 0.00 |     0 0.00 |     0 0.00 |     0 0.00 |     0 0.00 |     0 0.00 |     0 0.00 |     0 0.00 |     0 0.00 |
	// |   2 |  2 3955.65 |  2 6238.15 |  1 1500.16 | 2 18249.96 |     0 0.00 |     0 0.00 |     0 0.00 |     0 0.00 |     0 0.00 |     0 0.00 |     0 0.00 |     0 0.00 |     0 0.00 |     0 0.00 |     0 0.00 |
	// |   4 |  3 2011.65 |  2 1579.59 |  3 1500.16 | 3 12179.64 |  2 2480.10 | 3 10192.77 |     0 0.00 |     0 0.00 |     0 0.00 |     0 0.00 |     0 0.00 |     0 0.00 |     0 0.00 |     0 0.00 |     0 0.00 |
	// |   6 |  5 2011.65 |  4 1579.59 |  5 1500.16 |  3 5261.26 |  3 1401.80 |  3 5277.33 |  3 3581.00 |  3 9331.13 |     0 0.00 |     0 0.00 |     0 0.00 |     0 0.00 |     0 0.00 |     0 0.00 |     0 0.00 |
	// |   8 |  7 2011.65 |  6 1579.59 |  7 1500.16 |  4 3149.47 |  5 1401.80 |  3 1932.54 |  4 2094.91 |  3 4187.30 |  4 6384.09 |  3 5702.41 |     0 0.00 |     0 0.00 |     0 0.00 |     0 0.00 |     0 0.00 |
	// |  10 |  9 2011.65 |  8 1579.59 |  9 1500.16 |  4 1579.69 |  7 1401.80 |  5 1932.54 |  6 2094.91 |  3 1698.58 |  4 3619.15 |  3 1914.40 |  4 4255.69 |  5 6355.75 |     0 0.00 |     0 0.00 |     0 0.00 |
	// |  12 | 11 2011.65 | 10 1579.59 | 11 1500.16 |  6 1579.69 |  9 1401.80 |  7 1932.54 |  8 2094.91 |  5 1698.58 |  4 1754.07 |  5 1914.40 |  4 1731.22 |  5 3551.83 |  3 2448.48 |  4 4745.00 |     0 0.00 |
	// |  14 | 13 2011.65 | 12 1579.59 | 13 1500.16 |  8 1579.69 | 14 4023.20 |  9 1932.54 | 10 2094.91 |  7 1698.58 |  6 1754.07 |  7 1914.40 |  6 1731.22 |  5 1906.08 |  4 1628.85 |  4 2732.90 |  3 1856.07 |
	// |  16 | 15 2011.65 | 16 2068.79 | 16 2223.63 | 10 1579.69 | 14 3379.37 | 11 1932.54 | 12 2094.91 |  9 1698.58 |  8 1754.07 |  9 1914.40 |  8 1731.22 |  7 1906.08 |  6 1628.85 |  5 2164.05 |  5 1856.07 |
	// |  18 | 17 2011.65 | 18 2068.79 | 18 2223.63 | 13 1902.73 | 14 2687.28 | 13 1932.54 | 14 2094.91 | 11 1698.58 | 10 1754.07 | 11 1914.40 | 10 1731.22 |  9 1906.08 |  9 1997.92 |  7 2164.05 |  7 1856.07 |
	// |  20 | 19 2011.65 | 20 2068.79 | 20 2223.63 | 15 1902.73 | 15 2395.33 | 15 1932.54 | 16 2094.91 | 14 1990.53 | 12 1754.07 | 13 1914.40 | 12 1731.22 | 11 1906.08 | 11 1997.92 |  9 2164.05 |  9 1856.07 |
	// |  22 | 21 2011.65 | 22 2068.79 | 22 2223.63 | 17 1902.73 | 17 2395.33 | 17 1932.54 | 18 2094.91 | 16 1990.53 | 14 1754.07 | 15 1914.40 | 14 1731.22 | 13 1906.08 | 13 1997.92 | 11 2164.05 | 11 1856.07 |
	// |  24 | 23 2011.65 | 24 2068.79 | 24 2223.63 | 19 1902.73 | 19 2395.33 | 19 1932.54 | 20 2094.91 | 18 1990.53 | 16 1754.07 | 17 1914.40 | 16 1731.22 | 15 1906.08 | 15 1997.92 | 13 2164.05 | 13 1856.07 |
	// |  26 | 25 2011.65 | 26 2068.79 | 26 2223.63 | 21 1902.73 | 21 2395.33 | 21 1932.54 | 22 2094.91 | 20 1990.53 | 18 1754.07 | 19 1914.40 | 18 1731.22 | 17 1906.08 | 17 1997.92 | 15 2164.05 | 15 1856.07 |
	// |  28 | 27 2011.65 | 28 2068.79 | 28 2223.63 | 23 1902.73 | 23 2395.33 | 23 1932.54 | 24 2094.91 | 22 1990.53 | 20 1754.07 | 21 1914.40 | 20 1731.22 | 19 1906.08 | 19 1997.92 | 17 2164.05 | 17 1856.07 |
	// |  30 | 29 2011.65 | 30 2068.79 | 30 2223.63 | 25 1902.73 | 25 2395.33 | 25 1932.54 | 26 2094.91 | 24 1990.53 | 22 1754.07 | 23 1914.40 | 22 1731.22 | 21 1906.08 | 21 1997.92 | 19 2164.05 | 19 1856.07 |
	// |  32 | 31 2011.65 | 32 2068.79 | 32 2223.63 | 27 1902.73 | 27 2395.33 | 27 1932.54 | 28 2094.91 | 26 1990.53 | 24 1754.07 | 25 1914.40 | 24 1731.22 | 23 1906.08 | 23 1997.92 | 21 2164.05 | 21 1856.07 |
	// |  34 | 33 2011.65 | 34 2068.79 | 34 2223.63 | 29 1902.73 | 29 2395.33 | 29 1932.54 | 30 2094.91 | 28 1990.53 | 26 1754.07 | 27 1914.40 | 26 1731.22 | 25 1906.08 | 25 1997.92 | 23 2164.05 | 23 1856.07 |
	// |  36 | 35 2011.65 | 36 2068.79 | 36 2223.63 | 31 1902.73 | 31 2395.33 | 31 1932.54 | 32 2094.91 | 30 1990.53 | 28 1754.07 | 29 1914.40 | 28 1731.22 | 27 1906.08 | 27 1997.92 | 25 2164.05 | 25 1856.07 |
	// |  38 | 37 2011.65 | 38 2068.79 | 38 2223.63 | 33 1902.73 | 33 2395.33 | 33 1932.54 | 34 2094.91 | 32 1990.53 | 30 1754.07 | 31 1914.40 | 30 1731.22 | 29 1906.08 | 29 1997.92 | 27 2164.05 | 27 1856.07 |
	// |  40 | 39 2011.65 | 40 2068.79 | 40 2223.63 | 35 1902.73 | 35 2395.33 | 35 1932.54 | 36 2094.91 | 34 1990.53 | 32 1754.07 | 33 1914.40 | 32 1731.22 | 31 1906.08 | 31 1997.92 | 29 2164.05 | 29 1856.07 |
	// |  42 | 41 2011.65 | 42 2068.79 | 42 2223.63 | 37 1902.73 | 37 2395.33 | 37 1932.54 | 38 2094.91 | 36 1990.53 | 34 1754.07 | 35 1914.40 | 34 1731.22 | 33 1906.08 | 33 1997.92 | 31 2164.05 | 31 1856.07 |
	// |  44 | 43 2011.65 | 44 2068.79 | 44 2223.63 | 39 1902.73 | 39 2395.33 | 39 1932.54 | 40 2094.91 | 38 1990.53 | 36 1754.07 | 37 1914.40 | 36 1731.22 | 35 1906.08 | 35 1997.92 | 33 2164.05 | 33 1856.07 |
	// |  46 | 45 2011.65 | 46 2068.79 | 46 2223.63 | 41 1902.73 | 41 2395.33 | 41 1932.54 | 42 2094.91 | 40 1990.53 | 38 1754.07 | 39 1914.40 | 38 1731.22 | 37 1906.08 | 37 1997.92 | 35 2164.05 | 35 1856.07 |
	// |  48 | 47 2011.65 | 48 2068.79 | 48 2223.63 | 43 1902.73 | 43 2395.33 | 43 1932.54 | 44 2094.91 | 42 1990.53 | 40 1754.07 | 41 1914.40 | 40 1731.22 | 39 1906.08 | 39 1997.92 | 37 2164.05 | 37 1856.07 |
	// |  50 | 49 2011.65 | 50 2068.79 | 50 2223.63 | 45 1902.73 | 45 2395.33 | 45 1932.54 | 46 2094.91 | 44 1990.53 | 42 1754.07 | 43 1914.40 | 42 1731.22 | 41 1906.08 | 41 1997.92 | 39 2164.05 | 39 1856.07 |
	// |  52 | 51 2011.65 | 52 2068.79 | 52 2223.63 | 47 1902.73 | 47 2395.33 | 47 1932.54 | 48 2094.91 | 46 1990.53 | 44 1754.07 | 45 1914.40 | 44 1731.22 | 43 1906.08 | 43 1997.92 | 41 2164.05 | 41 1856.07 |
	// |  54 | 53 2011.65 | 54 2068.79 | 54 2223.63 | 49 1902.73 | 49 2395.33 | 49 1932.54 | 50 2094.91 | 48 1990.53 | 46 1754.07 | 47 1914.40 | 46 1731.22 | 45 1906.08 | 45 1997.92 | 43 2164.05 | 43 1856.07 |
	// |  56 | 55 2011.65 | 56 2068.79 | 56 2223.63 | 51 1902.73 | 51 2395.33 | 51 1932.54 | 52 2094.91 | 50 1990.53 | 48 1754.07 | 49 1914.40 | 48 1731.22 | 47 1906.08 | 47 1997.92 | 45 2164.05 | 45 1856.07 |
	// |  58 | 57 2011.65 | 58 2068.79 | 58 2223.63 | 53 1902.73 | 53 2395.33 | 53 1932.54 | 54 2094.91 | 52 1990.53 | 50 1754.07 | 51 1914.40 | 50 1731.22 | 49 1906.08 | 49 1997.92 | 47 2164.05 | 47 1856.07 |
	// |  60 | 59 2011.65 | 60 2068.79 | 60 2223.63 | 55 1902.73 | 55 2395.33 | 55 1932.54 | 56 2094.91 | 54 1990.53 | 52 1754.07 | 53 1914.40 | 52 1731.22 | 51 1906.08 | 51 1997.92 | 49 2164.05 | 49 1856.07 |
	// |  62 | 61 2011.65 | 62 2068.79 | 62 2223.63 | 57 1902.73 | 57 2395.33 | 57 1932.54 | 58 2094.91 | 56 1990.53 | 54 1754.07 | 55 1914.40 | 54 1731.22 | 53 1906.08 | 53 1997.92 | 51 2164.05 | 51 1856.07 |
	// |  64 | 63 2011.65 | 64 2068.79 | 64 2223.63 | 59 1902.73 | 59 2395.33 | 59 1932.54 | 60 2094.91 | 58 1990.53 | 56 1754.07 | 57 1914.40 | 56 1731.22 | 55 1906.08 | 55 1997.92 | 53 2164.05 | 53 1856.07 |
	// |  66 | 65 2011.65 | 66 2068.79 | 66 2223.63 | 61 1902.73 | 61 2395.33 | 61 1932.54 | 62 2094.91 | 60 1990.53 | 58 1754.07 | 59 1914.40 | 58 1731.22 | 57 1906.08 | 57 1997.92 | 55 2164.05 | 55 1856.07 |
	// |  68 | 67 2011.65 | 68 2068.79 | 68 2223.63 | 63 1902.73 | 63 2395.33 | 63 1932.54 | 64 2094.91 | 62 1990.53 | 60 1754.07 | 61 1914.40 | 60 1731.22 | 59 1906.08 | 59 1997.92 | 57 2164.05 | 57 1856.07 |
	// |  70 | 69 2011.65 | 70 2068.79 | 70 2223.63 | 65 1902.73 | 65 2395.33 | 65 1932.54 | 66 2094.91 | 64 1990.53 | 62 1754.07 | 63 1914.40 | 62 1731.22 | 61 1906.08 | 61 1997.92 | 59 2164.05 | 59 1856.07 |
	// |  72 | 71 2011.65 | 72 2068.79 | 72 2223.63 | 67 1902.73 | 67 2395.33 | 67 1932.54 | 68 2094.91 | 66 1990.53 | 64 1754.07 | 65 1914.40 | 64 1731.22 | 63 1906.08 | 63 1997.92 | 61 2164.05 | 61 1856.07 |
	// |  74 | 73 2011.65 | 74 2068.79 | 74 2223.63 | 69 1902.73 | 69 2395.33 | 69 1932.54 | 70 2094.91 | 68 1990.53 | 66 1754.07 | 67 1914.40 | 66 1731.22 | 65 1906.08 | 65 1997.92 | 63 2164.05 | 63 1856.07 |
	// |  76 | 75 2011.65 | 76 2068.79 | 76 2223.63 | 71 1902.73 | 71 2395.33 | 71 1932.54 | 72 2094.91 | 70 1990.53 | 68 1754.07 | 69 1914.40 | 68 1731.22 | 67 1906.08 | 67 1997.92 | 65 2164.05 | 65 1856.07 |
	// |  78 | 77 2011.65 | 78 2068.79 | 78 2223.63 | 73 1902.73 | 73 2395.33 | 73 1932.54 | 74 2094.91 | 72 1990.53 | 70 1754.07 | 71 1914.40 | 70 1731.22 | 69 1906.08 | 69 1997.92 | 67 2164.05 | 67 1856.07 |
	// |  80 | 79 2011.65 | 80 2068.79 | 80 2223.63 | 75 1902.73 | 75 2395.33 | 75 1932.54 | 76 2094.91 | 74 1990.53 | 72 1754.07 | 73 1914.40 | 72 1731.22 | 71 1906.08 | 71 1997.92 | 69 2164.05 | 69 1856.07 |
	// |  82 | 81 2011.65 | 82 2068.79 | 82 2223.63 | 77 1902.73 | 77 2395.33 | 77 1932.54 | 78 2094.91 | 76 1990.53 | 74 1754.07 | 75 1914.40 | 74 1731.22 | 73 1906.08 | 73 1997.92 | 71 2164.05 | 71 1856.07 |
	// |  84 | 83 2011.65 | 84 2068.79 | 84 2223.63 | 79 1902.73 | 79 2395.33 | 79 1932.54 | 80 2094.91 | 78 1990.53 | 76 1754.07 | 77 1914.40 | 76 1731.22 | 75 1906.08 | 75 1997.92 | 73 2164.05 | 73 1856.07 |
	// |  86 | 85 2011.65 | 86 2068.79 | 86 2223.63 | 81 1902.73 | 81 2395.33 | 81 1932.54 | 82 2094.91 | 80 1990.53 | 78 1754.07 | 79 1914.40 | 78 1731.22 | 77 1906.08 | 77 1997.92 | 75 2164.05 | 75 1856.07 |
	// |  88 | 87 2011.65 | 88 2068.79 | 88 2223.63 | 83 1902.73 | 83 2395.33 | 83 1932.54 | 84 2094.91 | 82 1990.53 | 80 1754.07 | 81 1914.40 | 80 1731.22 | 79 1906.08 | 79 1997.92 | 77 2164.05 | 77 1856.07 |
	// |  90 | 89 2011.65 | 90 2068.79 | 90 2223.63 | 85 1902.73 | 85 2395.33 | 85 1932.54 | 86 2094.91 | 84 1990.53 | 82 1754.07 | 83 1914.40 | 82 1731.22 | 81 1906.08 | 81 1997.92 | 79 2164.05 | 79 1856.07 |
	// |  92 | 91 2011.65 | 92 2068.79 | 92 2223.63 | 87 1902.73 | 87 2395.33 | 87 1932.54 | 88 2094.91 | 86 1990.53 | 84 1754.07 | 85 1914.40 | 84 1731.22 | 83 1906.08 | 83 1997.92 | 81 2164.05 | 81 1856.07 |
	// |  94 | 93 2011.65 | 94 2068.79 | 94 2223.63 | 89 1902.73 | 89 2395.33 | 89 1932.54 | 90 2094.91 | 88 1990.53 | 86 1754.07 | 87 1914.40 | 86 1731.22 | 85 1906.08 | 85 1997.92 | 83 2164.05 | 83 1856.07 |
	// |  96 | 95 2011.65 | 96 2068.79 | 96 2223.63 | 91 1902.73 | 91 2395.33 | 91 1932.54 | 92 2094.91 | 90 1990.53 | 88 1754.07 | 89 1914.40 | 88 1731.22 | 87 1906.08 | 87 1997.92 | 85 2164.05 | 85 1856.07 |
	// |  98 | 97 2011.65 | 98 2068.79 | 98 2223.63 | 93 1902.73 | 93 2395.33 | 93 1932.54 | 94 2094.91 | 92 1990.53 | 90 1754.07 | 91 1914.40 | 90 1731.22 | 89 1906.08 | 89 1997.92 | 87 2164.05 | 87 1856.07 |
	// +-----+------------+------------+------------+------------+------------+------------+------------+------------+------------+------------+------------+------------+------------+------------+------------+
	// Total bytes=708507194, ranges=1396
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
	clock := hlc.NewClockWithSystemTimeSource(time.Nanosecond /* maxOffset */)

	// Model a set of stores in a cluster,
	// adding / rebalancing ranges of random sizes.
	rpcContext := rpc.NewContext(ctx, rpc.ContextOptions{
		TenantID:  roachpb.SystemTenantID,
		Config:    &base.Config{Insecure: true},
		Clock:     clock.WallClock(),
		MaxOffset: clock.MaxOffset(),
		Stopper:   stopper,
		Settings:  st,
	})
	server := rpc.NewServer(rpcContext) // never started
	g := gossip.NewTest(1, rpcContext, server, stopper, metric.NewRegistry(), zonepb.DefaultZoneConfigRef())

	storepool.TimeUntilStoreDead.Override(ctx, &st.SV, storepool.TestTimeUntilStoreDeadOff)

	const nodes = 20

	// Deterministic must be set as this test is comparing the exact output
	// after each rebalance.
	sp := storepool.NewStorePool(
		ambientCtx,
		st,
		g,
		clock,
		func() int {
			return nodes
		},
		storepool.NewMockNodeLiveness(livenesspb.NodeLivenessStatus_LIVE).NodeLivenessFunc,
		/* deterministic */ true,
	)
	alloc := MakeAllocator(sp, func(string) (time.Duration, bool) {
		return 0, false
	}, nil)

	var wg sync.WaitGroup
	g.RegisterCallback(gossip.MakePrefixPattern(gossip.KeyStoreDescPrefix),
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
				gossip.MakeStoreDescKey(roachpb.StoreID(j)),
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
