// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/allocatorimpl"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/storepool"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/load"
	"github.com/cockroachdb/cockroach/pkg/raft"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/raft/tracker"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils/gossiputil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

const firstRangeID = roachpb.RangeID(1)

var simpleSpanConfig = &roachpb.SpanConfig{
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

var threeStores = []*roachpb.StoreDescriptor{
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
		Attrs:   roachpb.Attributes{Attrs: []string{"ssd"}},
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
}

var fourSingleStoreRacks = []*roachpb.StoreDescriptor{
	{
		StoreID: 1,
		Attrs:   roachpb.Attributes{Attrs: []string{"red"}},
		Node: roachpb.NodeDescriptor{
			NodeID: 1,
			Locality: roachpb.Locality{
				Tiers: []roachpb.Tier{
					{
						Key:   "region",
						Value: "local",
					},
					{
						Key:   "rack",
						Value: "1",
					},
				},
			},
		},
		Capacity: roachpb.StoreCapacity{
			Capacity:     200,
			Available:    100,
			LogicalBytes: 100,
		},
	},
	{
		StoreID: 2,
		Attrs:   roachpb.Attributes{Attrs: []string{"red"}},
		Node: roachpb.NodeDescriptor{
			NodeID: 2,
			Locality: roachpb.Locality{
				Tiers: []roachpb.Tier{
					{
						Key:   "region",
						Value: "local",
					},
					{
						Key:   "rack",
						Value: "2",
					},
				},
			},
		},
		Capacity: roachpb.StoreCapacity{
			Capacity:     200,
			Available:    100,
			LogicalBytes: 100,
		},
	},
	{
		StoreID: 3,
		Attrs:   roachpb.Attributes{Attrs: []string{"black"}},
		Node: roachpb.NodeDescriptor{
			NodeID: 3,
			Locality: roachpb.Locality{
				Tiers: []roachpb.Tier{
					{
						Key:   "region",
						Value: "local",
					},
					{
						Key:   "rack",
						Value: "3",
					},
				},
			},
		},
		Capacity: roachpb.StoreCapacity{
			Capacity:     200,
			Available:    100,
			LogicalBytes: 100,
		},
	},
	{
		StoreID: 4,
		Attrs:   roachpb.Attributes{Attrs: []string{"black"}},
		Node: roachpb.NodeDescriptor{
			NodeID: 4,
			Locality: roachpb.Locality{
				Tiers: []roachpb.Tier{
					{
						Key:   "region",
						Value: "local",
					},
					{
						Key:   "rack",
						Value: "4",
					},
				},
			},
		},
		Capacity: roachpb.StoreCapacity{
			Capacity:     200,
			Available:    100,
			LogicalBytes: 100,
		},
	},
}

// TestAllocatorRebalanceTarget could help us to verify whether we'll rebalance
// to a target that we'll immediately remove.
func TestAllocatorRebalanceTarget(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	clock := hlc.NewClockForTesting(timeutil.NewManualTime(timeutil.Unix(0, 123)))
	ctx := context.Background()
	stopper, g, sp, a, _ := allocatorimpl.CreateTestAllocator(ctx, 5, false /* deterministic */)
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
	repl.shMu.state.Stats = &enginepb.MVCCStats{}
	repl.loadStats = load.NewReplicaLoad(clock, nil)

	var rangeUsageInfo allocator.RangeUsageInfo

	status := &raft.Status{
		Progress: make(map[raftpb.PeerID]tracker.Progress),
	}
	status.Lead = 1
	status.RaftState = raftpb.StateLeader
	status.Commit = 10
	for _, replica := range replicas {
		status.Progress[raftpb.PeerID(replica.ReplicaID)] = tracker.Progress{
			Match: 10,
			State: tracker.StateReplicate,
		}
	}
	for i := 0; i < 10; i++ {
		result, _, details, ok := a.RebalanceVoter(
			ctx,
			sp,
			&roachpb.SpanConfig{},
			status,
			replicas,
			nil,
			rangeUsageInfo,
			storepool.StoreFilterThrottled,
			a.ScorerOptions(ctx),
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
			sp,
			&roachpb.SpanConfig{},
			status,
			replicas,
			nil,
			rangeUsageInfo,
			storepool.StoreFilterThrottled,
			a.ScorerOptions(ctx),
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
			sp,
			&roachpb.SpanConfig{},
			status,
			replicas,
			nil,
			rangeUsageInfo,
			storepool.StoreFilterThrottled,
			a.ScorerOptions(ctx),
		)
		expTo := stores[1].StoreID
		expFrom := stores[0].StoreID
		if !ok || target.StoreID != expTo || origin.StoreID != expFrom {
			t.Fatalf("%d: expected rebalance from either of %v to s%d, but got %v->%v; details: %s",
				i, expFrom, expTo, origin, target, details)
		}
	}
}

// TestAllocatorThrottled ensures that when a store is throttled, the replica
// will not be sent to purgatory.
func TestAllocatorThrottled(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper, g, sp, a, _ := allocatorimpl.CreateTestAllocator(ctx, 10, false /* deterministic */)
	defer stopper.Stop(ctx)

	// First test to make sure we would send the replica to purgatory.
	_, _, err := a.AllocateVoter(ctx, sp, simpleSpanConfig, []roachpb.ReplicaDescriptor{}, nil, nil, allocatorimpl.Dead)
	if _, ok := IsPurgatoryError(err); !ok {
		t.Fatalf("expected a purgatory error, got: %+v", err)
	}

	// Second, test the normal case in which we can allocate to the store.
	gossiputil.NewStoreGossiper(g).GossipStores(singleStore, t)
	result, _, err := a.AllocateVoter(ctx, sp, simpleSpanConfig, []roachpb.ReplicaDescriptor{}, nil, nil, allocatorimpl.Dead)
	if err != nil {
		t.Fatalf("unable to perform allocation: %+v", err)
	}
	if result.NodeID != 1 || result.StoreID != 1 {
		t.Errorf("expected NodeID 1 and StoreID 1: %+v", result)
	}

	// Finally, set that store to be throttled and ensure we don't send the
	// replica to purgatory.
	sp.DetailsMu.Lock()
	storeDetail, ok := sp.DetailsMu.StoreDetails[singleStore[0].StoreID]
	if !ok {
		t.Fatalf("store:%d was not found in the store pool", singleStore[0].StoreID)
	}
	storeDetail.ThrottledUntil = hlc.Timestamp{WallTime: timeutil.Now().Add(24 * time.Hour).UnixNano()}
	sp.DetailsMu.Unlock()
	_, _, err = a.AllocateVoter(ctx, sp, simpleSpanConfig, []roachpb.ReplicaDescriptor{}, nil, nil, allocatorimpl.Dead)
	if _, ok := IsPurgatoryError(err); ok {
		t.Fatalf("expected a non purgatory error, got: %+v", err)
	}
}
