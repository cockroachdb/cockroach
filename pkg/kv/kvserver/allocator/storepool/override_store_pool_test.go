// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storepool

import (
	"context"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils/gossiputil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestOverrideStorePoolStatusString tests the status string output of the
// store pool implementation, including any liveness overrides.
func TestOverrideStorePoolStatusString(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	const nodeCount = 5

	stopper, g, _, testStorePool, mnl := CreateTestStorePool(ctx, st,
		liveness.TestTimeUntilNodeDead, false, /* deterministic */
		func() int { return nodeCount }, /* nodeCount */
		livenesspb.NodeLivenessStatus_DEAD)
	defer stopper.Stop(ctx)
	sg := gossiputil.NewStoreGossiper(g)

	livenessOverrides := make(map[roachpb.NodeID]livenesspb.NodeLivenessStatus)
	sp := NewOverrideStorePool(testStorePool, func(nid roachpb.NodeID) livenesspb.NodeLivenessStatus {
		if overriddenLiveness, ok := livenessOverrides[nid]; ok {
			return overriddenLiveness
		}

		return mnl.NodeLivenessFunc(nid)
	}, func() int {
		excluded := 0
		for _, overriddenLiveness := range livenessOverrides {
			if overriddenLiveness == livenesspb.NodeLivenessStatus_DECOMMISSIONING ||
				overriddenLiveness == livenesspb.NodeLivenessStatus_DECOMMISSIONED {
				excluded++
			}
		}
		return nodeCount - excluded
	})

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

	sg.GossipStores(stores, t)
	for i := 1; i <= 5; i++ {
		mnl.SetNodeStatus(roachpb.NodeID(i), livenesspb.NodeLivenessStatus_LIVE)
	}

	// Override node 2 as dead.
	livenessOverrides[roachpb.NodeID(2)] = livenesspb.NodeLivenessStatus_DEAD

	// Override node 3 as decommissioning.
	livenessOverrides[roachpb.NodeID(3)] = livenesspb.NodeLivenessStatus_DECOMMISSIONING

	// Mark node 5 as draining.
	mnl.SetNodeStatus(5, livenesspb.NodeLivenessStatus_DRAINING)

	require.Equal(t,
		"1: range-count=0 fraction-used=0.00\n"+
			"2 (status=dead): range-count=0 fraction-used=0.00\n"+
			"3 (status=decommissioning): range-count=0 fraction-used=0.00\n"+
			"4: range-count=0 fraction-used=0.00\n"+
			"5 (status=draining): range-count=0 fraction-used=0.00\n",
		sp.String(),
	)
}

// TestOverrideStorePoolDecommissioningReplicas validates the ability of to use
// both liveness as well as liveness overrides in determining the set of
// decommissioning replicas.
func TestOverrideStorePoolDecommissioningReplicas(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	const nodeCount = 5

	stopper, g, _, testStorePool, mnl := CreateTestStorePool(ctx, st,
		liveness.TestTimeUntilNodeDead, false, /* deterministic */
		func() int { return nodeCount }, /* nodeCount */
		livenesspb.NodeLivenessStatus_DEAD)
	defer stopper.Stop(ctx)
	sg := gossiputil.NewStoreGossiper(g)

	livenessOverrides := make(map[roachpb.NodeID]livenesspb.NodeLivenessStatus)
	sp := NewOverrideStorePool(testStorePool, func(nid roachpb.NodeID) livenesspb.NodeLivenessStatus {
		if overriddenLiveness, ok := livenessOverrides[nid]; ok {
			return overriddenLiveness
		}

		return mnl.NodeLivenessFunc(nid)
	}, func() int {
		excluded := 0
		for _, overriddenLiveness := range livenessOverrides {
			if overriddenLiveness == livenesspb.NodeLivenessStatus_DECOMMISSIONING ||
				overriddenLiveness == livenesspb.NodeLivenessStatus_DECOMMISSIONED {
				excluded++
			}
		}
		return nodeCount - excluded
	})

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
		mnl.SetNodeStatus(roachpb.NodeID(i), livenesspb.NodeLivenessStatus_LIVE)
	}

	liveReplicas, deadReplicas := sp.LiveAndDeadReplicas(replicas, false /* includeSuspectAndDrainingStores */)
	require.Equalf(t, 5, len(liveReplicas), "expected five live replicas, found %d (%v)", len(liveReplicas), liveReplicas)
	require.Emptyf(t, deadReplicas, "expected no dead replicas initially, found %d (%v)", len(deadReplicas), deadReplicas)
	// Mark node 4 as decommissioning.
	mnl.SetNodeStatus(4, livenesspb.NodeLivenessStatus_DECOMMISSIONING)
	// Mark node 5 as dead.
	mnl.SetNodeStatus(5, livenesspb.NodeLivenessStatus_DEAD)

	// Override node 3 as decommissioning.
	livenessOverrides[roachpb.NodeID(3)] = livenesspb.NodeLivenessStatus_DECOMMISSIONING

	liveReplicas, deadReplicas = sp.LiveAndDeadReplicas(replicas, false /* includeSuspectAndDrainingStores */)
	// Decommissioning replicas are considered live.
	if a, e := liveReplicas, replicas[:4]; !reflect.DeepEqual(a, e) {
		t.Fatalf("expected live replicas %+v; got %+v", e, a)
	}
	if a, e := deadReplicas, replicas[4:]; !reflect.DeepEqual(a, e) {
		t.Fatalf("expected dead replicas %+v; got %+v", e, a)
	}

	decommissioningReplicas := sp.DecommissioningReplicas(replicas)
	if a, e := decommissioningReplicas, replicas[2:4]; !reflect.DeepEqual(a, e) {
		t.Fatalf("expected decommissioning replicas %+v; got %+v", e, a)
	}
}

// TestOverrideStorePoolGetStoreList tests the functionality of the store list
// with and without liveness overrides.
func TestOverrideStorePoolGetStoreList(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	const nodeCount = 8

	// We're going to manually mark stores dead in this test.
	stopper, g, _, testStorePool, mnl := CreateTestStorePool(ctx, st,
		liveness.TestTimeUntilNodeDead, false, /* deterministic */
		func() int { return nodeCount }, /* nodeCount */
		livenesspb.NodeLivenessStatus_DEAD)
	defer stopper.Stop(ctx)
	sg := gossiputil.NewStoreGossiper(g)

	livenessOverrides := make(map[roachpb.NodeID]livenesspb.NodeLivenessStatus)
	sp := NewOverrideStorePool(testStorePool, func(nid roachpb.NodeID) livenesspb.NodeLivenessStatus {
		if overriddenLiveness, ok := livenessOverrides[nid]; ok {
			return overriddenLiveness
		}

		return mnl.NodeLivenessFunc(nid)
	}, func() int {
		excluded := 0
		for _, overriddenLiveness := range livenessOverrides {
			if overriddenLiveness == livenesspb.NodeLivenessStatus_DECOMMISSIONING ||
				overriddenLiveness == livenesspb.NodeLivenessStatus_DECOMMISSIONED {
				excluded++
			}
		}
		return nodeCount - excluded
	})

	constraints := []roachpb.ConstraintsConjunction{
		{
			Constraints: []roachpb.Constraint{
				{Type: roachpb.Constraint_REQUIRED, Value: "ssd"},
				{Type: roachpb.Constraint_REQUIRED, Value: "dc"},
			},
		},
	}
	required := []string{"ssd", "dc"}
	// Nothing yet.
	sl, _, _ := sp.GetStoreList(StoreFilterNone)
	sl = sl.ExcludeInvalid(constraints)
	require.Emptyf(t, sl.Stores, "expected no stores, instead %+v", sl.Stores)

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
	decommissioningStore := roachpb.StoreDescriptor{
		StoreID: 6,
		Node:    roachpb.NodeDescriptor{NodeID: 6},
		Attrs:   roachpb.Attributes{Attrs: required},
	}
	absentStore := roachpb.StoreDescriptor{
		StoreID: 7,
		Node:    roachpb.NodeDescriptor{NodeID: 7},
		Attrs:   roachpb.Attributes{Attrs: required},
	}
	suspectedStore := roachpb.StoreDescriptor{
		StoreID: 8,
		Node:    roachpb.NodeDescriptor{NodeID: 8},
		Attrs:   roachpb.Attributes{Attrs: required},
	}

	// Gossip and mark all alive initially.
	sg.GossipStores([]*roachpb.StoreDescriptor{
		&matchingStore,
		&supersetStore,
		&unmatchingStore,
		&emptyStore,
		&deadStore,
		&decommissioningStore,
		&suspectedStore,
		// absentStore is purposefully not gossiped.
	}, t)
	for i := 1; i <= 8; i++ {
		mnl.SetNodeStatus(roachpb.NodeID(i), livenesspb.NodeLivenessStatus_LIVE)
	}

	// Set deadStore as dead.
	livenessOverrides[deadStore.Node.NodeID] = livenesspb.NodeLivenessStatus_DEAD

	// Set decommissioningStore as decommissioning.
	livenessOverrides[decommissioningStore.Node.NodeID] = livenesspb.NodeLivenessStatus_DECOMMISSIONING

	// Set suspectedStore as suspected.
	testStorePool.DetailsMu.Lock()
	testStorePool.DetailsMu.StoreDetails[suspectedStore.StoreID].LastUnavailable = testStorePool.clock.Now()
	testStorePool.DetailsMu.Unlock()

	// No filter or limited set of store IDs.
	require.NoError(t, verifyStoreList(
		sp,
		constraints,
		nil, /* storeIDs */
		StoreFilterNone,
		[]int{
			int(matchingStore.StoreID),
			int(supersetStore.StoreID),
			int(suspectedStore.StoreID),
		},
		/* expectedAliveStoreCount */ 5,
		/* expectedThrottledStoreCount */ 1,
	))

	// Filter out suspected stores but don't limit the set of store IDs.
	require.NoError(t, verifyStoreList(
		sp,
		constraints,
		nil, /* storeIDs */
		StoreFilterSuspect,
		[]int{
			int(matchingStore.StoreID),
			int(supersetStore.StoreID),
		},
		/* expectedAliveStoreCount */ 5,
		/* expectedThrottledStoreCount */ 1,
	))

	limitToStoreIDs := roachpb.StoreIDSlice{
		matchingStore.StoreID,
		decommissioningStore.StoreID,
		absentStore.StoreID,
		suspectedStore.StoreID,
	}

	// No filter but limited to limitToStoreIDs.
	// Note that supersetStore is not included.
	require.NoError(t, verifyStoreList(
		sp,
		constraints,
		limitToStoreIDs,
		StoreFilterNone,
		[]int{
			int(matchingStore.StoreID),
			int(suspectedStore.StoreID),
		},
		/* expectedAliveStoreCount */ 2,
		/* expectedThrottledStoreCount */ 1,
	))

	// Filter out suspected stores and limit to limitToStoreIDs.
	// Note that suspectedStore is not included.
	require.NoError(t, verifyStoreList(
		sp,
		constraints,
		limitToStoreIDs,
		StoreFilterSuspect,
		[]int{
			int(matchingStore.StoreID),
		},
		/* expectedAliveStoreCount */ 2,
		/* expectedThrottledStoreCount */ 1,
	))
}
