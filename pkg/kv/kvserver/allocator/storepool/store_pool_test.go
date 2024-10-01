// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storepool

import (
	"context"
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils/gossiputil"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/kr/pretty"
	"github.com/stretchr/testify/require"
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

// TestStorePoolGossipUpdate ensures that the gossip callback in StorePool
// correctly updates a store's details.
func TestStorePoolGossipUpdate(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	stopper, g, _, sp, _ := CreateTestStorePool(ctx, st,
		liveness.TestTimeUntilNodeDead, false, /* deterministic */
		func() int { return 0 }, /* NodeCount */
		livenesspb.NodeLivenessStatus_DEAD)
	defer stopper.Stop(ctx)
	sg := gossiputil.NewStoreGossiper(g)

	sp.DetailsMu.RLock()
	if _, ok := sp.DetailsMu.StoreDetails[2]; ok {
		t.Fatalf("store 2 is already in the pool's store list")
	}
	sp.DetailsMu.RUnlock()

	sg.GossipStores(uniqueStore, t)

	sp.DetailsMu.RLock()
	if _, ok := sp.DetailsMu.StoreDetails[2]; !ok {
		t.Fatalf("store 2 isn't in the pool's store list")
	}
	sp.DetailsMu.RUnlock()
}

// verifyStoreList ensures that the returned list of stores is correct.
func verifyStoreList(
	sp AllocatorStorePool,
	constraints []roachpb.ConstraintsConjunction,
	storeIDs roachpb.StoreIDSlice, // optional
	filter StoreFilter,
	expected []int,
	expectedAliveStoreCount int,
	expectedThrottledStoreCount int,
) error {
	var sl StoreList
	var aliveStoreCount int
	var throttled ThrottledStoreReasons
	if storeIDs == nil {
		sl, aliveStoreCount, throttled = sp.GetStoreList(filter)
	} else {
		sl, aliveStoreCount, throttled = sp.GetStoreListFromIDs(storeIDs, filter)
	}
	throttledStoreCount := len(throttled)
	sl = sl.ExcludeInvalid(constraints)
	if aliveStoreCount != expectedAliveStoreCount {
		return errors.Errorf("expected AliveStoreCount %d does not match actual %d",
			expectedAliveStoreCount, aliveStoreCount)
	}
	if throttledStoreCount != expectedThrottledStoreCount {
		return errors.Errorf("expected ThrottledStoreCount %d does not match actual %d",
			expectedThrottledStoreCount, throttledStoreCount)
	}
	var actual []int
	for _, store := range sl.Stores {
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
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	// We're going to manually mark stores dead in this test.
	stopper, g, _, sp, mnl := CreateTestStorePool(ctx, st,
		liveness.TestTimeUntilNodeDead, false, /* deterministic */
		func() int { return 10 }, /* nodeCount */
		livenesspb.NodeLivenessStatus_DEAD)
	defer stopper.Stop(ctx)
	sg := gossiputil.NewStoreGossiper(g)
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
	if len(sl.Stores) != 0 {
		t.Errorf("expected no stores, instead %+v", sl.Stores)
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
		&declinedStore,
		&suspectedStore,
		// absentStore is purposefully not gossiped.
	}, t)
	for i := 1; i <= 8; i++ {
		mnl.SetNodeStatus(roachpb.NodeID(i), livenesspb.NodeLivenessStatus_LIVE)
	}

	// Set deadStore as dead.
	mnl.SetNodeStatus(deadStore.Node.NodeID, livenesspb.NodeLivenessStatus_DEAD)
	sp.DetailsMu.Lock()
	// Set declinedStore as throttled.
	sp.DetailsMu.StoreDetails[declinedStore.StoreID].ThrottledUntil = sp.clock.Now().AddDuration(time.Hour)
	// Set suspectedStore as suspected.
	sp.DetailsMu.StoreDetails[suspectedStore.StoreID].LastUnavailable = sp.clock.Now()
	sp.DetailsMu.Unlock()

	// No filter or limited set of store IDs.
	if err := verifyStoreList(
		sp,
		constraints,
		nil, /* storeIDs */
		StoreFilterNone,
		[]int{
			int(matchingStore.StoreID),
			int(supersetStore.StoreID),
			int(declinedStore.StoreID),
			int(suspectedStore.StoreID),
		},
		/* expectedAliveStoreCount */ 6,
		/* expectedThrottledStoreCount */ 2,
	); err != nil {
		t.Error(err)
	}

	// Filter out throttled stores but don't limit the set of store IDs.
	if err := verifyStoreList(
		sp,
		constraints,
		nil, /* storeIDs */
		StoreFilterThrottled,
		[]int{
			int(matchingStore.StoreID),
			int(supersetStore.StoreID),
		},
		/* expectedAliveStoreCount */ 6,
		/* expectedThrottledStoreCount */ 2,
	); err != nil {
		t.Error(err)
	}

	// Filter out suspected stores but don't limit the set of store IDs.
	if err := verifyStoreList(
		sp,
		constraints,
		nil, /* storeIDs */
		StoreFilterSuspect,
		[]int{
			int(matchingStore.StoreID),
			int(supersetStore.StoreID),
			int(declinedStore.StoreID),
		},
		/* expectedAliveStoreCount */ 6,
		/* expectedThrottledStoreCount */ 2,
	); err != nil {
		t.Error(err)
	}

	limitToStoreIDs := roachpb.StoreIDSlice{
		matchingStore.StoreID,
		declinedStore.StoreID,
		absentStore.StoreID,
		suspectedStore.StoreID,
	}

	// No filter but limited to limitToStoreIDs.
	// Note that supersetStore is not included.
	if err := verifyStoreList(
		sp,
		constraints,
		limitToStoreIDs,
		StoreFilterNone,
		[]int{
			int(matchingStore.StoreID),
			int(declinedStore.StoreID),
			int(suspectedStore.StoreID),
		},
		/* expectedAliveStoreCount */ 3,
		/* expectedThrottledStoreCount */ 2,
	); err != nil {
		t.Error(err)
	}

	// Filter out throttled stores and limit to limitToStoreIDs.
	// Note that supersetStore is not included.
	if err := verifyStoreList(
		sp,
		constraints,
		limitToStoreIDs,
		StoreFilterThrottled,
		[]int{
			int(matchingStore.StoreID),
		},
		/* expectedAliveStoreCount */ 3,
		/* expectedThrottledStoreCount */ 2,
	); err != nil {
		t.Error(err)
	}

	// Filter out suspected stores and limit to limitToStoreIDs.
	// Note that suspectedStore is not included.
	if err := verifyStoreList(
		sp,
		constraints,
		limitToStoreIDs,
		StoreFilterSuspect,
		[]int{
			int(matchingStore.StoreID),
			int(declinedStore.StoreID),
		},
		/* expectedAliveStoreCount */ 3,
		/* expectedThrottledStoreCount */ 2,
	); err != nil {
		t.Error(err)
	}
}

// TestStoreListFilter ensures that the store list constraint filtering works
// properly.
func TestStoreListFilter(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	constraints := []roachpb.ConstraintsConjunction{
		{
			Constraints: []roachpb.Constraint{
				{Type: roachpb.Constraint_REQUIRED, Key: "region", Value: "us-west"},
				{Type: roachpb.Constraint_REQUIRED, Value: "MustMatch"},
				{Type: roachpb.Constraint_PROHIBITED, Value: "MustNotMatch"},
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
		sl.Stores = append(sl.Stores, storeDesc)
		if s.expected {
			expected = append(expected, storeDesc)
		}
	}

	filtered := sl.ExcludeInvalid(constraints)
	if !reflect.DeepEqual(expected, filtered.Stores) {
		t.Errorf("did not get expected stores %s", pretty.Diff(expected, filtered.Stores))
	}
}

func TestStorePoolGetStoreDetails(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	stopper, g, _, sp, _ := CreateTestStorePool(ctx, st,
		liveness.TestTimeUntilNodeDead, false, /* deterministic */
		func() int { return 10 }, /* nodeCount */
		livenesspb.NodeLivenessStatus_DEAD)
	defer stopper.Stop(ctx)
	sg := gossiputil.NewStoreGossiper(g)
	sg.GossipStores(uniqueStore, t)

	sp.DetailsMu.Lock()
	defer sp.DetailsMu.Unlock()
	if detail := sp.GetStoreDetailLocked(roachpb.StoreID(1)); detail.Desc != nil {
		t.Errorf("unexpected fetched store ID 1: %+v", detail.Desc)
	}
	if detail := sp.GetStoreDetailLocked(roachpb.StoreID(2)); detail.Desc == nil {
		t.Errorf("failed to fetch store ID 2")
	}
}

func TestStorePoolFindDeadReplicas(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	stopper, g, _, sp, mnl := CreateTestStorePool(ctx, st,
		liveness.TestTimeUntilNodeDead, false, /* deterministic */
		func() int { return 10 }, /* nodeCount */
		livenesspb.NodeLivenessStatus_DEAD)
	defer stopper.Stop(ctx)
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
		mnl.SetNodeStatus(roachpb.NodeID(i), livenesspb.NodeLivenessStatus_LIVE)
	}

	liveReplicas, deadReplicas := sp.LiveAndDeadReplicas(replicas, false /* includeSuspectAndDrainingStores */)
	if len(liveReplicas) != 5 {
		t.Fatalf("expected five live replicas, found %d (%v)", len(liveReplicas), liveReplicas)
	}
	if len(deadReplicas) > 0 {
		t.Fatalf("expected no dead replicas initially, found %d (%v)", len(deadReplicas), deadReplicas)
	}
	// Mark nodes 4 & 5 as dead.
	mnl.SetNodeStatus(4, livenesspb.NodeLivenessStatus_DEAD)
	mnl.SetNodeStatus(5, livenesspb.NodeLivenessStatus_DEAD)

	liveReplicas, deadReplicas = sp.LiveAndDeadReplicas(replicas, false /* includeSuspectNodes */)
	if a, e := liveReplicas, replicas[:3]; !reflect.DeepEqual(a, e) {
		t.Fatalf("expected live replicas %+v; got %+v", e, a)
	}
	if a, e := deadReplicas, replicas[3:]; !reflect.DeepEqual(a, e) {
		t.Fatalf("expected dead replicas %+v; got %+v", e, a)
	}

	// Mark node 4 as merely unavailable.
	mnl.SetNodeStatus(4, livenesspb.NodeLivenessStatus_UNAVAILABLE)

	liveReplicas, deadReplicas = sp.LiveAndDeadReplicas(replicas, false /* includeSuspectAndDrainingStores */)
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
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	stopper, _, _, sp, _ := CreateTestStorePool(ctx, st,
		liveness.TestTimeUntilNodeDead, false, /* deterministic */
		func() int { return 10 }, /* nodeCount */
		livenesspb.NodeLivenessStatus_DEAD)
	defer stopper.Stop(ctx)

	liveReplicas, deadReplicas := sp.LiveAndDeadReplicas(
		[]roachpb.ReplicaDescriptor{{StoreID: 1}},
		false, /* includeSuspectAndDrainingStores */
	)
	if len(liveReplicas) != 0 || len(deadReplicas) != 0 {
		t.Errorf("expected 0 live and 0 dead replicas; got %v and %v", liveReplicas, deadReplicas)
	}

	sl, alive, throttled := sp.GetStoreList(StoreFilterNone)
	if len(sl.Stores) > 0 {
		t.Errorf("expected no live stores; got list of %v", sl)
	}
	if alive != 0 {
		t.Errorf("expected no live stores; got a live count of %d", alive)
	}
	if len(throttled) != 0 {
		t.Errorf("expected no live stores; got throttled %v", throttled)
	}
}

func TestStorePoolThrottle(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	stopper, g, _, sp, _ := CreateTestStorePool(ctx, st,
		liveness.TestTimeUntilNodeDead, false, /* deterministic */
		func() int { return 10 }, /* nodeCount */
		livenesspb.NodeLivenessStatus_DEAD)
	defer stopper.Stop(ctx)

	sg := gossiputil.NewStoreGossiper(g)
	sg.GossipStores(uniqueStore, t)

	expected := sp.clock.Now().AddDuration(FailedReservationsTimeout.Get(&sp.st.SV))
	sp.Throttle(ThrottleFailed, "", 1)

	sp.DetailsMu.Lock()
	detail := sp.GetStoreDetailLocked(1)
	sp.DetailsMu.Unlock()
	if detail.ThrottledUntil.WallTime != expected.WallTime {
		t.Errorf("expected store to have been throttled to %v, found %v",
			expected, detail.ThrottledUntil)
	}
}

// See state transition diagram in storeDetail.status() for a visual
// representation of what this test asserts.
func TestStorePoolSuspected(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	stopper, g, _, sp, mnl := CreateTestStorePool(ctx, st,
		liveness.TestTimeUntilNodeDeadOff, false, /* deterministic */
		func() int { return 10 }, /* nodeCount */
		livenesspb.NodeLivenessStatus_DEAD)
	defer stopper.Stop(ctx)

	now := sp.clock.Now()
	timeUntilNodeDead := liveness.TimeUntilNodeDead.Get(&sp.st.SV)
	timeAfterNodeSuspect := liveness.TimeAfterNodeSuspect.Get(&sp.st.SV)

	// Verify a store that we haven't seen yet is unknown status.
	detail := sp.GetStoreDetailLocked(0)
	s := detail.status(now, timeUntilNodeDead, sp.NodeLivenessFn, timeAfterNodeSuspect)
	require.Equal(t, s, storeStatusUnknown)
	require.Equal(t, hlc.Timestamp{}, detail.LastUnavailable)

	// Now start gossiping the stores statuses.
	sg := gossiputil.NewStoreGossiper(g)
	sg.GossipStores(uniqueStore, t)
	store := uniqueStore[0]

	// Store starts in a live state if it hasn't been marked suspect yet.
	mnl.SetNodeStatus(store.Node.NodeID, livenesspb.NodeLivenessStatus_LIVE)
	sp.DetailsMu.Lock()
	detail = sp.GetStoreDetailLocked(store.StoreID)
	defer sp.DetailsMu.Unlock()

	s = detail.status(now, timeUntilNodeDead, sp.NodeLivenessFn, timeAfterNodeSuspect)
	require.Equal(t, s, storeStatusAvailable)
	require.Equal(t, hlc.Timestamp{}, detail.LastUnavailable)

	// When the store transitions to unavailable, its status changes to temporarily unknown.
	mnl.SetNodeStatus(store.Node.NodeID, livenesspb.NodeLivenessStatus_UNAVAILABLE)
	s = detail.status(now, timeUntilNodeDead, sp.NodeLivenessFn, timeAfterNodeSuspect)
	require.Equal(t, s, storeStatusUnknown)
	require.NotEqual(t, hlc.Timestamp{}, detail.LastUnavailable)

	// When the store transitions back to live, it passes through suspect for a period.
	mnl.SetNodeStatus(store.Node.NodeID, livenesspb.NodeLivenessStatus_LIVE)
	s = detail.status(now, timeUntilNodeDead, sp.NodeLivenessFn, timeAfterNodeSuspect)
	require.Equal(t, s, storeStatusSuspect)

	// Once the window has passed, it will return to available.
	now = now.AddDuration(timeAfterNodeSuspect).AddDuration(time.Millisecond)
	s = detail.status(now, timeUntilNodeDead, sp.NodeLivenessFn, timeAfterNodeSuspect)
	require.Equal(t, s, storeStatusAvailable)

	// Return a liveness of dead.
	mnl.SetNodeStatus(store.Node.NodeID, livenesspb.NodeLivenessStatus_DEAD)
	s = detail.status(now, timeUntilNodeDead, sp.NodeLivenessFn, timeAfterNodeSuspect)
	require.Equal(t, s, storeStatusDead)

	// When the store transitions back to live, it passes through suspect for a period.
	mnl.SetNodeStatus(store.Node.NodeID, livenesspb.NodeLivenessStatus_LIVE)
	s = detail.status(now, timeUntilNodeDead, sp.NodeLivenessFn, timeAfterNodeSuspect)
	require.Equal(t, s, storeStatusSuspect)

	// Verify it also returns correctly to available after suspect time.
	now = now.AddDuration(timeAfterNodeSuspect).AddDuration(time.Millisecond)
	s = detail.status(now, timeUntilNodeDead, sp.NodeLivenessFn, timeAfterNodeSuspect)
	require.Equal(t, s, storeStatusAvailable)

	// Verify that restart after draining also makes it temporarily suspect.
	mnl.SetNodeStatus(store.Node.NodeID, livenesspb.NodeLivenessStatus_DRAINING)
	s = detail.status(now, timeUntilNodeDead, sp.NodeLivenessFn, timeAfterNodeSuspect)
	require.Equal(t, s, storeStatusDraining)

	// Verify suspect when restarting after a drain.
	mnl.SetNodeStatus(store.Node.NodeID, livenesspb.NodeLivenessStatus_LIVE)
	s = detail.status(now, timeUntilNodeDead, sp.NodeLivenessFn, timeAfterNodeSuspect)
	require.Equal(t, s, storeStatusSuspect)

	now = now.AddDuration(timeAfterNodeSuspect).AddDuration(time.Millisecond)
	mnl.SetNodeStatus(store.Node.NodeID, livenesspb.NodeLivenessStatus_LIVE)
	s = detail.status(now, timeUntilNodeDead, sp.NodeLivenessFn, timeAfterNodeSuspect)
	require.Equal(t, s, storeStatusAvailable)
}

func TestGetLocalities(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	stopper, g, _, sp, _ := CreateTestStorePool(ctx, st,
		liveness.TestTimeUntilNodeDead, false, /* deterministic */
		func() int { return 10 }, /* nodeCount */
		livenesspb.NodeLivenessStatus_DEAD)
	defer stopper.Stop(ctx)
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
			Locality: createLocality(tierCount - 1),
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
		existingReplicas = append(existingReplicas,
			roachpb.ReplicaDescriptor{
				NodeID:  store.Node.NodeID,
				StoreID: store.StoreID,
			},
		)
	}

	localitiesByStore := sp.GetLocalitiesByStore(existingReplicas)
	localitiesByNode := sp.GetLocalitiesByNode(existingReplicas)
	for _, store := range stores {
		storeID := store.StoreID
		nodeID := store.Node.NodeID
		localityByStore, ok := localitiesByStore[storeID]
		if !ok {
			t.Fatalf("could not find locality for store %d", storeID)
		}
		localityByNode, ok := localitiesByNode[nodeID]
		require.Truef(t, ok, "could not find locality for node %d", nodeID)
		require.Equal(t, int(nodeID), len(localityByStore.Tiers))
		require.Equal(t, localityByStore.Tiers[len(localityByStore.Tiers)-1],
			roachpb.Tier{Key: "node", Value: nodeID.String()})
		require.Equal(t, int(nodeID)-1, len(localityByNode.Tiers))
		require.Equal(t, createLocality(int(nodeID)-1).String(), sp.GetNodeLocalityString(nodeID))
	}
}

func TestStorePoolDecommissioningReplicas(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	stopper, g, _, sp, mnl := CreateTestStorePool(ctx, st,
		liveness.TestTimeUntilNodeDead, false, /* deterministic */
		func() int { return 10 }, /* nodeCount */
		livenesspb.NodeLivenessStatus_DEAD)
	defer stopper.Stop(ctx)
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
		mnl.SetNodeStatus(roachpb.NodeID(i), livenesspb.NodeLivenessStatus_LIVE)
	}

	liveReplicas, deadReplicas := sp.LiveAndDeadReplicas(replicas, false /* includeSuspectAndDrainingStores */)
	if len(liveReplicas) != 5 {
		t.Fatalf("expected five live replicas, found %d (%v)", len(liveReplicas), liveReplicas)
	}
	if len(deadReplicas) > 0 {
		t.Fatalf("expected no dead replicas initially, found %d (%v)", len(deadReplicas), deadReplicas)
	}
	// Mark node 4 as decommissioning.
	mnl.SetNodeStatus(4, livenesspb.NodeLivenessStatus_DECOMMISSIONING)
	// Mark node 5 as dead.
	mnl.SetNodeStatus(5, livenesspb.NodeLivenessStatus_DEAD)

	liveReplicas, deadReplicas = sp.LiveAndDeadReplicas(replicas, false /* includeSuspectAndDrainingStores */)
	// Decommissioning replicas are considered live.
	if a, e := liveReplicas, replicas[:4]; !reflect.DeepEqual(a, e) {
		t.Fatalf("expected live replicas %+v; got %+v", e, a)
	}
	if a, e := deadReplicas, replicas[4:]; !reflect.DeepEqual(a, e) {
		t.Fatalf("expected dead replicas %+v; got %+v", e, a)
	}

	decommissioningReplicas := sp.DecommissioningReplicas(replicas)
	if a, e := decommissioningReplicas, replicas[3:4]; !reflect.DeepEqual(a, e) {
		t.Fatalf("expected decommissioning replicas %+v; got %+v", e, a)
	}
}

// TestStorePoolString tests the string output of the store pool
// implementation.
func TestStorePoolString(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const nodeCount = 9

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	stopper, g, _, sp, mnl := CreateTestStorePool(ctx, st,
		liveness.TestTimeUntilNodeDead, false, /* deterministic */
		func() int { return nodeCount },
		livenesspb.NodeLivenessStatus_DEAD)
	defer stopper.Stop(ctx)
	sg := gossiputil.NewStoreGossiper(g)

	stores := []*roachpb.StoreDescriptor{}
	for i := 1; i <= nodeCount; i++ {
		stores = append(stores, &roachpb.StoreDescriptor{
			StoreID: roachpb.StoreID(i),
			Node:    roachpb.NodeDescriptor{NodeID: roachpb.NodeID(i)},
			Capacity: roachpb.StoreCapacity{
				Capacity:   100,
				Available:  int64(100 - i*10),
				RangeCount: int32(i * 10),
			},
		})
	}

	sg.GossipStores(stores, t)
	mnl.SetNodeStatus(1, livenesspb.NodeLivenessStatus_UNKNOWN)
	mnl.SetNodeStatus(2, livenesspb.NodeLivenessStatus_DEAD)
	mnl.SetNodeStatus(3, livenesspb.NodeLivenessStatus_UNAVAILABLE)
	mnl.SetNodeStatus(4, livenesspb.NodeLivenessStatus_LIVE)
	mnl.SetNodeStatus(5, livenesspb.NodeLivenessStatus_DECOMMISSIONING)
	mnl.SetNodeStatus(6, livenesspb.NodeLivenessStatus_DECOMMISSIONED)
	mnl.SetNodeStatus(7, livenesspb.NodeLivenessStatus_DRAINING)
	mnl.SetNodeStatus(8, livenesspb.NodeLivenessStatus_LIVE)
	mnl.SetNodeStatus(9, livenesspb.NodeLivenessStatus_LIVE)
	sp.DetailsMu.StoreDetails[8].LastUnavailable = sp.clock.Now()
	sp.DetailsMu.StoreDetails[9].ThrottledUntil = sp.clock.Now().AddDuration(time.Second)

	require.Equal(t, "1 (status=unknown): range-count=10 fraction-used=0.10\n"+
		"2 (status=dead): range-count=20 fraction-used=0.20\n"+
		"3 (status=unknown): range-count=30 fraction-used=0.30\n"+
		"4: range-count=40 fraction-used=0.40\n"+
		"5 (status=decommissioning): range-count=50 fraction-used=0.50\n"+
		"6 (status=dead): range-count=60 fraction-used=0.60\n"+
		"7 (status=draining): range-count=70 fraction-used=0.70\n"+
		"8 (status=suspect): range-count=80 fraction-used=0.80\n"+
		"9 (status=throttled): range-count=90 fraction-used=0.90 [throttled=1s]\n",
		sp.String())
}

// TestStoreListString tests the string output of store list.
func TestStoreListString(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const nodeCount = 5
	const scale = 10
	stores := []roachpb.StoreDescriptor{}
	for i := 1; i <= nodeCount; i++ {
		stores = append(stores, roachpb.StoreDescriptor{
			StoreID: roachpb.StoreID(i),
			Node:    roachpb.NodeDescriptor{NodeID: roachpb.NodeID(i)},
			Capacity: roachpb.StoreCapacity{
				LogicalBytes:     int64(i << scale),
				RangeCount:       int32(i * scale),
				LeaseCount:       int32(i * scale),
				QueriesPerSecond: float64(i * scale),
				CPUPerSecond:     float64(i << scale),
				IOThreshold: admissionpb.IOThreshold{
					L0NumSubLevels:          int64(i),
					L0NumSubLevelsThreshold: scale,
					L0NumFiles:              int64(i),
					L0NumFilesThreshold:     scale,
				},
				IOThresholdMax: admissionpb.IOThreshold{
					L0NumSubLevels:          10 * int64(i),
					L0NumSubLevelsThreshold: scale,
					L0NumFiles:              10 * int64(i),
					L0NumFilesThreshold:     scale,
				},
			},
		})
	}

	require.Equal(t,
		"  candidate: avg-ranges=0.00 avg-leases=0.00 avg-disk-usage=0 B avg-queries-per-second=0.00 avg-store-cpu-per-second=0µs avg-io-overload=0.00(max=0.00) <no candidates>",
		MakeStoreList([]roachpb.StoreDescriptor{}).String())

	require.Equal(t, "  candidate: avg-ranges=30.00 avg-leases=30.00 avg-disk-usage=3.0 KiB avg-queries-per-second=30.00 avg-store-cpu-per-second=3µs avg-io-overload=0.30(max=3.00)\n"+
		"  1: ranges=10 leases=10 disk-usage=1.0 KiB queries-per-second=10.00 store-cpu-per-second=1µs io-overload=0.10(max=1.00)\n"+
		"  2: ranges=20 leases=20 disk-usage=2.0 KiB queries-per-second=20.00 store-cpu-per-second=2µs io-overload=0.20(max=2.00)\n"+
		"  3: ranges=30 leases=30 disk-usage=3.0 KiB queries-per-second=30.00 store-cpu-per-second=3µs io-overload=0.30(max=3.00)\n"+
		"  4: ranges=40 leases=40 disk-usage=4.0 KiB queries-per-second=40.00 store-cpu-per-second=4µs io-overload=0.40(max=4.00)\n"+
		"  5: ranges=50 leases=50 disk-usage=5.0 KiB queries-per-second=50.00 store-cpu-per-second=5µs io-overload=0.50(max=5.00)\n",
		MakeStoreList(stores).String())
}
