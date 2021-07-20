// Copyright 2015 The Cockroach Authors.
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
	"math/rand"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils/gossiputil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
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

type mockNodeLiveness struct {
	syncutil.Mutex
	defaultNodeStatus livenesspb.NodeLivenessStatus
	nodes             map[roachpb.NodeID]livenesspb.NodeLivenessStatus
}

func newMockNodeLiveness(defaultNodeStatus livenesspb.NodeLivenessStatus) *mockNodeLiveness {
	return &mockNodeLiveness{
		defaultNodeStatus: defaultNodeStatus,
		nodes:             map[roachpb.NodeID]livenesspb.NodeLivenessStatus{},
	}
}

func (m *mockNodeLiveness) setNodeStatus(
	nodeID roachpb.NodeID, status livenesspb.NodeLivenessStatus,
) {
	m.Lock()
	defer m.Unlock()
	m.nodes[nodeID] = status
}

func (m *mockNodeLiveness) nodeLivenessFunc(
	nodeID roachpb.NodeID, now time.Time, threshold time.Duration,
) livenesspb.NodeLivenessStatus {
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
	timeUntilStoreDeadValue time.Duration,
	deterministic bool,
	nodeCount NodeCountFunc,
	defaultNodeStatus livenesspb.NodeLivenessStatus,
) (*stop.Stopper, *gossip.Gossip, *hlc.ManualClock, *StorePool, *mockNodeLiveness) {
	stopper := stop.NewStopper()
	mc := hlc.NewManualClock(123)
	clock := hlc.NewClock(mc.UnixNano, time.Nanosecond)
	st := cluster.MakeTestingClusterSettings()
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
	mnl := newMockNodeLiveness(defaultNodeStatus)

	TimeUntilStoreDead.Override(context.Background(), &st.SV, timeUntilStoreDeadValue)
	storePool := NewStorePool(
		log.AmbientContext{Tracer: st.Tracer},
		st,
		g,
		clock,
		nodeCount,
		mnl.nodeLivenessFunc,
		deterministic,
	)
	return stopper, g, mc, storePool, mnl
}

// TestStorePoolGossipUpdate ensures that the gossip callback in StorePool
// correctly updates a store's details.
func TestStorePoolGossipUpdate(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	stopper, g, _, sp, _ := createTestStorePool(
		TestTimeUntilStoreDead, false, /* deterministic */
		func() int { return 0 }, /* NodeCount */
		livenesspb.NodeLivenessStatus_DEAD)
	defer stopper.Stop(context.Background())
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
	constraints []zonepb.ConstraintsConjunction,
	storeIDs roachpb.StoreIDSlice, // optional
	filter storeFilter,
	expected []int,
	expectedAliveStoreCount int,
	expectedThrottledStoreCount int,
) error {
	var sl StoreList
	var aliveStoreCount int
	var throttled throttledStoreReasons
	if storeIDs == nil {
		sl, aliveStoreCount, throttled = sp.getStoreList(filter)
	} else {
		sl, aliveStoreCount, throttled = sp.getStoreListFromIDs(storeIDs, filter)
	}
	throttledStoreCount := len(throttled)
	sl = sl.filter(constraints)
	if aliveStoreCount != expectedAliveStoreCount {
		return errors.Errorf("expected AliveStoreCount %d does not match actual %d",
			expectedAliveStoreCount, aliveStoreCount)
	}
	if throttledStoreCount != expectedThrottledStoreCount {
		return errors.Errorf("expected ThrottledStoreCount %d does not match actual %d",
			expectedThrottledStoreCount, throttledStoreCount)
	}
	var actual []int
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
	defer log.Scope(t).Close(t)
	// We're going to manually mark stores dead in this test.
	stopper, g, _, sp, mnl := createTestStorePool(
		TestTimeUntilStoreDead, false, /* deterministic */
		func() int { return 10 }, /* nodeCount */
		livenesspb.NodeLivenessStatus_DEAD)
	defer stopper.Stop(context.Background())
	sg := gossiputil.NewStoreGossiper(g)
	constraints := []zonepb.ConstraintsConjunction{
		{
			Constraints: []zonepb.Constraint{
				{Type: zonepb.Constraint_REQUIRED, Value: "ssd"},
				{Type: zonepb.Constraint_REQUIRED, Value: "dc"},
			},
		},
	}
	required := []string{"ssd", "dc"}
	// Nothing yet.
	sl, _, _ := sp.getStoreList(storeFilterNone)
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
		mnl.setNodeStatus(roachpb.NodeID(i), livenesspb.NodeLivenessStatus_LIVE)
	}

	// Set deadStore as dead.
	mnl.setNodeStatus(deadStore.Node.NodeID, livenesspb.NodeLivenessStatus_DEAD)
	sp.detailsMu.Lock()
	// Set declinedStore as throttled.
	sp.detailsMu.storeDetails[declinedStore.StoreID].throttledUntil = sp.clock.Now().GoTime().Add(time.Hour)
	// Set suspectedStore as suspected.
	sp.detailsMu.storeDetails[suspectedStore.StoreID].lastAvailable = sp.clock.Now().GoTime()
	sp.detailsMu.storeDetails[suspectedStore.StoreID].lastUnavailable = sp.clock.Now().GoTime()
	sp.detailsMu.Unlock()

	// No filter or limited set of store IDs.
	if err := verifyStoreList(
		sp,
		constraints,
		nil, /* storeIDs */
		storeFilterNone,
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
		storeFilterThrottled,
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
		storeFilterSuspect,
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
		storeFilterNone,
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
		storeFilterThrottled,
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
		storeFilterSuspect,
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

	constraints := []zonepb.ConstraintsConjunction{
		{
			Constraints: []zonepb.Constraint{
				{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "us-west"},
				{Type: zonepb.Constraint_REQUIRED, Value: "MustMatch"},
				{Type: zonepb.Constraint_DEPRECATED_POSITIVE, Value: "MatchingOptional"},
				{Type: zonepb.Constraint_PROHIBITED, Value: "MustNotMatch"},
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
		sl.stores = append(sl.stores, storeDesc)
		if s.expected {
			expected = append(expected, storeDesc)
		}
	}

	filtered := sl.filter(constraints)
	if !reflect.DeepEqual(expected, filtered.stores) {
		t.Errorf("did not get expected stores %s", pretty.Diff(expected, filtered.stores))
	}
}

func TestStorePoolUpdateLocalStore(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	manual := hlc.NewManualClock(123)
	clock := hlc.NewClock(manual.UnixNano, time.Nanosecond)
	// We're going to manually mark stores dead in this test.
	stopper, g, _, sp, _ := createTestStorePool(
		TestTimeUntilStoreDead, false, /* deterministic */
		func() int { return 10 }, /* nodeCount */
		livenesspb.NodeLivenessStatus_DEAD)
	defer stopper.Stop(context.Background())
	sg := gossiputil.NewStoreGossiper(g)
	stores := []*roachpb.StoreDescriptor{
		{
			StoreID: 1,
			Node:    roachpb.NodeDescriptor{NodeID: 1},
			Capacity: roachpb.StoreCapacity{
				Capacity:         100,
				Available:        50,
				RangeCount:       5,
				LeaseCount:       1,
				LogicalBytes:     30,
				QueriesPerSecond: 100,
				WritesPerSecond:  30,
			},
		},
		{
			StoreID: 2,
			Node:    roachpb.NodeDescriptor{NodeID: 2},
			Capacity: roachpb.StoreCapacity{
				Capacity:         100,
				Available:        55,
				RangeCount:       4,
				LeaseCount:       2,
				LogicalBytes:     25,
				QueriesPerSecond: 50,
				WritesPerSecond:  25,
			},
		},
	}
	sg.GossipStores(stores, t)

	replica := &Replica{RangeID: 1}
	replica.mu.Lock()
	replica.mu.state.Stats = &enginepb.MVCCStats{
		KeyBytes: 2,
		ValBytes: 4,
	}
	replica.mu.Unlock()
	rs := newReplicaStats(clock, nil)
	for _, store := range stores {
		rs.record(store.Node.NodeID)
	}
	manual.Increment(int64(MinStatsDuration + time.Second))
	replica.leaseholderStats = rs
	replica.writeStats = rs

	rangeUsageInfo := rangeUsageInfoForRepl(replica)

	sp.updateLocalStoreAfterRebalance(roachpb.StoreID(1), rangeUsageInfo, roachpb.ADD_VOTER)
	desc, ok := sp.getStoreDescriptor(roachpb.StoreID(1))
	if !ok {
		t.Fatalf("couldn't find StoreDescriptor for Store ID %d", 1)
	}
	QPS, _ := replica.leaseholderStats.avgQPS()
	WPS, _ := replica.writeStats.avgQPS()
	if expectedRangeCount := int32(6); desc.Capacity.RangeCount != expectedRangeCount {
		t.Errorf("expected RangeCount %d, but got %d", expectedRangeCount, desc.Capacity.RangeCount)
	}
	if expectedBytes := int64(36); desc.Capacity.LogicalBytes != expectedBytes {
		t.Errorf("expected logical bytes %d, but got %d", expectedBytes, desc.Capacity.LogicalBytes)
	}
	if expectedQPS := float64(100); desc.Capacity.QueriesPerSecond != expectedQPS {
		t.Errorf("expected QueriesPerSecond %f, but got %f", expectedQPS, desc.Capacity.QueriesPerSecond)
	}
	if expectedWPS := 30 + WPS; desc.Capacity.WritesPerSecond != expectedWPS {
		t.Errorf("expected WritesPerSecond %f, but got %f", expectedWPS, desc.Capacity.WritesPerSecond)
	}

	sp.updateLocalStoreAfterRebalance(roachpb.StoreID(2), rangeUsageInfo, roachpb.REMOVE_VOTER)
	desc, ok = sp.getStoreDescriptor(roachpb.StoreID(2))
	if !ok {
		t.Fatalf("couldn't find StoreDescriptor for Store ID %d", 2)
	}
	if expectedRangeCount := int32(3); desc.Capacity.RangeCount != expectedRangeCount {
		t.Errorf("expected RangeCount %d, but got %d", expectedRangeCount, desc.Capacity.RangeCount)
	}
	if expectedBytes := int64(19); desc.Capacity.LogicalBytes != expectedBytes {
		t.Errorf("expected logical bytes %d, but got %d", expectedBytes, desc.Capacity.LogicalBytes)
	}
	if expectedQPS := float64(50); desc.Capacity.QueriesPerSecond != expectedQPS {
		t.Errorf("expected QueriesPerSecond %f, but got %f", expectedQPS, desc.Capacity.QueriesPerSecond)
	}
	if expectedWPS := 25 - WPS; desc.Capacity.WritesPerSecond != expectedWPS {
		t.Errorf("expected WritesPerSecond %f, but got %f", expectedWPS, desc.Capacity.WritesPerSecond)
	}

	sp.updateLocalStoresAfterLeaseTransfer(roachpb.StoreID(1), roachpb.StoreID(2), rangeUsageInfo.QueriesPerSecond)
	desc, ok = sp.getStoreDescriptor(roachpb.StoreID(1))
	if !ok {
		t.Fatalf("couldn't find StoreDescriptor for Store ID %d", 1)
	}
	if expectedLeaseCount := int32(0); desc.Capacity.LeaseCount != expectedLeaseCount {
		t.Errorf("expected LeaseCount %d, but got %d", expectedLeaseCount, desc.Capacity.LeaseCount)
	}
	if expectedQPS := 100 - QPS; desc.Capacity.QueriesPerSecond != expectedQPS {
		t.Errorf("expected QueriesPerSecond %f, but got %f", expectedQPS, desc.Capacity.QueriesPerSecond)
	}
	desc, ok = sp.getStoreDescriptor(roachpb.StoreID(2))
	if !ok {
		t.Fatalf("couldn't find StoreDescriptor for Store ID %d", 2)
	}
	if expectedLeaseCount := int32(3); desc.Capacity.LeaseCount != expectedLeaseCount {
		t.Errorf("expected LeaseCount %d, but got %d", expectedLeaseCount, desc.Capacity.LeaseCount)
	}
	if expectedQPS := 50 + QPS; desc.Capacity.QueriesPerSecond != expectedQPS {
		t.Errorf("expected QueriesPerSecond %f, but got %f", expectedQPS, desc.Capacity.QueriesPerSecond)
	}
}

// TestStorePoolUpdateLocalStoreBeforeGossip verifies that an attempt to update
// the local copy of store before that store has been gossiped will be a no-op.
func TestStorePoolUpdateLocalStoreBeforeGossip(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	manual := hlc.NewManualClock(123)
	clock := hlc.NewClock(manual.UnixNano, time.Nanosecond)
	stopper, _, _, sp, _ := createTestStorePool(
		TestTimeUntilStoreDead, false, /* deterministic */
		func() int { return 10 }, /* nodeCount */
		livenesspb.NodeLivenessStatus_DEAD)
	defer stopper.Stop(ctx)

	// Create store.
	node := roachpb.NodeDescriptor{NodeID: roachpb.NodeID(1)}
	eng := storage.NewDefaultInMemForTesting()
	stopper.AddCloser(eng)
	cfg := TestStoreConfig(clock)
	cfg.Transport = NewDummyRaftTransport(cfg.Settings)
	store := NewStore(ctx, cfg, eng, &node)
	// Fake an ident because this test doesn't want to start the store
	// but without an Ident there will be NPEs.
	store.Ident = &roachpb.StoreIdent{
		ClusterID: uuid.Nil,
		StoreID:   1,
		NodeID:    1,
	}

	// Create replica.
	rg := roachpb.RangeDescriptor{
		RangeID:       1,
		StartKey:      roachpb.RKey([]byte("a")),
		EndKey:        roachpb.RKey([]byte("b")),
		NextReplicaID: 1,
	}
	rg.AddReplica(1, 1, roachpb.VOTER_FULL)
	replica, err := newReplica(ctx, &rg, store, 1)
	if err != nil {
		t.Fatalf("make replica error : %+v", err)
	}
	replica.leaseholderStats = newReplicaStats(store.Clock(), nil)

	rangeUsageInfo := rangeUsageInfoForRepl(replica)

	// Update StorePool, which should be a no-op.
	storeID := roachpb.StoreID(1)
	if _, ok := sp.getStoreDescriptor(storeID); ok {
		t.Fatalf("StoreDescriptor not gossiped, should not be found")
	}
	sp.updateLocalStoreAfterRebalance(storeID, rangeUsageInfo, roachpb.ADD_VOTER)
	if _, ok := sp.getStoreDescriptor(storeID); ok {
		t.Fatalf("StoreDescriptor still not gossiped, should not be found")
	}
}

func TestStorePoolGetStoreDetails(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	stopper, g, _, sp, _ := createTestStorePool(
		TestTimeUntilStoreDead, false, /* deterministic */
		func() int { return 10 }, /* nodeCount */
		livenesspb.NodeLivenessStatus_DEAD)
	defer stopper.Stop(context.Background())
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
	defer log.Scope(t).Close(t)
	stopper, g, _, sp, mnl := createTestStorePool(
		TestTimeUntilStoreDead, false, /* deterministic */
		func() int { return 10 }, /* nodeCount */
		livenesspb.NodeLivenessStatus_DEAD)
	defer stopper.Stop(context.Background())
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
		mnl.setNodeStatus(roachpb.NodeID(i), livenesspb.NodeLivenessStatus_LIVE)
	}

	liveReplicas, deadReplicas := sp.liveAndDeadReplicas(replicas)
	if len(liveReplicas) != 5 {
		t.Fatalf("expected five live replicas, found %d (%v)", len(liveReplicas), liveReplicas)
	}
	if len(deadReplicas) > 0 {
		t.Fatalf("expected no dead replicas initially, found %d (%v)", len(deadReplicas), deadReplicas)
	}
	// Mark nodes 4 & 5 as dead.
	mnl.setNodeStatus(4, livenesspb.NodeLivenessStatus_DEAD)
	mnl.setNodeStatus(5, livenesspb.NodeLivenessStatus_DEAD)

	liveReplicas, deadReplicas = sp.liveAndDeadReplicas(replicas)
	if a, e := liveReplicas, replicas[:3]; !reflect.DeepEqual(a, e) {
		t.Fatalf("expected live replicas %+v; got %+v", e, a)
	}
	if a, e := deadReplicas, replicas[3:]; !reflect.DeepEqual(a, e) {
		t.Fatalf("expected dead replicas %+v; got %+v", e, a)
	}

	// Mark node 4 as merely unavailable.
	mnl.setNodeStatus(4, livenesspb.NodeLivenessStatus_UNAVAILABLE)

	liveReplicas, deadReplicas = sp.liveAndDeadReplicas(replicas)
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
	stopper, _, _, sp, _ := createTestStorePool(
		TestTimeUntilStoreDead, false, /* deterministic */
		func() int { return 10 }, /* nodeCount */
		livenesspb.NodeLivenessStatus_DEAD)
	defer stopper.Stop(context.Background())

	liveReplicas, deadReplicas := sp.liveAndDeadReplicas([]roachpb.ReplicaDescriptor{{StoreID: 1}})
	if len(liveReplicas) != 0 || len(deadReplicas) != 0 {
		t.Errorf("expected 0 live and 0 dead replicas; got %v and %v", liveReplicas, deadReplicas)
	}

	sl, alive, throttled := sp.getStoreList(storeFilterNone)
	if len(sl.stores) > 0 {
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
	stopper, g, _, sp, _ := createTestStorePool(
		TestTimeUntilStoreDead, false, /* deterministic */
		func() int { return 10 }, /* nodeCount */
		livenesspb.NodeLivenessStatus_DEAD)
	defer stopper.Stop(context.Background())

	sg := gossiputil.NewStoreGossiper(g)
	sg.GossipStores(uniqueStore, t)

	{
		expected := sp.clock.Now().GoTime().Add(DeclinedReservationsTimeout.Get(&sp.st.SV))
		sp.throttle(throttleDeclined, "", 1)

		sp.detailsMu.Lock()
		detail := sp.getStoreDetailLocked(1)
		sp.detailsMu.Unlock()
		if !detail.throttledUntil.Equal(expected) {
			t.Errorf("expected store to have been throttled to %v, found %v",
				expected, detail.throttledUntil)
		}
	}

	{
		expected := sp.clock.Now().GoTime().Add(FailedReservationsTimeout.Get(&sp.st.SV))
		sp.throttle(throttleFailed, "", 1)

		sp.detailsMu.Lock()
		detail := sp.getStoreDetailLocked(1)
		sp.detailsMu.Unlock()
		if !detail.throttledUntil.Equal(expected) {
			t.Errorf("expected store to have been throttled to %v, found %v",
				expected, detail.throttledUntil)
		}
	}
}

func TestStorePoolSuspected(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	stopper, g, _, sp, mnl := createTestStorePool(
		TestTimeUntilStoreDeadOff, false, /* deterministic */
		func() int { return 10 }, /* nodeCount */
		livenesspb.NodeLivenessStatus_DEAD)
	defer stopper.Stop(context.Background())

	sg := gossiputil.NewStoreGossiper(g)
	sg.GossipStores(uniqueStore, t)
	store := uniqueStore[0]

	now := sp.clock.Now().GoTime()
	timeUntilStoreDead := TimeUntilStoreDead.Get(&sp.st.SV)
	timeAfterStoreSuspect := TimeAfterStoreSuspect.Get(&sp.st.SV)

	mnl.setNodeStatus(store.Node.NodeID, livenesspb.NodeLivenessStatus_LIVE)
	sp.detailsMu.Lock()
	detail := sp.getStoreDetailLocked(store.StoreID)
	s := detail.status(now, timeUntilStoreDead, sp.nodeLivenessFn, timeAfterStoreSuspect)
	sp.detailsMu.Unlock()
	require.Equal(t, s, storeStatusAvailable)
	require.False(t, detail.lastAvailable.IsZero())
	require.True(t, detail.lastUnavailable.IsZero())

	mnl.setNodeStatus(store.Node.NodeID, livenesspb.NodeLivenessStatus_UNAVAILABLE)
	sp.detailsMu.Lock()
	s = detail.status(now, timeUntilStoreDead, sp.nodeLivenessFn, timeAfterStoreSuspect)
	sp.detailsMu.Unlock()
	require.Equal(t, s, storeStatusSuspect)
	require.False(t, detail.lastAvailable.IsZero())
	require.False(t, detail.lastUnavailable.IsZero())

	mnl.setNodeStatus(store.Node.NodeID, livenesspb.NodeLivenessStatus_LIVE)
	sp.detailsMu.Lock()
	s = detail.status(now.Add(timeAfterStoreSuspect).Add(time.Millisecond),
		timeUntilStoreDead, sp.nodeLivenessFn, timeAfterStoreSuspect)
	sp.detailsMu.Unlock()
	require.Equal(t, s, storeStatusAvailable)

	mnl.setNodeStatus(store.Node.NodeID, livenesspb.NodeLivenessStatus_DRAINING)
	sp.detailsMu.Lock()
	s = detail.status(now,
		timeUntilStoreDead, sp.nodeLivenessFn, timeAfterStoreSuspect)
	sp.detailsMu.Unlock()
	require.Equal(t, s, storeStatusUnknown)
	require.True(t, detail.lastAvailable.IsZero())

	mnl.setNodeStatus(store.Node.NodeID, livenesspb.NodeLivenessStatus_LIVE)
	sp.detailsMu.Lock()
	s = detail.status(now.Add(timeAfterStoreSuspect).Add(time.Millisecond),
		timeUntilStoreDead, sp.nodeLivenessFn, timeAfterStoreSuspect)
	sp.detailsMu.Unlock()
	require.Equal(t, s, storeStatusAvailable)
	require.False(t, detail.lastAvailable.IsZero())
}

func TestGetLocalities(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	stopper, g, _, sp, _ := createTestStorePool(
		TestTimeUntilStoreDead, false, /* deterministic */
		func() int { return 10 }, /* nodeCount */
		livenesspb.NodeLivenessStatus_DEAD)
	defer stopper.Stop(context.Background())
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

	localitiesByStore := sp.getLocalitiesByStore(existingReplicas)
	localitiesByNode := sp.getLocalitiesByNode(existingReplicas)
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
		require.Equal(t, createLocality(int(nodeID)-1).String(), sp.getNodeLocalityString(nodeID))
	}
}

func TestStorePoolDecommissioningReplicas(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	stopper, g, _, sp, mnl := createTestStorePool(
		TestTimeUntilStoreDead, false, /* deterministic */
		func() int { return 10 }, /* nodeCount */
		livenesspb.NodeLivenessStatus_DEAD)
	defer stopper.Stop(context.Background())
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
		mnl.setNodeStatus(roachpb.NodeID(i), livenesspb.NodeLivenessStatus_LIVE)
	}

	liveReplicas, deadReplicas := sp.liveAndDeadReplicas(replicas)
	if len(liveReplicas) != 5 {
		t.Fatalf("expected five live replicas, found %d (%v)", len(liveReplicas), liveReplicas)
	}
	if len(deadReplicas) > 0 {
		t.Fatalf("expected no dead replicas initially, found %d (%v)", len(deadReplicas), deadReplicas)
	}
	// Mark node 4 as decommissioning.
	mnl.setNodeStatus(4, livenesspb.NodeLivenessStatus_DECOMMISSIONING)
	// Mark node 5 as dead.
	mnl.setNodeStatus(5, livenesspb.NodeLivenessStatus_DEAD)

	liveReplicas, deadReplicas = sp.liveAndDeadReplicas(replicas)
	// Decommissioning replicas are considered live.
	if a, e := liveReplicas, replicas[:4]; !reflect.DeepEqual(a, e) {
		t.Fatalf("expected live replicas %+v; got %+v", e, a)
	}
	if a, e := deadReplicas, replicas[4:]; !reflect.DeepEqual(a, e) {
		t.Fatalf("expected dead replicas %+v; got %+v", e, a)
	}

	decommissioningReplicas := sp.decommissioningReplicas(replicas)
	if a, e := decommissioningReplicas, replicas[3:4]; !reflect.DeepEqual(a, e) {
		t.Fatalf("expected decommissioning replicas %+v; got %+v", e, a)
	}
}

func TestNodeLivenessLivenessStatus(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	now := timeutil.Now()
	threshold := 5 * time.Minute

	for _, tc := range []struct {
		liveness livenesspb.Liveness
		expected livenesspb.NodeLivenessStatus
	}{
		// Valid status.
		{
			liveness: livenesspb.Liveness{
				NodeID: 1,
				Epoch:  1,
				Expiration: hlc.LegacyTimestamp{
					WallTime: now.Add(5 * time.Minute).UnixNano(),
				},
				Draining: false,
			},
			expected: livenesspb.NodeLivenessStatus_LIVE,
		},
		{
			liveness: livenesspb.Liveness{
				NodeID: 1,
				Epoch:  1,
				Expiration: hlc.LegacyTimestamp{
					// Expires just slightly in the future.
					WallTime: now.UnixNano() + 1,
				},
				Draining: false,
			},
			expected: livenesspb.NodeLivenessStatus_LIVE,
		},
		// Expired status.
		{
			liveness: livenesspb.Liveness{
				NodeID: 1,
				Epoch:  1,
				Expiration: hlc.LegacyTimestamp{
					// Just expired.
					WallTime: now.UnixNano(),
				},
				Draining: false,
			},
			expected: livenesspb.NodeLivenessStatus_UNAVAILABLE,
		},
		// Expired status.
		{
			liveness: livenesspb.Liveness{
				NodeID: 1,
				Epoch:  1,
				Expiration: hlc.LegacyTimestamp{
					WallTime: now.UnixNano(),
				},
				Draining: false,
			},
			expected: livenesspb.NodeLivenessStatus_UNAVAILABLE,
		},
		// Max bound of expired.
		{
			liveness: livenesspb.Liveness{
				NodeID: 1,
				Epoch:  1,
				Expiration: hlc.LegacyTimestamp{
					WallTime: now.Add(-threshold).UnixNano() + 1,
				},
				Draining: false,
			},
			expected: livenesspb.NodeLivenessStatus_UNAVAILABLE,
		},
		// Dead status.
		{
			liveness: livenesspb.Liveness{
				NodeID: 1,
				Epoch:  1,
				Expiration: hlc.LegacyTimestamp{
					WallTime: now.Add(-threshold).UnixNano(),
				},
				Draining: false,
			},
			expected: livenesspb.NodeLivenessStatus_DEAD,
		},
		// Decommissioning.
		{
			liveness: livenesspb.Liveness{
				NodeID: 1,
				Epoch:  1,
				Expiration: hlc.LegacyTimestamp{
					WallTime: now.Add(time.Second).UnixNano(),
				},
				Membership: livenesspb.MembershipStatus_DECOMMISSIONING,
				Draining:   false,
			},
			expected: livenesspb.NodeLivenessStatus_DECOMMISSIONING,
		},
		// Decommissioning + expired.
		{
			liveness: livenesspb.Liveness{
				NodeID: 1,
				Epoch:  1,
				Expiration: hlc.LegacyTimestamp{
					WallTime: now.Add(-threshold).UnixNano(),
				},
				Membership: livenesspb.MembershipStatus_DECOMMISSIONING,
				Draining:   false,
			},
			expected: livenesspb.NodeLivenessStatus_DECOMMISSIONED,
		},
		// Decommissioned + live.
		{
			liveness: livenesspb.Liveness{
				NodeID: 1,
				Epoch:  1,
				Expiration: hlc.LegacyTimestamp{
					WallTime: now.Add(time.Second).UnixNano(),
				},
				Membership: livenesspb.MembershipStatus_DECOMMISSIONED,
				Draining:   false,
			},
			// Despite having marked the node as fully decommissioned, through
			// this NodeLivenessStatus API we still surface the node as
			// "Decommissioning". See #50707 for more details.
			expected: livenesspb.NodeLivenessStatus_DECOMMISSIONING,
		},
		// Decommissioned + expired.
		{
			liveness: livenesspb.Liveness{
				NodeID: 1,
				Epoch:  1,
				Expiration: hlc.LegacyTimestamp{
					WallTime: now.Add(-threshold).UnixNano(),
				},
				Membership: livenesspb.MembershipStatus_DECOMMISSIONED,
				Draining:   false,
			},
			expected: livenesspb.NodeLivenessStatus_DECOMMISSIONED,
		},
		// Draining (reports as unavailable).
		{
			liveness: livenesspb.Liveness{
				NodeID: 1,
				Epoch:  1,
				Expiration: hlc.LegacyTimestamp{
					WallTime: now.Add(5 * time.Minute).UnixNano(),
				},
				Draining: true,
			},
			expected: livenesspb.NodeLivenessStatus_DRAINING,
		},
	} {
		t.Run("", func(t *testing.T) {
			if a, e := LivenessStatus(tc.liveness, now, threshold), tc.expected; a != e {
				t.Errorf("liveness status was %s, wanted %s", a.String(), e.String())
			}
		})
	}
}
