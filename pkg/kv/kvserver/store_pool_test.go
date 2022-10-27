// Copyright 2022 The Cockroach Authors.
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
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/storepool"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/replicastats"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils/gossiputil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

func TestStorePoolUpdateLocalStore(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	manual := timeutil.NewManualTime(timeutil.Unix(0, 123))
	clock := hlc.NewClock(manual, time.Nanosecond /* maxOffset */)
	ctx := context.Background()
	// We're going to manually mark stores dead in this test.
	stopper, g, _, sp, _ := storepool.CreateTestStorePool(ctx,
		storepool.TestTimeUntilStoreDead, false, /* deterministic */
		func() int { return 10 }, /* nodeCount */
		livenesspb.NodeLivenessStatus_DEAD)
	defer stopper.Stop(ctx)
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
				L0Sublevels:      4,
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
				L0Sublevels:      8,
			},
		},
	}
	sg.GossipStores(stores, t)

	replica := Replica{RangeID: 1}
	replica.mu.Lock()
	replica.mu.state.Stats = &enginepb.MVCCStats{
		KeyBytes: 2,
		ValBytes: 4,
	}
	replica.mu.Unlock()
	replica.loadStats = NewReplicaLoad(clock, nil)
	for _, store := range stores {
		replica.loadStats.batchRequests.RecordCount(1, store.Node.NodeID)
		replica.loadStats.writeKeys.RecordCount(1, store.Node.NodeID)
	}
	manual.Advance(replicastats.MinStatsDuration + time.Second)

	rangeUsageInfo := RangeUsageInfoForRepl(&replica)
	QPS, _ := replica.loadStats.batchRequests.AverageRatePerSecond()
	WPS, _ := replica.loadStats.writeKeys.AverageRatePerSecond()

	sp.UpdateLocalStoreAfterRebalance(roachpb.StoreID(1), rangeUsageInfo, roachpb.ADD_VOTER)
	desc, ok := sp.GetStoreDescriptor(roachpb.StoreID(1))
	if !ok {
		t.Fatalf("couldn't find StoreDescriptor for Store ID %d", 1)
	}
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
	if expectedL0Sublevels := int64(4); desc.Capacity.L0Sublevels != expectedL0Sublevels {
		t.Errorf("expected L0 Sub-Levels %d, but got %d", expectedL0Sublevels, desc.Capacity.L0Sublevels)
	}

	sp.UpdateLocalStoreAfterRebalance(roachpb.StoreID(2), rangeUsageInfo, roachpb.REMOVE_VOTER)
	desc, ok = sp.GetStoreDescriptor(roachpb.StoreID(2))
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
	if expectedL0Sublevels := int64(8); desc.Capacity.L0Sublevels != expectedL0Sublevels {
		t.Errorf("expected L0 Sub-Levels %d, but got %d", expectedL0Sublevels, desc.Capacity.L0Sublevels)
	}

	sp.UpdateLocalStoresAfterLeaseTransfer(roachpb.StoreID(1), roachpb.StoreID(2), rangeUsageInfo)
	desc, ok = sp.GetStoreDescriptor(roachpb.StoreID(1))
	if !ok {
		t.Fatalf("couldn't find StoreDescriptor for Store ID %d", 1)
	}
	if expectedLeaseCount := int32(0); desc.Capacity.LeaseCount != expectedLeaseCount {
		t.Errorf("expected LeaseCount %d, but got %d", expectedLeaseCount, desc.Capacity.LeaseCount)
	}
	if expectedQPS := 100 - QPS; desc.Capacity.QueriesPerSecond != expectedQPS {
		t.Errorf("expected QueriesPerSecond %f, but got %f", expectedQPS, desc.Capacity.QueriesPerSecond)
	}
	desc, ok = sp.GetStoreDescriptor(roachpb.StoreID(2))
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
	clock := hlc.NewClock(timeutil.NewManualTime(timeutil.Unix(0, 123)), time.Nanosecond /* maxOffset */)
	stopper, _, _, sp, _ := storepool.CreateTestStorePool(ctx,
		storepool.TestTimeUntilStoreDead, false, /* deterministic */
		func() int { return 10 }, /* nodeCount */
		livenesspb.NodeLivenessStatus_DEAD)
	defer stopper.Stop(ctx)

	// Create store.
	node := roachpb.NodeDescriptor{NodeID: roachpb.NodeID(1)}
	eng := storage.NewDefaultInMemForTesting()
	stopper.AddCloser(eng)
	cfg := TestStoreConfig(clock)
	cfg.Transport = NewDummyRaftTransport(cfg.Settings, cfg.AmbientCtx.Tracer)
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
	replica.loadStats = NewReplicaLoad(store.Clock(), nil)

	rangeUsageInfo := RangeUsageInfoForRepl(replica)

	// Update StorePool, which should be a no-op.
	storeID := roachpb.StoreID(1)
	if _, ok := sp.GetStoreDescriptor(storeID); ok {
		t.Fatalf("StoreDescriptor not gossiped, should not be found")
	}
	sp.UpdateLocalStoreAfterRebalance(storeID, rangeUsageInfo, roachpb.ADD_VOTER)
	if _, ok := sp.GetStoreDescriptor(storeID); ok {
		t.Fatalf("StoreDescriptor still not gossiped, should not be found")
	}
}
