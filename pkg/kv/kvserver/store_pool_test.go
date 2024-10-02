// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/storepool"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/load"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/logstore"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/replicastats"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils/gossiputil"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

func TestStorePoolUpdateLocalStore(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	manual := timeutil.NewManualTime(timeutil.Unix(0, 123))
	clock := hlc.NewClockForTesting(manual)
	ctx := context.Background()
	// We're going to manually mark stores dead in this test.
	st := cluster.MakeTestingClusterSettings()
	stopper, g, _, sp, _ := storepool.CreateTestStorePool(ctx, st,
		liveness.TestTimeUntilNodeDead, false, /* deterministic */
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
				IOThreshold: admissionpb.IOThreshold{
					L0NumSubLevels:          5,
					L0NumSubLevelsThreshold: 20,
					L0NumFiles:              5,
					L0NumFilesThreshold:     1000,
				},
				IOThresholdMax: admissionpb.IOThreshold{
					L0NumSubLevels:          5,
					L0NumSubLevelsThreshold: 20,
					L0NumFiles:              5,
					L0NumFilesThreshold:     1000,
				},
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
				IOThreshold: admissionpb.IOThreshold{
					L0NumSubLevels:          10,
					L0NumSubLevelsThreshold: 20,
					L0NumFiles:              10,
					L0NumFilesThreshold:     1000,
				},
				IOThresholdMax: admissionpb.IOThreshold{
					L0NumSubLevels:          10,
					L0NumSubLevelsThreshold: 20,
					L0NumFiles:              10,
					L0NumFilesThreshold:     1000,
				},
			},
		},
	}
	callbacks := []roachpb.StoreID{}
	sp.SetOnCapacityChange(func(
		storeID roachpb.StoreID,
		_, _ roachpb.StoreCapacity,
	) {
		callbacks = append(callbacks, storeID)
	})
	// Gossip the initial stores. There should trigger two callbacks as the
	// capacity has changed from no capacity to a new capacity.
	sg.GossipStores(stores, t)
	require.Len(t, callbacks, 2)
	// Gossip the initial stores again, with the same capacity. This shouldn't
	// trigger any callbacks as the capacity hasn't changed.
	sg.GossipStores(stores, t)
	require.Len(t, callbacks, 2)

	replica := Replica{RangeID: 1}
	replica.mu.Lock()
	replica.shMu.state.Stats = &enginepb.MVCCStats{
		KeyBytes: 2,
		ValBytes: 4,
	}
	replica.mu.Unlock()
	replica.loadStats = load.NewReplicaLoad(clock, nil)
	for _, store := range stores {
		replica.loadStats.RecordBatchRequests(1, store.Node.NodeID)
		replica.loadStats.RecordWriteKeys(1)
	}
	manual.Advance(replicastats.MinStatsDuration + time.Second)

	rangeUsageInfo := replica.RangeUsageInfo()
	stats := replica.LoadStats()
	QPS := stats.QueriesPerSecond
	WPS := stats.WriteKeysPerSecond

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
	if expectedNumL0Sublevels := int64(5); desc.Capacity.IOThreshold.L0NumSubLevels != expectedNumL0Sublevels {
		t.Errorf("expected L0 Sub-Levels %d, but got %d", expectedNumL0Sublevels, desc.Capacity.IOThreshold.L0NumSubLevels)
	}
	ioScoreMax, _ := desc.Capacity.IOThresholdMax.Score()
	if expectedIOThresholdScoreMax := 0.25; ioScoreMax != expectedIOThresholdScoreMax {
		t.Errorf("expected IOThresholdMax score %f, but got %f", expectedIOThresholdScoreMax, ioScoreMax)
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
	if expectedNumL0Sublevels := int64(10); desc.Capacity.IOThreshold.L0NumSubLevels != expectedNumL0Sublevels {
		t.Errorf("expected L0 Sub-Levels %d, but got %d", expectedNumL0Sublevels, desc.Capacity.IOThreshold.L0NumFiles)
	}
	ioScoreMax, _ = desc.Capacity.IOThresholdMax.Score()
	if expectedIOThresholdScoreMax := 0.5; ioScoreMax != expectedIOThresholdScoreMax {
		t.Errorf("expected IOThresholdMax score %f, but got %f", expectedIOThresholdScoreMax, ioScoreMax)
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

	sp.UpdateLocalStoreAfterRelocate(
		[]roachpb.ReplicationTarget{{StoreID: roachpb.StoreID(1)}}, []roachpb.ReplicationTarget{},
		[]roachpb.ReplicaDescriptor{{StoreID: roachpb.StoreID(2)}}, []roachpb.ReplicaDescriptor{},
		roachpb.StoreID(2), rangeUsageInfo)

	desc, ok = sp.GetStoreDescriptor(roachpb.StoreID(1))
	require.True(t, ok)
	require.Equal(t, 100.0, desc.Capacity.QueriesPerSecond)
	require.Equal(t, int32(1), desc.Capacity.LeaseCount)
	require.Equal(t, int32(7), desc.Capacity.RangeCount)

	desc, ok = sp.GetStoreDescriptor(roachpb.StoreID(2))
	require.True(t, ok)
	require.Equal(t, 50.0, desc.Capacity.QueriesPerSecond)
	require.Equal(t, int32(2), desc.Capacity.LeaseCount)
	require.Equal(t, int32(2), desc.Capacity.RangeCount)
}

// TestStorePoolUpdateLocalStoreBeforeGossip verifies that an attempt to update
// the local copy of store before that store has been gossiped will be a no-op.
func TestStorePoolUpdateLocalStoreBeforeGossip(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	clock := hlc.NewClockForTesting(timeutil.NewManualTime(timeutil.Unix(0, 123)))
	cfg := TestStoreConfig(clock)
	var stopper *stop.Stopper
	stopper, _, _, cfg.StorePool, _ = storepool.CreateTestStorePool(ctx, cfg.Settings,
		liveness.TestTimeUntilNodeDead, false, /* deterministic */
		func() int { return 10 }, /* nodeCount */
		livenesspb.NodeLivenessStatus_DEAD)
	defer stopper.Stop(ctx)

	// Create store.
	node := roachpb.NodeDescriptor{NodeID: roachpb.NodeID(1)}
	eng := storage.NewDefaultInMemForTesting()
	stopper.AddCloser(eng)

	cfg.Transport = NewDummyRaftTransport(cfg.AmbientCtx, cfg.Settings, cfg.Clock)
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

	const replicaID = 1
	require.NoError(t,
		logstore.NewStateLoader(rg.RangeID).SetRaftReplicaID(ctx, store.TODOEngine(), replicaID))
	replica, err := loadInitializedReplicaForTesting(ctx, store, &rg, replicaID)
	if err != nil {
		t.Fatalf("make replica error : %+v", err)
	}
	replica.loadStats = load.NewReplicaLoad(store.Clock(), nil)

	rangeUsageInfo := replica.RangeUsageInfo()

	// Update StorePool, which should be a no-op.
	storeID := roachpb.StoreID(1)
	if _, ok := cfg.StorePool.GetStoreDescriptor(storeID); ok {
		t.Fatalf("StoreDescriptor not gossiped, should not be found")
	}
	cfg.StorePool.UpdateLocalStoreAfterRebalance(storeID, rangeUsageInfo, roachpb.ADD_VOTER)
	if _, ok := cfg.StorePool.GetStoreDescriptor(storeID); ok {
		t.Fatalf("StoreDescriptor still not gossiped, should not be found")
	}
}
