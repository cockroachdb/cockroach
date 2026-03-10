// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mmaprototype

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// setupBenchCluster builds a clusterState with 4 stores on 4 nodes (3
// regions, round-robin), 1000 ranges with 3 replicas each leased on store 1.
// 100 ranges are hot (CPU=250 each, totaling 25000) and 900 are cold (CPU=0).
// Store 1 reports the total hot-range load; stores 2-4 report low load. This
// makes store 1 overloaded and triggers load-based lease shedding.
func setupBenchCluster(ts *timeutil.ManualTime) *clusterState {
	cs := newClusterState(ts, newStringInterner())
	cs.diskUtilRefuseThreshold = 0.9
	cs.diskUtilShedThreshold = 0.9

	ctx := context.Background()

	type storeInfo struct {
		region string
		zone   string
	}
	stores := []storeInfo{
		{"us-west-1", "us-west-1a"},
		{"us-east-1", "us-east-1a"},
		{"us-central-1", "us-central-1a"},
		{"us-west-1", "us-west-1b"},
	}

	// Register stores. setStore is the prerequisite for processStoreLoadMsg
	// (which panics if the store doesn't exist). The external equivalent is
	// allocatorState.SetStore.
	storeStatuses := make(map[roachpb.StoreID]Status, len(stores))
	for i, si := range stores {
		storeID := roachpb.StoreID(i + 1)
		sal := StoreAttributesAndLocality{
			StoreID: storeID,
			NodeID:  roachpb.NodeID(i + 1),
			NodeLocality: roachpb.Locality{
				Tiers: []roachpb.Tier{
					{Key: "region", Value: si.region},
					{Key: "zone", Value: si.zone},
				},
			},
		}
		cs.setStore(sal.withNodeTier())
		storeStatuses[storeID] = Status{Health: HealthOK}
	}

	// 100 hot ranges at CPU=250 each => store 1 total CPU = 25000.
	const (
		numRanges    = 1000
		numHotRanges = 100
		hotCPU       = LoadValue(250)
		hotRaftCPU   = LoadValue(20)
	)
	totalCPU := hotCPU * numHotRanges // 25000

	// Send store load messages. Store 1 carries all the hot-range load;
	// stores 2-4 are lightly loaded.
	for i := range stores {
		storeID := roachpb.StoreID(i + 1)
		cpuLoad := totalCPU / 10
		if storeID == 1 {
			cpuLoad = totalCPU
		}
		msg := &StoreLoadMsg{
			NodeID:          roachpb.NodeID(i + 1),
			StoreID:         storeID,
			Load:            LoadVector{cpuLoad, 0, 0},
			Capacity:        LoadVector{totalCPU, 1000, 1000},
			NodeCPULoad:     cpuLoad,
			NodeCPUCapacity: totalCPU,
		}
		cs.processStoreLoadMsg(ctx, msg, nil)
	}

	// Update store statuses after load messages so that disk capacity is
	// populated and updateStoreStatuses doesn't warn about unknown capacity.
	cs.updateStoreStatuses(ctx, storeStatuses)

	// Build 1000 ranges: first 100 are hot, rest are cold. All leased on
	// store 1 with replicas on stores 2 and 3.
	ranges := make([]RangeMsg, numRanges)
	for r := 0; r < numRanges; r++ {
		rangeID := roachpb.RangeID(r + 1)
		load := LoadVector{0, 0, 0}
		raftCPU := LoadValue(0)
		if r < numHotRanges {
			load[CPURate] = hotCPU
			raftCPU = hotRaftCPU
		}
		// Replicas on stores 1 (leaseholder), 2, 3.
		ranges[r] = RangeMsg{
			RangeID: rangeID,
			RangeLoad: RangeLoad{
				Load:    load,
				RaftCPU: raftCPU,
			},
			MaybeSpanConfIsPopulated: true,
			MaybeSpanConf:            roachpb.SpanConfig{NumReplicas: 3},
			Replicas: []StoreIDAndReplicaState{
				{
					StoreID: 1,
					ReplicaState: ReplicaState{
						ReplicaIDAndType: ReplicaIDAndType{
							ReplicaID:   roachpb.ReplicaID(1),
							ReplicaType: ReplicaType{ReplicaType: roachpb.VOTER_FULL, IsLeaseholder: true},
						},
					},
				},
				{
					StoreID: 2,
					ReplicaState: ReplicaState{
						ReplicaIDAndType: ReplicaIDAndType{
							ReplicaID:   roachpb.ReplicaID(2),
							ReplicaType: ReplicaType{ReplicaType: roachpb.VOTER_FULL},
						},
					},
				},
				{
					StoreID: 3,
					ReplicaState: ReplicaState{
						ReplicaIDAndType: ReplicaIDAndType{
							ReplicaID:   roachpb.ReplicaID(3),
							ReplicaType: ReplicaType{ReplicaType: roachpb.VOTER_FULL},
						},
					},
				},
			},
		}
	}

	cs.processStoreLeaseholderMsg(ctx, &StoreLeaseholderMsg{
		StoreID: 1,
		Ranges:  ranges,
	}, nil)

	return cs
}

// undoAllPendingChanges reverses all pending changes in the cluster state
// and resets leaked fields so the state is ready for another benchmark
// iteration.
func undoAllPendingChanges(ctx context.Context, cs *clusterState) {
	// Collect change IDs and affected range IDs before mutating the map.
	type pending struct {
		cid     changeID
		rangeID roachpb.RangeID
	}
	var toUndo []pending
	for cid, pc := range cs.pendingChanges {
		toUndo = append(toUndo, pending{cid: cid, rangeID: pc.rangeID})
	}

	for _, p := range toUndo {
		cs.undoPendingChange(ctx, p.cid)
	}

	// Reset lastFailedChange on affected ranges (undoPendingChange sets it
	// to now, which causes a 60s backoff in rebalanceStores).
	for _, p := range toUndo {
		if rs, ok := cs.ranges[p.rangeID]; ok {
			rs.lastFailedChange = time.Time{}
		}
	}

	// Clear the means memo since loadSeqNum changes during undo invalidate
	// cached entries.
	cs.meansMemo.clear()
}

func BenchmarkRebalanceStores(b *testing.B) {
	ts := timeutil.NewManualTime(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
	cs := setupBenchCluster(ts)
	ctx := context.Background()

	// Save overload times for all stores so we can restore them after
	// each iteration.
	type overloadTimes struct {
		start time.Time
		end   time.Time
	}
	savedOverload := make(map[roachpb.StoreID]overloadTimes, len(cs.stores))
	for sid, ss := range cs.stores {
		savedOverload[sid] = overloadTimes{
			start: ss.overloadStartTime,
			end:   ss.overloadEndTime,
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		re := newRebalanceEnv(
			cs,
			rand.New(rand.NewSource(0)),
			newDiversityScoringMemo(),
			ts.Now(),
			nil, /* passObs */
		)
		changes := re.rebalanceStores(ctx, 1 /* localStoreID */)
		if len(changes) == 0 {
			b.Fatal("rebalanceStores emitted no changes")
		}

		b.StopTimer()
		undoAllPendingChanges(ctx, cs)
		// Restore overload times.
		for sid, ot := range savedOverload {
			cs.stores[sid].overloadStartTime = ot.start
			cs.stores[sid].overloadEndTime = ot.end
		}
		b.StartTimer()
	}
}
