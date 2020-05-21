// Copyright 2018 The Cockroach Authors.
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
	"reflect"
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils/gossiputil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/gogo/protobuf/proto"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/tracker"
)

var (
	// noLocalityStores specifies a set of stores where one store is
	// under-utilized in terms of QPS, three are in the middle, and one is
	// over-utilized.
	noLocalityStores = []*roachpb.StoreDescriptor{
		{
			StoreID: 1,
			Node:    roachpb.NodeDescriptor{NodeID: 1},
			Capacity: roachpb.StoreCapacity{
				QueriesPerSecond: 1500,
			},
		},
		{
			StoreID: 2,
			Node:    roachpb.NodeDescriptor{NodeID: 2},
			Capacity: roachpb.StoreCapacity{
				QueriesPerSecond: 1100,
			},
		},
		{
			StoreID: 3,
			Node:    roachpb.NodeDescriptor{NodeID: 3},
			Capacity: roachpb.StoreCapacity{
				QueriesPerSecond: 1000,
			},
		},
		{
			StoreID: 4,
			Node:    roachpb.NodeDescriptor{NodeID: 4},
			Capacity: roachpb.StoreCapacity{
				QueriesPerSecond: 900,
			},
		},
		{
			StoreID: 5,
			Node:    roachpb.NodeDescriptor{NodeID: 5},
			Capacity: roachpb.StoreCapacity{
				QueriesPerSecond: 500,
			},
		},
	}
)

type testRange struct {
	// The first storeID in the list will be the leaseholder.
	storeIDs []roachpb.StoreID
	qps      float64
}

func loadRanges(rr *replicaRankings, s *Store, ranges []testRange) {
	acc := rr.newAccumulator()
	for _, r := range ranges {
		repl := &Replica{store: s}
		repl.mu.state.Desc = &roachpb.RangeDescriptor{}
		repl.mu.zone = s.cfg.DefaultZoneConfig
		for _, storeID := range r.storeIDs {
			repl.mu.state.Desc.InternalReplicas = append(repl.mu.state.Desc.InternalReplicas, roachpb.ReplicaDescriptor{
				NodeID:    roachpb.NodeID(storeID),
				StoreID:   storeID,
				ReplicaID: roachpb.ReplicaID(storeID),
			})
		}
		repl.mu.state.Lease = &roachpb.Lease{
			Expiration: &hlc.MaxTimestamp,
			Replica:    repl.mu.state.Desc.InternalReplicas[0],
		}
		// TODO(a-robinson): The below three lines won't be needed once the old
		// rangeInfo code is ripped out of the allocator.
		repl.mu.state.Stats = &enginepb.MVCCStats{}
		repl.leaseholderStats = newReplicaStats(s.Clock(), nil)
		repl.writeStats = newReplicaStats(s.Clock(), nil)
		acc.addReplica(replicaWithStats{
			repl: repl,
			qps:  r.qps,
		})
	}
	rr.update(acc)
}

func TestChooseLeaseToTransfer(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	stopper, g, _, a, _ := createTestAllocator(10, false /* deterministic */)
	defer stopper.Stop(context.Background())
	gossiputil.NewStoreGossiper(g).GossipStores(noLocalityStores, t)
	storeList, _, _ := a.storePool.getStoreList(storeFilterThrottled)
	storeMap := storeListToMap(storeList)

	const minQPS = 800
	const maxQPS = 1200

	localDesc := *noLocalityStores[0]
	cfg := TestStoreConfig(nil)
	s := createTestStoreWithoutStart(t, stopper, testStoreOpts{createSystemRanges: true}, &cfg)
	s.Ident = &roachpb.StoreIdent{StoreID: localDesc.StoreID}
	rq := newReplicateQueue(s, g, a)
	rr := newReplicaRankings()

	sr := NewStoreRebalancer(cfg.AmbientCtx, cfg.Settings, rq, rr)

	// Rather than trying to populate every Replica with a real raft group in
	// order to pass replicaIsBehind checks, fake out the function for getting
	// raft status with one that always returns all replicas as up to date.
	sr.getRaftStatusFn = func(r *Replica) *raft.Status {
		status := &raft.Status{
			Progress: make(map[uint64]tracker.Progress),
		}
		status.Lead = uint64(r.ReplicaID())
		status.Commit = 1
		for _, replica := range r.Desc().InternalReplicas {
			status.Progress[uint64(replica.ReplicaID)] = tracker.Progress{
				Match: 1,
				State: tracker.StateReplicate,
			}
		}
		return status
	}

	testCases := []struct {
		storeIDs     []roachpb.StoreID
		qps          float64
		expectTarget roachpb.StoreID
	}{
		{[]roachpb.StoreID{1}, 100, 0},
		{[]roachpb.StoreID{1, 2}, 100, 0},
		{[]roachpb.StoreID{1, 3}, 100, 0},
		{[]roachpb.StoreID{1, 4}, 100, 4},
		{[]roachpb.StoreID{1, 5}, 100, 5},
		{[]roachpb.StoreID{5, 1}, 100, 0},
		{[]roachpb.StoreID{1, 2}, 200, 0},
		{[]roachpb.StoreID{1, 3}, 200, 0},
		{[]roachpb.StoreID{1, 4}, 200, 0},
		{[]roachpb.StoreID{1, 5}, 200, 5},
		{[]roachpb.StoreID{1, 2}, 500, 0},
		{[]roachpb.StoreID{1, 3}, 500, 0},
		{[]roachpb.StoreID{1, 4}, 500, 0},
		{[]roachpb.StoreID{1, 5}, 500, 5},
		{[]roachpb.StoreID{1, 5}, 600, 5},
		{[]roachpb.StoreID{1, 5}, 700, 5},
		{[]roachpb.StoreID{1, 5}, 800, 0},
		{[]roachpb.StoreID{1, 4}, 1.5, 4},
		{[]roachpb.StoreID{1, 5}, 1.5, 5},
		{[]roachpb.StoreID{1, 4}, 1.49, 0},
		{[]roachpb.StoreID{1, 5}, 1.49, 0},
	}

	for _, tc := range testCases {
		loadRanges(rr, s, []testRange{{storeIDs: tc.storeIDs, qps: tc.qps}})
		hottestRanges := rr.topQPS()
		_, target, _ := sr.chooseLeaseToTransfer(
			ctx, &hottestRanges, &localDesc, storeList, storeMap, minQPS, maxQPS)
		if target.StoreID != tc.expectTarget {
			t.Errorf("got target store %d for range with replicas %v and %f qps; want %d",
				target.StoreID, tc.storeIDs, tc.qps, tc.expectTarget)
		}
	}
}

func TestChooseReplicaToRebalance(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	stopper, g, _, a, _ := createTestAllocator(10, false /* deterministic */)
	defer stopper.Stop(context.Background())
	gossiputil.NewStoreGossiper(g).GossipStores(noLocalityStores, t)
	storeList, _, _ := a.storePool.getStoreList(storeFilterThrottled)
	storeMap := storeListToMap(storeList)

	const minQPS = 800
	const maxQPS = 1200

	localDesc := *noLocalityStores[0]
	cfg := TestStoreConfig(nil)
	s := createTestStoreWithoutStart(t, stopper, testStoreOpts{createSystemRanges: true}, &cfg)
	s.Ident = &roachpb.StoreIdent{StoreID: localDesc.StoreID}
	rq := newReplicateQueue(s, g, a)
	rr := newReplicaRankings()

	sr := NewStoreRebalancer(cfg.AmbientCtx, cfg.Settings, rq, rr)

	// Rather than trying to populate every Replica with a real raft group in
	// order to pass replicaIsBehind checks, fake out the function for getting
	// raft status with one that always returns all replicas as up to date.
	sr.getRaftStatusFn = func(r *Replica) *raft.Status {
		status := &raft.Status{
			Progress: make(map[uint64]tracker.Progress),
		}
		status.Lead = uint64(r.ReplicaID())
		status.Commit = 1
		for _, replica := range r.Desc().InternalReplicas {
			status.Progress[uint64(replica.ReplicaID)] = tracker.Progress{
				Match: 1,
				State: tracker.StateReplicate,
			}
		}
		return status
	}

	testCases := []struct {
		storeIDs      []roachpb.StoreID
		qps           float64
		expectTargets []roachpb.StoreID // the first listed store is expected to be the leaseholder
	}{
		{[]roachpb.StoreID{1}, 100, []roachpb.StoreID{5}},
		{[]roachpb.StoreID{1}, 500, []roachpb.StoreID{5}},
		{[]roachpb.StoreID{1}, 700, []roachpb.StoreID{5}},
		{[]roachpb.StoreID{1}, 800, nil},
		{[]roachpb.StoreID{1}, 1.5, []roachpb.StoreID{5}},
		{[]roachpb.StoreID{1}, 1.49, nil},
		{[]roachpb.StoreID{1, 2}, 100, []roachpb.StoreID{5, 2}},
		{[]roachpb.StoreID{1, 3}, 100, []roachpb.StoreID{5, 3}},
		{[]roachpb.StoreID{1, 4}, 100, []roachpb.StoreID{5, 4}},
		{[]roachpb.StoreID{1, 2}, 800, nil},
		{[]roachpb.StoreID{1, 2}, 1.49, nil},
		{[]roachpb.StoreID{1, 4, 5}, 500, nil},
		{[]roachpb.StoreID{1, 4, 5}, 100, nil},
		{[]roachpb.StoreID{1, 3, 5}, 500, nil},
		{[]roachpb.StoreID{1, 3, 4}, 500, []roachpb.StoreID{5, 4, 3}},
		{[]roachpb.StoreID{1, 3, 5}, 100, []roachpb.StoreID{5, 4, 3}},
		// Rebalancing to s2 isn't chosen even though it's better than s1 because it's above the mean.
		{[]roachpb.StoreID{1, 3, 4, 5}, 100, nil},
		{[]roachpb.StoreID{1, 2, 4, 5}, 100, nil},
		{[]roachpb.StoreID{1, 2, 3, 5}, 100, []roachpb.StoreID{5, 4, 3, 2}},
		{[]roachpb.StoreID{1, 2, 3, 4}, 100, []roachpb.StoreID{5, 4, 3, 2}},
	}

	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			s.cfg.DefaultZoneConfig.NumReplicas = proto.Int32(int32(len(tc.storeIDs)))
			loadRanges(rr, s, []testRange{{storeIDs: tc.storeIDs, qps: tc.qps}})
			hottestRanges := rr.topQPS()
			_, targets := sr.chooseReplicaToRebalance(
				ctx, &hottestRanges, &localDesc, storeList, storeMap, minQPS, maxQPS)

			if len(targets) != len(tc.expectTargets) {
				t.Fatalf("chooseReplicaToRebalance(existing=%v, qps=%f) got %v; want %v",
					tc.storeIDs, tc.qps, targets, tc.expectTargets)
			}
			if len(targets) == 0 {
				return
			}

			if targets[0].StoreID != tc.expectTargets[0] {
				t.Errorf("chooseReplicaToRebalance(existing=%v, qps=%f) chose s%d as leaseholder; want s%v",
					tc.storeIDs, tc.qps, targets[0], tc.expectTargets[0])
			}

			targetStores := make([]roachpb.StoreID, len(targets))
			for i, target := range targets {
				targetStores[i] = target.StoreID
			}
			sort.Sort(roachpb.StoreIDSlice(targetStores))
			sort.Sort(roachpb.StoreIDSlice(tc.expectTargets))
			if !reflect.DeepEqual(targetStores, tc.expectTargets) {
				t.Errorf("chooseReplicaToRebalance(existing=%v, qps=%f) chose targets %v; want %v",
					tc.storeIDs, tc.qps, targetStores, tc.expectTargets)
			}
		})
	}
}

func TestNoLeaseTransferToBehindReplicas(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Lots of setup boilerplate.

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	stopper, g, _, a, _ := createTestAllocator(10, false /* deterministic */)
	defer stopper.Stop(context.Background())
	gossiputil.NewStoreGossiper(g).GossipStores(noLocalityStores, t)
	storeList, _, _ := a.storePool.getStoreList(storeFilterThrottled)
	storeMap := storeListToMap(storeList)

	const minQPS = 800
	const maxQPS = 1200

	localDesc := *noLocalityStores[0]
	cfg := TestStoreConfig(nil)
	s := createTestStoreWithoutStart(t, stopper, testStoreOpts{createSystemRanges: true}, &cfg)
	s.Ident = &roachpb.StoreIdent{StoreID: localDesc.StoreID}
	rq := newReplicateQueue(s, g, a)
	rr := newReplicaRankings()

	sr := NewStoreRebalancer(cfg.AmbientCtx, cfg.Settings, rq, rr)

	// Load in a range with replicas on an overfull node, a slightly underfull
	// node, and a very underfull node.
	loadRanges(rr, s, []testRange{{storeIDs: []roachpb.StoreID{1, 4, 5}, qps: 100}})
	hottestRanges := rr.topQPS()
	repl := hottestRanges[0].repl

	// Set up a fake RaftStatus that indicates s5 is behind (but all other stores
	// are caught up). We thus shouldn't transfer a lease to s5.
	sr.getRaftStatusFn = func(r *Replica) *raft.Status {
		status := &raft.Status{
			Progress: make(map[uint64]tracker.Progress),
		}
		status.Lead = uint64(r.ReplicaID())
		status.Commit = 1
		for _, replica := range r.Desc().InternalReplicas {
			match := uint64(1)
			if replica.StoreID == roachpb.StoreID(5) {
				match = 0
			}
			status.Progress[uint64(replica.ReplicaID)] = tracker.Progress{
				Match: match,
				State: tracker.StateReplicate,
			}
		}
		return status
	}

	_, target, _ := sr.chooseLeaseToTransfer(
		ctx, &hottestRanges, &localDesc, storeList, storeMap, minQPS, maxQPS)
	expectTarget := roachpb.StoreID(4)
	if target.StoreID != expectTarget {
		t.Errorf("got target store s%d for range with RaftStatus %v; want s%d",
			target.StoreID, sr.getRaftStatusFn(repl), expectTarget)
	}

	// Then do the same, but for replica rebalancing. Make s5 an existing replica
	// that's behind, and see how a new replica is preferred as the leaseholder
	// over it.
	loadRanges(rr, s, []testRange{{storeIDs: []roachpb.StoreID{1, 3, 5}, qps: 100}})
	hottestRanges = rr.topQPS()
	repl = hottestRanges[0].repl

	_, targets := sr.chooseReplicaToRebalance(
		ctx, &hottestRanges, &localDesc, storeList, storeMap, minQPS, maxQPS)
	expectTargets := []roachpb.ReplicationTarget{
		{NodeID: 4, StoreID: 4}, {NodeID: 5, StoreID: 5}, {NodeID: 3, StoreID: 3},
	}
	if !reflect.DeepEqual(targets, expectTargets) {
		t.Errorf("got targets %v for range with RaftStatus %v; want %v",
			targets, sr.getRaftStatusFn(repl), expectTargets)
	}
}
