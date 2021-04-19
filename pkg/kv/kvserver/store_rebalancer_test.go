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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils/gossiputil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/tracker"
)

var (
	// noLocalityStores specifies a set of stores where s5 is
	// under-utilized in terms of QPS, s2-s4 are in the middle, and s1 is
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
	voters, nonVoters []roachpb.StoreID
	qps               float64
}

func loadRanges(rr *replicaRankings, s *Store, ranges []testRange) {
	acc := rr.newAccumulator()
	for _, r := range ranges {
		repl := &Replica{store: s}
		repl.mu.state.Desc = &roachpb.RangeDescriptor{}
		repl.mu.zone = s.cfg.DefaultZoneConfig
		for _, storeID := range r.voters {
			repl.mu.state.Desc.InternalReplicas = append(repl.mu.state.Desc.InternalReplicas, roachpb.ReplicaDescriptor{
				NodeID:    roachpb.NodeID(storeID),
				StoreID:   storeID,
				ReplicaID: roachpb.ReplicaID(storeID),
				Type:      roachpb.ReplicaTypeVoterFull(),
			})
		}
		repl.mu.state.Lease = &roachpb.Lease{
			Expiration: &hlc.MaxTimestamp,
			Replica:    repl.mu.state.Desc.InternalReplicas[0],
		}
		for _, storeID := range r.nonVoters {
			repl.mu.state.Desc.InternalReplicas = append(repl.mu.state.Desc.InternalReplicas, roachpb.ReplicaDescriptor{
				NodeID:    roachpb.NodeID(storeID),
				StoreID:   storeID,
				ReplicaID: roachpb.ReplicaID(storeID),
				Type:      roachpb.ReplicaTypeNonVoter(),
			})
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
	defer log.Scope(t).Close(t)

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
		loadRanges(rr, s, []testRange{{voters: tc.storeIDs, qps: tc.qps}})
		hottestRanges := rr.topQPS()
		_, target, _ := sr.chooseLeaseToTransfer(
			ctx, &hottestRanges, &localDesc, storeList, storeMap, minQPS, maxQPS)
		if target.StoreID != tc.expectTarget {
			t.Errorf("got target store %d for range with replicas %v and %f qps; want %d",
				target.StoreID, tc.storeIDs, tc.qps, tc.expectTarget)
		}
	}
}

func TestChooseRangeToRebalance(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

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
		voters, nonVoters []roachpb.StoreID
		// stores that are not to be considered for rebalancing
		nonLive []roachpb.StoreID
		qps     float64
		// the first listed voter target is expected to be the leaseholder
		expectedRebalancedVoters, expectedRebalancedNonVoters []roachpb.StoreID
	}{
		{
			voters:                      []roachpb.StoreID{1},
			nonVoters:                   nil,
			nonLive:                     nil,
			qps:                         100,
			expectedRebalancedVoters:    []roachpb.StoreID{5},
			expectedRebalancedNonVoters: nil,
		},
		// If s5 is unavailable, s4 is the next best guess.
		{
			voters:                      []roachpb.StoreID{1},
			nonVoters:                   nil,
			nonLive:                     []roachpb.StoreID{5},
			qps:                         100,
			expectedRebalancedVoters:    []roachpb.StoreID{4},
			expectedRebalancedNonVoters: nil,
		},
		{
			voters:                      []roachpb.StoreID{1},
			nonVoters:                   nil,
			nonLive:                     []roachpb.StoreID{4, 5},
			qps:                         100,
			expectedRebalancedVoters:    []roachpb.StoreID{},
			expectedRebalancedNonVoters: nil,
		},
		{
			voters:                      []roachpb.StoreID{1},
			nonVoters:                   nil,
			nonLive:                     nil,
			qps:                         500,
			expectedRebalancedVoters:    []roachpb.StoreID{5},
			expectedRebalancedNonVoters: nil,
		},
		{
			voters:                      []roachpb.StoreID{1},
			nonVoters:                   nil,
			nonLive:                     []roachpb.StoreID{5},
			qps:                         500,
			expectedRebalancedVoters:    []roachpb.StoreID{},
			expectedRebalancedNonVoters: nil,
		},
		{
			voters:                      []roachpb.StoreID{1},
			nonVoters:                   nil,
			nonLive:                     nil,
			qps:                         800,
			expectedRebalancedVoters:    nil,
			expectedRebalancedNonVoters: nil,
		},
		{
			voters:                      []roachpb.StoreID{1},
			nonVoters:                   nil,
			nonLive:                     nil,
			qps:                         1.5,
			expectedRebalancedVoters:    []roachpb.StoreID{5},
			expectedRebalancedNonVoters: nil,
		},
		{
			voters:                      []roachpb.StoreID{1},
			nonVoters:                   nil,
			nonLive:                     []roachpb.StoreID{5},
			qps:                         1.5,
			expectedRebalancedVoters:    []roachpb.StoreID{4},
			expectedRebalancedNonVoters: nil,
		},
		{
			voters:                      []roachpb.StoreID{1},
			nonVoters:                   nil,
			nonLive:                     nil,
			qps:                         1.49,
			expectedRebalancedVoters:    nil,
			expectedRebalancedNonVoters: nil,
		},
		{
			voters:                      []roachpb.StoreID{1, 2},
			nonVoters:                   nil,
			nonLive:                     nil,
			qps:                         100,
			expectedRebalancedVoters:    []roachpb.StoreID{5, 2},
			expectedRebalancedNonVoters: nil,
		},
		{
			voters:                      []roachpb.StoreID{1, 2},
			nonVoters:                   nil,
			nonLive:                     []roachpb.StoreID{5},
			qps:                         100,
			expectedRebalancedVoters:    []roachpb.StoreID{4, 2},
			expectedRebalancedNonVoters: nil,
		},
		{
			voters:                      []roachpb.StoreID{1, 3},
			nonVoters:                   nil,
			nonLive:                     nil,
			qps:                         100,
			expectedRebalancedVoters:    []roachpb.StoreID{5, 3},
			expectedRebalancedNonVoters: nil,
		},
		{
			voters:                      []roachpb.StoreID{1, 4},
			nonVoters:                   nil,
			nonLive:                     nil,
			qps:                         100,
			expectedRebalancedVoters:    []roachpb.StoreID{5, 4},
			expectedRebalancedNonVoters: nil,
		},
		{
			voters:                      []roachpb.StoreID{1, 2},
			nonVoters:                   nil,
			nonLive:                     nil,
			qps:                         800,
			expectedRebalancedVoters:    nil,
			expectedRebalancedNonVoters: nil,
		},
		{
			voters:                      []roachpb.StoreID{1, 2},
			nonVoters:                   nil,
			nonLive:                     nil,
			qps:                         1.49,
			expectedRebalancedVoters:    nil,
			expectedRebalancedNonVoters: nil,
		},
		{
			voters:                      []roachpb.StoreID{1, 4, 5},
			nonVoters:                   nil,
			nonLive:                     nil,
			qps:                         500,
			expectedRebalancedVoters:    nil,
			expectedRebalancedNonVoters: nil,
		},
		{
			voters:                      []roachpb.StoreID{1, 4, 5},
			nonVoters:                   nil,
			nonLive:                     nil,
			qps:                         100,
			expectedRebalancedVoters:    nil,
			expectedRebalancedNonVoters: nil,
		},
		{
			voters:                      []roachpb.StoreID{1, 3, 5},
			nonVoters:                   nil,
			nonLive:                     nil,
			qps:                         500,
			expectedRebalancedVoters:    nil,
			expectedRebalancedNonVoters: nil,
		},
		{
			voters:                      []roachpb.StoreID{1, 3, 4},
			nonVoters:                   nil,
			nonLive:                     nil,
			qps:                         500,
			expectedRebalancedVoters:    []roachpb.StoreID{5, 4, 3},
			expectedRebalancedNonVoters: nil,
		},
		{
			voters:                      []roachpb.StoreID{1, 3, 5},
			nonVoters:                   nil,
			nonLive:                     nil,
			qps:                         100,
			expectedRebalancedVoters:    []roachpb.StoreID{5, 4, 3},
			expectedRebalancedNonVoters: nil,
		},
		{
			voters:                      []roachpb.StoreID{1, 3, 5},
			nonVoters:                   nil,
			nonLive:                     []roachpb.StoreID{4},
			qps:                         100,
			expectedRebalancedVoters:    nil,
			expectedRebalancedNonVoters: nil,
		},
		// Rebalancing to s2 isn't chosen even though it's better than s1 because it's above the mean.
		{
			voters:                      []roachpb.StoreID{1, 3, 4, 5},
			nonVoters:                   nil,
			nonLive:                     nil,
			qps:                         100,
			expectedRebalancedVoters:    nil,
			expectedRebalancedNonVoters: nil,
		},
		{
			voters:                      []roachpb.StoreID{1, 2, 4, 5},
			nonVoters:                   nil,
			nonLive:                     nil,
			qps:                         100,
			expectedRebalancedVoters:    nil,
			expectedRebalancedNonVoters: nil,
		},
		{
			voters:                      []roachpb.StoreID{1, 2, 3, 5},
			nonVoters:                   nil,
			nonLive:                     nil,
			qps:                         100,
			expectedRebalancedVoters:    []roachpb.StoreID{5, 4, 3, 2},
			expectedRebalancedNonVoters: nil,
		},
		{
			voters:                      []roachpb.StoreID{1, 2, 3, 4},
			nonVoters:                   nil,
			nonLive:                     nil,
			qps:                         100,
			expectedRebalancedVoters:    []roachpb.StoreID{5, 4, 3, 2},
			expectedRebalancedNonVoters: nil,
		},
		{
			// Don't bother moving any replicas around since it won't make much of a
			// difference. See `minQPSFraction` inside `chooseRangeToRebalance()`.
			voters:                      []roachpb.StoreID{1},
			nonVoters:                   []roachpb.StoreID{2, 3, 4},
			nonLive:                     nil,
			qps:                         1,
			expectedRebalancedVoters:    nil,
			expectedRebalancedNonVoters: nil,
		},
		{
			// None of the stores are worth moving to because they will be above the
			// maxQPS after the move.
			voters:                      []roachpb.StoreID{1},
			nonVoters:                   []roachpb.StoreID{2, 3, 4},
			nonLive:                     nil,
			qps:                         1000,
			expectedRebalancedVoters:    nil,
			expectedRebalancedNonVoters: nil,
		},
		{
			voters:                      []roachpb.StoreID{1},
			nonVoters:                   []roachpb.StoreID{2, 3, 4},
			nonLive:                     nil,
			qps:                         100,
			expectedRebalancedVoters:    []roachpb.StoreID{5},
			expectedRebalancedNonVoters: []roachpb.StoreID{4, 3, 2},
		},
		// Voters may rebalance to stores that have a non-voter, and those
		// displaced non-voters will be rebalanced to other valid stores.
		{
			voters:                      []roachpb.StoreID{1},
			nonVoters:                   []roachpb.StoreID{5},
			nonLive:                     nil,
			qps:                         100,
			expectedRebalancedVoters:    []roachpb.StoreID{5},
			expectedRebalancedNonVoters: []roachpb.StoreID{4},
		},
		{
			voters:                      []roachpb.StoreID{1},
			nonVoters:                   []roachpb.StoreID{5, 2, 3},
			nonLive:                     nil,
			qps:                         100,
			expectedRebalancedVoters:    []roachpb.StoreID{5},
			expectedRebalancedNonVoters: []roachpb.StoreID{2, 3, 4},
		},
		{
			// Voters may rebalance to stores that have a non-voter, but only if the
			// displaced non-voters can be rebalanced to other underfull (based on
			// QPS) stores. Note that stores 1 and 2 are above the maxQPS and the
			// meanQPS, respectively, so non-voters cannot be rebalanced to them.
			voters:                      []roachpb.StoreID{1, 2},
			nonVoters:                   []roachpb.StoreID{5, 4, 3},
			nonLive:                     nil,
			qps:                         100,
			expectedRebalancedVoters:    nil,
			expectedRebalancedNonVoters: nil,
		},
	}

	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			a.storePool.isStoreReadyForRoutineReplicaTransfer = func(_ context.Context, storeID roachpb.StoreID) bool {
				for _, s := range tc.nonLive {
					if s == storeID {
						return false
					}
				}
				return true
			}

			s.cfg.DefaultZoneConfig.NumVoters = proto.Int32(int32(len(tc.voters)))
			s.cfg.DefaultZoneConfig.NumReplicas = proto.Int32(int32(len(tc.voters) + len(tc.nonVoters)))
			loadRanges(
				rr, s, []testRange{
					{voters: tc.voters, nonVoters: tc.nonVoters, qps: tc.qps},
				},
			)
			hottestRanges := rr.topQPS()
			_, voterTargets, nonVoterTargets := sr.chooseRangeToRebalance(
				ctx, &hottestRanges, &localDesc, storeList, storeMap, minQPS, maxQPS,
			)

			require.Len(t, voterTargets, len(tc.expectedRebalancedVoters))
			if len(voterTargets) > 0 && voterTargets[0].StoreID != tc.expectedRebalancedVoters[0] {
				t.Errorf("chooseRangeToRebalance(existing=%v, qps=%f) chose s%d as leaseholder; want s%v",
					tc.voters, tc.qps, voterTargets[0], tc.expectedRebalancedVoters[0])
			}

			voterStoreIDs := make([]roachpb.StoreID, len(voterTargets))
			for i, target := range voterTargets {
				voterStoreIDs[i] = target.StoreID
			}
			require.ElementsMatch(t, voterStoreIDs, tc.expectedRebalancedVoters)

			require.Len(t, nonVoterTargets, len(tc.expectedRebalancedNonVoters))
			nonVoterStoreIDs := make([]roachpb.StoreID, len(nonVoterTargets))
			for i, target := range nonVoterTargets {
				nonVoterStoreIDs[i] = target.StoreID
			}
			require.ElementsMatch(t, nonVoterStoreIDs, tc.expectedRebalancedNonVoters)
		})
	}
}

func TestNoLeaseTransferToBehindReplicas(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

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
	loadRanges(rr, s, []testRange{{voters: []roachpb.StoreID{1, 4, 5}, qps: 100}})
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
	loadRanges(rr, s, []testRange{{voters: []roachpb.StoreID{1, 3, 5}, qps: 100}})
	hottestRanges = rr.topQPS()
	repl = hottestRanges[0].repl

	_, targets, _ := sr.chooseRangeToRebalance(
		ctx, &hottestRanges, &localDesc, storeList, storeMap, minQPS, maxQPS)
	expectTargets := []roachpb.ReplicationTarget{
		{NodeID: 4, StoreID: 4}, {NodeID: 5, StoreID: 5}, {NodeID: 3, StoreID: 3},
	}
	if !reflect.DeepEqual(targets, expectTargets) {
		t.Errorf("got targets %v for range with RaftStatus %v; want %v",
			targets, sr.getRaftStatusFn(repl), expectTargets)
	}
}
