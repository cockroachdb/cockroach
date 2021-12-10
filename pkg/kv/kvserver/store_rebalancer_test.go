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
	"fmt"
	"math/rand"
	"reflect"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils/gossiputil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/tracker"
)

var (
	// multiRegionStores specifies a set of stores across 3 regions. These stores
	// are arranged in descending order of the QPS they are receiving. Store 1 is
	// the most heavily loaded, and store 9 is the least heavily loaded store.
	// Consequently, region "a" is fielding the most QPS whereas region "c" is
	// fielding the least.
	multiRegionStores = []*roachpb.StoreDescriptor{
		{
			StoreID: 1,
			Node: roachpb.NodeDescriptor{
				NodeID: 1,
				Locality: roachpb.Locality{
					Tiers: []roachpb.Tier{
						{
							Key:   "region",
							Value: "a",
						},
					},
				},
			},
			Capacity: roachpb.StoreCapacity{
				QueriesPerSecond: 3000,
			},
		},
		{
			StoreID: 2,
			Node: roachpb.NodeDescriptor{
				NodeID: 2,
				Locality: roachpb.Locality{
					Tiers: []roachpb.Tier{
						{
							Key:   "region",
							Value: "a",
						},
					},
				},
			},
			Capacity: roachpb.StoreCapacity{
				QueriesPerSecond: 2800,
			},
		},
		{
			StoreID: 3,
			Node: roachpb.NodeDescriptor{
				NodeID: 3,
				Locality: roachpb.Locality{
					Tiers: []roachpb.Tier{
						{
							Key:   "region",
							Value: "a",
						},
					},
				},
			},
			Capacity: roachpb.StoreCapacity{
				QueriesPerSecond: 2600,
			},
		},
		{
			StoreID: 4,
			Node: roachpb.NodeDescriptor{
				NodeID: 4,
				Locality: roachpb.Locality{
					Tiers: []roachpb.Tier{
						{
							Key:   "region",
							Value: "b",
						},
					},
				},
			},
			Capacity: roachpb.StoreCapacity{
				QueriesPerSecond: 2400,
			},
		},
		{
			StoreID: 5,
			Node: roachpb.NodeDescriptor{
				NodeID: 5,
				Locality: roachpb.Locality{
					Tiers: []roachpb.Tier{
						{
							Key:   "region",
							Value: "b",
						},
					},
				},
			},
			Capacity: roachpb.StoreCapacity{
				QueriesPerSecond: 2200,
			},
		},
		{
			StoreID: 6,
			Node: roachpb.NodeDescriptor{
				NodeID: 6,
				Locality: roachpb.Locality{
					Tiers: []roachpb.Tier{
						{
							Key:   "region",
							Value: "b",
						},
					},
				},
			},
			Capacity: roachpb.StoreCapacity{
				QueriesPerSecond: 2000,
			},
		},
		{
			StoreID: 7,
			Node: roachpb.NodeDescriptor{
				NodeID: 7,
				Locality: roachpb.Locality{
					Tiers: []roachpb.Tier{
						{
							Key:   "region",
							Value: "c",
						},
					},
				},
			},
			Capacity: roachpb.StoreCapacity{
				QueriesPerSecond: 1800,
			},
		},
		{
			StoreID: 8,
			Node: roachpb.NodeDescriptor{
				NodeID: 8,
				Locality: roachpb.Locality{
					Tiers: []roachpb.Tier{
						{
							Key:   "region",
							Value: "c",
						},
					},
				},
			},
			Capacity: roachpb.StoreCapacity{
				QueriesPerSecond: 1600,
			},
		},
		{
			StoreID: 9,
			Node: roachpb.NodeDescriptor{
				NodeID: 9,
				Locality: roachpb.Locality{
					Tiers: []roachpb.Tier{
						{
							Key:   "region",
							Value: "c",
						},
					},
				},
			},
			Capacity: roachpb.StoreCapacity{
				QueriesPerSecond: 1400,
			},
		},
	}

	// noLocalityStores specifies a set of stores that do not have any associated
	// locality tags, where s5 is under-utilized in terms of QPS, s2-s4 are in the
	// middle, and s1 is over-utilized.
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
		repl.mu.conf = s.cfg.DefaultSpanConfig
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
		repl.leaseholderStats.setAvgQPSForTesting(r.qps)

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

	stopper, g, _, a, _ := createTestAllocatorWithKnobs(
		10, false /* deterministic */, &AllocatorTestingKnobs{
			// Let the allocator pick lease transfer targets that are replicas in need
			// of snapshots, in order to avoid mocking out a fake raft group for the
			// `replicaMayNeedSnapshot` checks inside `TransferLeaseTarget`.
			AllowLeaseTransfersToReplicasNeedingSnapshots: true,
		},
	)
	defer stopper.Stop(context.Background())
	gossiputil.NewStoreGossiper(g).GossipStores(noLocalityStores, t)
	storeList, _, _ := a.storePool.getStoreList(storeFilterThrottled)
	storeMap := storeListToMap(storeList)
	localDesc := *noLocalityStores[0]
	cfg := TestStoreConfig(nil)
	cfg.Gossip = g
	s := createTestStoreWithoutStart(t, stopper, testStoreOpts{createSystemRanges: true}, &cfg)
	s.Ident = &roachpb.StoreIdent{StoreID: localDesc.StoreID}
	rq := newReplicateQueue(s, a)
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
		{
			storeIDs:     []roachpb.StoreID{1},
			qps:          100,
			expectTarget: 0,
		},
		{
			storeIDs:     []roachpb.StoreID{1, 2},
			qps:          100,
			expectTarget: 2,
		},
		{
			storeIDs:     []roachpb.StoreID{1, 3},
			qps:          100,
			expectTarget: 3,
		},
		{
			storeIDs:     []roachpb.StoreID{1, 4},
			qps:          100,
			expectTarget: 4,
		},
		{
			storeIDs:     []roachpb.StoreID{1, 5},
			qps:          100,
			expectTarget: 5,
		},
		{
			storeIDs:     []roachpb.StoreID{5, 1},
			qps:          100,
			expectTarget: 0,
		},
		{
			storeIDs:     []roachpb.StoreID{1, 2},
			qps:          200,
			expectTarget: 2,
		},
		{
			storeIDs:     []roachpb.StoreID{1, 3},
			qps:          200,
			expectTarget: 3,
		},
		{
			storeIDs:     []roachpb.StoreID{1, 4},
			qps:          200,
			expectTarget: 4,
		},
		{
			storeIDs:     []roachpb.StoreID{1, 5},
			qps:          200,
			expectTarget: 5,
		},
		{
			storeIDs:     []roachpb.StoreID{1, 2},
			qps:          500,
			expectTarget: 0,
		},
		{
			storeIDs:     []roachpb.StoreID{1, 3},
			qps:          500,
			expectTarget: 0,
		},
		{
			storeIDs:     []roachpb.StoreID{1, 4},
			qps:          500,
			expectTarget: 4,
		},
		{
			storeIDs:     []roachpb.StoreID{1, 5},
			qps:          500,
			expectTarget: 5,
		},
		{
			storeIDs:     []roachpb.StoreID{1, 5},
			qps:          600,
			expectTarget: 5,
		},
		{
			storeIDs:     []roachpb.StoreID{1, 5},
			qps:          700,
			expectTarget: 5,
		},
		{
			storeIDs:     []roachpb.StoreID{1, 5},
			qps:          800,
			expectTarget: 5,
		},
		{
			storeIDs:     []roachpb.StoreID{1, 4, 5},
			qps:          800,
			expectTarget: 5,
		},
		{
			storeIDs:     []roachpb.StoreID{1, 3, 4, 5},
			qps:          800,
			expectTarget: 5,
		},
		{
			storeIDs:     []roachpb.StoreID{1, 4},
			qps:          1.5,
			expectTarget: 4,
		},
		{
			storeIDs:     []roachpb.StoreID{1, 5},
			qps:          1.5,
			expectTarget: 5,
		},
		{
			storeIDs:     []roachpb.StoreID{1, 4},
			qps:          1.49,
			expectTarget: 0,
		},
		{
			storeIDs:     []roachpb.StoreID{1, 5},
			qps:          1.49,
			expectTarget: 0,
		},
		{
			storeIDs:     []roachpb.StoreID{1, 2, 3, 4},
			qps:          1500,
			expectTarget: 0,
		},
		{
			storeIDs:     []roachpb.StoreID{1, 2, 3, 4, 5},
			qps:          1500,
			expectTarget: 0,
		},
	}

	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			loadRanges(rr, s, []testRange{{voters: tc.storeIDs, qps: tc.qps}})
			hottestRanges := rr.topQPS()
			_, target, _ := sr.chooseLeaseToTransfer(ctx, &hottestRanges, &localDesc, storeList, storeMap)
			if target.StoreID != tc.expectTarget {
				t.Errorf("got target store %d for range with replicas %v and %f qps; want %d",
					target.StoreID, tc.storeIDs, tc.qps, tc.expectTarget)
			}
		})
	}
}

func randomNoLocalityStores(
	numNodes int, qpsMultiplier float64,
) (stores []*roachpb.StoreDescriptor, qpsMean float64) {
	var totalQPS float64
	for i := 1; i <= numNodes; i++ {
		qps := rand.Float64() * qpsMultiplier
		stores = append(
			stores, &roachpb.StoreDescriptor{
				StoreID:  roachpb.StoreID(i),
				Node:     roachpb.NodeDescriptor{NodeID: roachpb.NodeID(i)},
				Capacity: roachpb.StoreCapacity{QueriesPerSecond: qps},
			},
		)
		totalQPS = totalQPS + qps
	}
	return stores, totalQPS / float64(numNodes)
}

func logSummary(
	ctx context.Context, allStores, deadStores []*roachpb.StoreDescriptor, meanQPS float64,
) {
	var summary strings.Builder
	for _, store := range allStores {
		summary.WriteString(
			fmt.Sprintf("s%d: %.2f qps", store.StoreID, store.Capacity.QueriesPerSecond),
		)
		for _, dead := range deadStores {
			if dead.StoreID == store.StoreID {
				summary.WriteString(" (dead)")
			}
		}
		summary.WriteString("\n")
	}
	summary.WriteString(fmt.Sprintf("overall-mean: %.2f", meanQPS))
	log.Infof(ctx, "generated random store list:\n%s", summary.String())
}

func TestChooseRangeToRebalanceRandom(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const (
		numIterations = 10

		qpsMultiplier         = 2000
		numVoters             = 3
		numNonVoters          = 3
		numNodes              = 12
		numDeadNodes          = 3
		perReplicaQPS         = 100
		qpsRebalanceThreshold = 0.25

		epsilon = 1
	)

	for i := 0; i < numIterations; i++ {
		t.Run(fmt.Sprintf("%d", i+1), func(t *testing.T) {
			ctx := context.Background()
			stopper, g, _, a, _ := createTestAllocator(numNodes, false /* deterministic */)
			defer stopper.Stop(context.Background())

			stores, actualQPSMean := randomNoLocalityStores(numNodes, qpsMultiplier)
			deadStores := stores[len(stores)-numDeadNodes:]
			logSummary(ctx, stores, deadStores, actualQPSMean)
			meanQPS := func(targets []roachpb.StoreID) float64 {
				var totalQPS float64
				for _, store := range stores {
					for _, target := range targets {
						if target == store.StoreID {
							totalQPS = totalQPS + store.Capacity.QueriesPerSecond
							break
						}
					}
				}
				return totalQPS / float64(len(stores))
			}

			// Test setup boilerplate.
			gossiputil.NewStoreGossiper(g).GossipStores(stores, t)
			storeList, _, _ := a.storePool.getStoreList(storeFilterThrottled)
			localDesc := *stores[0]
			cfg := TestStoreConfig(nil)
			s := createTestStoreWithoutStart(t, stopper, testStoreOpts{createSystemRanges: true}, &cfg)
			s.Ident = &roachpb.StoreIdent{StoreID: localDesc.StoreID}
			rq := newReplicateQueue(s, a)
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
			a.storePool.isStoreReadyForRoutineReplicaTransfer = func(_ context.Context, this roachpb.StoreID) bool {
				for _, deadStore := range deadStores {
					// NodeID match StoreIDs here, so this comparison is valid.
					if deadStore.StoreID == this {
						return false
					}
				}
				return true
			}
			s.cfg.DefaultSpanConfig.NumVoters = int32(numVoters)
			s.cfg.DefaultSpanConfig.NumReplicas = int32(numVoters + numNonVoters)
			// Place voters on the first `numVoters` stores and place non-voters on the
			// next `numNonVoters` stores.
			var voterStores, nonVoterStores []roachpb.StoreID
			for i := 0; i < numVoters; i++ {
				voterStores = append(voterStores, stores[i].StoreID)
			}
			for i := numVoters; i < numVoters+numNonVoters; i++ {
				nonVoterStores = append(nonVoterStores, stores[i].StoreID)
			}
			loadRanges(
				rr, s, []testRange{
					{voters: voterStores, nonVoters: nonVoterStores, qps: perReplicaQPS},
				},
			)
			hottestRanges := rr.topQPS()
			_, voterTargets, nonVoterTargets := sr.chooseRangeToRebalance(
				ctx,
				&hottestRanges,
				&localDesc,
				storeList,
				qpsScorerOptions{
					deterministic:         false,
					qpsRebalanceThreshold: qpsRebalanceThreshold,
				},
			)
			var rebalancedVoterStores, rebalancedNonVoterStores []roachpb.StoreID
			for _, target := range voterTargets {
				rebalancedVoterStores = append(rebalancedVoterStores, target.StoreID)
			}
			for _, target := range nonVoterTargets {
				rebalancedNonVoterStores = append(rebalancedNonVoterStores, target.StoreID)
			}
			log.Infof(
				ctx,
				"rebalanced voters from %v to %v: %.2f qps -> %.2f qps",
				voterStores,
				voterTargets,
				meanQPS(voterStores),
				meanQPS(rebalancedVoterStores),
			)
			log.Infof(
				ctx,
				"rebalanced non-voters from %v to %v: %.2f qps -> %.2f qps",
				nonVoterStores,
				nonVoterTargets,
				meanQPS(nonVoterStores),
				meanQPS(rebalancedNonVoterStores),
			)
			if r, o := meanQPS(rebalancedVoterStores), meanQPS(voterStores); r-o > epsilon {
				t.Errorf("voters were rebalanced onto a set of stores with higher QPS (%.2f to %.2f)", o, r)
			}
			previousMean := meanQPS(append(voterStores, nonVoterStores...))
			newMean := meanQPS(append(rebalancedVoterStores, rebalancedNonVoterStores...))
			log.Infof(
				ctx,
				"rebalanced range from stores with %.2f average qps to %.2f average qps",
				previousMean,
				newMean,
			)
			if newMean-previousMean > epsilon {
				t.Errorf("replicas were rebalanced onto a set of stores with higher QPS (%.2f to %.2f)", previousMean, newMean)
			}
		})
	}
}

func TestChooseRangeToRebalanceAcrossHeterogeneousZones(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	constraint := func(region string, numReplicas int32) roachpb.ConstraintsConjunction {
		return roachpb.ConstraintsConjunction{
			NumReplicas: numReplicas,
			Constraints: []roachpb.Constraint{
				{
					Type:  roachpb.Constraint_REQUIRED,
					Key:   "region",
					Value: region,
				},
			},
		}
	}

	oneReplicaPerRegion := []roachpb.ConstraintsConjunction{
		constraint("a", 1),
		constraint("b", 1),
		constraint("c", 1),
	}
	twoReplicasInHotRegion := []roachpb.ConstraintsConjunction{
		constraint("a", 2),
	}
	allReplicasInHotRegion := []roachpb.ConstraintsConjunction{
		constraint("a", 3),
	}
	twoReplicasInSecondHottestRegion := []roachpb.ConstraintsConjunction{
		constraint("b", 2),
	}

	testCases := []struct {
		name                          string
		voters, nonVoters             []roachpb.StoreID
		voterConstraints, constraints []roachpb.ConstraintsConjunction

		// the first listed voter target is expected to be the leaseholder.
		expRebalancedVoters, expRebalancedNonVoters []roachpb.StoreID
	}{
		// All the replicas are already on the best possible stores. No
		// rebalancing should be attempted.
		{
			name:                "no rebalance",
			voters:              []roachpb.StoreID{3, 6, 9},
			constraints:         oneReplicaPerRegion,
			expRebalancedVoters: []roachpb.StoreID{9, 6, 3},
		},
		// A replica is in a heavily loaded region, on a relatively heavily loaded
		// store. We expect it to be moved to a less busy store within the same
		// region.
		{
			name:                "rebalance one replica within heavy region",
			voters:              []roachpb.StoreID{1, 6, 9},
			constraints:         oneReplicaPerRegion,
			expRebalancedVoters: []roachpb.StoreID{9, 6, 3},
		},
		// Two replicas are in the hot region, both on relatively heavily loaded
		// nodes. We expect one of those replicas to be moved to a less busy store
		// within the same region.
		{
			name:                "rebalance two replicas out of three within heavy region",
			voters:              []roachpb.StoreID{1, 2, 9},
			constraints:         twoReplicasInHotRegion,
			expRebalancedVoters: []roachpb.StoreID{9, 2, 3},
		},
		{
			name:        "rebalance two replicas out of five within heavy region",
			voters:      []roachpb.StoreID{1, 2, 6, 8, 9},
			constraints: twoReplicasInHotRegion,
			// NB: Because of the diversity heuristic we won't rebalance to node 7.
			expRebalancedVoters: []roachpb.StoreID{9, 3, 6, 8, 2},
		},
		// In the absence of any constraints, ensure that as long as diversity is
		// maximized, replicas on hot stores are rebalanced to cooler stores within
		// the same region.
		{
			// Within the hottest region, expect rebalance from the hottest node (n1)
			// to the coolest node (n3). Within the lease hot region, expect movement
			// from n8 to n9.
			name:                "QPS balance without constraints",
			voters:              []roachpb.StoreID{1, 5, 8},
			expRebalancedVoters: []roachpb.StoreID{9, 5, 3},
		},
		{
			// Within the second hottest region, expect rebalance from the hottest
			// node (n4) to the coolest node (n6). Within the lease hot region, expect
			// movement from n8 to n9.
			name:                "QPS balance without constraints",
			voters:              []roachpb.StoreID{8, 4, 3},
			expRebalancedVoters: []roachpb.StoreID{9, 6, 3},
		},

		// Multi-region database configurations.
		{
			name:      "primary region with highest QPS, zone survival, one non-voter on hot node",
			voters:    []roachpb.StoreID{1, 2, 3},
			nonVoters: []roachpb.StoreID{4, 9},
			// Pin all voters to the hottest region (region A) and have overall
			// constraints require at least one replica per each region.
			voterConstraints: allReplicasInHotRegion,
			constraints:      oneReplicaPerRegion,

			expRebalancedVoters: []roachpb.StoreID{3, 2, 1},
			// NB: Expect the non-voter on node 4 (hottest node in region B) to move
			// to node 6 (least hot region in region B).
			expRebalancedNonVoters: []roachpb.StoreID{6, 9},
		},
		{
			name:   "primary region with second highest QPS, region survival, one voter on sub-optimal node",
			voters: []roachpb.StoreID{3, 4, 5, 8, 9},
			// Pin two voters to the second hottest region (region B) and have overall
			// constraints require at least one replica per each region.
			voterConstraints: twoReplicasInSecondHottestRegion,
			constraints:      oneReplicaPerRegion,
			// NB: Expect the voter on node 4 (hottest node in region B) to move to
			// node 6 (least hot region in region B).
			expRebalancedVoters: []roachpb.StoreID{9, 5, 6, 8, 3},
		},
		{
			name:   "primary region with highest QPS, region survival, two voters on sub-optimal nodes",
			voters: []roachpb.StoreID{1, 2, 3, 4, 9},
			// Pin two voters to the hottest region (region A) and have overall
			// constraints require at least one replica per each region.
			voterConstraints: twoReplicasInHotRegion,
			constraints:      oneReplicaPerRegion,
			// NB: We've got 3 voters in the hottest region, but we only need 2. We
			// expect that one of the voters from the hottest region will be moved to
			// the least hot region.
			expRebalancedVoters: []roachpb.StoreID{9, 2, 4, 8, 3},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Boilerplate for test setup.
			stopper, g, _, a, _ := createTestAllocator(10, false /* deterministic */)
			defer stopper.Stop(context.Background())
			gossiputil.NewStoreGossiper(g).GossipStores(multiRegionStores, t)
			storeList, _, _ := a.storePool.getStoreList(storeFilterThrottled)

			var localDesc roachpb.StoreDescriptor
			for _, store := range multiRegionStores {
				if store.StoreID == tc.voters[0] {
					localDesc = *store
				}
			}
			cfg := TestStoreConfig(nil)
			s := createTestStoreWithoutStart(t, stopper, testStoreOpts{createSystemRanges: true}, &cfg)
			s.Ident = &roachpb.StoreIdent{StoreID: localDesc.StoreID}
			rq := newReplicateQueue(s, a)
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

			s.cfg.DefaultSpanConfig.NumVoters = int32(len(tc.voters))
			s.cfg.DefaultSpanConfig.NumReplicas = int32(len(tc.voters) + len(tc.nonVoters))
			s.cfg.DefaultSpanConfig.Constraints = tc.constraints
			s.cfg.DefaultSpanConfig.VoterConstraints = tc.voterConstraints
			const testingQPS = float64(50)
			loadRanges(
				rr, s, []testRange{
					{voters: tc.voters, nonVoters: tc.nonVoters, qps: testingQPS},
				},
			)
			hottestRanges := rr.topQPS()
			_, voterTargets, nonVoterTargets := sr.chooseRangeToRebalance(
				ctx,
				&hottestRanges,
				&localDesc,
				storeList,
				qpsScorerOptions{deterministic: true, qpsRebalanceThreshold: 0.05},
			)

			require.Len(t, voterTargets, len(tc.expRebalancedVoters))
			if len(voterTargets) > 0 && voterTargets[0].StoreID != tc.expRebalancedVoters[0] {
				t.Errorf("chooseRangeToRebalance(existing=%v, qps=%f) chose s%d as leaseholder; want s%v",
					tc.voters, testingQPS, voterTargets[0], tc.expRebalancedVoters[0])
			}

			voterStoreIDs := make([]roachpb.StoreID, len(voterTargets))
			for i, target := range voterTargets {
				voterStoreIDs[i] = target.StoreID
			}
			require.ElementsMatch(t, voterStoreIDs, tc.expRebalancedVoters)
			// Check that things "still work" when `VoterConstraints` are used
			// instead.
			s.cfg.DefaultSpanConfig.Constraints = []roachpb.ConstraintsConjunction{}
			s.cfg.DefaultSpanConfig.VoterConstraints = tc.constraints
			require.ElementsMatch(t, voterStoreIDs, tc.expRebalancedVoters)

			require.Len(t, nonVoterTargets, len(tc.expRebalancedNonVoters))
			nonVoterStoreIDs := make([]roachpb.StoreID, len(nonVoterTargets))
			for i, target := range nonVoterTargets {
				nonVoterStoreIDs[i] = target.StoreID
			}
			require.ElementsMatch(t, nonVoterStoreIDs, tc.expRebalancedNonVoters)
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

	stopper, g, _, a, _ := createTestAllocatorWithKnobs(
		10,
		false, /* deterministic */
		&AllocatorTestingKnobs{AllowLeaseTransfersToReplicasNeedingSnapshots: true},
	)
	defer stopper.Stop(context.Background())
	storeList, _, _ := a.storePool.getStoreList(storeFilterThrottled)
	storeMap := storeListToMap(storeList)

	localDesc := *noLocalityStores[0]
	cfg := TestStoreConfig(nil)
	cfg.Gossip = g
	cfg.StorePool = a.storePool
	s := createTestStoreWithoutStart(t, stopper, testStoreOpts{createSystemRanges: true}, &cfg)
	gossiputil.NewStoreGossiper(cfg.Gossip).GossipStores(noLocalityStores, t)
	s.Ident = &roachpb.StoreIdent{StoreID: localDesc.StoreID}
	rq := newReplicateQueue(s, a)
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

	_, target, _ := sr.chooseLeaseToTransfer(ctx, &hottestRanges, &localDesc, storeList, storeMap)
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
		ctx,
		&hottestRanges,
		&localDesc,
		storeList,
		qpsScorerOptions{deterministic: true, qpsRebalanceThreshold: 0.05},
	)
	expectTargets := []roachpb.ReplicationTarget{
		{NodeID: 4, StoreID: 4}, {NodeID: 3, StoreID: 3}, {NodeID: 5, StoreID: 5},
	}
	if !reflect.DeepEqual(targets, expectTargets) {
		t.Errorf("got targets %v for range with RaftStatus %v; want %v",
			targets, sr.getRaftStatusFn(repl), expectTargets)
	}
}
