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

	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
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

const defaultQPSRebalanceThreshold = 0.25

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
					[]roachpb.Tier{
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
					[]roachpb.Tier{
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
					[]roachpb.Tier{
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
					[]roachpb.Tier{
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
					[]roachpb.Tier{
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
					[]roachpb.Tier{
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
					[]roachpb.Tier{
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
					[]roachpb.Tier{
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
					[]roachpb.Tier{
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
		qpsRebalanceThreshold = 0.1
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
			a.storePool.isNodeReadyForRoutineReplicaTransfer = func(_ context.Context, n roachpb.NodeID) bool {
				for _, s := range deadStores {
					// NodeID match StoreIDs here, so this comparison is valid.
					if roachpb.NodeID(s.StoreID) == n {
						return false
					}
				}
				return true
			}
			s.cfg.DefaultZoneConfig.NumVoters = proto.Int32(int32(numVoters))
			s.cfg.DefaultZoneConfig.NumReplicas = proto.Int32(int32(numVoters + numNonVoters))
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
			require.GreaterOrEqualf(
				t,
				meanQPS(voterStores),
				meanQPS(rebalancedVoterStores),
				"voters were rebalanced onto a set of stores with higher QPS",
			)

			previousMean := meanQPS(append(voterStores, nonVoterStores...))
			newMean := meanQPS(append(rebalancedVoterStores, rebalancedNonVoterStores...))
			log.Infof(
				ctx,
				"rebalanced range from stores with %.2f average qps to  %.2f average qps",
				previousMean,
				newMean,
			)
			require.GreaterOrEqualf(
				t,
				previousMean,
				newMean,
				"replicas were rebalanced onto a set of stores with higher QPS",
			)
		})
	}
}

func TestChooseRangeToRebalanceAcrossHeterogeneousZones(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	constraint := func(region string, numReplicas int32) zonepb.ConstraintsConjunction {
		return zonepb.ConstraintsConjunction{
			NumReplicas: numReplicas,
			Constraints: []zonepb.Constraint{
				{
					Type:  zonepb.Constraint_REQUIRED,
					Key:   "region",
					Value: region,
				},
			},
		}
	}

	oneReplicaPerRegion := []zonepb.ConstraintsConjunction{
		constraint("a", 1),
		constraint("b", 1),
		constraint("c", 1),
	}
	twoReplicasInHotRegion := []zonepb.ConstraintsConjunction{
		constraint("a", 2),
	}

	testCases := []struct {
		name              string
		voters, nonVoters []roachpb.StoreID
		constraints       []zonepb.ConstraintsConjunction

		qps float64
		// the first listed voter target is expected to be the leaseholder.
		expRebalancedVoters, expectedRebalancedNonVoters []roachpb.StoreID
	}{
		// All the replicas are already on the best possible stores. No
		// rebalancing should be attempted.
		{
			name:                "no rebalance",
			voters:              []roachpb.StoreID{3, 6, 9},
			constraints:         oneReplicaPerRegion,
			expRebalancedVoters: []roachpb.StoreID{9, 6, 3},
			qps:                 50,
		},
		// A replica is in a heavily loaded region, on a relatively heavily loaded
		// store. We expect it to be moved to a less busy store within the same
		// region.
		{
			name:                "rebalance one replica within heavy region",
			voters:              []roachpb.StoreID{1, 6, 9},
			constraints:         oneReplicaPerRegion,
			expRebalancedVoters: []roachpb.StoreID{9, 6, 3},
			qps:                 50,
		},
		// Two replicas are in the hot region, both on relatively heavily loaded
		// nodes. We expect one of those replicas to be moved to a less busy store
		// within the same region.
		{
			name:                "rebalance two replicas out of three within heavy region",
			voters:              []roachpb.StoreID{1, 2, 9},
			constraints:         twoReplicasInHotRegion,
			expRebalancedVoters: []roachpb.StoreID{9, 2, 3},
			qps:                 50,
		},
		{
			name:        "rebalance two replicas out of five within heavy region",
			voters:      []roachpb.StoreID{1, 2, 6, 8, 9},
			constraints: twoReplicasInHotRegion,
			// NB: Because of the diversity heuristic we won't rebalance to node 7.
			expRebalancedVoters: []roachpb.StoreID{9, 3, 6, 8, 2},
			qps:                 50,
		},
		// In the absence of any constraints, ensure that as long as diversity is
		// maximized, replicas on hot stores are rebalanced to cooler stores within
		// the same region.
		{
			// Within the hottest region, expect rebalance from the hottest node (n1)
			// to the coolest node (n3).
			name:                "QPS balance without constraints",
			voters:              []roachpb.StoreID{1, 5, 8},
			expRebalancedVoters: []roachpb.StoreID{8, 5, 3},
			qps:                 50,
		},
		{
			// Within the second hottest region, expect rebalance from the hottest
			// node (n4) to the coolest node (n6).
			name:                "QPS balance without constraints",
			voters:              []roachpb.StoreID{8, 4, 3},
			expRebalancedVoters: []roachpb.StoreID{8, 6, 3},
			qps:                 50,
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

			s.cfg.DefaultZoneConfig.NumVoters = proto.Int32(int32(len(tc.voters)))
			s.cfg.DefaultZoneConfig.NumReplicas = proto.Int32(int32(len(tc.voters) + len(tc.nonVoters)))
			s.cfg.DefaultZoneConfig.Constraints = tc.constraints
			loadRanges(
				rr, s, []testRange{
					{voters: tc.voters, nonVoters: tc.nonVoters, qps: tc.qps},
				},
			)
			hottestRanges := rr.topQPS()
			_, voterTargets, nonVoterTargets := sr.chooseRangeToRebalance(
				ctx,
				&hottestRanges,
				&localDesc,
				storeList,
			)

			require.Len(t, voterTargets, len(tc.expRebalancedVoters))
			if len(voterTargets) > 0 && voterTargets[0].StoreID != tc.expRebalancedVoters[0] {
				t.Errorf("chooseRangeToRebalance(existing=%v, qps=%f) chose s%d as leaseholder; want s%v",
					tc.voters, tc.qps, voterTargets[0], tc.expRebalancedVoters[0])
			}

			voterStoreIDs := make([]roachpb.StoreID, len(voterTargets))
			for i, target := range voterTargets {
				voterStoreIDs[i] = target.StoreID
			}
			require.ElementsMatch(t, voterStoreIDs, tc.expRebalancedVoters)
			// Check that things "still work" when `VoterConstraints` are used
			// instead.
			s.cfg.DefaultZoneConfig.Constraints = []zonepb.ConstraintsConjunction{}
			s.cfg.DefaultZoneConfig.VoterConstraints = tc.constraints
			require.ElementsMatch(t, voterStoreIDs, tc.expRebalancedVoters)

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

	_, targets, _ := sr.chooseRangeToRebalance(ctx, &hottestRanges, &localDesc, storeList)
	expectTargets := []roachpb.ReplicationTarget{
		{NodeID: 4, StoreID: 4}, {NodeID: 3, StoreID: 3}, {NodeID: 5, StoreID: 5},
	}
	if !reflect.DeepEqual(targets, expectTargets) {
		t.Errorf("got targets %v for range with RaftStatus %v; want %v",
			targets, sr.getRaftStatusFn(repl), expectTargets)
	}
}
