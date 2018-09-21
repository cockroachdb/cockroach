// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package storage

import (
	"context"
	"reflect"
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils/gossiputil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
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
		repl.mu.zone = config.DefaultZoneConfigRef()
		for _, storeID := range r.storeIDs {
			repl.mu.state.Desc.Replicas = append(repl.mu.state.Desc.Replicas, roachpb.ReplicaDescriptor{
				NodeID:    roachpb.NodeID(storeID),
				StoreID:   storeID,
				ReplicaID: roachpb.ReplicaID(storeID),
			})
		}
		repl.mu.state.Lease = &roachpb.Lease{
			Expiration: &hlc.MaxTimestamp,
			Replica:    repl.mu.state.Desc.Replicas[0],
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

	stopper, g, _, a, _ := createTestAllocator( /* deterministic */ false)
	defer stopper.Stop(context.Background())
	gossiputil.NewStoreGossiper(g).GossipStores(noLocalityStores, t)
	storeList, _, _ := a.storePool.getStoreList(firstRange, storeFilterThrottled)
	storeMap := storeListToMap(storeList)

	const minQPS = 800
	const maxQPS = 1200

	localDesc := *noLocalityStores[0]
	cfg := TestStoreConfig(nil)
	s := createTestStoreWithoutStart(t, stopper, &cfg)
	s.Ident = &roachpb.StoreIdent{StoreID: localDesc.StoreID}
	rq := newReplicateQueue(s, g, a)
	rr := newReplicaRankings()

	sr := NewStoreRebalancer(cfg.AmbientCtx, cfg.Settings, rq, rr)

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

	stopper, g, _, a, _ := createTestAllocator( /* deterministic */ false)
	defer stopper.Stop(context.Background())
	gossiputil.NewStoreGossiper(g).GossipStores(noLocalityStores, t)
	storeList, _, _ := a.storePool.getStoreList(firstRange, storeFilterThrottled)
	storeMap := storeListToMap(storeList)

	const minQPS = 800
	const maxQPS = 1200

	localDesc := *noLocalityStores[0]
	cfg := TestStoreConfig(nil)
	s := createTestStoreWithoutStart(t, stopper, &cfg)
	s.Ident = &roachpb.StoreIdent{StoreID: localDesc.StoreID}
	rq := newReplicateQueue(s, g, a)
	rr := newReplicaRankings()

	sr := NewStoreRebalancer(cfg.AmbientCtx, cfg.Settings, rq, rr)

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
			zone := config.DefaultZoneConfig()
			zone.NumReplicas = int32(len(tc.storeIDs))
			defer config.TestingSetDefaultZoneConfig(zone)()
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
