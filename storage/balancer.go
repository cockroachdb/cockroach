// Copyright 2014 The Cockroach Authors.
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
//
// Author: Matt Tracy (matt@cockroachlabs.com)

package storage

import (
	"math"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util/log"
)

type nodeIDSet map[roachpb.NodeID]struct{}

func makeNodeIDSet(nodeIDs ...roachpb.NodeID) nodeIDSet {
	nodeSet := make(nodeIDSet, len(nodeIDs))
	for _, id := range nodeIDs {
		nodeSet[id] = struct{}{}
	}
	return nodeSet
}

// rangeCountBalancer attempts to balance ranges across the cluster while
// considering only the number of ranges being serviced each store.
type rangeCountBalancer struct {
	rand allocatorRand
}

func (rcb rangeCountBalancer) selectGood(
	sl StoreList, excluded nodeIDSet,
) *roachpb.StoreDescriptor {
	// Consider a random sample of stores from the store list.
	candidates := selectRandom(rcb.rand, 3, sl, excluded)
	var best *roachpb.StoreDescriptor
	for _, candidate := range candidates {
		if best == nil {
			best = candidate
			continue
		}
		if candidate.Capacity.RangeCount < best.Capacity.RangeCount {
			best = candidate
		}
	}
	return best
}

func (rcb rangeCountBalancer) selectBad(sl StoreList) *roachpb.StoreDescriptor {
	var worst *roachpb.StoreDescriptor
	for _, candidate := range sl.stores {
		if worst == nil {
			worst = candidate
			continue
		}
		if candidate.Capacity.RangeCount > worst.Capacity.RangeCount {
			worst = candidate
		}
	}
	return worst
}

// improve returns a candidate StoreDescriptor to rebalance a replica to. The
// strategy is to always converge on the mean range count. If that isn't
// possible, we don't return any candidate.
func (rcb rangeCountBalancer) improve(
	store *roachpb.StoreDescriptor, sl StoreList, excluded nodeIDSet,
) *roachpb.StoreDescriptor {
	// Moving a replica from the given store makes its range count converge on
	// the mean range count.
	if store.Capacity.FractionUsed() <= maxFractionUsedThreshold &&
		(math.Abs(float64(store.Capacity.RangeCount-1)-sl.candidateCount.mean) >
			math.Abs(float64(store.Capacity.RangeCount)-sl.candidateCount.mean)) {
		if log.V(2) {
			log.Infof("not rebalancing: source store %d wouldn't converge on the mean",
				store.StoreID)
		}
		return nil
	}

	// Attempt to select a better candidate from the supplied list.
	candidate := rcb.selectGood(sl, excluded)
	if candidate == nil {
		if log.V(2) {
			log.Infof("not rebalancing: no candidate targets (all stores nearly full?)")
		}
		return nil
	}

	// Adding a replica to the candidate must make its range count converge on the
	// mean range count.
	if math.Abs(float64(candidate.Capacity.RangeCount+1)-sl.candidateCount.mean) >=
		math.Abs(float64(candidate.Capacity.RangeCount)-sl.candidateCount.mean) {
		if log.V(2) {
			log.Infof("not rebalancing: candidate store %d wouldn't converge on the mean",
				store.StoreID)
		}
		return nil
	}

	if log.V(2) {
		log.Infof("found candidate store %d", candidate.StoreID)
	}

	return candidate
}

// selectRandom chooses up to count random store descriptors from the given
// store list, excluding any stores that are too full to accept more replicas.
func selectRandom(
	randGen allocatorRand, count int, sl StoreList, excluded nodeIDSet,
) []*roachpb.StoreDescriptor {
	var descs []*roachpb.StoreDescriptor
	// Randomly permute available stores matching the required attributes.
	randGen.Lock()
	defer randGen.Unlock()
	for _, idx := range randGen.Perm(len(sl.stores)) {
		desc := sl.stores[idx]
		// Skip if store is in excluded set.
		if _, ok := excluded[desc.Node.NodeID]; ok {
			continue
		}

		// Don't overfill stores.
		if desc.Capacity.FractionUsed() > maxFractionUsedThreshold {
			continue
		}

		// Add this store; exit loop if we've satisfied count.
		descs = append(descs, sl.stores[idx])
		if len(descs) >= count {
			break
		}
	}
	if len(descs) == 0 {
		return nil
	}
	return descs
}
