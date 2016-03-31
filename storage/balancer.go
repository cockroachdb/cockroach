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

import "github.com/cockroachdb/cockroach/roachpb"

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

func (rcb rangeCountBalancer) selectGood(sl StoreList, excluded nodeIDSet) *roachpb.StoreDescriptor {
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

func (rcb rangeCountBalancer) improve(store *roachpb.StoreDescriptor, sl StoreList,
	excluded nodeIDSet) *roachpb.StoreDescriptor {
	// If existing replica has a stable range count, return false immediately.
	if float64(store.Capacity.RangeCount) < sl.count.mean*(1+rebalanceFromMean) {
		return nil
	}

	// Attempt to select a better candidate from the supplied list.
	// Only approve the candidate if its range count is sufficiently below the
	// cluster mean.
	candidate := rcb.selectGood(sl, excluded)
	if candidate == nil {
		return nil
	}
	if float64(candidate.Capacity.RangeCount) <= sl.count.mean*(1-rebalanceFromMean) {
		return candidate
	}
	return nil
}

// usedCapacityBalancer attempts to balance ranges by considering the used
// disk capacity of each store.
type usedCapacityBalancer struct {
	rand allocatorRand
}

func (ucb usedCapacityBalancer) selectGood(sl StoreList, excluded nodeIDSet) *roachpb.StoreDescriptor {
	// Consider a random sample of stores from the store list.
	candidates := selectRandom(ucb.rand, 3, sl, excluded)
	var best *roachpb.StoreDescriptor
	for _, candidate := range candidates {
		if best == nil {
			best = candidate
			continue
		}
		if candidate.Capacity.FractionUsed() <= best.Capacity.FractionUsed() {
			best = candidate
		}
	}
	return best
}

func (ucb usedCapacityBalancer) selectBad(sl StoreList) *roachpb.StoreDescriptor {
	var worst *roachpb.StoreDescriptor
	for _, candidate := range sl.stores {
		if worst == nil {
			worst = candidate
			continue
		}
		if candidate.Capacity.FractionUsed() > worst.Capacity.FractionUsed() {
			worst = candidate
		}
	}
	return worst
}

func (ucb usedCapacityBalancer) improve(store *roachpb.StoreDescriptor, sl StoreList,
	excluded nodeIDSet) *roachpb.StoreDescriptor {
	storeFraction := store.Capacity.FractionUsed()

	// If existing replica has stable capacity usage, return false immediately.
	if float64(storeFraction) < sl.used.mean*(1+rebalanceFromMean) {
		return nil
	}

	// Attempt to select a better candidate from the supplied list.  Only
	// approve the candidate if its disk usage is sufficiently below the cluster
	// mean.
	candidate := ucb.selectGood(sl, excluded)
	if candidate == nil {
		return nil
	}
	if candidate.Capacity.FractionUsed() < sl.used.mean*(1-rebalanceFromMean) {
		return candidate
	}
	return nil
}

// selectRandom chooses up to count random store descriptors from the given
// store list.
func selectRandom(randGen allocatorRand, count int, sl StoreList,
	excluded nodeIDSet) []*roachpb.StoreDescriptor {
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
