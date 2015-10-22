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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Matt Tracy (matt@cockroachlabs.com)

package storage

import (
	"math/rand"

	"github.com/cockroachdb/cockroach/roachpb"
)

// A Balancer is an object that makes decisions on how to move ranges between
// nodes on the cluster, based on collected statistics, in order to optimally
// balance the workload across the cluster.
type balancer interface {
	// selectGood attempts to select a store from the supplied store list that
	// it considers to be 'Good' relative to the other stores in the list.
	// Returns the selected store or nil if no such store can be found.
	selectGood(sl StoreList) *roachpb.StoreDescriptor
	// selectBad attempts to select a store from the supplied store list that it
	// considers to be 'Bad' relative to the other stores in the list. Returns
	// the selected store or nil if no such store can be found.
	selectBad(sl StoreList) *roachpb.StoreDescriptor
	// improve attempts to select an improvement over the given store from the
	// stores in the given store list. Returns the selected store nil if no such
	// store can be found.
	improve(store *roachpb.StoreDescriptor, sl StoreList) *roachpb.StoreDescriptor
}

// rangeCountBalancer attempts to balance ranges across the cluster while
// considering only the number of ranges being serviced each store.
type rangeCountBalancer struct {
	rand *rand.Rand
}

func (rcb rangeCountBalancer) selectGood(sl StoreList) *roachpb.StoreDescriptor {
	// Consider a random sample of stores from the store list.
	candidates := selectRandom(rcb.rand, 3, sl)
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
	// Consider a random sample of stores from the store list.
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

func (rcb rangeCountBalancer) improve(store *roachpb.StoreDescriptor, sl StoreList) *roachpb.StoreDescriptor {
	// If existing replica has a stable range count, return false immediately.
	if float64(store.Capacity.RangeCount) < sl.count.mean*(1+rebalanceFromMean) {
		return nil
	}

	// Attempt to select a better candidate from the supplied list.
	// Only approve the candidate if its range count is sufficiently below the
	// cluster mean.
	candidate := rcb.selectGood(sl)
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
	rand *rand.Rand
}

func (ucb usedCapacityBalancer) selectGood(sl StoreList) *roachpb.StoreDescriptor {
	// Consider a random sample of stores from the store list.
	candidates := selectRandom(ucb.rand, 3, sl)
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

func (ucb usedCapacityBalancer) improve(store *roachpb.StoreDescriptor, sl StoreList) *roachpb.StoreDescriptor {
	storeFraction := store.Capacity.FractionUsed()

	// If existing replica has stable capacity usage,, return false immediately.
	if float64(storeFraction) < sl.used.mean*(1+rebalanceFromMean) {
		return nil
	}

	// Attempt to select a better candidate from the supplied list.  Only
	// approve the candidate if its disk usage is sufficiently below the cluster
	// mean.
	candidate := ucb.selectGood(sl)
	if candidate == nil {
		return nil
	}
	if candidate.Capacity.FractionUsed() < sl.used.mean*(1-rebalanceFromMean) {
		return candidate
	}
	return nil
}

// defaultBalancer is the default rebalancer currently used by Cockroach. It
// multiplexes RangeCountBalancer and UsedCapacityBalancer, preferring the
// latter but using RangeCountBalancer on clusters with very low average disk
// usage.
type defaultBalancer struct {
	rand *rand.Rand
}

func (db defaultBalancer) selectGood(sl StoreList) *roachpb.StoreDescriptor {
	if sl.used.mean < minFractionUsedThreshold {
		rcb := rangeCountBalancer{db.rand}
		return rcb.selectGood(sl)
	}
	ucb := usedCapacityBalancer{db.rand}
	return ucb.selectGood(sl)
}

func (db defaultBalancer) selectBad(sl StoreList) *roachpb.StoreDescriptor {
	if sl.used.mean < minFractionUsedThreshold {
		rcb := rangeCountBalancer{db.rand}
		return rcb.selectBad(sl)
	}
	ucb := usedCapacityBalancer{db.rand}
	return ucb.selectBad(sl)
}

func (db defaultBalancer) improve(store *roachpb.StoreDescriptor, sl StoreList) *roachpb.StoreDescriptor {
	if sl.used.mean < minFractionUsedThreshold {
		rcb := rangeCountBalancer{db.rand}
		return rcb.improve(store, sl)
	}
	ucb := usedCapacityBalancer{db.rand}
	return ucb.improve(store, sl)
}

// selectRandom chooses up to count random store descriptors from the given
// store list.
func selectRandom(randGen *rand.Rand, count int, sl StoreList) []*roachpb.StoreDescriptor {
	var descs []*roachpb.StoreDescriptor
	// Randomly permute available stores matching the required attributes.
	for _, idx := range randGen.Perm(len(sl.stores)) {
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
