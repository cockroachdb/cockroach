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
// Author: Spencer Kimball (spencer.kimball@gmail.com)
// Author: Kathy Spradlin (kathyspradlin@gmail.com)

package storage

import (
	"fmt"
	"math/rand"
	"sort"
	"sync"

	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/util"
)

const (
	// minAvailCapacityThreshold: if the capacity of a store descriptor
	// falls below this percentage, it will never be used as an
	// rebalance target.
	minAvailCapacityThreshold = 0.05
	// maxAvailCapacityThreshold: if the mean available capacity of a
	// list of store descriptors falls above this percentage, then the
	// count of ranges on stores will be used to make rebalancing
	// decisions instead of capacity.
	maxAvailCapacityThreshold = 0.98
	// rebalancePctFromMean: if the capacity or range count of a store
	// is within rebalancePctFromMean of the mean, it is considered a
	// viable target for rebalancing.
	rebalancePctFromMean = 0.025 // 2.5%
)

// stat provides a running sample size and mean.
type stat struct {
	n, mean float64
}

// Update adds the specified value to the stat, augmenting the sample
// size & mean.
func (s *stat) Update(x float64) {
	s.n += 1
	s.mean += (x - s.mean) / s.n
}

// storeList keeps a list of store descriptors and associated count
// and capacity stats across the stores.
type storeList struct {
	stores          []*proto.StoreDescriptor
	count, capacity stat
}

// Update updates count and capacity stats using the specified descriptor.
func (sl *storeList) Update(s *proto.StoreDescriptor) {
	sl.stores = append(sl.stores, s)
	sl.count.Update(float64(s.Capacity.RangeCount))
	sl.capacity.Update(s.Capacity.PercentAvail())
}

// allocator makes allocation decisions based on available capacity
// in other stores which match the required attributes for a desired
// range replica.
//
// The allocator listens for gossip updates from stores and updates
// statistics for mean and variance of available capacity and total
// range count.
//
// When choosing a new allocation target, three candidates from
// available stores meeting a minimum available capacity threshold
// (minAvailCapacityThreshold) are chosen at random and the least
// loaded of the three is selected in order to bias loading towards a
// more balanced cluster, but while still spreading load over all
// available servers. "Load" is defined according to capacity if mean
// capacity is less than maxAvailCapacityThreshold; otherwise
// according to number of ranges.
//
// When choosing a rebalance target, a random store is selected from
// amongst the set of stores with capacity within rebalancePctFromMean
// from the mean.
type allocator struct {
	sync.Mutex
	gossip        *gossip.Gossip
	randGen       *rand.Rand
	deterministic bool                  // Set deterministic for unittests
	capacityKeys  map[string]struct{}   // Tracks gossip keys used for capacity
	storeLists    map[string]*storeList // Cache from attributes to storeList
}

// newAllocator creates a new allocator for the store specified by
// storeID.
func newAllocator(g *gossip.Gossip) *allocator {
	a := &allocator{
		gossip:  g,
		randGen: rand.New(rand.NewSource(rand.Int63())),
	}
	// Callback triggers on any capacity gossip updates.
	if a.gossip != nil {
		capacityRegex := gossip.MakePrefixPattern(gossip.KeyCapacityPrefix)
		a.gossip.RegisterCallback(capacityRegex, a.capacityGossipUpdate)
	}
	return a
}

// getUsedNodes returns a set of node IDs which are already being used
// to store replicas.
func getUsedNodes(existing []proto.Replica) map[proto.NodeID]struct{} {
	// Get a set of current nodes -- we never want to allocate on an existing node.
	usedNodes := map[proto.NodeID]struct{}{}
	for _, replica := range existing {
		usedNodes[replica.NodeID] = struct{}{}
	}
	return usedNodes
}

// storeDescFromGossip retrieves a StoreDescriptor from the specified
// capacity gossip key. Returns an error if the gossip doesn't exist
// or is not a StoreDescriptor.
func storeDescFromGossip(key string, g *gossip.Gossip) (*proto.StoreDescriptor, error) {
	info, err := g.GetInfo(key)

	if err != nil {
		return nil, err
	}
	storeDesc, ok := info.(proto.StoreDescriptor)
	if !ok {
		return nil, fmt.Errorf("gossiped info is not a StoreDescriptor: %+v", info)
	}
	return &storeDesc, nil
}

// capacityGossipUpdate is a gossip callback triggered whenever capacity
// information is gossiped. It just tracks keys used for capacity
// gossip.
func (a *allocator) capacityGossipUpdate(key string, contentsChanged bool) {
	a.Lock()
	defer a.Unlock()

	// Clear the cached store lists on new gossip.
	a.storeLists = nil
	if a.capacityKeys == nil {
		a.capacityKeys = map[string]struct{}{}
	}
	a.capacityKeys[key] = struct{}{}
}

// Allocate returns a suitable store for a new allocation with the
// required attributes. Nodes already accommodating existing replicas
// are ruled out as targets.
func (a *allocator) Allocate(required proto.Attributes, existing []proto.Replica) (
	*proto.StoreDescriptor, error) {
	a.Lock()
	defer a.Unlock()
	stores, err := a.selectRandom(3, required, existing, nil)
	if err != nil {
		return nil, err
	}

	// Choose the store with the highest percent capacity available.
	var highStore *proto.StoreDescriptor
	for _, s := range stores {
		if highStore == nil || s.Capacity.PercentAvail() > highStore.Capacity.PercentAvail() {
			highStore = s
		}
	}
	return highStore, nil
}

// Rebalance returns a suitable store for a rebalance target with
// required attributes. Rebalance targets are selected at random
// from amongst stores which fall within an acceptable multiple
// of standard deviations from either the capacity or range count
// means.
func (a *allocator) Rebalance(required proto.Attributes, existing []proto.Replica) (
	*proto.StoreDescriptor, error) {
	a.Lock()
	defer a.Unlock()
	filter := func(s *proto.StoreDescriptor, count, capacity *stat) bool {
		// Use counts instead of capacities if the cluster has mean capacity
		// below a threshold level. This is primarily useful for balancing
		// load evenly in nascent deployments.
		if capacity.mean > maxAvailCapacityThreshold {
			return float64(s.Capacity.RangeCount) < count.mean
		}
		minAvailCapacity := capacity.mean * (1 + rebalancePctFromMean)
		if minAvailCapacityThreshold > minAvailCapacity {
			minAvailCapacity = minAvailCapacityThreshold
		}
		return s.Capacity.PercentAvail() > minAvailCapacity
	}
	stores, err := a.selectRandom(1, required, existing, filter)
	if err != nil {
		return nil, err
	}
	return stores[0], nil
}

// ShouldRebalance returns whether the specified store is overweight
// according to the cluster mean and should rebalance a range.
func (a *allocator) ShouldRebalance(s *proto.StoreDescriptor) bool {
	a.Lock()
	defer a.Unlock()
	sl := a.findStores(*s.CombinedAttrs())

	if sl.capacity.mean > maxAvailCapacityThreshold {
		return float64(s.Capacity.RangeCount) > sl.count.mean
	}
	return s.Capacity.PercentAvail() < sl.capacity.mean
}

// selectRandom chooses count random store descriptors which match
// the required attributes, do not include any of the existing
// replicas, and return true when passed to the provided filter.
// If the supplied filter is nil, it is ignored.
func (a *allocator) selectRandom(count int, required proto.Attributes, existing []proto.Replica,
	filter func(*proto.StoreDescriptor, *stat, *stat) bool) ([]*proto.StoreDescriptor, error) {
	var descs []*proto.StoreDescriptor
	sl := a.findStores(required)
	used := getUsedNodes(existing)

	// Randomly permute available stores matching the required attributes.
	for _, idx := range a.randGen.Perm(len(sl.stores)) {
		// Skip used nodes.
		if _, ok := used[sl.stores[idx].Node.NodeID]; ok {
			continue
		}
		// Filter descriptor.
		if filter != nil && !filter(sl.stores[idx], &sl.count, &sl.capacity) {
			continue
		}
		// Add this store; exit loop if we've satisfied count.
		descs = append(descs, sl.stores[idx])
		if len(descs) >= count {
			break
		}
	}
	if len(descs) == 0 {
		return nil, util.Errorf("no available stores for %s", required)
	}
	return descs, nil
}

// findStores returns a store list matching the required attributes.
// Results are cached for performance.
//
// If it cannot retrieve a StoreDescriptor from the Store's gossip, it
// garbage collects the failed key.
//
// TODO(embark, spencer): consider using a reverse index map from
// Attr->stores, for efficiency. Ensure that entries in this map still
// have an opportunity to be garbage collected.
func (a *allocator) findStores(required proto.Attributes) *storeList {
	if a.storeLists == nil {
		a.storeLists = map[string]*storeList{}
	}
	key := required.SortedString()
	if sl, ok := a.storeLists[key]; ok {
		return sl
	}
	sl := &storeList{}
	a.storeLists[key] = sl

	updateStoreList := func(key string) {
		storeDesc, err := storeDescFromGossip(key, a.gossip)
		if err != nil {
			// We can no longer retrieve this key from the gossip store,
			// perhaps it expired.
			delete(a.capacityKeys, key)
		} else if required.IsSubset(*storeDesc.CombinedAttrs()) {
			sl.Update(storeDesc)
		}
	}

	if a.deterministic {
		var keys []string
		for key := range a.capacityKeys {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			updateStoreList(key)
		}
		return sl
	}

	for key := range a.capacityKeys {
		updateStoreList(key)
	}
	return sl
}
