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
	"math"
	"math/rand"
	"sort"
	"sync"

	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/util"
)

const (
	// maxFractionUsedThreshold: if the fraction used of a store
	// descriptor capacity is greater than this, it will not be used as
	// a rebalance target.
	maxFractionUsedThreshold = 0.95
	// minFractionUsedThreshold: if the mean fraction used of a list of
	// store descriptors is less than this, then range count will be used
	// to make rebalancing decisions instead of fraction of bytes used.
	minFractionUsedThreshold = 0.02
	// rebalanceFromMean: if the fraction of bytes used of a store
	// is within rebalanceFromMean of the mean, it is considered a
	// viable target to rebalance to.
	rebalanceFromMean = 0.025 // 2.5%
)

// stat provides a running sample size and mean.
type stat struct {
	n, mean float64
}

// Update adds the specified value to the stat, augmenting the sample
// size & mean.
func (s *stat) Update(x float64) {
	s.n++
	s.mean += (x - s.mean) / s.n
}

// storeList keeps a list of store descriptors and associated count
// and used stats across the stores.
type storeList struct {
	stores      []*proto.StoreDescriptor
	count, used stat
}

// Add adds the store descriptor to the list of stores and updates
// maintained statistics.
func (sl *storeList) Add(s *proto.StoreDescriptor) {
	sl.stores = append(sl.stores, s)
	sl.count.Update(float64(s.Capacity.RangeCount))
	sl.used.Update(s.Capacity.FractionUsed())
}

// allocator makes allocation decisions based on available capacity
// in other stores which match the required attributes for a desired
// range replica.
//
// The allocator listens for gossip updates from stores and updates
// statistics for mean of fraction of bytes used and total range count.
//
// When choosing a new allocation target, three candidates from
// available stores meeting a max fraction of bytes used threshold
// (maxFractionUsedThreshold) are chosen at random and the least
// loaded of the three is selected in order to bias loading towards a
// more balanced cluster, while still spreading load over all
// available servers. "Load" is defined according to fraction of bytes
// used, if greater than minFractionUsedThreshold; otherwise it's
// defined according to range count.
//
// When choosing a rebalance target, a random store is selected from
// amongst the set of stores with fraction of bytes within
// rebalanceFromMean from the mean.
type allocator struct {
	sync.Mutex
	gossip        *gossip.Gossip
	randGen       *rand.Rand
	deterministic bool                  // Set deterministic for unittests
	capacityKeys  map[string]struct{}   // Tracks gossip keys used for capacity
	storeLists    map[string]*storeList // Cache from attributes to storeList
}

// newAllocator creates a new allocator using the specified gossip.
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
func (a *allocator) capacityGossipUpdate(key string, _ bool) {
	a.Lock()
	defer a.Unlock()

	// Clear the cached store lists on new gossip.
	a.storeLists = nil
	if a.capacityKeys == nil {
		a.capacityKeys = map[string]struct{}{}
	}
	a.capacityKeys[key] = struct{}{}
}

// AllocateTarget returns a suitable store for a new allocation with
// the required attributes. Nodes already accommodating existing
// replicas are ruled out as targets. If relaxConstraints is true,
// then the required attributes will be relaxed as necessary, from
// least specific to most specific, in order to allocate a target.
func (a *allocator) AllocateTarget(required proto.Attributes, existing []proto.Replica,
	relaxConstraints bool) (*proto.StoreDescriptor, error) {
	a.Lock()
	defer a.Unlock()
	return a.allocateTargetInternal(required, existing, relaxConstraints, nil)
}

func (a *allocator) allocateTargetInternal(required proto.Attributes, existing []proto.Replica,
	relaxConstraints bool, filter func(*proto.StoreDescriptor, *stat, *stat) bool) (*proto.StoreDescriptor, error) {
	attrs := append([]string(nil), required.Attrs...)
	for {
		stores, sl := a.selectRandom(3, proto.Attributes{Attrs: attrs}, existing)

		// Choose the store with the least fraction of bytes used.
		var leastStore *proto.StoreDescriptor
		for _, s := range stores {
			// Filter store descriptor.
			if filter != nil && !filter(s, &sl.count, &sl.used) {
				continue
			}
			if leastStore == nil {
				leastStore = s
				continue
			}
			// Use counts instead of capacities if the cluster has mean
			// fraction used below a threshold level. This is primarily useful
			// for balancing load evenly in nascent deployments.
			if sl.used.mean < minFractionUsedThreshold {
				if s.Capacity.RangeCount < leastStore.Capacity.RangeCount {
					leastStore = s
				}
			} else if s.Capacity.FractionUsed() < leastStore.Capacity.FractionUsed() {
				leastStore = s
			}
		}
		if leastStore != nil {
			return leastStore, nil
		}

		// Otherwise, we have not found a store. Because more redundancy
		// is better than less, if relaxConstraints, we still try to find
		// a target by relaxing an attribute constraint, from last
		// attribute to first.
		if len(attrs) == 0 || !relaxConstraints {
			return nil, util.Errorf("unable to allocate a target store for %s", required)
		}
		attrs = attrs[:len(attrs)-1]
	}
}

// RebalanceTarget returns a suitable store for a rebalance target
// with required attributes. Rebalance targets are selected via the
// same mechanism as AllocateTarget(), except the chosen target must
// follow some additional criteria. Namely, if chosen, it must further
// the goal of balancing the cluster.
//
// Simply ignoring a rebalance opportunity in the event that the
// target chosen by AllocateTarget() doesn't fit balancing criteria
// is perfectly fine, as other stores in the cluster will also be
// doing their probabilistic best to rebalance. This helps prevent
// a stampeding herd targeting an abnormally under-utilized store.
func (a *allocator) RebalanceTarget(required proto.Attributes, existing []proto.Replica) *proto.StoreDescriptor {
	a.Lock()
	defer a.Unlock()
	filter := func(s *proto.StoreDescriptor, count, used *stat) bool {
		// Use counts instead of capacities if the cluster has mean
		// fraction used below a threshold level. This is primarily useful
		// for balancing load evenly in nascent deployments.
		if used.mean < minFractionUsedThreshold {
			return float64(s.Capacity.RangeCount) < count.mean
		}
		maxFractionUsed := used.mean * (1 - rebalanceFromMean)
		if maxFractionUsedThreshold < maxFractionUsed {
			maxFractionUsed = maxFractionUsedThreshold
		}
		return s.Capacity.FractionUsed() < maxFractionUsed
	}
	// Note that relaxConstraints is false; on a rebalance, there is
	// no sense in relaxing constraints; wait until a better option
	// is available.
	s, err := a.allocateTargetInternal(required, existing, false /* relaxConstraints */, filter)
	if err != nil {
		return nil
	}
	return s
}

// ShouldRebalance returns whether the specified store is overweight
// according to the cluster mean and should rebalance a range.
func (a *allocator) ShouldRebalance(s *proto.StoreDescriptor) bool {
	a.Lock()
	defer a.Unlock()
	sl := a.getStoreList(*s.CombinedAttrs())

	if sl.used.mean < minFractionUsedThreshold {
		return s.Capacity.RangeCount > int32(math.Ceil(sl.count.mean))
	}
	return s.Capacity.FractionUsed() > sl.used.mean
}

// selectRandom chooses count random store descriptors which match the
// required attributes and do not include any of the existing
// replicas. If the supplied filter is nil, it is ignored. Returns the
// list of matching descriptors, and the store list matching the
// required attributes.
func (a *allocator) selectRandom(count int, required proto.Attributes, existing []proto.Replica) (
	[]*proto.StoreDescriptor, *storeList) {
	var descs []*proto.StoreDescriptor
	sl := a.getStoreList(required)
	used := getUsedNodes(existing)

	// Randomly permute available stores matching the required attributes.
	for _, idx := range a.randGen.Perm(len(sl.stores)) {
		// Skip used nodes.
		if _, ok := used[sl.stores[idx].Node.NodeID]; ok {
			continue
		}
		// Add this store; exit loop if we've satisfied count.
		descs = append(descs, sl.stores[idx])
		if len(descs) >= count {
			break
		}
	}
	if len(descs) == 0 {
		return nil, nil
	}
	return descs, sl
}

// getStoreList returns a store list matching the required attributes.
// Results are cached for performance.
//
// If it cannot retrieve a StoreDescriptor from the Store's gossip, it
// garbage collects the failed key.
//
// TODO(embark, spencer): consider using a reverse index map from
// Attr->stores, for efficiency. Ensure that entries in this map still
// have an opportunity to be garbage collected.
func (a *allocator) getStoreList(required proto.Attributes) *storeList {
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
			sl.Add(storeDesc)
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
