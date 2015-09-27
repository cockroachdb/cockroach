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
// Author: Matt Tracy (matt@cockroachlabs.com)

package storage

import (
	"math"
	"math/rand"

	"github.com/cockroachdb/cockroach/config"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
)

const (
	// maxFractionUsedThreshold: if the fraction used of a store descriptor
	// capacity is greater than this value, it will never be used as a rebalance
	// target and it will always be eligible to rebalance replicas to other
	// stores.
	maxFractionUsedThreshold = 0.95
	// minFractionUsedThreshold: if the mean fraction used of a list of store
	// descriptors is less than this, then range count will be used to make
	// rebalancing decisions instead of the fraction of bytes used. This is
	// useful for distributing load evenly on nascent deployments.
	minFractionUsedThreshold = 0.02
	// rebalanceFromMean is used to declare a range above and below the average
	// used capacity of the cluster. If a store's usage is below this range, it
	// is a rebalancing target and can accept new replicas; if usage is above
	// this range, the store is eligible to rebalance replicas to other stores.
	rebalanceFromMean = 0.025 // 2.5%
	// rebalanceShouldRebalanceChance represents a chance that an individual
	// replica should attempt to rebalance. This helps introduce some
	// probabilistic "jitter" to shouldRebalance() function: the store will not
	// take every rebalancing opportunity available.
	rebalanceShouldRebalanceChance = 0.05

	// priorities for various repair operations.
	removeDeadReplicaPriority  float64 = 10000
	addMissingReplicaPriority  float64 = 1000
	removeExtraReplicaPriority float64 = 100
)

// AllocatorAction enumerates the various replication adjustments that may be
// recommended by the allocator.
type AllocatorAction int

// These are the possible allocator actions.
const (
	_ AllocatorAction = iota
	AllocatorNoop
	AllocatorRemove
	AllocatorAdd
	AllocatorRemoveDead
)

// RebalancingOptions are configurable options which effect the way that the
// replicate queue will handle rebalancing opportunities.
type RebalancingOptions struct {
	// AllowRebalance allows this store to attempt to rebalance its own
	// replicas to other stores.
	AllowRebalance bool

	// Deterministic makes rebalance decisions deterministic, based on
	// current cluster statistics. If this flag is not set, rebalance operations
	// will have random behavior. This flag is intended to be set for testing
	// purposes only.
	Deterministic bool
}

// Allocator makes allocation decisions based on available capacity
// in other stores which match the required attributes for a desired
// range replica.
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
type Allocator struct {
	storePool *StorePool
	randGen   *rand.Rand
	options   RebalancingOptions
}

// MakeAllocator creates a new allocator using the specified StorePool.
func MakeAllocator(storePool *StorePool, options RebalancingOptions) Allocator {
	return Allocator{
		storePool: storePool,
		randGen:   rand.New(rand.NewSource(rand.Int63())),
		options:   options,
	}
}

// getUsedNodes returns a set of node IDs which are already being used
// to store replicas.
func getUsedNodes(existing []proto.ReplicaDescriptor) map[proto.NodeID]struct{} {
	usedNodes := map[proto.NodeID]struct{}{}
	for _, replica := range existing {
		usedNodes[replica.NodeID] = struct{}{}
	}
	return usedNodes
}

// ComputeAction determines the exact operation needed to repair the supplied
// range, as governed by the supplied zone configuration. It returns the
// required action that should be taken and a replica on which the action should
// be performed.
func (a *Allocator) ComputeAction(zone config.ZoneConfig, desc *proto.RangeDescriptor) (
	AllocatorAction, float64) {
	deadReplicas := a.storePool.deadReplicas(desc.Replicas)
	if len(deadReplicas) > 0 {
		// The range has dead replicas, which should be removed immediately.
		// Adjust the priority by the number of dead replicas the range has.
		quorum := computeQuorum(len(desc.Replicas))
		liveReplicas := len(desc.Replicas) - len(deadReplicas)
		return AllocatorRemoveDead, removeDeadReplicaPriority + float64(quorum-liveReplicas)
	}

	// TODO(mrtracy): Handle non-homogenous and mismatched attribute sets.
	need := len(zone.ReplicaAttrs)
	have := len(desc.Replicas)
	if have < need {
		// Range is under-replicated, and should add an additional replica.
		// Priority is adjusted by the difference between the current replica
		// count and the quorum of the desired replica count.
		neededQuorum := computeQuorum(need)
		return AllocatorAdd, addMissingReplicaPriority + float64(neededQuorum-have)
	}
	if have > need {
		// Range is over-replicated, and should remove a replica.
		// Ranges with an even number of replicas get extra priority because
		// they have a more fragile quorum.
		return AllocatorRemove, removeExtraReplicaPriority - float64(have%2)
	}

	// Nothing to do.
	return AllocatorNoop, 0
}

// AllocateTarget returns a suitable store for a new allocation with the
// required attributes. Nodes already accommodating existing replicas are ruled
// out as targets. If relaxConstraints is true, then the required attributes
// will be relaxed as necessary, from least specific to most specific, in order
// to allocate a target. If needed, a filter function can be added that further
// filter the results. The function will be passed the storeDesc and the used
// and new counts. It returns a bool indicating inclusion or exclusion from the
// set of stores being considered.
func (a *Allocator) AllocateTarget(required proto.Attributes, existing []proto.ReplicaDescriptor, relaxConstraints bool,
	filter func(storeDesc *proto.StoreDescriptor, count, used *stat) bool) (*proto.StoreDescriptor, error) {
	// Because more redundancy is better than less, if relaxConstraints, the
	// matching here is lenient, and tries to find a target by relaxing an
	// attribute constraint, from last attribute to first.
	for attrs := append([]string(nil), required.Attrs...); ; attrs = attrs[:len(attrs)-1] {
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
		if len(attrs) == 0 {
			return nil, util.Errorf("unable to allocate a target store; no candidates available")
		} else if !relaxConstraints {
			return nil, util.Errorf("unable to allocate a target store; no candidates available with attributes %s", required)
		}
	}
}

// RemoveTarget returns a suitable replica to remove from the provided replica
// set. It attempts to consider which of the provided replicas would be the best
// candidate for removal.
//
// TODO(mrtracy): removeTarget eventually needs to accept the attributes from
// the zone config associated with the provided replicas. This will allow it to
// make correct decisions in the case of ranges with heterogeneous replica
// requirements (i.e. multiple data centers).
func (a Allocator) RemoveTarget(existing []proto.ReplicaDescriptor) (proto.ReplicaDescriptor, error) {
	if len(existing) == 0 {
		return proto.ReplicaDescriptor{}, util.Errorf("must supply at least one replica to allocator.RemoveTarget()")
	}

	// Retrieve store descriptors for the provided replicas from the StorePool.
	type replStore struct {
		repl  proto.ReplicaDescriptor
		store *proto.StoreDescriptor
	}
	replStores := make([]replStore, len(existing))
	usedStat := stat{}
	for i := range existing {
		desc := a.storePool.getStoreDescriptor(existing[i].StoreID)
		if desc == nil {
			continue
		}
		replStores[i] = replStore{
			repl:  existing[i],
			store: desc,
		}
		usedStat.update(desc.Capacity.FractionUsed())
	}

	// Based on store statistics, determine which replica is the "worst" and
	// thus should be removed.
	var worst replStore
	for i, rs := range replStores {
		if i == 0 {
			worst = rs
			continue
		}

		if usedStat.mean < minFractionUsedThreshold {
			if rs.store.Capacity.RangeCount > worst.store.Capacity.RangeCount {
				worst = rs
			}
			continue
		}
		if rs.store.Capacity.FractionUsed() > worst.store.Capacity.FractionUsed() {
			worst = rs
		}
	}
	return worst.repl, nil
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
func (a Allocator) RebalanceTarget(required proto.Attributes, existing []proto.ReplicaDescriptor) *proto.StoreDescriptor {
	filter := func(s *proto.StoreDescriptor, count, used *stat) bool {
		// In clusters with very low disk usage, a store is eligible to be a
		// rebalancing target if the number of ranges on that store is below
		// average. This is primarily useful for distributing load evenly in a
		// nascent deployment.
		if used.mean < minFractionUsedThreshold {
			return float64(s.Capacity.RangeCount) < count.mean
		}
		// A store is eligible to be a rebalancing target if its disk usage is
		// sufficiently below the mean usage for stores with matching
		// attributes.
		maxFractionUsed := used.mean * (1 - rebalanceFromMean)
		if maxFractionUsedThreshold < maxFractionUsed {
			// In clusters with very high average usage, rebalancing is clamped
			// at maxFractionUsedThreshold: even if a store's usage is below
			// average, it will not be a rebalancing target if usage is above
			// this maximum threshold.
			maxFractionUsed = maxFractionUsedThreshold
		}
		return s.Capacity.FractionUsed() < maxFractionUsed
	}
	if !a.options.AllowRebalance {
		return nil
	}
	// Note that relaxConstraints is false; on a rebalance, there is
	// no sense in relaxing constraints; wait until a better option
	// is available.
	s, err := a.AllocateTarget(required, existing, false /* relaxConstraints */, filter)
	if err != nil {
		return nil
	}
	return s
}

// ShouldRebalance returns whether the specified store should attempt to
// rebalance a replica to another store.
func (a Allocator) ShouldRebalance(storeID proto.StoreID) bool {
	if !a.options.AllowRebalance {
		return false
	}
	// In production, add some random jitter to shouldRebalance.
	if !a.options.Deterministic && a.randGen.Float32() > rebalanceShouldRebalanceChance {
		return false
	}
	if log.V(2) {
		log.Infof("Attempting to rebalance from store %d", storeID)
	}
	storeDesc := a.storePool.getStoreDescriptor(storeID)
	if storeDesc == nil {
		if log.V(2) {
			log.Warningf(
				"shouldRebalance couldn't find store with id %d in StorePool",
				storeID)
		}
		return false
	}

	sl := a.storePool.getStoreList(*storeDesc.CombinedAttrs(), a.options.Deterministic)

	// In clusters with very low disk usage, a store is eligible for rebalancing
	// if the number of ranges on the store is above average. This is primarily
	// useful for distributing load in a nascent deployment.
	if sl.used.mean < minFractionUsedThreshold {
		if log.V(2) {
			log.Infof("Attempting to rebalance using range counts, count = %d, mean = %f", storeDesc.Capacity.RangeCount, sl.count.mean)
		}
		return float64(storeDesc.Capacity.RangeCount) > math.Ceil(sl.count.mean)
	}
	// A store is eligible for rebalancing if its disk usage is sufficiently above
	// the mean usage for stores with matching attributes.
	minFractionUsed := sl.used.mean * (1 + rebalanceFromMean)
	if maxFractionUsedThreshold < minFractionUsed {
		// In clusters with very high usage, we will allow replicas to seek
		// rebalancing opportunities even if they are below the cluster's average
		// usage.
		minFractionUsed = maxFractionUsedThreshold
	}
	if log.V(2) {
		log.Infof("Attempting to rebalance using total fraction used, threshold = %f, used = %f", minFractionUsed, storeDesc.Capacity.FractionUsed())
	}
	return storeDesc.Capacity.FractionUsed() > minFractionUsed
}

// selectRandom chooses count random store descriptors which match the
// required attributes and do not include any of the existing
// replicas. If the supplied filter is nil, it is ignored. Returns the
// list of matching descriptors, and the store list matching the
// required attributes.
func (a Allocator) selectRandom(count int, required proto.Attributes, existing []proto.ReplicaDescriptor) ([]*proto.StoreDescriptor, *StoreList) {
	var descs []*proto.StoreDescriptor
	sl := a.storePool.getStoreList(required, a.options.Deterministic)
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

// computeQuorum computes the quorum value for the given number of nodes.
func computeQuorum(nodes int) int {
	return (nodes / 2) + 1
}
