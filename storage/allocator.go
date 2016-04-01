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
// Author: Spencer Kimball (spencer.kimball@gmail.com)
// Author: Kathy Spradlin (kathyspradlin@gmail.com)
// Author: Matt Tracy (matt@cockroachlabs.com)

package storage

import (
	"fmt"
	"math/rand"
	"sync"

	"github.com/cockroachdb/cockroach/config"
	"github.com/cockroachdb/cockroach/roachpb"
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

func pluralize(n int) string {
	if n == 1 {
		return ""
	}
	return "s"
}

// allocatorError indicates a retryable error condition which sends replicas
// being processed through the replicate_queue into purgatory so that they
// can be retried quickly as soon as new stores come online, or additional
// space frees up.
type allocatorError struct {
	required         roachpb.Attributes
	relaxConstraints bool
	aliveStoreCount  int
}

func (ae *allocatorError) Error() string {
	anyAll := "all attributes"
	if ae.relaxConstraints {
		anyAll = "an attribute"
	}
	var auxInfo string
	// Whenever the likely problem is not having enough nodes up, make the
	// message really clear.
	if ae.relaxConstraints || len(ae.required.Attrs) == 0 {
		auxInfo = "; likely not enough nodes in cluster"
	}
	return fmt.Sprintf("0 of %d store%s with %s matching [%s]%s",
		ae.aliveStoreCount, pluralize(ae.aliveStoreCount), anyAll, ae.required, auxInfo)
}

func (*allocatorError) purgatoryErrorMarker() {}

var _ purgatoryError = &allocatorError{}

// allocatorRand pairs a rand.Rand with a mutex.
// TODO: Allocator is typically only accessed from a single thread (the
// replication queue), but this assumption is broken in tests which force
// replication scans. If those tests can be modified to suspend the normal
// replication queue during the forced scan, then this rand could be used
// without a mutex.
type allocatorRand struct {
	*sync.Mutex
	*rand.Rand
}

func makeAllocatorRand(source rand.Source) allocatorRand {
	return allocatorRand{
		Mutex: &sync.Mutex{},
		Rand:  rand.New(source),
	}
}

// AllocatorOptions are configurable options which effect the way that the
// replicate queue will handle rebalancing opportunities.
type AllocatorOptions struct {
	// AllowRebalance allows this store to attempt to rebalance its own
	// replicas to other stores.
	AllowRebalance bool

	// Deterministic makes allocation decisions deterministic, based on
	// current cluster statistics. If this flag is not set, allocation operations
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
	randGen   allocatorRand
	options   AllocatorOptions
}

// MakeAllocator creates a new allocator using the specified StorePool.
func MakeAllocator(storePool *StorePool, options AllocatorOptions) Allocator {
	var randSource rand.Source
	if options.Deterministic {
		randSource = rand.NewSource(777)
	} else {
		randSource = rand.NewSource(rand.Int63())
	}
	return Allocator{
		storePool: storePool,
		options:   options,
		randGen:   makeAllocatorRand(randSource),
	}
}

// ComputeAction determines the exact operation needed to repair the supplied
// range, as governed by the supplied zone configuration. It returns the
// required action that should be taken and a replica on which the action should
// be performed.
func (a *Allocator) ComputeAction(zone config.ZoneConfig, desc *roachpb.RangeDescriptor) (
	AllocatorAction, float64) {
	if a.storePool == nil {
		// Do nothing if storePool is nil for some unittests.
		return AllocatorNoop, 0
	}

	deadReplicas := a.storePool.deadReplicas(desc.Replicas)
	if len(deadReplicas) > 0 {
		// The range has dead replicas, which should be removed immediately.
		// Adjust the priority by the number of dead replicas the range has.
		quorum := computeQuorum(len(desc.Replicas))
		liveReplicas := len(desc.Replicas) - len(deadReplicas)
		return AllocatorRemoveDead, removeDeadReplicaPriority + float64(quorum-liveReplicas)
	}

	// TODO(mrtracy): Handle non-homogeneous and mismatched attribute sets.
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
func (a *Allocator) AllocateTarget(required roachpb.Attributes, existing []roachpb.ReplicaDescriptor, relaxConstraints bool,
	filter func(storeDesc *roachpb.StoreDescriptor, count, used *stat) bool) (*roachpb.StoreDescriptor, error) {
	existingNodes := make(nodeIDSet, len(existing))
	for _, repl := range existing {
		existingNodes[repl.NodeID] = struct{}{}
	}

	// Because more redundancy is better than less, if relaxConstraints, the
	// matching here is lenient, and tries to find a target by relaxing an
	// attribute constraint, from last attribute to first.
	for attrs := append([]string(nil), required.Attrs...); ; attrs = attrs[:len(attrs)-1] {
		sl, aliveStoreCount := a.storePool.getStoreList(roachpb.Attributes{Attrs: attrs}, a.options.Deterministic)
		if target := a.selectGood(sl, existingNodes); target != nil {
			return target, nil
		}
		if len(attrs) == 0 || !relaxConstraints {
			return nil, &allocatorError{
				required:         required,
				relaxConstraints: relaxConstraints,
				aliveStoreCount:  aliveStoreCount,
			}
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
func (a Allocator) RemoveTarget(existing []roachpb.ReplicaDescriptor) (roachpb.ReplicaDescriptor, error) {
	if len(existing) == 0 {
		return roachpb.ReplicaDescriptor{}, util.Errorf("must supply at least one replica to allocator.RemoveTarget()")
	}

	// Retrieve store descriptors for the provided replicas from the StorePool.
	sl := StoreList{}
	for i := range existing {
		desc := a.storePool.getStoreDescriptor(existing[i].StoreID)
		if desc == nil {
			continue
		}
		sl.add(desc)
	}

	if bad := a.selectBad(sl); bad != nil {
		for i := range existing {
			if existing[i].StoreID == bad.StoreID {
				return existing[i], nil
			}
		}
	}
	return roachpb.ReplicaDescriptor{}, util.Errorf("RemoveTarget() could not select an appropriate replica to be remove")
}

// RebalanceTarget returns a suitable store for a rebalance target
// with required attributes. Rebalance targets are selected via the
// same mechanism as AllocateTarget(), except the chosen target must
// follow some additional criteria. Namely, if chosen, it must further
// the goal of balancing the cluster.
//
// The supplied parameters are the StoreID of the replica being rebalanced, the
// required attributes for the replica being rebalanced, and a list of the
// existing replicas of the range (which must include the replica being
// rebalanced).
//
// Simply ignoring a rebalance opportunity in the event that the
// target chosen by AllocateTarget() doesn't fit balancing criteria
// is perfectly fine, as other stores in the cluster will also be
// doing their probabilistic best to rebalance. This helps prevent
// a stampeding herd targeting an abnormally under-utilized store.
func (a Allocator) RebalanceTarget(
	storeID roachpb.StoreID, required roachpb.Attributes, existing []roachpb.ReplicaDescriptor,
) *roachpb.StoreDescriptor {
	if !a.options.AllowRebalance {
		return nil
	}
	existingNodes := make(nodeIDSet, len(existing))
	for _, repl := range existing {
		existingNodes[repl.NodeID] = struct{}{}
	}
	storeDesc := a.storePool.getStoreDescriptor(storeID)
	sl, _ := a.storePool.getStoreList(required, a.options.Deterministic)
	if replacement := a.improve(storeDesc, sl, existingNodes); replacement != nil {
		return replacement
	}
	return nil
}

// ShouldRebalance returns whether the specified store should attempt to
// rebalance a replica to another store.
func (a Allocator) ShouldRebalance(storeID roachpb.StoreID) bool {
	if !a.options.AllowRebalance {
		return false
	}
	if log.V(2) {
		log.Infof("ShouldRebalance from store %d", storeID)
	}
	storeDesc := a.storePool.getStoreDescriptor(storeID)
	if storeDesc == nil {
		if log.V(2) {
			log.Warningf(
				"ShouldRebalance couldn't find store with id %d in StorePool",
				storeID)
		}
		return false
	}

	sl, _ := a.storePool.getStoreList(*storeDesc.CombinedAttrs(), a.options.Deterministic)

	// ShouldRebalance is true if a suitable replacement can be found.
	return a.improve(storeDesc, sl, makeNodeIDSet(storeDesc.Node.NodeID)) != nil
}

// selectGood attempts to select a store from the supplied store list that it
// considers to be 'Good' relative to the other stores in the list. Any nodes
// in the supplied 'exclude' list will be disqualified from selection. Returns
// the selected store or nil if no such store can be found.
func (a Allocator) selectGood(sl StoreList, excluded nodeIDSet) *roachpb.StoreDescriptor {
	if sl.used.mean < minFractionUsedThreshold {
		rcb := rangeCountBalancer{a.randGen}
		return rcb.selectGood(sl, excluded)
	}
	ucb := usedCapacityBalancer{a.randGen}
	return ucb.selectGood(sl, excluded)
}

// selectBad attempts to select a store from the supplied store list that it
// considers to be 'Bad' relative to the other stores in the list. Returns the
// selected store or nil if no such store can be found.
func (a Allocator) selectBad(sl StoreList) *roachpb.StoreDescriptor {
	if sl.used.mean < minFractionUsedThreshold {
		rcb := rangeCountBalancer{a.randGen}
		return rcb.selectBad(sl)
	}
	ucb := usedCapacityBalancer{a.randGen}
	return ucb.selectBad(sl)
}

// improve attempts to select an improvement over the given store from the
// stores in the given store list. Any nodes in the supplied 'exclude' list
// will be disqualified from selection. Returns the selected store, or nil if
// no such store can be found.
func (a Allocator) improve(store *roachpb.StoreDescriptor, sl StoreList,
	excluded nodeIDSet) *roachpb.StoreDescriptor {
	if sl.used.mean < minFractionUsedThreshold {
		rcb := rangeCountBalancer{a.randGen}
		return rcb.improve(store, sl, excluded)
	}
	ucb := usedCapacityBalancer{a.randGen}
	return ucb.improve(store, sl, excluded)
}

// computeQuorum computes the quorum value for the given number of nodes.
func computeQuorum(nodes int) int {
	return (nodes / 2) + 1
}
