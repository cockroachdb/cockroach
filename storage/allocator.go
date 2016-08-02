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

	"github.com/cockroachdb/cockroach/config"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/syncutil"
	"github.com/pkg/errors"
)

const (
	// maxFractionUsedThreshold: if the fraction used of a store descriptor
	// capacity is greater than this value, it will never be used as a rebalance
	// target and it will always be eligible to rebalance replicas to other
	// stores.
	maxFractionUsedThreshold = 0.95

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
		ae.aliveStoreCount, util.Pluralize(int64(ae.aliveStoreCount)),
		anyAll, ae.required, auxInfo)
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
	*syncutil.Mutex
	*rand.Rand
}

func makeAllocatorRand(source rand.Source) allocatorRand {
	return allocatorRand{
		Mutex: &syncutil.Mutex{},
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

// Allocator tries to spread replicas as evenly as possible across the stores
// in the cluster.
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

	deadReplicas := a.storePool.deadReplicas(desc.RangeID, desc.Replicas)
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
// to allocate a target.
func (a *Allocator) AllocateTarget(
	required roachpb.Attributes,
	existing []roachpb.ReplicaDescriptor,
	relaxConstraints bool,
) (*roachpb.StoreDescriptor, error) {
	existingNodes := make(nodeIDSet, len(existing))
	for _, repl := range existing {
		existingNodes[repl.NodeID] = struct{}{}
	}

	// Because more redundancy is better than less, if relaxConstraints, the
	// matching here is lenient, and tries to find a target by relaxing an
	// attribute constraint, from last attribute to first.
	for attrs := append([]string(nil), required.Attrs...); ; attrs = attrs[:len(attrs)-1] {
		sl, aliveStoreCount, throttledStoreCount := a.storePool.getStoreList(
			roachpb.Attributes{Attrs: attrs},
			a.options.Deterministic,
		)
		if target := a.selectGood(sl, existingNodes); target != nil {
			return target, nil
		}

		// When there are throttled stores that do match, we shouldn't send
		// the replica to purgatory or even consider relaxing the constraints.
		if throttledStoreCount > 0 {
			return nil, errors.Errorf("%d matching stores are currently throttled", throttledStoreCount)
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
// candidate for removal. It also will exclude any replica that belongs to the
// range lease holder's store ID.
//
// TODO(mrtracy): removeTarget eventually needs to accept the attributes from
// the zone config associated with the provided replicas. This will allow it to
// make correct decisions in the case of ranges with heterogeneous replica
// requirements (i.e. multiple data centers).
func (a Allocator) RemoveTarget(existing []roachpb.ReplicaDescriptor, leaseStoreID roachpb.StoreID) (roachpb.ReplicaDescriptor, error) {
	if len(existing) == 0 {
		return roachpb.ReplicaDescriptor{}, errors.Errorf("must supply at least one replica to allocator.RemoveTarget()")
	}

	// Retrieve store descriptors for the provided replicas from the StorePool.
	sl := StoreList{}
	for _, exist := range existing {
		if exist.StoreID == leaseStoreID {
			continue
		}
		desc, ok := a.storePool.getStoreDescriptor(exist.StoreID)
		if !ok {
			continue
		}
		sl.add(desc)
	}

	if bad := a.selectBad(sl); bad != nil {
		for _, exist := range existing {
			if exist.StoreID == bad.StoreID {
				return exist, nil
			}
		}
	}
	return roachpb.ReplicaDescriptor{}, errors.Errorf("RemoveTarget() could not select an appropriate replica to be remove")
}

// RebalanceTarget returns a suitable store for a rebalance target with
// required attributes. Rebalance targets are selected via the same mechanism
// as AllocateTarget(), except the chosen target must follow some additional
// criteria. Namely, if chosen, it must further the goal of balancing the
// cluster.
//
// The supplied parameters are the required attributes for the range, a list of
// the existing replicas of the range and the store ID of the lease-holder
// replica. The existing replicas modulo the lease-holder replica are
// candidates for rebalancing. Note that rebalancing is accomplished by first
// adding a new replica to the range, then removing the most undesirable
// replica.
//
// Simply ignoring a rebalance opportunity in the event that the target chosen
// by AllocateTarget() doesn't fit balancing criteria is perfectly fine, as
// other stores in the cluster will also be doing their probabilistic best to
// rebalance. This helps prevent a stampeding herd targeting an abnormally
// under-utilized store.
func (a Allocator) RebalanceTarget(
	required roachpb.Attributes,
	existing []roachpb.ReplicaDescriptor,
	leaseStoreID roachpb.StoreID,
) *roachpb.StoreDescriptor {
	if !a.options.AllowRebalance {
		return nil
	}

	sl, _, _ := a.storePool.getStoreList(required, a.options.Deterministic)

	var shouldRebalance bool
	for _, repl := range existing {
		if leaseStoreID == repl.StoreID {
			continue
		}
		storeDesc, ok := a.storePool.getStoreDescriptor(repl.StoreID)
		if ok && a.shouldRebalance(storeDesc, sl) {
			shouldRebalance = true
			break
		}
	}
	if !shouldRebalance {
		return nil
	}

	existingNodes := make(nodeIDSet, len(existing))
	for _, repl := range existing {
		existingNodes[repl.NodeID] = struct{}{}
	}
	return a.improve(sl, existingNodes)
}

// ShouldRebalance returns whether the specified store should attempt to
// rebalance a replica to another store.
//
// TODO(bram): This is only used by the simulator and should be removed.
func (a *Allocator) ShouldRebalance(storeID roachpb.StoreID) bool {
	if !a.options.AllowRebalance {
		return false
	}
	desc, ok := a.storePool.getStoreDescriptor(storeID)
	if !ok {
		return false
	}
	sl, _, _ := a.storePool.getStoreList(roachpb.Attributes{}, true)
	return a.shouldRebalance(desc, sl)
}

// selectGood attempts to select a store from the supplied store list that it
// considers to be 'Good' relative to the other stores in the list. Any nodes
// in the supplied 'exclude' list will be disqualified from selection. Returns
// the selected store or nil if no such store can be found.
func (a Allocator) selectGood(sl StoreList, excluded nodeIDSet) *roachpb.StoreDescriptor {
	rcb := rangeCountBalancer{a.randGen}
	return rcb.selectGood(sl, excluded)
}

// selectBad attempts to select a store from the supplied store list that it
// considers to be 'Bad' relative to the other stores in the list. Returns the
// selected store or nil if no such store can be found.
func (a Allocator) selectBad(sl StoreList) *roachpb.StoreDescriptor {
	rcb := rangeCountBalancer{a.randGen}
	return rcb.selectBad(sl)
}

// improve attempts to select an improvement over the given store from the
// stores in the given store list. Any nodes in the supplied 'exclude' list
// will be disqualified from selection. Returns the selected store, or nil if
// no such store can be found.
func (a Allocator) improve(
	sl StoreList, excluded nodeIDSet,
) *roachpb.StoreDescriptor {
	rcb := rangeCountBalancer{a.randGen}
	return rcb.improve(sl, excluded)
}

// shouldRebalance returns whether the specified store is a candidate for
// having a replica removed from it given the candidate store list.
func (a Allocator) shouldRebalance(
	store roachpb.StoreDescriptor, sl StoreList,
) bool {
	rcb := rangeCountBalancer{a.randGen}
	return rcb.shouldRebalance(store, sl)
}

// computeQuorum computes the quorum value for the given number of nodes.
func computeQuorum(nodes int) int {
	return (nodes / 2) + 1
}
