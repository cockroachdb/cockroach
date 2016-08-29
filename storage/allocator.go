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

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/config"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
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
	required         []config.Constraint
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
	if ae.relaxConstraints || len(ae.required) == 0 {
		auxInfo = "; likely not enough nodes in cluster"
	}
	return fmt.Sprintf("0 of %d store%s with %s matching %s%s",
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
}

// Allocator tries to spread replicas as evenly as possible across the stores
// in the cluster.
type Allocator struct {
	storePool  *StorePool
	options    AllocatorOptions
	ruleSolver *RuleSolver
}

// MakeAllocator creates a new allocator using the specified StorePool.
func MakeAllocator(storePool *StorePool, options AllocatorOptions) Allocator {
	return Allocator{
		storePool:  storePool,
		options:    options,
		ruleSolver: MakeDefaultRuleSolver(storePool),
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
	need := int(zone.NumReplicas)
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
// out as targets.
func (a *Allocator) AllocateTarget(
	constraints config.Constraints,
	existing []roachpb.ReplicaDescriptor,
) (*roachpb.StoreDescriptor, error) {
	candidates, err := a.ruleSolver.Solve(constraints, existing)
	if err != nil {
		return nil, err
	}

	if len(candidates) == 0 {
		return nil, &allocatorError{
			required: constraints.Constraints,
		}
	}
	return &candidates[0].Store, nil
}

// RemoveTarget returns a suitable replica to remove from the provided replica
// set. It attempts to consider which of the provided replicas would be the best
// candidate for removal. It also will exclude any replica that belongs to the
// range lease holder's store ID.
func (a Allocator) RemoveTarget(
	constraints config.Constraints,
	existing []roachpb.ReplicaDescriptor,
	leaseStoreID roachpb.StoreID,
) (roachpb.ReplicaDescriptor, error) {
	if len(existing) == 0 {
		return roachpb.ReplicaDescriptor{}, errors.Errorf("must supply at least one replica to allocator.RemoveTarget()")
	}

	sl, _, _ := a.storePool.getStoreList()

	found := false
	var worst roachpb.ReplicaDescriptor
	var worstScore float64
	for _, exist := range existing {
		if exist.StoreID == leaseStoreID {
			continue
		}
		desc, ok := a.storePool.getStoreDescriptor(exist.StoreID)
		if !ok {
			continue
		}
		// If it's not a valid candidate, score will be zero.
		candidate, _ := a.ruleSolver.computeCandidate(solveState{
			constraints: constraints,
			store:       desc,
			existing:    nil,
			sl:          sl,
			tierOrder:   canonicalTierOrder(sl),
			tiers:       storeTierMap(sl),
		})
		if !found || candidate.Score < worstScore {
			worstScore = candidate.Score
			worst = exist
			found = true
		}
	}

	if found {
		return worst, nil
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
	constraints config.Constraints,
	existing []roachpb.ReplicaDescriptor,
	leaseStoreID roachpb.StoreID,
) (*roachpb.StoreDescriptor, error) {
	if !a.options.AllowRebalance {
		return nil, nil
	}

	sl, _, _ := a.storePool.getStoreList()
	if log.V(3) {
		log.Infof(context.TODO(), "rebalance-target (lease-holder=%d):\n%s", leaseStoreID, sl)
	}

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
		return nil, nil
	}

	// Get candidate stores.
	candidates, err := a.ruleSolver.Solve(constraints, nil)
	if err != nil {
		return nil, err
	}

	// Find a candidate that is better than one of the existing stores, otherwise
	// return nil.
	candidatesFound := 0
	for _, candidate := range candidates {
		store := candidate.Store
		found := false
		for _, repl := range existing {
			if repl.StoreID == store.StoreID {
				found = true
				break
			}
		}
		if !found {
			return &store, nil
		}
		candidatesFound++
		if candidatesFound > len(existing) {
			break
		}
	}
	return nil, nil
}

// shouldRebalance returns whether the specified store is a candidate for
// having a replica removed from it given the candidate store list.
func (a Allocator) shouldRebalance(
	store roachpb.StoreDescriptor, sl StoreList,
) bool {
	const replicaInbalanceTolerance = 1

	// Moving a replica from the given store makes its range count converge on
	// the mean range count.
	//
	// TODO(peter,bram,cuong): The FractionUsed check seems suspicious. When a
	// node becomes fuller than maxFractionUsedThreshold we will always select it
	// for rebalancing. This is currently utilized by tests.
	shouldRebalance := store.Capacity.FractionUsed() >= maxFractionUsedThreshold || (float64(store.Capacity.RangeCount)-sl.candidateCount.mean) >= replicaInbalanceTolerance

	if log.V(2) {
		log.Infof(context.TODO(),
			"%d: should-rebalance=%t: fraction-used=%.2f range-count=%d (mean=%.1f)",
			store.StoreID, shouldRebalance, store.Capacity.FractionUsed(),
			store.Capacity.RangeCount, sl.candidateCount.mean)
	}
	return shouldRebalance
}

// computeQuorum computes the quorum value for the given number of nodes.
func computeQuorum(nodes int) int {
	return (nodes / 2) + 1
}
