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
	"math"
	"math/rand"

	"github.com/coreos/etcd/raft"
	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

const (
	// maxFractionUsedThreshold: if the fraction used of a store descriptor
	// capacity is greater than this value, it will never be used as a rebalance
	// target and it will always be eligible to rebalance replicas to other
	// stores.
	maxFractionUsedThreshold = 0.95

	// priorities for various repair operations.
	addMissingReplicaPriority  float64 = 10000
	removeDeadReplicaPriority  float64 = 1000
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

var allocatorActionNames = map[AllocatorAction]string{
	AllocatorNoop:       "noop",
	AllocatorRemove:     "remove",
	AllocatorAdd:        "add",
	AllocatorRemoveDead: "remove dead",
}

func (a AllocatorAction) String() string {
	return allocatorActionNames[a]
}

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

// Allocator tries to spread replicas as evenly as possible across the stores
// in the cluster.
type Allocator struct {
	storePool *StorePool
	randGen   allocatorRand
}

// MakeAllocator creates a new allocator using the specified StorePool.
func MakeAllocator(storePool *StorePool) Allocator {
	var randSource rand.Source
	// There are number of test cases that make a test store but don't add
	// gossip or a store pool. So we can't rely on the existence of the
	// store pool in those cases.
	if storePool != nil && storePool.deterministic {
		randSource = rand.NewSource(777)
	} else {
		randSource = rand.NewSource(rand.Int63())
	}
	return Allocator{
		storePool: storePool,
		randGen:   makeAllocatorRand(randSource),
	}
}

// ComputeAction determines the exact operation needed to repair the supplied
// range, as governed by the supplied zone configuration. It returns the
// required action that should be taken and a replica on which the action should
// be performed.
func (a *Allocator) ComputeAction(
	zone config.ZoneConfig, desc *roachpb.RangeDescriptor,
) (AllocatorAction, float64) {
	if a.storePool == nil {
		// Do nothing if storePool is nil for some unittests.
		return AllocatorNoop, 0
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
	deadReplicas := a.storePool.deadReplicas(desc.RangeID, desc.Replicas)
	if len(deadReplicas) > 0 {
		// The range has dead replicas, which should be removed immediately.
		// Adjust the priority by the number of dead replicas the range has.
		quorum := computeQuorum(len(desc.Replicas))
		liveReplicas := len(desc.Replicas) - len(deadReplicas)
		return AllocatorRemoveDead, removeDeadReplicaPriority + float64(quorum-liveReplicas)
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
// out as targets. The range ID of the replica being allocated for is also
// passed in to ensure that we don't try to replace an existing dead replica on
// a store. If relaxConstraints is true, then the required attributes will be
// relaxed as necessary, from least specific to most specific, in order to
// allocate a target.
func (a *Allocator) AllocateTarget(
	constraints config.Constraints,
	existing []roachpb.ReplicaDescriptor,
	rangeID roachpb.RangeID,
	relaxConstraints bool,
) (*roachpb.StoreDescriptor, error) {
	sl, _, throttledStoreCount := a.storePool.getStoreList(rangeID)

	candidates := allocateCandidates(
		sl,
		constraints,
		existing,
		a.storePool.getNodeLocalities(existing),
		a.storePool.deterministic,
	)
	if log.V(3) {
		log.Infof(context.TODO(), "allocate candidates: %s", candidates)
	}
	if target := candidates.selectGood(a.randGen); target != nil {
		return target, nil
	}

	// When there are throttled stores that do match, we shouldn't send
	// the replica to purgatory.
	if throttledStoreCount > 0 {
		return nil, errors.Errorf("%d matching stores are currently throttled", throttledStoreCount)
	}
	return nil, &allocatorError{
		required: constraints.Constraints,
	}
}

// RemoveTarget returns a suitable replica to remove from the provided replica
// set. It first attempts to randomly select a target from the set of stores
// that have greater than the average number of replicas. Failing that, it
// falls back to selecting a random target from any of the existing
// replicas. It also will exclude any replica that lives on leaseStoreID.
func (a Allocator) RemoveTarget(
	constraints config.Constraints,
	existing []roachpb.ReplicaDescriptor,
	leaseStoreID roachpb.StoreID,
) (roachpb.ReplicaDescriptor, error) {
	if len(existing) == 0 {
		return roachpb.ReplicaDescriptor{}, errors.Errorf("must supply at least one replica to allocator.RemoveTarget()")
	}

	// Retrieve store descriptors for the provided replicas from the StorePool.
	descriptors := make([]roachpb.StoreDescriptor, 0, len(existing))
	for _, exist := range existing {
		if desc, ok := a.storePool.getStoreDescriptor(exist.StoreID); ok {
			if exist.StoreID == leaseStoreID {
				continue
			}
			descriptors = append(descriptors, desc)
		}
	}
	sl := makeStoreList(descriptors)

	candidates := removeCandidates(
		sl,
		constraints,
		a.storePool.getNodeLocalities(existing),
		a.storePool.deterministic,
	)
	if log.V(3) {
		log.Infof(context.TODO(), "remove candidates: %s", candidates)
	}
	if bad := candidates.selectBad(a.randGen); bad != nil {
		for _, exist := range existing {
			if exist.StoreID == bad.StoreID {
				return exist, nil
			}
		}
	}

	return roachpb.ReplicaDescriptor{}, errors.New("could not select an appropriate replica to be removed")
}

// RebalanceTarget returns a suitable store for a rebalance target with
// required attributes. Rebalance targets are selected via the same mechanism
// as AllocateTarget(), except the chosen target must follow some additional
// criteria. Namely, if chosen, it must further the goal of balancing the
// cluster.
//
// The supplied parameters are the required attributes for the range, a list of
// the existing replicas of the range, the store ID of the lease-holder
// replica and the range ID of the replica being allocated.
//
// The existing replicas modulo the lease-holder replica and any store with
// dead replicas are candidates for rebalancing. Note that rebalancing is
// accomplished by first adding a new replica to the range, then removing the
// most undesirable replica.
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
	rangeID roachpb.RangeID,
) (*roachpb.StoreDescriptor, error) {
	sl, _, _ := a.storePool.getStoreList(rangeID)
	existingCandidates, candidates := rebalanceCandidates(
		sl,
		constraints,
		existing,
		a.storePool.getNodeLocalities(existing),
		a.storePool.deterministic,
	)

	if len(existingCandidates) == 0 {
		return nil, errors.Errorf(
			"all existing replicas' stores are not present in the store pool: %v\n%s", existing, sl)
	}

	if log.V(3) {
		log.Infof(context.TODO(), "existing replicas: %s", existingCandidates)
		log.Infof(context.TODO(), "candidates: %s", candidates)
	}

	// Find all candidates that are better than the worst existing replica.
	targets := candidates.betterThan(existingCandidates[len(existingCandidates)-1])
	return targets.selectGood(a.randGen), nil
}

// TransferLeaseTarget returns a suitable replica to transfer the range lease
// to from the provided list. It excludes the current lease holder replica.
func (a *Allocator) TransferLeaseTarget(
	constraints config.Constraints,
	existing []roachpb.ReplicaDescriptor,
	leaseStoreID roachpb.StoreID,
	rangeID roachpb.RangeID,
	checkTransferLeaseSource bool,
) roachpb.ReplicaDescriptor {
	sl, _, _ := a.storePool.getStoreList(rangeID)
	sl = sl.filter(constraints)

	// Filter stores that are on nodes containing existing replicas, but leave
	// the stores containing the existing replicas in place. This excludes stores
	// that we can't rebalance to, avoiding an issue in a 3-node cluster where
	// there are multiple stores per node.
	//
	// TODO(peter,bram): This will need adjustment with the new allocator. `sl`
	// needs to contain only the possible rebalance candidates + the existing
	// stores the replicas are on.
	filteredDescs := make([]roachpb.StoreDescriptor, 0, len(sl.stores))
	for _, s := range sl.stores {
		var exclude bool
		for _, r := range existing {
			if r.NodeID == s.Node.NodeID && r.StoreID != s.StoreID {
				exclude = true
				break
			}
		}
		if !exclude {
			filteredDescs = append(filteredDescs, s)
		}
	}
	sl = makeStoreList(filteredDescs)

	source, ok := a.storePool.getStoreDescriptor(leaseStoreID)
	if !ok {
		return roachpb.ReplicaDescriptor{}
	}
	if checkTransferLeaseSource && !a.shouldTransferLease(sl, source, existing) {
		return roachpb.ReplicaDescriptor{}
	}

	candidates := make([]roachpb.ReplicaDescriptor, 0, len(existing))
	for _, repl := range existing {
		if leaseStoreID == repl.StoreID {
			continue
		}
		storeDesc, ok := a.storePool.getStoreDescriptor(repl.StoreID)
		if !ok {
			continue
		}
		if float64(storeDesc.Capacity.LeaseCount) < sl.candidateLeases.mean-0.5 {
			candidates = append(candidates, repl)
		}
	}
	if len(candidates) == 0 {
		return roachpb.ReplicaDescriptor{}
	}
	a.randGen.Lock()
	defer a.randGen.Unlock()
	return candidates[a.randGen.Intn(len(candidates))]
}

// ShouldTransferLease returns true if the specified store is overfull in terms
// of leases with respect to the other stores matching the specified
// attributes.
func (a *Allocator) ShouldTransferLease(
	constraints config.Constraints,
	existing []roachpb.ReplicaDescriptor,
	leaseStoreID roachpb.StoreID,
	rangeID roachpb.RangeID,
) bool {
	source, ok := a.storePool.getStoreDescriptor(leaseStoreID)
	if !ok {
		return false
	}
	sl, _, _ := a.storePool.getStoreList(rangeID)
	sl = sl.filter(constraints)
	if log.V(3) {
		log.Infof(context.TODO(), "transfer-lease-source (lease-holder=%d):\n%s", leaseStoreID, sl)
	}
	return a.shouldTransferLease(sl, source, existing)
}

// EnableLeaseRebalancing controls whether lease rebalancing is enabled or
// not. Exported for testing.
var EnableLeaseRebalancing = envutil.EnvOrDefaultBool("COCKROACH_ENABLE_LEASE_REBALANCING", true)

func (a Allocator) shouldTransferLease(
	sl StoreList, source roachpb.StoreDescriptor, existing []roachpb.ReplicaDescriptor,
) bool {
	if !EnableLeaseRebalancing {
		return false
	}
	// Allow lease transfer if we're above the overfull threshold, which is
	// mean*(1+rebalanceThreshold).
	overfullLeaseThreshold := int32(math.Ceil(sl.candidateLeases.mean * (1 + rebalanceThreshold)))
	minOverfullThreshold := int32(math.Ceil(sl.candidateLeases.mean + 5))
	if overfullLeaseThreshold < minOverfullThreshold {
		overfullLeaseThreshold = minOverfullThreshold
	}
	if source.Capacity.LeaseCount > overfullLeaseThreshold {
		return true
	}

	if float64(source.Capacity.LeaseCount) > sl.candidateLeases.mean {
		underfullLeaseThreshold := int32(math.Ceil(sl.candidateLeases.mean * (1 - rebalanceThreshold)))
		minUnderfullThreshold := int32(math.Ceil(sl.candidateLeases.mean - 5))
		if underfullLeaseThreshold > minUnderfullThreshold {
			underfullLeaseThreshold = minUnderfullThreshold
		}

		for _, repl := range existing {
			storeDesc, ok := a.storePool.getStoreDescriptor(repl.StoreID)
			if !ok {
				continue
			}
			if storeDesc.Capacity.LeaseCount < underfullLeaseThreshold {
				return true
			}
		}
	}
	return false
}

// rebalanceThreshold is the minimum ratio of a store's range/lease surplus to
// the mean range/lease count that permits rebalances/lease-transfers away from
// that store.
var rebalanceThreshold = envutil.EnvOrDefaultFloat("COCKROACH_REBALANCE_THRESHOLD", 0.05)

// computeQuorum computes the quorum value for the given number of nodes.
func computeQuorum(nodes int) int {
	return (nodes / 2) + 1
}

// filterBehindReplicas removes any "behind" replicas from the supplied
// slice. A "behind" replica is one which is not at or past the quorum commit
// index.
func filterBehindReplicas(
	raftStatus *raft.Status, replicas []roachpb.ReplicaDescriptor,
) []roachpb.ReplicaDescriptor {
	if raftStatus == nil || len(raftStatus.Progress) == 0 {
		// raftStatus.Progress is only populated on the Raft leader which means we
		// won't be able to rebalance a lease away if the lease holder is not the
		// Raft leader. This is rare enough not to matter.
		return nil
	}
	// NB: We use raftStatus.Commit instead of getQuorumIndex() because the
	// latter can return a value that is less than the commit index. This is
	// useful for Raft log truncation which sometimes wishes to keep those
	// earlier indexes, but not appropriate for determining which nodes are
	// behind the actual commit index of the range.
	candidates := make([]roachpb.ReplicaDescriptor, 0, len(replicas))
	for _, r := range replicas {
		if progress, ok := raftStatus.Progress[uint64(r.ReplicaID)]; ok {
			if progress.State == raft.ProgressStateReplicate &&
				progress.Match >= raftStatus.Commit {
				candidates = append(candidates, r)
			}
		}
	}
	return candidates
}
