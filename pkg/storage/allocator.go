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

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/coreos/etcd/raft"
	"github.com/pkg/errors"
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

// AllocatorOptions are configurable options which effect the way that the
// replicate queue will handle rebalancing opportunities.
type AllocatorOptions struct {
	// AllowRebalance allows this store to attempt to rebalance its own
	// replicas to other stores.
	AllowRebalance bool

	// UseRuleSolver enables this store to use the updated rules based
	// constraint solver instead of the original rebalancer.
	UseRuleSolver bool
}

// Allocator tries to spread replicas as evenly as possible across the stores
// in the cluster.
type Allocator struct {
	storePool  *StorePool
	randGen    allocatorRand
	options    AllocatorOptions
	ruleSolver ruleSolver
}

// MakeAllocator creates a new allocator using the specified StorePool.
func MakeAllocator(storePool *StorePool, options AllocatorOptions) Allocator {
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
		storePool:  storePool,
		options:    options,
		randGen:    makeAllocatorRand(randSource),
		ruleSolver: defaultRuleSolver,
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
	if a.options.UseRuleSolver {
		sl, _, throttledStoreCount := a.storePool.getStoreList(rangeID)
		// When there are throttled stores that do match, we shouldn't send
		// the replica to purgatory.
		if throttledStoreCount > 0 {
			return nil, errors.Errorf("%d matching stores are currently throttled", throttledStoreCount)
		}

		candidates, err := a.ruleSolver.Solve(
			sl,
			constraints,
			existing,
			a.storePool.getNodeLocalities(existing),
		)
		if err != nil {
			return nil, err
		}

		if len(candidates) == 0 {
			return nil, &allocatorError{
				required: constraints.Constraints,
			}
		}
		// TODO(bram): #10275 Need some randomness here!
		return &candidates[0].store, nil
	}

	existingNodes := make(nodeIDSet, len(existing))
	for _, repl := range existing {
		existingNodes[repl.NodeID] = struct{}{}
	}

	sl, aliveStoreCount, throttledStoreCount := a.storePool.getStoreList(rangeID)

	// Because more redundancy is better than less, if relaxConstraints, the
	// matching here is lenient, and tries to find a target by relaxing an
	// attribute constraint, from last attribute to first.
	for attrs := constraints.Constraints; ; attrs = attrs[:len(attrs)-1] {
		filteredSL := sl.filter(config.Constraints{Constraints: attrs})
		if target := a.selectGood(filteredSL, existingNodes); target != nil {
			return target, nil
		}

		// When there are throttled stores that do match, we shouldn't send
		// the replica to purgatory or even consider relaxing the constraints.
		if throttledStoreCount > 0 {
			return nil, errors.Errorf("%d matching stores are currently throttled", throttledStoreCount)
		}
		if len(attrs) == 0 || !relaxConstraints {
			return nil, &allocatorError{
				required:         constraints.Constraints,
				relaxConstraints: relaxConstraints,
				aliveStoreCount:  aliveStoreCount,
			}
		}
	}
}

// RemoveTarget returns a suitable replica to remove from the provided replica
// set. It first attempts to randomly select a target from the set of stores
// that have greater than the average number of replicas. Failing that, it
// falls back to selecting a random target from any of the existing
// replicas. It also will exclude any replica that lives on leaseStoreID.
//
// TODO(mrtracy): removeTarget eventually needs to accept the attributes from
// the zone config associated with the provided replicas. This will allow it to
// make correct decisions in the case of ranges with heterogeneous replica
// requirements (i.e. multiple data centers).
func (a Allocator) RemoveTarget(
	constraints config.Constraints,
	existing []roachpb.ReplicaDescriptor,
	leaseStoreID roachpb.StoreID,
) (roachpb.ReplicaDescriptor, error) {
	if len(existing) == 0 {
		return roachpb.ReplicaDescriptor{}, errors.Errorf("must supply at least one replica to allocator.RemoveTarget()")
	}

	if a.options.UseRuleSolver {
		// TODO(bram): #10275 Is this getStoreList call required? Compute candidate
		// requires a store list, but we should be able to create one using only
		// the stores that belong to the range.
		// Use an invalid range ID as we don't care about a corrupt replicas since
		// as we are removing a replica and not trying to add one.
		sl, _, _ := a.storePool.getStoreList(roachpb.RangeID(0))

		worstCandidate := candidate{constraint: math.Inf(0)}
		var worstReplica roachpb.ReplicaDescriptor
		for _, exist := range existing {
			if exist.StoreID == leaseStoreID {
				continue
			}
			desc, ok := a.storePool.getStoreDescriptor(exist.StoreID)
			if !ok {
				continue
			}

			currentCandidate, valid := a.ruleSolver.computeCandidate(desc, solveState{
				constraints:            constraints,
				sl:                     sl,
				existing:               nil,
				existingNodeLocalities: a.storePool.getNodeLocalities(existing),
			})
			// When a candidate is not valid, it means that it can be
			// considered the worst existing replica.
			if !valid {
				return exist, nil
			}

			if currentCandidate.less(worstCandidate) {
				worstCandidate = currentCandidate
				worstReplica = exist
			}

		}

		if !math.IsInf(worstCandidate.constraint, 0) {
			return worstReplica, nil
		}

		return roachpb.ReplicaDescriptor{}, errors.New("could not select an appropriate replica to be removed")
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
	if bad := a.selectBad(sl); bad != nil {
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
	if !a.options.AllowRebalance {
		return nil, nil
	}

	if a.options.UseRuleSolver {
		sl, _, _ := a.storePool.getStoreList(rangeID)
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

		// Load the exiting storesIDs into a map so to eliminate having to loop
		// through the existing descriptors more than once.
		existingStoreIDs := make(map[roachpb.StoreID]struct{})
		for _, repl := range existing {
			existingStoreIDs[repl.StoreID] = struct{}{}
		}

		// Split the store list into existing and candidate stores lists so that
		// we can call solve independently on both store lists.
		var existingDescs []roachpb.StoreDescriptor
		var candidateDescs []roachpb.StoreDescriptor
		for _, desc := range sl.stores {
			if _, ok := existingStoreIDs[desc.StoreID]; ok {
				existingDescs = append(existingDescs, desc)
			} else {
				candidateDescs = append(candidateDescs, desc)
			}
		}

		existingStoreList := makeStoreList(existingDescs)
		candidateStoreList := makeStoreList(candidateDescs)

		existingCandidates, err := a.ruleSolver.Solve(existingStoreList, constraints, nil, nil)
		if err != nil {
			return nil, err
		}
		candidates, err := a.ruleSolver.Solve(candidateStoreList, constraints, nil, nil)
		if err != nil {
			return nil, err
		}

		// Find all candidates that are better than the worst existing store.
		var worstCandidate candidate
		// If any store from existing is not included in existingCandidates, it
		// is because it no longer meets the constraints. If so, its score is
		// considered to be 0.
		if len(existingCandidates) == len(existing) {
			worstCandidate = existingCandidates[len(existingCandidates)-1]
		}

		// TODO(bram): #10275 Need some randomness here!
		for _, cand := range candidates {
			if worstCandidate.less(cand) {
				return &candidates[0].store, nil
			}
		}

		return nil, nil
	}

	sl, _, _ := a.storePool.getStoreList(rangeID)
	sl = sl.filter(constraints)
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

	existingNodes := make(nodeIDSet, len(existing))
	for _, repl := range existing {
		existingNodes[repl.NodeID] = struct{}{}
	}
	return a.improve(sl, existingNodes), nil
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
	if !a.options.AllowRebalance {
		return roachpb.ReplicaDescriptor{}
	}

	sl, _, _ := a.storePool.getStoreList(rangeID)
	sl = sl.filter(constraints)

	source, ok := a.storePool.getStoreDescriptor(leaseStoreID)
	if !ok {
		return roachpb.ReplicaDescriptor{}
	}
	if checkTransferLeaseSource && !shouldTransferLease(sl, source) {
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
	constraints config.Constraints, leaseStoreID roachpb.StoreID, rangeID roachpb.RangeID,
) bool {
	if !a.options.AllowRebalance {
		return false
	}

	source, ok := a.storePool.getStoreDescriptor(leaseStoreID)
	if !ok {
		return false
	}
	sl, _, _ := a.storePool.getStoreList(rangeID)
	sl = sl.filter(constraints)
	if log.V(3) {
		log.Infof(context.TODO(), "transfer-lease-source (lease-holder=%d):\n%s", leaseStoreID, sl)
	}
	return shouldTransferLease(sl, source)
}

// EnableLeaseRebalancing controls whether lease rebalancing is enabled or
// not. Exported for testing.
var EnableLeaseRebalancing = envutil.EnvOrDefaultBool("COCKROACH_ENABLE_LEASE_REBALANCING", false)

func shouldTransferLease(sl StoreList, source roachpb.StoreDescriptor) bool {
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
	return source.Capacity.LeaseCount > overfullLeaseThreshold
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
func (a Allocator) improve(sl StoreList, excluded nodeIDSet) *roachpb.StoreDescriptor {
	rcb := rangeCountBalancer{a.randGen}
	return rcb.improve(sl, excluded)
}

// rebalanceThreshold is the minimum ratio of a store's range surplus to the
// mean range count that permits rebalances away from that store.
var rebalanceThreshold = envutil.EnvOrDefaultFloat("COCKROACH_REBALANCE_THRESHOLD", 0.05)

// shouldRebalance returns whether the specified store is a candidate for
// having a replica removed from it given the candidate store list.
func (a Allocator) shouldRebalance(store roachpb.StoreDescriptor, sl StoreList) bool {
	// TODO(peter,bram,cuong): The FractionUsed check seems suspicious. When a
	// node becomes fuller than maxFractionUsedThreshold we will always select it
	// for rebalancing. This is currently utilized by tests.
	maxCapacityUsed := store.Capacity.FractionUsed() >= maxFractionUsedThreshold

	// Rebalance if we're above the rebalance target, which is
	// mean*(1+rebalanceThreshold).
	target := int32(math.Ceil(sl.candidateCount.mean * (1 + rebalanceThreshold)))
	rangeCountAboveTarget := store.Capacity.RangeCount > target

	// Rebalance if the candidate store has a range count above the mean, and
	// there exists another store that is underfull: its range count is smaller
	// than mean*(1-rebalanceThreshold).
	var rebalanceToUnderfullStore bool
	if float64(store.Capacity.RangeCount) > sl.candidateCount.mean {
		underfullThreshold := int32(math.Floor(sl.candidateCount.mean * (1 - rebalanceThreshold)))
		for _, desc := range sl.stores {
			if desc.Capacity.RangeCount < underfullThreshold {
				rebalanceToUnderfullStore = true
				break
			}
		}
	}

	// Require that moving a replica from the given store makes its range count
	// converge on the mean range count. This only affects clusters with a
	// small number of ranges.
	rebalanceConvergesOnMean := rebalanceFromConvergesOnMean(sl, store)

	shouldRebalance :=
		(maxCapacityUsed || rangeCountAboveTarget || rebalanceToUnderfullStore) && rebalanceConvergesOnMean
	if log.V(2) {
		log.Infof(context.TODO(),
			"%d: should-rebalance=%t: fraction-used=%.2f range-count=%d "+
				"(mean=%.1f, target=%d, fraction-used=%t, above-target=%t, underfull=%t, converges=%t)",
			store.StoreID, shouldRebalance, store.Capacity.FractionUsed(), store.Capacity.RangeCount,
			sl.candidateCount.mean, target, maxCapacityUsed, rangeCountAboveTarget,
			rebalanceToUnderfullStore, rebalanceConvergesOnMean)
	}
	return shouldRebalance
}

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
		return nil
	}
	quorumIndex := getQuorumIndex(raftStatus, 0)
	candidates := make([]roachpb.ReplicaDescriptor, 0, len(replicas))
	for _, r := range replicas {
		if progress, ok := raftStatus.Progress[uint64(r.ReplicaID)]; ok {
			if progress.Match >= quorumIndex {
				candidates = append(candidates, r)
			}
		}
	}
	return candidates
}
