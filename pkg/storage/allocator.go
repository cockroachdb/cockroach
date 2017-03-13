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
	"time"

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

var (
	// MinLeaseTransferStatsDuration configures the minimum amount of time a
	// replica must wait for stats about request counts to accumulate before
	// making decisions based on them.
	// Made configurable for the sake of testing.
	MinLeaseTransferStatsDuration = time.Minute
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

type transferDecision int

const (
	_ transferDecision = iota
	shouldTransfer
	shouldNotTransfer
	decideWithoutStats
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

// Allocator tries to spread replicas as evenly as possible across the stores
// in the cluster.
type Allocator struct {
	storePool     *StorePool
	nodeLatencyFn func(addr string) (time.Duration, bool)
	randGen       allocatorRand
}

// MakeAllocator creates a new allocator using the specified StorePool.
func MakeAllocator(
	storePool *StorePool, nodeLatencyFn func(addr string) (time.Duration, bool),
) Allocator {
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
		storePool:     storePool,
		nodeLatencyFn: nodeLatencyFn,
		randGen:       makeAllocatorRand(randSource),
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
	ctx context.Context,
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
		a.storePool.getLocalities(existing),
		a.storePool.deterministic,
	)
	if log.V(3) {
		log.Infof(ctx, "allocate candidates: %s", candidates)
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
	ctx context.Context,
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
		a.storePool.getLocalities(existing),
		a.storePool.deterministic,
	)
	if log.V(3) {
		log.Infof(ctx, "remove candidates: %s", candidates)
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
	ctx context.Context,
	constraints config.Constraints,
	existing []roachpb.ReplicaDescriptor,
	leaseStoreID roachpb.StoreID,
	rangeID roachpb.RangeID,
) (*roachpb.StoreDescriptor, error) {
	sl, _, _ := a.storePool.getStoreList(rangeID)

	// TODO(bram): ShouldRebalance should be part of rebalanceCandidates
	// and decision made afterward, not it's own function. It is
	// performing the same operations as rebalanceCandidates and any
	// missing functionality can be added.
	var shouldRebalance bool
	for _, repl := range existing {
		if leaseStoreID == repl.StoreID {
			continue
		}
		storeDesc, ok := a.storePool.getStoreDescriptor(repl.StoreID)
		if ok && a.shouldRebalance(ctx, storeDesc, sl) {
			shouldRebalance = true
			break
		}
	}
	if !shouldRebalance {
		return nil, nil
	}

	existingCandidates, candidates := rebalanceCandidates(
		sl,
		constraints,
		existing,
		a.storePool.getLocalities(existing),
		a.storePool.deterministic,
	)

	if len(existingCandidates) == 0 {
		return nil, errors.Errorf(
			"all existing replicas' stores are not present in the store pool: %v\n%s", existing, sl)
	}

	if log.V(3) {
		log.Infof(ctx, "existing replicas: %s", existingCandidates)
		log.Infof(ctx, "candidates: %s", candidates)
	}

	// Find all candidates that are better than the worst existing replica.
	targets := candidates.betterThan(existingCandidates[len(existingCandidates)-1])
	return targets.selectGood(a.randGen), nil
}

// TransferLeaseTarget returns a suitable replica to transfer the range lease
// to from the provided list. It excludes the current lease holder replica
// unless asked to do otherwise by the checkTransferLeaseSource parameter.
func (a *Allocator) TransferLeaseTarget(
	ctx context.Context,
	constraints config.Constraints,
	existing []roachpb.ReplicaDescriptor,
	leaseStoreID roachpb.StoreID,
	rangeID roachpb.RangeID,
	stats *replicaStats,
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

	// Try to pick a replica to transfer the lease to while also determining
	// whether we actually should be transferring the lease. The transfer
	// decision is only needed if we've been asked to check the source.
	transferDec, repl := a.shouldTransferLeaseUsingStats(ctx, sl, source, existing, stats)
	if checkTransferLeaseSource {
		switch transferDec {
		case shouldNotTransfer:
			return roachpb.ReplicaDescriptor{}
		case shouldTransfer:
		case decideWithoutStats:
			if !a.shouldTransferLeaseWithoutStats(ctx, sl, source, existing) {
				return roachpb.ReplicaDescriptor{}
			}
		default:
			log.Fatalf(ctx, "unexpected transfer decision %d with replica %+v", transferDec, repl)
		}
	}

	if repl != (roachpb.ReplicaDescriptor{}) {
		return repl
	}

	// Fall back to logic that doesn't take request counts and latency into
	// account if the counts/latency-based logic couldn't pick a best replica.
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

// EnableLeaseRebalancing controls whether lease rebalancing is enabled or
// not. Exported for testing.
var EnableLeaseRebalancing = envutil.EnvOrDefaultBool("COCKROACH_ENABLE_LEASE_REBALANCING", true)

// EnableLoadBasedLeaseRebalancing controls whether lease rebalancing is done
// via the new heuristic based on request load and latency or via the simpler
// approach that purely seeks to balance the number of leases per node evenly.
var EnableLoadBasedLeaseRebalancing = envutil.EnvOrDefaultBool("COCKROACH_ENABLE_LOAD_BASED_LEASE_REBALANCING", false)

// ShouldTransferLease returns true if the specified store is overfull in terms
// of leases with respect to the other stores matching the specified
// attributes.
func (a *Allocator) ShouldTransferLease(
	ctx context.Context,
	constraints config.Constraints,
	existing []roachpb.ReplicaDescriptor,
	leaseStoreID roachpb.StoreID,
	rangeID roachpb.RangeID,
	stats *replicaStats,
) bool {
	if !EnableLeaseRebalancing {
		return false
	}

	source, ok := a.storePool.getStoreDescriptor(leaseStoreID)
	if !ok {
		return false
	}
	sl, _, _ := a.storePool.getStoreList(rangeID)
	sl = sl.filter(constraints)
	if log.V(3) {
		log.Infof(ctx, "ShouldTransferLease source (lease-holder=%d):\n%s", leaseStoreID, sl)
	}

	transferDec, _ := a.shouldTransferLeaseUsingStats(ctx, sl, source, existing, stats)
	switch transferDec {
	case shouldNotTransfer:
		return false
	case shouldTransfer:
		return true
	case decideWithoutStats:
	default:
		log.Fatalf(ctx, "unexpected transfer decision %d", transferDec)
	}

	return a.shouldTransferLeaseWithoutStats(ctx, sl, source, existing)
}

func (a Allocator) shouldTransferLeaseUsingStats(
	ctx context.Context,
	sl StoreList,
	source roachpb.StoreDescriptor,
	existing []roachpb.ReplicaDescriptor,
	stats *replicaStats,
) (transferDecision, roachpb.ReplicaDescriptor) {
	if stats == nil || !EnableLoadBasedLeaseRebalancing {
		return decideWithoutStats, roachpb.ReplicaDescriptor{}
	}
	requestCounts, requestCountsDur := stats.getRequestCounts()

	// If we haven't yet accumulated enough data, avoid transferring for now. Do
	// not fall back to the algorithm that doesn't use stats, since it can easily
	// start fighting with the stats-based algorithm. This provides some amount of
	// safety from lease thrashing, since leases cannot transfer more frequently
	// than this threshold (because replica stats get reset upon lease transfer).
	if requestCountsDur < MinLeaseTransferStatsDuration {
		return shouldNotTransfer, roachpb.ReplicaDescriptor{}
	}

	// On the other hand, if we don't have any stats with associated localities,
	// then do fall back to the algorithm that doesn't use request stats.
	delete(requestCounts, "")
	if len(requestCounts) == 0 {
		return decideWithoutStats, roachpb.ReplicaDescriptor{}
	}

	replicaWeights := make(map[roachpb.NodeID]float64)
	replicaLocalities := a.storePool.getLocalities(existing)
	for requestLocalityStr, count := range requestCounts {
		var requestLocality roachpb.Locality
		if err := requestLocality.Set(requestLocalityStr); err != nil {
			log.Errorf(ctx, "unable to parse locality string %q: %s", requestLocalityStr, err)
			continue
		}
		for nodeID, replicaLocality := range replicaLocalities {
			// Add weights to each replica based on the number of requests from
			// that replica's locality and neighboring localities.
			replicaWeights[nodeID] += (1 - replicaLocality.DiversityScore(requestLocality)) * count
		}
	}
	sourceWeight := math.Max(1.0, replicaWeights[source.Node.NodeID])

	if log.V(1) {
		log.Infof(ctx,
			"shouldTransferLease requestCounts: %+v, replicaLocalities: %+v, replicaWeights: %+v",
			requestCounts, replicaLocalities, replicaWeights)
	}

	// TODO(a-robinson): This may not have enough protection against all leases
	// ending up on a single node in extreme cases. Continue testing against
	// different situations.
	var bestRepl roachpb.ReplicaDescriptor
	bestReplScore := int32(math.MinInt32)
	for _, repl := range existing {
		if repl.NodeID == source.Node.NodeID {
			continue
		}
		storeDesc, ok := a.storePool.getStoreDescriptor(repl.StoreID)
		if !ok {
			continue
		}
		addr, err := a.storePool.gossip.GetNodeIDAddress(repl.NodeID)
		if err != nil {
			log.Errorf(ctx, "missing address for node %d: %s", repl.NodeID, err)
			continue
		}
		remoteLatency, ok := a.nodeLatencyFn(addr.String())
		if !ok {
			continue
		}

		remoteWeight := math.Max(1.0, replicaWeights[repl.NodeID])
		score := loadBasedLeaseRebalanceScore(
			ctx, remoteWeight, remoteLatency, storeDesc, sourceWeight, source, sl.candidateLeases.mean)
		if score > bestReplScore {
			bestReplScore = score
			bestRepl = repl
		}
	}

	// Return the best replica even in cases where transferring is not advised in
	// order to support forced lease transfers, such as when removing a replica or
	// draining all leases before shutdown.
	if bestReplScore > 0 {
		return shouldTransfer, bestRepl
	}
	return shouldNotTransfer, bestRepl
}

// This is a bit of magic that was determined to work well via a bunch of
// testing. See #13232 for context.
// TODO(a-robinson): Document the thinking behind this logic.
func loadBasedLeaseRebalanceScore(
	ctx context.Context,
	remoteWeight float64,
	remoteLatency time.Duration,
	remoteStore roachpb.StoreDescriptor,
	sourceWeight float64,
	source roachpb.StoreDescriptor,
	meanLeases float64,
) int32 {
	remoteLatencyMillis := float64(remoteLatency) / float64(time.Millisecond)
	rebalanceThreshold :=
		baseRebalanceThreshold - 0.1*math.Log10(remoteWeight/sourceWeight)*math.Log1p(remoteLatencyMillis)

	overfullLeaseThreshold := int32(math.Ceil(meanLeases * (1 + rebalanceThreshold)))
	overfullScore := source.Capacity.LeaseCount - overfullLeaseThreshold
	underfullLeaseThreshold := int32(math.Ceil(meanLeases * (1 - rebalanceThreshold)))
	underfullScore := underfullLeaseThreshold - remoteStore.Capacity.LeaseCount
	totalScore := overfullScore + underfullScore

	if log.V(1) {
		log.Infof(ctx,
			"node: %d, remoteWeight: %.2f, sourceWeight: %.2f, remoteLatency: %v, "+
				"rebalanceThreshold: %.2f, meanLeases: %.2f, remoteLeaseCount: %d, overfullThreshold: %d, "+
				"sourceLeaseCount: %d, underfullThreshold: %d, totalScore: %d",
			remoteStore.Node.NodeID, remoteWeight, sourceWeight, remoteLatency,
			rebalanceThreshold, meanLeases, source.Capacity.LeaseCount, overfullLeaseThreshold,
			remoteStore.Capacity.LeaseCount, underfullLeaseThreshold, totalScore,
		)
	}
	return totalScore
}

func (a Allocator) shouldTransferLeaseWithoutStats(
	ctx context.Context,
	sl StoreList,
	source roachpb.StoreDescriptor,
	existing []roachpb.ReplicaDescriptor,
) bool {
	// Allow lease transfer if we're above the overfull threshold, which is
	// mean*(1+baseRebalanceThreshold).
	overfullLeaseThreshold := int32(math.Ceil(sl.candidateLeases.mean * (1 + baseRebalanceThreshold)))
	minOverfullThreshold := int32(math.Ceil(sl.candidateLeases.mean + 5))
	if overfullLeaseThreshold < minOverfullThreshold {
		overfullLeaseThreshold = minOverfullThreshold
	}
	if source.Capacity.LeaseCount > overfullLeaseThreshold {
		return true
	}

	if float64(source.Capacity.LeaseCount) > sl.candidateLeases.mean {
		underfullLeaseThreshold := int32(math.Ceil(sl.candidateLeases.mean * (1 - baseRebalanceThreshold)))
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

// baseRebalanceThreshold is the minimum ratio of a store's range/lease surplus to
// the mean range/lease count that permits rebalances/lease-transfers away from
// that store.
var baseRebalanceThreshold = envutil.EnvOrDefaultFloat("COCKROACH_REBALANCE_THRESHOLD", 0.05)

// shouldRebalance returns whether the specified store is a candidate for
// having a replica removed from it given the candidate store list.
func (a Allocator) shouldRebalance(
	ctx context.Context, store roachpb.StoreDescriptor, sl StoreList,
) bool {
	// TODO(peter,bram,cuong): The FractionUsed check seems suspicious. When a
	// node becomes fuller than maxFractionUsedThreshold we will always select it
	// for rebalancing. This is currently utilized by tests.
	maxCapacityUsed := store.Capacity.FractionUsed() >= maxFractionUsedThreshold

	// Rebalance if we're above the rebalance target, which is
	// mean*(1+baseRebalanceThreshold).
	target := int32(math.Ceil(sl.candidateCount.mean * (1 + baseRebalanceThreshold)))
	rangeCountAboveTarget := store.Capacity.RangeCount > target

	// Rebalance if the candidate store has a range count above the mean, and
	// there exists another store that is underfull: its range count is smaller
	// than mean*(1-baseRebalanceThreshold).
	var rebalanceToUnderfullStore bool
	if float64(store.Capacity.RangeCount) > sl.candidateCount.mean {
		underfullThreshold := int32(math.Floor(sl.candidateCount.mean * (1 - baseRebalanceThreshold)))
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
		log.Infof(ctx,
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
			if uint64(r.ReplicaID) == raftStatus.Lead ||
				(progress.State == raft.ProgressStateReplicate &&
					progress.Match >= raftStatus.Commit) {
				candidates = append(candidates, r)
			}
		}
	}
	return candidates
}
