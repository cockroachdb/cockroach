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
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

const (
	// maxFractionUsedThreshold: if the fraction used of a store descriptor
	// capacity is greater than this value, it will never be used as a rebalance
	// target and it will always be eligible to rebalance replicas to other
	// stores.
	maxFractionUsedThreshold = 0.95

	// baseLeaseRebalanceThreshold is the minimum ratio of a store's lease surplus
	// to the mean range/lease count that permits lease-transfers away from that
	// store.
	baseLeaseRebalanceThreshold = 0.05

	// minReplicaWeight sets a floor for how low a replica weight can be. This is
	// needed because a weight of zero doesn't work in the current lease scoring
	// algorithm.
	minReplicaWeight = 0.001

	// priorities for various repair operations.
	addMissingReplicaPriority             float64 = 10000
	addDecommissioningReplacementPriority float64 = 5000
	removeDeadReplicaPriority             float64 = 1000
	removeDecommissioningReplicaPriority  float64 = 200
	removeExtraReplicaPriority            float64 = 100
)

// MinLeaseTransferStatsDuration configures the minimum amount of time a
// replica must wait for stats about request counts to accumulate before
// making decisions based on them. The higher this is, the less likely
// thrashing is (up to a point).
// Made configurable for the sake of testing.
var MinLeaseTransferStatsDuration = 30 * time.Second

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
	AllocatorRemoveDecommissioning
)

var allocatorActionNames = map[AllocatorAction]string{
	AllocatorNoop:                  "noop",
	AllocatorRemove:                "remove",
	AllocatorAdd:                   "add",
	AllocatorRemoveDead:            "remove dead",
	AllocatorRemoveDecommissioning: "remove decommissioning",
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

// RangeInfo contains the information needed by the allocator to make
// rebalancing decisions for a given range.
type RangeInfo struct {
	Desc            *roachpb.RangeDescriptor
	LiveBytes       int64
	WritesPerSecond float64
}

func rangeInfoForRepl(repl *Replica, desc *roachpb.RangeDescriptor) RangeInfo {
	writesPerSecond, _ := repl.writeStats.avgQPS()
	return RangeInfo{
		Desc:            desc,
		LiveBytes:       repl.GetMVCCStats().LiveBytes,
		WritesPerSecond: writesPerSecond,
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

// ComputeAction determines the exact operation needed to repair the
// supplied range, as governed by the supplied zone configuration. It
// returns the required action that should be taken and a priority.
func (a *Allocator) ComputeAction(
	ctx context.Context, zone config.ZoneConfig, rangeInfo RangeInfo,
) (AllocatorAction, float64) {
	if a.storePool == nil {
		// Do nothing if storePool is nil for some unittests.
		return AllocatorNoop, 0
	}

	// TODO(mrtracy): Handle non-homogeneous and mismatched attribute sets.
	need := int(zone.NumReplicas)
	have := len(rangeInfo.Desc.Replicas)
	quorum := computeQuorum(need)
	if have < need {
		// Range is under-replicated, and should add an additional replica.
		// Priority is adjusted by the difference between the current replica
		// count and the quorum of the desired replica count.
		priority := addMissingReplicaPriority + float64(quorum-have)
		if log.V(3) {
			log.Infof(ctx, "AllocatorAdd - missing replica need=%d, have=%d, priority=%.2f", need, have, priority)
		}
		return AllocatorAdd, priority
	}

	decommissioningReplicas := a.storePool.decommissioningReplicas(rangeInfo.Desc.RangeID, rangeInfo.Desc.Replicas)
	if have == need && len(decommissioningReplicas) > 0 {
		// Range has decommissioning replica(s). We should up-replicate to add
		// another replica. The decommissioning replica(s) will be down-replicated
		// later.
		priority := addDecommissioningReplacementPriority
		if log.V(3) {
			log.Infof(ctx, "AllocatorAdd - replacement for %d decommissioning replicas priority=%.2f", len(decommissioningReplicas), priority)
		}
		return AllocatorAdd, priority
	}

	liveReplicas, deadReplicas := a.storePool.liveAndDeadReplicas(rangeInfo.Desc.RangeID, rangeInfo.Desc.Replicas)
	if len(liveReplicas) < quorum {
		// Do not take any removal action if we do not have a quorum of live
		// replicas.
		return AllocatorNoop, 0
	}
	// Removal actions follow.
	if len(deadReplicas) > 0 {
		// The range has dead replicas, which should be removed immediately.
		removeDead := false
		switch {
		case have > need:
			// Allow removal of a dead replica if we have more than we need.
			// Reduce priority for this case?
			removeDead = true
		default: // have == need
			// Only allow removal of a dead replica if we have a suitable allocation
			// target that we can up-replicate to. This isn't necessarily the target
			// we'll up-replicate to, just an indication that such a target exists.
			if _, err := a.AllocateTarget(
				ctx,
				zone.Constraints,
				liveReplicas,
				rangeInfo,
				true, /* relaxConstraints */
			); err == nil {
				removeDead = true
			}
		}
		if removeDead {
			// Adjust the priority by the distance of live replicas from quorum.
			priority := removeDeadReplicaPriority + float64(quorum-len(liveReplicas))
			if log.V(3) {
				log.Infof(ctx, "AllocatorRemoveDead - dead=%d, live=%d, quorum=%d, priority=%.2f",
					len(deadReplicas), len(liveReplicas), quorum, priority)
			}
			return AllocatorRemoveDead, priority
		}
	}

	if have > need && len(decommissioningReplicas) > 0 {
		// Range is over-replicated, and has a decommissioning replica which
		// should be removed.
		priority := removeDecommissioningReplicaPriority
		if log.V(3) {
			log.Infof(
				ctx, "AllocatorRemoveDecommissioning - need=%d, have=%d, num_decommissioning=%d, priority=%.2f",
				need, have, len(decommissioningReplicas), priority)
		}
		return AllocatorRemoveDecommissioning, priority
	}

	if have > need {
		// Range is over-replicated, and should remove a replica.
		// Ranges with an even number of replicas get extra priority because
		// they have a more fragile quorum.
		priority := removeExtraReplicaPriority - float64(have%2)
		if log.V(3) {
			log.Infof(ctx, "AllocatorRemove - need=%d, have=%d, priority=%.2f", need, have, priority)
		}
		return AllocatorRemove, priority
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
	rangeInfo RangeInfo,
	relaxConstraints bool,
) (*roachpb.StoreDescriptor, error) {
	sl, _, throttledStoreCount := a.storePool.getStoreList(rangeInfo.Desc.RangeID, storeFilterThrottled)

	candidates := allocateCandidates(
		a.storePool.st,
		sl,
		constraints,
		existing,
		rangeInfo,
		a.storePool.getLocalities(existing),
		a.storePool.deterministic,
	)
	if log.V(3) {
		log.Infof(ctx, "allocate candidates: %s", candidates)
	}
	if target := candidates.selectGood(a.randGen); target != nil {
		if log.V(3) {
			log.Infof(ctx, "add target: %s", target)
		}
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
// replicas.
func (a Allocator) RemoveTarget(
	ctx context.Context,
	constraints config.Constraints,
	candidates []roachpb.ReplicaDescriptor,
	rangeInfo RangeInfo,
) (roachpb.ReplicaDescriptor, error) {
	if len(candidates) == 0 {
		return roachpb.ReplicaDescriptor{}, errors.Errorf("must supply at least one candidate replica to allocator.RemoveTarget()")
	}

	// Retrieve store descriptors for the provided candidates from the StorePool.
	existingStoreIDs := make(roachpb.StoreIDSlice, len(candidates))
	for i, exist := range candidates {
		existingStoreIDs[i] = exist.StoreID
	}
	sl, _, _ := a.storePool.getStoreListFromIDs(existingStoreIDs, roachpb.RangeID(0), storeFilterNone)

	rankedCandidates := removeCandidates(
		a.storePool.st,
		sl,
		constraints,
		rangeInfo,
		a.storePool.getLocalities(rangeInfo.Desc.Replicas),
		a.storePool.deterministic,
	)
	if log.V(3) {
		log.Infof(ctx, "remove candidates: %s", rankedCandidates)
	}
	if bad := rankedCandidates.selectBad(a.randGen); bad != nil {
		for _, exist := range rangeInfo.Desc.Replicas {
			if exist.StoreID == bad.StoreID {
				if log.V(3) {
					log.Infof(ctx, "remove target: %s", bad)
				}
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
// The supplied parameters are the required attributes for the range and
// information about the range being considered for rebalancing.
//
// The existing replicas modulo any store with dead replicas are candidates for
// rebalancing. Note that rebalancing is accomplished by first adding a new
// replica to the range, then removing the most undesirable replica.
//
// Simply ignoring a rebalance opportunity in the event that the target chosen
// by AllocateTarget() doesn't fit balancing criteria is perfectly fine, as
// other stores in the cluster will also be doing their probabilistic best to
// rebalance. This helps prevent a stampeding herd targeting an abnormally
// under-utilized store.
func (a Allocator) RebalanceTarget(
	ctx context.Context, constraints config.Constraints, rangeInfo RangeInfo, filter storeFilter,
) *roachpb.StoreDescriptor {
	sl, _, _ := a.storePool.getStoreList(rangeInfo.Desc.RangeID, filter)

	existingCandidates, candidates := rebalanceCandidates(
		ctx,
		a.storePool.st,
		sl,
		constraints,
		rangeInfo.Desc.Replicas,
		rangeInfo,
		a.storePool.getLocalities(rangeInfo.Desc.Replicas),
		a.storePool.deterministic,
	)

	// We're going to add another replica to the range which will change the
	// quorum size. Verify that the number of existing candidates is sufficient
	// to meet the new quorum. Note that "existingCandidates" only contains
	// replicas on live nodes while "rangeInfo.Desc.Replicas" contains all of the
	// replicas for a range. For a range configured for 3 replicas, this will
	// disable rebalancing if one of the replicas is on a down node. Instead,
	// we'll have to wait for the down node to be declared dead and go through the
	// dead-node removal dance: remove dead replica, add new replica.
	//
	// NB: The len(rangeInfo.Desc.Replicas) > 1 check allows rebalancing of ranges
	// with only a single replica. This is a corner case which could happen in
	// practice and also affects tests.
	newQuorum := computeQuorum(len(rangeInfo.Desc.Replicas) + 1)
	if len(rangeInfo.Desc.Replicas) > 1 && len(existingCandidates) < newQuorum {
		// Don't rebalance as we won't be able to make quorum after the rebalance
		// until the new replica has been caught up.
		return nil
	}

	// No need to rebalance.
	if len(existingCandidates) == 0 {
		return nil
	}

	// Find all candidates that are better than the worst existing replica.
	targets := candidates.betterThan(existingCandidates[len(existingCandidates)-1])
	target := targets.selectGood(a.randGen)
	if log.V(3) {
		log.Infof(ctx, "rebalance candidates: %s\nexisting replicas: %s\ntarget: %s",
			candidates, existingCandidates, target)
	}
	return target
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
	checkCandidateFullness bool,
	alwaysAllowDecisionWithoutStats bool,
) roachpb.ReplicaDescriptor {
	sl, _, _ := a.storePool.getStoreList(rangeID, storeFilterNone)
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
	transferDec, repl := a.shouldTransferLeaseUsingStats(
		ctx, sl, source, existing, stats,
	)
	if checkTransferLeaseSource {
		switch transferDec {
		case shouldNotTransfer:
			if !alwaysAllowDecisionWithoutStats {
				return roachpb.ReplicaDescriptor{}
			}
			fallthrough
		case decideWithoutStats:
			if !a.shouldTransferLeaseWithoutStats(ctx, sl, source, existing) {
				return roachpb.ReplicaDescriptor{}
			}
		case shouldTransfer:
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
		if !checkCandidateFullness || float64(storeDesc.Capacity.LeaseCount) < sl.candidateLeases.mean-0.5 {
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
	ctx context.Context,
	constraints config.Constraints,
	existing []roachpb.ReplicaDescriptor,
	leaseStoreID roachpb.StoreID,
	rangeID roachpb.RangeID,
	stats *replicaStats,
) bool {
	source, ok := a.storePool.getStoreDescriptor(leaseStoreID)
	if !ok {
		return false
	}
	sl, _, _ := a.storePool.getStoreList(rangeID, storeFilterNone)
	sl = sl.filter(constraints)
	if log.V(3) {
		log.Infof(ctx, "ShouldTransferLease (lease-holder=%d):\n%s", leaseStoreID, sl)
	}

	transferDec, _ := a.shouldTransferLeaseUsingStats(ctx, sl, source, existing, stats)
	var result bool
	switch transferDec {
	case shouldNotTransfer:
		result = false
	case shouldTransfer:
		result = true
	case decideWithoutStats:
		result = a.shouldTransferLeaseWithoutStats(ctx, sl, source, existing)
	default:
		log.Fatalf(ctx, "unexpected transfer decision %d", transferDec)
	}

	if log.V(3) {
		log.Infof(ctx, "ShouldTransferLease decision (lease-holder=%d): %t", leaseStoreID, result)
	}
	return result
}

func (a Allocator) shouldTransferLeaseUsingStats(
	ctx context.Context,
	sl StoreList,
	source roachpb.StoreDescriptor,
	existing []roachpb.ReplicaDescriptor,
	stats *replicaStats,
) (transferDecision, roachpb.ReplicaDescriptor) {
	// Only use load-based rebalancing if it's enabled and we have both
	// stats and locality information to base our decision on.
	if stats == nil || !a.storePool.st.EnableLoadBasedLeaseRebalancing.Get() {
		return decideWithoutStats, roachpb.ReplicaDescriptor{}
	}
	replicaLocalities := a.storePool.getLocalities(existing)
	for _, locality := range replicaLocalities {
		if len(locality.Tiers) == 0 {
			return decideWithoutStats, roachpb.ReplicaDescriptor{}
		}
	}

	qpsStats, qpsStatsDur := stats.perLocalityDecayingQPS()

	// If we haven't yet accumulated enough data, avoid transferring for now,
	// unless we've been explicitly asked otherwise. Do not fall back to the
	// algorithm that doesn't use stats, since it can easily start fighting with
	// the stats-based algorithm. This provides some amount of safety from lease
	// thrashing, since leases cannot transfer more frequently than this threshold
	// (because replica stats get reset upon lease transfer).
	if qpsStatsDur < MinLeaseTransferStatsDuration {
		return shouldNotTransfer, roachpb.ReplicaDescriptor{}
	}

	// On the other hand, if we don't have any stats with associated localities,
	// then do fall back to the algorithm that doesn't use request stats.
	delete(qpsStats, "")
	if len(qpsStats) == 0 {
		return decideWithoutStats, roachpb.ReplicaDescriptor{}
	}

	replicaWeights := make(map[roachpb.NodeID]float64)
	for requestLocalityStr, qps := range qpsStats {
		var requestLocality roachpb.Locality
		if err := requestLocality.Set(requestLocalityStr); err != nil {
			log.Errorf(ctx, "unable to parse locality string %q: %s", requestLocalityStr, err)
			continue
		}
		for nodeID, replicaLocality := range replicaLocalities {
			// Add weights to each replica based on the number of requests from
			// that replica's locality and neighboring localities.
			replicaWeights[nodeID] += (1 - replicaLocality.DiversityScore(requestLocality)) * qps
		}
	}

	if log.V(1) {
		log.Infof(ctx,
			"shouldTransferLease qpsStats: %+v, replicaLocalities: %+v, replicaWeights: %+v",
			qpsStats, replicaLocalities, replicaWeights)
	}
	sourceWeight := math.Max(minReplicaWeight, replicaWeights[source.Node.NodeID])

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

		remoteWeight := math.Max(minReplicaWeight, replicaWeights[repl.NodeID])
		score := loadBasedLeaseRebalanceScore(
			ctx, a.storePool.st, remoteWeight, remoteLatency, storeDesc, sourceWeight, source, sl.candidateLeases.mean)
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

// loadBasedLeaseRebalanceScore attempts to give a score to how desirable it
// would be to transfer a range lease from the local store to a remote store.
// It does so using a formula based on the latency between the stores and
// a number that we call the "weight" of each replica, which represents how
// many requests for the range have been coming from localities near the
// replica.
//
// The overarching goal is to move leases towards where requests are coming
// from when the latency between localities is high, because the leaseholder
// being near the request gateway makes for lower request latencies.
// This must be balanced against hurting throughput by putting too many leases
// one just a few nodes, though, which is why we get progressively more
// aggressive about moving the leases toward requests when latencies are high.
//
// The calculations below were determined via a bunch of manual testing (see
// #13232 or the leaseholder_locality.md RFC for more details), but the general
// logic behind each part of the formula is as follows:
//
// * LeaseRebalancingAggressiveness: Allow the aggressiveness to be tuned via
//   an environment variable.
// * 0.1: Constant factor to reduce aggressiveness by default
// * math.Log10(remoteWeight/sourceWeight): Comparison of the remote replica's
//   weight to the local replica's weight. Taking the log of the ratio instead
//   of using the ratio directly makes things symmetric -- i.e. r1 comparing
//   itself to r2 will come to the same conclusion as r2 comparing itself to r1.
// * math.Log1p(remoteLatencyMillis): This will be 0 if there's no latency,
//   removing the weight/latency factor from consideration. Otherwise, it grows
//   the aggressiveness for stores that are farther apart. Note that Log1p grows
//   faster than Log10 as its argument gets larger, which is intentional to
//   increase the importance of latency.
// * overfullScore and underfullScore: rebalanceThreshold helps us get an idea
//   of the ideal number of leases on each store. We then calculate these to
//   compare how close each node is to its ideal state and use the differences
//   from the ideal state on each node to compute a final score.
func loadBasedLeaseRebalanceScore(
	ctx context.Context,
	st *cluster.Settings,
	remoteWeight float64,
	remoteLatency time.Duration,
	remoteStore roachpb.StoreDescriptor,
	sourceWeight float64,
	source roachpb.StoreDescriptor,
	meanLeases float64,
) int32 {
	remoteLatencyMillis := float64(remoteLatency) / float64(time.Millisecond)
	rebalanceAdjustment :=
		st.LeaseRebalancingAggressiveness.Get() * 0.1 * math.Log10(remoteWeight/sourceWeight) * math.Log1p(remoteLatencyMillis)
	// Start with twice the base rebalance threshold in order to fight more
	// strongly against thrashing caused by small variances in the distribution
	// of request weights.
	rebalanceThreshold := (2 * baseLeaseRebalanceThreshold) - rebalanceAdjustment

	overfullLeaseThreshold := int32(math.Ceil(meanLeases * (1 + rebalanceThreshold)))
	overfullScore := source.Capacity.LeaseCount - overfullLeaseThreshold
	underfullLeaseThreshold := int32(math.Floor(meanLeases * (1 - rebalanceThreshold)))
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
	// mean*(1+baseLeaseRebalanceThreshold).
	overfullLeaseThreshold := int32(math.Ceil(sl.candidateLeases.mean * (1 + baseLeaseRebalanceThreshold)))
	minOverfullThreshold := int32(math.Ceil(sl.candidateLeases.mean + 5))
	if overfullLeaseThreshold < minOverfullThreshold {
		overfullLeaseThreshold = minOverfullThreshold
	}
	if source.Capacity.LeaseCount > overfullLeaseThreshold {
		return true
	}

	if float64(source.Capacity.LeaseCount) > sl.candidateLeases.mean {
		underfullLeaseThreshold := int32(math.Ceil(sl.candidateLeases.mean * (1 - baseLeaseRebalanceThreshold)))
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

// filterUnremovableReplicas removes any unremovable replicas from the supplied
// slice. An unremovable replicas is one which is a necessary part of the
// quorum that will result from removing 1 replica.
func filterUnremovableReplicas(
	raftStatus *raft.Status, replicas []roachpb.ReplicaDescriptor,
) []roachpb.ReplicaDescriptor {
	upToDateReplicas := filterBehindReplicas(raftStatus, replicas)
	quorum := computeQuorum(len(replicas) - 1)
	if len(upToDateReplicas) < quorum {
		// The number of up-to-date replicas is less than quorum. No replicas can
		// be removed.
		return nil
	}
	if len(upToDateReplicas) > quorum {
		// The number of up-to-date replicas is larger than quorum. Any replica can
		// be removed.
		return replicas
	}
	candidates := make([]roachpb.ReplicaDescriptor, 0, len(replicas)-len(upToDateReplicas))
	necessary := func(r roachpb.ReplicaDescriptor) bool {
		for _, t := range upToDateReplicas {
			if t == r {
				return true
			}
		}
		return false
	}
	for _, r := range replicas {
		if !necessary(r) {
			candidates = append(candidates, r)
		}
	}
	return candidates
}
