// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/constraint"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/tracker"
)

const (
	// leaseRebalanceThreshold is the minimum ratio of a store's lease surplus
	// to the mean range/lease count that permits lease-transfers away from that
	// store.
	leaseRebalanceThreshold = 0.05

	// baseLoadBasedLeaseRebalanceThreshold is the equivalent of
	// leaseRebalanceThreshold for load-based lease rebalance decisions (i.e.
	// "follow-the-workload"). It's the base threshold for decisions that get
	// adjusted based on the load and latency of the involved ranges/nodes.
	baseLoadBasedLeaseRebalanceThreshold = 2 * leaseRebalanceThreshold

	// minReplicaWeight sets a floor for how low a replica weight can be. This is
	// needed because a weight of zero doesn't work in the current lease scoring
	// algorithm.
	minReplicaWeight = 0.001

	// Priorities for various repair operations.
	finalizeAtomicReplicationChangePriority float64 = 12002
	removeLearnerReplicaPriority            float64 = 12001
	addDeadReplacementPriority              float64 = 12000
	addMissingReplicaPriority               float64 = 10000
	addDecommissioningReplacementPriority   float64 = 5000
	removeDeadReplicaPriority               float64 = 1000
	removeDecommissioningReplicaPriority    float64 = 200
	removeExtraReplicaPriority              float64 = 100
)

// MinLeaseTransferStatsDuration configures the minimum amount of time a
// replica must wait for stats about request counts to accumulate before
// making decisions based on them. The higher this is, the less likely
// thrashing is (up to a point).
// Made configurable for the sake of testing.
var MinLeaseTransferStatsDuration = 30 * time.Second

// enableLoadBasedLeaseRebalancing controls whether lease rebalancing is done
// via the new heuristic based on request load and latency or via the simpler
// approach that purely seeks to balance the number of leases per node evenly.
var enableLoadBasedLeaseRebalancing = settings.RegisterPublicBoolSetting(
	"kv.allocator.load_based_lease_rebalancing.enabled",
	"set to enable rebalancing of range leases based on load and latency",
	true,
)

// leaseRebalancingAggressiveness enables users to tweak how aggressive their
// cluster is at moving leases towards the localities where the most requests
// are coming from. Settings lower than 1.0 will make the system less
// aggressive about moving leases toward requests than the default, while
// settings greater than 1.0 will cause more aggressive placement.
//
// Setting this to 0 effectively disables load-based lease rebalancing, and
// settings less than 0 are disallowed.
var leaseRebalancingAggressiveness = settings.RegisterNonNegativeFloatSetting(
	"kv.allocator.lease_rebalancing_aggressiveness",
	"set greater than 1.0 to rebalance leases toward load more aggressively, "+
		"or between 0 and 1.0 to be more conservative about rebalancing leases",
	1.0,
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
	AllocatorReplaceDead
	AllocatorRemoveDead
	AllocatorReplaceDecommissioning
	AllocatorRemoveDecommissioning
	AllocatorRemoveLearner
	AllocatorConsiderRebalance
	AllocatorRangeUnavailable
	AllocatorFinalizeAtomicReplicationChange
)

var allocatorActionNames = map[AllocatorAction]string{
	AllocatorNoop:                            "noop",
	AllocatorRemove:                          "remove",
	AllocatorAdd:                             "add",
	AllocatorReplaceDead:                     "replace dead",
	AllocatorRemoveDead:                      "remove dead",
	AllocatorReplaceDecommissioning:          "replace decommissioning",
	AllocatorRemoveDecommissioning:           "remove decommissioning",
	AllocatorRemoveLearner:                   "remove learner",
	AllocatorConsiderRebalance:               "consider rebalance",
	AllocatorRangeUnavailable:                "range unavailable",
	AllocatorFinalizeAtomicReplicationChange: "finalize conf change",
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
	constraints      []zonepb.ConstraintsConjunction
	existingReplicas int
	aliveStores      int
	throttledStores  int
}

func (ae *allocatorError) Error() string {
	var existingReplsStr string
	if ae.existingReplicas == 1 {
		existingReplsStr = "1 already has a replica"
	} else {
		existingReplsStr = fmt.Sprintf("%d already have a replica", ae.existingReplicas)
	}

	var baseMsg string
	if ae.throttledStores != 0 {
		baseMsg = fmt.Sprintf(
			"0 of %d live stores are able to take a new replica for the range (%d throttled, %s)",
			ae.aliveStores, ae.throttledStores, existingReplsStr)
	} else {
		baseMsg = fmt.Sprintf(
			"0 of %d live stores are able to take a new replica for the range (%s)",
			ae.aliveStores, existingReplsStr)
	}

	if len(ae.constraints) == 0 {
		if ae.throttledStores > 0 {
			return baseMsg
		}
		return baseMsg + "; likely not enough nodes in cluster"
	}
	var b strings.Builder
	b.WriteString(baseMsg)
	b.WriteString("; must match constraints [")
	for i := range ae.constraints {
		if i > 0 {
			b.WriteByte(' ')
		}
		b.WriteByte('{')
		b.WriteString(ae.constraints[i].String())
		b.WriteByte('}')
	}
	b.WriteString("]")
	return b.String()
}

func (*allocatorError) purgatoryErrorMarker() {}

var _ purgatoryError = &allocatorError{}

// allocatorRand pairs a rand.Rand with a mutex.
// NOTE: Allocator is typically only accessed from a single thread (the
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

// RangeUsageInfo contains usage information (sizes and traffic) needed by the
// allocator to make rebalancing decisions for a given range.
type RangeUsageInfo struct {
	LogicalBytes     int64
	QueriesPerSecond float64
	WritesPerSecond  float64
}

func rangeUsageInfoForRepl(repl *Replica) RangeUsageInfo {
	info := RangeUsageInfo{
		LogicalBytes: repl.GetMVCCStats().Total(),
	}
	if queriesPerSecond, dur := repl.leaseholderStats.avgQPS(); dur >= MinStatsDuration {
		info.QueriesPerSecond = queriesPerSecond
	}
	if writesPerSecond, dur := repl.writeStats.avgQPS(); dur >= MinStatsDuration {
		info.WritesPerSecond = writesPerSecond
	}
	return info
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

// GetNeededReplicas calculates the number of replicas a range should
// have given its zone config and the number of nodes available for
// up-replication (i.e. not dead and not decommissioning).
func GetNeededReplicas(zoneConfigReplicaCount int32, clusterNodes int) int {
	numZoneReplicas := int(zoneConfigReplicaCount)
	need := numZoneReplicas

	// Adjust the replication factor for all ranges if there are fewer
	// nodes than replicas specified in the zone config, so the cluster
	// can still function.
	if clusterNodes < need {
		need = clusterNodes
	}

	// Ensure that we don't up- or down-replicate to an even number of replicas
	// unless an even number of replicas was specifically requested by the user
	// in the zone config.
	//
	// Note that in the case of 5 desired replicas and a decommissioning store,
	// this prefers down-replicating from 5 to 3 rather than sticking with 4
	// desired stores or blocking the decommissioning from completing.
	if need == numZoneReplicas {
		return need
	}
	if need%2 == 0 {
		need = need - 1
	}
	if need < 3 {
		need = 3
	}
	if need > numZoneReplicas {
		need = numZoneReplicas
	}

	return need
}

// ComputeAction determines the exact operation needed to repair the
// supplied range, as governed by the supplied zone configuration. It
// returns the required action that should be taken and a priority.
func (a *Allocator) ComputeAction(
	ctx context.Context, zone *zonepb.ZoneConfig, desc *roachpb.RangeDescriptor,
) (AllocatorAction, float64) {
	if a.storePool == nil {
		// Do nothing if storePool is nil for some unittests.
		return AllocatorNoop, 0
	}

	if desc.Replicas().InAtomicReplicationChange() {
		// With a similar reasoning to the learner branch below, if we're in a
		// joint configuration the top priority is to leave it before we can
		// even think about doing anything else.
		return AllocatorFinalizeAtomicReplicationChange, finalizeAtomicReplicationChangePriority
	}

	// Seeing a learner replica at this point is unexpected because learners are a
	// short-lived (ish) transient state in a learner+snapshot+voter cycle, which
	// is always done atomically. Only two places could have added a learner: the
	// replicate queue or AdminChangeReplicas request.
	//
	// The replicate queue only operates on leaseholders, which means that only
	// one node at a time is operating on a given range except in rare cases (old
	// leaseholder could start the operation, and a new leaseholder steps up and
	// also starts an overlapping operation). Combined with the above atomicity,
	// this means that if the replicate queue sees a learner, either the node that
	// was adding it crashed somewhere in the learner+snapshot+voter cycle and
	// we're the new leaseholder or we caught a race.
	//
	// In the first case, we could assume the node that was adding it knew what it
	// was doing and finish the addition. Or we could leave it and do higher
	// priority operations first if there are any. However, this comes with code
	// complexity and concept complexity (computing old vs new quorum sizes
	// becomes ambiguous, the learner isn't in the quorum but it likely will be
	// soon, so do you count it?). Instead, we do the simplest thing and remove it
	// before doing any other operations to the range. We'll revisit this decision
	// if and when the complexity becomes necessary.
	//
	// If we get the race where AdminChangeReplicas is adding a replica and the
	// queue happens to run during the snapshot, this will remove the learner and
	// AdminChangeReplicas will notice either during the snapshot transfer or when
	// it tries to promote the learner to a voter. AdminChangeReplicas should
	// retry.
	//
	// On the other hand if we get the race where a leaseholder starts adding a
	// replica in the replicate queue and during this loses its lease, it should
	// probably not retry.
	if learners := desc.Replicas().Learners(); len(learners) > 0 {
		// TODO(dan): Since this goes before anything else, the priority here should
		// be influenced by whatever operations would happen right after the learner
		// is removed. In the meantime, we don't want to block something important
		// from happening (like addDeadReplacementPriority) by queueing this at a
		// low priority so until this TODO is done, keep
		// removeLearnerReplicaPriority as the highest priority.
		return AllocatorRemoveLearner, removeLearnerReplicaPriority
	}
	// computeAction expects to operate only on voters.
	return a.computeAction(ctx, zone, desc.Replicas().Voters())
}

func (a *Allocator) computeAction(
	ctx context.Context, zone *zonepb.ZoneConfig, voterReplicas []roachpb.ReplicaDescriptor,
) (AllocatorAction, float64) {
	// TODO(mrtracy): Handle non-homogeneous and mismatched attribute sets.
	have := len(voterReplicas)
	decommissioningReplicas := a.storePool.decommissioningReplicas(voterReplicas)
	clusterNodes := a.storePool.ClusterNodeCount()
	need := GetNeededReplicas(*zone.NumReplicas, clusterNodes)
	desiredQuorum := computeQuorum(need)
	quorum := computeQuorum(have)

	if have < need {
		// Range is under-replicated, and should add an additional replica.
		// Priority is adjusted by the difference between the current replica
		// count and the quorum of the desired replica count.
		priority := addMissingReplicaPriority + float64(desiredQuorum-have)
		action := AllocatorAdd
		log.VEventf(ctx, 3, "%s - missing replica need=%d, have=%d, priority=%.2f",
			action, need, have, priority)
		return action, priority
	}

	liveVoterReplicas, deadVoterReplicas := a.storePool.liveAndDeadReplicas(voterReplicas)

	if len(liveVoterReplicas) < quorum {
		// Do not take any replacement/removal action if we do not have a quorum of live
		// replicas. If we're correctly assessing the unavailable state of the range, we
		// also won't be able to add replicas as we try above, but hope springs eternal.
		log.VEventf(ctx, 1, "unable to take action - live replicas %v don't meet quorum of %d",
			liveVoterReplicas, quorum)
		return AllocatorRangeUnavailable, 0
	}

	if have == need && len(deadVoterReplicas) > 0 {
		// Range has dead replica(s). We should up-replicate to add another before
		// before removing the dead one. This can avoid permanent data loss in cases
		// where the node is only temporarily dead, but we remove it from the range
		// and lose a second node before we can up-replicate (#25392).
		// The dead replica(s) will be down-replicated later.
		priority := addDeadReplacementPriority
		action := AllocatorReplaceDead
		log.VEventf(ctx, 3, "%s - replacement for %d dead replicas priority=%.2f",
			action, len(deadVoterReplicas), priority)
		return action, priority
	}

	if have == need && len(decommissioningReplicas) > 0 {
		// Range has decommissioning replica(s), which should be replaced.
		priority := addDecommissioningReplacementPriority
		action := AllocatorReplaceDecommissioning
		log.VEventf(ctx, 3, "%s - replacement for %d decommissioning replicas priority=%.2f",
			action, len(decommissioningReplicas), priority)
		return action, priority
	}

	// Removal actions follow.
	// TODO(a-robinson): There's an additional case related to dead replicas that
	// we should handle above. If there are one or more dead replicas, have <
	// need, and there are no available stores to up-replicate to, then we should
	// try to remove the dead replica(s) to get down to an odd number of
	// replicas.
	if len(deadVoterReplicas) > 0 {
		// The range has dead replicas, which should be removed immediately.
		priority := removeDeadReplicaPriority + float64(quorum-len(liveVoterReplicas))
		action := AllocatorRemoveDead
		log.VEventf(ctx, 3, "%s - dead=%d, live=%d, quorum=%d, priority=%.2f",
			action, len(deadVoterReplicas), len(liveVoterReplicas), quorum, priority)
		return action, priority
	}

	if len(decommissioningReplicas) > 0 {
		// Range is over-replicated, and has a decommissioning replica which
		// should be removed.
		priority := removeDecommissioningReplicaPriority
		action := AllocatorRemoveDecommissioning
		log.VEventf(ctx, 3,
			"%s - need=%d, have=%d, num_decommissioning=%d, priority=%.2f",
			action, need, have, len(decommissioningReplicas), priority)
		return action, priority
	}

	if have > need {
		// Range is over-replicated, and should remove a replica.
		// Ranges with an even number of replicas get extra priority because
		// they have a more fragile quorum.
		priority := removeExtraReplicaPriority - float64(have%2)
		action := AllocatorRemove
		log.VEventf(ctx, 3, "%s - need=%d, have=%d, priority=%.2f", action, need, have, priority)
		return action, priority
	}

	// Nothing needs to be done, but we may want to rebalance.
	return AllocatorConsiderRebalance, 0
}

type decisionDetails struct {
	Target   string
	Existing string `json:",omitempty"`
}

// AllocateTarget returns a suitable store for a new allocation with the
// required attributes. Nodes already accommodating existing replicas are ruled
// out as targets. The range ID of the replica being allocated for is also
// passed in to ensure that we don't try to replace an existing dead replica on
// a store.
//
// TODO(tbg): AllocateReplacement?
func (a *Allocator) AllocateTarget(
	ctx context.Context, zone *zonepb.ZoneConfig, existingReplicas []roachpb.ReplicaDescriptor,
) (*roachpb.StoreDescriptor, string, error) {
	sl, aliveStoreCount, throttled := a.storePool.getStoreList(storeFilterThrottled)

	target, details := a.allocateTargetFromList(
		ctx, sl, zone, existingReplicas, a.scorerOptions())

	if target != nil {
		return target, details, nil
	}

	// When there are throttled stores that do match, we shouldn't send
	// the replica to purgatory.
	if len(throttled) > 0 {
		return nil, "", errors.Errorf(
			"%d matching stores are currently throttled: %v", len(throttled), throttled,
		)
	}
	return nil, "", &allocatorError{
		constraints:      zone.Constraints,
		existingReplicas: len(existingReplicas),
		aliveStores:      aliveStoreCount,
		throttledStores:  len(throttled),
	}
}

func (a *Allocator) allocateTargetFromList(
	ctx context.Context,
	sl StoreList,
	zone *zonepb.ZoneConfig,
	candidateReplicas []roachpb.ReplicaDescriptor,
	options scorerOptions,
) (*roachpb.StoreDescriptor, string) {
	analyzedConstraints := constraint.AnalyzeConstraints(
		ctx, a.storePool.getStoreDescriptor, candidateReplicas, zone)
	candidates := allocateCandidates(
		sl, analyzedConstraints, candidateReplicas, a.storePool.getLocalities(candidateReplicas),
		options,
	)
	log.VEventf(ctx, 3, "allocate candidates: %s", candidates)
	if target := candidates.selectGood(a.randGen); target != nil {
		log.VEventf(ctx, 3, "add target: %s", target)
		details := decisionDetails{Target: target.compactString(options)}
		detailsBytes, err := json.Marshal(details)
		if err != nil {
			log.Warningf(ctx, "failed to marshal details for choosing allocate target: %+v", err)
		}
		return &target.store, string(detailsBytes)
	}

	return nil, ""
}

func (a Allocator) simulateRemoveTarget(
	ctx context.Context,
	targetStore roachpb.StoreID,
	zone *zonepb.ZoneConfig,
	candidates []roachpb.ReplicaDescriptor,
	existingReplicas []roachpb.ReplicaDescriptor,
	rangeUsageInfo RangeUsageInfo,
) (roachpb.ReplicaDescriptor, string, error) {
	// Update statistics first
	// TODO(a-robinson): This could theoretically interfere with decisions made by other goroutines,
	// but as of October 2017 calls to the Allocator are mostly serialized by the ReplicateQueue
	// (with the main exceptions being Scatter and the status server's allocator debug endpoint).
	// Try to make this interfere less with other callers.
	a.storePool.updateLocalStoreAfterRebalance(targetStore, rangeUsageInfo, roachpb.ADD_REPLICA)
	defer func() {
		a.storePool.updateLocalStoreAfterRebalance(targetStore, rangeUsageInfo, roachpb.REMOVE_REPLICA)
	}()
	log.VEventf(ctx, 3, "simulating which replica would be removed after adding s%d", targetStore)
	return a.RemoveTarget(ctx, zone, candidates, existingReplicas)
}

// RemoveTarget returns a suitable replica to remove from the provided replica
// set. It first attempts to randomly select a target from the set of stores
// that have greater than the average number of replicas. Failing that, it
// falls back to selecting a random target from any of the existing
// replicas.
func (a Allocator) RemoveTarget(
	ctx context.Context,
	zone *zonepb.ZoneConfig,
	candidates []roachpb.ReplicaDescriptor,
	existingReplicas []roachpb.ReplicaDescriptor,
) (roachpb.ReplicaDescriptor, string, error) {
	if len(candidates) == 0 {
		return roachpb.ReplicaDescriptor{}, "", errors.Errorf("must supply at least one candidate replica to allocator.RemoveTarget()")
	}

	// Retrieve store descriptors for the provided candidates from the StorePool.
	existingStoreIDs := make(roachpb.StoreIDSlice, len(candidates))
	for i, exist := range candidates {
		existingStoreIDs[i] = exist.StoreID
	}
	sl, _, _ := a.storePool.getStoreListFromIDs(existingStoreIDs, storeFilterNone)

	analyzedConstraints := constraint.AnalyzeConstraints(
		ctx, a.storePool.getStoreDescriptor, existingReplicas, zone)
	options := a.scorerOptions()
	rankedCandidates := removeCandidates(
		sl,
		analyzedConstraints,
		a.storePool.getLocalities(existingReplicas),
		options,
	)
	log.VEventf(ctx, 3, "remove candidates: %s", rankedCandidates)
	if bad := rankedCandidates.selectBad(a.randGen); bad != nil {
		for _, exist := range existingReplicas {
			if exist.StoreID == bad.store.StoreID {
				log.VEventf(ctx, 3, "remove target: %s", bad)
				details := decisionDetails{Target: bad.compactString(options)}
				detailsBytes, err := json.Marshal(details)
				if err != nil {
					log.Warningf(ctx, "failed to marshal details for choosing remove target: %+v", err)
				}
				return exist, string(detailsBytes), nil
			}
		}
	}

	return roachpb.ReplicaDescriptor{}, "", errors.New("could not select an appropriate replica to be removed")
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
//
// The return values are, in order:
//
// 1. The target on which to add a new replica,
// 2. An existing replica to remove,
// 3. a JSON string for use in the range log, and
// 4. a boolean indicationg whether 1-3 were populated (i.e. whether a rebalance
//    opportunity was found).
func (a Allocator) RebalanceTarget(
	ctx context.Context,
	zone *zonepb.ZoneConfig,
	raftStatus *raft.Status,
	existingReplicas []roachpb.ReplicaDescriptor,
	rangeUsageInfo RangeUsageInfo,
	filter storeFilter,
) (add roachpb.ReplicationTarget, remove roachpb.ReplicationTarget, details string, ok bool) {
	sl, _, _ := a.storePool.getStoreList(filter)

	zero := roachpb.ReplicationTarget{}

	// We're going to add another replica to the range which will change the
	// quorum size. Verify that the number of existing live replicas is sufficient
	// to meet the new quorum. For a range configured for 3 replicas, this will
	// disable rebalancing if one of the replicas is on a down node. Instead,
	// we'll have to wait for the down node to be declared dead and go through the
	// dead-node removal dance: remove dead replica, add new replica.
	//
	// NB: The len(replicas) > 1 check allows rebalancing of ranges with only a
	// single replica. This is a corner case which could happen in practice and
	// also affects tests.
	if len(existingReplicas) > 1 {
		var numLiveReplicas int
		for _, s := range sl.stores {
			for _, repl := range existingReplicas {
				if s.StoreID == repl.StoreID {
					numLiveReplicas++
					break
				}
			}
		}
		newQuorum := computeQuorum(len(existingReplicas) + 1)
		if numLiveReplicas < newQuorum {
			// Don't rebalance as we won't be able to make quorum after the rebalance
			// until the new replica has been caught up.
			return zero, zero, "", false
		}
	}

	analyzedConstraints := constraint.AnalyzeConstraints(
		ctx, a.storePool.getStoreDescriptor, existingReplicas, zone)
	options := a.scorerOptions()
	results := rebalanceCandidates(
		ctx,
		sl,
		analyzedConstraints,
		existingReplicas,
		a.storePool.getLocalities(existingReplicas),
		a.storePool.getNodeLocalityString,
		options,
	)

	if len(results) == 0 {
		return zero, zero, "", false
	}
	// Keep looping until we either run out of options or find a target that we're
	// pretty sure we won't want to remove immediately after adding it.
	// If we would, we don't want to actually rebalance to that target.
	var target *candidate
	var removeReplica roachpb.ReplicaDescriptor
	var existingCandidates candidateList
	for {
		target, existingCandidates = bestRebalanceTarget(a.randGen, results)
		if target == nil {
			return zero, zero, "", false
		}

		// Add a fake new replica to our copy of the range descriptor so that we can
		// simulate the removal logic. If we decide not to go with this target, note
		// that this needs to be removed from desc before we try any other target.
		newReplica := roachpb.ReplicaDescriptor{
			NodeID:    target.store.Node.NodeID,
			StoreID:   target.store.StoreID,
			ReplicaID: maxReplicaID(existingReplicas) + 1,
		}
		// Deep-copy the Replicas slice since we'll mutate it below.
		existingPlusOneNew := append([]roachpb.ReplicaDescriptor(nil), existingReplicas...)
		existingPlusOneNew = append(existingPlusOneNew, newReplica)
		replicaCandidates := existingPlusOneNew
		// If we can, filter replicas as we would if we were actually removing one.
		// If we can't (e.g. because we're the leaseholder but not the raft leader),
		// it's better to simulate the removal with the info that we do have than to
		// assume that the rebalance is ok (#20241).
		if raftStatus != nil && raftStatus.Progress != nil {
			replicaCandidates = simulateFilterUnremovableReplicas(
				ctx, raftStatus, replicaCandidates, newReplica.ReplicaID)
		}
		if len(replicaCandidates) == 0 {
			// No existing replicas are suitable to remove.
			log.VEventf(ctx, 2, "not rebalancing to s%d because there are no existing "+
				"replicas that can be removed", target.store.StoreID)
			return zero, zero, "", false
		}

		var removeDetails string
		var err error
		removeReplica, removeDetails, err = a.simulateRemoveTarget(
			ctx,
			target.store.StoreID,
			zone,
			replicaCandidates,
			existingPlusOneNew,
			rangeUsageInfo,
		)
		if err != nil {
			log.Warningf(ctx, "simulating RemoveTarget failed: %+v", err)
			return zero, zero, "", false
		}
		if target.store.StoreID != removeReplica.StoreID {
			// Successfully populated these variables
			_, _ = target, removeReplica
			break
		}

		log.VEventf(ctx, 2, "not rebalancing to s%d because we'd immediately remove it: %s",
			target.store.StoreID, removeDetails)
	}

	// Compile the details entry that will be persisted into system.rangelog for
	// debugging/auditability purposes.
	dDetails := decisionDetails{
		Target:   target.compactString(options),
		Existing: existingCandidates.compactString(options),
	}
	detailsBytes, err := json.Marshal(dDetails)
	if err != nil {
		log.Warningf(ctx, "failed to marshal details for choosing rebalance target: %+v", err)
	}

	addTarget := roachpb.ReplicationTarget{
		NodeID:  target.store.Node.NodeID,
		StoreID: target.store.StoreID,
	}
	removeTarget := roachpb.ReplicationTarget{
		NodeID:  removeReplica.NodeID,
		StoreID: removeReplica.StoreID,
	}
	return addTarget, removeTarget, string(detailsBytes), true
}

func (a *Allocator) scorerOptions() scorerOptions {
	return scorerOptions{
		deterministic:           a.storePool.deterministic,
		rangeRebalanceThreshold: rangeRebalanceThreshold.Get(&a.storePool.st.SV),
	}
}

// TransferLeaseTarget returns a suitable replica to transfer the range lease
// to from the provided list. It excludes the current lease holder replica
// unless asked to do otherwise by the checkTransferLeaseSource parameter.
func (a *Allocator) TransferLeaseTarget(
	ctx context.Context,
	zone *zonepb.ZoneConfig,
	existing []roachpb.ReplicaDescriptor,
	leaseStoreID roachpb.StoreID,
	stats *replicaStats,
	checkTransferLeaseSource bool,
	checkCandidateFullness bool,
	alwaysAllowDecisionWithoutStats bool,
) roachpb.ReplicaDescriptor {
	sl, _, _ := a.storePool.getStoreList(storeFilterNone)
	sl = sl.filter(zone.Constraints)

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

	// Determine which store(s) is preferred based on user-specified preferences.
	// If any stores match, only consider those stores as candidates. If only one
	// store matches, it's where the lease should be (unless the preferred store
	// is the current one and checkTransferLeaseSource is false).
	var preferred []roachpb.ReplicaDescriptor
	if checkTransferLeaseSource {
		preferred = a.preferredLeaseholders(zone, existing)
	} else {
		// TODO(a-robinson): Should we just always remove the source store from
		// existing when checkTransferLeaseSource is false? I'd do it now, but
		// it's too big a change to make right before a major release.
		var candidates []roachpb.ReplicaDescriptor
		for _, repl := range existing {
			if repl.StoreID != leaseStoreID {
				candidates = append(candidates, repl)
			}
		}
		preferred = a.preferredLeaseholders(zone, candidates)
	}
	if len(preferred) == 1 {
		if preferred[0].StoreID == leaseStoreID {
			return roachpb.ReplicaDescriptor{}
		}
		return preferred[0]
	} else if len(preferred) > 1 {
		// If the current leaseholder is not preferred, set checkTransferLeaseSource
		// to false to motivate the below logic to transfer the lease.
		existing = preferred
		if !storeHasReplica(leaseStoreID, preferred) {
			checkTransferLeaseSource = false
		}
	}

	// Only consider live, non-draining replicas.
	existing, _ = a.storePool.liveAndDeadReplicas(existing)

	// Short-circuit if there are no valid targets out there.
	if len(existing) == 0 || (len(existing) == 1 && existing[0].StoreID == leaseStoreID) {
		log.VEventf(ctx, 2, "no lease transfer target found")
		return roachpb.ReplicaDescriptor{}
	}

	// Try to pick a replica to transfer the lease to while also determining
	// whether we actually should be transferring the lease. The transfer
	// decision is only needed if we've been asked to check the source.
	transferDec, repl := a.shouldTransferLeaseUsingStats(
		ctx, sl, source, existing, stats, nil,
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
	var bestOption roachpb.ReplicaDescriptor
	bestOptionLeaseCount := int32(math.MaxInt32)
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
		} else if storeDesc.Capacity.LeaseCount < bestOptionLeaseCount {
			bestOption = repl
			bestOptionLeaseCount = storeDesc.Capacity.LeaseCount
		}
	}
	if len(candidates) == 0 {
		// If we aren't supposed to be considering the current leaseholder (e.g.
		// because we need to remove this replica for some reason), return
		// our best option if we otherwise wouldn't want to do anything.
		if !checkTransferLeaseSource {
			return bestOption
		}
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
	zone *zonepb.ZoneConfig,
	existing []roachpb.ReplicaDescriptor,
	leaseStoreID roachpb.StoreID,
	stats *replicaStats,
) bool {
	source, ok := a.storePool.getStoreDescriptor(leaseStoreID)
	if !ok {
		return false
	}

	// Determine which store(s) is preferred based on user-specified preferences.
	// If any stores match, only consider those stores as options. If only one
	// store matches, it's where the lease should be.
	preferred := a.preferredLeaseholders(zone, existing)
	if len(preferred) == 1 {
		return preferred[0].StoreID != leaseStoreID
	} else if len(preferred) > 1 {
		existing = preferred
		// If the current leaseholder isn't one of the preferred stores, then we
		// should try to transfer the lease.
		if !storeHasReplica(leaseStoreID, existing) {
			return true
		}
	}

	sl, _, _ := a.storePool.getStoreList(storeFilterNone)
	sl = sl.filter(zone.Constraints)
	log.VEventf(ctx, 3, "ShouldTransferLease (lease-holder=%d):\n%s", leaseStoreID, sl)

	// Only consider live, non-draining replicas.
	existing, _ = a.storePool.liveAndDeadReplicas(existing)

	// Short-circuit if there are no valid targets out there.
	if len(existing) == 0 || (len(existing) == 1 && existing[0].StoreID == source.StoreID) {
		return false
	}

	transferDec, _ := a.shouldTransferLeaseUsingStats(ctx, sl, source, existing, stats, nil)
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

	log.VEventf(ctx, 3, "ShouldTransferLease decision (lease-holder=%d): %t", leaseStoreID, result)
	return result
}

func (a Allocator) followTheWorkloadPrefersLocal(
	ctx context.Context,
	sl StoreList,
	source roachpb.StoreDescriptor,
	candidate roachpb.StoreID,
	existing []roachpb.ReplicaDescriptor,
	stats *replicaStats,
) bool {
	adjustments := make(map[roachpb.StoreID]float64)
	decision, _ := a.shouldTransferLeaseUsingStats(ctx, sl, source, existing, stats, adjustments)
	if decision == decideWithoutStats {
		return false
	}
	adjustment := adjustments[candidate]
	if adjustment > baseLoadBasedLeaseRebalanceThreshold {
		log.VEventf(ctx, 3,
			"s%d is a better fit than s%d due to follow-the-workload (score: %.2f; threshold: %.2f)",
			source.StoreID, candidate, adjustment, baseLoadBasedLeaseRebalanceThreshold)
		return true
	}
	return false
}

func (a Allocator) shouldTransferLeaseUsingStats(
	ctx context.Context,
	sl StoreList,
	source roachpb.StoreDescriptor,
	existing []roachpb.ReplicaDescriptor,
	stats *replicaStats,
	rebalanceAdjustments map[roachpb.StoreID]float64,
) (transferDecision, roachpb.ReplicaDescriptor) {
	// Only use load-based rebalancing if it's enabled and we have both
	// stats and locality information to base our decision on.
	if stats == nil || !enableLoadBasedLeaseRebalancing.Get(&a.storePool.st.SV) {
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
			log.Errorf(ctx, "unable to parse locality string %q: %+v", requestLocalityStr, err)
			continue
		}
		for nodeID, replicaLocality := range replicaLocalities {
			// Add weights to each replica based on the number of requests from
			// that replica's locality and neighboring localities.
			replicaWeights[nodeID] += (1 - replicaLocality.DiversityScore(requestLocality)) * qps
		}
	}

	log.VEventf(ctx, 1,
		"shouldTransferLease qpsStats: %+v, replicaLocalities: %+v, replicaWeights: %+v",
		qpsStats, replicaLocalities, replicaWeights)
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
			log.Errorf(ctx, "missing address for n%d: %+v", repl.NodeID, err)
			continue
		}
		remoteLatency, ok := a.nodeLatencyFn(addr.String())
		if !ok {
			continue
		}

		remoteWeight := math.Max(minReplicaWeight, replicaWeights[repl.NodeID])
		replScore, rebalanceAdjustment := loadBasedLeaseRebalanceScore(
			ctx, a.storePool.st, remoteWeight, remoteLatency, storeDesc, sourceWeight, source, sl.candidateLeases.mean)
		if replScore > bestReplScore {
			bestReplScore = replScore
			bestRepl = repl
		}
		if rebalanceAdjustments != nil {
			rebalanceAdjustments[repl.StoreID] = rebalanceAdjustment
		}
	}

	if bestReplScore > 0 {
		return shouldTransfer, bestRepl
	}

	// Return the best replica even in cases where transferring is not advised in
	// order to support forced lease transfers, such as when removing a replica or
	// draining all leases before shutdown.
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
//   a cluster setting.
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
//
// Returns a total score for the replica that takes into account the number of
// leases already on each store. Also returns the raw "adjustment" value that's
// purely based on replica weights and latency in order for the caller to
// determine how large a role the user's workload played in the decision.  The
// adjustment value is positive if the remote store is preferred for load-based
// reasons or negative if the local store is preferred. The magnitude depends
// on the difference in load and the latency between the nodes.
//
// TODO(a-robinson): Should this be changed to avoid even thinking about lease
// counts now that we try to spread leases and replicas based on QPS? As is it
// may fight back a little bit against store-level QPS-based rebalancing.
func loadBasedLeaseRebalanceScore(
	ctx context.Context,
	st *cluster.Settings,
	remoteWeight float64,
	remoteLatency time.Duration,
	remoteStore roachpb.StoreDescriptor,
	sourceWeight float64,
	source roachpb.StoreDescriptor,
	meanLeases float64,
) (int32, float64) {
	remoteLatencyMillis := float64(remoteLatency) / float64(time.Millisecond)
	rebalanceAdjustment :=
		leaseRebalancingAggressiveness.Get(&st.SV) * 0.1 * math.Log10(remoteWeight/sourceWeight) * math.Log1p(remoteLatencyMillis)
	// Start with twice the base rebalance threshold in order to fight more
	// strongly against thrashing caused by small variances in the distribution
	// of request weights.
	rebalanceThreshold := baseLoadBasedLeaseRebalanceThreshold - rebalanceAdjustment

	overfullLeaseThreshold := int32(math.Ceil(meanLeases * (1 + rebalanceThreshold)))
	overfullScore := source.Capacity.LeaseCount - overfullLeaseThreshold
	underfullLeaseThreshold := int32(math.Floor(meanLeases * (1 - rebalanceThreshold)))
	underfullScore := underfullLeaseThreshold - remoteStore.Capacity.LeaseCount
	totalScore := overfullScore + underfullScore

	log.VEventf(ctx, 1,
		"node: %d, sourceWeight: %.2f, remoteWeight: %.2f, remoteLatency: %v, "+
			"rebalanceThreshold: %.2f, meanLeases: %.2f, sourceLeaseCount: %d, overfullThreshold: %d, "+
			"remoteLeaseCount: %d, underfullThreshold: %d, totalScore: %d",
		remoteStore.Node.NodeID, sourceWeight, remoteWeight, remoteLatency,
		rebalanceThreshold, meanLeases, source.Capacity.LeaseCount, overfullLeaseThreshold,
		remoteStore.Capacity.LeaseCount, underfullLeaseThreshold, totalScore,
	)
	return totalScore, rebalanceAdjustment
}

func (a Allocator) shouldTransferLeaseWithoutStats(
	ctx context.Context,
	sl StoreList,
	source roachpb.StoreDescriptor,
	existing []roachpb.ReplicaDescriptor,
) bool {
	// TODO(a-robinson): Should we disable this behavior when load-based lease
	// rebalancing is enabled? In happy cases it's nice to keep this working
	// to even out the number of leases in addition to the number of replicas,
	// but it's certainly a blunt instrument that could undo what we want.

	// Allow lease transfer if we're above the overfull threshold, which is
	// mean*(1+leaseRebalanceThreshold).
	overfullLeaseThreshold := int32(math.Ceil(sl.candidateLeases.mean * (1 + leaseRebalanceThreshold)))
	minOverfullThreshold := int32(math.Ceil(sl.candidateLeases.mean + 5))
	if overfullLeaseThreshold < minOverfullThreshold {
		overfullLeaseThreshold = minOverfullThreshold
	}
	if source.Capacity.LeaseCount > overfullLeaseThreshold {
		return true
	}

	if float64(source.Capacity.LeaseCount) > sl.candidateLeases.mean {
		underfullLeaseThreshold := int32(math.Ceil(sl.candidateLeases.mean * (1 - leaseRebalanceThreshold)))
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

func (a Allocator) preferredLeaseholders(
	zone *zonepb.ZoneConfig, existing []roachpb.ReplicaDescriptor,
) []roachpb.ReplicaDescriptor {
	// Go one preference at a time. As soon as we've found replicas that match a
	// preference, we don't need to look at the later preferences, because
	// they're meant to be ordered by priority.
	for _, preference := range zone.LeasePreferences {
		var preferred []roachpb.ReplicaDescriptor
		for _, repl := range existing {
			// TODO(a-robinson): Do all these lookups at once, up front? We could
			// easily be passing a slice of StoreDescriptors around all the Allocator
			// functions instead of ReplicaDescriptors.
			storeDesc, ok := a.storePool.getStoreDescriptor(repl.StoreID)
			if !ok {
				continue
			}
			if constraint.ConjunctionsCheck(storeDesc, preference.Constraints) {
				preferred = append(preferred, repl)
			}
		}
		if len(preferred) > 0 {
			return preferred
		}
	}
	return nil
}

// computeQuorum computes the quorum value for the given number of nodes.
func computeQuorum(nodes int) int {
	return (nodes / 2) + 1
}

// filterBehindReplicas removes any "behind" replicas from the supplied
// slice. A "behind" replica is one which is not at or past the quorum commit
// index.
func filterBehindReplicas(
	ctx context.Context, raftStatus *raft.Status, replicas []roachpb.ReplicaDescriptor,
) []roachpb.ReplicaDescriptor {
	if raftStatus == nil || len(raftStatus.Progress) == 0 {
		// raftStatus.Progress is only populated on the Raft leader which means we
		// won't be able to rebalance a lease away if the lease holder is not the
		// Raft leader. This is rare enough not to matter.
		return nil
	}
	candidates := make([]roachpb.ReplicaDescriptor, 0, len(replicas))
	for _, r := range replicas {
		if !replicaIsBehind(raftStatus, r.ReplicaID) {
			candidates = append(candidates, r)
		}
	}
	return candidates
}

func replicaIsBehind(raftStatus *raft.Status, replicaID roachpb.ReplicaID) bool {
	if raftStatus == nil || len(raftStatus.Progress) == 0 {
		return true
	}
	// NB: We use raftStatus.Commit instead of getQuorumIndex() because the
	// latter can return a value that is less than the commit index. This is
	// useful for Raft log truncation which sometimes wishes to keep those
	// earlier indexes, but not appropriate for determining which nodes are
	// behind the actual commit index of the range.
	if progress, ok := raftStatus.Progress[uint64(replicaID)]; ok {
		if uint64(replicaID) == raftStatus.Lead ||
			(progress.State == tracker.StateReplicate &&
				progress.Match >= raftStatus.Commit) {
			return false
		}
	}
	return true
}

// simulateFilterUnremovableReplicas removes any unremovable replicas from the
// supplied slice. Unlike filterUnremovableReplicas, brandNewReplicaID is
// considered up-to-date (and thus can participate in quorum), but is not
// considered a candidate for removal.
func simulateFilterUnremovableReplicas(
	ctx context.Context,
	raftStatus *raft.Status,
	replicas []roachpb.ReplicaDescriptor,
	brandNewReplicaID roachpb.ReplicaID,
) []roachpb.ReplicaDescriptor {
	status := *raftStatus
	status.Progress[uint64(brandNewReplicaID)] = tracker.Progress{
		State: tracker.StateReplicate,
		Match: status.Commit,
	}
	return filterUnremovableReplicas(ctx, &status, replicas, brandNewReplicaID)
}

// filterUnremovableReplicas removes any unremovable replicas from the supplied
// slice. An unremovable replica is one which is a necessary part of the
// quorum that will result from removing 1 replica. We forgive brandNewReplicaID
// for being behind, since a new range can take a little while to catch up.
// This is important when we've just added a replica in order to rebalance to
// it (#17879).
func filterUnremovableReplicas(
	ctx context.Context,
	raftStatus *raft.Status,
	replicas []roachpb.ReplicaDescriptor,
	brandNewReplicaID roachpb.ReplicaID,
) []roachpb.ReplicaDescriptor {
	upToDateReplicas := filterBehindReplicas(ctx, raftStatus, replicas)
	oldQuorum := computeQuorum(len(replicas))
	if len(upToDateReplicas) < oldQuorum {
		// The number of up-to-date replicas is less than the old quorum. No
		// replicas can be removed. A below quorum range won't be able to process a
		// replica removal in any case. The logic here prevents any attempt to even
		// try the removal.
		return nil
	}

	newQuorum := computeQuorum(len(replicas) - 1)
	if len(upToDateReplicas) > newQuorum {
		// The number of up-to-date replicas is larger than the new quorum. Any
		// replica can be removed, though we want to filter out brandNewReplicaID.
		if brandNewReplicaID != 0 {
			candidates := make([]roachpb.ReplicaDescriptor, 0, len(replicas)-len(upToDateReplicas))
			for _, r := range replicas {
				if r.ReplicaID != brandNewReplicaID {
					candidates = append(candidates, r)
				}
			}
			return candidates
		}
		return replicas
	}

	// The number of up-to-date replicas is equal to the new quorum. Only allow
	// removal of behind replicas (except for brandNewReplicaID which is given a
	// free pass).
	candidates := make([]roachpb.ReplicaDescriptor, 0, len(replicas)-len(upToDateReplicas))
	necessary := func(r roachpb.ReplicaDescriptor) bool {
		if r.ReplicaID == brandNewReplicaID {
			return true
		}
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

func maxReplicaID(replicas []roachpb.ReplicaDescriptor) roachpb.ReplicaID {
	var max roachpb.ReplicaID
	for i := range replicas {
		if replicaID := replicas[i].ReplicaID; replicaID > max {
			max = replicaID
		}
	}
	return max
}
