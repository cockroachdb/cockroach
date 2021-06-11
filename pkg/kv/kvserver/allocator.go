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
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/tracker"
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
var enableLoadBasedLeaseRebalancing = settings.RegisterBoolSetting(
	"kv.allocator.load_based_lease_rebalancing.enabled",
	"set to enable rebalancing of range leases based on load and latency",
	true,
).WithPublic()

// leaseRebalancingAggressiveness enables users to tweak how aggressive their
// cluster is at moving leases towards the localities where the most requests
// are coming from. Settings lower than 1.0 will make the system less
// aggressive about moving leases toward requests than the default, while
// settings greater than 1.0 will cause more aggressive placement.
//
// Setting this to 0 effectively disables load-based lease rebalancing, and
// settings less than 0 are disallowed.
var leaseRebalancingAggressiveness = settings.RegisterFloatSetting(
	"kv.allocator.lease_rebalancing_aggressiveness",
	"set greater than 1.0 to rebalance leases toward load more aggressively, "+
		"or between 0 and 1.0 to be more conservative about rebalancing leases",
	1.0,
	settings.NonNegativeFloat,
)

// AllocatorAction enumerates the various replication adjustments that may be
// recommended by the allocator.
type AllocatorAction int

// These are the possible allocator actions.
const (
	_ AllocatorAction = iota
	AllocatorNoop
	AllocatorRemoveVoter
	AllocatorRemoveNonVoter
	AllocatorAddVoter
	AllocatorAddNonVoter
	AllocatorReplaceDeadVoter
	AllocatorReplaceDeadNonVoter
	AllocatorRemoveDeadVoter
	AllocatorRemoveDeadNonVoter
	AllocatorReplaceDecommissioningVoter
	AllocatorReplaceDecommissioningNonVoter
	AllocatorRemoveDecommissioningVoter
	AllocatorRemoveDecommissioningNonVoter
	AllocatorRemoveLearner
	AllocatorConsiderRebalance
	AllocatorRangeUnavailable
	AllocatorFinalizeAtomicReplicationChange
)

var allocatorActionNames = map[AllocatorAction]string{
	AllocatorNoop:                            "noop",
	AllocatorRemoveVoter:                     "remove voter",
	AllocatorRemoveNonVoter:                  "remove non-voter",
	AllocatorAddVoter:                        "add voter",
	AllocatorAddNonVoter:                     "add non-voter",
	AllocatorReplaceDeadVoter:                "replace dead voter",
	AllocatorReplaceDeadNonVoter:             "replace dead non-voter",
	AllocatorRemoveDeadVoter:                 "remove dead voter",
	AllocatorRemoveDeadNonVoter:              "remove dead non-voter",
	AllocatorReplaceDecommissioningVoter:     "replace decommissioning voter",
	AllocatorReplaceDecommissioningNonVoter:  "replace decommissioning non-voter",
	AllocatorRemoveDecommissioningVoter:      "remove decommissioning voter",
	AllocatorRemoveDecommissioningNonVoter:   "remove decommissioning non-voter",
	AllocatorRemoveLearner:                   "remove learner",
	AllocatorConsiderRebalance:               "consider rebalance",
	AllocatorRangeUnavailable:                "range unavailable",
	AllocatorFinalizeAtomicReplicationChange: "finalize conf change",
}

func (a AllocatorAction) String() string {
	return allocatorActionNames[a]
}

// Priority defines the priorities for various repair operations.
//
// NB: These priorities only influence the replicateQueue's understanding of
// which ranges are to be dealt with before others. In other words, these
// priorities don't influence the relative order of actions taken on a given
// range. Within a given range, the ordering of the various checks inside
// `Allocator.computeAction` determines which repair/rebalancing actions are
// taken before the others.
func (a AllocatorAction) Priority() float64 {
	switch a {
	case AllocatorFinalizeAtomicReplicationChange:
		return 12002
	case AllocatorRemoveLearner:
		return 12001
	case AllocatorReplaceDeadVoter:
		return 12000
	case AllocatorAddVoter:
		return 10000
	case AllocatorReplaceDecommissioningVoter:
		return 5000
	case AllocatorRemoveDeadVoter:
		return 1000
	case AllocatorRemoveDecommissioningVoter:
		return 900
	case AllocatorRemoveVoter:
		return 800
	case AllocatorReplaceDeadNonVoter:
		return 700
	case AllocatorAddNonVoter:
		return 600
	case AllocatorReplaceDecommissioningNonVoter:
		return 500
	case AllocatorRemoveDeadNonVoter:
		return 400
	case AllocatorRemoveDecommissioningNonVoter:
		return 300
	case AllocatorRemoveNonVoter:
		return 200
	case AllocatorConsiderRebalance, AllocatorRangeUnavailable, AllocatorNoop:
		return 0
	default:
		panic(fmt.Sprintf("unknown AllocatorAction: %s", a))
	}
}

type targetReplicaType int

const (
	_ targetReplicaType = iota
	voterTarget
	nonVoterTarget
)

// AddChangeType returns the roachpb.ReplicaChangeType corresponding to the
// given targetReplicaType.
//
// TODO(aayush): Clean up usages of ADD_{NON_}VOTER. Use
// targetReplicaType.{Add,Remove}ChangeType methods wherever possible.
func (t targetReplicaType) AddChangeType() roachpb.ReplicaChangeType {
	switch t {
	case voterTarget:
		return roachpb.ADD_VOTER
	case nonVoterTarget:
		return roachpb.ADD_NON_VOTER
	default:
		panic(fmt.Sprintf("unknown targetReplicaType %d", t))
	}
}

// RemoveChangeType returns the roachpb.ReplicaChangeType corresponding to the
// given targetReplicaType.
func (t targetReplicaType) RemoveChangeType() roachpb.ReplicaChangeType {
	switch t {
	case voterTarget:
		return roachpb.REMOVE_VOTER
	case nonVoterTarget:
		return roachpb.REMOVE_NON_VOTER
	default:
		panic(fmt.Sprintf("unknown targetReplicaType %d", t))
	}
}

func (t targetReplicaType) String() string {
	switch t {
	case voterTarget:
		return "voter"
	case nonVoterTarget:
		return "non-voter"
	default:
		panic(fmt.Sprintf("unknown targetReplicaType %d", t))
	}
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
	constraints           []zonepb.ConstraintsConjunction
	voterConstraints      []zonepb.ConstraintsConjunction
	existingVoterCount    int
	existingNonVoterCount int
	aliveStores           int
	throttledStores       int
}

func (ae *allocatorError) Error() string {
	var existingVoterStr string
	if ae.existingVoterCount == 1 {
		existingVoterStr = "1 already has a voter"
	} else {
		existingVoterStr = fmt.Sprintf("%d already have a voter", ae.existingVoterCount)
	}

	var existingNonVoterStr string
	if ae.existingNonVoterCount == 1 {
		existingNonVoterStr = "1 already has a non-voter"
	} else {
		existingNonVoterStr = fmt.Sprintf("%d already have a non-voter", ae.existingNonVoterCount)
	}

	var baseMsg string
	if ae.throttledStores != 0 {
		baseMsg = fmt.Sprintf(
			"0 of %d live stores are able to take a new replica for the range (%d throttled, %s, %s)",
			ae.aliveStores, ae.throttledStores, existingVoterStr, existingNonVoterStr)
	} else {
		baseMsg = fmt.Sprintf(
			"0 of %d live stores are able to take a new replica for the range (%s, %s)",
			ae.aliveStores, existingVoterStr, existingNonVoterStr)
	}

	if len(ae.constraints) == 0 && len(ae.voterConstraints) == 0 {
		if ae.throttledStores > 0 {
			return baseMsg
		}
		return baseMsg + "; likely not enough nodes in cluster"
	}

	var b strings.Builder
	b.WriteString(baseMsg)
	b.WriteString("; replicas must match constraints [")
	for i := range ae.constraints {
		if i > 0 {
			b.WriteByte(' ')
		}
		b.WriteByte('{')
		b.WriteString(ae.constraints[i].String())
		b.WriteByte('}')
	}
	b.WriteString("]")

	b.WriteString("; voting replicas must match voter_constraints [")
	for i := range ae.voterConstraints {
		if i > 0 {
			b.WriteByte(' ')
		}
		b.WriteByte('{')
		b.WriteString(ae.voterConstraints[i].String())
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

// GetNeededVoters calculates the number of voters a range should have given its
// zone config and the number of nodes available for up-replication (i.e. not
// decommissioning).
func GetNeededVoters(zoneConfigVoterCount int32, clusterNodes int) int {
	numZoneReplicas := int(zoneConfigVoterCount)
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

// GetNeededNonVoters calculates the number of non-voters a range should have
// given the number of voting replicas the range has and the number of nodes
// available for up-replication.
//
// NB: This method assumes that we have exactly as many voters as we need, since
// this method should only be consulted after voting replicas have been
// upreplicated / rebalanced off of dead/decommissioning nodes.
func GetNeededNonVoters(numVoters, zoneConfigNonVoterCount, clusterNodes int) int {
	need := zoneConfigNonVoterCount
	if clusterNodes-numVoters < need {
		// We only need non-voting replicas for the nodes that do not have a voting
		// replica.
		need = clusterNodes - numVoters
	}
	if need < 0 {
		need = 0 // Must be non-negative.
	}
	return need
}

// ComputeAction determines the exact operation needed to repair the
// supplied range, as governed by the supplied zone configuration. It
// returns the required action that should be taken and a priority.
func (a *Allocator) ComputeAction(
	ctx context.Context, zone *zonepb.ZoneConfig, desc *roachpb.RangeDescriptor,
) (action AllocatorAction, priority float64) {
	if a.storePool == nil {
		// Do nothing if storePool is nil for some unittests.
		action = AllocatorNoop
		return action, action.Priority()
	}

	if desc.Replicas().InAtomicReplicationChange() {
		// With a similar reasoning to the learner branch below, if we're in a
		// joint configuration the top priority is to leave it before we can
		// even think about doing anything else.
		action = AllocatorFinalizeAtomicReplicationChange
		return action, action.Priority()
	}

	if learners := desc.Replicas().LearnerDescriptors(); len(learners) > 0 {
		// Seeing a learner replica at this point is unexpected because learners are
		// a short-lived (ish) transient state in a learner+snapshot+voter cycle,
		// which is always done atomically. Only two places could have added a
		// learner: the replicate queue or AdminChangeReplicas request.
		//
		// The replicate queue only operates on leaseholders, which means that only
		// one node at a time is operating on a given range except in rare cases
		// (old leaseholder could start the operation, and a new leaseholder steps
		// up and also starts an overlapping operation). Combined with the above
		// atomicity, this means that if the replicate queue sees a learner, either
		// the node that was adding it crashed somewhere in the
		// learner+snapshot+voter cycle and we're the new leaseholder or we caught a
		// race.
		//
		// In the first case, we could assume the node that was adding it knew what
		// it was doing and finish the addition. Or we could leave it and do higher
		// priority operations first if there are any. However, this comes with code
		// complexity and concept complexity (computing old vs new quorum sizes
		// becomes ambiguous, the learner isn't in the quorum but it likely will be
		// soon, so do you count it?). Instead, we do the simplest thing and remove
		// it before doing any other operations to the range. We'll revisit this
		// decision if and when the complexity becomes necessary.
		//
		// If we get the race where AdminChangeReplicas is adding a replica and the
		// queue happens to run during the snapshot, this will remove the learner
		// and AdminChangeReplicas will notice either during the snapshot transfer
		// or when it tries to promote the learner to a voter. AdminChangeReplicas
		// should retry.
		//
		// On the other hand if we get the race where a leaseholder starts adding a
		// replica in the replicate queue and during this loses its lease, it should
		// probably not retry.
		//
		// TODO(dan): Since this goes before anything else, the priority here should
		// be influenced by whatever operations would happen right after the learner
		// is removed. In the meantime, we don't want to block something important
		// from happening (like addDeadReplacementVoterPriority) by queueing this at
		// a low priority so until this TODO is done, keep
		// removeLearnerReplicaPriority as the highest priority.
		action = AllocatorRemoveLearner
		return action, action.Priority()
	}

	return a.computeAction(ctx, zone, desc.Replicas().VoterDescriptors(),
		desc.Replicas().NonVoterDescriptors())

}

func (a *Allocator) computeAction(
	ctx context.Context,
	zone *zonepb.ZoneConfig,
	voterReplicas []roachpb.ReplicaDescriptor,
	nonVoterReplicas []roachpb.ReplicaDescriptor,
) (action AllocatorAction, adjustedPriority float64) {
	// NB: The ordering of the checks in this method is intentional. The order in
	// which these actions are returned by this method determines the relative
	// priority of the actions taken on a given range. We want this to be
	// symmetric with regards to the priorities defined at the top of this file
	// (which influence the replicateQueue's decision of which range it'll pick to
	// repair/rebalance before the others).
	//
	// In broad strokes, we first handle all voting replica-based actions and then
	// the actions pertaining to non-voting replicas. Within each replica set, we
	// first handle operations that correspond to repairing/recovering the range.
	// After that we handle rebalancing related actions, followed by removal
	// actions.
	haveVoters := len(voterReplicas)
	decommissioningVoters := a.storePool.decommissioningReplicas(voterReplicas)
	// Node count including dead nodes but excluding
	// decommissioning/decommissioned nodes.
	clusterNodes := a.storePool.ClusterNodeCount()
	neededVoters := GetNeededVoters(zone.GetNumVoters(), clusterNodes)
	desiredQuorum := computeQuorum(neededVoters)
	quorum := computeQuorum(haveVoters)

	// TODO(aayush): When haveVoters < neededVoters but we don't have quorum to
	// actually execute the addition of a new replica, we should be returning a
	// AllocatorRangeUnavailable.
	if haveVoters < neededVoters {
		// Range is under-replicated, and should add an additional voter.
		// Priority is adjusted by the difference between the current voter
		// count and the quorum of the desired voter count.
		action = AllocatorAddVoter
		adjustedPriority = action.Priority() + float64(desiredQuorum-haveVoters)
		log.VEventf(ctx, 3, "%s - missing voter need=%d, have=%d, priority=%.2f",
			action, neededVoters, haveVoters, adjustedPriority)
		return action, adjustedPriority
	}

	liveVoters, deadVoters := a.storePool.liveAndDeadReplicas(voterReplicas)

	if len(liveVoters) < quorum {
		// Do not take any replacement/removal action if we do not have a quorum of
		// live voters. If we're correctly assessing the unavailable state of the
		// range, we also won't be able to add replicas as we try above, but hope
		// springs eternal.
		action = AllocatorRangeUnavailable
		log.VEventf(ctx, 1, "unable to take action - live voters %v don't meet quorum of %d",
			liveVoters, quorum)
		return action, action.Priority()
	}

	if haveVoters == neededVoters && len(deadVoters) > 0 {
		// Range has dead voter(s). We should up-replicate to add another before
		// before removing the dead one. This can avoid permanent data loss in cases
		// where the node is only temporarily dead, but we remove it from the range
		// and lose a second node before we can up-replicate (#25392).
		action = AllocatorReplaceDeadVoter
		log.VEventf(ctx, 3, "%s - replacement for %d dead voters priority=%.2f",
			action, len(deadVoters), action.Priority())
		return action, action.Priority()
	}

	if haveVoters == neededVoters && len(decommissioningVoters) > 0 {
		// Range has decommissioning voter(s), which should be replaced.
		action = AllocatorReplaceDecommissioningVoter
		log.VEventf(ctx, 3, "%s - replacement for %d decommissioning voters priority=%.2f",
			action, len(decommissioningVoters), action.Priority())
		return action, action.Priority()
	}

	// Voting replica removal actions follow.
	// TODO(aayush): There's an additional case related to dead voters that we
	// should handle above. If there are one or more dead replicas, have < need,
	// and there are no available stores to up-replicate to, then we should try to
	// remove the dead replica(s) to get down to an odd number of replicas.
	if len(deadVoters) > 0 {
		// The range has dead replicas, which should be removed immediately.
		action = AllocatorRemoveDeadVoter
		adjustedPriority = action.Priority() + float64(quorum-len(liveVoters))
		log.VEventf(ctx, 3, "%s - dead=%d, live=%d, quorum=%d, priority=%.2f",
			action, len(deadVoters), len(liveVoters), quorum, adjustedPriority)
		return action, adjustedPriority
	}

	if len(decommissioningVoters) > 0 {
		// Range is over-replicated, and has a decommissioning voter which
		// should be removed.
		action = AllocatorRemoveDecommissioningVoter
		log.VEventf(ctx, 3,
			"%s - need=%d, have=%d, num_decommissioning=%d, priority=%.2f",
			action, neededVoters, haveVoters, len(decommissioningVoters), action.Priority())
		return action, action.Priority()
	}

	if haveVoters > neededVoters {
		// Range is over-replicated, and should remove a voter.
		// Ranges with an even number of voters get extra priority because
		// they have a more fragile quorum.
		action = AllocatorRemoveVoter
		adjustedPriority = action.Priority() - float64(haveVoters%2)
		log.VEventf(ctx, 3, "%s - need=%d, have=%d, priority=%.2f", action, neededVoters,
			haveVoters, adjustedPriority)
		return action, adjustedPriority
	}

	// Non-voting replica actions follow.
	//
	// Non-voting replica addition / replacement.
	haveNonVoters := len(nonVoterReplicas)
	neededNonVoters := GetNeededNonVoters(haveVoters, int(zone.GetNumNonVoters()), clusterNodes)
	if haveNonVoters < neededNonVoters {
		action = AllocatorAddNonVoter
		log.VEventf(ctx, 3, "%s - missing non-voter need=%d, have=%d, priority=%.2f",
			action, neededNonVoters, haveNonVoters, action.Priority())
		return action, action.Priority()
	}

	liveNonVoters, deadNonVoters := a.storePool.liveAndDeadReplicas(nonVoterReplicas)
	if haveNonVoters == neededNonVoters && len(deadNonVoters) > 0 {
		// The range has non-voter(s) on a dead node that we should replace.
		action = AllocatorReplaceDeadNonVoter
		log.VEventf(ctx, 3, "%s - replacement for %d dead non-voters priority=%.2f",
			action, len(deadNonVoters), action.Priority())
		return action, action.Priority()
	}

	decommissioningNonVoters := a.storePool.decommissioningReplicas(nonVoterReplicas)
	if haveNonVoters == neededNonVoters && len(decommissioningNonVoters) > 0 {
		// The range has non-voter(s) on a decommissioning node that we should
		// replace.
		action = AllocatorReplaceDecommissioningNonVoter
		log.VEventf(ctx, 3, "%s - replacement for %d decommissioning non-voters priority=%.2f",
			action, len(decommissioningNonVoters), action.Priority())
		return action, action.Priority()
	}

	// Non-voting replica removal.
	if len(deadNonVoters) > 0 {
		// The range is over-replicated _and_ has non-voter(s) on a dead node. We'll
		// just remove these.
		action = AllocatorRemoveDeadNonVoter
		log.VEventf(ctx, 3, "%s - dead=%d, live=%d, priority=%.2f",
			action, len(deadNonVoters), len(liveNonVoters), action.Priority())
		return action, action.Priority()
	}

	if len(decommissioningNonVoters) > 0 {
		// The range is over-replicated _and_ has non-voter(s) on a decommissioning
		// node. We'll just remove these.
		action = AllocatorRemoveDecommissioningNonVoter
		log.VEventf(ctx, 3,
			"%s - need=%d, have=%d, num_decommissioning=%d, priority=%.2f",
			action, neededNonVoters, haveNonVoters, len(decommissioningNonVoters), action.Priority())
		return action, action.Priority()
	}

	if haveNonVoters > neededNonVoters {
		// The range is simply over-replicated and should remove a non-voter.
		action = AllocatorRemoveNonVoter
		log.VEventf(ctx, 3, "%s - need=%d, have=%d, priority=%.2f", action,
			neededNonVoters, haveNonVoters, action.Priority())
		return action, action.Priority()
	}

	// Nothing needs to be done, but we may want to rebalance.
	action = AllocatorConsiderRebalance
	return action, action.Priority()
}

// getReplicasForDiversityCalc returns the set of replica descriptors that
// should be used for computing the diversity scores for a target when
// allocating/removing/rebalancing a replica of `targetType`.
func getReplicasForDiversityCalc(
	targetType targetReplicaType, existingVoters, allExistingReplicas []roachpb.ReplicaDescriptor,
) []roachpb.ReplicaDescriptor {
	switch t := targetType; t {
	case voterTarget:
		// When computing the "diversity score" for a given store for a voting
		// replica allocation/rebalance/removal, we consider the localities of only
		// the stores that contain a voting replica for the range.
		//
		// Note that if we were to consider all stores that have any kind of replica
		// for the range, voting replica allocation would be disincentivized to pick
		// stores that (partially or fully) share locality hierarchies with stores
		// that contain a non-voting replica. This is undesirable because this could
		// inadvertently reduce the fault-tolerance of the range in cases like the
		// following:
		//
		// Consider 3 regions (A, B, C), each with 2 AZs. Suppose that regions A and
		// B have a voting replica each, whereas region C has a non-voting replica.
		// In cases like these, we would want region C to be picked over regions A
		// and B for allocating a new third voting replica since that improves our
		// fault tolerance to the greatest degree.
		// In the counterfactual (i.e. if we were to compute diversity scores based
		// off of all `existingReplicas`), regions A, B, and C would all be equally
		// likely to get a new voting replica.
		return existingVoters
	case nonVoterTarget:
		return allExistingReplicas
	default:
		panic(fmt.Sprintf("unsupported targetReplicaType: %v", t))
	}
}

type decisionDetails struct {
	Target   string
	Existing string `json:",omitempty"`
}

func (a *Allocator) allocateTarget(
	ctx context.Context,
	zone *zonepb.ZoneConfig,
	existingVoters, existingNonVoters []roachpb.ReplicaDescriptor,
	targetType targetReplicaType,
) (*roachpb.StoreDescriptor, string, error) {
	candidateStoreList, aliveStoreCount, throttled := a.storePool.getStoreList(storeFilterThrottled)

	target, details := a.allocateTargetFromList(
		ctx,
		candidateStoreList,
		zone,
		existingVoters,
		existingNonVoters,
		a.scorerOptions(),
		// When allocating a *new* replica, we explicitly disregard nodes with any
		// existing replicas. This is important for multi-store scenarios as
		// otherwise, stores on the nodes that have existing replicas are simply
		// discouraged via the diversity heuristic. We want to entirely avoid
		// allocating multiple replicas onto different stores of the same node.
		false, /* allowMultipleReplsPerNode */
		targetType,
	)
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
		voterConstraints:      zone.VoterConstraints,
		constraints:           zone.Constraints,
		existingVoterCount:    len(existingVoters),
		existingNonVoterCount: len(existingNonVoters),
		aliveStores:           aliveStoreCount,
		throttledStores:       len(throttled),
	}
}

// AllocateVoter returns a suitable store for a new allocation of a voting
// replica with the required attributes. Nodes already accommodating existing
// voting replicas are ruled out as targets.
func (a *Allocator) AllocateVoter(
	ctx context.Context,
	zone *zonepb.ZoneConfig,
	existingVoters, existingNonVoters []roachpb.ReplicaDescriptor,
) (*roachpb.StoreDescriptor, string, error) {
	return a.allocateTarget(ctx, zone, existingVoters, existingNonVoters, voterTarget)
}

// AllocateNonVoter returns a suitable store for a new allocation of a
// non-voting replica with the required attributes. Nodes already accommodating
// _any_ existing replicas are ruled out as targets.
func (a *Allocator) AllocateNonVoter(
	ctx context.Context,
	zone *zonepb.ZoneConfig,
	existingVoters, existingNonVoters []roachpb.ReplicaDescriptor,
) (*roachpb.StoreDescriptor, string, error) {
	return a.allocateTarget(ctx, zone, existingVoters, existingNonVoters, nonVoterTarget)
}

func (a *Allocator) allocateTargetFromList(
	ctx context.Context,
	candidateStores StoreList,
	zone *zonepb.ZoneConfig,
	existingVoters, existingNonVoters []roachpb.ReplicaDescriptor,
	options scorerOptions,
	allowMultipleReplsPerNode bool,
	targetType targetReplicaType,
) (*roachpb.StoreDescriptor, string) {
	existingReplicas := append(existingVoters, existingNonVoters...)
	analyzedOverallConstraints := constraint.AnalyzeConstraints(ctx, a.storePool.getStoreDescriptor,
		existingReplicas, *zone.NumReplicas, zone.Constraints)
	analyzedVoterConstraints := constraint.AnalyzeConstraints(ctx, a.storePool.getStoreDescriptor,
		existingVoters, zone.GetNumVoters(), zone.VoterConstraints)

	var constraintsChecker constraintsCheckFn
	switch t := targetType; t {
	case voterTarget:
		constraintsChecker = voterConstraintsCheckerForAllocation(
			analyzedOverallConstraints,
			analyzedVoterConstraints,
		)
	case nonVoterTarget:
		constraintsChecker = nonVoterConstraintsCheckerForAllocation(analyzedOverallConstraints)
	default:
		log.Fatalf(ctx, "unsupported targetReplicaType: %v", t)
	}

	// We'll consider the targets that have a non-voter as feasible
	// relocation/up-replication targets for existing/new voting replicas, since
	// we always want voter constraint conformance to take precedence over
	// non-voters. For instance, in cases where we can only satisfy constraints
	// for either 1 voter or 1 non-voter, we want the voter to be able to displace
	// the non-voter.
	existingReplicaSet := getReplicasForDiversityCalc(targetType, existingVoters, existingReplicas)
	candidates := rankedCandidateListForAllocation(
		ctx,
		candidateStores,
		constraintsChecker,
		existingReplicaSet,
		a.storePool.getLocalitiesByStore(existingReplicaSet),
		a.storePool.isStoreReadyForRoutineReplicaTransfer,
		allowMultipleReplsPerNode,
		options,
	)

	log.VEventf(ctx, 3, "allocate %s: %s", targetType, candidates)
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
	existingVoters []roachpb.ReplicaDescriptor,
	existingNonVoters []roachpb.ReplicaDescriptor,
	rangeUsageInfo RangeUsageInfo,
	targetType targetReplicaType,
) (roachpb.ReplicaDescriptor, string, error) {
	// Update statistics first
	// TODO(a-robinson): This could theoretically interfere with decisions made by other goroutines,
	// but as of October 2017 calls to the Allocator are mostly serialized by the ReplicateQueue
	// (with the main exceptions being Scatter and the status server's allocator debug endpoint).
	// Try to make this interfere less with other callers.
	switch t := targetType; t {
	case voterTarget:
		a.storePool.updateLocalStoreAfterRebalance(targetStore, rangeUsageInfo, roachpb.ADD_VOTER)
		defer a.storePool.updateLocalStoreAfterRebalance(
			targetStore,
			rangeUsageInfo,
			roachpb.REMOVE_VOTER,
		)
		log.VEventf(ctx, 3, "simulating which voter would be removed after adding s%d",
			targetStore)
		return a.RemoveVoter(ctx, zone, candidates, existingVoters, existingNonVoters)
	case nonVoterTarget:
		a.storePool.updateLocalStoreAfterRebalance(targetStore, rangeUsageInfo, roachpb.ADD_NON_VOTER)
		defer a.storePool.updateLocalStoreAfterRebalance(
			targetStore,
			rangeUsageInfo,
			roachpb.REMOVE_NON_VOTER,
		)
		log.VEventf(ctx, 3, "simulating which non-voter would be removed after adding s%d",
			targetStore)
		return a.RemoveNonVoter(ctx, zone, candidates, existingVoters, existingNonVoters)
	default:
		panic(fmt.Sprintf("unknown targetReplicaType: %s", t))
	}
}

func (a Allocator) removeTarget(
	ctx context.Context,
	zone *zonepb.ZoneConfig,
	candidates []roachpb.ReplicationTarget,
	existingVoters []roachpb.ReplicaDescriptor,
	existingNonVoters []roachpb.ReplicaDescriptor,
	targetType targetReplicaType,
) (roachpb.ReplicaDescriptor, string, error) {
	if len(candidates) == 0 {
		return roachpb.ReplicaDescriptor{}, "", errors.Errorf("must supply at least one" +
			" candidate replica to allocator.removeTarget()")
	}

	existingReplicas := append(existingVoters, existingNonVoters...)
	// Retrieve store descriptors for the provided candidates from the StorePool.
	candidateStoreIDs := make(roachpb.StoreIDSlice, len(candidates))
	for i, exist := range candidates {
		candidateStoreIDs[i] = exist.StoreID
	}
	candidateStoreList, _, _ := a.storePool.getStoreListFromIDs(candidateStoreIDs, storeFilterNone)
	analyzedOverallConstraints := constraint.AnalyzeConstraints(ctx, a.storePool.getStoreDescriptor,
		existingReplicas, *zone.NumReplicas, zone.Constraints)
	analyzedVoterConstraints := constraint.AnalyzeConstraints(ctx, a.storePool.getStoreDescriptor,
		existingVoters, zone.GetNumVoters(), zone.VoterConstraints)
	options := a.scorerOptions()

	var constraintsChecker constraintsCheckFn
	switch t := targetType; t {
	case voterTarget:
		// Voting replicas have to abide by both the overall `constraints` (which
		// apply to all replicas) and `voter_constraints` which apply only to voting
		// replicas.
		constraintsChecker = voterConstraintsCheckerForRemoval(
			analyzedOverallConstraints,
			analyzedVoterConstraints,
		)
	case nonVoterTarget:
		constraintsChecker = nonVoterConstraintsCheckerForRemoval(analyzedOverallConstraints)
	default:
		log.Fatalf(ctx, "unsupported targetReplicaType: %v", t)
	}

	replicaSetForDiversityCalc := getReplicasForDiversityCalc(targetType, existingVoters, existingReplicas)
	rankedCandidates := rankedCandidateListForRemoval(
		candidateStoreList,
		constraintsChecker,
		a.storePool.getLocalitiesByStore(replicaSetForDiversityCalc),
		options,
	)

	log.VEventf(ctx, 3, "remove %s: %s", targetType, rankedCandidates)
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

// RemoveVoter returns a suitable replica to remove from the provided replica
// set. It first attempts to randomly select a target from the set of stores
// that have greater than the average number of replicas. Failing that, it falls
// back to selecting a random target from any of the existing voting replicas.
func (a Allocator) RemoveVoter(
	ctx context.Context,
	zone *zonepb.ZoneConfig,
	voterCandidates []roachpb.ReplicaDescriptor,
	existingVoters []roachpb.ReplicaDescriptor,
	existingNonVoters []roachpb.ReplicaDescriptor,
) (roachpb.ReplicaDescriptor, string, error) {
	return a.removeTarget(
		ctx,
		zone,
		roachpb.MakeReplicaSet(voterCandidates).ReplicationTargets(),
		existingVoters,
		existingNonVoters,
		voterTarget,
	)
}

// RemoveNonVoter returns a suitable non-voting replica to remove from the
// provided set. It first attempts to randomly select a target from the set of
// stores that have greater than the average number of replicas. Failing that,
// it falls back to selecting a random target from any of the existing
// non-voting replicas.
func (a Allocator) RemoveNonVoter(
	ctx context.Context,
	zone *zonepb.ZoneConfig,
	nonVoterCandidates []roachpb.ReplicaDescriptor,
	existingVoters []roachpb.ReplicaDescriptor,
	existingNonVoters []roachpb.ReplicaDescriptor,
) (roachpb.ReplicaDescriptor, string, error) {
	return a.removeTarget(
		ctx,
		zone,
		roachpb.MakeReplicaSet(nonVoterCandidates).ReplicationTargets(),
		existingVoters,
		existingNonVoters,
		nonVoterTarget,
	)
}

func (a Allocator) rebalanceTarget(
	ctx context.Context,
	zone *zonepb.ZoneConfig,
	raftStatus *raft.Status,
	existingVoters, existingNonVoters []roachpb.ReplicaDescriptor,
	rangeUsageInfo RangeUsageInfo,
	filter storeFilter,
	targetType targetReplicaType,
) (add roachpb.ReplicationTarget, remove roachpb.ReplicationTarget, details string, ok bool) {
	sl, _, _ := a.storePool.getStoreList(filter)
	existingReplicas := append(existingVoters, existingNonVoters...)

	zero := roachpb.ReplicationTarget{}
	analyzedOverallConstraints := constraint.AnalyzeConstraints(
		ctx, a.storePool.getStoreDescriptor, existingReplicas, *zone.NumReplicas, zone.Constraints)
	analyzedVoterConstraints := constraint.AnalyzeConstraints(
		ctx, a.storePool.getStoreDescriptor, existingVoters, zone.GetNumVoters(), zone.VoterConstraints)
	var removalConstraintsChecker constraintsCheckFn
	var rebalanceConstraintsChecker rebalanceConstraintsCheckFn
	var replicaSetToRebalance, replicasWithExcludedStores []roachpb.ReplicaDescriptor
	var otherReplicaSet []roachpb.ReplicaDescriptor

	switch t := targetType; t {
	case voterTarget:
		removalConstraintsChecker = voterConstraintsCheckerForRemoval(
			analyzedOverallConstraints,
			analyzedVoterConstraints,
		)
		rebalanceConstraintsChecker = voterConstraintsCheckerForRebalance(
			analyzedOverallConstraints,
			analyzedVoterConstraints,
		)
		replicaSetToRebalance = existingVoters
		otherReplicaSet = existingNonVoters
	case nonVoterTarget:
		removalConstraintsChecker = nonVoterConstraintsCheckerForRemoval(analyzedOverallConstraints)
		rebalanceConstraintsChecker = nonVoterConstraintsCheckerForRebalance(analyzedOverallConstraints)
		replicaSetToRebalance = existingNonVoters
		// When rebalancing non-voting replicas, we don't consider stores that
		// already have voting replicas as possible candidates. Voting replicas are
		// supposed to be rebalanced before non-voting replicas, and they do
		// consider the non-voters' stores as possible candidates.
		replicasWithExcludedStores = existingVoters
		otherReplicaSet = existingVoters
	default:
		log.Fatalf(ctx, "unsupported targetReplicaType: %v", t)
	}

	options := a.scorerOptions()
	replicaSetForDiversityCalc := getReplicasForDiversityCalc(targetType, existingVoters, existingReplicas)
	results := rankedCandidateListForRebalancing(
		ctx,
		sl,
		removalConstraintsChecker,
		rebalanceConstraintsChecker,
		replicaSetToRebalance,
		replicasWithExcludedStores,
		a.storePool.getLocalitiesByStore(replicaSetForDiversityCalc),
		a.storePool.isStoreReadyForRoutineReplicaTransfer,
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
		existingPlusOneNew := append([]roachpb.ReplicaDescriptor(nil), replicaSetToRebalance...)
		existingPlusOneNew = append(existingPlusOneNew, newReplica)
		replicaCandidates := existingPlusOneNew
		// If we can, filter replicas as we would if we were actually removing one.
		// If we can't (e.g. because we're the leaseholder but not the raft leader),
		// it's better to simulate the removal with the info that we do have than to
		// assume that the rebalance is ok (#20241).
		if targetType == voterTarget && raftStatus != nil && raftStatus.Progress != nil {
			replicaCandidates = simulateFilterUnremovableReplicas(
				ctx, raftStatus, replicaCandidates, newReplica.ReplicaID)
		}
		if len(replicaCandidates) == 0 {
			// No existing replicas are suitable to remove.
			log.VEventf(ctx, 2, "not rebalancing %s to s%d because there are no existing "+
				"replicas that can be removed", targetType, target.store.StoreID)
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
			otherReplicaSet,
			rangeUsageInfo,
			targetType,
		)
		if err != nil {
			log.Warningf(ctx, "simulating removal of %s failed: %+v", targetType, err)
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

// RebalanceVoter returns a suitable store for a rebalance target with required
// attributes. Rebalance targets are selected via the same mechanism as
// AllocateVoter(), except the chosen target must follow some additional
// criteria. Namely, if chosen, it must further the goal of balancing the
// cluster.
//
// The supplied parameters are the required attributes for the range and
// information about the range being considered for rebalancing.
//
// The existing voting replicas modulo any store with dead replicas are
// candidates for rebalancing.
//
// Simply ignoring a rebalance opportunity in the event that the target chosen
// by rankedCandidateListForRebalancing() doesn't fit balancing criteria is
// perfectly fine, as other stores in the cluster will also be doing their
// probabilistic best to rebalance. This helps prevent a stampeding herd
// targeting an abnormally under-utilized store.
//
// The return values are, in order:
//
// 1. The target on which to add a new replica,
// 2. An existing replica to remove,
// 3. a JSON string for use in the range log, and
// 4. a boolean indicationg whether 1-3 were populated (i.e. whether a rebalance
//    opportunity was found).
func (a Allocator) RebalanceVoter(
	ctx context.Context,
	zone *zonepb.ZoneConfig,
	raftStatus *raft.Status,
	existingVoters, existingNonVoters []roachpb.ReplicaDescriptor,
	rangeUsageInfo RangeUsageInfo,
	filter storeFilter,
) (add roachpb.ReplicationTarget, remove roachpb.ReplicationTarget, details string, ok bool) {
	return a.rebalanceTarget(
		ctx,
		zone,
		raftStatus,
		existingVoters,
		existingNonVoters,
		rangeUsageInfo,
		filter,
		voterTarget,
	)
}

// RebalanceNonVoter returns a suitable pair of rebalance candidates for a
// non-voting replica. This behaves very similarly to `RebalanceVoter` as
// explained above. The key differences are the following:
//
// 1. Non-voting replicas only adhere to the overall `constraints` and not the
// `voter_constraints`.
// 2. We do not consider stores that have voters as valid candidates for
// rebalancing.
// 3. Diversity score calculation for non-voters is relative to all existing
// replicas. This is in contrast to how we compute the diversity scores for
// voting replicas, which are computed relative to just the set of voting
// replicas.
func (a Allocator) RebalanceNonVoter(
	ctx context.Context,
	zone *zonepb.ZoneConfig,
	raftStatus *raft.Status,
	existingVoters, existingNonVoters []roachpb.ReplicaDescriptor,
	rangeUsageInfo RangeUsageInfo,
	filter storeFilter,
) (add roachpb.ReplicationTarget, remove roachpb.ReplicationTarget, details string, ok bool) {
	return a.rebalanceTarget(
		ctx,
		zone,
		raftStatus,
		existingVoters,
		existingNonVoters,
		rangeUsageInfo,
		filter,
		nonVoterTarget,
	)
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
//
// Returns an empty descriptor if no target is found.
//
// TODO(aayush, andrei): If a draining leaseholder doesn't see any other voters
// in its locality, but sees a learner, rather than allowing the lease to be
// transferred outside of its current locality (likely violating leaseholder
// preferences, at least temporarily), it would be nice to promote the existing
// learner to a voter. This could be further extended to cases where we have a
// dead voter in a given locality along with a live learner. In such cases, we
// would want to promote the live learner to a voter and demote the dead voter
// to a learner.
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
	sl, _, _ := a.storePool.getStoreList(storeFilterSuspect)
	sl = sl.filter(zone.Constraints)
	sl = sl.filter(zone.VoterConstraints)
	// The only thing we use the storeList for is for the lease mean across the
	// eligible stores, make that explicit here.
	candidateLeasesMean := sl.candidateLeases.mean

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
		// Verify that the preferred replica is eligible to receive the lease.
		preferred, _ = a.storePool.liveAndDeadReplicas(preferred)
		if len(preferred) == 1 {
			return preferred[0]
		}
		return roachpb.ReplicaDescriptor{}
	} else if len(preferred) > 1 {
		// If the current leaseholder is not preferred, set checkTransferLeaseSource
		// to false to motivate the below logic to transfer the lease.
		existing = preferred
		if !storeHasReplica(leaseStoreID, roachpb.MakeReplicaSet(preferred).ReplicationTargets()) {
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
		ctx, source, existing, stats, nil, candidateLeasesMean,
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
		if !checkCandidateFullness || float64(storeDesc.Capacity.LeaseCount) < candidateLeasesMean-0.5 {
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
		if !storeHasReplica(leaseStoreID, roachpb.MakeReplicaSet(existing).ReplicationTargets()) {
			return true
		}
	}

	sl, _, _ := a.storePool.getStoreList(storeFilterSuspect)
	sl = sl.filter(zone.Constraints)
	sl = sl.filter(zone.VoterConstraints)
	log.VEventf(ctx, 3, "ShouldTransferLease (lease-holder=%d):\n%s", leaseStoreID, sl)

	// Only consider live, non-draining replicas.
	existing, _ = a.storePool.liveAndDeadReplicas(existing)

	// Short-circuit if there are no valid targets out there.
	if len(existing) == 0 || (len(existing) == 1 && existing[0].StoreID == source.StoreID) {
		return false
	}

	transferDec, _ := a.shouldTransferLeaseUsingStats(ctx, source, existing, stats, nil, sl.candidateLeases.mean)
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
	decision, _ := a.shouldTransferLeaseUsingStats(ctx, source, existing, stats, adjustments, sl.candidateLeases.mean)
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
	source roachpb.StoreDescriptor,
	existing []roachpb.ReplicaDescriptor,
	stats *replicaStats,
	rebalanceAdjustments map[roachpb.StoreID]float64,
	candidateLeasesMean float64,
) (transferDecision, roachpb.ReplicaDescriptor) {
	// Only use load-based rebalancing if it's enabled and we have both
	// stats and locality information to base our decision on.
	if stats == nil || !enableLoadBasedLeaseRebalancing.Get(&a.storePool.st.SV) {
		return decideWithoutStats, roachpb.ReplicaDescriptor{}
	}
	replicaLocalities := a.storePool.getLocalitiesByNode(existing)
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
			ctx, a.storePool.st, remoteWeight, remoteLatency, storeDesc, sourceWeight, source, candidateLeasesMean)
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
