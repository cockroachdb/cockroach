// Copyright 2022 The Cockroach Authors.
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
	"fmt"
	"math"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/allocatorimpl"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/storepool"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/raftutil"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/redact"
	"go.etcd.io/etcd/raft/v3"
)

func (sr *StoreRebalancer) deprecatedChooseLeaseToTransfer(
	ctx context.Context,
	hottestRanges *[]replicaWithStats,
	localDesc *roachpb.StoreDescriptor,
	storeList storepool.StoreList,
	storeMap map[roachpb.StoreID]*roachpb.StoreDescriptor,
	minQPS float64,
	maxQPS float64,
) (replicaWithStats, roachpb.ReplicaDescriptor, []replicaWithStats) {
	var considerForRebalance []replicaWithStats
	now := sr.rq.store.Clock().NowAsClockTimestamp()
	for {
		if len(*hottestRanges) == 0 {
			return replicaWithStats{}, roachpb.ReplicaDescriptor{}, considerForRebalance
		}
		replWithStats := (*hottestRanges)[0]
		*hottestRanges = (*hottestRanges)[1:]

		// We're all out of replicas.
		if replWithStats.repl == nil {
			return replicaWithStats{}, roachpb.ReplicaDescriptor{}, considerForRebalance
		}

		if shouldNotMoveAway(ctx, replWithStats, localDesc, now, minQPS) {
			continue
		}

		// Don't bother moving leases whose QPS is below some small fraction of the
		// store's QPS (unless the store has extra leases to spare anyway). It's
		// just unnecessary churn with no benefit to move leases responsible for,
		// for example, 1 qps on a store with 5000 qps.
		const minQPSFraction = .001
		if replWithStats.qps < localDesc.Capacity.QueriesPerSecond*minQPSFraction &&
			float64(localDesc.Capacity.LeaseCount) <= storeList.CandidateLeases.Mean {
			log.VEventf(ctx, 5, "r%d's %.2f qps is too little to matter relative to s%d's %.2f total qps",
				replWithStats.repl.RangeID, replWithStats.qps, localDesc.StoreID, localDesc.Capacity.QueriesPerSecond)
			continue
		}

		desc, conf := replWithStats.repl.DescAndSpanConfig()
		log.VEventf(ctx, 3, "considering lease transfer for r%d with %.2f qps",
			desc.RangeID, replWithStats.qps)

		// Check all the other voting replicas in order of increasing qps.
		// Learners or non-voters aren't allowed to become leaseholders or raft
		// leaders, so only consider the `Voter` replicas.
		candidates := desc.Replicas().DeepCopy().VoterDescriptors()
		sort.Slice(candidates, func(i, j int) bool {
			var iQPS, jQPS float64
			if desc := storeMap[candidates[i].StoreID]; desc != nil {
				iQPS = desc.Capacity.QueriesPerSecond
			}
			if desc := storeMap[candidates[j].StoreID]; desc != nil {
				jQPS = desc.Capacity.QueriesPerSecond
			}
			return iQPS < jQPS
		})

		var raftStatus *raft.Status

		preferred := sr.rq.allocator.PreferredLeaseholders(conf, candidates)

		// Filter both the list of preferred stores as well as the list of all
		// candidate replicas to only consider live (non-suspect, non-draining)
		// nodes.
		const includeSuspectAndDrainingStores = false
		preferred, _ = sr.rq.allocator.StorePool.LiveAndDeadReplicas(preferred, includeSuspectAndDrainingStores)
		candidates, _ = sr.rq.allocator.StorePool.LiveAndDeadReplicas(candidates, includeSuspectAndDrainingStores)

		for _, candidate := range candidates {
			if candidate.StoreID == localDesc.StoreID {
				continue
			}

			meanQPS := storeList.CandidateQueriesPerSecond.Mean
			if sr.shouldNotMoveTo(ctx, storeMap, replWithStats, candidate.StoreID, meanQPS, minQPS, maxQPS) {
				continue
			}

			if raftStatus == nil {
				raftStatus = sr.getRaftStatusFn(replWithStats.repl)
			}
			if raftutil.ReplicaIsBehind(raftStatus, candidate.ReplicaID) {
				log.VEventf(ctx, 3, "%v is behind or this store isn't the raft leader for r%d; raftStatus: %v",
					candidate, desc.RangeID, raftStatus)
				continue
			}

			if len(preferred) > 0 && !allocatorimpl.StoreHasReplica(candidate.StoreID, roachpb.MakeReplicaSet(preferred).ReplicationTargets()) {
				log.VEventf(ctx, 3, "s%d not a preferred leaseholder for r%d; preferred: %v",
					candidate.StoreID, desc.RangeID, preferred)
				continue
			}

			filteredStoreList := storeList.ExcludeInvalid(conf.VoterConstraints)
			if sr.rq.allocator.FollowTheWorkloadPrefersLocal(
				ctx,
				filteredStoreList,
				*localDesc,
				candidate.StoreID,
				candidates,
				replWithStats.repl.loadStats.batchRequests,
			) {
				log.VEventf(ctx, 3, "r%d is on s%d due to follow-the-workload; skipping",
					desc.RangeID, localDesc.StoreID)
				continue
			}

			return replWithStats, candidate, considerForRebalance
		}

		// If none of the other replicas are valid lease transfer targets, consider
		// this range for replica rebalancing.
		considerForRebalance = append(considerForRebalance, replWithStats)
	}
}

// rangeRebalanceContext represents a snapshot of a range's state during the
// StoreRebalancer's attempt to rebalance it based on QPS.
type deprecatedRebalanceContext struct {
	replWithStats                         replicaWithStats
	rangeDesc                             *roachpb.RangeDescriptor
	conf                                  roachpb.SpanConfig
	clusterNodes                          int
	numDesiredVoters, numDesiredNonVoters int
}

func (rbc *deprecatedRebalanceContext) numDesiredReplicas(
	targetType allocatorimpl.TargetReplicaType,
) int {
	switch targetType {
	case allocatorimpl.VoterTarget:
		return rbc.numDesiredVoters
	case allocatorimpl.NonVoterTarget:
		return rbc.numDesiredNonVoters
	default:
		panic(fmt.Sprintf("unknown targetReplicaType %s", targetType))
	}
}

func (sr *StoreRebalancer) deprecatedChooseRangeToRebalance(
	ctx context.Context,
	hottestRanges *[]replicaWithStats,
	localDesc *roachpb.StoreDescriptor,
	storeList storepool.StoreList,
	storeMap map[roachpb.StoreID]*roachpb.StoreDescriptor,
	minQPS float64,
	maxQPS float64,
) (replWithStats replicaWithStats, voterTargets, nonVoterTargets []roachpb.ReplicationTarget) {
	now := sr.rq.store.Clock().NowAsClockTimestamp()
	for {
		if len(*hottestRanges) == 0 {
			return replicaWithStats{}, nil, nil
		}
		replWithStats := (*hottestRanges)[0]
		*hottestRanges = (*hottestRanges)[1:]

		if replWithStats.repl == nil {
			return replicaWithStats{}, nil, nil
		}

		if shouldNotMoveAway(ctx, replWithStats, localDesc, now, minQPS) {
			continue
		}

		// Don't bother moving ranges whose QPS is below some small fraction of the
		// store's QPS (unless the store has extra ranges to spare anyway). It's
		// just unnecessary churn with no benefit to move ranges responsible for,
		// for example, 1 qps on a store with 5000 qps.
		const minQPSFraction = .001
		if replWithStats.qps < localDesc.Capacity.QueriesPerSecond*minQPSFraction {
			log.VEventf(
				ctx,
				5,
				"r%d's %.2f qps is too little to matter relative to s%d's %.2f total qps",
				replWithStats.repl.RangeID,
				replWithStats.qps,
				localDesc.StoreID,
				localDesc.Capacity.QueriesPerSecond,
			)
			continue
		}

		log.VEventf(ctx, 3, "considering replica rebalance for r%d with %.2f qps",
			replWithStats.repl.GetRangeID(), replWithStats.qps)
		rangeDesc, conf := replWithStats.repl.DescAndSpanConfig()
		clusterNodes := sr.rq.allocator.StorePool.ClusterNodeCount()
		numDesiredVoters := allocatorimpl.GetNeededVoters(conf.GetNumVoters(), clusterNodes)
		numDesiredNonVoters := allocatorimpl.GetNeededNonVoters(
			numDesiredVoters, int(conf.GetNumNonVoters()), clusterNodes,
		)
		if rs := rangeDesc.Replicas(); numDesiredVoters != len(rs.VoterDescriptors()) ||
			numDesiredNonVoters != len(rs.NonVoterDescriptors()) {
			// If the StoreRebalancer is allowed past this point, it may accidentally
			// downreplicate and this can cause unavailable ranges.
			//
			// See: https://github.com/cockroachdb/cockroach/issues/54444#issuecomment-707706553
			log.VEventf(ctx, 3, "range needs up/downreplication; not considering rebalance")
			continue
		}

		rebalanceCtx := deprecatedRebalanceContext{
			replWithStats:       replWithStats,
			rangeDesc:           rangeDesc,
			conf:                conf,
			clusterNodes:        clusterNodes,
			numDesiredVoters:    numDesiredVoters,
			numDesiredNonVoters: numDesiredNonVoters,
		}
		targetVoterRepls, targetNonVoterRepls := sr.getRebalanceCandidatesBasedOnQPS(
			ctx, rebalanceCtx, localDesc, storeMap, storeList, minQPS, maxQPS,
		)

		// If we couldn't find enough valid targets, forget about this range.
		//
		// TODO(a-robinson): Support more incremental improvements -- move what we
		// can if it makes things better even if it isn't great. For example,
		// moving one of the other existing replicas that's on a store with less
		// qps than the max threshold but above the mean would help in certain
		// locality configurations.
		if len(targetVoterRepls) < rebalanceCtx.numDesiredVoters {
			log.VEventf(ctx, 3, "couldn't find enough voter rebalance targets for r%d (%d/%d)",
				rangeDesc.RangeID, len(targetVoterRepls), rebalanceCtx.numDesiredVoters)
			continue
		}
		if len(targetNonVoterRepls) < rebalanceCtx.numDesiredNonVoters {
			log.VEventf(ctx, 3, "couldn't find enough non-voter rebalance targets for r%d (%d/%d)",
				rangeDesc.RangeID, len(targetNonVoterRepls), rebalanceCtx.numDesiredNonVoters)
			continue
		}

		// If the new set of replicas has lower diversity scores than the existing
		// set, we don't continue with the rebalance. since we want to ensure we
		// don't hurt locality diversity just to improve QPS.
		//
		// 1. Ensure that diversity among voting replicas is not hurt by this
		// rebalancing decision.
		if sr.worsensDiversity(
			ctx,
			rangeDesc.GetRangeID(),
			rangeDesc.Replicas().VoterDescriptors(),
			targetVoterRepls,
			true, /* onlyVoters */
		) {
			continue
		}
		// 2. Ensure that diversity among all replicas is not hurt by this decision.
		allTargetRepls := append(targetVoterRepls, targetNonVoterRepls...)
		if sr.worsensDiversity(
			ctx,
			rangeDesc.GetRangeID(),
			rangeDesc.Replicas().Descriptors(),
			allTargetRepls,
			false, /* onlyVoters */
		) {
			continue
		}

		// Pick the voter with the least QPS to be leaseholder;
		// RelocateRange transfers the lease to the first provided target.
		newLeaseIdx := 0
		newLeaseQPS := math.MaxFloat64
		var raftStatus *raft.Status
		for i := 0; i < len(targetVoterRepls); i++ {
			// Ensure we don't transfer the lease to an existing replica that is behind
			// in processing its raft log.
			if replica, ok := rangeDesc.GetReplicaDescriptor(targetVoterRepls[i].StoreID); ok {
				if raftStatus == nil {
					raftStatus = sr.getRaftStatusFn(replWithStats.repl)
				}
				if raftutil.ReplicaIsBehind(raftStatus, replica.ReplicaID) {
					continue
				}
			}

			storeDesc, ok := storeMap[targetVoterRepls[i].StoreID]
			if ok && storeDesc.Capacity.QueriesPerSecond < newLeaseQPS {
				newLeaseIdx = i
				newLeaseQPS = storeDesc.Capacity.QueriesPerSecond
			}
		}
		targetVoterRepls[0], targetVoterRepls[newLeaseIdx] = targetVoterRepls[newLeaseIdx], targetVoterRepls[0]
		return replWithStats,
			roachpb.MakeReplicaSet(targetVoterRepls).ReplicationTargets(),
			roachpb.MakeReplicaSet(targetNonVoterRepls).ReplicationTargets()
	}
}

// worsensDiversity returns true iff the diversity score of `currentRepls` is
// higher than `targetRepls` (either among just the set of voting replicas, or
// across all replicas in the range -- determined by `onlyVoters`).
func (sr *StoreRebalancer) worsensDiversity(
	ctx context.Context,
	rangeID roachpb.RangeID,
	currentRepls, targetRepls []roachpb.ReplicaDescriptor,
	onlyVoters bool,
) bool {
	curDiversity := allocatorimpl.RangeDiversityScore(
		sr.rq.allocator.StorePool.GetLocalitiesByStore(currentRepls),
	)
	newDiversity := allocatorimpl.RangeDiversityScore(
		sr.rq.allocator.StorePool.GetLocalitiesByStore(targetRepls),
	)
	replicaStr := "replica"
	if onlyVoters {
		replicaStr = "voting replica"
	}
	if curDiversity > newDiversity {
		log.VEventf(
			ctx,
			3,
			"new %s diversity %.2f for r%d worse than current diversity %.2f; not rebalancing",
			replicaStr,
			newDiversity,
			rangeID,
			curDiversity,
		)
		return true
	}
	return false
}

// getRebalanceCandidatesBasedOnQPS returns a list of rebalance targets for
// voting and non-voting replicas on the range that match the relevant
// constraints on the range and would further the goal of balancing the QPS on
// the stores in this cluster. In case there aren't enough stores that meet the
// constraints and are valid rebalance candidates based on QPS, the list of
// targets returned may contain fewer-than-required replicas.
//
// NB: `localStoreDesc` is expected to be the leaseholder of the range being
// operated on.
func (sr *StoreRebalancer) getRebalanceCandidatesBasedOnQPS(
	ctx context.Context,
	rebalanceCtx deprecatedRebalanceContext,
	localStoreDesc *roachpb.StoreDescriptor,
	storeMap map[roachpb.StoreID]*roachpb.StoreDescriptor,
	storeList storepool.StoreList,
	minQPS, maxQPS float64,
) (finalVoterTargets, finalNonVoterTargets []roachpb.ReplicaDescriptor) {
	options := sr.scorerOptions(ctx)
	options.QPSRebalanceThreshold = allocator.QPSRebalanceThreshold.Get(&sr.st.SV)

	// Decide which voting / non-voting replicas we want to keep around and find
	// rebalance targets for the rest.
	partialVoterTargets := sr.pickReplsToKeep(
		ctx,
		rebalanceCtx,
		nil, /* replsToExclude */
		localStoreDesc,
		storeMap,
		maxQPS,
		allocatorimpl.VoterTarget,
	)
	finalVoterTargets = sr.pickRemainingRepls(
		ctx,
		rebalanceCtx,
		partialVoterTargets,
		nil, /* partialNonVoterTargets */
		storeMap,
		storeList,
		options,
		minQPS, maxQPS,
		allocatorimpl.VoterTarget,
	)

	partialNonVoterTargets := sr.pickReplsToKeep(
		ctx,
		rebalanceCtx,
		// NB: `finalVoterTargets` may contain replicas that are part of the
		// existing set of non-voter targets, so we make sure that we don't keep
		// those replicas around in `partialNonVoterTargets`.
		finalVoterTargets,
		localStoreDesc,
		storeMap,
		maxQPS,
		allocatorimpl.NonVoterTarget,
	)
	finalNonVoterTargets = sr.pickRemainingRepls(
		ctx,
		rebalanceCtx,
		finalVoterTargets,
		partialNonVoterTargets,
		storeMap,
		storeList,
		options,
		minQPS,
		maxQPS,
		allocatorimpl.NonVoterTarget,
	)

	return finalVoterTargets, finalNonVoterTargets
}

// pickRemainingRepls determines the set of rebalance targets to fill in the
// rest of `partial{Voter,NonVoter}Targets` such that the resulting set contains
// exactly as many replicas as dictated by the zone configs.
//
// The caller is expected to synthesize the set of
// `partial{Voter,NonVoter}Targets` via `StoreRebalancer.pickReplsToKeep`.
func (sr *StoreRebalancer) pickRemainingRepls(
	ctx context.Context,
	rebalanceCtx deprecatedRebalanceContext,
	partialVoterTargets, partialNonVoterTargets []roachpb.ReplicaDescriptor,
	storeMap map[roachpb.StoreID]*roachpb.StoreDescriptor,
	storeList storepool.StoreList,
	options allocatorimpl.ScorerOptions,
	minQPS, maxQPS float64,
	targetType allocatorimpl.TargetReplicaType,
) []roachpb.ReplicaDescriptor {
	// Alias the slice that corresponds to the set of replicas that is being
	// appended to. This is because we want subsequent calls to
	// `allocateTargetFromList` to observe the results of previous calls (note the
	// append to the slice referenced by `finalTargetsForType`).
	var finalTargetsForType *[]roachpb.ReplicaDescriptor
	switch targetType {
	case allocatorimpl.VoterTarget:
		finalTargetsForType = &partialVoterTargets
	case allocatorimpl.NonVoterTarget:
		finalTargetsForType = &partialNonVoterTargets
	default:
		log.Fatalf(ctx, "unknown targetReplicaType: %s", targetType)
	}
	for len(*finalTargetsForType) < rebalanceCtx.numDesiredReplicas(targetType) {
		// Use the preexisting Allocate{Non}Voter logic to ensure that
		// considerations such as zone constraints, locality diversity, and full
		// disk come into play.
		target, _ := sr.rq.allocator.AllocateTargetFromList(
			ctx,
			storeList,
			rebalanceCtx.conf,
			partialVoterTargets,
			partialNonVoterTargets,
			options,
			// The store rebalancer should never need to perform lateral relocations,
			// so we ask the allocator to disregard all the nodes that exist in
			// `partial{Non}VoterTargets`.
			false, /* allowMultipleReplsPerNode */
			targetType,
		)
		if roachpb.Empty(target) {
			log.VEventf(
				ctx, 3, "no rebalance %ss found to replace the current store for r%d",
				targetType, rebalanceCtx.rangeDesc.RangeID,
			)
			break
		}

		meanQPS := storeList.CandidateQueriesPerSecond.Mean
		if sr.shouldNotMoveTo(
			ctx,
			storeMap,
			rebalanceCtx.replWithStats,
			target.StoreID,
			meanQPS,
			minQPS,
			maxQPS,
		) {
			// NB: If the target store returned by the allocator is not fit to
			// receive a new replica due to balancing reasons, there is no point
			// continuing with this loop since we'd expect future calls to
			// `allocateTargetFromList` to return the same target.
			break
		}

		*finalTargetsForType = append(*finalTargetsForType, roachpb.ReplicaDescriptor{
			NodeID:  target.NodeID,
			StoreID: target.StoreID,
		})
	}
	return *finalTargetsForType
}

// pickReplsToKeep determines the set of existing replicas for a range which
// should _not_ be rebalanced (because they belong to stores that aren't
// overloaded).
func (sr *StoreRebalancer) pickReplsToKeep(
	ctx context.Context,
	rebalanceCtx deprecatedRebalanceContext,
	replsToExclude []roachpb.ReplicaDescriptor,
	localStoreDesc *roachpb.StoreDescriptor,
	storeMap map[roachpb.StoreID]*roachpb.StoreDescriptor,
	maxQPS float64,
	targetType allocatorimpl.TargetReplicaType,
) (partialTargetRepls []roachpb.ReplicaDescriptor) {
	shouldExclude := func(repl roachpb.ReplicaDescriptor) bool {
		for _, excluded := range replsToExclude {
			if repl.StoreID == excluded.StoreID {
				return true
			}
		}
		return false
	}

	var currentReplsForType []roachpb.ReplicaDescriptor
	switch targetType {
	case allocatorimpl.VoterTarget:
		currentReplsForType = rebalanceCtx.rangeDesc.Replicas().VoterDescriptors()
	case allocatorimpl.NonVoterTarget:
		currentReplsForType = rebalanceCtx.rangeDesc.Replicas().NonVoterDescriptors()
	default:
		log.Fatalf(ctx, "unknown targetReplicaType: %s", targetType)
	}

	// Check the existing replicas, keeping around those that aren't overloaded.
	for i := range currentReplsForType {
		if shouldExclude(currentReplsForType[i]) ||
			currentReplsForType[i].StoreID == localStoreDesc.StoreID {
			continue
		}

		// Keep the replica in the range if we don't know its QPS or if its QPS is
		// below the upper threshold. Punishing stores not in our store map could
		// cause mass evictions if the storePool gets out of sync.
		storeDesc, ok := storeMap[currentReplsForType[i].StoreID]
		if !ok || storeDesc.Capacity.QueriesPerSecond < maxQPS {
			if log.V(3) {
				var reason redact.RedactableString
				if ok {
					reason = redact.Sprintf(
						" (qps %.2f vs max %.2f)",
						storeDesc.Capacity.QueriesPerSecond,
						maxQPS,
					)
				}
				log.VEventf(
					ctx,
					3,
					"keeping %s r%d/%d on s%d%s",
					targetType,
					rebalanceCtx.rangeDesc.RangeID,
					currentReplsForType[i].ReplicaID,
					currentReplsForType[i].StoreID,
					reason,
				)
			}

			partialTargetRepls = append(partialTargetRepls, currentReplsForType[i])
		}
	}
	return partialTargetRepls
}

func shouldNotMoveAway(
	ctx context.Context,
	replWithStats replicaWithStats,
	localDesc *roachpb.StoreDescriptor,
	now hlc.ClockTimestamp,
	minQPS float64,
) bool {
	if !replWithStats.repl.OwnsValidLease(ctx, now) {
		log.VEventf(ctx, 3, "store doesn't own the lease for r%d", replWithStats.repl.RangeID)
		return true
	}
	if localDesc.Capacity.QueriesPerSecond-replWithStats.qps < minQPS {
		log.VEventf(ctx, 3, "moving r%d's %.2f qps would bring s%d below the min threshold (%.2f)",
			replWithStats.repl.RangeID, replWithStats.qps, localDesc.StoreID, minQPS)
		return true
	}
	return false
}

func (sr *StoreRebalancer) shouldNotMoveTo(
	ctx context.Context,
	storeMap map[roachpb.StoreID]*roachpb.StoreDescriptor,
	replWithStats replicaWithStats,
	candidateStoreID roachpb.StoreID,
	meanQPS float64,
	minQPS float64,
	maxQPS float64,
) bool {
	candidateStore, ok := storeMap[candidateStoreID]
	if !ok {
		log.VEventf(ctx, 3, "missing store descriptor for s%d", candidateStoreID)
		return true
	}

	newCandidateQPS := candidateStore.Capacity.QueriesPerSecond + replWithStats.qps
	if candidateStore.Capacity.QueriesPerSecond < minQPS {
		if newCandidateQPS > maxQPS {
			log.VEventf(ctx, 3,
				"r%d's %.2f qps would push s%d over the max threshold (%.2f) with %.2f qps afterwards",
				replWithStats.repl.RangeID, replWithStats.qps, candidateStoreID, maxQPS, newCandidateQPS)
			return true
		}
	} else if newCandidateQPS > meanQPS {
		log.VEventf(ctx, 3,
			"r%d's %.2f qps would push s%d over the mean (%.2f) with %.2f qps afterwards",
			replWithStats.repl.RangeID, replWithStats.qps, candidateStoreID, meanQPS, newCandidateQPS)
		return true
	}

	// If the target store is on a separate node, we will also care
	// about node liveness.
	targetNodeID := candidateStore.Node.NodeID
	if targetNodeID != sr.rq.store.Ident.NodeID {
		if !sr.rq.allocator.StorePool.IsStoreReadyForRoutineReplicaTransfer(ctx, candidateStore.StoreID) {
			log.VEventf(ctx, 3,
				"refusing to transfer replica to n%d/s%d", targetNodeID, candidateStore.StoreID)
			return true
		}
	}
	return false
}
