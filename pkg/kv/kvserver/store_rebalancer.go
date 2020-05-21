// Copyright 2018 The Cockroach Authors.
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
	"math"
	"math/rand"
	"sort"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/contextutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"go.etcd.io/etcd/raft"
)

const (
	// storeRebalancerTimerDuration is how frequently to check the store-level
	// balance of the cluster.
	storeRebalancerTimerDuration = time.Minute

	// minQPSThresholdDifference is the minimum QPS difference from the cluster
	// mean that this system should care about. In other words, we won't worry
	// about rebalancing for QPS reasons if a store's QPS differs from the mean
	// by less than this amount even if the amount is greater than the percentage
	// threshold. This avoids too many lease transfers in lightly loaded clusters.
	minQPSThresholdDifference = 100
)

var (
	metaStoreRebalancerLeaseTransferCount = metric.Metadata{
		Name:        "rebalancing.lease.transfers",
		Help:        "Number of lease transfers motivated by store-level load imbalances",
		Measurement: "Lease Transfers",
		Unit:        metric.Unit_COUNT,
	}
	metaStoreRebalancerRangeRebalanceCount = metric.Metadata{
		Name:        "rebalancing.range.rebalances",
		Help:        "Number of range rebalance operations motivated by store-level load imbalances",
		Measurement: "Range Rebalances",
		Unit:        metric.Unit_COUNT,
	}
)

// StoreRebalancerMetrics is the set of metrics for the store-level rebalancer.
type StoreRebalancerMetrics struct {
	LeaseTransferCount  *metric.Counter
	RangeRebalanceCount *metric.Counter
}

func makeStoreRebalancerMetrics() StoreRebalancerMetrics {
	return StoreRebalancerMetrics{
		LeaseTransferCount:  metric.NewCounter(metaStoreRebalancerLeaseTransferCount),
		RangeRebalanceCount: metric.NewCounter(metaStoreRebalancerRangeRebalanceCount),
	}
}

// LoadBasedRebalancingMode controls whether range rebalancing takes
// additional variables such as write load and disk usage into account.
// If disabled, rebalancing is done purely based on replica count.
var LoadBasedRebalancingMode = settings.RegisterPublicEnumSetting(
	"kv.allocator.load_based_rebalancing",
	"whether to rebalance based on the distribution of QPS across stores",
	"leases and replicas",
	map[int64]string{
		int64(LBRebalancingOff):               "off",
		int64(LBRebalancingLeasesOnly):        "leases",
		int64(LBRebalancingLeasesAndReplicas): "leases and replicas",
	},
)

// qpsRebalanceThreshold is much like rangeRebalanceThreshold, but for
// QPS rather than range count. This should be set higher than
// rangeRebalanceThreshold because QPS can naturally vary over time as
// workloads change and clients come and go, so we need to be a little more
// forgiving to avoid thrashing.
var qpsRebalanceThreshold = func() *settings.FloatSetting {
	s := settings.RegisterNonNegativeFloatSetting(
		"kv.allocator.qps_rebalance_threshold",
		"minimum fraction away from the mean a store's QPS (such as queries per second) can be before it is considered overfull or underfull",
		0.25,
	)
	s.SetVisibility(settings.Public)
	return s
}()

// LBRebalancingMode controls if and when we do store-level rebalancing
// based on load.
type LBRebalancingMode int64

const (
	// LBRebalancingOff means that we do not do store-level rebalancing
	// based on load statistics.
	LBRebalancingOff LBRebalancingMode = iota
	// LBRebalancingLeasesOnly means that we rebalance leases based on
	// store-level QPS imbalances.
	LBRebalancingLeasesOnly
	// LBRebalancingLeasesAndReplicas means that we rebalance both leases and
	// replicas based on store-level QPS imbalances.
	LBRebalancingLeasesAndReplicas
)

// StoreRebalancer is responsible for examining how the associated store's load
// compares to the load on other stores in the cluster and transferring leases
// or replicas away if the local store is overloaded.
//
// This isn't implemented as a Queue because the Queues all operate on one
// replica at a time, making a local decision about each replica. Queues don't
// really know how the replica they're looking at compares to other replicas on
// the store. Our goal is balancing stores, though, so it's preferable to make
// decisions about each store and then carefully pick replicas to move that
// will best accomplish the store-level goals.
type StoreRebalancer struct {
	log.AmbientContext
	metrics         StoreRebalancerMetrics
	st              *cluster.Settings
	rq              *replicateQueue
	replRankings    *replicaRankings
	getRaftStatusFn func(replica *Replica) *raft.Status
}

// NewStoreRebalancer creates a StoreRebalancer to work in tandem with the
// provided replicateQueue.
func NewStoreRebalancer(
	ambientCtx log.AmbientContext,
	st *cluster.Settings,
	rq *replicateQueue,
	replRankings *replicaRankings,
) *StoreRebalancer {
	sr := &StoreRebalancer{
		AmbientContext: ambientCtx,
		metrics:        makeStoreRebalancerMetrics(),
		st:             st,
		rq:             rq,
		replRankings:   replRankings,
		getRaftStatusFn: func(replica *Replica) *raft.Status {
			return replica.RaftStatus()
		},
	}
	sr.AddLogTag("store-rebalancer", nil)
	sr.rq.store.metrics.registry.AddMetricStruct(&sr.metrics)
	return sr
}

// Start runs an infinite loop in a goroutine which regularly checks whether
// the store is overloaded along any important dimension (e.g. range count,
// QPS, disk usage), and if so attempts to correct that by moving leases or
// replicas elsewhere.
//
// This worker acts on store-level imbalances, whereas the replicate queue
// makes decisions based on the zone config constraints and diversity of
// individual ranges. This means that there are two different workers that
// could potentially be making decisions about a given range, so they have to
// be careful to avoid stepping on each others' toes.
//
// TODO(a-robinson): Expose metrics to make this understandable without having
// to dive into logspy.
func (sr *StoreRebalancer) Start(ctx context.Context, stopper *stop.Stopper) {
	ctx = sr.AnnotateCtx(ctx)

	// Start a goroutine that watches and proactively renews certain
	// expiration-based leases.
	stopper.RunWorker(ctx, func(ctx context.Context) {
		timer := timeutil.NewTimer()
		defer timer.Stop()
		timer.Reset(jitteredInterval(storeRebalancerTimerDuration))
		for {
			// Wait out the first tick before doing anything since the store is still
			// starting up and we might as well wait for some qps/wps stats to
			// accumulate.
			select {
			case <-stopper.ShouldQuiesce():
				return
			case <-timer.C:
				timer.Read = true
				timer.Reset(jitteredInterval(storeRebalancerTimerDuration))
			}

			mode := LBRebalancingMode(LoadBasedRebalancingMode.Get(&sr.st.SV))
			if mode == LBRebalancingOff {
				continue
			}

			storeList, _, _ := sr.rq.allocator.storePool.getStoreList(storeFilterNone)
			sr.rebalanceStore(ctx, mode, storeList)
		}
	})
}

func (sr *StoreRebalancer) rebalanceStore(
	ctx context.Context, mode LBRebalancingMode, storeList StoreList,
) {
	qpsThresholdFraction := qpsRebalanceThreshold.Get(&sr.st.SV)

	// First check if we should transfer leases away to better balance QPS.
	qpsMinThreshold := math.Min(storeList.candidateQueriesPerSecond.mean*(1-qpsThresholdFraction),
		storeList.candidateQueriesPerSecond.mean-minQPSThresholdDifference)
	qpsMaxThreshold := math.Max(storeList.candidateQueriesPerSecond.mean*(1+qpsThresholdFraction),
		storeList.candidateQueriesPerSecond.mean+minQPSThresholdDifference)

	var localDesc *roachpb.StoreDescriptor
	for i := range storeList.stores {
		if storeList.stores[i].StoreID == sr.rq.store.StoreID() {
			localDesc = &storeList.stores[i]
		}
	}
	if localDesc == nil {
		log.Warningf(ctx, "StorePool missing descriptor for local store")
		return
	}

	if !(localDesc.Capacity.QueriesPerSecond > qpsMaxThreshold) {
		log.VEventf(ctx, 1, "local QPS %.2f is below max threshold %.2f (mean=%.2f); no rebalancing needed",
			localDesc.Capacity.QueriesPerSecond, qpsMaxThreshold, storeList.candidateQueriesPerSecond.mean)
		return
	}

	var replicasToMaybeRebalance []replicaWithStats
	storeMap := storeListToMap(storeList)

	log.Infof(ctx,
		"considering load-based lease transfers for s%d with %.2f qps (mean=%.2f, upperThreshold=%.2f)",
		localDesc.StoreID, localDesc.Capacity.QueriesPerSecond, storeList.candidateQueriesPerSecond.mean, qpsMaxThreshold)

	hottestRanges := sr.replRankings.topQPS()
	for localDesc.Capacity.QueriesPerSecond > qpsMaxThreshold {
		replWithStats, target, considerForRebalance := sr.chooseLeaseToTransfer(
			ctx, &hottestRanges, localDesc, storeList, storeMap, qpsMinThreshold, qpsMaxThreshold)
		replicasToMaybeRebalance = append(replicasToMaybeRebalance, considerForRebalance...)
		if replWithStats.repl == nil {
			break
		}

		log.VEventf(ctx, 1, "transferring r%d (%.2f qps) to s%d to better balance load",
			replWithStats.repl.RangeID, replWithStats.qps, target.StoreID)
		timeout := sr.rq.processTimeoutFunc(sr.st, replWithStats.repl)
		if err := contextutil.RunWithTimeout(ctx, "transfer lease", timeout, func(ctx context.Context) error {
			return sr.rq.transferLease(ctx, replWithStats.repl, target, replWithStats.qps)
		}); err != nil {
			log.Errorf(ctx, "unable to transfer lease to s%d: %+v", target.StoreID, err)
			continue
		}
		sr.metrics.LeaseTransferCount.Inc(1)

		// Finally, update our local copies of the descriptors so that if
		// additional transfers are needed we'll be making the decisions with more
		// up-to-date info. The StorePool copies are updated by transferLease.
		localDesc.Capacity.LeaseCount--
		localDesc.Capacity.QueriesPerSecond -= replWithStats.qps
		if otherDesc := storeMap[target.StoreID]; otherDesc != nil {
			otherDesc.Capacity.LeaseCount++
			otherDesc.Capacity.QueriesPerSecond += replWithStats.qps
		}
	}

	if !(localDesc.Capacity.QueriesPerSecond > qpsMaxThreshold) {
		log.Infof(ctx,
			"load-based lease transfers successfully brought s%d down to %.2f qps (mean=%.2f, upperThreshold=%.2f)",
			localDesc.StoreID, localDesc.Capacity.QueriesPerSecond, storeList.candidateQueriesPerSecond.mean, qpsMaxThreshold)
		return
	}

	if mode != LBRebalancingLeasesAndReplicas {
		log.Infof(ctx,
			"ran out of leases worth transferring and qps (%.2f) is still above desired threshold (%.2f)",
			localDesc.Capacity.QueriesPerSecond, qpsMaxThreshold)
		return
	}
	log.Infof(ctx,
		"ran out of leases worth transferring and qps (%.2f) is still above desired threshold (%.2f); considering load-based replica rebalances",
		localDesc.Capacity.QueriesPerSecond, qpsMaxThreshold)

	// Re-combine replicasToMaybeRebalance with what remains of hottestRanges so
	// that we'll reconsider them for replica rebalancing.
	replicasToMaybeRebalance = append(replicasToMaybeRebalance, hottestRanges...)

	for localDesc.Capacity.QueriesPerSecond > qpsMaxThreshold {
		replWithStats, targets := sr.chooseReplicaToRebalance(
			ctx,
			&replicasToMaybeRebalance,
			localDesc,
			storeList,
			storeMap,
			qpsMinThreshold,
			qpsMaxThreshold)
		if replWithStats.repl == nil {
			log.Infof(ctx,
				"ran out of replicas worth transferring and qps (%.2f) is still above desired threshold (%.2f); will check again soon",
				localDesc.Capacity.QueriesPerSecond, qpsMaxThreshold)
			return
		}

		descBeforeRebalance := replWithStats.repl.Desc()
		log.VEventf(ctx, 1, "rebalancing r%d (%.2f qps) from %v to %v to better balance load",
			replWithStats.repl.RangeID, replWithStats.qps, descBeforeRebalance.Replicas(), targets)
		timeout := sr.rq.processTimeoutFunc(sr.st, replWithStats.repl)
		if err := contextutil.RunWithTimeout(ctx, "relocate range", timeout, func(ctx context.Context) error {
			return sr.rq.store.AdminRelocateRange(ctx, *descBeforeRebalance, targets)
		}); err != nil {
			log.Errorf(ctx, "unable to relocate range to %v: %+v", targets, err)
			continue
		}
		sr.metrics.RangeRebalanceCount.Inc(1)

		// Finally, update our local copies of the descriptors so that if
		// additional transfers are needed we'll be making the decisions with more
		// up-to-date info.
		//
		// TODO(a-robinson): This just updates the copies used locally by the
		// storeRebalancer. We may also want to update the copies in the StorePool
		// itself.
		replicasBeforeRebalance := descBeforeRebalance.Replicas().All()
		for i := range replicasBeforeRebalance {
			if storeDesc := storeMap[replicasBeforeRebalance[i].StoreID]; storeDesc != nil {
				storeDesc.Capacity.RangeCount--
			}
		}
		localDesc.Capacity.LeaseCount--
		localDesc.Capacity.QueriesPerSecond -= replWithStats.qps
		for i := range targets {
			if storeDesc := storeMap[targets[i].StoreID]; storeDesc != nil {
				storeDesc.Capacity.RangeCount++
				if i == 0 {
					storeDesc.Capacity.LeaseCount++
					storeDesc.Capacity.QueriesPerSecond += replWithStats.qps
				}
			}
		}
	}

	log.Infof(ctx,
		"load-based replica transfers successfully brought s%d down to %.2f qps (mean=%.2f, upperThreshold=%.2f)",
		localDesc.StoreID, localDesc.Capacity.QueriesPerSecond, storeList.candidateQueriesPerSecond.mean, qpsMaxThreshold)
}

// TODO(a-robinson): Should we take the number of leases on each store into
// account here or just continue to let that happen in allocator.go?
func (sr *StoreRebalancer) chooseLeaseToTransfer(
	ctx context.Context,
	hottestRanges *[]replicaWithStats,
	localDesc *roachpb.StoreDescriptor,
	storeList StoreList,
	storeMap map[roachpb.StoreID]*roachpb.StoreDescriptor,
	minQPS float64,
	maxQPS float64,
) (replicaWithStats, roachpb.ReplicaDescriptor, []replicaWithStats) {
	var considerForRebalance []replicaWithStats
	now := sr.rq.store.Clock().Now()
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
			float64(localDesc.Capacity.LeaseCount) <= storeList.candidateLeases.mean {
			log.VEventf(ctx, 5, "r%d's %.2f qps is too little to matter relative to s%d's %.2f total qps",
				replWithStats.repl.RangeID, replWithStats.qps, localDesc.StoreID, localDesc.Capacity.QueriesPerSecond)
			continue
		}

		desc, zone := replWithStats.repl.DescAndZone()
		log.VEventf(ctx, 3, "considering lease transfer for r%d with %.2f qps",
			desc.RangeID, replWithStats.qps)

		// Check all the other replicas in order of increasing qps. Learner replicas
		// aren't allowed to become the leaseholder or raft leader, so only consider
		// the `Voters` replicas.
		candidates := desc.Replicas().DeepCopy().Voters()
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

		preferred := sr.rq.allocator.preferredLeaseholders(zone, candidates)
		for _, candidate := range candidates {
			if candidate.StoreID == localDesc.StoreID {
				continue
			}

			meanQPS := storeList.candidateQueriesPerSecond.mean
			if shouldNotMoveTo(ctx, storeMap, replWithStats, candidate.StoreID, meanQPS, minQPS, maxQPS) {
				continue
			}

			if raftStatus == nil {
				raftStatus = sr.getRaftStatusFn(replWithStats.repl)
			}
			if replicaIsBehind(raftStatus, candidate.ReplicaID) {
				log.VEventf(ctx, 3, "%v is behind or this store isn't the raft leader for r%d; raftStatus: %v",
					candidate, desc.RangeID, raftStatus)
				continue
			}

			if len(preferred) > 0 && !storeHasReplica(candidate.StoreID, preferred) {
				log.VEventf(ctx, 3, "s%d not a preferred leaseholder for r%d; preferred: %v",
					candidate.StoreID, desc.RangeID, preferred)
				continue
			}

			filteredStoreList := storeList.filter(zone.Constraints)
			if sr.rq.allocator.followTheWorkloadPrefersLocal(
				ctx,
				filteredStoreList,
				*localDesc,
				candidate.StoreID,
				candidates,
				replWithStats.repl.leaseholderStats,
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

func (sr *StoreRebalancer) chooseReplicaToRebalance(
	ctx context.Context,
	hottestRanges *[]replicaWithStats,
	localDesc *roachpb.StoreDescriptor,
	storeList StoreList,
	storeMap map[roachpb.StoreID]*roachpb.StoreDescriptor,
	minQPS float64,
	maxQPS float64,
) (replicaWithStats, []roachpb.ReplicationTarget) {
	now := sr.rq.store.Clock().Now()
	for {
		if len(*hottestRanges) == 0 {
			return replicaWithStats{}, nil
		}
		replWithStats := (*hottestRanges)[0]
		*hottestRanges = (*hottestRanges)[1:]

		if replWithStats.repl == nil {
			return replicaWithStats{}, nil
		}

		if shouldNotMoveAway(ctx, replWithStats, localDesc, now, minQPS) {
			continue
		}

		// Don't bother moving ranges whose QPS is below some small fraction of the
		// store's QPS (unless the store has extra ranges to spare anyway). It's
		// just unnecessary churn with no benefit to move ranges responsible for,
		// for example, 1 qps on a store with 5000 qps.
		const minQPSFraction = .001
		if replWithStats.qps < localDesc.Capacity.QueriesPerSecond*minQPSFraction &&
			float64(localDesc.Capacity.RangeCount) <= storeList.candidateRanges.mean {
			log.VEventf(ctx, 5, "r%d's %.2f qps is too little to matter relative to s%d's %.2f total qps",
				replWithStats.repl.RangeID, replWithStats.qps, localDesc.StoreID, localDesc.Capacity.QueriesPerSecond)
			continue
		}

		desc, zone := replWithStats.repl.DescAndZone()
		log.VEventf(ctx, 3, "considering replica rebalance for r%d with %.2f qps",
			desc.RangeID, replWithStats.qps)

		clusterNodes := sr.rq.allocator.storePool.ClusterNodeCount()
		desiredReplicas := GetNeededReplicas(*zone.NumReplicas, clusterNodes)
		targets := make([]roachpb.ReplicationTarget, 0, desiredReplicas)
		targetReplicas := make([]roachpb.ReplicaDescriptor, 0, desiredReplicas)
		currentReplicas := desc.Replicas().All()

		// Check the range's existing diversity score, since we want to ensure we
		// don't hurt locality diversity just to improve QPS.
		curDiversity := rangeDiversityScore(
			sr.rq.allocator.storePool.getLocalities(currentReplicas))

		// Check the existing replicas, keeping around those that aren't overloaded.
		for i := range currentReplicas {
			if currentReplicas[i].StoreID == localDesc.StoreID {
				continue
			}
			// Keep the replica in the range if we don't know its QPS or if its QPS
			// is below the upper threshold. Punishing stores not in our store map
			// could cause mass evictions if the storePool gets out of sync.
			storeDesc, ok := storeMap[currentReplicas[i].StoreID]
			if !ok || storeDesc.Capacity.QueriesPerSecond < maxQPS {
				targets = append(targets, roachpb.ReplicationTarget{
					NodeID:  currentReplicas[i].NodeID,
					StoreID: currentReplicas[i].StoreID,
				})
				targetReplicas = append(targetReplicas, roachpb.ReplicaDescriptor{
					NodeID:  currentReplicas[i].NodeID,
					StoreID: currentReplicas[i].StoreID,
				})
			}
		}

		// Then pick out which new stores to add the remaining replicas to.
		options := sr.rq.allocator.scorerOptions()
		options.qpsRebalanceThreshold = qpsRebalanceThreshold.Get(&sr.st.SV)
		for len(targets) < desiredReplicas {
			// Use the preexisting AllocateTarget logic to ensure that considerations
			// such as zone constraints, locality diversity, and full disk come
			// into play.
			target, _ := sr.rq.allocator.allocateTargetFromList(
				ctx,
				storeList,
				zone,
				targetReplicas,
				options,
			)
			if target == nil {
				log.VEventf(ctx, 3, "no rebalance targets found to replace the current store for r%d",
					desc.RangeID)
				break
			}

			meanQPS := storeList.candidateQueriesPerSecond.mean
			if shouldNotMoveTo(ctx, storeMap, replWithStats, target.StoreID, meanQPS, minQPS, maxQPS) {
				break
			}

			targets = append(targets, roachpb.ReplicationTarget{
				NodeID:  target.Node.NodeID,
				StoreID: target.StoreID,
			})
			targetReplicas = append(targetReplicas, roachpb.ReplicaDescriptor{
				NodeID:  target.Node.NodeID,
				StoreID: target.StoreID,
			})
		}

		// If we couldn't find enough valid targets, forget about this range.
		//
		// TODO(a-robinson): Support more incremental improvements -- move what we
		// can if it makes things better even if it isn't great. For example,
		// moving one of the other existing replicas that's on a store with less
		// qps than the max threshold but above the mean would help in certain
		// locality configurations.
		if len(targets) < desiredReplicas {
			log.VEventf(ctx, 3, "couldn't find enough rebalance targets for r%d (%d/%d)",
				desc.RangeID, len(targets), desiredReplicas)
			continue
		}
		newDiversity := rangeDiversityScore(sr.rq.allocator.storePool.getLocalities(targetReplicas))
		if newDiversity < curDiversity {
			log.VEventf(ctx, 3,
				"new diversity %.2f for r%d worse than current diversity %.2f; not rebalancing",
				newDiversity, desc.RangeID, curDiversity)
			continue
		}

		// Pick the replica with the least QPS to be leaseholder;
		// RelocateRange transfers the lease to the first provided target.
		newLeaseIdx := 0
		newLeaseQPS := math.MaxFloat64
		var raftStatus *raft.Status
		for i := 0; i < len(targets); i++ {
			// Ensure we don't transfer the lease to an existing replica that is behind
			// in processing its raft log.
			if replica, ok := desc.GetReplicaDescriptor(targets[i].StoreID); ok {
				if raftStatus == nil {
					raftStatus = sr.getRaftStatusFn(replWithStats.repl)
				}
				if replicaIsBehind(raftStatus, replica.ReplicaID) {
					continue
				}
			}

			storeDesc, ok := storeMap[targets[i].StoreID]
			if ok && storeDesc.Capacity.QueriesPerSecond < newLeaseQPS {
				newLeaseIdx = i
				newLeaseQPS = storeDesc.Capacity.QueriesPerSecond
			}
		}
		targets[0], targets[newLeaseIdx] = targets[newLeaseIdx], targets[0]
		return replWithStats, targets
	}
}

func shouldNotMoveAway(
	ctx context.Context,
	replWithStats replicaWithStats,
	localDesc *roachpb.StoreDescriptor,
	now hlc.Timestamp,
	minQPS float64,
) bool {
	if !replWithStats.repl.OwnsValidLease(now) {
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

func shouldNotMoveTo(
	ctx context.Context,
	storeMap map[roachpb.StoreID]*roachpb.StoreDescriptor,
	replWithStats replicaWithStats,
	candidateStore roachpb.StoreID,
	meanQPS float64,
	minQPS float64,
	maxQPS float64,
) bool {
	storeDesc, ok := storeMap[candidateStore]
	if !ok {
		log.VEventf(ctx, 3, "missing store descriptor for s%d", candidateStore)
		return true
	}

	newCandidateQPS := storeDesc.Capacity.QueriesPerSecond + replWithStats.qps
	if storeDesc.Capacity.QueriesPerSecond < minQPS {
		if newCandidateQPS > maxQPS {
			log.VEventf(ctx, 3,
				"r%d's %.2f qps would push s%d over the max threshold (%.2f) with %.2f qps afterwards",
				replWithStats.repl.RangeID, replWithStats.qps, candidateStore, maxQPS, newCandidateQPS)
			return true
		}
	} else if newCandidateQPS > meanQPS {
		log.VEventf(ctx, 3,
			"r%d's %.2f qps would push s%d over the mean (%.2f) with %.2f qps afterwards",
			replWithStats.repl.RangeID, replWithStats.qps, candidateStore, meanQPS, newCandidateQPS)
		return true
	}

	return false
}

func storeListToMap(sl StoreList) map[roachpb.StoreID]*roachpb.StoreDescriptor {
	storeMap := make(map[roachpb.StoreID]*roachpb.StoreDescriptor)
	for i := range sl.stores {
		storeMap[sl.stores[i].StoreID] = &sl.stores[i]
	}
	return storeMap
}

// jitteredInterval returns a randomly jittered (+/-25%) duration
// from checkInterval.
func jitteredInterval(interval time.Duration) time.Duration {
	return time.Duration(float64(interval) * (0.75 + 0.5*rand.Float64()))
}
