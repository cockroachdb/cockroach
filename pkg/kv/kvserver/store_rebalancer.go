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
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/contextutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"go.etcd.io/etcd/raft/v3"
)

const (
	// storeRebalancerTimerDuration is how frequently to check the store-level
	// balance of the cluster.
	storeRebalancerTimerDuration = time.Minute

	// minQPSThresholdDifference is the minimum QPS difference from the cluster
	// mean that this system should care about. In other words, we won't worry
	// about rebalancing for QPS reasons if a store's QPS differs from the mean by
	// less than this amount even if the amount is greater than the percentage
	// threshold. This avoids too many lease transfers / range rebalances in
	// lightly loaded clusters.
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
var LoadBasedRebalancingMode = settings.RegisterEnumSetting(
	settings.SystemOnly,
	"kv.allocator.load_based_rebalancing",
	"whether to rebalance based on the distribution of QPS across stores",
	"leases and replicas",
	map[int64]string{
		int64(LBRebalancingOff):               "off",
		int64(LBRebalancingLeasesOnly):        "leases",
		int64(LBRebalancingLeasesAndReplicas): "leases and replicas",
	},
).WithPublic()

// qpsRebalanceThreshold is much like rangeRebalanceThreshold, but for
// QPS rather than range count. This should be set higher than
// rangeRebalanceThreshold because QPS can naturally vary over time as
// workloads change and clients come and go, so we need to be a little more
// forgiving to avoid thrashing.
var qpsRebalanceThreshold = func() *settings.FloatSetting {
	s := settings.RegisterFloatSetting(
		settings.SystemOnly,
		"kv.allocator.qps_rebalance_threshold",
		"minimum fraction away from the mean a store's QPS (such as queries per second) can be before it is considered overfull or underfull",
		0.25,
		settings.NonNegativeFloat,
	)
	s.SetVisibility(settings.Public)
	return s
}()

// minQPSDifferenceForTransfers is the minimum QPS difference that the store
// rebalancer would care to reconcile (via lease or replica rebalancing) between
// any two stores.
//
// NB: This value is used to compare the QPS of two stores _without accounting_
// for the QPS of the replica or lease that is being considered for the
// transfer. This is set to be twice the minimum threshold that a store needs to
// be above or below the mean to be considered overfull or underfull
// respectively. This is to make lease and replica transfers less sensitive to
// the jitters in any given workload.
var minQPSDifferenceForTransfers = func() *settings.FloatSetting {
	s := settings.RegisterFloatSetting(
		settings.SystemOnly,
		"kv.allocator.min_qps_difference_for_transfers",
		"the minimum qps difference that must exist between any two stores"+
			" for the allocator to allow a lease or replica transfer between them",
		2*minQPSThresholdDifference,
		settings.NonNegativeFloat,
	)
	s.SetVisibility(settings.Reserved)
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
	_ = stopper.RunAsyncTask(ctx, "store-rebalancer", func(ctx context.Context) {
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

			storeList, _, _ := sr.rq.allocator.storePool.getStoreList(storeFilterSuspect)
			sr.rebalanceStore(ctx, mode, storeList)
		}
	})
}

// NB: The StoreRebalancer only cares about the convergence of QPS across
// stores, not the convergence of range count. So, we don't use the allocator's
// `scorerOptions` here, which sets the range count rebalance threshold.
// Instead, we use our own implementation of `scorerOptions` that promotes QPS
// balance.
func (sr *StoreRebalancer) scorerOptions() *qpsScorerOptions {
	return &qpsScorerOptions{
		deterministic:         sr.rq.allocator.storePool.deterministic,
		qpsRebalanceThreshold: qpsRebalanceThreshold.Get(&sr.st.SV),
		minRequiredQPSDiff:    minQPSDifferenceForTransfers.Get(&sr.st.SV),
	}
}

// rebalanceStore iterates through the top K hottest ranges on this store and
// for each such range, performs a lease transfer if it determines that that
// will improve QPS balance across the stores in the cluster. After it runs out
// of leases to transfer away (i.e. because it couldn't find better
// replacements), it considers these ranges for replica rebalancing.
//
// TODO(aayush): We don't try to move replicas or leases away from the local
// store unless it is fielding more than the overfull threshold of QPS based off
// of all the stores in the cluster. Is this desirable? Should we be more
// aggressive?
func (sr *StoreRebalancer) rebalanceStore(
	ctx context.Context, mode LBRebalancingMode, allStoresList StoreList,
) {
	options := sr.scorerOptions()
	var localDesc *roachpb.StoreDescriptor
	for i := range allStoresList.stores {
		if allStoresList.stores[i].StoreID == sr.rq.store.StoreID() {
			localDesc = &allStoresList.stores[i]
			break
		}
	}
	if localDesc == nil {
		log.Warningf(ctx, "StorePool missing descriptor for local store")
		return
	}

	// We only bother rebalancing stores that are fielding more than the
	// cluster-level overfull threshold of QPS.
	qpsMaxThreshold := overfullQPSThreshold(options, allStoresList.candidateQueriesPerSecond.mean)
	if !(localDesc.Capacity.QueriesPerSecond > qpsMaxThreshold) {
		log.Infof(ctx, "local QPS %.2f is below max threshold %.2f (mean=%.2f); no rebalancing needed",
			localDesc.Capacity.QueriesPerSecond, qpsMaxThreshold, allStoresList.candidateQueriesPerSecond.mean)
		return
	}

	var replicasToMaybeRebalance []replicaWithStats
	storeMap := storeListToMap(allStoresList)

	// First check if we should transfer leases away to better balance QPS.
	log.Infof(ctx,
		"considering load-based lease transfers for s%d with %.2f qps (mean=%.2f, upperThreshold=%.2f)",
		localDesc.StoreID, localDesc.Capacity.QueriesPerSecond, allStoresList.candidateQueriesPerSecond.mean, qpsMaxThreshold)
	hottestRanges := sr.replRankings.topQPS()
	for localDesc.Capacity.QueriesPerSecond > qpsMaxThreshold {
		replWithStats, target, considerForRebalance := sr.chooseLeaseToTransfer(
			ctx,
			&hottestRanges,
			localDesc,
			allStoresList,
			storeMap,
		)
		replicasToMaybeRebalance = append(replicasToMaybeRebalance, considerForRebalance...)
		if replWithStats.repl == nil {
			break
		}

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
			localDesc.StoreID, localDesc.Capacity.QueriesPerSecond, allStoresList.candidateQueriesPerSecond.mean, qpsMaxThreshold)
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
		replWithStats, voterTargets, nonVoterTargets := sr.chooseRangeToRebalance(
			ctx,
			&replicasToMaybeRebalance,
			localDesc,
			allStoresList,
			sr.scorerOptions(),
		)
		if replWithStats.repl == nil {
			log.Infof(ctx,
				"ran out of replicas worth transferring and qps (%.2f) is still above desired threshold (%.2f); will check again soon",
				localDesc.Capacity.QueriesPerSecond, qpsMaxThreshold)
			return
		}

		descBeforeRebalance := replWithStats.repl.Desc()
		log.VEventf(
			ctx,
			1,
			"rebalancing r%d (%.2f qps) to better balance load: voters from %v to %v; non-voters from %v to %v",
			replWithStats.repl.RangeID,
			replWithStats.qps,
			descBeforeRebalance.Replicas().Voters(),
			voterTargets,
			descBeforeRebalance.Replicas().NonVoters(),
			nonVoterTargets,
		)

		timeout := sr.rq.processTimeoutFunc(sr.st, replWithStats.repl)
		if err := contextutil.RunWithTimeout(ctx, "relocate range", timeout, func(ctx context.Context) error {
			return sr.rq.store.DB().AdminRelocateRange(
				ctx,
				descBeforeRebalance.StartKey.AsRawKey(),
				voterTargets,
				nonVoterTargets,
				true, /* transferLeaseToFirstVoter */
			)
		}); err != nil {
			log.Errorf(ctx, "unable to relocate range to %v: %+v", voterTargets, err)
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
		replicasBeforeRebalance := descBeforeRebalance.Replicas().Descriptors()
		for i := range replicasBeforeRebalance {
			if storeDesc := storeMap[replicasBeforeRebalance[i].StoreID]; storeDesc != nil {
				storeDesc.Capacity.RangeCount--
			}
		}
		localDesc.Capacity.LeaseCount--
		localDesc.Capacity.QueriesPerSecond -= replWithStats.qps
		for i := range voterTargets {
			if storeDesc := storeMap[voterTargets[i].StoreID]; storeDesc != nil {
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
		localDesc.StoreID, localDesc.Capacity.QueriesPerSecond, allStoresList.candidateQueriesPerSecond.mean, qpsMaxThreshold)
}

func (sr *StoreRebalancer) chooseLeaseToTransfer(
	ctx context.Context,
	hottestRanges *[]replicaWithStats,
	localDesc *roachpb.StoreDescriptor,
	storeList StoreList,
	storeMap map[roachpb.StoreID]*roachpb.StoreDescriptor,
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

		if !replWithStats.repl.OwnsValidLease(ctx, now) {
			log.VEventf(ctx, 3, "store doesn't own the lease for r%d", replWithStats.repl.RangeID)
			continue
		}

		// Don't bother moving leases whose QPS is below some small fraction of the
		// store's QPS. It's just unnecessary churn with no benefit to move leases
		// responsible for, for example, 1 qps on a store with 5000 qps.
		const minQPSFraction = .001
		if replWithStats.qps < localDesc.Capacity.QueriesPerSecond*minQPSFraction {
			log.VEventf(ctx, 3, "r%d's %.2f qps is too little to matter relative to s%d's %.2f total qps",
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

		// Only consider replicas that are not lagging behind the leader in order to
		// avoid hurting QPS in the short term. This is a stronger check than what
		// `TransferLeaseTarget` performs (it only excludes replicas that are
		// waiting for a snapshot).
		candidates = filterBehindReplicas(ctx, sr.getRaftStatusFn(replWithStats.repl), candidates)

		candidate := sr.rq.allocator.TransferLeaseTarget(
			ctx,
			conf,
			candidates,
			replWithStats.repl,
			replWithStats.repl.leaseholderStats,
			true, /* forceDecisionWithoutStats */
			transferLeaseOptions{
				goal:                     qpsConvergence,
				checkTransferLeaseSource: true,
			},
		)

		if candidate == (roachpb.ReplicaDescriptor{}) {
			log.VEventf(
				ctx,
				3,
				"could not find a better lease transfer target for r%d; considering replica rebalance instead",
				desc.RangeID,
			)
			considerForRebalance = append(considerForRebalance, replWithStats)
			continue
		}

		filteredStoreList := storeList.excludeInvalid(conf.Constraints)
		filteredStoreList = storeList.excludeInvalid(conf.VoterConstraints)
		if sr.rq.allocator.followTheWorkloadPrefersLocal(
			ctx,
			filteredStoreList,
			*localDesc,
			candidate.StoreID,
			candidates,
			replWithStats.repl.leaseholderStats,
		) {
			log.VEventf(
				ctx, 3, "r%d is on s%d due to follow-the-workload; considering replica rebalance instead",
				desc.RangeID, localDesc.StoreID,
			)
			considerForRebalance = append(considerForRebalance, replWithStats)
			continue
		}
		if targetStore, ok := storeMap[candidate.StoreID]; ok {
			log.VEventf(
				ctx,
				1,
				"transferring lease for r%d (qps=%.2f) to store s%d (qps=%.2f) from local store s%d (qps=%.2f)",
				desc.RangeID,
				replWithStats.qps,
				targetStore.StoreID,
				targetStore.Capacity.QueriesPerSecond,
				localDesc.StoreID,
				localDesc.Capacity.QueriesPerSecond,
			)
		}
		return replWithStats, candidate, considerForRebalance
	}
}

// rangeRebalanceContext represents a snapshot of a replicas's state along with
// the state of the cluster during the StoreRebalancer's attempt to rebalance it
// based on QPS.
type rangeRebalanceContext struct {
	replWithStats replicaWithStats
	rangeDesc     *roachpb.RangeDescriptor
	conf          roachpb.SpanConfig
}

func (sr *StoreRebalancer) chooseRangeToRebalance(
	ctx context.Context,
	hottestRanges *[]replicaWithStats,
	localDesc *roachpb.StoreDescriptor,
	allStoresList StoreList,
	options *qpsScorerOptions,
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

		// Don't bother moving ranges whose QPS is below some small fraction of the
		// store's QPS. It's just unnecessary churn with no benefit to move ranges
		// responsible for, for example, 1 qps on a store with 5000 qps.
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

		rangeDesc, conf := replWithStats.repl.DescAndSpanConfig()
		clusterNodes := sr.rq.allocator.storePool.ClusterNodeCount()
		numDesiredVoters := GetNeededVoters(conf.GetNumVoters(), clusterNodes)
		numDesiredNonVoters := GetNeededNonVoters(numDesiredVoters, int(conf.GetNumNonVoters()), clusterNodes)
		if expected, actual := numDesiredVoters, len(rangeDesc.Replicas().VoterDescriptors()); expected != actual {
			log.VEventf(
				ctx,
				3,
				"r%d is either over or under replicated (expected %d voters, found %d); ignoring",
				rangeDesc.RangeID,
				expected,
				actual,
			)
			continue
		}
		if expected, actual := numDesiredNonVoters, len(rangeDesc.Replicas().NonVoterDescriptors()); expected != actual {
			log.VEventf(
				ctx,
				3,
				"r%d is either over or under replicated (expected %d non-voters, found %d); ignoring",
				rangeDesc.RangeID,
				expected,
				actual,
			)
			continue
		}
		rebalanceCtx := rangeRebalanceContext{
			replWithStats: replWithStats,
			rangeDesc:     rangeDesc,
			conf:          conf,
		}

		// We ascribe the leaseholder's QPS to every follower replica. The store
		// rebalancer first attempts to transfer the leases of its hot ranges away
		// in `chooseLeaseToTransfer`. If it cannot move enough leases away to bring
		// down the store's QPS below the cluster-level overfullness threshold, it
		// moves on to rebalancing replicas. In other words, for every hot range on
		// the store, the StoreRebalancer first tries moving the load away to one of
		// its existing replicas but then tries to reconfigure the range (i.e. move
		// the range to a different set of stores) to _then_ hopefully succeed in
		// moving the lease away to another replica.
		//
		// Thus, we ideally want to base our replica rebalancing on the assumption
		// that all of the load from the leaseholder's replica is going to shift to
		// the new store that we end up rebalancing to.
		options.qpsPerReplica = replWithStats.qps

		if !replWithStats.repl.OwnsValidLease(ctx, now) {
			log.VEventf(ctx, 3, "store doesn't own the lease for r%d", replWithStats.repl.RangeID)
			continue
		}

		log.VEventf(
			ctx,
			3,
			"considering replica rebalance for r%d with %.2f qps",
			replWithStats.repl.GetRangeID(),
			replWithStats.qps,
		)

		targetVoterRepls, targetNonVoterRepls, foundRebalance := sr.getRebalanceTargetsBasedOnQPS(
			ctx,
			rebalanceCtx,
			options,
		)

		if !foundRebalance {
			// Bail if there are no stores that are better for the existing replicas.
			// If the range needs a lease transfer to enable better load distribution,
			// it will be handled by the logic in `chooseLeaseToTransfer()`.
			log.VEventf(ctx, 3, "could not find rebalance opportunities for r%d", replWithStats.repl.RangeID)
			continue
		}

		storeDescMap := storeListToMap(allStoresList)

		// Pick the voter with the least QPS to be leaseholder;
		// RelocateRange transfers the lease to the first provided target.
		//
		// TODO(aayush): Does this logic need to exist? This logic does not take
		// lease preferences into account. So it is already broken in a way.
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
				if replicaIsBehind(raftStatus, replica.ReplicaID) {
					continue
				}
			}

			storeDesc, ok := storeDescMap[targetVoterRepls[i].StoreID]
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

// getRebalanceTargetsBasedOnQPS returns a list of rebalance targets for
// voting and non-voting replicas on the range that match the relevant
// constraints on the range and would further the goal of balancing the QPS on
// the stores in this cluster.
func (sr *StoreRebalancer) getRebalanceTargetsBasedOnQPS(
	ctx context.Context, rbCtx rangeRebalanceContext, options scorerOptions,
) (finalVoterTargets, finalNonVoterTargets []roachpb.ReplicaDescriptor, foundRebalance bool) {
	finalVoterTargets = rbCtx.rangeDesc.Replicas().VoterDescriptors()
	finalNonVoterTargets = rbCtx.rangeDesc.Replicas().NonVoterDescriptors()

	// NB: We attempt to rebalance N times for N replicas as we may want to
	// replace all of them (they could all be on suboptimal stores).
	for i := 0; i < len(finalVoterTargets); i++ {
		// TODO(aayush): Figure out a way to plumb the `details` here into
		// `AdminRelocateRange` so that these decisions show up in system.rangelog
		add, remove, _, shouldRebalance := sr.rq.allocator.rebalanceTarget(
			ctx,
			rbCtx.conf,
			rbCtx.replWithStats.repl.RaftStatus(),
			finalVoterTargets,
			finalNonVoterTargets,
			rangeUsageInfoForRepl(rbCtx.replWithStats.repl),
			storeFilterSuspect,
			voterTarget,
			options,
		)
		if !shouldRebalance {
			log.VEventf(
				ctx,
				3,
				"no more rebalancing opportunities for r%d voters that improve QPS balance",
				rbCtx.rangeDesc.RangeID,
			)
			break
		} else {
			// Record the fact that we found at least one rebalance opportunity.
			foundRebalance = true
		}
		log.VEventf(
			ctx,
			3,
			"rebalancing voter (qps=%.2f) for r%d on %v to %v in order to improve QPS balance",
			rbCtx.replWithStats.qps,
			rbCtx.rangeDesc.RangeID,
			remove,
			add,
		)

		afterVoters := make([]roachpb.ReplicaDescriptor, 0, len(finalVoterTargets))
		afterNonVoters := make([]roachpb.ReplicaDescriptor, 0, len(finalNonVoterTargets))
		for _, voter := range finalVoterTargets {
			if voter.StoreID == remove.StoreID {
				afterVoters = append(
					afterVoters, roachpb.ReplicaDescriptor{
						StoreID: add.StoreID,
						NodeID:  add.NodeID,
					})
			} else {
				afterVoters = append(afterVoters, voter)
			}
		}
		// Voters are allowed to relocate to stores that have non-voters, which may
		// displace them.
		for _, nonVoter := range finalNonVoterTargets {
			if nonVoter.StoreID == add.StoreID {
				afterNonVoters = append(afterNonVoters, roachpb.ReplicaDescriptor{
					StoreID: remove.StoreID,
					NodeID:  remove.NodeID,
				})
			} else {
				afterNonVoters = append(afterNonVoters, nonVoter)
			}
		}
		// Pretend that we've executed upon this rebalancing decision.
		finalVoterTargets = afterVoters
		finalNonVoterTargets = afterNonVoters
	}

	for i := 0; i < len(finalNonVoterTargets); i++ {
		add, remove, _, shouldRebalance := sr.rq.allocator.rebalanceTarget(
			ctx,
			rbCtx.conf,
			rbCtx.replWithStats.repl.RaftStatus(),
			finalVoterTargets,
			finalNonVoterTargets,
			rangeUsageInfoForRepl(rbCtx.replWithStats.repl),
			storeFilterSuspect,
			nonVoterTarget,
			options,
		)
		if !shouldRebalance {
			log.VEventf(
				ctx,
				3,
				"no more rebalancing opportunities for r%d non-voters that improve QPS balance",
				rbCtx.rangeDesc.RangeID,
			)
			break
		} else {
			// Record the fact that we found at least one rebalance opportunity.
			foundRebalance = true
		}
		log.VEventf(
			ctx,
			3,
			"rebalancing non-voter (qps=%.2f) for r%d on %v to %v in order to improve QPS balance",
			rbCtx.replWithStats.qps,
			rbCtx.rangeDesc.RangeID,
			remove,
			add,
		)
		var newNonVoters []roachpb.ReplicaDescriptor
		for _, nonVoter := range finalNonVoterTargets {
			if nonVoter.StoreID == remove.StoreID {
				newNonVoters = append(
					newNonVoters, roachpb.ReplicaDescriptor{
						StoreID: add.StoreID,
						NodeID:  add.NodeID,
					})
			} else {
				newNonVoters = append(newNonVoters, nonVoter)
			}
		}
		// Pretend that we've executed upon this rebalancing decision.
		finalNonVoterTargets = newNonVoters
	}
	return finalVoterTargets, finalNonVoterTargets, foundRebalance
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
