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

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/allocatorimpl"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/storepool"
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

// RebalanceSearchOutcome returns the result of a rebalance target search. It
// is used to determine whether to transition from lease to range based
// rebalancing, exit early or apply a rebalancing action if a target is found.
type RebalanceSearchOutcome int

const (
	// NoRebalanceNeeded indicates that the state of the local store is within
	// the target goal for QPS and does not need rebalancing.
	NoRebalanceNeeded RebalanceSearchOutcome = iota
	// NoRebalanceTarget indicates that the state of the local store is outside
	// of the target goal for QPS however no rebalancing opportunities were
	// found.
	NoRebalanceTarget
	// RebalanceTargetFound indicates that the state local store is outside of
	// the target goal for QPS and a rebalance target was found
	RebalanceTargetFound
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
	metrics          StoreRebalancerMetrics
	st               *cluster.Settings
	storeID          roachpb.StoreID
	allocator        allocatorimpl.Allocator
	rr               RangeRebalancer
	replicaRankings  *ReplicaRankings
	getRaftStatusFn  func(replica CandidateReplica) *raft.Status
	processTimeoutFn func(replica CandidateReplica) time.Duration
}

// NewStoreRebalancer creates a StoreRebalancer to work in tandem with the
// provided replicateQueue.
func NewStoreRebalancer(
	ambientCtx log.AmbientContext, st *cluster.Settings, rq *replicateQueue, rr *ReplicaRankings,
) *StoreRebalancer {
	sr := &StoreRebalancer{
		AmbientContext:  ambientCtx,
		metrics:         makeStoreRebalancerMetrics(),
		st:              st,
		storeID:         rq.store.StoreID(),
		rr:              rq,
		allocator:       rq.allocator,
		replicaRankings: rr,
		getRaftStatusFn: func(replica CandidateReplica) *raft.Status {
			return replica.RaftStatus()
		},
		processTimeoutFn: func(replica CandidateReplica) time.Duration {
			return rq.processTimeoutFunc(st, replica.Repl())
		},
	}
	sr.AddLogTag("store-rebalancer", nil)
	rq.store.metrics.registry.AddMetricStruct(&sr.metrics)
	return sr
}

// SimulatorStoreRebalancer returns a StoreRebalancer with the given function
// pointers, for the purposes of breaking dependencies on real code paths.
func SimulatorStoreRebalancer(
	storeID roachpb.StoreID,
	alocator allocatorimpl.Allocator,
	getRaftStatusFn func(replica CandidateReplica) *raft.Status,
) *StoreRebalancer {
	sr := &StoreRebalancer{
		AmbientContext:  log.MakeTestingAmbientCtxWithNewTracer(),
		metrics:         makeStoreRebalancerMetrics(),
		st:              &cluster.Settings{},
		storeID:         storeID,
		allocator:       alocator,
		getRaftStatusFn: getRaftStatusFn,
	}

	return sr
}

// RebalanceContext maintains the current state for calls to rebalanceStore. It
// should be discarded and a new one created on each call.
type RebalanceContext struct {
	LocalDesc                          *roachpb.StoreDescriptor
	QPSMaxThreshold                    float64
	options                            *allocatorimpl.QPSScorerOptions
	mode                               LBRebalancingMode
	allStoresList                      storepool.StoreList
	hottestRanges, rebalanceCandidates []CandidateReplica
}

// NewRebalanceContext returns a new RebalanceContext struct, using the given
// fields. This method is exported to init this struct outside of this package,
// currently only the asim pkg.
func NewRebalanceContext(
	localDesc *roachpb.StoreDescriptor,
	options *allocatorimpl.QPSScorerOptions,
	mode LBRebalancingMode,
	qpsMaxThreshold float64,
	allStoresList storepool.StoreList,
	hottestRanges, rebalanceCandidates []CandidateReplica,
) *RebalanceContext {
	return &RebalanceContext{
		LocalDesc:           localDesc,
		options:             options,
		mode:                mode,
		QPSMaxThreshold:     qpsMaxThreshold,
		allStoresList:       allStoresList,
		hottestRanges:       hottestRanges,
		rebalanceCandidates: rebalanceCandidates,
	}
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
		timer.Reset(jitteredInterval(allocator.LoadBasedRebalanceInterval.Get(&sr.st.SV)))
		for {
			// Wait out the first tick before doing anything since the store is still
			// starting up and we might as well wait for some qps/wps stats to
			// accumulate.
			select {
			case <-stopper.ShouldQuiesce():
				return
			case <-timer.C:
				timer.Read = true
				timer.Reset(jitteredInterval(allocator.LoadBasedRebalanceInterval.Get(&sr.st.SV)))
			}

			mode := LBRebalancingMode(LoadBasedRebalancingMode.Get(&sr.st.SV))
			if mode == LBRebalancingOff {
				continue
			}

			rctx := sr.makeRebalanceContext(ctx)
			sr.rebalanceStore(ctx, rctx)
		}
	})
}

// NB: The StoreRebalancer only cares about the convergence of QPS across
// stores, not the convergence of range count. So, we don't use the allocator's
// `scorerOptions` here, which sets the range count rebalance threshold.
// Instead, we use our own implementation of `scorerOptions` that promotes QPS
// balance.
func (sr *StoreRebalancer) scorerOptions(ctx context.Context) *allocatorimpl.QPSScorerOptions {
	return &allocatorimpl.QPSScorerOptions{
		StoreHealthOptions:    sr.allocator.StoreHealthOptions(ctx),
		Deterministic:         sr.allocator.StorePool.Deterministic,
		QPSRebalanceThreshold: allocator.QPSRebalanceThreshold.Get(&sr.st.SV),
		MinRequiredQPSDiff:    allocator.MinQPSDifferenceForTransfers.Get(&sr.st.SV),
	}
}

func (sr *StoreRebalancer) makeRebalanceContext(ctx context.Context) *RebalanceContext {
	allStoresList, _, _ := sr.allocator.StorePool.GetStoreList(storepool.StoreFilterSuspect)

	// Find the store descriptor for the local store.
	var localDesc *roachpb.StoreDescriptor
	for i := range allStoresList.Stores {
		if allStoresList.Stores[i].StoreID == sr.storeID {
			localDesc = &allStoresList.Stores[i]
			break
		}
	}

	if localDesc == nil {
		log.KvDistribution.Warningf(
			ctx,
			"StorePool missing descriptor for local store with ID %d, store list %v",
			sr.storeID,
			allStoresList,
		)
		return nil
	}

	options := sr.scorerOptions(ctx)

	return &RebalanceContext{
		LocalDesc:           localDesc,
		options:             options,
		mode:                LBRebalancingMode(LoadBasedRebalancingMode.Get(&sr.st.SV)),
		QPSMaxThreshold:     allocatorimpl.OverfullQPSThreshold(options, allStoresList.CandidateQueriesPerSecond.Mean),
		allStoresList:       allStoresList,
		hottestRanges:       sr.replicaRankings.TopQPS(),
		rebalanceCandidates: []CandidateReplica{},
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
func (sr *StoreRebalancer) rebalanceStore(ctx context.Context, rctx *RebalanceContext) {
	// Check whether the store should be rebalanced first, if not exit early
	// before searching for targets.
	if !sr.ShouldRebalanceStore(ctx, rctx) {
		return
	}
	// This store exceeds the max threshold, we should attempt to find
	// rebalance targets.
	// (1) Search for lease transfer targets for the hottest leases we
	//     hold.
	// (2) Search for replica rebalances for ranges this store holds a
	//     lease for. Where the lowest QPS store that ends up with a voting
	//     replica will have the lease transferred to it.

	// Phase (1) Hot Lease Rebalancing Loop:
	// (1) If local QPS <= maximum QPS then goto (5)
	// (2) Find best lease to transfer away, if none exists goto step (5)
	// (3) Transfer best lease away, if unsuccessful goto (1)
	// (4) Update local view of cluster state (local QPS), then goto step (1)
	// (5) Terminate loop.
	for {
		outcome, candidateReplica, target := sr.RebalanceLeases(ctx, rctx)
		if outcome == NoRebalanceNeeded {
			break
		}
		if outcome == NoRebalanceTarget {
			break
		}
		if ok := sr.applyLeaseRebalance(ctx, candidateReplica, target); ok {
			sr.PostLeaseRebalance(rctx, candidateReplica, target)
		}
	}

	// Check whether we should continue to range based rebalancing, after
	// trying lease based rebalancing.
	shouldRebalanceRanges := sr.TransferToRebalanceRanges(ctx, rctx)
	if !shouldRebalanceRanges {
		return
	}

	// Phase (2) Hot Range Rebalancing Loop:
	// (1) If local QPS <= maximum QPS then goto (5)
	// (2) Find best range to change its replicas and transfer lease for, if none exists goto (5)
	// (3) RelocateRange and transfer lease to lowest QPS replica, if unsuccessful goto (1)
	// (4) Update local view of cluster state (local QPS), then goto step (1)
	// (5) Terminate loop.
	for {
		outcome, candidateReplica, voterTargets, nonVoterTargets := sr.RebalanceRanges(ctx, rctx)
		if outcome == NoRebalanceNeeded {
			break
		}
		if outcome == NoRebalanceTarget {
			break
		}
		oldVoters := candidateReplica.Desc().Replicas().VoterDescriptors()
		oldNonVoters := candidateReplica.Desc().Replicas().NonVoterDescriptors()
		if ok := sr.applyRangeRebalance(ctx, candidateReplica, voterTargets, nonVoterTargets); ok {
			sr.PostRangeRebalance(rctx, candidateReplica, voterTargets, nonVoterTargets, oldVoters, oldNonVoters)
		}
	}
	// Log the range rebalancing outcome, we ignore whether we were succesful
	// or not, as it doesn't change the period we will wait before searching
	// for balancing targets again.
	sr.LogRangeRebalanceOutcome(ctx, rctx)
}

// ShouldRebalanceStore determines whether the store should be rebalanced - if
// so it returns true. If the store doesn't require rebalancing, we log the
// reason and return false.
func (sr *StoreRebalancer) ShouldRebalanceStore(ctx context.Context, rctx *RebalanceContext) bool {
	// When there is no local store descriptor found, it is possible for a nil
	// rctx. In this case, we can't rebalance the store so we bail out.
	if rctx == nil {
		log.KvDistribution.Warningf(ctx,
			"no rebalance context given, bailing out of rebalancing store, will try again later")
		return false
	}
	// We only bother rebalancing stores that are fielding more than the
	// cluster-level overfull threshold of QPS.
	if !(rctx.LocalDesc.Capacity.QueriesPerSecond > rctx.QPSMaxThreshold) {
		// Since the lack of activity is the most common case, we don't
		// log externally by default. Only show the details when
		// requested by log config or when looking at traces.
		log.KvDistribution.VEventf(
			ctx, 1,
			"local QPS %.2f is below max threshold %.2f (mean=%.2f); no rebalancing needed",
			rctx.LocalDesc.Capacity.QueriesPerSecond, rctx.QPSMaxThreshold, rctx.allStoresList.CandidateQueriesPerSecond.Mean)
		return false
	}
	// NB: This log statement is included here so that it will be covered by
	// the simulator, in practice we currently always move into lease
	// rebalancing first - however this may change in the future.
	log.KvDistribution.Infof(ctx,
		"considering load-based lease transfers for s%d with %.2f qps (mean=%.2f, upperThreshold=%.2f)",
		rctx.LocalDesc.StoreID, rctx.LocalDesc.Capacity.QueriesPerSecond, rctx.allStoresList.CandidateQueriesPerSecond.Mean, rctx.QPSMaxThreshold)
	return true
}

// RebalanceLeases searches for lease rebalancing opportunties, it will return
// the outcome RebalanceTargetFound, if there are valid lease transfer targets,
// NoRebalanceTarget if there are no rebalance targets but the QPS still
// exceeds the threshold and NoRebalanceNeeded if the threshold is not
// exceeded.
func (sr *StoreRebalancer) RebalanceLeases(
	ctx context.Context, rctx *RebalanceContext,
) (
	outcome RebalanceSearchOutcome,
	candidateReplica CandidateReplica,
	target roachpb.ReplicaDescriptor,
) {
	if rctx.LocalDesc.Capacity.QueriesPerSecond <= rctx.QPSMaxThreshold {
		return NoRebalanceNeeded, candidateReplica, target
	}

	//log.Infof(ctx, "storemap state %v\n storepool state %v\n", rctx.)

	candidateReplica, target, considerForRebalance := sr.chooseLeaseToTransfer(
		ctx,
		rctx,
	)

	rctx.rebalanceCandidates = append(rctx.rebalanceCandidates, considerForRebalance...)

	if candidateReplica == nil {
		return NoRebalanceTarget, candidateReplica, target
	}

	return RebalanceTargetFound, candidateReplica, target
}

func (sr *StoreRebalancer) applyLeaseRebalance(
	ctx context.Context, candidateReplica CandidateReplica, target roachpb.ReplicaDescriptor,
) bool {
	timeout := sr.processTimeoutFn(candidateReplica)
	if err := contextutil.RunWithTimeout(ctx, "transfer lease", timeout, func(ctx context.Context) error {
		return sr.rr.TransferLease(
			ctx,
			candidateReplica,
			candidateReplica.StoreID(),
			target.StoreID,
			candidateReplica.QPS(),
		)
	}); err != nil {
		log.KvDistribution.Errorf(ctx, "unable to transfer lease to s%d: %+v", target.StoreID, err)
		return false
	}
	return true
}

// RefreshContext updates the local rebalance loop context to use the latest
// storepool information. After a rebalance or lease transfer the storepool is
// updated.
func (sr *StoreRebalancer) RefreshContext(rctx *RebalanceContext) {
	if localDesc, ok := sr.allocator.StorePool.GetStoreDescriptor(rctx.LocalDesc.StoreID); ok {
		rctx.LocalDesc = &localDesc
	}
	allStoresList, _, _ := sr.allocator.StorePool.GetStoreList(storepool.StoreFilterSuspect)
	rctx.allStoresList = allStoresList
}

// PostLeaseRebalance applies housekeeping to the store rebalancer state,
// updating metrics the local store descriptor capacity and the capacity of
// target stores.
func (sr *StoreRebalancer) PostLeaseRebalance(
	rctx *RebalanceContext, candidateReplica CandidateReplica, target roachpb.ReplicaDescriptor,
) {
	sr.metrics.LeaseTransferCount.Inc(1)
	sr.RefreshContext(rctx)
}

// TransferToRebalanceRanges determines whether the store rebalancer should
// continue from lease rebalancing to range rebalancing. It logs the reason for
// continuing or not.
func (sr *StoreRebalancer) TransferToRebalanceRanges(
	ctx context.Context, rctx *RebalanceContext,
) bool {
	if !(rctx.LocalDesc.Capacity.QueriesPerSecond > rctx.QPSMaxThreshold) {
		log.KvDistribution.Infof(ctx,
			"load-based lease transfers successfully brought s%d down to %.2f qps (mean=%.2f, upperThreshold=%.2f)",
			rctx.LocalDesc.StoreID, rctx.LocalDesc.Capacity.QueriesPerSecond,
			rctx.allStoresList.CandidateQueriesPerSecond.Mean, rctx.QPSMaxThreshold)
		return false
	}

	if rctx.mode != LBRebalancingLeasesAndReplicas {
		log.KvDistribution.Infof(ctx,
			"ran out of leases worth transferring and qps (%.2f) is still above desired threshold (%.2f)",
			rctx.LocalDesc.Capacity.QueriesPerSecond, rctx.QPSMaxThreshold)
		return false
	}

	log.KvDistribution.Infof(ctx,
		"ran out of leases worth transferring and qps (%.2f) is still above desired "+
			"threshold (%.2f); considering load-based replica rebalances",
		rctx.LocalDesc.Capacity.QueriesPerSecond, rctx.QPSMaxThreshold)
	// Re-combine replicasToMaybeRebalance with what remains of hottestRanges so
	// that we'll reconsider them for replica rebalancing.
	rctx.rebalanceCandidates = append(rctx.rebalanceCandidates, rctx.hottestRanges...)
	return true
}

// LogRangeRebalanceOutcome logs the outcome of rebalancing replicas.
func (sr *StoreRebalancer) LogRangeRebalanceOutcome(ctx context.Context, rctx *RebalanceContext) {
	// We failed rebalancing below the max threshold, failing our goal, QPS >
	// max threshold. Log the failure.
	if rctx.LocalDesc.Capacity.QueriesPerSecond > rctx.QPSMaxThreshold {
		log.KvDistribution.Infof(ctx,
			"ran out of replicas worth transferring and qps (%.2f) is still above desired threshold (%.2f); will check again soon",
			rctx.LocalDesc.Capacity.QueriesPerSecond, rctx.QPSMaxThreshold)
		return
	}

	// We successfully rebalanced below or equal to the max threshold,
	// fulfilling our goal, QPS <= max threshold. Log the success.
	log.KvDistribution.Infof(ctx,
		"load-based replica transfers successfully brought s%d down to %.2f qps (mean=%.2f, upperThreshold=%.2f)",
		rctx.LocalDesc.StoreID, rctx.LocalDesc.Capacity.QueriesPerSecond, rctx.allStoresList.CandidateQueriesPerSecond.Mean, rctx.QPSMaxThreshold)
}

// RebalanceRanges searches for range rebalancing opportunties, it will return
// the outcome RebalanceTargetFound, if there are valid range rebalancing
// targets, NoRebalanceTarget if there are no rebalance targets but the QPS
// still exceeds the threshold and NoRebalanceNeeded if the threshold is not
// exceeded.
func (sr *StoreRebalancer) RebalanceRanges(
	ctx context.Context, rctx *RebalanceContext,
) (
	outcome RebalanceSearchOutcome,
	candidateReplica CandidateReplica,
	voterTargets, nonVoterTargets []roachpb.ReplicationTarget,
) {
	if rctx.LocalDesc.Capacity.QueriesPerSecond <= rctx.QPSMaxThreshold {
		return NoRebalanceNeeded, candidateReplica, voterTargets, nonVoterTargets
	}

	candidateReplica, voterTargets, nonVoterTargets = sr.chooseRangeToRebalance(
		ctx,
		rctx,
	)

	if candidateReplica == nil {
		return NoRebalanceTarget, candidateReplica, voterTargets, nonVoterTargets
	}

	return RebalanceTargetFound, candidateReplica, voterTargets, nonVoterTargets
}

func (sr *StoreRebalancer) applyRangeRebalance(
	ctx context.Context,
	candidateReplica CandidateReplica,
	voterTargets, nonVoterTargets []roachpb.ReplicationTarget,
) bool {
	descBeforeRebalance, _ := candidateReplica.DescAndSpanConfig()
	log.KvDistribution.VEventf(
		ctx,
		1,
		"rebalancing r%d (%.2f qps) to better balance load: voters from %v to %v; non-voters from %v to %v",
		candidateReplica.GetRangeID(),
		candidateReplica.QPS(),
		descBeforeRebalance.Replicas().Voters(),
		voterTargets,
		descBeforeRebalance.Replicas().NonVoters(),
		nonVoterTargets,
	)

	timeout := sr.processTimeoutFn(candidateReplica)
	if err := contextutil.RunWithTimeout(ctx, "relocate range", timeout, func(ctx context.Context) error {
		return sr.rr.RelocateRange(
			ctx,
			descBeforeRebalance.StartKey.AsRawKey(),
			voterTargets,
			nonVoterTargets,
			true, /* transferLeaseToFirstVoter */
		)
	}); err != nil {
		log.KvDistribution.Errorf(ctx, "unable to relocate range to %v: %v", voterTargets, err)
		return false
	}

	return true
}

// PostRangeRebalance applies housekeeping to the store rebalancer state,
// updating metrics the local store descriptor capacity and the capacity of
// target stores.
func (sr *StoreRebalancer) PostRangeRebalance(
	rctx *RebalanceContext,
	candidateReplica CandidateReplica,
	voterTargets, nonVoterTargets []roachpb.ReplicationTarget,
	oldVoters, oldNonVoters []roachpb.ReplicaDescriptor,
) {
	sr.metrics.RangeRebalanceCount.Inc(1)

	// Finally, update our local copies of the descriptors so that if
	// additional transfers are needed we'll be making the decisions with more
	// up-to-date info.
	sr.allocator.StorePool.UpdateLocalStoreAfterRelocate(
		voterTargets, nonVoterTargets,
		oldVoters, oldNonVoters,
		rctx.LocalDesc.StoreID,
		candidateReplica.QPS(),
	)
	sr.RefreshContext(rctx)
}

func (sr *StoreRebalancer) chooseLeaseToTransfer(
	ctx context.Context, rctx *RebalanceContext,
) (CandidateReplica, roachpb.ReplicaDescriptor, []CandidateReplica) {
	var considerForRebalance []CandidateReplica
	now := sr.allocator.StorePool.Clock.NowAsClockTimestamp()
	for {
		if len(rctx.hottestRanges) == 0 {
			return nil, roachpb.ReplicaDescriptor{}, considerForRebalance
		}
		candidateReplica := (rctx.hottestRanges)[0]
		rctx.hottestRanges = (rctx.hottestRanges)[1:]

		// We're all out of replicas.
		if candidateReplica == nil {
			return candidateReplica, roachpb.ReplicaDescriptor{}, considerForRebalance
		}

		if !candidateReplica.OwnsValidLease(ctx, now) {
			log.KvDistribution.VEventf(ctx, 3, "store doesn't own the lease for r%d", candidateReplica.GetRangeID())
			continue
		}

		// Don't bother moving leases whose QPS is below some small fraction of the
		// store's QPS. It's just unnecessary churn with no benefit to move leases
		// responsible for, for example, 1 qps on a store with 5000 qps.
		const minQPSFraction = .001
		if candidateReplica.QPS() < rctx.LocalDesc.Capacity.QueriesPerSecond*minQPSFraction {
			log.KvDistribution.VEventf(ctx, 3, "r%d's %.2f qps is too little to matter relative to s%d's %.2f total qps",
				candidateReplica.GetRangeID(), candidateReplica.QPS(), rctx.LocalDesc.StoreID, rctx.LocalDesc.Capacity.QueriesPerSecond)
			continue
		}

		desc, conf := candidateReplica.DescAndSpanConfig()
		log.KvDistribution.VEventf(ctx, 3, "considering lease transfer for r%d with %.2f qps",
			desc.RangeID, candidateReplica.QPS())

		// Check all the other voting replicas in order of increasing qps.
		// Learners or non-voters aren't allowed to become leaseholders or raft
		// leaders, so only consider the `Voter` replicas.
		candidates := desc.Replicas().DeepCopy().VoterDescriptors()

		// Only consider replicas that are not lagging behind the leader in order to
		// avoid hurting QPS in the short term. This is a stronger check than what
		// `TransferLeaseTarget` performs (it only excludes replicas that are
		// waiting for a snapshot).
		candidates = allocatorimpl.FilterBehindReplicas(ctx, sr.getRaftStatusFn(candidateReplica), candidates)

		candidate := sr.allocator.TransferLeaseTarget(
			ctx,
			conf,
			candidates,
			candidateReplica,
			candidateReplica.Stats(),
			true, /* forceDecisionWithoutStats */
			allocator.TransferLeaseOptions{
				Goal:             allocator.QPSConvergence,
				ExcludeLeaseRepl: false,
			},
		)

		if candidate == (roachpb.ReplicaDescriptor{}) {
			log.KvDistribution.VEventf(
				ctx,
				3,
				"could not find a better lease transfer target for r%d; considering replica rebalance instead",
				desc.RangeID,
			)
			considerForRebalance = append(considerForRebalance, candidateReplica)
			continue
		}

		filteredStoreList := rctx.allStoresList.ExcludeInvalid(conf.VoterConstraints)
		if sr.allocator.FollowTheWorkloadPrefersLocal(
			ctx,
			filteredStoreList,
			*rctx.LocalDesc,
			candidate.StoreID,
			candidates,
			candidateReplica.Stats(),
		) {
			log.KvDistribution.VEventf(
				ctx, 3, "r%d is on s%d due to follow-the-workload; considering replica rebalance instead",
				desc.RangeID, rctx.LocalDesc.StoreID,
			)
			considerForRebalance = append(considerForRebalance, candidateReplica)
			continue
		}
		if targetStore, ok := sr.allocator.StorePool.GetStoreDescriptor(candidate.StoreID); ok {
			log.KvDistribution.VEventf(
				ctx,
				1,
				"transferring lease for r%d (qps=%.2f) to store s%d (qps=%.2f) from local store s%d (qps=%.2f)",
				desc.RangeID,
				candidateReplica.QPS(),
				targetStore.StoreID,
				targetStore.Capacity.QueriesPerSecond,
				rctx.LocalDesc.StoreID,
				rctx.LocalDesc.Capacity.QueriesPerSecond,
			)
		}
		return candidateReplica, candidate, considerForRebalance
	}
}

// rangeRebalanceContext represents a snapshot of a replicas's state along with
// the state of the cluster during the StoreRebalancer's attempt to rebalance it
// based on QPS.
type rangeRebalanceContext struct {
	candidateReplica CandidateReplica
	rangeDesc        *roachpb.RangeDescriptor
	conf             roachpb.SpanConfig
}

func (sr *StoreRebalancer) chooseRangeToRebalance(
	ctx context.Context, rctx *RebalanceContext,
) (candidateReplica CandidateReplica, voterTargets, nonVoterTargets []roachpb.ReplicationTarget) {
	now := sr.allocator.StorePool.Clock.NowAsClockTimestamp()
	if len(rctx.rebalanceCandidates) == 0 && len(rctx.hottestRanges) >= 0 {
		// NB: In practice, the rebalanceCandidates will be populated with
		// hottest ranges by the preceeding function call, rebalance leases.
		// Some tests, that only call this function without transferring from
		// lease rebalancing first, won't populate rebalanceCandidates, so
		// instead we check if any hottestRanges exist and append them if so.
		rctx.rebalanceCandidates = append(rctx.rebalanceCandidates, rctx.hottestRanges...)
	}

	for {
		var candidateReplica CandidateReplica
		if len(rctx.rebalanceCandidates) == 0 {
			return candidateReplica, nil, nil
		}
		candidateReplica = rctx.rebalanceCandidates[0]
		rctx.rebalanceCandidates = rctx.rebalanceCandidates[1:]

		if candidateReplica == nil {
			return candidateReplica, nil, nil
		}

		// Don't bother moving ranges whose QPS is below some small fraction of the
		// store's QPS. It's just unnecessary churn with no benefit to move ranges
		// responsible for, for example, 1 qps on a store with 5000 qps.
		const minQPSFraction = .001
		if candidateReplica.QPS() < rctx.LocalDesc.Capacity.QueriesPerSecond*minQPSFraction {
			log.KvDistribution.VEventf(
				ctx,
				5,
				"r%d's %.2f qps is too little to matter relative to s%d's %.2f total qps",
				candidateReplica.GetRangeID(),
				candidateReplica.QPS(),
				rctx.LocalDesc.StoreID,
				rctx.LocalDesc.Capacity.QueriesPerSecond,
			)
			continue
		}

		rangeDesc, conf := candidateReplica.DescAndSpanConfig()
		clusterNodes := sr.allocator.StorePool.ClusterNodeCount()
		numDesiredVoters := allocatorimpl.GetNeededVoters(conf.GetNumVoters(), clusterNodes)
		numDesiredNonVoters := allocatorimpl.GetNeededNonVoters(numDesiredVoters, int(conf.GetNumNonVoters()), clusterNodes)
		if expected, actual := numDesiredVoters, len(rangeDesc.Replicas().VoterDescriptors()); expected != actual {
			log.KvDistribution.VEventf(
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
			log.KvDistribution.VEventf(
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
			candidateReplica: candidateReplica,
			rangeDesc:        rangeDesc,
			conf:             conf,
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
		rctx.options.QPSPerReplica = candidateReplica.QPS()

		if !candidateReplica.OwnsValidLease(ctx, now) {
			log.KvDistribution.VEventf(ctx, 3, "store doesn't own the lease for r%d", candidateReplica.GetRangeID())
			continue
		}

		log.KvDistribution.VEventf(
			ctx,
			3,
			"considering replica rebalance for r%d with %.2f qps",
			candidateReplica.GetRangeID(),
			candidateReplica.QPS(),
		)

		targetVoterRepls, targetNonVoterRepls, foundRebalance := sr.getRebalanceTargetsBasedOnQPS(
			ctx,
			rebalanceCtx,
			rctx.options,
		)

		if !foundRebalance {
			// Bail if there are no stores that are better for the existing replicas.
			// If the range needs a lease transfer to enable better load distribution,
			// it will be handled by the logic in `chooseLeaseToTransfer()`.
			log.KvDistribution.VEventf(ctx, 3, "could not find rebalance opportunities for r%d", candidateReplica.GetRangeID())
			continue
		}

		// Pick the voter with the least QPS to be leaseholder;
		// RelocateRange transfers the lease to the first provided target.
		//
		// If a lease preference exists among the incoming voting set, then we
		// consider only those stores as lease transfer targets. Otherwise, if
		// there are no preferred leaseholders, either due to no lease
		// preference being set or no preferred stores in the incoming voting
		// set, we consider every incoming voter as a transfer candidate.
		// NB: This implies that lease preferences will be ignored if no voting
		// replicas exist that satisfy the lease preference. We could also
		// ignore this rebalance opportunity in this case, however we do not as
		// it is more likely than not that this would only occur under
		// misconfiguration.
		validTargets := sr.allocator.ValidLeaseTargets(
			ctx,
			rebalanceCtx.conf,
			targetVoterRepls,
			rebalanceCtx.candidateReplica,
			allocator.TransferLeaseOptions{
				AllowUninitializedCandidates: true,
			},
		)

		// When there are no valid targets, due to all existing voting replicas
		// requiring a snapshot as well as the incoming, new voting replicas
		// being on dead stores, ignore this rebalance option. The lease for
		// this range post-rebalance would have no suitable location.
		if len(validTargets) == 0 {
			log.KvDistribution.VEventf(
				ctx,
				3,
				"could not find rebalance opportunities for r%d, no replica found to hold lease",
				candidateReplica.GetRangeID(),
			)
			continue
		}

		storeDescMap := rctx.allStoresList.ToMap()
		newLeaseIdx := 0
		newLeaseQPS := math.MaxFloat64

		// Find the voter in the resulting voting set, which is a valid lease
		// target and on a store with the least QPS to become the leaseholder.
		// NB: The reason we do not call allocator.TransferLeaseTarget is
		// because it requires that all the candidates are existing, rather
		// than possibly new, incoming voters that are yet to be initialized.
		for i := range validTargets {
			storeDesc, ok := storeDescMap[validTargets[i].StoreID]
			if ok && storeDesc.Capacity.QueriesPerSecond < newLeaseQPS {
				newLeaseIdx = i
				newLeaseQPS = storeDesc.Capacity.QueriesPerSecond
			}
		}

		// Swap the target leaseholder with the first target voter, to transfer
		// the lease to this voter target when rebalancing the range.
		for i := range targetVoterRepls {
			if targetVoterRepls[i].StoreID == validTargets[newLeaseIdx].StoreID {
				targetVoterRepls[0], targetVoterRepls[i] = targetVoterRepls[i], targetVoterRepls[0]
				break
			}
		}

		return candidateReplica,
			roachpb.MakeReplicaSet(targetVoterRepls).ReplicationTargets(),
			roachpb.MakeReplicaSet(targetNonVoterRepls).ReplicationTargets()
	}
}

// getRebalanceTargetsBasedOnQPS returns a list of rebalance targets for
// voting and non-voting replicas on the range that match the relevant
// constraints on the range and would further the goal of balancing the QPS on
// the stores in this cluster.
func (sr *StoreRebalancer) getRebalanceTargetsBasedOnQPS(
	ctx context.Context, rbCtx rangeRebalanceContext, options allocatorimpl.ScorerOptions,
) (finalVoterTargets, finalNonVoterTargets []roachpb.ReplicaDescriptor, foundRebalance bool) {
	finalVoterTargets = rbCtx.rangeDesc.Replicas().VoterDescriptors()
	finalNonVoterTargets = rbCtx.rangeDesc.Replicas().NonVoterDescriptors()

	// NB: We attempt to rebalance N times for N replicas as we may want to
	// replace all of them (they could all be on suboptimal stores).
	for i := 0; i < len(finalVoterTargets); i++ {
		// TODO(aayush): Figure out a way to plumb the `details` here into
		// `AdminRelocateRange` so that these decisions show up in system.rangelog
		add, remove, _, shouldRebalance := sr.allocator.RebalanceTarget(
			ctx,
			rbCtx.conf,
			rbCtx.candidateReplica.RaftStatus(),
			finalVoterTargets,
			finalNonVoterTargets,
			rbCtx.candidateReplica.RangeUsageInfo(),
			storepool.StoreFilterSuspect,
			allocatorimpl.VoterTarget,
			options,
		)
		if !shouldRebalance {
			log.KvDistribution.VEventf(
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
		log.KvDistribution.VEventf(
			ctx,
			3,
			"rebalancing voter (qps=%.2f) for r%d on %v to %v in order to improve QPS balance",
			rbCtx.candidateReplica.QPS(),
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
		add, remove, _, shouldRebalance := sr.allocator.RebalanceTarget(
			ctx,
			rbCtx.conf,
			rbCtx.candidateReplica.RaftStatus(),
			finalVoterTargets,
			finalNonVoterTargets,
			rbCtx.candidateReplica.RangeUsageInfo(),
			storepool.StoreFilterSuspect,
			allocatorimpl.NonVoterTarget,
			options,
		)
		if !shouldRebalance {
			log.KvDistribution.VEventf(
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
		log.KvDistribution.VEventf(
			ctx,
			3,
			"rebalancing non-voter (qps=%.2f) for r%d on %v to %v in order to improve QPS balance",
			rbCtx.candidateReplica.QPS(),
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

// jitteredInterval returns a randomly jittered (+/-25%) duration
// from checkInterval.
func jitteredInterval(interval time.Duration) time.Duration {
	return time.Duration(float64(interval) * (0.75 + 0.5*rand.Float64()))
}
