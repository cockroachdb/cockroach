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
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/load"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/storepool"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/contextutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"go.etcd.io/raft/v3"
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
	"whether to rebalance based on the distribution of load across stores",
	"leases and replicas",
	map[int64]string{
		int64(LBRebalancingOff):               "off",
		int64(LBRebalancingLeasesOnly):        "leases",
		int64(LBRebalancingLeasesAndReplicas): "leases and replicas",
	},
).WithPublic()

// LoadBasedRebalancingDimension controls what dimension rebalancing takes into
// account.
// NB: This value is set to private on purpose, as this cluster setting is a
// noop at the moment.
var LoadBasedRebalancingDimension = settings.RegisterEnumSetting(
	settings.SystemOnly,
	"kv.allocator.load_based_rebalancing_dimension",
	"what dimension of load does rebalancing consider",
	"cpu",
	map[int64]string{
		int64(LBRebalancingQueries): "qps",
		int64(LBRebalancingCPUTime): "cpu",
	},
)

// LBRebalancingMode controls if and when we do store-level rebalancing
// based on load.
type LBRebalancingMode int64

const (
	// LBRebalancingOff means that we do not do store-level rebalancing
	// based on load statistics.
	LBRebalancingOff LBRebalancingMode = iota
	// LBRebalancingLeasesOnly means that we rebalance leases based on
	// store-level load imbalances.
	LBRebalancingLeasesOnly
	// LBRebalancingLeasesAndReplicas means that we rebalance both leases and
	// replicas based on store-level load imbalances.
	LBRebalancingLeasesAndReplicas
)

// LBRebalancingDimension is the load dimension to balance.
type LBRebalancingDimension int64

const (
	// LBRebalancingQueries is a rebalancing mode that balances queries (QPS).
	LBRebalancingQueries LBRebalancingDimension = iota

	// LBRebalancingCPUTime  is a rebalancing mode that balances cpu time per
	// second.
	LBRebalancingCPUTime
)

func (d LBRebalancingDimension) ToDimension() load.Dimension {
	switch d {
	case LBRebalancingQueries:
		return load.Queries
	case LBRebalancingCPUTime:
		return load.CPUTime
	default:
		panic("unknown dimension")
	}
}

// RebalanceSearchOutcome returns the result of a rebalance target search. It
// is used to determine whether to transition from lease to range based
// rebalancing, exit early or apply a rebalancing action if a target is found.
type RebalanceSearchOutcome int

const (
	// NoRebalanceNeeded indicates that the state of the local store is within
	// the target goal for load and does not need rebalancing.
	NoRebalanceNeeded RebalanceSearchOutcome = iota
	// NoRebalanceTarget indicates that the state of the local store is outside
	// of the target goal for load however no rebalancing opportunities were
	// found.
	NoRebalanceTarget
	// RebalanceTargetFound indicates that the state local store is outside of
	// the target goal for load and a rebalance target was found
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
	storePool        storepool.AllocatorStorePool
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
	var storePool storepool.AllocatorStorePool
	if rq.store.cfg.StorePool != nil {
		storePool = rq.store.cfg.StorePool
	}
	sr := &StoreRebalancer{
		AmbientContext:  ambientCtx,
		metrics:         makeStoreRebalancerMetrics(),
		st:              st,
		storeID:         rq.store.StoreID(),
		rr:              rq,
		allocator:       rq.allocator,
		storePool:       storePool,
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
	storePool storepool.AllocatorStorePool,
	getRaftStatusFn func(replica CandidateReplica) *raft.Status,
) *StoreRebalancer {
	sr := &StoreRebalancer{
		AmbientContext:  log.MakeTestingAmbientCtxWithNewTracer(),
		metrics:         makeStoreRebalancerMetrics(),
		st:              &cluster.Settings{},
		storeID:         storeID,
		allocator:       alocator,
		storePool:       storePool,
		getRaftStatusFn: getRaftStatusFn,
	}
	return sr
}

// RebalanceContext maintains the current state for calls to rebalanceStore. It
// should be discarded and a new one created on each call.
type RebalanceContext struct {
	LocalDesc                          roachpb.StoreDescriptor
	loadDimension                      load.Dimension
	maxThresholds                      load.Load
	options                            *allocatorimpl.LoadScorerOptions
	mode                               LBRebalancingMode
	allStoresList                      storepool.StoreList
	hottestRanges, rebalanceCandidates []CandidateReplica
}

// LessThanMaxThresholds returns true if the local store is below the maximum
// threshold w.r.t the balanced load dimension, false otherwise.
func (r *RebalanceContext) LessThanMaxThresholds() bool {
	return !load.Greater(r.LocalDesc.Capacity.Load(), r.maxThresholds, r.loadDimension)
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
			// starting up and we might as well wait for some stats to
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

			hottestRanges := sr.replicaRankings.TopLoad()
			options := sr.scorerOptions(ctx)
			rctx := sr.NewRebalanceContext(ctx, options, hottestRanges, LBRebalancingMode(LoadBasedRebalancingMode.Get(&sr.st.SV)))
			sr.rebalanceStore(ctx, rctx)
		}
	})
}

// NB: The StoreRebalancer only cares about the convergence of load across
// stores, not the convergence of range count. So, we don't use the allocator's
// `scorerOptions` here, which sets the range count rebalance threshold.
// Instead, we use our own implementation of `scorerOptions` that promotes load
// balance.
func (sr *StoreRebalancer) scorerOptions(ctx context.Context) *allocatorimpl.LoadScorerOptions {
	lbDimension := LBRebalancingDimension(LoadBasedRebalancingDimension.Get(&sr.st.SV)).ToDimension()
	return &allocatorimpl.LoadScorerOptions{
		StoreHealthOptions:           sr.allocator.StoreHealthOptions(ctx),
		Deterministic:                sr.storePool.IsDeterministic(),
		LoadDims:                     []load.Dimension{lbDimension},
		LoadThreshold:                allocatorimpl.LoadThresholds(&sr.st.SV, lbDimension),
		MinLoadThreshold:             allocatorimpl.LoadMinThresholds(lbDimension),
		MinRequiredRebalanceLoadDiff: allocatorimpl.LoadRebalanceRequiredMinDiff(&sr.st.SV, lbDimension),
		// NB: RebalanceImpact is set just before request time, when calling RebalanceTarget.
		RebalanceImpact: nil,
	}
}

// NewRebalanceContext creates a RebalanceContext using the storepool
// associated with the store rebalancer and scorer options given.
func (sr *StoreRebalancer) NewRebalanceContext(
	ctx context.Context,
	options *allocatorimpl.LoadScorerOptions,
	hottestRanges []CandidateReplica,
	rebalancingMode LBRebalancingMode,
) *RebalanceContext {
	allStoresList, _, _ := sr.storePool.GetStoreList(storepool.StoreFilterSuspect)
	// Find the store descriptor for the local store.
	localDesc, ok := allStoresList.FindStoreByID(sr.storeID)
	if !ok {
		log.KvDistribution.Warningf(
			ctx,
			"StorePool missing descriptor for local store with ID %d, store list %v",
			sr.storeID,
			allStoresList,
		)
		return nil
	}

	dims := LBRebalancingDimension(LoadBasedRebalancingDimension.Get(&sr.st.SV)).ToDimension()
	return &RebalanceContext{
		LocalDesc:     localDesc,
		loadDimension: dims,
		options:       options,
		mode:          rebalancingMode,
		maxThresholds: allocatorimpl.OverfullLoadThresholds(
			allStoresList.LoadMeans(),
			options.LoadThreshold,
			options.MinLoadThreshold,
		),
		allStoresList:       allStoresList,
		rebalanceCandidates: []CandidateReplica{},
		hottestRanges:       hottestRanges,
	}
}

// rebalanceStore iterates through the top K hottest ranges on this store and
// for each such range, performs a lease transfer if it determines that that
// will improve load balance across the stores in the cluster. After it runs out
// of leases to transfer away (i.e. because it couldn't find better
// replacements), it considers these ranges for replica rebalancing.
//
// TODO(aayush): We don't try to move replicas or leases away from the local
// store unless it is fielding more than the overfull threshold of load based off
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
	//     lease for. Where the lowest loaded store that ends up with a voting
	//     replica will have the lease transferred to it.

	// Phase (1) Hot Lease Rebalancing Loop:
	// (1) If local load <= maximum load then goto (5)
	// (2) Find best lease to transfer away, if none exists goto step (5)
	// (3) Transfer best lease away, if unsuccessful goto (1)
	// (4) Update local view of cluster state (local load), then goto step (1)
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
			sr.PostLeaseRebalance(ctx, rctx, candidateReplica, target)
		}
	}

	// Check whether we should continue to range based rebalancing, after
	// trying lease based rebalancing.
	shouldRebalanceRanges := sr.TransferToRebalanceRanges(ctx, rctx)
	if !shouldRebalanceRanges {
		return
	}

	// Phase (2) Hot Range Rebalancing Loop:
	// (1) If local load <= maximum load then goto (5)
	// (2) Find best range to change its replicas and transfer lease for, if none exists goto (5)
	// (3) RelocateRange and transfer lease to lowest load replica, if unsuccessful goto (1)
	// (4) Update local view of cluster state (local load), then goto step (1)
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
			sr.PostRangeRebalance(ctx, rctx, candidateReplica, voterTargets, nonVoterTargets, oldVoters, oldNonVoters)
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
	// cluster-level overfull threshold of load.
	if rctx.LessThanMaxThresholds() {
		// Since the lack of activity is the most common case, we don't
		// log externally by default. Only show the details when
		// requested by log config or when looking at traces.
		log.KvDistribution.VEventf(
			ctx, 1,
			"local load %s is below max threshold %s mean=%s; no rebalancing needed",
			rctx.LocalDesc.Capacity.Load(), rctx.maxThresholds, rctx.allStoresList.LoadMeans())
		return false
	}
	// NB: This log statement is included here so that it will be covered by
	// the simulator, in practice we currently always move into lease
	// rebalancing first - however this may change in the future.
	log.KvDistribution.Infof(ctx,
		"considering load-based lease transfers for s%d with %s load, mean=%s, upperThreshold=%s",
		rctx.LocalDesc.StoreID, rctx.LocalDesc.Capacity.Load(), rctx.allStoresList.LoadMeans(), rctx.maxThresholds)
	return true
}

// RebalanceLeases searches for lease rebalancing opportunties, it will return
// the outcome RebalanceTargetFound, if there are valid lease transfer targets,
// NoRebalanceTarget if there are no rebalance targets but the load still
// exceeds the threshold and NoRebalanceNeeded if the threshold is not
// exceeded.
func (sr *StoreRebalancer) RebalanceLeases(
	ctx context.Context, rctx *RebalanceContext,
) (
	outcome RebalanceSearchOutcome,
	candidateReplica CandidateReplica,
	target roachpb.ReplicaDescriptor,
) {
	if rctx.LessThanMaxThresholds() {
		return NoRebalanceNeeded, candidateReplica, target
	}

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
			candidateReplica.RangeUsageInfo(),
		)
	}); err != nil {
		log.KvDistribution.Errorf(ctx, "unable to transfer lease to s%d: %+v", target.StoreID, err)
		return false
	}
	return true
}

// RefreshRebalanceContext updates the local rebalance loop context to use the
// latest storepool information. After a rebalance or lease transfer the
// storepool is updated.
func (sr *StoreRebalancer) RefreshRebalanceContext(ctx context.Context, rctx *RebalanceContext) {
	allStoresList, _, _ := sr.storePool.GetStoreList(storepool.StoreFilterSuspect)

	// Find the local descriptor in the all store list. If the store descriptor
	// doesn't exist, then log an error rather than just a warning.
	// RefreshRebalanceContext is only called following the execution of
	// actions which would indicate that the local store was very recently in
	// this list. It is possible for some reason that it was unable to gossip
	// to itself in which case it would filter itself out as suspect. This is
	// however unlikely to occur.
	localDesc, ok := allStoresList.FindStoreByID(sr.storeID)
	if !ok {
		log.KvDistribution.Errorf(
			ctx,
			"StorePool missing descriptor for local store with ID %d, store list %v",
			sr.storeID,
			allStoresList,
		)
		return
	}

	// Update the overfull threshold to reflect the refreshed mean values for
	// the store list.
	rctx.maxThresholds = allocatorimpl.OverfullLoadThresholds(
		allStoresList.LoadMeans(),
		rctx.options.LoadThreshold,
		rctx.options.MinLoadThreshold,
	)

	rctx.allStoresList = allStoresList
	rctx.LocalDesc = localDesc
}

// PostLeaseRebalance applies housekeeping to the store rebalancer state,
// updating metrics the local store descriptor capacity and the capacity of
// target stores. This method mutates the rebalance context.
func (sr *StoreRebalancer) PostLeaseRebalance(
	ctx context.Context,
	rctx *RebalanceContext,
	candidateReplica CandidateReplica,
	target roachpb.ReplicaDescriptor,
) {
	// NB: Lease transfers are handled by the replicate queue for updating the
	// local storepool, just refresh our context with the updated state.
	sr.metrics.LeaseTransferCount.Inc(1)
	sr.RefreshRebalanceContext(ctx, rctx)
}

// TransferToRebalanceRanges determines whether the store rebalancer should
// continue from lease rebalancing to range rebalancing. It logs the reason for
// continuing or not.
func (sr *StoreRebalancer) TransferToRebalanceRanges(
	ctx context.Context, rctx *RebalanceContext,
) bool {
	if rctx.LessThanMaxThresholds() {
		log.KvDistribution.Infof(ctx,
			"load-based lease transfers successfully brought s%d down to %s load, mean=%s, upperThreshold=%s",
			rctx.LocalDesc.StoreID, rctx.LocalDesc.Capacity.Load(),
			rctx.allStoresList.LoadMeans(), rctx.maxThresholds)
		return false
	}

	if rctx.mode != LBRebalancingLeasesAndReplicas {
		log.KvDistribution.Infof(ctx,
			"ran out of leases worth transferring and load %s is still above desired threshold %s",
			rctx.LocalDesc.Capacity.Load(), rctx.maxThresholds)
		return false
	}

	log.KvDistribution.Infof(ctx,
		"ran out of leases worth transferring and load %s is still above desired "+
			"threshold %s; considering load-based replica rebalances",
		rctx.LocalDesc.Capacity.Load(), rctx.maxThresholds)
	// Re-combine replicasToMaybeRebalance with what remains of hottestRanges so
	// that we'll reconsider them for replica rebalancing.
	rctx.rebalanceCandidates = append(rctx.rebalanceCandidates, rctx.hottestRanges...)
	return true
}

// LogRangeRebalanceOutcome logs the outcome of rebalancing replicas.
func (sr *StoreRebalancer) LogRangeRebalanceOutcome(ctx context.Context, rctx *RebalanceContext) {
	// We failed rebalancing below the max threshold, failing our goal, load >
	// max threshold. Log the failure.
	if !rctx.LessThanMaxThresholds() {
		log.KvDistribution.Infof(ctx,
			"ran out of replicas worth transferring and load %s is still above desired threshold %s; will check again soon",
			rctx.LocalDesc.Capacity.Load(), rctx.maxThresholds)
	}

	// We successfully rebalanced below or equal to the max threshold,
	// fulfilling our goal, load <= max threshold. Log the success.
	log.KvDistribution.Infof(ctx,
		"load-based replica transfers successfully brought s%d down to %s load, mean=%s, upperThreshold=%s",
		rctx.LocalDesc.StoreID, rctx.LocalDesc.Capacity.Load(), rctx.allStoresList.LoadMeans(), rctx.maxThresholds)
}

// RebalanceRanges searches for range rebalancing opportunties, it will return
// the outcome RebalanceTargetFound, if there are valid range rebalancing
// targets, NoRebalanceTarget if there are no rebalance targets but the load
// still exceeds the threshold and NoRebalanceNeeded if the threshold is not
// exceeded.
func (sr *StoreRebalancer) RebalanceRanges(
	ctx context.Context, rctx *RebalanceContext,
) (
	outcome RebalanceSearchOutcome,
	candidateReplica CandidateReplica,
	voterTargets, nonVoterTargets []roachpb.ReplicationTarget,
) {
	if rctx.LessThanMaxThresholds() {
		return NoRebalanceNeeded, candidateReplica, voterTargets, nonVoterTargets
	}

	candidateReplica, voterTargets, nonVoterTargets = sr.chooseRangeToRebalance(
		ctx,
		rctx,
	)

	if candidateReplica == nil {
		log.KvDistribution.Infof(ctx,
			"ran out of replicas worth transferring and load %s is still above desired threshold %s; will check again soon",
			rctx.LocalDesc.Capacity.Load(), rctx.maxThresholds)
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
		"rebalancing r%d (%s load) to better balance load: voters from %v to %v; non-voters from %v to %v",
		candidateReplica.GetRangeID(),
		candidateReplica.RangeUsageInfo().Load(),
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
// target stores. This method mutates the rebalance context.
func (sr *StoreRebalancer) PostRangeRebalance(
	ctx context.Context,
	rctx *RebalanceContext,
	candidateReplica CandidateReplica,
	voterTargets, nonVoterTargets []roachpb.ReplicationTarget,
	oldVoters, oldNonVoters []roachpb.ReplicaDescriptor,
) {
	sr.metrics.RangeRebalanceCount.Inc(1)

	// Finally, update our local copies of the descriptors so that if
	// additional transfers are needed we'll be making the decisions with more
	// up-to-date info.
	sr.storePool.UpdateLocalStoreAfterRelocate(
		voterTargets, nonVoterTargets,
		oldVoters, oldNonVoters,
		rctx.LocalDesc.StoreID,
		candidateReplica.RangeUsageInfo(),
	)
	sr.RefreshRebalanceContext(ctx, rctx)
}

func (sr *StoreRebalancer) chooseLeaseToTransfer(
	ctx context.Context, rctx *RebalanceContext,
) (CandidateReplica, roachpb.ReplicaDescriptor, []CandidateReplica) {
	var considerForRebalance []CandidateReplica
	now := sr.storePool.Clock().NowAsClockTimestamp()
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

		// Don't bother moving leases whose load is below some small fraction of the
		// store's load. It's just unnecessary churn with no benefit to move leases
		// responsible for, for example, 1 load unit on a store with 5000 load units.
		const minLoadFraction = .001
		if candidateReplica.RangeUsageInfo().TransferImpact().Dim(rctx.loadDimension) <
			rctx.LocalDesc.Capacity.Load().Dim(rctx.loadDimension)*minLoadFraction {
			log.KvDistribution.VEventf(ctx, 3, "r%d's %s load is too little to matter relative to s%d's %s total load",
				candidateReplica.GetRangeID(), candidateReplica.RangeUsageInfo().TransferImpact(),
				rctx.LocalDesc.StoreID, rctx.LocalDesc.Capacity.Load())
			continue
		}

		desc, conf := candidateReplica.DescAndSpanConfig()
		log.KvDistribution.VEventf(ctx, 3, "considering lease transfer for r%d with %s load",
			desc.RangeID, candidateReplica.RangeUsageInfo().TransferImpact())

		// Check all the other voting replicas in order of increasing load.
		// Learners or non-voters aren't allowed to become leaseholders or raft
		// leaders, so only consider the `Voter` replicas.
		candidates := desc.Replicas().DeepCopy().VoterDescriptors()

		// Only consider replicas that are not lagging behind the leader in order to
		// avoid hurting load in the short term. This is a stronger check than what
		// `TransferLeaseTarget` performs (it only excludes replicas that are
		// waiting for a snapshot).
		candidates = allocatorimpl.FilterBehindReplicas(ctx, sr.getRaftStatusFn(candidateReplica), candidates)

		candidate := sr.allocator.TransferLeaseTarget(
			ctx,
			sr.storePool,
			conf,
			candidates,
			candidateReplica,
			candidateReplica.RangeUsageInfo(),
			true, /* forceDecisionWithoutStats */
			allocator.TransferLeaseOptions{
				Goal:             allocator.LoadConvergence,
				ExcludeLeaseRepl: false,
				LoadDimensions:   rctx.options.LoadDims,
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
			sr.storePool,
			filteredStoreList,
			rctx.LocalDesc,
			candidate.StoreID,
			candidates,
			candidateReplica.RangeUsageInfo(),
		) {
			log.KvDistribution.VEventf(
				ctx, 3, "r%d is on s%d due to follow-the-workload; considering replica rebalance instead",
				desc.RangeID, rctx.LocalDesc.StoreID,
			)
			considerForRebalance = append(considerForRebalance, candidateReplica)
			continue
		}
		if targetStore, ok := rctx.allStoresList.FindStoreByID(candidate.StoreID); ok {
			log.KvDistribution.VEventf(
				ctx,
				1,
				"transferring lease for r%d load=%s to store s%d load=%s from local store s%d load=%s",
				desc.RangeID,
				candidateReplica.RangeUsageInfo().TransferImpact(),
				targetStore.StoreID,
				targetStore.Capacity.Load(),
				rctx.LocalDesc.StoreID,
				rctx.LocalDesc.Capacity.Load(),
			)
		}
		return candidateReplica, candidate, considerForRebalance
	}
}

// rangeRebalanceContext represents a snapshot of a replicas's state along with
// the state of the cluster during the StoreRebalancer's attempt to rebalance it
// based on load.
type rangeRebalanceContext struct {
	candidateReplica CandidateReplica
	rangeDesc        *roachpb.RangeDescriptor
	conf             roachpb.SpanConfig
}

func (sr *StoreRebalancer) chooseRangeToRebalance(
	ctx context.Context, rctx *RebalanceContext,
) (candidateReplica CandidateReplica, voterTargets, nonVoterTargets []roachpb.ReplicationTarget) {
	now := sr.storePool.Clock().NowAsClockTimestamp()
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
			panic("programming error: nil candidate replica found")
		}

		// Don't bother moving ranges whose load is below some small fraction of the
		// store's load. It's just unnecessary churn with no benefit to move ranges
		// responsible for, for example, 1 load unit on a store with 5000 load units.
		const minLoadFraction = .001
		if candidateReplica.RangeUsageInfo().Load().Dim(rctx.loadDimension) <
			rctx.LocalDesc.Capacity.Load().Dim(rctx.loadDimension)*minLoadFraction {
			log.KvDistribution.VEventf(
				ctx,
				5,
				"r%d's %s load is too little to matter relative to s%d's %s total load",
				candidateReplica.GetRangeID(),
				candidateReplica.RangeUsageInfo().Load(),
				rctx.LocalDesc.StoreID,
				rctx.LocalDesc.Capacity.Load(),
			)
			log.KvDistribution.Infof(
				ctx,
				"r%d's %s load is too little to matter relative to s%d's %s total load",
				candidateReplica.GetRangeID(),
				candidateReplica.RangeUsageInfo().Load(),
				rctx.LocalDesc.StoreID,
				rctx.LocalDesc.Capacity.Load(),
			)
			continue
		}

		rangeDesc, conf := candidateReplica.DescAndSpanConfig()
		clusterNodes := sr.storePool.ClusterNodeCount()
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

		// We ascribe the leaseholder's load to every follower replica. The store
		// rebalancer first attempts to transfer the leases of its hot ranges away
		// in `chooseLeaseToTransfer`. If it cannot move enough leases away to bring
		// down the store's load below the cluster-level overfullness threshold, it
		// moves on to rebalancing replicas. In other words, for every hot range on
		// the store, the StoreRebalancer first tries moving the load away to one of
		// its existing replicas but then tries to reconfigure the range (i.e. move
		// the range to a different set of stores) to _then_ hopefully succeed in
		// moving the lease away to another replica.
		//
		// Thus, we ideally want to base our replica rebalancing on the assumption
		// that all of the load from the leaseholder's replica is going to shift to
		// the new store that we end up rebalancing to.
		//
		// TODO(kvoli): If we assume that we are going to transfer the lease
		// aftewards, then we should use just the impact of transferring a
		// lease when ascribing the value to each replica.
		rctx.options.RebalanceImpact = candidateReplica.RangeUsageInfo().Load()

		if !candidateReplica.OwnsValidLease(ctx, now) {
			log.KvDistribution.VEventf(ctx, 3, "store doesn't own the lease for r%d", candidateReplica.GetRangeID())
			continue
		}

		log.KvDistribution.VEventf(
			ctx,
			3,
			"considering replica rebalance for r%d with %s load",
			candidateReplica.GetRangeID(),
			candidateReplica.RangeUsageInfo().Load(),
		)

		targetVoterRepls, targetNonVoterRepls, foundRebalance := sr.getRebalanceTargetsBasedOnLoad(
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

		// Pick the voter with the least load to be leaseholder;
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
			sr.storePool,
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
		newLeaseLoad := math.MaxFloat64

		// Find the voter in the resulting voting set, which is a valid lease
		// target and on a store with the least load to become the leaseholder.
		// NB: The reason we do not call allocator.TransferLeaseTarget is
		// because it requires that all the candidates are existing, rather
		// than possibly new, incoming voters that are yet to be initialized.
		for i := range validTargets {
			storeDesc, ok := storeDescMap[validTargets[i].StoreID]

			// Target doesn't exist in our store list, this can happen as we
			// don't always have a consistent view of the storepool as the
			// allocator. This could be due to it originally being suspect but
			// later not etc. Skip this target.
			// TODO(kvoli): We should be sharing a consistent store list or
			// calling the storepool every time required, rather than caching a
			// store list locally for use, which is currently refreshed after
			// each operation. Alternatively, allocator calls may return the
			// storelist considered.
			if !ok {
				continue
			}

			storeLoad := storeDesc.Capacity.Load()
			if storeLoad.Dim(rctx.loadDimension) < newLeaseLoad {
				newLeaseIdx = i
				newLeaseLoad = storeLoad.Dim(rctx.loadDimension)
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

// getRebalanceTargetsBasedOnLoad returns a list of rebalance targets for
// voting and non-voting replicas on the range that match the relevant
// constraints on the range and would further the goal of balancing the load on
// the stores in this cluster.
func (sr *StoreRebalancer) getRebalanceTargetsBasedOnLoad(
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
			sr.storePool,
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
				"no more rebalancing opportunities for r%d voters that improve load balance",
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
			"rebalancing voter load=%s for r%d on %v to %v in order to improve load balance",
			rbCtx.candidateReplica.RangeUsageInfo().Load(),
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
			sr.storePool,
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
				"no more rebalancing opportunities for r%d non-voters that improve load balance",
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
			"rebalancing non-voter load=%s for r%d on %v to %v in order to improve load balance",
			rbCtx.candidateReplica.RangeUsageInfo().Load(),
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
