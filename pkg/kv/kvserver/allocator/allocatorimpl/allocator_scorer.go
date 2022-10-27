// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package allocatorimpl

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/load"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/storepool"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/constraint"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

const (
	// This is a somehow arbitrary chosen upper bound on the relative error to be
	// used when comparing doubles for equality. The assumption that comes with it
	// is that any sequence of operations on doubles won't produce a result with
	// accuracy that is worse than this relative error. There is no guarantee
	// however that this will be the case. A programmer writing code using
	// floating point numbers will still need to be aware of the effect of the
	// operations on the results and the possible loss of accuracy.
	// More info https://en.wikipedia.org/wiki/Machine_epsilon
	// https://en.wikipedia.org/wiki/Floating-point_arithmetic
	epsilon = 1e-10

	// The number of random candidates to select from a larger list of possible
	// candidates. Because the allocator heuristics are being run on every node it
	// is actually not desirable to set this value higher. Doing so can lead to
	// situations where the allocator determistically selects the "best" node for a
	// decision and all of the nodes pile on allocations to that node. See "power
	// of two random choices":
	// https://brooker.co.za/blog/2012/01/17/two-random.html and
	// https://www.eecs.harvard.edu/~michaelm/postscripts/mythesis.pdf.
	allocatorRandomCount = 2

	// rebalanceToMaxFractionUsedThreshold: if the fraction used of a store
	// descriptor capacity is greater than this value, it will never be used as a
	// rebalance target. This is important for providing a buffer between fully
	// healthy stores and full stores (as determined by
	// allocator.MaxFractionUsedThreshold).  Without such a buffer, replicas could
	// hypothetically ping pong back and forth between two nodes, making one full
	// and then the other.
	rebalanceToMaxFractionUsedThreshold = 0.925

	// minRangeRebalanceThreshold is the number of replicas by which a store
	// must deviate from the mean number of replicas to be considered overfull
	// or underfull. This absolute bound exists to account for deployments
	// with a small number of replicas to avoid premature replica movement.
	// With few enough replicas per node (<<30), a rangeRebalanceThreshold
	// of 5% (the default at time of writing, see below) would otherwise
	// result in rebalancing at one replica above/below the mean, which
	// could lead to a condition that would always fire. Instead, we only
	// consider a store full/empty if it's at least minRebalanceThreshold
	// away from the mean.
	minRangeRebalanceThreshold = 2

	// MaxL0SublevelThreshold is the number of L0 sub-levels of a store
	// descriptor, that when greater than this value and in excees of the
	// average L0 sub-levels in the cluster - will have the action defined by
	// l0SublevelsThresholdEnforce taken. This value does not affect the
	// allocator in deciding to remove replicas from it's store, only
	// potentially block adding or moving replicas to other stores.
	MaxL0SublevelThreshold = 20

	// L0SublevelInterval is the period over which to accumulate statistics on
	// the number of L0 sublevels within a store.
	L0SublevelInterval = time.Minute * 2

	// L0SublevelMaxSampled is maximum number of L0 sub-levels that may exist
	// in a sample. This setting limits the extreme skew that could occur by
	// capping the highest possible value considered.
	L0SublevelMaxSampled = 500

	// l0SubLevelWaterMark is the percentage above the mean after which a store
	// could be conisdered unhealthy if also exceeding the threshold.
	l0SubLevelWaterMark = 1.10
)

// StoreHealthEnforcement represents the level of action that may be taken or
// excluded when a candidate disk is considered unhealthy.
type StoreHealthEnforcement int64

const (
	// StoreHealthNoAction will take no action upon candidate stores when they
	// exceed l0SublevelThreshold.
	StoreHealthNoAction StoreHealthEnforcement = iota
	// StoreHealthLogOnly will take no action upon candidate stores when they
	// exceed l0SublevelThreshold except log an event.
	StoreHealthLogOnly
	// StoreHealthBlockRebalanceTo will take action to exclude candidate stores
	// when they exceed l0SublevelThreshold and mean from being considered
	// targets for rebalance actions only. Allocation actions such as adding
	// upreplicaing an range will not be affected.
	StoreHealthBlockRebalanceTo
	// StoreHealthBlockAll will take action to exclude candidate stores when
	// they exceed l0SublevelThreshold and mean from being candidates for all
	// replica allocation and rebalancing. When enabled and stores exceed the
	// threshold, they will not receive any new replicas.
	StoreHealthBlockAll
)

// RangeRebalanceThreshold is the minimum ratio of a store's range count to
// the mean range count at which that store is considered overfull or underfull
// of ranges.
var RangeRebalanceThreshold = func() *settings.FloatSetting {
	s := settings.RegisterFloatSetting(
		settings.SystemOnly,
		"kv.allocator.range_rebalance_threshold",
		"minimum fraction away from the mean a store's range count can be before it is considered overfull or underfull",
		0.05,
		settings.NonNegativeFloat,
	)
	s.SetVisibility(settings.Public)
	return s
}()

// l0SublevelsThreshold is the maximum number of sub-levels within level 0 that
// may exist on candidate store descriptors before they are considered
// unhealthy. Once considered unhealthy, the action taken will be dictated by
// l0SublevelsThresholdEnforce cluster setting defined below. The rationale for
// using L0 sub-levels as opposed to read amplification is that it is more
// generally the variable component that makes up read amplification. When
// L0 sub-levels is high, it is an indicator of poor LSM health as L0 is usually
// in memory and must be first visited before traversing any further level. See
// this issue for additional information:
// https://github.com/cockroachdb/pebble/issues/609
var l0SublevelsThreshold = settings.RegisterIntSetting(
	settings.SystemOnly,
	"kv.allocator.l0_sublevels_threshold",
	"the maximum number of l0 sublevels within a store that may exist "+
		"before the action defined in "+
		"`kv.allocator.l0_sublevels_threshold_enforce` will be taken "+
		"if also exceeding the cluster average",
	MaxL0SublevelThreshold,
)

// l0SublevelsThresholdEnforce is the level of enforcement taken upon candidate
// stores when their L0-sublevels exceeds the threshold defined in
// l0SublevelThreshold. Under disabled and log enforcement, no action is taken
// to exclude the candidate store either as a potential allocation nor
// rebalance target by the replicate queue and store rebalancer. When the
// enforcement level is rebalance, candidate stores will be excluded as targets
// for rebalancing when exceeding the threshold, however will remain candidates
// for allocation of voters and non-voters.  When allocate is set, candidates
// are excluded as targets for all rebalancing and also allocation of voters
// and non-voters.
var l0SublevelsThresholdEnforce = settings.RegisterEnumSetting(
	settings.SystemOnly,
	"kv.allocator.l0_sublevels_threshold_enforce",
	"the level of enforcement when a candidate disk has L0 sub-levels "+
		"exceeding `kv.allocator.l0_sublevels_threshold` and above the "+
		"cluster average:`block_none` will exclude "+
		"no candidate stores, `block_none_log` will exclude no candidates but log an "+
		"event, `block_rebalance_to` will exclude candidates stores from being "+
		"targets of rebalance actions, `block_all` will exclude candidate stores "+
		"from being targets of both allocation and rebalancing",
	"block_rebalance_to",
	map[int64]string{
		int64(StoreHealthNoAction):         "block_none",
		int64(StoreHealthLogOnly):          "block_none_log",
		int64(StoreHealthBlockRebalanceTo): "block_rebalance_to",
		int64(StoreHealthBlockAll):         "block_all",
	},
)

// ScorerOptions defines the interface for the two heuristics that trigger
// replica rebalancing: range count convergence and QPS convergence.
type ScorerOptions interface {
	// maybeJitterStoreStats returns a `StoreList` that's identical to the
	// parameter `sl`, but may have jittered stats on the stores.
	//
	// This is to ensure that, when scattering via `AdminScatterRequest`, we will
	// be more likely to find a rebalance opportunity.
	maybeJitterStoreStats(sl storepool.StoreList, allocRand allocatorRand) storepool.StoreList
	// deterministic is set by tests to have the allocator methods sort their
	// results by constraints score as well as by store IDs, as opposed to just
	// the score.
	deterministicForTesting() bool
	// shouldRebalanceBasedOnThresholds returns whether the specified store is a
	// candidate for having a replica removed from it given the candidate store
	// list based on either range count or QPS.
	//
	// CRDB's rebalancing logic first checks whether any existing replica is in
	// violation of constraints or is on stores that have an almost-full disk. If
	// not, it then uses `shouldRebalanceBasedOnThresholds()` to determine whether
	// the `eqClass`'s current stats are divergent enough to justify rebalancing
	// replicas.
	shouldRebalanceBasedOnThresholds(
		ctx context.Context,
		eqClass equivalenceClass,
		metrics AllocatorMetrics,
	) bool
	// balanceScore returns a discrete score (`balanceStatus`) based on whether
	// the store represented by `sc` classifies as underfull, aroundTheMean, or
	// overfull relative to all the stores in `sl`.
	balanceScore(sl storepool.StoreList, sc roachpb.StoreCapacity) balanceStatus
	// rebalanceFromConvergenceScore assigns a convergence score to the store
	// referred to by `eqClass.existing` based on whether moving a replica away
	// from this store would converge its stats towards the mean (relative to the
	// equivalence class `eqClass`). If moving the replica away from the existing
	// store would not converge its stats towards the mean, a high convergence
	// score is assigned, which would make it less likely for us to pick this
	// store's replica to move away.
	rebalanceFromConvergesScore(eqClass equivalenceClass) int
	// rebalanceToConvergesScore is similar to `rebalanceFromConvergesScore` but
	// it assigns a high convergence score iff moving a replica to the store
	// referred to by `candidate` will converge its stats towards the mean
	// (relative to the equivalence class `eqClass`). This makes it more likely
	// for us to pick this store as the rebalance target.
	rebalanceToConvergesScore(eqClass equivalenceClass, candidate roachpb.StoreDescriptor) int
	// removalConvergesScore is similar to  `rebalanceFromConvergesScore` (both
	// deal with computing a converges score for existing stores that might
	// relinquish a replica). removalConvergesScore assigns a negative convergence
	// score to the existing store (or multiple replicas, if there are multiple
	// with the same QPS) that would converge the range's existing stores' QPS the
	// most.
	removalMaximallyConvergesScore(removalCandStoreList storepool.StoreList, existing roachpb.StoreDescriptor) int
	// getStoreHealthOptions returns the scorer options for store health. It is
	// used to inform scoring based on the health of a store.
	getStoreHealthOptions() StoreHealthOptions
}

func jittered(val float64, jitter float64, rand allocatorRand) float64 {
	rand.Lock()
	defer rand.Unlock()
	result := val * jitter * (rand.Float64())
	if rand.Int31()%2 == 0 {
		result *= -1
	}
	return result
}

// ScatterScorerOptions is used by the replicateQueue when called via the
// `AdminScatterRequest`. It is like `RangeCountScorerOptions` but with the
// rangeRebalanceThreshold set to zero (i.e. with all padding disabled). It also
// perturbs the stats on existing stores to add a bit of random jitter.
type ScatterScorerOptions struct {
	RangeCountScorerOptions
	// jitter specifies the degree to which we will perturb existing store stats.
	jitter float64
}

var _ ScorerOptions = &ScatterScorerOptions{}

func (o *ScatterScorerOptions) getStoreHealthOptions() StoreHealthOptions {
	return o.RangeCountScorerOptions.StoreHealthOptions
}

func (o *ScatterScorerOptions) maybeJitterStoreStats(
	sl storepool.StoreList, allocRand allocatorRand,
) (perturbedSL storepool.StoreList) {
	perturbedStoreDescs := make([]roachpb.StoreDescriptor, 0, len(sl.Stores))
	for _, store := range sl.Stores {
		store.Capacity.RangeCount += int32(jittered(
			float64(store.Capacity.RangeCount), o.jitter, allocRand,
		))
		perturbedStoreDescs = append(perturbedStoreDescs, store)
	}

	return storepool.MakeStoreList(perturbedStoreDescs)
}

// RangeCountScorerOptions is used by the replicateQueue to tell the Allocator's
// rebalancing machinery to base its balance/convergence scores on range counts.
// This means that the resulting rebalancing decisions will further the goal of
// converging range counts across stores in the cluster.
type RangeCountScorerOptions struct {
	StoreHealthOptions
	deterministic           bool
	rangeRebalanceThreshold float64
}

var _ ScorerOptions = &RangeCountScorerOptions{}

func (o *RangeCountScorerOptions) getStoreHealthOptions() StoreHealthOptions {
	return o.StoreHealthOptions
}

func (o *RangeCountScorerOptions) maybeJitterStoreStats(
	sl storepool.StoreList, _ allocatorRand,
) (perturbedSL storepool.StoreList) {
	return sl
}

func (o *RangeCountScorerOptions) deterministicForTesting() bool {
	return o.deterministic
}

func (o RangeCountScorerOptions) shouldRebalanceBasedOnThresholds(
	ctx context.Context, eqClass equivalenceClass, metrics AllocatorMetrics,
) bool {
	store := eqClass.existing
	sl := eqClass.candidateSL
	if len(sl.Stores) == 0 {
		return false
	}

	overfullThreshold := int32(math.Ceil(overfullRangeThreshold(&o, sl.CandidateRanges.Mean)))
	// 1. We rebalance if `store` is too far above the mean (i.e. stores
	// that are overfull).
	if store.Capacity.RangeCount > overfullThreshold {
		log.KvDistribution.VEventf(ctx, 2,
			"s%d: should-rebalance(ranges-overfull): rangeCount=%d, mean=%.2f, overfull-threshold=%d",
			store.StoreID, store.Capacity.RangeCount, sl.CandidateRanges.Mean, overfullThreshold)
		return true
	}
	// 2. We rebalance if `store` isn't overfull, but it is above the mean and
	// there is at least one other store that is "underfull" (i.e. too far below
	// the mean).
	if float64(store.Capacity.RangeCount) > sl.CandidateRanges.Mean {
		underfullThreshold := int32(math.Floor(underfullRangeThreshold(&o, sl.CandidateRanges.Mean)))
		for _, desc := range sl.Stores {
			if desc.Capacity.RangeCount < underfullThreshold {
				log.KvDistribution.VEventf(ctx, 2,
					"s%d: should-rebalance(better-fit-ranges=s%d): rangeCount=%d, otherRangeCount=%d, "+
						"mean=%.2f, underfull-threshold=%d",
					store.StoreID, desc.StoreID, store.Capacity.RangeCount, desc.Capacity.RangeCount,
					sl.CandidateRanges.Mean, underfullThreshold)
				return true
			}
		}
	}
	// If we reached this point, we're happy with the range where it is.
	return false
}

func (o *RangeCountScorerOptions) balanceScore(
	sl storepool.StoreList, sc roachpb.StoreCapacity,
) balanceStatus {
	maxRangeCount := overfullRangeThreshold(o, sl.CandidateRanges.Mean)
	minRangeCount := underfullRangeThreshold(o, sl.CandidateRanges.Mean)
	curRangeCount := float64(sc.RangeCount)
	if curRangeCount < minRangeCount {
		return underfull
	} else if curRangeCount >= maxRangeCount {
		return overfull
	}
	return aroundTheMean
}

// rebalanceFromConvergesScore returns 1 iff rebalancing a replica away from
// `sd` will _not_ converge its range count towards the mean of the candidate
// stores in the equivalence class `eqClass`. When we're considering whether to
// rebalance a replica away from a store or not, we want to give it a "boost"
// (i.e. make it a less likely candidate for removal) if it doesn't further our
// goal to converge range count towards the mean.
func (o *RangeCountScorerOptions) rebalanceFromConvergesScore(eqClass equivalenceClass) int {
	if !rebalanceConvergesRangeCountOnMean(
		eqClass.candidateSL, eqClass.existing.Capacity, eqClass.existing.Capacity.RangeCount-1,
	) {
		return 1
	}
	return 0
}

// rebalanceToConvergesScore returns 1 if rebalancing a replica to `sd` will
// converge its range count towards the mean of the candidate stores inside
// `eqClass`.
func (o *RangeCountScorerOptions) rebalanceToConvergesScore(
	eqClass equivalenceClass, candidate roachpb.StoreDescriptor,
) int {
	if rebalanceConvergesRangeCountOnMean(eqClass.candidateSL, candidate.Capacity, candidate.Capacity.RangeCount+1) {
		return 1
	}
	return 0
}

// removalConvergesScore assigns a low convergesScore to the existing store if
// removing it would converge the range counts of the existing stores towards
// the mean (this low score makes it more likely to be picked for removal).
// Otherwise, a high convergesScore is assigned (which would make this store
// less likely to be picked for removal).
func (o *RangeCountScorerOptions) removalMaximallyConvergesScore(
	removalCandStoreList storepool.StoreList, existing roachpb.StoreDescriptor,
) int {
	if !rebalanceConvergesRangeCountOnMean(
		removalCandStoreList, existing.Capacity, existing.Capacity.RangeCount-1,
	) {
		return 1
	}
	return 0
}

// LoadScorerOptions is used by the StoreRebalancer to tell the Allocator's
// rebalancing machinery to base its balance/convergence scores on
// queries-per-second. This means that the resulting rebalancing decisions will
// further the goal of converging QPS across stores in the cluster.
type LoadScorerOptions struct {
	StoreHealthOptions StoreHealthOptions
	Deterministic      bool
	LoadDims           []load.Dimension

	// LoadThreshold and MinLoadThreshold track the threshold beyond which a
	// store should be considered under/overfull and the minimum absolute
	// difference required, which is used to cover corner cases where the
	// values dealt with are relatively low.
	LoadThreshold, MinLoadThreshold load.Load

	// MinRequiredRebalanceLoadDiff declares the minimum load difference
	// between stores required before recommending an action to rebalance.
	MinRequiredRebalanceLoadDiff load.Load

	// QPS-based rebalancing assumes that:
	// 1. Every replica of a range currently receives the same level of traffic.
	// 2. Transferring this replica to another store would also transfer all of
	// this replica's load onto that receiving store.
	//
	// See comment inside `StoreRebalancer.chooseRangeToRebalance()` for why these
	// assumptions are justified in the case of replica rebalancing. The second
	// assumption is not always valid for lease transfers because only the
	// non-follower-read traffic will move to the target replica, but we don't
	// track it separately yet. See
	// https://github.com/cockroachdb/cockroach/issues/75630.

	// RebalanceImpact states the impact of moving a replica or a range lease
	// would be expected to have. More generally it represents the weight of an
	// action.
	//
	// TODO(kvoli): This value should differ depending on whether it is a
	// rebalance or transfer. These shoulld more generally use two different
	// methods when estimating an impact.
	RebalanceImpact load.Load
}

func (o *LoadScorerOptions) getStoreHealthOptions() StoreHealthOptions {
	return o.StoreHealthOptions
}

func (o *LoadScorerOptions) maybeJitterStoreStats(
	sl storepool.StoreList, _ allocatorRand,
) storepool.StoreList {
	return sl
}

func (o *LoadScorerOptions) deterministicForTesting() bool {
	return o.Deterministic
}

// shouldRebalanceBasedOnThresholds tries to determine if, within the given
// equivalenceClass `eqClass`, rebalancing a replica from one of the existing
// stores to one of the candidate stores will lead to QPS convergence among the
// stores in the equivalence class.
func (o LoadScorerOptions) shouldRebalanceBasedOnThresholds(
	ctx context.Context, eqClass equivalenceClass, metrics AllocatorMetrics,
) bool {
	if len(eqClass.candidateSL.Stores) == 0 {
		return false
	}

	bestStore, declineReason := o.getRebalanceTargetToMinimizeDelta(eqClass)
	switch declineReason {
	case noBetterCandidate:
		metrics.LoadBasedReplicaRebalanceMetrics.CannotFindBetterCandidate.Inc(1)
		log.KvDistribution.VEventf(
			ctx, 4, "could not find a better candidate to replace s%d", eqClass.existing.StoreID,
		)
	case existingNotOverfull:
		metrics.LoadBasedReplicaRebalanceMetrics.ExistingNotOverfull.Inc(1)
		log.KvDistribution.VEventf(ctx, 4, "existing store s%d is not overfull", eqClass.existing.StoreID)
	case deltaNotSignificant:
		metrics.LoadBasedReplicaRebalanceMetrics.DeltaNotSignificant.Inc(1)
		log.KvDistribution.VEventf(
			ctx, 4,
			"delta between s%d and the next best candidate is not significant enough",
			eqClass.existing.StoreID,
		)
	case missingStatsForExistingStore:
		metrics.LoadBasedReplicaRebalanceMetrics.MissingStatsForExistingStore.Inc(1)
		log.KvDistribution.VEventf(ctx, 4, "missing QPS stats for s%d", eqClass.existing.StoreID)
	case shouldRebalance:
		metrics.LoadBasedReplicaRebalanceMetrics.ShouldRebalance.Inc(1)
		var bestStoreQPS float64
		for _, store := range eqClass.candidateSL.Stores {
			if bestStore == store.StoreID {
				bestStoreQPS = store.Capacity.QueriesPerSecond
			}
		}
		log.KvDistribution.VEventf(
			ctx, 4,
			"should rebalance replica with %0.2f qps from s%d (qps=%0.2f) to s%d (qps=%0.2f)",
			o.RebalanceImpact, eqClass.existing.StoreID,
			eqClass.existing.Capacity.QueriesPerSecond,
			bestStore, bestStoreQPS,
		)
	default:
		log.KvDistribution.Fatalf(ctx, "unknown reason to decline rebalance: %v", declineReason)
	}

	return declineReason == shouldRebalance
}

func (o *LoadScorerOptions) balanceScore(
	sl storepool.StoreList, sc roachpb.StoreCapacity,
) balanceStatus {
	// balanceScore returns
	maxLoad := OverfullLoadThresholds(sl.LoadMeans(), o.LoadThreshold, o.MinLoadThreshold)
	minLoad := UnderfullLoadThresholds(sl.LoadMeans(), o.LoadThreshold, o.MinLoadThreshold)
	curLoad := sc.Load()
	if curLoad.Less(minLoad, o.LoadDims...) {
		return underfull
	} else if !curLoad.Less(maxLoad, o.LoadDims...) {
		return overfull
	} else {
		return aroundTheMean
	}
}

// rebalanceFromConvergesScore returns a score of -1 if the existing store in
// eqClass needs to be rebalanced away in order to minimize the QPS delta
// between the stores in the equivalence class `eqClass`.
func (o *LoadScorerOptions) rebalanceFromConvergesScore(eqClass equivalenceClass) int {
	_, declineReason := o.getRebalanceTargetToMinimizeDelta(eqClass)
	// If there are any rebalance opportunities that minimize the QPS delta in
	// this equivalence class, we return a score of -1 to make the existing store
	// more likely to be picked for removal.
	if declineReason == shouldRebalance {
		return -1
	}
	return 0
}

// rebalanceToConvergesScore returns a score of 1 if `candidate` needs to be
// rebalanced to in order to minimize the QPS delta between the stores in the
// equivalence class `eqClass`
func (o *LoadScorerOptions) rebalanceToConvergesScore(
	eqClass equivalenceClass, candidate roachpb.StoreDescriptor,
) int {
	bestTarget, declineReason := o.getRebalanceTargetToMinimizeDelta(eqClass)
	if declineReason == shouldRebalance && bestTarget == candidate.StoreID {
		return 1
	}
	return 0
}

// removalMaximallyConvergesScore returns a score of -1 `existing` is the
// hottest store (based on QPS) among the stores inside
// `removalCandidateStores`.
func (o *LoadScorerOptions) removalMaximallyConvergesScore(
	removalCandStoreList storepool.StoreList, existing roachpb.StoreDescriptor,
) int {
	maxQPS := float64(-1)
	for _, store := range removalCandStoreList.Stores {
		if store.Capacity.QueriesPerSecond > maxQPS {
			maxQPS = store.Capacity.QueriesPerSecond
		}
	}
	// NB: Note that if there are multiple stores inside `removalCandStoreList`
	// with the same (or similar) maxQPS, we will return a
	// removalMaximallyConvergesScore of -1 for all of them.
	if scoresAlmostEqual(maxQPS, existing.Capacity.QueriesPerSecond) {
		return -1
	}
	return 0
}

// candidate store for allocation. These are ordered by importance.
type candidate struct {
	store          roachpb.StoreDescriptor
	valid          bool
	fullDisk       bool
	necessary      bool
	diversityScore float64
	highReadAmp    bool
	l0SubLevels    int
	convergesScore int
	balanceScore   balanceStatus
	hasNonVoter    bool
	rangeCount     int
	details        string
}

func (c candidate) String() string {
	str := fmt.Sprintf("s%d, valid:%t, fulldisk:%t, necessary:%t, diversity:%.2f, highReadAmp: %t, l0SubLevels: %d, converges:%d, "+
		"balance:%d, hasNonVoter:%t, rangeCount:%d, queriesPerSecond:%.2f",
		c.store.StoreID, c.valid, c.fullDisk, c.necessary, c.diversityScore, c.highReadAmp, c.l0SubLevels, c.convergesScore,
		c.balanceScore, c.hasNonVoter, c.rangeCount, c.store.Capacity.QueriesPerSecond)
	if c.details != "" {
		return fmt.Sprintf("%s, details:(%s)", str, c.details)
	}
	return str
}

func (c candidate) compactString() string {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "s%d", c.store.StoreID)
	if !c.valid {
		fmt.Fprintf(&buf, ", valid:%t", c.valid)
	}
	if c.fullDisk {
		fmt.Fprintf(&buf, ", fullDisk:%t", c.fullDisk)
	}
	if c.necessary {
		fmt.Fprintf(&buf, ", necessary:%t", c.necessary)
	}
	if c.diversityScore != 0 {
		fmt.Fprintf(&buf, ", diversity:%.2f", c.diversityScore)
	}
	if c.highReadAmp {
		fmt.Fprintf(&buf, ", highReadAmp:%t", c.highReadAmp)
	}
	if c.l0SubLevels > 0 {
		fmt.Fprintf(&buf, ", l0SubLevels:%d", c.l0SubLevels)
	}
	fmt.Fprintf(&buf, ", converges:%d, balance:%d, rangeCount:%d",
		c.convergesScore, c.balanceScore, c.rangeCount)
	if c.details != "" {
		fmt.Fprintf(&buf, ", details:(%s)", c.details)
	}
	return buf.String()
}

// less returns true if o is a better fit for some range than c is.
func (c candidate) less(o candidate) bool {
	return c.compare(o) < 0
}

// compare is analogous to strcmp in C or string::compare in C++ -- it returns
// a positive result if c is a better fit for the range than o, 0 if they're
// equivalent, or a negative result if o is a better fit than c. The magnitude
// of the result reflects some rough idea of how much better the better
// candidate is.
func (c candidate) compare(o candidate) float64 {
	if !o.valid {
		return 600
	}
	if !c.valid {
		return -600
	}
	if o.fullDisk {
		return 500
	}
	if c.fullDisk {
		return -500
	}
	if c.necessary != o.necessary {
		if c.necessary {
			return 400
		}
		return -400
	}
	if !scoresAlmostEqual(c.diversityScore, o.diversityScore) {
		if c.diversityScore > o.diversityScore {
			return 300
		}
		return -300
	}
	// If both o and c have high read amplification, then we prefer the
	// canidate with lower read amp.
	if o.highReadAmp && c.highReadAmp {
		if o.l0SubLevels > c.l0SubLevels {
			return 250
		}
	}
	if c.highReadAmp {
		return -250
	}
	if o.highReadAmp {
		return 250
	}

	if c.convergesScore != o.convergesScore {
		if c.convergesScore > o.convergesScore {
			return 200 + float64(c.convergesScore-o.convergesScore)/10.0
		}
		return -(200 + float64(o.convergesScore-c.convergesScore)/10.0)
	}
	if c.balanceScore != o.balanceScore {
		if c.balanceScore > o.balanceScore {
			return 150 + (float64(c.balanceScore-o.balanceScore))/10.0
		}
		return -(150 + (float64(o.balanceScore-c.balanceScore))/10.0)
	}
	if c.hasNonVoter != o.hasNonVoter {
		if c.hasNonVoter {
			return 100
		}
		return -100
	}
	// Sometimes we compare partially-filled in candidates, e.g. those with
	// diversity scores filled in but not balance scores or range counts. This
	// avoids returning NaN in such cases.
	if c.rangeCount == 0 && o.rangeCount == 0 {
		return 0
	}
	if c.rangeCount < o.rangeCount {
		return float64(o.rangeCount-c.rangeCount) / float64(o.rangeCount)
	}
	return -float64(c.rangeCount-o.rangeCount) / float64(c.rangeCount)
}

type candidateList []candidate

func (cl candidateList) String() string {
	if len(cl) == 0 {
		return "[]"
	}
	var buffer bytes.Buffer
	buffer.WriteRune('[')
	for _, c := range cl {
		buffer.WriteRune('\n')
		buffer.WriteString(c.String())
	}
	buffer.WriteRune(']')
	return buffer.String()
}

// byScore implements sort.Interface to sort by scores.
type byScore candidateList

var _ sort.Interface = byScore(nil)

func (c byScore) Len() int           { return len(c) }
func (c byScore) Less(i, j int) bool { return c[i].less(c[j]) }
func (c byScore) Swap(i, j int)      { c[i], c[j] = c[j], c[i] }

// byScoreAndID implements sort.Interface to sort by scores and ids.
type byScoreAndID candidateList

var _ sort.Interface = byScoreAndID(nil)

func (c byScoreAndID) Len() int { return len(c) }
func (c byScoreAndID) Less(i, j int) bool {
	if scoresAlmostEqual(c[i].diversityScore, c[j].diversityScore) &&
		c[i].convergesScore == c[j].convergesScore &&
		c[i].balanceScore == c[j].balanceScore &&
		c[i].hasNonVoter == c[j].hasNonVoter &&
		c[i].rangeCount == c[j].rangeCount &&
		c[i].necessary == c[j].necessary &&
		c[i].fullDisk == c[j].fullDisk &&
		c[i].highReadAmp == c[j].highReadAmp &&
		c[i].valid == c[j].valid {
		return c[i].store.StoreID < c[j].store.StoreID
	}
	return c[i].less(c[j])
}
func (c byScoreAndID) Swap(i, j int) { c[i], c[j] = c[j], c[i] }

// onlyValidAndHealthyDisk returns all the elements in a sorted (by score
// reversed) candidate list that are valid and not nearly full or with high
// read amplification.
func (cl candidateList) onlyValidAndHealthyDisk() candidateList {
	for i := len(cl) - 1; i >= 0; i-- {
		if cl[i].valid && !cl[i].fullDisk && !cl[i].highReadAmp {
			return cl[:i+1]
		}
	}
	return candidateList{}
}

// best returns all the elements in a sorted (by score reversed) candidate list
// that share the highest constraint score and are valid.
func (cl candidateList) best() candidateList {
	cl = cl.onlyValidAndHealthyDisk()
	if len(cl) <= 1 {
		return cl
	}
	for i := 1; i < len(cl); i++ {
		if cl[i].necessary == cl[0].necessary &&
			scoresAlmostEqual(cl[i].diversityScore, cl[0].diversityScore) &&
			cl[i].convergesScore == cl[0].convergesScore &&
			cl[i].balanceScore == cl[0].balanceScore &&
			cl[i].hasNonVoter == cl[0].hasNonVoter {
			continue
		}
		return cl[:i]
	}
	return cl
}

// good returns all the elements in a sorted (by score reversed) candidate list
// that share the highest diversity score and are valid.
func (cl candidateList) good() candidateList {
	cl = cl.onlyValidAndHealthyDisk()
	if len(cl) <= 1 {
		return cl
	}
	for i := 1; i < len(cl); i++ {
		if cl[i].necessary == cl[0].necessary &&
			scoresAlmostEqual(cl[i].diversityScore, cl[0].diversityScore) {
			continue
		}
		return cl[:i]
	}
	return cl
}

// worst returns all the elements in a sorted (by score reversed) candidate list
// that share the lowest constraint score (for instance, the set of candidates
// that result in the lowest diversity score for the range, or the set of
// candidates that are on heavily loaded stores and thus, have the lowest
// `balanceScore`). This means that the resulting candidateList will contain all
// candidates that should be considered equally for removal.
func (cl candidateList) worst() candidateList {
	if len(cl) <= 1 {
		return cl
	}
	// Are there invalid candidates? If so, pick those.
	if !cl[len(cl)-1].valid {
		for i := len(cl) - 2; i >= 0; i-- {
			if cl[i].valid {
				return cl[i+1:]
			}
		}
	}
	// Are there candidates with a nearly full disk? If so, pick those.
	if cl[len(cl)-1].fullDisk {
		for i := len(cl) - 2; i >= 0; i-- {
			if !cl[i].fullDisk {
				return cl[i+1:]
			}
		}
	}
	// Are there candidates with high read amplification? If so, pick those.
	if cl[len(cl)-1].highReadAmp {
		for i := len(cl) - 2; i >= 0; i-- {
			if !cl[i].highReadAmp {
				return cl[i+1:]
			}
		}
	}
	// Find the worst constraint/locality/converges/balanceScore values.
	for i := len(cl) - 2; i >= 0; i-- {
		if cl[i].necessary == cl[len(cl)-1].necessary &&
			scoresAlmostEqual(cl[i].diversityScore, cl[len(cl)-1].diversityScore) &&
			cl[i].convergesScore == cl[len(cl)-1].convergesScore &&
			cl[i].balanceScore == cl[len(cl)-1].balanceScore {
			continue
		}
		return cl[i+1:]
	}
	return cl
}

// betterThan returns all elements from a sorted (by score reversed) candidate
// list that have a higher score than the candidate
func (cl candidateList) betterThan(c candidate) candidateList {
	for i := 0; i < len(cl); i++ {
		if !c.less(cl[i]) {
			return cl[:i]
		}
	}
	return cl
}

// selectBest randomly chooses one of the best candidate stores from a sorted
// (by score reversed) candidate list using the provided random generator.
func (cl candidateList) selectBest(randGen allocatorRand) *candidate {
	cl = cl.best()
	if len(cl) == 0 {
		return nil
	}
	if len(cl) == 1 {
		return &cl[0]
	}
	randGen.Lock()
	order := randGen.Perm(len(cl))
	randGen.Unlock()
	best := &cl[order[0]]
	for i := 1; i < allocatorRandomCount; i++ {
		if best.less(cl[order[i]]) {
			best = &cl[order[i]]
		}
	}
	return best
}

// selectGood randomly chooses a good candidate store from a sorted (by score
// reversed) candidate list using the provided random generator.
func (cl candidateList) selectGood(randGen allocatorRand) *candidate {
	cl = cl.good()
	if len(cl) == 0 {
		return nil
	}
	if len(cl) == 1 {
		return &cl[0]
	}
	randGen.Lock()
	r := randGen.Intn(len(cl))
	randGen.Unlock()
	c := &cl[r]
	return c
}

// selectWorst randomly chooses one of the worst candidate stores from a sorted
// (by score reversed) candidate list using the provided random generator.
func (cl candidateList) selectWorst(randGen allocatorRand) *candidate {
	cl = cl.worst()
	if len(cl) == 0 {
		return nil
	}
	if len(cl) == 1 {
		return &cl[0]
	}
	randGen.Lock()
	order := randGen.Perm(len(cl))
	randGen.Unlock()
	worst := &cl[order[0]]
	for i := 1; i < allocatorRandomCount; i++ {
		if cl[order[i]].less(*worst) {
			worst = &cl[order[i]]
		}
	}
	return worst
}

// removeCandidate remove the specified candidate from candidateList.
func (cl candidateList) removeCandidate(c candidate) candidateList {
	for i := 0; i < len(cl); i++ {
		if cl[i].store.StoreID == c.store.StoreID {
			cl = append(cl[:i], cl[i+1:]...)
			break
		}
	}
	return cl
}

// rankedCandidateListForAllocation creates a candidate list of all stores that
// can be used for allocating a new replica ordered from the best to the worst.
// Only stores that meet the criteria are included in the list.
//
// NB: When `allowMultipleReplsPerNode` is set to false, we disregard the
// *nodes* of `existingReplicas`. Otherwise, we disregard only the *stores* of
// `existingReplicas`. For instance, `allowMultipleReplsPerNode` is set to true
// by callers performing lateral relocation of replicas within the same node.
func rankedCandidateListForAllocation(
	ctx context.Context,
	candidateStores storepool.StoreList,
	constraintsCheck constraintsCheckFn,
	existingReplicas []roachpb.ReplicaDescriptor,
	nonVoterReplicas []roachpb.ReplicaDescriptor,
	existingStoreLocalities map[roachpb.StoreID]roachpb.Locality,
	isStoreValidForRoutineReplicaTransfer func(context.Context, roachpb.StoreID) bool,
	allowMultipleReplsPerNode bool,
	options ScorerOptions,
	targetType TargetReplicaType,
) candidateList {
	var candidates candidateList
	existingReplTargets := roachpb.MakeReplicaSet(existingReplicas).ReplicationTargets()
	var nonVoterReplTargets []roachpb.ReplicationTarget
	for _, s := range candidateStores.Stores {
		// Disregard all the stores that already have replicas.
		if StoreHasReplica(s.StoreID, existingReplTargets) {
			continue
		}
		// Unless the caller specifically allows us to allocate multiple replicas on
		// the same node, we disregard nodes with existing replicas.
		if !allowMultipleReplsPerNode && nodeHasReplica(s.Node.NodeID, existingReplTargets) {
			continue
		}
		if !isStoreValidForRoutineReplicaTransfer(ctx, s.StoreID) {
			log.KvDistribution.VEventf(
				ctx,
				3,
				"not considering store s%d as a potential rebalance candidate because it is on a non-live node n%d",
				s.StoreID,
				s.Node.NodeID,
			)
			continue
		}
		constraintsOK, necessary := constraintsCheck(s)
		if !constraintsOK {
			continue
		}

		if !allocator.MaxCapacityCheck(s) || !options.getStoreHealthOptions().readAmpIsHealthy(
			ctx,
			s,
			candidateStores.CandidateL0Sublevels.Mean,
		) {
			continue
		}
		diversityScore := diversityAllocateScore(s, existingStoreLocalities)
		balanceScore := options.balanceScore(candidateStores, s.Capacity)
		var hasNonVoter bool
		if targetType == VoterTarget {
			if nonVoterReplTargets == nil {
				nonVoterReplTargets = roachpb.MakeReplicaSet(nonVoterReplicas).ReplicationTargets()
			}
			hasNonVoter = StoreHasReplica(s.StoreID, nonVoterReplTargets)
		}
		candidates = append(candidates, candidate{
			store:          s,
			valid:          constraintsOK,
			necessary:      necessary,
			diversityScore: diversityScore,
			balanceScore:   balanceScore,
			hasNonVoter:    hasNonVoter,
			rangeCount:     int(s.Capacity.RangeCount),
		})
	}
	if options.deterministicForTesting() {
		sort.Sort(sort.Reverse(byScoreAndID(candidates)))
	} else {
		sort.Sort(sort.Reverse(byScore(candidates)))
	}
	return candidates
}

// candidateListForRemoval creates a candidate list of the existing
// replicas' stores that are most qualified for a removal. Callers trying to
// remove a replica from a range are expected to randomly pick a candidate from
// the result set of this method.
//
// This is determined based on factors like (in order of precedence) constraints
// conformance, disk fullness, the diversity score of the range without the
// given replica, as well as load-based factors like range count or QPS of the
// host store.
//
// Stores that are marked as not valid, are in violation of a required criteria.
func candidateListForRemoval(
	ctx context.Context,
	existingReplsStoreList storepool.StoreList,
	constraintsCheck constraintsCheckFn,
	existingStoreLocalities map[roachpb.StoreID]roachpb.Locality,
	options ScorerOptions,
) candidateList {
	var candidates candidateList
	for _, s := range existingReplsStoreList.Stores {
		constraintsOK, necessary := constraintsCheck(s)
		if !constraintsOK {
			candidates = append(candidates, candidate{
				store:     s,
				valid:     false,
				necessary: necessary,
				details:   "constraint check fail",
			})
			continue
		}
		diversityScore := diversityRemovalScore(s.StoreID, existingStoreLocalities)
		candidates = append(candidates, candidate{
			store:     s,
			valid:     constraintsOK,
			necessary: necessary,
			fullDisk:  !allocator.MaxCapacityCheck(s),
			// When removing a replica from a store, we do not want to include
			// high amplification in ranking stores. This would submit already
			// high read amplification stores to additional load of moving a
			// replica.
			highReadAmp:    false,
			diversityScore: diversityScore,
		})
	}
	if options.deterministicForTesting() {
		sort.Sort(sort.Reverse(byScoreAndID(candidates)))
	} else {
		sort.Sort(sort.Reverse(byScore(candidates)))
	}
	// We compute the converges and balance scores only in relation to stores that
	// are the top candidates for removal based on diversity (i.e. only among
	// candidates that are non-diverse relative to the rest of the replicas).
	//
	// This is because, in the face of heterogeneously loaded localities,
	// load-based scores (convergesScore, balanceScore) relative to the entire set
	// of stores are going to be misleading / inaccurate. To see this, consider a
	// situation with 4 replicas where 2 of the replicas are in the same locality:
	//
	// Region A: [1, 2]
	// Region B: [3]
	// Region C: [4]
	//
	// In such an example, replicas 1 and 2 would have the worst diversity scores
	// and thus, we'd pick one of them for removal. However, if we were computing
	// the balanceScore and convergesScore across all replicas and region A was
	// explicitly configured to have a heavier load profile than regions B and C,
	// both these replicas would likely have a discrete balanceScore of
	// `moreThanMean`. Effectively, this would mean that we would
	// randomly pick one of these for removal. This would be unfortunate as we
	// might have had a better removal candidate if we were just comparing these
	// stats among the 2 replicas that are being considered for removal (for
	// instance, replica 1 may actually be a better candidate for removal because
	// it is on a store that has more replicas than the store of replica 2).
	//
	// Computing balance and convergence scores only relative to replicas that
	// actually being considered for removal lets us make more accurate removal
	// decisions in a cluster with heterogeneously loaded localities. In
	// homogeneously loaded clusters, this ends up being roughly equivalent to
	// computing convergence and balance scores across all stores.
	candidates = candidates.worst()
	removalCandidateStores := make([]roachpb.StoreDescriptor, 0, len(candidates))
	for _, cand := range candidates {
		removalCandidateStores = append(removalCandidateStores, cand.store)
	}
	removalCandidateStoreList := storepool.MakeStoreList(removalCandidateStores)
	for i := range candidates {
		// If removing this candidate replica does not converge the store
		// stats to their mean, we make it less attractive for removal by
		// adding 1 to the constraint score. Note that when selecting a
		// candidate for removal the candidates with the lowest scores are
		// more likely to be removed.
		candidates[i].convergesScore = options.removalMaximallyConvergesScore(
			removalCandidateStoreList, candidates[i].store,
		)
		candidates[i].balanceScore = options.balanceScore(
			removalCandidateStoreList, candidates[i].store.Capacity,
		)
		candidates[i].rangeCount = int(candidates[i].store.Capacity.RangeCount)
	}
	// Re-sort to account for the ordering changes resulting from the addition of
	// convergesScore, balanceScore, etc.
	if options.deterministicForTesting() {
		sort.Sort(sort.Reverse(byScoreAndID(candidates)))
	} else {
		sort.Sort(sort.Reverse(byScore(candidates)))
	}
	return candidates
}

// rebalanceOptions contains:
//
// 1. an existing replica.
// 2. a corresponding list of comparable stores that could be legal replacements
// for the aforementioned existing replica -- ordered from `best()` to
// `worst()`.
type rebalanceOptions struct {
	existing   candidate
	candidates candidateList
}

// equivalenceClass captures the set of "equivalent" replacement candidates
// for each existing replica. "equivalent" here means the candidates that are
// just as diverse as the existing replica, conform to zone config constraints
// on the range and don't have a full disk.
// Following are a few examples:
// 1. Consider a 3 region cluster with regions A, B and C. Assume there is a
// range that has 1 replica in each of those regions. For each existing
// replica, its equivalence class would contain all the other stores in its
// own region.
// 2. Consider a cluster with 10 racks, each with 2 stores (on 2 different
// nodes). Assume that racks 1, 2 and 3 each have a replica for a range. For
// the existing replica in rack 1, its equivalence class would contain its
// neighboring store in rack 1 and all stores in racks 4...10.
type equivalenceClass struct {
	existing roachpb.StoreDescriptor
	// `candidateSl` is the `StoreList` representation of `candidates` (maintained
	// separately to avoid converting the latter into the former for all the
	// `scorerOptions` methods).
	candidateSL storepool.StoreList
	candidates  candidateList
}

// declineReason enumerates the various results of a call into
// `bestStoreToMinimizeLoadDelta`. The result may be that we have a good
// candidate to rebalance to (indicated by `shouldRebalance`) or it might be
// rejected due to a number of reasons (see below).
type declineReason int

const (
	shouldRebalance declineReason = iota
	// noBetterCandidate indicates that the existing store is already the best
	// store the lease / replica could be on. In other words, there are no further
	// opportunities to converge load using this lease/replica.
	noBetterCandidate
	// existingNotOverfull indicates that the existing store is not hot enough to
	// justify a lease transfer / rebalance away from this store. We only allow
	// lease / replica transfers away from stores that are "overfull" relative to
	// their equivalence class.
	existingNotOverfull
	// deltaNotSignificant indicates that the delta between the existing store and
	// the best candidate store is not high enough to justify a lease transfer or
	// replica rebalance. This delta is computed _ignoring_ the QPS of the
	// lease/replica in question.
	deltaNotSignificant
	// missingStatsForExistingStore indicates that we're missing the store
	// descriptor of the existing store, which means we don't have access to the
	// QPS levels of the existing store. Nothing we can do in this case except
	// bail early.
	missingStatsForExistingStore
)

// bestStoreToMinimizeLoadDelta computes a rebalance (or lease transfer) target
// for the existing store such that executing the rebalance (or lease transfer)
// decision would minimize the QPS range between the existing store and the
// coldest store in the equivalence class.
func bestStoreToMinimizeLoadDelta(
	replLoadValue load.Load,
	existing roachpb.StoreID,
	candidates []roachpb.StoreID,
	storeDescMap map[roachpb.StoreID]*roachpb.StoreDescriptor,
	options *LoadScorerOptions,
) (bestCandidate roachpb.StoreID, reason declineReason) {
	// This function is only intended to be called with the purpose one load
	// dimension. Panic if not.
	if len(options.LoadDims) != 1 {
		panic(fmt.Sprintf(
			"bestStoreToMinimizeLoadDelta only supports one minimizing the "+
				"delta over 1-dimension, %d dimensions given",
			len(options.LoadDims)))
	}
	dimension := options.LoadDims[0]

	storeLoadMap := make(map[roachpb.StoreID]load.Load, len(candidates)+1)
	for _, store := range candidates {
		if desc, ok := storeDescMap[store]; ok {
			storeLoadMap[store] = desc.Capacity.Load()
		}
	}
	desc, ok := storeDescMap[existing]
	if !ok {
		return 0, missingStatsForExistingStore
	}
	storeLoadMap[existing] = desc.Capacity.Load()

	// domain defines the domain over which this function tries to minimize the
	// load delta.
	domain := append(candidates, existing)
	storeDescs := make([]roachpb.StoreDescriptor, 0, len(domain))
	for _, desc := range storeDescMap {
		storeDescs = append(storeDescs, *desc)
	}
	domainStoreList := storepool.MakeStoreList(storeDescs)

	bestCandidate = getCandidateWithMinLoad(storeLoadMap, candidates, dimension)
	if bestCandidate == 0 {
		return 0, noBetterCandidate
	}

	bestCandLoad := storeLoadMap[bestCandidate]
	existingLoad := storeLoadMap[existing]
	if bestCandLoad.Greater(existingLoad, dimension) {
		return 0, noBetterCandidate
	}

	// NB: The store's load and the replica's QPS aren't captured at the same
	// time, so they may be mutually inconsistent. Thus, it is possible for
	// the store's load captured here to be lower than the replica's load. So we
	// defensively use the `math.Max` here.
	existingLoadIgnoringRepl := existingLoad.Sub(replLoadValue).Max(load.Vector{})

	// Only proceed if the QPS difference between `existing` and
	// `bestCandidate` (not accounting for the replica under consideration) is
	// higher than `minQPSDifferenceForTransfers`.
	diffIgnoringRepl := existingLoadIgnoringRepl.Sub(bestCandLoad)

	if options.MinRequiredRebalanceLoadDiff.Greater(diffIgnoringRepl, dimension) {
		return 0, deltaNotSignificant
	}

	// Only proceed with rebalancing iff `existingStore` is overfull relative to
	// the equivalence class.
	overfullThreshold := OverfullLoadThresholds(
		domainStoreList.LoadMeans(),
		options.LoadThreshold,
		options.MinLoadThreshold,
	)

	// The existing load does not exceed the overfull threshold so it is not
	// worthwhile rebalancing.
	if !existingLoad.Greater(overfullThreshold, dimension) {
		return 0, existingNotOverfull
	}

	// converge values
	currentLoadDelta := getLoadDelta(storeLoadMap, domain, dimension)
	// Simulate the coldest candidate's QPS after it receives a lease/replica for
	// the range.
	storeLoadMap[bestCandidate] = storeLoadMap[bestCandidate].Add(replLoadValue)
	// Simulate the hottest existing store's QPS after it sheds the lease/replica
	// away.
	storeLoadMap[existing] = existingLoadIgnoringRepl

	// NB: We proceed with a lease transfer / rebalance even if `currentQPSDelta`
	// is exactly equal to `newLoadDelta`. Consider the following example:
	// perReplicaQPS: 10qps
	// existingQPS: 100qps
	// candidates: [100qps, 0qps, 0qps]
	//
	// In such (perhaps unrealistic) scenarios, rebalancing from the existing
	// store to the coldest store is not going to reduce the delta between all
	// these stores, but it is still a desirable action to take.
	newLoadDelta := getLoadDelta(storeLoadMap, domain, dimension)
	if currentLoadDelta < newLoadDelta {
		panic(
			fmt.Sprintf(
				"programming error: projected load delta %f higher than current delta %f;"+
					" existing: %s, coldest candidate: %s, replica/lease: %s, dimension %s",
				newLoadDelta, currentLoadDelta, existingLoad, bestCandLoad, replLoadValue,
				load.DimensionNames[dimension],
			),
		)
	}

	return bestCandidate, shouldRebalance
}

// getRebalanceTargetToMinimizeDelta returns the best store (from the set of
// candidates in the equivalence class) such that rebalancing to this store
// would minimize the delta between the existing store and the coldest store in
// the equivalence class.
func (o *LoadScorerOptions) getRebalanceTargetToMinimizeDelta(
	eqClass equivalenceClass,
) (bestStore roachpb.StoreID, declineReason declineReason) {
	domainStoreList := storepool.MakeStoreList(append(eqClass.candidateSL.Stores, eqClass.existing))
	candidates := make([]roachpb.StoreID, 0, len(eqClass.candidateSL.Stores))
	for _, store := range eqClass.candidateSL.Stores {
		candidates = append(candidates, store.StoreID)
	}
	return bestStoreToMinimizeLoadDelta(
		o.RebalanceImpact,
		eqClass.existing.StoreID,
		candidates,
		domainStoreList.ToMap(),
		o,
	)
}

// rankedCandidateListForRebalancing returns a list of `rebalanceOptions`, i.e.
// groups of candidate stores and the existing replicas that they could legally
// replace in the range. See comment above `rebalanceOptions()` for more
// details.
func rankedCandidateListForRebalancing(
	ctx context.Context,
	allStores storepool.StoreList,
	removalConstraintsChecker constraintsCheckFn,
	rebalanceConstraintsChecker rebalanceConstraintsCheckFn,
	existingReplicasForType, replicasOnExemptedStores []roachpb.ReplicaDescriptor,
	existingStoreLocalities map[roachpb.StoreID]roachpb.Locality,
	isStoreValidForRoutineReplicaTransfer func(context.Context, roachpb.StoreID) bool,
	options ScorerOptions,
	metrics AllocatorMetrics,
) []rebalanceOptions {
	// 1. Determine whether existing replicas are valid and/or necessary.
	existingStores := make(map[roachpb.StoreID]candidate)
	var needRebalanceFrom bool
	curDiversityScore := RangeDiversityScore(existingStoreLocalities)
	for _, store := range allStores.Stores {
		for _, repl := range existingReplicasForType {
			if store.StoreID != repl.StoreID {
				continue
			}
			valid, necessary := removalConstraintsChecker(store)
			fullDisk := !allocator.MaxCapacityCheck(store)

			if !valid {
				if !needRebalanceFrom {
					log.KvDistribution.VEventf(ctx, 2, "s%d: should-rebalance(invalid): locality:%q",
						store.StoreID, store.Locality())
				}
				needRebalanceFrom = true
			}
			if fullDisk {
				if !needRebalanceFrom {
					log.KvDistribution.VEventf(ctx, 2, "s%d: should-rebalance(full-disk): capacity:%q",
						store.StoreID, store.Capacity)
				}
				needRebalanceFrom = true
			}
			existingStores[store.StoreID] = candidate{
				store:     store,
				valid:     valid,
				necessary: necessary,
				fullDisk:  fullDisk,
				// When rebalancing a replica away from a store, we do not want
				// to include high amplification in ranking stores. This would
				// submit already high read amplification stores to additional
				// load of moving a replica.
				highReadAmp:    false,
				diversityScore: curDiversityScore,
			}
		}
	}

	// 2. For each store, determine the stores that would be the best
	// replacements on the basis of constraints, disk fullness, and diversity.
	// Only the best should be included when computing balanceScores, since it
	// isn't fair to compare the fullness of stores in a valid/necessary/diverse
	// locality to those in an invalid/unnecessary/nondiverse locality (see
	// #20751).  Along the way, determine whether rebalance is needed to improve
	// the range along these critical dimensions.
	//
	// This creates groups of stores that are valid to compare with each other.
	// For example, if a range has a replica in localities A, B, and C, it's ok
	// to compare other stores in locality A with the existing store in locality
	// A, but would be bad for diversity if we were to compare them to the
	// existing stores in localities B and C (see #20751 for more background).
	//
	// NOTE: We can't just do this once per localityStr because constraints can
	// also include node Attributes or store Attributes. We could try to group
	// stores by attributes as well, but it's simplest to just run this for each
	// store.

	var equivalenceClasses []equivalenceClass
	var needRebalanceTo bool

	// NB: The existing stores must be sorted during iteration in order to
	// ensure determinism. When determinism isn't required, we still sort them
	// as the cases where ordering is relevant are rare enough to not cause
	// instability.
	existingStoreList := make(roachpb.StoreIDSlice, 0, len(existingStores))
	for storeID := range existingStores {
		existingStoreList = append(existingStoreList, storeID)
	}
	sort.Sort(existingStoreList)

	for _, storeID := range existingStoreList {
		existing := existingStores[storeID]
		var comparableCands candidateList
		for _, store := range allStores.Stores {
			// Only process replacement candidates, not existing stores.
			if store.StoreID == existing.store.StoreID {
				continue
			}

			// Ignore any stores on dead nodes or stores that contain any of the
			// replicas within `replicasOnExemptedStores`.
			if !isStoreValidForRoutineReplicaTransfer(ctx, store.StoreID) {
				log.KvDistribution.VEventf(
					ctx,
					3,
					"not considering store s%d as a potential rebalance candidate because it is on a non-live node n%d",
					store.StoreID,
					store.Node.NodeID,
				)
				continue
			}

			// `replsOnExemptedStores` is used during non-voting replica rebalancing
			// to ignore stores that already have a voting replica for the same range.
			// When rebalancing a voter, `replsOnExemptedStores` is empty since voters
			// are allowed to "displace" non-voting replicas (we correctly turn such
			// actions into non-voter promotions, see replicationChangesForRebalance()).
			var exempted bool
			for _, replOnExemptedStore := range replicasOnExemptedStores {
				if store.StoreID == replOnExemptedStore.StoreID {
					log.KvDistribution.VEventf(
						ctx,
						6,
						"s%d is not a possible rebalance candidate for non-voters because it already has a voter of the range; ignoring",
						store.StoreID,
					)
					exempted = true
					break
				}
			}
			if exempted {
				continue
			}

			// NB: We construct equivalence classes based on locality hierarchies,
			// the diversityScore must be the only thing that's populated at
			// this stage, in additon to hard checks and validation.
			// TODO(kvoli,ayushshah15): Refactor this to make it harder to
			// inadvertently break the invariant above,
			constraintsOK, necessary := rebalanceConstraintsChecker(store, existing.store)
			diversityScore := diversityRebalanceFromScore(
				store, existing.store.StoreID, existingStoreLocalities)
			cand := candidate{
				store:          store,
				valid:          constraintsOK,
				necessary:      necessary,
				fullDisk:       !allocator.MaxCapacityCheck(store),
				diversityScore: diversityScore,
			}
			if !cand.less(existing) {
				// If `cand` is not worse than `existing`, add it to the list.
				comparableCands = append(comparableCands, cand)

				if !needRebalanceFrom && !needRebalanceTo && existing.less(cand) {
					needRebalanceTo = true
					log.KvDistribution.VEventf(ctx, 2,
						"s%d: should-rebalance(necessary/diversity=s%d): oldNecessary:%t, newNecessary:%t, "+
							"oldDiversity:%f, newDiversity:%f, locality:%q",
						existing.store.StoreID, store.StoreID, existing.necessary, cand.necessary,
						existing.diversityScore, cand.diversityScore, store.Locality())
				}
			}
		}
		if options.deterministicForTesting() {
			sort.Sort(sort.Reverse(byScoreAndID(comparableCands)))
		} else {
			sort.Sort(sort.Reverse(byScore(comparableCands)))
		}

		// Filter down to the set of stores that are better than the rest based on
		// diversity, disk fullness and constraints conformance. These stores are
		// all in the same equivalence class with regards to the range in question.
		bestCands := comparableCands.best()

		bestStores := make([]roachpb.StoreDescriptor, len(bestCands))
		for i := range bestCands {
			bestStores[i] = bestCands[i].store
		}
		eqClass := equivalenceClass{
			existing:    existing.store,
			candidateSL: storepool.MakeStoreList(bestStores),
			candidates:  bestCands,
		}
		equivalenceClasses = append(equivalenceClasses, eqClass)
	}

	// 3. Decide whether we should try to rebalance. Note that for each existing
	// store, we only compare its fullness stats to the stats of stores within the
	// same equivalence class i.e. those stores that are at least as valid,
	// necessary, and diverse as the existing store.
	needRebalance := needRebalanceFrom || needRebalanceTo
	var shouldRebalanceCheck bool
	if !needRebalance {
		for _, eqClass := range equivalenceClasses {
			if options.shouldRebalanceBasedOnThresholds(
				ctx,
				eqClass,
				metrics,
			) {
				shouldRebalanceCheck = true
				break
			}
		}
	}

	if !needRebalance && !shouldRebalanceCheck {
		return nil
	}

	// 4. Create sets of rebalance options, i.e. groups of candidate stores and
	// the existing replicas that they could legally replace in the range.  We
	// have to make a separate set of these for each group of equivalenceClasses.
	results := make([]rebalanceOptions, 0, len(equivalenceClasses))
	for _, comparable := range equivalenceClasses {
		existing, ok := existingStores[comparable.existing.StoreID]
		if !ok {
			log.KvDistribution.Errorf(ctx, "BUG: missing candidate struct for existing store %+v; stores: %+v",
				comparable.existing, existingStores)
			continue
		}
		if !existing.valid {
			existing.details = "constraint check fail"
		} else {
			// Similarly to in candidateListForRemoval, any replica whose
			// removal would not converge the range stats to their mean is given a
			// constraint score boost of 1 to make it less attractive for removal.
			convergesScore := options.rebalanceFromConvergesScore(comparable)
			balanceScore := options.balanceScore(comparable.candidateSL, existing.store.Capacity)
			existing.convergesScore = convergesScore
			existing.balanceScore = balanceScore
			existing.rangeCount = int(existing.store.Capacity.RangeCount)
		}

		var candidates candidateList
		for _, cand := range comparable.candidates {
			// We handled the possible candidates for removal above. Don't process
			// anymore here.
			if _, ok := existingStores[cand.store.StoreID]; ok {
				continue
			}
			// We already computed valid, necessary, fullDisk, and diversityScore
			// above, but recompute fullDisk using special rebalanceTo logic for
			// rebalance candidates.
			s := cand.store
			cand.fullDisk = !rebalanceToMaxCapacityCheck(s)
			cand.l0SubLevels = int(s.Capacity.L0Sublevels)
			cand.highReadAmp = !options.getStoreHealthOptions().rebalanceToReadAmpIsHealthy(
				ctx,
				s,
				// We only wish to compare the read amplification to the
				// comparable stores average and not the cluster.
				comparable.candidateSL.CandidateL0Sublevels.Mean,
			)
			cand.balanceScore = options.balanceScore(comparable.candidateSL, s.Capacity)
			cand.convergesScore = options.rebalanceToConvergesScore(comparable, s)
			cand.rangeCount = int(s.Capacity.RangeCount)
			candidates = append(candidates, cand)
		}

		if len(candidates) == 0 {
			continue
		}

		if options.deterministicForTesting() {
			sort.Sort(sort.Reverse(byScoreAndID(candidates)))
		} else {
			sort.Sort(sort.Reverse(byScore(candidates)))
		}

		// Only return candidates better than the existing replica.
		improvementCandidates := candidates.betterThan(existing)
		if len(improvementCandidates) == 0 {
			continue
		}
		results = append(results, rebalanceOptions{
			existing:   existing,
			candidates: improvementCandidates,
		})
		log.KvDistribution.VEventf(ctx, 5, "rebalance candidates #%d: %s\nexisting replicas: %s",
			len(results), results[len(results)-1].candidates, results[len(results)-1].existing)
	}

	return results
}

// bestRebalanceTarget returns the best target to try to rebalance to out of
// the provided options, and removes it from the relevant candidate list.
// Also returns the existing replicas that the chosen candidate was compared to.
// Returns nil if there are no more targets worth rebalancing to.
func bestRebalanceTarget(
	randGen allocatorRand, options []rebalanceOptions,
) (target, existingCandidate *candidate) {
	bestIdx := -1
	var bestTarget *candidate
	var replaces candidate
	for i, option := range options {
		if len(option.candidates) == 0 {
			continue
		}
		target := option.candidates.selectBest(randGen)
		if target == nil {
			continue
		}
		existing := option.existing
		if betterRebalanceTarget(target, &existing, bestTarget, &replaces) == target {
			bestIdx = i
			bestTarget = target
			replaces = existing
		}
	}
	if bestIdx == -1 {
		return nil, nil
	}
	// Copy the selected target out of the candidates slice before modifying
	// the slice. Without this, the returned pointer likely will be pointing
	// to a different candidate than intended due to movement within the slice.
	copiedTarget := *bestTarget
	options[bestIdx].candidates = options[bestIdx].candidates.removeCandidate(copiedTarget)
	return &copiedTarget, &options[bestIdx].existing
}

// betterRebalanceTarget returns whichever of target1 or target2 is a larger
// improvement over its corresponding existing replica that it will be
// replacing in the range.
func betterRebalanceTarget(target1, existing1, target2, existing2 *candidate) *candidate {
	if target2 == nil {
		return target1
	}
	// Try to pick whichever target is a larger improvement over the replica that
	// they'll replace.
	comp1 := target1.compare(*existing1)
	comp2 := target2.compare(*existing2)
	if !scoresAlmostEqual(comp1, comp2) {
		if comp1 > comp2 {
			return target1
		}
		if comp1 < comp2 {
			return target2
		}
	}
	// If the two targets are equally better than their corresponding existing
	// replicas, just return whichever target is better.
	if target1.less(*target2) {
		return target2
	}
	return target1
}

// nodeHasReplica returns true if the provided NodeID contains an entry in
// the provided list of existing replicas.
func nodeHasReplica(nodeID roachpb.NodeID, existing []roachpb.ReplicationTarget) bool {
	for _, r := range existing {
		if r.NodeID == nodeID {
			return true
		}
	}
	return false
}

// StoreHasReplica returns true if the provided StoreID contains an entry in
// the provided list of existing replicas.
func StoreHasReplica(storeID roachpb.StoreID, existing []roachpb.ReplicationTarget) bool {
	for _, r := range existing {
		if r.StoreID == storeID {
			return true
		}
	}
	return false
}

// constraintsCheckFn determines whether the given store is a valid and/or
// necessary candidate for an addition of a new replica or a removal of an
// existing one.
type constraintsCheckFn func(roachpb.StoreDescriptor) (valid, necessary bool)

// rebalanceConstraintsCheckFn determines whether `toStore` is a valid and/or
// necessary replacement candidate for `fromStore` (which must contain an
// existing replica).
type rebalanceConstraintsCheckFn func(toStore, fromStore roachpb.StoreDescriptor) (valid, necessary bool)

// voterConstraintsCheckerForAllocation returns a constraintsCheckFn that
// determines whether a candidate for a new voting replica is valid and/or
// necessary as per the `voter_constraints` and `constraints` on the range.
//
// NB: Potential voting replica candidates are "valid" only if they satisfy both
// the `voter_constraints` as well as the overall `constraints`. Additionally,
// candidates are only marked "necessary" if they're required in order to
// satisfy either the `voter_constraints` set or the `constraints` set.
func voterConstraintsCheckerForAllocation(
	overallConstraints, voterConstraints constraint.AnalyzedConstraints,
) constraintsCheckFn {
	return func(s roachpb.StoreDescriptor) (valid, necessary bool) {
		overallConstraintsOK, necessaryOverall := allocateConstraintsCheck(s, overallConstraints)
		voterConstraintsOK, necessaryForVoters := allocateConstraintsCheck(s, voterConstraints)

		return overallConstraintsOK && voterConstraintsOK, necessaryOverall || necessaryForVoters
	}
}

// nonVoterConstraintsCheckerForAllocation returns a constraintsCheckFn that
// determines whether a candidate for a new non-voting replica is valid and/or
// necessary as per the `constraints` on the range.
//
// NB: Non-voting replicas don't care about `voter_constraints`, so that
// constraint set is entirely disregarded here.
func nonVoterConstraintsCheckerForAllocation(
	overallConstraints constraint.AnalyzedConstraints,
) constraintsCheckFn {
	return func(s roachpb.StoreDescriptor) (valid, necessary bool) {
		return allocateConstraintsCheck(s, overallConstraints)
	}
}

// voterConstraintsCheckerForRemoval returns a constraintsCheckFn that
// determines whether an existing voting replica is valid and/or necessary with
// respect to the `constraints` and `voter_constraints` on the range.
//
// NB: Candidates are marked invalid if their removal would result in a
// violation of `voter_constraints` or `constraints` on the range. They are
// marked necessary if constraints conformance (for either `constraints` or
// `voter_constraints`) is not possible without them.
func voterConstraintsCheckerForRemoval(
	overallConstraints, voterConstraints constraint.AnalyzedConstraints,
) constraintsCheckFn {
	return func(s roachpb.StoreDescriptor) (valid, necessary bool) {
		overallConstraintsOK, necessaryOverall := removeConstraintsCheck(s, overallConstraints)
		voterConstraintsOK, necessaryForVoters := removeConstraintsCheck(s, voterConstraints)

		return overallConstraintsOK && voterConstraintsOK, necessaryOverall || necessaryForVoters
	}
}

// nonVoterConstraintsCheckerForRemoval returns a constraintsCheckFn that
// determines whether an existing non-voting replica is valid and/or necessary
// with respect to the `constraints` on the range.
//
// NB: Candidates are marked invalid if their removal would result in a
// violation of `constraints` on the range. They are marked necessary if
// constraints conformance (for `constraints`) is not possible without them.
func nonVoterConstraintsCheckerForRemoval(
	overallConstraints constraint.AnalyzedConstraints,
) constraintsCheckFn {
	return func(s roachpb.StoreDescriptor) (valid, necessary bool) {
		return removeConstraintsCheck(s, overallConstraints)
	}
}

// voterConstraintsCheckerForRebalance returns a rebalanceConstraintsCheckFn
// that determines whether a given store is a valid and/or necessary rebalance
// candidate from a given store of an existing voting replica.
func voterConstraintsCheckerForRebalance(
	overallConstraints, voterConstraints constraint.AnalyzedConstraints,
) rebalanceConstraintsCheckFn {
	return func(toStore, fromStore roachpb.StoreDescriptor) (valid, necessary bool) {
		overallConstraintsOK, necessaryOverall := rebalanceFromConstraintsCheck(toStore, fromStore, overallConstraints)
		voterConstraintsOK, necessaryForVoters := rebalanceFromConstraintsCheck(toStore, fromStore, voterConstraints)

		return overallConstraintsOK && voterConstraintsOK, necessaryOverall || necessaryForVoters
	}
}

// nonVoterConstraintsCheckerForRebalance returns a rebalanceConstraintsCheckFn
// that determines whether a given store is a valid and/or necessary rebalance
// candidate from a given store of an existing non-voting replica.
func nonVoterConstraintsCheckerForRebalance(
	overallConstraints constraint.AnalyzedConstraints,
) rebalanceConstraintsCheckFn {
	return func(toStore, fromStore roachpb.StoreDescriptor) (valid, necessary bool) {
		return rebalanceFromConstraintsCheck(toStore, fromStore, overallConstraints)
	}
}

// allocateConstraintsCheck checks the potential allocation target store
// against all the constraints. If it matches a constraint at all, it's valid.
// If it matches a constraint that is not already fully satisfied by existing
// replicas, then it's necessary.
//
// NB: This assumes that the sum of all constraints.NumReplicas is equal to
// configured number of replicas for the range, or that there's just one set of
// constraints with NumReplicas set to 0. This is meant to be enforced in the
// config package.
func allocateConstraintsCheck(
	store roachpb.StoreDescriptor, analyzed constraint.AnalyzedConstraints,
) (valid bool, necessary bool) {
	// All stores are valid when there are no constraints.
	if len(analyzed.Constraints) == 0 {
		return true, false
	}

	for i, constraints := range analyzed.Constraints {
		if constraintsOK := constraint.ConjunctionsCheck(
			store, constraints.Constraints,
		); constraintsOK {
			valid = true
			matchingStores := analyzed.SatisfiedBy[i]
			// NB: We check for "<" here instead of "<=" because `matchingStores`
			// doesn't include `store`. Thus, `store` is only marked necessary if we
			// have strictly fewer matchingStores than we need.
			if len(matchingStores) < int(constraints.NumReplicas) {
				return true, true
			}
		}
	}

	if analyzed.UnconstrainedReplicas {
		valid = true
	}

	return valid, false
}

// removeConstraintsCheck checks the existing store against the analyzed
// constraints, determining whether it's valid (matches some constraint) and
// necessary (matches some constraint that no other existing replica matches).
// The difference between this and allocateConstraintsCheck is that this is to
// be used on an existing replica of the range, not a potential addition.
func removeConstraintsCheck(
	store roachpb.StoreDescriptor, analyzed constraint.AnalyzedConstraints,
) (valid bool, necessary bool) {
	// All stores are valid when there are no constraints.
	if len(analyzed.Constraints) == 0 {
		return true, false
	}

	// The store satisfies none of the constraints, and the zone is not configured
	// to desire more replicas than constraints have been specified for.
	if len(analyzed.Satisfies[store.StoreID]) == 0 && !analyzed.UnconstrainedReplicas {
		return false, false
	}

	// Check if the store matches a constraint that isn't overly satisfied.
	// If so, then keeping it around is necessary to ensure that constraint stays
	// fully satisfied.
	for _, constraintIdx := range analyzed.Satisfies[store.StoreID] {
		if len(analyzed.SatisfiedBy[constraintIdx]) <= int(analyzed.Constraints[constraintIdx].NumReplicas) {
			return true, true
		}
	}

	// If neither of the above is true, then the store is valid but nonessential.
	// NOTE: We could be more precise here by trying to find the least essential
	// existing replica and only considering that one nonessential, but this is
	// sufficient to avoid violating constraints.
	return true, false
}

// rebalanceConstraintsCheck checks the potential rebalance target store
// against the analyzed constraints, determining whether it's valid whether it
// will be necessary if fromStoreID (an existing replica) is removed from the
// range.
func rebalanceFromConstraintsCheck(
	store, fromStoreID roachpb.StoreDescriptor, analyzed constraint.AnalyzedConstraints,
) (valid bool, necessary bool) {
	// All stores are valid when there are no constraints.
	if len(analyzed.Constraints) == 0 {
		return true, false
	}

	// Check the store against all the constraints. If it matches a constraint at
	// all, it's valid. If it matches a constraint that is not already fully
	// satisfied by existing replicas or that is only fully satisfied because of
	// fromStoreID, then it's necessary.
	//
	// NB: This assumes that the sum of all constraints.NumReplicas is equal to
	// configured number of replicas for the range, or that there's just one set
	// of constraints with NumReplicas set to 0. This is meant to be enforced in
	// the config package.
	for i, constraints := range analyzed.Constraints {
		if constraintsOK := constraint.ConjunctionsCheck(
			store, constraints.Constraints,
		); constraintsOK {
			valid = true
			matchingStores := analyzed.SatisfiedBy[i]
			if len(matchingStores) < int(constraints.NumReplicas) ||
				(len(matchingStores) == int(constraints.NumReplicas) &&
					containsStore(analyzed.SatisfiedBy[i], fromStoreID.StoreID)) {
				return true, true
			}
		}
	}

	if analyzed.UnconstrainedReplicas {
		valid = true
	}

	return valid, false
}

// containsStore returns true if the list of StoreIDs contains the target.
func containsStore(stores []roachpb.StoreID, target roachpb.StoreID) bool {
	for _, storeID := range stores {
		if storeID == target {
			return true
		}
	}
	return false
}

// RangeDiversityScore returns a value between 0 and 1 based on how diverse the
// given range is. A higher score means the range is more diverse.
// All below diversity-scoring methods should in theory be implemented by
// calling into this one, but they aren't to avoid allocations.
func RangeDiversityScore(existingStoreLocalities map[roachpb.StoreID]roachpb.Locality) float64 {
	var sumScore float64
	var numSamples int
	for s1, l1 := range existingStoreLocalities {
		for s2, l2 := range existingStoreLocalities {
			// Only compare pairs of replicas where s2 > s1 to avoid computing the
			// diversity score between each pair of localities twice.
			if s2 <= s1 {
				continue
			}
			sumScore += l1.DiversityScore(l2)
			numSamples++
		}
	}
	if numSamples == 0 {
		return roachpb.MaxDiversityScore
	}
	return sumScore / float64(numSamples)
}

// diversityAllocateScore returns a value between 0 and 1 based on how
// desirable it would be to add a replica to store. A higher score means the
// store is a better fit.
func diversityAllocateScore(
	store roachpb.StoreDescriptor, existingStoreLocalities map[roachpb.StoreID]roachpb.Locality,
) float64 {
	var sumScore float64
	var numSamples int
	// We don't need to calculate the overall diversityScore for the range, just
	// how well the new store would fit, because for any store that we might
	// consider adding the pairwise average diversity of the existing replicas
	// is the same.
	for _, locality := range existingStoreLocalities {
		newScore := store.Locality().DiversityScore(locality)
		sumScore += newScore
		numSamples++
	}
	// If the range has no replicas, any node would be a perfect fit.
	if numSamples == 0 {
		return roachpb.MaxDiversityScore
	}
	return sumScore / float64(numSamples)
}

// diversityRemovalScore returns a value between 0 and 1 based on how desirable
// it would be to remove a store's replica of a range.  A higher score indicates
// that the node is a better fit (i.e. keeping it around is good for diversity).
func diversityRemovalScore(
	storeID roachpb.StoreID, existingStoreLocalities map[roachpb.StoreID]roachpb.Locality,
) float64 {
	var sumScore float64
	var numSamples int
	locality := existingStoreLocalities[storeID]
	// We don't need to calculate the overall diversityScore for the range,
	// because the original overall diversityScore of this range is always the
	// same.
	for otherStoreID, otherLocality := range existingStoreLocalities {
		if otherStoreID == storeID {
			continue
		}
		newScore := locality.DiversityScore(otherLocality)
		sumScore += newScore
		numSamples++
	}
	if numSamples == 0 {
		return roachpb.MaxDiversityScore
	}
	return sumScore / float64(numSamples)
}

// diversityRebalanceScore returns a value between 0 and 1 based on how
// desirable it would be to rebalance to `store` from any of the existing
// stores. Because the store to be removed isn't specified, this assumes that
// the worst-fitting store from a diversity perspective will be removed. A
// higher score indicates that the provided store is a better fit for the range.
func diversityRebalanceScore(
	store roachpb.StoreDescriptor, existingStoreLocalities map[roachpb.StoreID]roachpb.Locality,
) float64 {
	if len(existingStoreLocalities) == 0 {
		return roachpb.MaxDiversityScore
	}
	var maxScore float64
	// For every existing node, calculate what the diversity score would be if we
	// remove that node's replica to replace it with one on the provided store.
	for removedStoreID := range existingStoreLocalities {
		score := diversityRebalanceFromScore(store, removedStoreID, existingStoreLocalities)
		if score > maxScore {
			maxScore = score
		}
	}
	return maxScore
}

// diversityRebalanceFromScore returns a value between 0 and 1 based on how
// desirable it would be to rebalance away from the specified node and to
// the specified store. This is the same as diversityRebalanceScore, but for
// the case where there's a particular replica we want to consider removing.
// A higher score indicates that the provided store is a better fit for the
// range.
func diversityRebalanceFromScore(
	store roachpb.StoreDescriptor,
	fromStoreID roachpb.StoreID,
	existingStoreLocalities map[roachpb.StoreID]roachpb.Locality,
) float64 {
	// Compute the pairwise diversity score of all replicas that will exist
	// after adding store and removing fromNodeID.
	var sumScore float64
	var numSamples int
	for storeID, locality := range existingStoreLocalities {
		if storeID == fromStoreID {
			continue
		}
		newScore := store.Locality().DiversityScore(locality)
		sumScore += newScore
		numSamples++
		for otherStoreID, otherLocality := range existingStoreLocalities {
			// Only compare pairs of replicas where otherNodeID > nodeID to avoid
			// computing the diversity score between each pair of localities twice.
			if otherStoreID <= storeID || otherStoreID == fromStoreID {
				continue
			}
			newScore := locality.DiversityScore(otherLocality)
			sumScore += newScore
			numSamples++
		}
	}
	if numSamples == 0 {
		return roachpb.MaxDiversityScore
	}
	return sumScore / float64(numSamples)
}

// balanceStatus represents a ternary classification of a candidate based on
// the candidate's load, the mean among compared candidates and the threshold
// applied to the mean in order to generate a bounds.The balance score follows
// the rule below:
// candidate in [overfull,+inf)      -> overfull
// candidate in (-inf,underfull)     -> underfull
// candidate in [underfull,overfull) -> aroundTheMean
type balanceStatus int

const (
	// overfull indicates that the candidate's load is at least as large as the
	// overfull threshold.
	overfull balanceStatus = -1
	// aroundTheMean indicates that the candidate's load is less than the
	// overfull threshold and at least as large as the underfull threshold.
	aroundTheMean balanceStatus = 0
	// underfull indicates that the candidate's load is strictly less than the
	// underfull threshold.
	underfull balanceStatus = 1
)

func overfullRangeThreshold(options *RangeCountScorerOptions, mean float64) float64 {
	return mean + math.Max(mean*options.rangeRebalanceThreshold, minRangeRebalanceThreshold)
}

func underfullRangeThreshold(options *RangeCountScorerOptions, mean float64) float64 {
	return mean - math.Max(mean*options.rangeRebalanceThreshold, minRangeRebalanceThreshold)
}

func rebalanceConvergesRangeCountOnMean(
	sl storepool.StoreList, sc roachpb.StoreCapacity, newRangeCount int32,
) bool {
	return convergesOnMean(float64(sc.RangeCount), float64(newRangeCount), sl.CandidateRanges.Mean)
}

func convergesOnMean(oldVal, newVal, mean float64) bool {
	return math.Abs(newVal-mean) < math.Abs(oldVal-mean)
}

// StoreHealthOptions is the scorer options for store health. It is
// used to inform scoring based on the health of a store.
type StoreHealthOptions struct {
	EnforcementLevel    StoreHealthEnforcement
	L0SublevelThreshold int64
}

// readAmpIsHealthy returns true if the store read amplification does not exceed
// the cluster threshold and mean, or the enforcement level does not include
// excluding candidates from being allocation targets.
func (o StoreHealthOptions) readAmpIsHealthy(
	ctx context.Context, store roachpb.StoreDescriptor, avg float64,
) bool {
	if o.EnforcementLevel == StoreHealthNoAction ||
		store.Capacity.L0Sublevels < o.L0SublevelThreshold {
		return true
	}

	// Still log an event when the L0 sub-levels exceeds the threshold, however
	// does not exceed the cluster average. This is enabled to avoid confusion
	// where candidate stores are still targets, despite exeeding the
	// threshold.
	if float64(store.Capacity.L0Sublevels) < avg*l0SubLevelWaterMark {
		log.KvDistribution.VEventf(ctx, 5, "s%d, allocate check l0 sublevels %d exceeds threshold %d, but below average: %f, action enabled %d",
			store.StoreID, store.Capacity.L0Sublevels,
			o.L0SublevelThreshold, avg, o.EnforcementLevel)
		return true
	}

	log.KvDistribution.VEventf(ctx, 5, "s%d, allocate check l0 sublevels %d exceeds threshold %d, above average: %f, action enabled %d",
		store.StoreID, store.Capacity.L0Sublevels,
		o.L0SublevelThreshold, avg, o.EnforcementLevel)

	// The store is only considered unhealthy when the enforcement level is
	// storeHealthBlockAll.
	return o.EnforcementLevel < StoreHealthBlockAll
}

// rebalanceToReadAmpIsHealthy returns true if the store read amplification does
// not exceed the cluster threshold and mean, or the enforcement level does not
// include excluding candidates from being rebalancing targets.
func (o StoreHealthOptions) rebalanceToReadAmpIsHealthy(
	ctx context.Context, store roachpb.StoreDescriptor, avg float64,
) bool {
	if o.EnforcementLevel == StoreHealthNoAction ||
		store.Capacity.L0Sublevels < o.L0SublevelThreshold {
		return true
	}

	if float64(store.Capacity.L0Sublevels) < avg*l0SubLevelWaterMark {
		log.KvDistribution.VEventf(ctx, 5, "s%d, allocate check l0 sublevels %d exceeds threshold %d, but below average watermark: %f, action enabled %d",
			store.StoreID, store.Capacity.L0Sublevels,
			o.L0SublevelThreshold, avg*l0SubLevelWaterMark, o.EnforcementLevel)
		return true
	}

	log.KvDistribution.VEventf(ctx, 5, "s%d, allocate check l0 sublevels %d exceeds threshold %d, above average watermark: %f, action enabled %d",
		store.StoreID, store.Capacity.L0Sublevels,
		o.L0SublevelThreshold, avg*l0SubLevelWaterMark, o.EnforcementLevel)

	// The store is only considered unhealthy when the enforcement level is
	// storeHealthBlockRebalanceTo or storeHealthBlockAll.
	return o.EnforcementLevel < StoreHealthBlockRebalanceTo
}

// rebalanceToMaxCapacityCheck returns true if the store has enough room to
// accept a rebalance. The bar for this is stricter than for whether a store
// has enough room to accept a necessary replica (i.e. via AllocateCandidates).
func rebalanceToMaxCapacityCheck(store roachpb.StoreDescriptor) bool {
	return store.Capacity.FractionUsed() < rebalanceToMaxFractionUsedThreshold
}

func scoresAlmostEqual(score1, score2 float64) bool {
	return math.Abs(score1-score2) < epsilon
}
