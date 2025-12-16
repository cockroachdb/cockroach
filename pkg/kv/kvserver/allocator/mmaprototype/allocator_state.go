// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mmaprototype

import (
	"cmp"
	"context"
	"fmt"
	"math"
	"math/rand"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"github.com/cockroachdb/redact/interfaces"
)

type allocatorState struct {
	// Locking.
	//
	// Now (for prototype):
	//
	// mu is a crude stopgap to make everything in the MMA thread-safe. Every
	// method that implements the Allocator interface must acquire this mutex.
	//
	// Future (for production):
	//
	// The main concern with the cure approach is that mu is held while doing
	// slow decision-making. That prevents (a) other decision-making, (b) blocks
	// updates from AllocatorSync from other components that are not currently
	// using MMA.
	//
	// Option 1 Release mu frequently:
	//
	// The allocatorState is deliberately tightly coupled and we want it to be
	// internally consistent. We expect clusterState.rebalanceStores to be the
	// longest lived holder of mu. We could modify it so that after it computes
	// the cluster means and computes the overloaded stores it releases mu, and
	// then reacquires it when trying to shed for each overloaded store. Note
	// that when replicateQueue and leaseQueue also call into MMA, those calls
	// will be per range and will hold mu for a very short time.
	//
	// This option doesn't change the fact that effectively everything done by
	// MMA is single threaded, so it cannot scale to consume more than 1 vCPU.
	// However, we also would like MMA to be efficient enough that even beefy
	// nodes don't spend more than 1 vCPU on MMA, so perhaps this is ok.
	//
	// Option 2: Copy-on-write:
	//
	// Unclear how to make cow efficient for these data-structures.
	//
	// Option 3: Queue updates:
	//
	// Read lock would be held for computing decisions and then these decisions
	// would be queued for adding as pending changes to MMA state. Decisions can
	// be returned to enacting module with the expected range descriptor,
	// without waiting for the addition to the pending changes in MMA. The
	// assumption here is that there will be few conflicts on a single range,
	// and those rare conflicts will result in some decisions becoming noops.
	// The problem is that long-lived read locks with write locks result in
	// read-read contention (to prevent writer starvation). If we had a
	// try-write-lock that could quickly return with failure then we could avoid
	// this. We could of course build our own queueing mechanism instead of
	// relying on the queueing in mutex.

	mu syncutil.Mutex

	// TODO(sumeer): metricsMap is protected by mu. Nest in struct with mu, when
	// locking story is cleaned up.

	// metricsMap is keyed by local StoreID.
	metricsMap map[roachpb.StoreID]*metricsEtc
	cs         *clusterState

	// Ranges that are under-replicated, over-replicated, don't satisfy
	// constraints, have low diversity etc. Avoids iterating through all ranges.
	// A range is removed from this map if it has pending changes -- when those
	// pending changes go away it gets added back so we can check on it.
	rangesNeedingAttention map[roachpb.RangeID]struct{}

	diversityScoringMemo *diversityScoringMemo

	rand *rand.Rand

	metricsUnregisteredEvery log.EveryN
}

var _ Allocator = &allocatorState{}

// NewAllocatorState constructs a new implementation of Allocator.
//
// The metricRegistryProvider allows the allocator to lazily initialize
// per-local-store metrics once the StoreID is known. It can be nil in tests,
// in which case no metrics will be collected.
func NewAllocatorState(ts timeutil.TimeSource, rand *rand.Rand) *allocatorState {
	interner := newStringInterner()
	cs := newClusterState(ts, interner)
	return &allocatorState{
		metricsMap:               map[roachpb.StoreID]*metricsEtc{},
		cs:                       cs,
		rangesNeedingAttention:   map[roachpb.RangeID]struct{}{},
		diversityScoringMemo:     newDiversityScoringMemo(),
		rand:                     rand,
		metricsUnregisteredEvery: log.Every(time.Minute),
	}
}

type metricsEtc struct {
	counters             *counterMetrics
	passMetricsAndLogger *rebalancingPassMetricsAndLogger
	metricsRegistered    bool
}

// These constants are semi-arbitrary.

// Don't start moving ranges from a cpu overloaded remote store, to give it
// some time to shed its leases.
const remoteStoreLeaseSheddingGraceDuration = 2 * time.Minute
const overloadGracePeriod = time.Minute

func (a *allocatorState) InitMetricsForLocalStore(
	ctx context.Context, localStoreID roachpb.StoreID, registry *metric.Registry,
) {
	a.mu.Lock()
	defer a.mu.Unlock()
	_ = a.ensureMetricsForLocalStoreLocked(ctx, localStoreID, registry)
}

func (a *allocatorState) getCounterMetricsForLocalStoreLocked(
	ctx context.Context, localStoreID roachpb.StoreID,
) *counterMetrics {
	return a.ensureMetricsForLocalStoreLocked(ctx, localStoreID, nil).counters
}

func (a *allocatorState) preparePassMetricsAndLoggerLocked(
	ctx context.Context, localStoreID roachpb.StoreID,
) *rebalancingPassMetricsAndLogger {
	pm := a.ensureMetricsForLocalStoreLocked(ctx, localStoreID, nil).passMetricsAndLogger
	pm.resetForRebalancingPass()
	return pm
}

// ensureMetricsForLocalStoreLocked ensures that the metrics for localStoreID
// exist, creating them if necessary. The registry parameter, when non-nil, is
// used to register the metrics if they have not been registered previously.
func (a *allocatorState) ensureMetricsForLocalStoreLocked(
	ctx context.Context, localStoreID roachpb.StoreID, registry *metric.Registry,
) *metricsEtc {
	m, ok := a.metricsMap[localStoreID]
	if !ok {
		m = &metricsEtc{
			counters:             makeCounterMetrics(),
			passMetricsAndLogger: makeRebalancingPassMetricsAndLogger(localStoreID),
		}
		a.metricsMap[localStoreID] = m
	}
	if !m.metricsRegistered {
		if registry != nil {
			registry.AddMetricStruct(*m.counters)
			registry.AddMetricStruct(m.passMetricsAndLogger.m)
			m.metricsRegistered = true
		} else if a.shouldLogUnregisteredMetrics() {
			log.KvDistribution.Warningf(ctx, "metrics for store s%v are unregistered", localStoreID)
		}
	}
	return m
}

// shouldLogUnregisteredMetrics returns true if a warning should be logged when
// metrics cannot be registered due to a nil registry. This logging is skipped
// in test builds since tests often don't provide a metrics registry. In
// production, it rate-limits to once per minute to avoid log spam.
func (a *allocatorState) shouldLogUnregisteredMetrics() bool {
	return !buildutil.CrdbTestBuild && a.metricsUnregisteredEvery.ShouldLog()
}

func (a *allocatorState) LoadSummaryForAllStores(ctx context.Context) string {
	return a.cs.loadSummaryForAllStores(ctx)
}

// SetStore implements the Allocator interface.
func (a *allocatorState) SetStore(store StoreAttributesAndLocality) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.cs.setStore(store.withNodeTier())
}

// ProcessStoreLoadMsg implements the Allocator interface.
func (a *allocatorState) ProcessStoreLoadMsg(ctx context.Context, msg *StoreLoadMsg) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.cs.processStoreLoadMsg(ctx, msg)
}

// AdjustPendingChangeDisposition implements the Allocator interface.
func (a *allocatorState) AdjustPendingChangeDisposition(
	ctx context.Context, change ExternalRangeChange, success bool,
) {
	a.mu.Lock()
	defer a.mu.Unlock()
	metrics := a.getCounterMetricsForLocalStoreLocked(ctx, change.localStoreID)
	isLeaseTransfer := change.IsPureTransferLease()
	switch change.origin {
	case OriginExternal:
		if isLeaseTransfer {
			if success {
				metrics.ExternalLeaseChangeSuccess.Inc(1)
			} else {
				metrics.ExternalLeaseChangeFailure.Inc(1)
			}
		} else {
			if success {
				metrics.ExternalReplicaChangeSuccess.Inc(1)
			} else {
				metrics.ExternalReplicaChangeFailure.Inc(1)
			}
		}
	case originMMARebalance:
		if isLeaseTransfer {
			if success {
				metrics.RebalanceLeaseChangeSuccess.Inc(1)
			} else {
				metrics.RebalanceLeaseChangeFailure.Inc(1)
			}
		} else {
			if success {
				metrics.RebalanceReplicaChangeSuccess.Inc(1)
			} else {
				metrics.RebalanceReplicaChangeFailure.Inc(1)
			}
		}
	}
	_, ok := a.cs.ranges[change.RangeID]
	if !ok {
		// Range no longer exists. This can happen if the StoreLeaseholderMsg
		// which included the effect of the change that transferred the lease away
		// was already processed, causing the range to no longer be tracked by the
		// allocator.
		return
	}
	// NB: It is possible that some of the changes have already been enacted via
	// StoreLeaseholderMsg, and even been garbage collected. So no assumption
	// can be made about whether these changes will be found in the allocator's
	// state. We gather the found changes.
	var changes []*pendingReplicaChange
	for _, c := range change.Changes {
		ch, ok := a.cs.pendingChanges[c.changeID]
		if !ok {
			continue
		}
		changes = append(changes, ch)
	}
	if len(changes) == 0 {
		return
	}
	if !success {
		// Check that we can undo these changes.
		if err := a.cs.preCheckOnUndoReplicaChanges(PendingRangeChange{
			RangeID:               change.RangeID,
			pendingReplicaChanges: changes,
		}); err != nil {
			panic(err)
		}
	}
	for _, c := range changes {
		if success {
			a.cs.pendingChangeEnacted(c.changeID, a.cs.ts.Now())
		} else {
			a.cs.undoPendingChange(c.changeID)
		}
	}
}

// RegisterExternalChange implements the Allocator interface.
func (a *allocatorState) RegisterExternalChange(
	ctx context.Context, localStoreID roachpb.StoreID, change PendingRangeChange,
) (_ ExternalRangeChange, ok bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	counterMetrics := a.getCounterMetricsForLocalStoreLocked(ctx, localStoreID)
	if err := a.cs.preCheckOnApplyReplicaChanges(change); err != nil {
		counterMetrics.ExternalRegisterFailure.Inc(1)
		log.KvDistribution.Infof(context.Background(),
			"did not register external changes: due to %v", err)
		return ExternalRangeChange{}, false
	} else {
		counterMetrics.ExternalRegisterSuccess.Inc(1)
	}
	a.cs.addPendingRangeChange(change)
	return MakeExternalRangeChange(OriginExternal, localStoreID, change), true
}

// ComputeChanges implements the Allocator interface.
func (a *allocatorState) ComputeChanges(
	ctx context.Context, msg *StoreLeaseholderMsg, opts ChangeOptions,
) []ExternalRangeChange {
	a.mu.Lock()
	defer a.mu.Unlock()
	if msg.StoreID != opts.LocalStoreID {
		panic(fmt.Sprintf("ComputeChanges: expected StoreID %d, got %d", opts.LocalStoreID, msg.StoreID))
	}
	if opts.DryRun {
		panic(errors.AssertionFailedf("unsupported dry-run mode"))
	}
	counterMetrics := a.getCounterMetricsForLocalStoreLocked(ctx, opts.LocalStoreID)
	a.cs.processStoreLeaseholderMsg(ctx, msg, counterMetrics)
	var passObs *rebalancingPassMetricsAndLogger
	if opts.PeriodicCall {
		passObs = a.preparePassMetricsAndLoggerLocked(ctx, opts.LocalStoreID)
	}
	re := newRebalanceEnv(a.cs, a.rand, a.diversityScoringMemo, a.cs.ts.Now(), passObs)
	return re.rebalanceStores(ctx, opts.LocalStoreID)
}

// AdminRelocateOne implements the Allocator interface.
func (a *allocatorState) AdminRelocateOne(
	desc *roachpb.RangeDescriptor,
	conf *roachpb.SpanConfig,
	leaseholderStore roachpb.StoreID,
	voterTargets, nonVoterTargets []roachpb.ReplicationTarget,
	transferLeaseToFirstVoter bool,
) ([]pendingReplicaChange, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	panic("unimplemented")
}

// AdminScatterOne implements the Allocator interface.
func (a *allocatorState) AdminScatterOne(
	rangeID roachpb.RangeID, canTransferLease bool, opts ChangeOptions,
) ([]pendingReplicaChange, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	panic("unimplemented")
}

// KnownStores implements the Allocator interface.
func (a *allocatorState) KnownStores() map[roachpb.StoreID]struct{} {
	stores := make(map[roachpb.StoreID]struct{})
	a.mu.Lock()
	defer a.mu.Unlock()
	// The allocatorState is a wrapper around the clusterState, which contains
	// all the stores.
	for storeID := range a.cs.stores {
		stores[storeID] = struct{}{}
	}
	return stores
}

type candidateInfo struct {
	roachpb.StoreID
	storeLoadSummary
	// Higher is better.
	diversityScore float64
	// Lower is better.
	leasePreferenceIndex int32
}

type candidateSet struct {
	candidates []candidateInfo
	means      *meansLoad
}

type ignoreLevel uint8

const (
	// Default.
	ignoreLoadNoChangeAndHigher ignoreLevel = iota

	// NB: loadThreshold is always > loadNoChange.
	//
	// Has been overloaded over ignoreLoadThresholdAndHigherGraceDuration
	// (5*time.Minute). We are getting desperate and consider more overloaded
	// stores as long as they are < loadThreshold.
	ignoreLoadThresholdAndHigher

	// Has been overloaded over ignoreHigherThanLoadThresholdGraceDuration
	// (8*time.Minute). We are getting desperate and consider more overloaded
	// stores as long as they are <= loadThreshold.
	ignoreHigherThanLoadThreshold
)

const ignoreLoadThresholdAndHigherGraceDuration = 5 * time.Minute
const ignoreHigherThanLoadThresholdGraceDuration = 8 * time.Minute

func (i ignoreLevel) String() string {
	switch i {
	case ignoreLoadNoChangeAndHigher:
		return "only consider targets < LoadNoChange"
	case ignoreLoadThresholdAndHigher:
		return "only consider targets < load threshold"
	case ignoreHigherThanLoadThreshold:
		return "only consider targets <= load threshold"
	default:
		panic(fmt.Sprintf("unknown: %d", i))
	}
}

// SafeFormat implements the redact.SafeFormatter interface.
func (i ignoreLevel) SafeFormat(s interfaces.SafePrinter, verb rune) {
	s.SafeInt(redact.SafeInt(i))
}

// The logic in sortTargetCandidateSetAndPick related to load is a heuristic
// motivated by the following observations:
//
// 1. The high level objective is mostly to move towards the mean along all
// resource dimensions. For now, we are fuzzy on whether this should be the
// usage mean or the utilization mean, since historically the allocated has
// considered the former only, while mmaprototype in loadSummaryForDimension considers
// both.
//
// 2. We want to minimize range movement (and to a lesser extent lease
// movement) when moving towards the mean, so it is preferable to first move
// to stores that are not overloaded along any dimension, since they are
// guaranteed to not have to shed the moved load. Hence sorting based on the
// overall store loadSummary first for the targets.
//
// 3. It is possible (as also demonstrated by asim tests) that with multiple
// metrics, we can have scenarios where every store is loadNoChange or
// overloaded along some dimension while underloaded along another dimension
// (the latter is necessarily true if overload and underload are based on the
// mean). We have to be able to rebalance out of such local optima. One way to
// do so would be introduce the ability to decide changes to multiple ranges
// at once, however this adds code complexity. Instead, we can ignore the fact
// that the target is overall overloaded, and pick a candidate that is most
// underloaded in the dimension that the source wants to shed. This should
// allow the target to accept the range and later shed some other range. There
// is the possibility that will result in more range movement until we reach
// the goal state, but so be it.

// In the set of candidates it is possible that some are overloaded, or have
// too many pending changes. It divides them into sets with equal diversity
// score and sorts such that the set with higher diversity score is considered
// before one with lower diversity score. Then it finds the best diversity set
// which has some candidates that are not overloaded wrt disk capacity. Within
// this set it will exclude candidates that are overloaded or have too many
// pending changes, and then pick randomly among the least loaded ones.
//
// Our enum for loadSummary is quite coarse. This has pros and cons: the pros
// are (a) the coarseness increases the probability that we do eventually find
// something that can handle the multidimensional load of this range, (b)
// random picking in a large set results in different stores with allocators
// making different decisions, which reduces thrashing. The con is that we may
// not select the candidate that is the very best. Normally, we only consider
// candidates in the best equivalence class defined by the loadSummary
// aggregated across all dimensions, which is already coarse as mentioned
// above. However, when ignoreHigherThanLoadThreshold is set and an
// overloadedDim is provided, we extend beyond the first equivalence class, to
// consider all candidates that are underloaded in the overloadedDim.
//
// The caller must not exclude any candidates based on load or
// maxFractionPendingIncrease. That filtering must happen here. Depending on
// the value of ignoreLevel, only candidates < loadThreshold may be
// considered.
//
// overloadDim, if not set to NumLoadDimensions, represents the dimension that
// is overloaded in the source. It is used to narrow down the candidates to
// those that are most underloaded in that dimension, when all the candidates
// have an aggregate load summary (across all dimensions) that is >=
// loadNoChange. This function guarantees that when overloadedDim is set, all
// candidates returned will be < loadNoChange in that dimension.
//
// overloadDim will be set to NumLoadDimensions when the source is not
// shedding due to overload (say due to (impending) failure). In this case the
// caller should set loadThreshold to overloadSlow and ignoreLevel to
// ignoreHigherThanLoadThreshold, to maximize the probability of finding a
// candidate.
func sortTargetCandidateSetAndPick(
	ctx context.Context,
	cands candidateSet,
	loadThreshold loadSummary,
	ignoreLevel ignoreLevel,
	overloadedDim LoadDimension,
	rng *rand.Rand,
	maxFractionPendingThreshold float64,
	failLogger func(shedResult),
) roachpb.StoreID {
	var b strings.Builder
	var formatCandidatesLog = func(b *strings.Builder, candidates []candidateInfo) redact.SafeString {
		b.Reset()
		for _, c := range candidates {
			if overloadedDim != NumLoadDimensions {
				fmt.Fprintf(b, " s%v(SLS:%v, overloadedDimLoadSummary:%v)", c.StoreID, c.sls, c.dimSummary[overloadedDim])
			} else {
				fmt.Fprintf(b, " s%v(SLS:%v)", c.StoreID, c.storeLoadSummary)
			}
		}
		if len(candidates) > 0 {
			fmt.Fprintf(b, ", overloadedDim:%s", overloadedDim)
		}
		return redact.SafeString(b.String())
	}

	if loadThreshold <= loadNoChange {
		panic("loadThreshold must be > loadNoChange")
	}
	slices.SortFunc(cands.candidates, func(a, b candidateInfo) int {
		if diversityScoresAlmostEqual(a.diversityScore, b.diversityScore) {
			// Note: Consider the case where the current leaseholder's LPI is
			// 3 (lower is better) and we have the following candidates:
			// - LPI=1 SLS=normal
			// - LPI=2 SLS=low
			// Currently we consider the low-SLS candidate first. This is in
			// contrast to the single-metric allocator, which only considers
			// candidates in the lowest-SLS class (i.e. wouldn't even consider
			// the low-SLS candidate since we have a candidate at LPI=1).  If we
			// make the corresponding change in candidateToMoveLease, we would
			// match the single-metric allocator's behavior, but it's unclear
			// that that would be better. A good middle ground could be sorting
			// here by LPI first, then SLS. That should result in mma preferring
			// improving the lease preference, but if that is not possible, it
			// would settle for not making it worse (than the current
			// leaseholder), which the single-metric allocator won't.
			//
			// TODO(tbg): consider changing this to sort by LPI first, then SLS.
			return cmp.Or(cmp.Compare(a.sls, b.sls),
				cmp.Compare(a.leasePreferenceIndex, b.leasePreferenceIndex),
				cmp.Compare(a.StoreID, b.StoreID))
		}
		return -cmp.Compare(a.diversityScore, b.diversityScore)
	})
	bestDiversity := cands.candidates[0].diversityScore
	j := 0
	// Iterate over candidates with the same diversity. First such set that is
	// not disk capacity constrained is where we stop. Even if they can't accept
	// because they have too many pending changes or can't handle the addition
	// of the range. That is, we are not willing to reduce diversity when
	// rebalancing ranges. When rebalancing leases, the diversityScore of all
	// the candidates will be 0.
	for i, cand := range cands.candidates {
		if !diversityScoresAlmostEqual(bestDiversity, cand.diversityScore) {
			if j == 0 {
				// Don't have any candidates yet.
				bestDiversity = cand.diversityScore
			} else {
				// Have a set of candidates.
				if s := formatCandidatesLog(&b, cands.candidates[i:]); s != "" {
					log.KvDistribution.VEventf(ctx, 2, "discarding candidates due to lower diversity score: %s", s)
				}
				break
			}
		}
		// Diversity is the same. Include if not reaching disk capacity.
		// TODO(tbg): remove highDiskSpaceUtilization check here. These candidates
		// should instead be filtered out by retainReadyLeaseTargetStoresOnly (which
		// filters down the initial candidate set before computing the mean).
		if !cand.highDiskSpaceUtilization {
			cands.candidates[j] = cand
			j++
		} else {
			log.KvDistribution.VEventf(ctx, 2, "discarding candidate due to high disk space utilization: %v", cand.StoreID)
		}
	}
	if j == 0 {
		log.KvDistribution.VEventf(ctx, 2, "sortTargetCandidateSetAndPick: no candidates due to disk space util")
		failLogger(noCandidateDiskSpaceUtil)
		return 0
	}

	// Every candidate in [0:j] has same diversity and is sorted by increasing
	// load, and within the same load by increasing leasePreferenceIndex.
	cands.candidates = cands.candidates[:j]
	j = 0
	// Filter out the candidates that are overloaded or have too many pending
	// changes.
	//
	// lowestLoad is the load of the set of candidates with the lowest load,
	// among which we will later pick. If the set is found to be empty in the
	// following loop, the lowestLoad is updated.
	//
	// Consider the series of sets of candidates that have the same sls. The
	// only reason we will consider a set later than the first one is if the
	// earlier sets get fully discarded solely because of nls and have no
	// pending changes, or because of ignoreHigherThanLoadThreshold.
	lowestLoadSet := cands.candidates[0].sls
	currentLoadSet := lowestLoadSet
	discardedCandsHadNoPendingChanges := true
	for i, cand := range cands.candidates {
		if cand.sls > currentLoadSet {
			if !discardedCandsHadNoPendingChanges {
				// Never go to the next set if we have discarded candidates that have
				// pending changes. We will wait for those to have no pending changes
				// before we consider later sets.
				if s := formatCandidatesLog(&b, cands.candidates[i:]); s != "" {
					log.KvDistribution.VEventf(ctx, 2,
						"candidate with pending changes was discarded, discarding remaining candidates with higher load: %s", s)
				}
				break
			}
			currentLoadSet = cand.sls
		}
		if cand.sls > lowestLoadSet {
			if j == 0 {
				// This is the lowestLoad set being considered now.
				lowestLoadSet = cand.sls
			} else if ignoreLevel < ignoreHigherThanLoadThreshold || overloadedDim == NumLoadDimensions {
				// Past the lowestLoad set. We don't care about these.
				if s := formatCandidatesLog(&b, cands.candidates[i:]); s != "" {
					log.KvDistribution.VEventf(ctx, 2,
						"discarding candidates with higher load than lowestLoadSet(%s): %s", lowestLoadSet.String(), s)
				}
				break
			}
			// Else ignoreLevel >= ignoreHigherThanLoadThreshold && overloadedDim !=
			// NumLoadDimensions, so keep going and consider all candidates with
			// cand.sls <= loadThreshold.
		}
		if cand.sls > loadThreshold {
			if s := formatCandidatesLog(&b, cands.candidates[i:]); s != "" {
				log.KvDistribution.VEventf(ctx, 2,
					"discarding candidates with higher load than loadThreshold(%s): %s", loadThreshold.String(), s)
			}
			break
		}
		candDiscardedByNLS := cand.nls > loadThreshold ||
			(cand.nls == loadThreshold && ignoreLevel < ignoreHigherThanLoadThreshold)
		candDiscardedByOverloadDim := overloadedDim != NumLoadDimensions &&
			cand.dimSummary[overloadedDim] >= loadNoChange
		candDiscardedByPendingThreshold := cand.maxFractionPendingIncrease >= maxFractionPendingThreshold
		if candDiscardedByNLS || candDiscardedByOverloadDim || candDiscardedByPendingThreshold {
			// Discard this candidate.
			if cand.maxFractionPendingIncrease > epsilon && discardedCandsHadNoPendingChanges {
				discardedCandsHadNoPendingChanges = false
			}
			log.KvDistribution.VEventf(ctx, 2,
				"candidate store %v was discarded due to (nls=%t overloadDim=%t pending_thresh=%t): sls=%v", cand.StoreID,
				candDiscardedByNLS, candDiscardedByOverloadDim, candDiscardedByPendingThreshold, cand.storeLoadSummary)
			continue
		}
		cands.candidates[j] = cand
		j++
	}
	if j == 0 {
		log.KvDistribution.VEventf(ctx, 2, "sortTargetCandidateSetAndPick: no candidates due to load")
		failLogger(noCandidateDueToLoad)
		return 0
	}
	lowestLoadSet = cands.candidates[0].sls
	highestLoadSet := cands.candidates[j-1].sls
	cands.candidates = cands.candidates[:j]
	// The set of candidates we will consider all have load <= loadThreshold.
	// They may all be lowestLoad, or we may have allowed additional candidates
	// because of ignoreHigherThanLoadThreshold and a specified overloadedDim.
	// When the overloadedDim is specified, all these candidates will be <
	// loadNoChange in that dimension.
	//
	// If this set has some members that are load >= loadNoChange, we have a set
	// that we would not ordinarily consider as candidates. But we are willing
	// to shed to from overloadUrgent => {overloadSlow, loadNoChange} or
	// overloadSlow => loadNoChange, when absolutely necessary. This necessity
	// is defined by the fact that we didn't have any candidate in an earlier or
	// this set that was ignored because of pending changes. Because if a
	// candidate was ignored because of pending work, we want to wait for that
	// pending work to finish and then see if we can transfer to those. Note
	// that we used the condition cand.maxFractionPendingIncrease>epsilon and
	// not cand.maxFractionPendingIncrease>=maxFractionPendingThreshold when
	// setting discardedCandsHadNoPendingChanges. This is an additional
	// conservative choice, since pending added work is slightly inflated in
	// size, and we want to have a true picture of all of these potential
	// candidates before we start using the ones with load >= loadNoChange.
	if lowestLoadSet > loadThreshold {
		panic("candidates should not have lowestLoad > loadThreshold")
	}
	// INVARIANT: lowestLoad <= loadThreshold.
	if lowestLoadSet == loadThreshold && ignoreLevel < ignoreHigherThanLoadThreshold {
		log.KvDistribution.VEventf(ctx, 2, "sortTargetCandidateSetAndPick: no candidates due to equal to loadThreshold")
		failLogger(noCandidateDueToLoad)
		return 0
	}
	// INVARIANT: lowestLoad < loadThreshold ||
	// (lowestLoad <= loadThreshold && ignoreLevel >= ignoreHigherThanLoadThreshold).

	// < loadNoChange is fine. We need to check whether the following cases can continue.
	// [loadNoChange, loadThreshold), or loadThreshold && ignoreHigherThanLoadThreshold.
	if lowestLoadSet >= loadNoChange &&
		(!discardedCandsHadNoPendingChanges || ignoreLevel == ignoreLoadNoChangeAndHigher) {
		log.KvDistribution.VEventf(ctx, 2, "sortTargetCandidateSetAndPick: no candidates due to loadNoChange")
		failLogger(noCandidateDueToLoad)
		return 0
	}
	if lowestLoadSet != highestLoadSet {
		slices.SortFunc(cands.candidates, func(a, b candidateInfo) int {
			return cmp.Or(
				cmp.Compare(a.leasePreferenceIndex, b.leasePreferenceIndex),
				cmp.Compare(a.StoreID, b.StoreID))
		})
	}
	// Candidates are sorted by non-decreasing leasePreferenceIndex. Eliminate
	// ones that have notMatchedLeasePreferenceIndex.
	j = 0
	for _, cand := range cands.candidates {
		if cand.leasePreferenceIndex == notMatchedLeasePreferenceIndex {
			break
		}
		j++
	}
	if j == 0 {
		log.KvDistribution.VEventf(ctx, 2, "sortTargetCandidateSetAndPick: no candidates due to lease preference")
		failLogger(noCandidateDueToUnmatchedLeasePreference)
		return 0
	}
	cands.candidates = cands.candidates[:j]
	if lowestLoadSet != highestLoadSet || (lowestLoadSet >= loadNoChange && overloadedDim != NumLoadDimensions) {
		// Sort candidates from lowest to highest along overloaded dimension. We
		// limit when we do this, since this will further restrict the pool of
		// candidates and in general we don't want to restrict the pool.
		slices.SortFunc(cands.candidates, func(a, b candidateInfo) int {
			return cmp.Compare(a.dimSummary[overloadedDim], b.dimSummary[overloadedDim])
		})
		lowestOverloadedLoad := cands.candidates[0].dimSummary[overloadedDim]
		if lowestOverloadedLoad >= loadNoChange {
			log.KvDistribution.VEventf(ctx, 2, "sortTargetCandidateSetAndPick: no candidates due to overloadedDim")
			failLogger(noCandidateDueToLoad)
			return 0
		}
		j = 1
		for j < len(cands.candidates) {
			if cands.candidates[j].dimSummary[overloadedDim] > lowestOverloadedLoad {
				break
			}
			j++
		}
		if s := formatCandidatesLog(&b, cands.candidates[j:]); s != "" {
			log.KvDistribution.VEventf(ctx, 2, "discarding candidates due to overloadedDim: %s", s)
		}
		cands.candidates = cands.candidates[:j]
	}
	s := formatCandidatesLog(&b, cands.candidates)
	j = rng.Intn(j)
	log.KvDistribution.VEventf(ctx, 2, "sortTargetCandidateSetAndPick: candidates:%s, picked s%v", s, cands.candidates[j].StoreID)
	if ignoreLevel == ignoreLoadNoChangeAndHigher && cands.candidates[j].sls >= loadNoChange ||
		ignoreLevel == ignoreLoadThresholdAndHigher && cands.candidates[j].sls >= loadThreshold ||
		ignoreLevel == ignoreHigherThanLoadThreshold && cands.candidates[j].sls > loadThreshold {
		panic(errors.AssertionFailedf("saw higher load %v candidate than expected, ignoreLevel=%v",
			cands.candidates[j].sls, ignoreLevel))
	}
	return cands.candidates[j].StoreID
}

// ensureAnalyzedConstraints ensures that the constraints field of rangeState is
// populated. It uses rangeState.{replicas,conf} as inputs to the computation.
//
// rstate.conf may be nil if basic span config normalization failed (e.g.,
// invalid config). In this case, constraint analysis is skipped and
// rstate.constraints remains nil. In addition, rstate.constraints may also
// remain nil after the call if there is no leaseholder found. In such cases,
// the range should be skipped from rebalancing.
//
// NB: Caller is responsible for calling clearAnalyzedConstraints when rstate or
// the rstate.constraints is no longer needed.
func (cs *clusterState) ensureAnalyzedConstraints(rstate *rangeState) {
	if rstate.constraints != nil {
		return
	}
	// Skip the range if the configuration is invalid for constraint analysis.
	if rstate.conf == nil {
		log.KvDistribution.Warning(context.Background(),
			"no span config due to normalization error, skipping constraint analysis")
		return
	}
	// Populate the constraints.
	rac := newRangeAnalyzedConstraints()
	buf := rac.stateForInit()
	leaseholder := roachpb.StoreID(-1)
	for _, replica := range rstate.replicas {
		buf.tryAddingStore(replica.StoreID, replica.ReplicaIDAndType.ReplicaType.ReplicaType,
			cs.stores[replica.StoreID].localityTiers)
		if replica.IsLeaseholder {
			leaseholder = replica.StoreID
		}
	}
	if leaseholder < 0 {
		// Very dubious why the leaseholder (which must be a local store since there
		// are no pending changes) is not known.
		releaseRangeAnalyzedConstraints(rac)
		// Possible that we are observing stale state where we've transferred the
		// lease away but have not yet received a StoreLeaseholderMsg indicating
		// that there is a new leaseholder (and thus should drop this range).
		// However, even in this case, replica.IsLeaseholder should still be there
		// based on to the stale state, so this should still be impossible to hit.
		log.KvDistribution.Warningf(context.Background(),
			"mma: no leaseholders found in %v, skipping constraint analysis", rstate.replicas)
		return
	}
	if err := rac.finishInit(rstate.conf, cs.constraintMatcher, leaseholder); err != nil {
		releaseRangeAnalyzedConstraints(rac)
		log.KvDistribution.Warningf(context.Background(),
			"mma: error finishing constraint analysis: %v, skipping range", err)
		return
	}
	rstate.constraints = rac
}

// Consider the core logic for a change, rebalancing or recovery.
//
// - There is a constraint expression for the target: constraintDisj
//
// - There are stores to exclude, since may already be playing roles for that
//   range (or have another replica on the same node -- so not all of these
//   stores to exclude will satisfy the constraint expression). For something
//   like candidatesVoterConstraintsUnsatisfied(), the toRemoveVoters should
//   be in the nodes to exclude.
//
// - Optional store that is trying to shed load, since it is possible that
//   when we consider the context of this constraint expression, the store
//   won't be considered overloaded. This node is not specified if shedding
//   because of failure.
//
// Given these, we need to compute:
//
// - Mean load for constraint expression. We can remember this for the whole
//   rebalancing pass since many ranges will have the same constraint
//   expression. Done via meansMemo.
//
// - Set of nodes that satisfy constraint expression. Again, we can remember this.
//   Done via meansMemo.
//
// - loadSummary for all nodes that satisfy the constraint expression. This
//   may change during rebalancing when changes have been made to node. But
//   can be cached and invalidated. Done via meansMemo.
//
// - Need diversity change for each candidate.
//
// The first 3 bullets are encapsulated in the helper function
// computeCandidatesForReplicaTransfer. It works for both replica additions and
// rebalancing.
//
// For the last bullet (diversity), the caller of computeCandidatesForReplicaTransfer
// needs to populate candidateInfo.diversityScore for each candidate in
// candidateSet. It does so via diversityScoringMemo. Then the (loadSummary,
// diversityScore) pair can be used to order candidates for attempts to add.
//
// TODO(sumeer): caching of information should mostly make the allocator pass
// efficient, but we have one unaddressed problem: When moving a range we are
// having to do O(num-stores) amount of work to iterate over the cached
// information to populate candidateSet. Same to populate the diversity
// information for each candidate (which is also cached and looked up in a
// map). Then this candidateSet needs to be sorted which is O(num-stores*log
// num-stores). Allocator-like components that scale to large clusters try to
// have a constant cost per range that is being examined for rebalancing. If
// cpu profiles of microbenchmarks indicate we have a problem here we could
// try the following optimization:
//
// Assume most ranges in the cluster have exactly the same set of localities
// for their replicas, and the same set of constraints. We have computed the
// candidateSet fully for range R1 that is our current range being examined
// for rebalancing. And say we picked a candidate N1 and changed its adjusted
// load. We can recalculate N1's loadSummary and reposition (or remove) N1
// within this sorted set -- this can be done with cost O(log num-stores) (and
// typically we won't need to reposition). If the next range considered for
// rebalancing, R2, has the same set of replica localities and constraints, we
// can start with this existing computed set.

// loadSheddingStore is only specified if this candidate computation is
// happening because of overload.
//
// postMeansExclusions are filtered post-means: their load is included in the
// mean (they're viable locations in principle) but they're not candidates for
// this specific transfer (the classic case: already have a replica).
func (cs *clusterState) computeCandidatesForReplicaTransfer(
	ctx context.Context,
	conj constraintsConj,
	existingReplicas storeSet,
	postMeansExclusions storeSet,
	loadSheddingStore roachpb.StoreID,
	passObs *rebalancingPassMetricsAndLogger,
) (_ candidateSet, sheddingSLS storeLoadSummary) {
	// Start with computing the stores (and corresponding means) that satisfy
	// the constraint expression. If we don't see a need to filter out any of
	// these stores before computing the means, we can use it verbatim, otherwise
	// we will recompute the means again below.
	cs.scratchDisj[0] = conj
	means := cs.meansMemo.getMeans(cs.scratchDisj[:1])

	// Pre-means filtering: copy to scratch, then filter in place.
	// Filter out stores that have a non-OK replica disposition.
	cs.scratchStoreSet = append(cs.scratchStoreSet[:0], means.stores...)
	filteredStores := retainReadyReplicaTargetStoresOnly(ctx, cs.scratchStoreSet, cs.stores, existingReplicas)

	// Determine which means to use.
	//
	// TODO(tbg): unit testing.
	var effectiveMeans *meansLoad
	if len(filteredStores) == len(means.stores) {
		// Common case: nothing was filtered, use cached means.
		effectiveMeans = &means.meansLoad
	} else if len(filteredStores) == 0 {
		// No viable candidates at all.
		return candidateSet{}, sheddingSLS
	} else {
		// Some stores were filtered; recompute means over filtered set.
		cs.scratchMeans = computeMeansForStoreSet(
			cs, filteredStores, cs.meansMemo.scratchNodes, cs.meansMemo.scratchStores)
		effectiveMeans = &cs.scratchMeans
		log.KvDistribution.VEventf(ctx, 2,
			"pre-means filtered %d stores → remaining %v, means: store=%v node=%v",
			len(means.stores)-len(filteredStores), filteredStores,
			effectiveMeans.storeLoad, effectiveMeans.nodeLoad)
	}

	sheddingSLS = cs.computeLoadSummary(ctx, loadSheddingStore, &effectiveMeans.storeLoad, &effectiveMeans.nodeLoad)
	if sheddingSLS.sls <= loadNoChange && sheddingSLS.nls <= loadNoChange {
		// In this set of stores, this store no longer looks overloaded.
		passObs.replicaShed(notOverloaded)
		return candidateSet{}, sheddingSLS
	}

	var cset candidateSet
	for _, storeID := range filteredStores {
		if postMeansExclusions.contains(storeID) {
			// This store's load is included in the mean, but it's not a viable
			// target for this specific transfer (e.g. it already has a replica).
			continue
		}
		csls := cs.computeLoadSummary(ctx, storeID, &effectiveMeans.storeLoad, &effectiveMeans.nodeLoad)
		cset.candidates = append(cset.candidates, candidateInfo{
			StoreID:          storeID,
			storeLoadSummary: csls,
		})
	}
	cset.means = effectiveMeans
	return cset, sheddingSLS
}

// retainReadyReplicaTargetStoresOnly filters the input set to only those stores
// that are ready to accept a replica. A store is not ready if it has a non-OK
// replica disposition. In practice, the input set is already filtered by
// constraints.
//
// Stores already housing a replica (on top of being in the input storeSet)
// bypass this disposition check since they already have the replica - its load
// should be in the mean regardless of its disposition, as we'll pick candidates
// based on improving clustering around the mean.
//
// The input storeSet is mutated (and returned as the result).
func retainReadyReplicaTargetStoresOnly(
	ctx context.Context,
	in storeSet,
	stores map[roachpb.StoreID]*storeState,
	existingReplicas storeSet,
) storeSet {
	out := in[:0]
	for _, storeID := range in {
		if existingReplicas.contains(storeID) {
			// Stores on existing replicas already have the load and we want to
			// include them in the mean, even if they are not accepting new replicas
			// or even try to shed.
			//
			// TODO(tbg): health might play into this, though. For example, when
			// a store is dead, whatever load we have from it is stale and we
			// are better off not including it. For now, we ignore this problem
			// because the mma only handles rebalancing, whereas a replica on a
			// dead store would be removed by the single-metric allocator after
			// the TimeUntilStoreDead and so would disappear from our view.
			out = append(out, storeID)
			continue
		}
		ss := stores[storeID]
		switch {
		case ss.status.Disposition.Replica != ReplicaDispositionOK:
			log.KvDistribution.VEventf(ctx, 2, "skipping s%d for replica transfer: replica disposition %v (health %v)", storeID, ss.status.Disposition.Replica, ss.status.Health)
		case highDiskSpaceUtilization(ss.reportedLoad[ByteSize], ss.capacity[ByteSize]):
			// TODO(tbg): remove this from mma and just let the caller set this
			// disposition based on the following cluster settings:
			// - kv.allocator.max_disk_utilization_threshold
			// - kv.allocator.rebalance_to_max_disk_utilization_threshold
			log.KvDistribution.VEventf(ctx, 2, "skipping s%d for replica transfer: high disk utilization (health %v)", storeID, ss.status.Health)
		default:
			out = append(out, storeID)
		}
	}
	return out
}

// Diversity scoring is very amenable to caching, since the set of unique
// locality tiers for range replicas is likely to be small. And the cache does
// not need to be cleared after every allocator pass. This caching is done via
// diversityScoringMemo which contains existingReplicaLocalities.

// existingReplicaLocalities is the cache state for a set of replicas with
// locality tiers corresponding to replicasLocalityTiers (which serves as the
// key in diversityScoringMemo). NB: For a range, scoring for non-voters uses
// the set of all replicas, and for voters uses the set of existing voters.
//
// Cases:
type existingReplicaLocalities struct {
	replicasLocalityTiers

	// For a prospective candidate with localityTiers represented by the key,
	// this caches the sum of calls to replicas[i].diversityScore(candidate).
	//
	// If this is an addition, this value can be used directly if the candidate
	// is not already in replicas, else the diversity change is 0.
	//
	// If this is a removal, this is the reduction in diversity. Note that this
	// calculation includes diversityScore with self, but that value is 0.
	scoreSums map[string]float64
}

func (erl *existingReplicaLocalities) clear() {
	erl.replicas = erl.replicas[:0]
	for k := range erl.scoreSums {
		delete(erl.scoreSums, k)
	}
}

var _ mapEntry = &existingReplicaLocalities{}

// replicasLocalityTiers represents the set of localityTiers corresponding to
// a set of replicas (one localityTiers per replica).
type replicasLocalityTiers struct {
	// replicas are sorted in ascending order of localityTiers.str (this is
	// a set, so the order doesn't semantically matter).
	replicas []localityTiers
}

var _ mapKey = replicasLocalityTiers{}

func makeReplicasLocalityTiers(replicas []localityTiers) replicasLocalityTiers {
	slices.SortFunc(replicas, func(a, b localityTiers) int {
		return cmp.Compare(a.str, b.str)
	})
	return replicasLocalityTiers{replicas: replicas}
}

// hash implements the mapKey interface, using the FNV-1a hash algorithm.
func (rlt replicasLocalityTiers) hash() uint64 {
	h := uint64(offset64)
	for i := range rlt.replicas {
		for _, code := range rlt.replicas[i].tiers {
			h ^= uint64(code)
			h *= prime64
		}
		// Separator between different replicas. This is equivalent to
		// encountering the 0 code, which is the empty string. We don't expect to
		// see an empty string in a locality.
		h *= prime64
	}
	return h
}

// isEqual implements the mapKey interface.
func (rlt replicasLocalityTiers) isEqual(b mapKey) bool {
	other := b.(replicasLocalityTiers)
	if len(rlt.replicas) != len(other.replicas) {
		return false
	}
	for i := range rlt.replicas {
		if len(rlt.replicas[i].tiers) != len(other.replicas[i].tiers) {
			return false
		}
		for j := range rlt.replicas[i].tiers {
			if rlt.replicas[i].tiers[j] != other.replicas[i].tiers[j] {
				return false
			}
		}
	}
	return true
}

func (rlt replicasLocalityTiers) clone() replicasLocalityTiers {
	var r replicasLocalityTiers
	r.replicas = append(r.replicas, rlt.replicas...)
	return r
}

var existingReplicaLocalitiesSlicePool = sync.Pool{
	New: func() interface{} {
		return &mapEntrySlice[*existingReplicaLocalities]{}
	},
}

type existingReplicaLocalitiesSlicePoolImpl struct{}

func (p existingReplicaLocalitiesSlicePoolImpl) newEntry() *mapEntrySlice[*existingReplicaLocalities] {
	return existingReplicaLocalitiesSlicePool.New().(*mapEntrySlice[*existingReplicaLocalities])
}

func (p existingReplicaLocalitiesSlicePoolImpl) releaseEntry(
	slice *mapEntrySlice[*existingReplicaLocalities],
) {
	existingReplicaLocalitiesSlicePool.Put(slice)
}

type existingReplicaLocalitiesAllocator struct{}

func (a existingReplicaLocalitiesAllocator) ensureNonNilMapEntry(
	entry *existingReplicaLocalities,
) *existingReplicaLocalities {
	if entry == nil {
		return &existingReplicaLocalities{}
	}
	return entry
}

// diversityScoringMemo provides support for efficiently scoring the diversity
// for a set of replicas, and for adding/removing replicas.
type diversityScoringMemo struct {
	replicasMap *clearableMemoMap[replicasLocalityTiers, *existingReplicaLocalities]
}

func newDiversityScoringMemo() *diversityScoringMemo {
	return &diversityScoringMemo{
		replicasMap: newClearableMapMemo[replicasLocalityTiers, *existingReplicaLocalities](
			existingReplicaLocalitiesAllocator{}, existingReplicaLocalitiesSlicePoolImpl{}),
	}
}

// getExistingReplicaLocalities returns the existingReplicaLocalities object
// for the given replicas. The parameter can be mutated by the callee.
func (dsm *diversityScoringMemo) getExistingReplicaLocalities(
	existingReplicas replicasLocalityTiers,
) *existingReplicaLocalities {
	erl, ok := dsm.replicasMap.get(existingReplicas)
	if ok {
		return erl
	}
	erl.replicasLocalityTiers = existingReplicas.clone()
	erl.scoreSums = map[string]float64{}
	return erl
}

// If this is a NON_VOTER being added, then existingReplicaLocalities must
// represent both VOTER and NON_VOTER replicas, and replicaToAdd must not be
// an existing VOTER. Note that for the case where replicaToAdd is an existing
// VOTER, there is no score change, so this method should not be called.
//
// If this is a VOTER being added, then existingReplicaLocalities must
// represent only VOTER replicas.
func (erl *existingReplicaLocalities) getScoreChangeForNewReplica(
	replicaToAdd localityTiers,
) float64 {
	return erl.getScoreSum(replicaToAdd)
}

// If this is a NON_VOTER being removed, then existingReplicaLocalities must
// represent both VOTER and NON_VOTER replicas.
//
// If this is a VOTER being removed, then existingReplicaLocalities must
// represent only VOTER replicas.
func (erl *existingReplicaLocalities) getScoreChangeForReplicaRemoval(
	replicaToRemove localityTiers,
) float64 {
	// NB: we don't need to compensate for counting the pair-wise score of
	// replicaToRemove with itself, because that score is 0.
	return -erl.getScoreSum(replicaToRemove)
}

// If this is a VOTER being added and removed, replicaToRemove is of course a
// VOTER, and it doesn't matter if the one being added is already a NON_VOTER
// (since the existingReplicaLocalities represents only VOTERs).
//
// If this a NON_VOTER being added and removed, existingReplicaLocalities represents
// all VOTERs and NON_VOTERs. The replicaToAdd must not be an
// existing VOTER. For the case where replicaToAdd is an existing VOTER, there
// is no score change for the addition (to the full set of replicas), and:
//
//   - If replicaToRemove is not becoming a VOTER,
//     getScoreChangeForReplicaRemoval(replicaToRemove) should be called.
//
//   - If replicaToRemove is becoming a VOTER, the score change is 0 for the
//     full set of replicas.
//
// In the preceding cases where the VOTER set is also changing, of course the
// score change to the existingReplicaLocalities that represents only the
// VOTERs must also be considered.
func (erl *existingReplicaLocalities) getScoreChangeForRebalance(
	replicaToRemove localityTiers, replicaToAdd localityTiers,
) float64 {
	score := erl.getScoreSum(replicaToAdd) - erl.getScoreSum(replicaToRemove)
	// We have included the pair-wise diversity of (replicaToRemove,
	// replicaToAdd) above, but since replicaToRemove is being removed, we need
	// to subtract that pair.
	score -= replicaToRemove.diversityScore(replicaToAdd)
	return score
}

func (erl *existingReplicaLocalities) getScoreSum(replica localityTiers) float64 {
	score, ok := erl.scoreSums[replica.str]
	if !ok {
		for i := range erl.replicas {
			score += erl.replicas[i].diversityScore(replica)
		}
		erl.scoreSums[replica.str] = score
	}
	return score
}

func diversityScoresAlmostEqual(score1, score2 float64) bool {
	return math.Abs(score1-score2) < epsilon
}

// This is a somehow arbitrary chosen upper bound on the relative error to be
// used when comparing doubles for equality. The assumption that comes with it
// is that any sequence of operations on doubles won't produce a result with
// accuracy that is worse than this relative error. There is no guarantee
// however that this will be the case. A programmer writing code using
// floating point numbers will still need to be aware of the effect of the
// operations on the results and the possible loss of accuracy.
// More info https://en.wikipedia.org/wiki/Machine_epsilon
// https://en.wikipedia.org/wiki/Floating-point_arithmetic
const epsilon = 1e-10

// Avoid unused lint errors.

var _ = (&existingReplicaLocalities{}).clear
var _ = replicasLocalityTiers{}.hash
var _ = replicasLocalityTiers{}.isEqual
var _ = existingReplicaLocalitiesSlicePoolImpl{}.newEntry
var _ = existingReplicaLocalitiesSlicePoolImpl{}.releaseEntry
var _ = existingReplicaLocalitiesAllocator{}.ensureNonNilMapEntry
var _ = (&diversityScoringMemo{}).getExistingReplicaLocalities
var _ = (&existingReplicaLocalities{}).getScoreChangeForNewReplica
var _ = (&existingReplicaLocalities{}).getScoreChangeForReplicaRemoval
var _ = (&existingReplicaLocalities{}).getScoreChangeForRebalance
var _ = candidateInfo{}.diversityScore
