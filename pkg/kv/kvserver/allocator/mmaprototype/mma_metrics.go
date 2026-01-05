// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mmaprototype

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

type ChangeOrigin uint8

const (
	OriginExternal ChangeOrigin = iota
	originMMARebalance
)

// rangeOperationMetrics contains per-store metrics for range operation outcomes
// (replica/lease changes from external and rebalance sources) and span config
// normalization issues.
type rangeOperationMetrics struct {
	DroppedDueToStateInconsistency *metric.Counter

	ExternalRegisterSuccess *metric.Counter
	ExternalRegisterFailure *metric.Counter

	ExternalReplicaChangeSuccess *metric.Counter
	ExternalReplicaChangeFailure *metric.Counter
	ExternalLeaseChangeSuccess   *metric.Counter
	ExternalLeaseChangeFailure   *metric.Counter

	RebalanceReplicaChangeSuccess *metric.Counter
	RebalanceReplicaChangeFailure *metric.Counter
	RebalanceLeaseChangeSuccess   *metric.Counter
	RebalanceLeaseChangeFailure   *metric.Counter

	// Both metrics below counts ranges where the local store is leaseholder and
	// span config normalization encountered errors. Both metrics persist until a
	// new config arrives. This does not include cases where constraint analysis
	// fails. Both are unexpected, and operators should fix the zone config if
	// these metrics are non-zero.
	//
	// SpanConfigNormalizationError counts all normalization errors including
	// SpanConfigNormalizationSoftError.
	//  These ranges have nil
	// conf and are excluded from rebalancing.
	// SpanConfigNormalizationError counts ALL normalization errors.
	// This is a superset that includes both:
	//   - Hard errors: basic normalization failed (e.g.invalid user-provided
	//   config or a bug in the code). Results in nil conf; range is excluded from
	//   rebalancing. Note that range still remains in clusterState.ranges and may
	//   appear in topKReplicas, but will be skipped if selected as a rebalancing
	//   candidate.
	//   - Soft errors: doStructuralNormalization failed but produced a
	//   best-effort usable config. Range remains a valid rebalancing candidate.
	//   Same as SpanConfigNormalizationSoftError.
	SpanConfigNormalizationError *metric.Gauge

	// SpanConfigNormalizationSoftError counts ranges where structural
	// normalization failed but produced a best-effort usable config. These ranges
	// remain as valid rebalancing candidates. Counts only soft errors where
	// normalization produced a usable config despite errors (e.g., voter
	// constraints not fully satisfiable by replica constraints). These ranges
	// remain valid rebalancing candidates.
	// INVARIANT: SpanConfigNormalizationSoftError <=
	// SpanConfigNormalizationError.
	SpanConfigNormalizationSoftError *metric.Gauge
}

func makeRangeOperationMetrics() *rangeOperationMetrics {
	return &rangeOperationMetrics{
		DroppedDueToStateInconsistency:   metric.NewCounter(metaDroppedDueToStateInconsistency),
		ExternalRegisterSuccess:          metric.NewCounter(metaExternalRegisterSuccess),
		ExternalRegisterFailure:          metric.NewCounter(metaExternalRegisterFailure),
		ExternalReplicaChangeSuccess:     metric.NewCounter(metaExternalReplicaChangeSuccess),
		ExternalReplicaChangeFailure:     metric.NewCounter(metaExternalReplicaChangeFailure),
		ExternalLeaseChangeSuccess:       metric.NewCounter(metaExternalLeaseChangeSuccess),
		ExternalLeaseChangeFailure:       metric.NewCounter(metaExternalLeaseChangeFailure),
		RebalanceReplicaChangeSuccess:    metric.NewCounter(metaRebalanceReplicaChangeSuccess),
		RebalanceReplicaChangeFailure:    metric.NewCounter(metaRebalanceReplicaChangeFailure),
		RebalanceLeaseChangeSuccess:      metric.NewCounter(metaRebalanceLeaseChangeSuccess),
		RebalanceLeaseChangeFailure:      metric.NewCounter(metaRebalanceLeaseChangeFailure),
		SpanConfigNormalizationError:     metric.NewGauge(metaSpanConfigNormalizationError),
		SpanConfigNormalizationSoftError: metric.NewGauge(metaSpanConfigNormalizationSoftError),
	}
}

var (
	metaDroppedDueToStateInconsistency = metric.Metadata{
		Name:        "mma.dropped",
		Help:        "Number of operations dropped due to MMA state inconsistency",
		Measurement: "Replica/Lease Change",
		Unit:        metric.Unit_COUNT,
	}
	metaExternalRegisterSuccess = metric.Metadata{
		Name:        "mma.external.registration.success",
		Help:        "Number of external operations successfully registered with MMA",
		Measurement: "Replica/Lease Change",
		Unit:        metric.Unit_COUNT,
	}
	metaExternalRegisterFailure = metric.Metadata{
		Name:        "mma.external.registration.failure",
		Help:        "Number of external operations that failed to register with MMA",
		Measurement: "Replica/Lease Change",
		Unit:        metric.Unit_COUNT,
	}
	metaExternalReplicaChangeSuccess = metric.Metadata{
		Name:        "mma.change.external.replica.success",
		Help:        "Number of successful external replica change operations",
		Measurement: "Range Change",
		Unit:        metric.Unit_COUNT,
		LabeledName: "mma.change",
		StaticLabels: metric.MakeLabelPairs(
			metric.LabelOrigin, "external", metric.LabelType, "replica", metric.LabelResult, "success"),
	}
	metaExternalReplicaChangeFailure = metric.Metadata{
		Name:        "mma.change.external.replica.failure",
		Help:        "Number of failed external replica change operations",
		Measurement: "Range Change",
		Unit:        metric.Unit_COUNT,
		LabeledName: "mma.change",
		StaticLabels: metric.MakeLabelPairs(
			metric.LabelOrigin, "external", metric.LabelType, "replica", metric.LabelResult, "failure"),
	}
	metaExternalLeaseChangeSuccess = metric.Metadata{
		Name:        "mma.change.external.lease.success",
		Help:        "Number of successful external lease change operations",
		Measurement: "Lease Change",
		Unit:        metric.Unit_COUNT,
		LabeledName: "mma.change",
		StaticLabels: metric.MakeLabelPairs(
			metric.LabelOrigin, "external", metric.LabelType, "lease", metric.LabelResult, "success"),
	}
	metaExternalLeaseChangeFailure = metric.Metadata{
		Name:        "mma.change.external.lease.failure",
		Help:        "Number of failed external lease change operations",
		Measurement: "Lease Change",
		Unit:        metric.Unit_COUNT,
		LabeledName: "mma.change",
		StaticLabels: metric.MakeLabelPairs(
			metric.LabelOrigin, "external", metric.LabelType, "lease", metric.LabelResult, "failure"),
	}
	metaRebalanceReplicaChangeSuccess = metric.Metadata{
		Name:        "mma.change.rebalance.replica.success",
		Help:        "Number of successful MMA-initiated rebalance operations that change replicas",
		Measurement: "Range Change",
		Unit:        metric.Unit_COUNT,
		LabeledName: "mma.change",
		StaticLabels: metric.MakeLabelPairs(
			metric.LabelOrigin, "rebalance", metric.LabelType, "replica", metric.LabelResult, "success"),
	}
	metaRebalanceReplicaChangeFailure = metric.Metadata{
		Name:        "mma.change.rebalance.replica.failure",
		Help:        "Number of failed MMA-initiated rebalance operations that change replicas",
		Measurement: "Range Change",
		Unit:        metric.Unit_COUNT,
		LabeledName: "mma.change",
		StaticLabels: metric.MakeLabelPairs(
			metric.LabelOrigin, "rebalance", metric.LabelType, "replica", metric.LabelResult, "failure"),
	}
	metaRebalanceLeaseChangeSuccess = metric.Metadata{
		Name:        "mma.change.rebalance.lease.success",
		Help:        "Number of successful MMA-initiated rebalance operations that transfer the lease",
		Measurement: "Lease Change",
		Unit:        metric.Unit_COUNT,
		LabeledName: "mma.change",
		StaticLabels: metric.MakeLabelPairs(
			metric.LabelOrigin, "rebalance", metric.LabelType, "lease", metric.LabelResult, "success"),
	}
	metaRebalanceLeaseChangeFailure = metric.Metadata{
		Name:        "mma.change.rebalance.lease.failure",
		Help:        "Number of failed MMA-initiated rebalance operations that transfer the lease",
		Measurement: "Lease Change",
		Unit:        metric.Unit_COUNT,
		LabeledName: "mma.change",
		StaticLabels: metric.MakeLabelPairs(
			metric.LabelOrigin, "rebalance", metric.LabelType, "lease", metric.LabelResult, "failure"),
	}
	metaSpanConfigNormalizationError = metric.Metadata{
		Name: "mma.span_config.normalization.error",
		Help: "Number of ranges where the local store is leaseholder and span config " +
			"normalization encountered errors. Includes both hard errors (nil conf, " +
			"excluded from rebalancing) and soft errors (usable config, still rebalanced). " +
			"Operators should review zone config if this metric is non-zero.",
		Measurement: "Range",
		Unit:        metric.Unit_COUNT,
		LabeledName: "mma.span_config.normalization",
		StaticLabels: metric.MakeLabelPairs(
			metric.LabelResult, "error"),
	}
	metaSpanConfigNormalizationSoftError = metric.Metadata{
		Name: "mma.span_config.normalization.soft_error",
		Help: "Number of ranges where the local store is leaseholder and structural " +
			"span config normalization failed, but produced a best-effort usable config. " +
			"Operators should fix the zone config if this metric is non-zero.",
		Measurement: "Range",
		Unit:        metric.Unit_COUNT,
		LabeledName: "mma.span_config.normalization",
		StaticLabels: metric.MakeLabelPairs(
			metric.LabelResult, "soft_error"),
	}

	// Future: we will add additional origins for MMA-initiated operations that
	// are other than rebalance. Eventually, the external label value will go
	// away when everything is migrated to MMA and SMA is removed.
)

// overloadKind represents the various time-based overload states for a store.
type overloadKind uint8

const (
	// overloadedWaitingForLeaseShedding represents when the remote store is
	// still in the grace period for shedding its own leases.
	overloadedWaitingForLeaseShedding overloadKind = iota
	// overloadedShortDuration corresponds to ignoreLoadNoChangeAndHigher.
	overloadedShortDuration
	// overloadedMediumDuration corresponds to ignoreLoadThresholdAndHigher.
	overloadedMediumDuration
	// overloadedLongDuration corresponds to ignoreHigherThanLoadThresholdGraceDuration
	overloadedLongDuration
	numOverloadKinds
)

func toOverloadKind(withinLeaseSheddingGracePeriod bool, ignoreLevel ignoreLevel) overloadKind {
	if withinLeaseSheddingGracePeriod {
		return overloadedWaitingForLeaseShedding
	}
	switch ignoreLevel {
	case ignoreLoadNoChangeAndHigher:
		return overloadedShortDuration
	case ignoreLoadThresholdAndHigher:
		return overloadedMediumDuration
	case ignoreHigherThanLoadThreshold:
		return overloadedLongDuration
	default:
		panic(errors.AssertionFailedf("invalid ignoreLevel value: %d", ignoreLevel))
	}
}

// rebalancingPassMetricsAndLogger manages gauge metrics and logging for the
// first call to ComputeChanges from mmaStoreRebalancer. One per local store.
//
// Note: Metrics are registered to the LOCAL store's registry, not the overloaded
// stores'. Each store tracks "from my perspective, what overloaded stores did I
// observe and what were the outcomes?" during its rebalancing pass.
//
// Each rebalancing pass: states and skippedStores are reset at start. For each
// overloaded store, curState/curStoreID track shedding results; when processing
// of that store completes, curState is saved to states (or the store is added
// to skippedStores if skipped). At the end of the pass, states is aggregated to
// update m, and failedSummaries/successSummaries are built for logging.
type rebalancingPassMetricsAndLogger struct {
	// The local store where the rebalancing pass is happening.
	localStoreID roachpb.StoreID
	// Gauge metrics for overloaded store counts by duration/outcome.
	m gaugeMetrics

	// Per-pass state (reset each pass, used to update m and to log).

	// Overloaded store ID -> shedding counts and overload category.
	states map[roachpb.StoreID]storePassState
	// Overloaded stores skipped during the pass (due to max range/lease move
	// limit reached).
	skippedStores []roachpb.StoreID
	// Shedding results for the currently-processing store.
	curState storePassState
	// Store ID that curState belongs to.
	curStoreID roachpb.StoreID
	// Stores with shedding failures. Aggregated into at the end of the pass.
	failedSummaries []storePassSummary
	// Stores with shedding successes. Aggregated into at the end of the pass.
	successSummaries []storePassSummary
}

// shedKind represents either pure-lease shedding, or replica shedding
// (shedding a replica may also result in shedding the lease).
type shedKind uint8

const (
	shedLease shedKind = iota
	shedReplica
	numShedKinds
)

// storePassState contains all the state gathered for a store for
// which shedding was attempted.
type storePassState struct {
	overloadKind overloadKind
	shedCounts   [numShedKinds][numShedResults]int
}

func (s *storePassState) summarize() storePassSummary {
	var sum storePassSummary
	for kindIdx := range s.shedCounts {
		for resultIdx := range s.shedCounts[kindIdx] {
			count := s.shedCounts[kindIdx][resultIdx]
			if count == 0 {
				continue
			}
			result := shedResult(resultIdx)
			if result == shedSuccess {
				sum.numShedSuccesses += count
			} else {
				sum.numShedFailures += count
			}
			if count > sum.mostCommonCount {
				sum.mostCommonReason = result
				sum.mostCommonCount = count
			}
		}
	}
	return sum
}

// storePassSummary aggregates shedding outcomes for a single overloaded
// store.
type storePassSummary struct {
	// Overloaded store ID.
	storeID          roachpb.StoreID
	numShedSuccesses int
	numShedFailures  int
	// mostCommonReason/mostCommonCount track the most frequent shedResult for
	// this overloaded store, used in failure logging.
	mostCommonReason shedResult
	mostCommonCount  int
}

var (
	// NB: mma.overloaded_store.lease_grace.success should always be 0, but we
	// declare the metric to avoid making assumptions about rebalancing
	// behavior.
	metaOverloadedStoreLeaseGraceSuccess = metric.Metadata{
		Name:        "mma.overloaded_store.lease_grace.success",
		Help:        "Number of overloaded stores within lease shedding grace period for which shedding succeeded",
		Measurement: "Stores",
		Unit:        metric.Unit_COUNT,
		LabeledName: "mma.overloaded_store",
		StaticLabels: metric.MakeLabelPairs(
			metric.LabelType, "lease_grace", metric.LabelResult, "success"),
	}
	metaOverloadedStoreLeaseGraceFailure = metric.Metadata{
		Name:        "mma.overloaded_store.lease_grace.failure",
		Help:        "Number of overloaded stores within lease shedding grace period for which shedding failed",
		Measurement: "Stores",
		Unit:        metric.Unit_COUNT,
		LabeledName: "mma.overloaded_store",
		StaticLabels: metric.MakeLabelPairs(
			metric.LabelType, "lease_grace", metric.LabelResult, "failure"),
	}
	metaOverloadedStoreShortDurSuccess = metric.Metadata{
		Name:        "mma.overloaded_store.short_dur.success",
		Help:        "Number of stores overloaded for a short duration for which shedding succeeded",
		Measurement: "Stores",
		Unit:        metric.Unit_COUNT,
		LabeledName: "mma.overloaded_store",
		StaticLabels: metric.MakeLabelPairs(
			metric.LabelType, "short_dur", metric.LabelResult, "success"),
	}
	metaOverloadedStoreShortDurFailure = metric.Metadata{
		Name:        "mma.overloaded_store.short_dur.failure",
		Help:        "Number of stores overloaded for a short duration for which shedding failed",
		Measurement: "Stores",
		Unit:        metric.Unit_COUNT,
		LabeledName: "mma.overloaded_store",
		StaticLabels: metric.MakeLabelPairs(
			metric.LabelType, "short_dur", metric.LabelResult, "failure"),
	}
	metaOverloadedStoreMediumDurSuccess = metric.Metadata{
		Name:        "mma.overloaded_store.medium_dur.success",
		Help:        "Number of stores overloaded for a medium duration for which shedding succeeded",
		Measurement: "Stores",
		Unit:        metric.Unit_COUNT,
		LabeledName: "mma.overloaded_store",
		StaticLabels: metric.MakeLabelPairs(
			metric.LabelType, "medium_dur", metric.LabelResult, "success"),
	}
	metaOverloadedStoreMediumDurFailure = metric.Metadata{
		Name:        "mma.overloaded_store.medium_dur.failure",
		Help:        "Number of stores overloaded for a medium duration for which shedding failed",
		Measurement: "Stores",
		Unit:        metric.Unit_COUNT,
		LabeledName: "mma.overloaded_store",
		StaticLabels: metric.MakeLabelPairs(
			metric.LabelType, "medium_dur", metric.LabelResult, "failure"),
	}
	metaOverloadedStoreLongDurSuccess = metric.Metadata{
		Name:        "mma.overloaded_store.long_dur.success",
		Help:        "Number of stores overloaded for a long duration for which shedding succeeded",
		Measurement: "Stores",
		Unit:        metric.Unit_COUNT,
		LabeledName: "mma.overloaded_store",
		StaticLabels: metric.MakeLabelPairs(
			metric.LabelType, "long_dur", metric.LabelResult, "success"),
	}
	metaOverloadedStoreLongDurFailure = metric.Metadata{
		Name:        "mma.overloaded_store.long_dur.failure",
		Help:        "Number of stores overloaded for a long duration for which shedding failed",
		Measurement: "Stores",
		Unit:        metric.Unit_COUNT,
		LabeledName: "mma.overloaded_store",
		StaticLabels: metric.MakeLabelPairs(
			metric.LabelType, "long_dur", metric.LabelResult, "failure"),
	}
)

type gaugeMetrics struct {
	OverloadedStoreLeaseGraceSuccess *metric.Gauge
	OverloadedStoreLeaseGraceFailure *metric.Gauge
	OverloadedStoreShortDurSuccess   *metric.Gauge
	OverloadedStoreShortDurFailure   *metric.Gauge
	OverloadedStoreMediumDurSuccess  *metric.Gauge
	OverloadedStoreMediumDurFailure  *metric.Gauge
	OverloadedStoreLongDurSuccess    *metric.Gauge
	OverloadedStoreLongDurFailure    *metric.Gauge
}

func (m *gaugeMetrics) init() {
	*m = gaugeMetrics{
		OverloadedStoreLeaseGraceSuccess: metric.NewGauge(metaOverloadedStoreLeaseGraceSuccess),
		OverloadedStoreLeaseGraceFailure: metric.NewGauge(metaOverloadedStoreLeaseGraceFailure),
		OverloadedStoreShortDurSuccess:   metric.NewGauge(metaOverloadedStoreShortDurSuccess),
		OverloadedStoreShortDurFailure:   metric.NewGauge(metaOverloadedStoreShortDurFailure),
		OverloadedStoreMediumDurSuccess:  metric.NewGauge(metaOverloadedStoreMediumDurSuccess),
		OverloadedStoreMediumDurFailure:  metric.NewGauge(metaOverloadedStoreMediumDurFailure),
		OverloadedStoreLongDurSuccess:    metric.NewGauge(metaOverloadedStoreLongDurSuccess),
		OverloadedStoreLongDurFailure:    metric.NewGauge(metaOverloadedStoreLongDurFailure),
	}
}

func makeRebalancingPassMetricsAndLogger(
	localStoreID roachpb.StoreID,
) *rebalancingPassMetricsAndLogger {
	g := &rebalancingPassMetricsAndLogger{
		localStoreID: localStoreID,
		states:       map[roachpb.StoreID]storePassState{},
	}
	g.m.init()
	return g
}

func (g *rebalancingPassMetricsAndLogger) resetForRebalancingPass() {
	if g == nil {
		return
	}
	clear(g.states)
	g.skippedStores = g.skippedStores[:0]
}

func (g *rebalancingPassMetricsAndLogger) storeOverloaded(
	storeID roachpb.StoreID, withinLeaseSheddingGracePeriod bool, ignoreLevel ignoreLevel,
) {
	if g == nil {
		return
	}
	g.curStoreID = storeID
	g.curState = storePassState{
		overloadKind: toOverloadKind(withinLeaseSheddingGracePeriod, ignoreLevel),
	}
}

func (g *rebalancingPassMetricsAndLogger) finishStore() {
	if g == nil {
		return
	}
	g.states[g.curStoreID] = g.curState
}

// shedResult is specified for ranges that were considered for shedding. It
// excludes ranges with some transient behavior that exclude them from being
// considered, like pending changes, or recently failed changes.
type shedResult uint8

// The noCandidate* only represent the reason that the last candidate(s) were eliminated.
// Candidates could have been eliminated earlier for other reasons.
const (
	shedSuccess shedResult = iota
	// notOverloaded represents a store that looks overloaded when considering
	// cluster aggregates, but when considering a range, it is not overloaded
	// compared to the other candidates.
	notOverloaded
	noCandidate
	noHealthyCandidate
	noCandidateDiskSpaceUtil
	noCandidateDueToLoad
	noCandidateDueToUnmatchedLeasePreference
	noCandidateToAcceptLoad
	rangeConstraintsViolated
	numShedResults
)

func (sr shedResult) String() string {
	return redact.StringWithoutMarkers(sr)
}

func (sr shedResult) SafeFormat(w redact.SafePrinter, _ rune) {
	switch sr {
	case shedSuccess:
		w.SafeString("success")
	case notOverloaded:
		w.SafeString("not-overloaded")
	case noCandidate:
		w.SafeString("no-cand")
	case noHealthyCandidate:
		w.SafeString("no-healthy-cand")
	case noCandidateDiskSpaceUtil:
		w.SafeString("no-cand-diskspace")
	case noCandidateDueToLoad:
		w.SafeString("no-cand-load")
	case noCandidateDueToUnmatchedLeasePreference:
		w.SafeString("no-cand-lease-pref")
	case noCandidateToAcceptLoad:
		w.SafeString("no-cand-to-accept-load")
	case rangeConstraintsViolated:
		w.SafeString("constraint-violation")
	default:
		w.SafeString("unknown")
	}
}

// leaseShed is sandwiched between storeOverloaded and finishStore, and
// provides the result of the shedding attempt.
func (g *rebalancingPassMetricsAndLogger) leaseShed(result shedResult) {
	if g == nil {
		return
	}
	g.curState.shedCounts[shedLease][result]++
}

// replicaShed is sandwiched between storeOverloaded and finishStore, and
// provides the result of the shedding attempt.
func (g *rebalancingPassMetricsAndLogger) replicaShed(result shedResult) {
	if g == nil {
		return
	}
	g.curState.shedCounts[shedReplica][result]++
}

func (g *rebalancingPassMetricsAndLogger) skippedStore(storeID roachpb.StoreID) {
	if g == nil {
		return
	}
	g.skippedStores = append(g.skippedStores, storeID)
}

func (g *rebalancingPassMetricsAndLogger) finishRebalancingPass(ctx context.Context) {
	if g == nil {
		return
	}
	g.failedSummaries = g.failedSummaries[:0]
	g.successSummaries = g.successSummaries[:0]

	// Aggregate store counts by (overloadKind, outcome) for gauge metrics.
	// storeNumSummaries[kindIdx][0] = success count, [kindIdx][1] = failure
	// count. Each store is counted at most once based on whether it had any
	// shedding success.
	var storeNumSummaries [numOverloadKinds][2]int64
	for overloadedStoreID, passState := range g.states {
		s := passState.summarize()
		s.storeID = overloadedStoreID
		if s.numShedSuccesses > 0 {
			storeNumSummaries[passState.overloadKind][0]++
			g.successSummaries = append(g.successSummaries, s)
		} else if s.numShedFailures > 0 {
			storeNumSummaries[passState.overloadKind][1]++
			g.failedSummaries = append(g.failedSummaries, s)
		}
		// Else ignore. Some transient situation caused no ranges to be
		// considered.
	}

	// Update gauge metrics using storeNumSummaries aggregated from all overloaded
	// stores.
	for kindIdx := range storeNumSummaries {
		for outcomeIdx := range storeNumSummaries[kindIdx] {
			count := storeNumSummaries[kindIdx][outcomeIdx]
			isSuccess := outcomeIdx == 0
			switch overloadKind(kindIdx) {
			case overloadedWaitingForLeaseShedding:
				if isSuccess {
					g.m.OverloadedStoreLeaseGraceSuccess.Update(count)
				} else {
					g.m.OverloadedStoreLeaseGraceFailure.Update(count)
				}
			case overloadedShortDuration:
				if isSuccess {
					g.m.OverloadedStoreShortDurSuccess.Update(count)
				} else {
					g.m.OverloadedStoreShortDurFailure.Update(count)
				}
			case overloadedMediumDuration:
				if isSuccess {
					g.m.OverloadedStoreMediumDurSuccess.Update(count)
				} else {
					g.m.OverloadedStoreMediumDurFailure.Update(count)
				}
			case overloadedLongDuration:
				if isSuccess {
					g.m.OverloadedStoreLongDurSuccess.Update(count)
				} else {
					g.m.OverloadedStoreLongDurFailure.Update(count)
				}
			}
		}
	}

	// Build log message with up to k entries per category.
	// Example output: "rebalancing pass shed: {s1, s3} failures
	// (store,reason:count): (s2,no-cand:5) skipped: {s4, s5}"
	const k = 20
	var b strings.Builder

	// Log stores where shedding succeeded, sorted by store ID.
	n := len(g.successSummaries)
	if n > 0 {
		slices.SortFunc(g.successSummaries, func(a, b storePassSummary) int {
			return cmp.Compare(a.storeID, b.storeID)
		})
		omitted := false
		if n > k {
			omitted = true
			n = k
		}
		fmt.Fprintf(&b, "rebalancing pass shed: {")
		for i := 0; i < n; i++ {
			prefix := ", "
			if i == 0 {
				prefix = ""
			}
			fmt.Fprintf(&b, "%ss%v", prefix, g.successSummaries[i].storeID)
		}
		if omitted {
			fmt.Fprintf(&b, ",...")
		}
		fmt.Fprintf(&b, "}")
	}

	// Log stores where shedding failed, sorted by failure count (descending).
	n = len(g.failedSummaries)
	if n > 0 {
		slices.SortFunc(g.failedSummaries, func(a, b storePassSummary) int {
			return cmp.Or(cmp.Compare(-a.numShedFailures, -b.numShedFailures),
				cmp.Compare(a.storeID, b.storeID))
		})
		omitted := false
		if n > k {
			omitted = true
			n = k
		}
		if b.Len() == 0 {
			fmt.Fprintf(&b, "rebalancing pass")
		}
		fmt.Fprintf(&b, " failures (store,reason:count): ")
		for i := 0; i < n; i++ {
			prefix := ", "
			if i == 0 {
				prefix = ""
			}
			fmt.Fprintf(&b, "%s(s%v,%v:%d)", prefix, g.failedSummaries[i].storeID,
				g.failedSummaries[i].mostCommonReason, g.failedSummaries[i].mostCommonCount)
		}
		if omitted {
			fmt.Fprintf(&b, ",...")
		}
	}

	// Log stores that were skipped (e.g., max range move limit reached), sorted
	// by store ID.
	n = len(g.skippedStores)
	if n > 0 {
		slices.Sort(g.skippedStores)
		omitted := false
		if n > k {
			omitted = true
			n = k
		}
		fmt.Fprintf(&b, " skipped: {")
		for i := 0; i < n; i++ {
			prefix := ", "
			if i == 0 {
				prefix = ""
			}
			fmt.Fprintf(&b, "%ss%v", prefix, g.skippedStores[i])
		}
		if omitted {
			fmt.Fprintf(&b, ",...")
		}
		fmt.Fprintf(&b, "}")
	}

	if b.Len() > 0 {
		log.KvDistribution.Infof(ctx, "%s", redact.SafeString(b.String()))
	}
}
