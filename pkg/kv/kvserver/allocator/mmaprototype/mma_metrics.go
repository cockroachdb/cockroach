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

type counterMetrics struct {
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
}

func makeCounterMetrics() *counterMetrics {
	return &counterMetrics{
		DroppedDueToStateInconsistency: metric.NewCounter(metaDroppedDueToStateInconsistency),
		ExternalRegisterSuccess:        metric.NewCounter(metaExternalRegisterSuccess),
		ExternalRegisterFailure:        metric.NewCounter(metaExternalRegisterFailure),
		ExternalReplicaChangeSuccess:   metric.NewCounter(metaExternalReplicaChangeSuccess),
		ExternalReplicaChangeFailure:   metric.NewCounter(metaExternalReplicaChangeFailure),
		ExternalLeaseChangeSuccess:     metric.NewCounter(metaExternalLeaseChangeSuccess),
		ExternalLeaseChangeFailure:     metric.NewCounter(metaExternalLeaseChangeFailure),
		RebalanceReplicaChangeSuccess:  metric.NewCounter(metaRebalanceReplicaChangeSuccess),
		RebalanceReplicaChangeFailure:  metric.NewCounter(metaRebalanceReplicaChangeFailure),
		RebalanceLeaseChangeSuccess:    metric.NewCounter(metaRebalanceLeaseChangeSuccess),
		RebalanceLeaseChangeFailure:    metric.NewCounter(metaRebalanceLeaseChangeFailure),
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

// rebalancingPassMetricsAndLogger manages gauge metrics and logging for
// rebalancing passes.
type rebalancingPassMetricsAndLogger struct {
	localStoreID roachpb.StoreID
	m            gaugeMetrics

	// Pass state that is used only during a rebalancing pass to update m, and
	// to log.

	states           map[roachpb.StoreID]storePassState
	skippedStores    []roachpb.StoreID
	curState         storePassState
	curStoreID       roachpb.StoreID
	failedSummaries  []storePassSummary
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
	for i := range s.shedCounts {
		for j := range s.shedCounts[i] {
			if s.shedCounts[i][j] == 0 {
				continue
			}
			if shedResult(j) == shedSuccess {
				sum.numShedSuccesses += s.shedCounts[i][j]
			} else {
				sum.numShedFailures += s.shedCounts[i][j]
			}
			if s.shedCounts[i][j] > sum.mostCommonCount {
				sum.mostCommonReason = shedResult(j)
				sum.mostCommonCount = s.shedCounts[i][j]
			}
		}
	}
	return sum
}

// storePassSummary is a summary of storePassState, computed at the end of
// rebalancing.
type storePassSummary struct {
	storeID          roachpb.StoreID
	numShedSuccesses int
	numShedFailures  int
	mostCommonReason shedResult
	mostCommonCount  int
}

var (
	// NB: mma.overloaded_store.lease_grace.success should always be 0, but we
	// declare the metric to avoid making assumptions about rebalancing
	// behavior.
	metaOverloadedStoreLeaseGraceSuccess = metric.Metadata{
		Name:        "mma.overloaded_store.lease_grace.success",
		Help:        "Number of overloaded stores within lease shedding grace period for which shedding failed",
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
	// Index 0 is success and 1 is failure. The value is number of stores,
	// with each store counted at most once.
	var storeNumSummaries [numOverloadKinds][2]int64
	for storeID, state := range g.states {
		s := state.summarize()
		s.storeID = storeID
		if s.numShedSuccesses > 0 {
			storeNumSummaries[state.overloadKind][0]++
			g.successSummaries = append(g.successSummaries, s)
		} else if s.numShedFailures > 0 {
			storeNumSummaries[state.overloadKind][1]++
			g.failedSummaries = append(g.failedSummaries, s)
		}
		// Else ignore. Some transient situation caused no ranges to be
		// considered.
	}
	// Update gauge metrics using storeNumSummaries.
	for i := range storeNumSummaries {
		for j := range storeNumSummaries[i] {
			switch overloadKind(i) {
			case overloadedWaitingForLeaseShedding:
				if j == 0 {
					g.m.OverloadedStoreLeaseGraceSuccess.Update(storeNumSummaries[i][j])
				} else {
					g.m.OverloadedStoreLeaseGraceFailure.Update(storeNumSummaries[i][j])
				}
			case overloadedShortDuration:
				if j == 0 {
					g.m.OverloadedStoreShortDurSuccess.Update(storeNumSummaries[i][j])
				} else {
					g.m.OverloadedStoreShortDurFailure.Update(storeNumSummaries[i][j])
				}
			case overloadedMediumDuration:
				if j == 0 {
					g.m.OverloadedStoreMediumDurSuccess.Update(storeNumSummaries[i][j])
				} else {
					g.m.OverloadedStoreMediumDurFailure.Update(storeNumSummaries[i][j])
				}
			case overloadedLongDuration:
				if j == 0 {
					g.m.OverloadedStoreLongDurSuccess.Update(storeNumSummaries[i][j])
				} else {
					g.m.OverloadedStoreLongDurFailure.Update(storeNumSummaries[i][j])
				}
			}
		}
	}

	// Logging.
	const k = 20
	var b strings.Builder
	// Logging using g.successSummaries.
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
	// Logging using g.failedSummaries.
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
	// Logging using g.skippedStores
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
