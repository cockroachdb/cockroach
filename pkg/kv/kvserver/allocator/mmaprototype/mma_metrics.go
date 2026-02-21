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
	"github.com/cockroachdb/crlib/crstrings"
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
		Name: "mma.dropped",
		Help: "Number of pending replica or lease changes that MMA dropped because its internal " +
			"state became inconsistent with the actual cluster state. This can happen when an " +
			"external change (e.g., from another component or admin command) modifies the " +
			"cluster in a way that invalidates MMA's pending changes. A non-zero value is " +
			"expected during normal operation when external changes occur.",
		Measurement: "Replica/Lease Change",
		Unit:        metric.Unit_COUNT,
	}
	metaExternalRegisterSuccess = metric.Metadata{
		Name: "mma.external.registration.success",
		Help: "Number of external operations (replica/lease changes not initiated by MMA, e.g., " +
			"from admin commands or other allocators) that were successfully registered with " +
			"MMA. Registration allows MMA to track the change and keep its internal state " +
			"synchronized with the cluster.",
		Measurement: "Replica/Lease Change",
		Unit:        metric.Unit_COUNT,
	}
	metaExternalRegisterFailure = metric.Metadata{
		Name: "mma.external.registration.failure",
		Help: "Number of external operations (replica/lease changes not initiated by MMA) that " +
			"failed to register with MMA due to pre-check failures (e.g., the change conflicts " +
			"with MMA's current state). The external operation may still proceed, but MMA will " +
			"not track it until the next state synchronization.",
		Measurement: "Replica/Lease Change",
		Unit:        metric.Unit_COUNT,
	}
	metaExternalReplicaChangeSuccess = metric.Metadata{
		Name: "mma.change.external.replica.success",
		Help: "Number of external replica changes (adding, removing, or moving replicas not " +
			"initiated by MMA) that completed successfully. External changes come from other " +
			"components like admin commands or the legacy allocator (SMA).",
		Measurement: "Range Change",
		Unit:        metric.Unit_COUNT,
		LabeledName: "mma.change",
		StaticLabels: metric.MakeLabelPairs(
			metric.LabelOrigin, "external", metric.LabelType, "replica", metric.LabelResult, "success"),
	}
	metaExternalReplicaChangeFailure = metric.Metadata{
		Name: "mma.change.external.replica.failure",
		Help: "Number of external replica changes (adding, removing, or moving replicas not " +
			"initiated by MMA) that failed. External changes come from other components like " +
			"admin commands or the legacy allocator (SMA). Failures may indicate constraint " +
			"violations, unavailable nodes, or other issues.",
		Measurement: "Range Change",
		Unit:        metric.Unit_COUNT,
		LabeledName: "mma.change",
		StaticLabels: metric.MakeLabelPairs(
			metric.LabelOrigin, "external", metric.LabelType, "replica", metric.LabelResult, "failure"),
	}
	metaExternalLeaseChangeSuccess = metric.Metadata{
		Name: "mma.change.external.lease.success",
		Help: "Number of external lease transfers (moving the leaseholder to a different replica, " +
			"not initiated by MMA) that completed successfully. External changes come from other " +
			"components like admin commands or the legacy allocator (SMA).",
		Measurement: "Lease Change",
		Unit:        metric.Unit_COUNT,
		LabeledName: "mma.change",
		StaticLabels: metric.MakeLabelPairs(
			metric.LabelOrigin, "external", metric.LabelType, "lease", metric.LabelResult, "success"),
	}
	metaExternalLeaseChangeFailure = metric.Metadata{
		Name: "mma.change.external.lease.failure",
		Help: "Number of external lease transfers (moving the leaseholder to a different replica, " +
			"not initiated by MMA) that failed. External changes come from other components like " +
			"admin commands or the legacy allocator (SMA). Failures may indicate the target " +
			"replica is unavailable or doesn't meet lease preferences.",
		Measurement: "Lease Change",
		Unit:        metric.Unit_COUNT,
		LabeledName: "mma.change",
		StaticLabels: metric.MakeLabelPairs(
			metric.LabelOrigin, "external", metric.LabelType, "lease", metric.LabelResult, "failure"),
	}
	metaRebalanceReplicaChangeSuccess = metric.Metadata{
		Name: "mma.change.rebalance.replica.success",
		Help: "Number of MMA-initiated replica changes (adding, removing, or moving replicas to " +
			"balance load) that completed successfully. MMA moves replicas away from overloaded " +
			"stores to reduce their load.",
		Measurement: "Range Change",
		Unit:        metric.Unit_COUNT,
		LabeledName: "mma.change",
		StaticLabels: metric.MakeLabelPairs(
			metric.LabelOrigin, "rebalance", metric.LabelType, "replica", metric.LabelResult, "success"),
	}
	metaRebalanceReplicaChangeFailure = metric.Metadata{
		Name: "mma.change.rebalance.replica.failure",
		Help: "Number of MMA-initiated replica changes (adding, removing, or moving replicas to " +
			"balance load) that failed. Failures may indicate constraint violations, unavailable " +
			"target stores, or concurrent changes that invalidated the operation.",
		Measurement: "Range Change",
		Unit:        metric.Unit_COUNT,
		LabeledName: "mma.change",
		StaticLabels: metric.MakeLabelPairs(
			metric.LabelOrigin, "rebalance", metric.LabelType, "replica", metric.LabelResult, "failure"),
	}
	metaRebalanceLeaseChangeSuccess = metric.Metadata{
		Name: "mma.change.rebalance.lease.success",
		Help: "Number of MMA-initiated lease transfers (moving the leaseholder to balance load) " +
			"that completed successfully. MMA transfers leases away from overloaded stores to " +
			"reduce their CPU load, since leaseholders handle all reads and coordinate writes.",
		Measurement: "Lease Change",
		Unit:        metric.Unit_COUNT,
		LabeledName: "mma.change",
		StaticLabels: metric.MakeLabelPairs(
			metric.LabelOrigin, "rebalance", metric.LabelType, "lease", metric.LabelResult, "success"),
	}
	metaRebalanceLeaseChangeFailure = metric.Metadata{
		Name: "mma.change.rebalance.lease.failure",
		Help: "Number of MMA-initiated lease transfers (moving the leaseholder to balance load) " +
			"that failed. Failures may indicate the target replica is unavailable, doesn't meet " +
			"lease preferences, or a concurrent change invalidated the operation.",
		Measurement: "Lease Change",
		Unit:        metric.Unit_COUNT,
		LabeledName: "mma.change",
		StaticLabels: metric.MakeLabelPairs(
			metric.LabelOrigin, "rebalance", metric.LabelType, "lease", metric.LabelResult, "failure"),
	}
	metaSpanConfigNormalizationError = metric.Metadata{
		Name: "mma.span_config.normalization.error",
		Help: "Number of ranges where the local store is leaseholder and MMA encountered errors " +
			"while normalizing the span config (zone configuration). This includes both hard " +
			"errors (config is unusable, range excluded from rebalancing) and soft errors " +
			"(best-effort config produced, range still rebalanced). Non-zero values indicate " +
			"zone config issues that operators should review.",
		Measurement: "Range",
		Unit:        metric.Unit_COUNT,
		LabeledName: "mma.span_config.normalization",
		StaticLabels: metric.MakeLabelPairs(
			metric.LabelResult, "error"),
	}
	metaSpanConfigNormalizationSoftError = metric.Metadata{
		Name: "mma.span_config.normalization.soft_error",
		Help: "Number of ranges where the local store is leaseholder and MMA's structural " +
			"normalization of the span config failed, but MMA produced a best-effort usable " +
			"config. These ranges remain valid rebalancing candidates. Example: voter constraints " +
			"cannot be fully satisfied by the replica constraints. Non-zero values indicate zone " +
			"config issues that operators should fix.",
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

// loadAndCapacityMetrics contains per-store metrics related to the virtual
// load and capacity calculated at each store that are used by MMA to perform
// allocation. The allocator on each node calculates the load and capacity of
// every store, but we only report metrics for the local stores.
type loadAndCapacityMetrics struct {
	// The metrics below populate the CPU load and capacity as seen by MMA.
	CPULoad        *metric.Gauge
	CPUCapacity    *metric.Gauge
	CPUUtilization *metric.GaugeFloat64

	// The metric below populates the disk write bandwidth as seen by MMA.
	WriteBandwidth *metric.Gauge

	// The metrics below populate the disk load and capacity as seen by MMA.
	DiskLoad        *metric.Gauge
	DiskCapacity    *metric.Gauge
	DiskUtilization *metric.GaugeFloat64
}

var (
	metaStoreCPULoad = metric.Metadata{
		Name: "mma.store.cpu.load",
		Help: crstrings.UnwrapText(`
			CPU load that is attributed to the replicas on this store. This
			includes reads (for leaseholder) and raft. Since CPU is shared
			across stores on a node, we approximate this by measuring the CPU
			usage on the node and then dividing this equally among all stores
			on the node.
		`),
		Measurement: "Nanoseconds/Sec",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaStoreCPUCapacity = metric.Metadata{
		Name: "mma.store.cpu.capacity",
		Help: crstrings.UnwrapText(`
			Logical CPU capacity estimated by MMA by extrapolating from the
			current load and system CPU utilization after accounting for CPU
			load that MMA cannot account for that scales with KV work (RPC,
			DistSender, etc.) and load that doesn't (SQL).
		`),
		Measurement: "Nanoseconds/Sec",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaStoreCPUUtilization = metric.Metadata{
		Name:        "mma.store.cpu.utilization",
		Help:        "Ratio of logical CPU load to capacity expressed as a percentage",
		Measurement: "CPU Utilization",
		Unit:        metric.Unit_PERCENT,
	}
	metaStoreWriteBandwidth = metric.Metadata{
		Name:        "mma.store.write.bandwidth",
		Help:        "Disk write bandwidth as observed by MMA corresponding to the store",
		Measurement: "Bytes/Sec",
		Unit:        metric.Unit_BYTES,
	}
	metaStoreDiskLoad = metric.Metadata{
		Name: "mma.store.disk.logical",
		Help: crstrings.UnwrapText(`
			Logical bytes consumed by the replicas on this store as reported by
			MVCC statistics without accounting for any space amplification or
			compression.
		`),
		Measurement: "Bytes",
		Unit:        metric.Unit_BYTES,
	}
	metaStoreDiskCapacity = metric.Metadata{
		Name: "mma.store.disk.capacity",
		Help: crstrings.UnwrapText(`
			Logical disk capacity estimated by MMA by extrapolating from the
			logical bytes consumed by the replicas and the current used and free
			physical disk bytes.
		`), Measurement: "Bytes",
		Unit: metric.Unit_BYTES,
	}
	metaStoreDiskUtilization = metric.Metadata{
		Name:        "mma.store.disk.utilization",
		Help:        "Ratio of logical disk usage to capacity expressed as a percentage",
		Measurement: "Disk Utilization",
		Unit:        metric.Unit_PERCENT,
	}
)

func makeLoadAndCapacityMetrics() *loadAndCapacityMetrics {
	return &loadAndCapacityMetrics{
		CPULoad:         metric.NewGauge(metaStoreCPULoad),
		CPUCapacity:     metric.NewGauge(metaStoreCPUCapacity),
		CPUUtilization:  metric.NewGaugeFloat64(metaStoreCPUUtilization),
		WriteBandwidth:  metric.NewGauge(metaStoreWriteBandwidth),
		DiskLoad:        metric.NewGauge(metaStoreDiskLoad),
		DiskCapacity:    metric.NewGauge(metaStoreDiskCapacity),
		DiskUtilization: metric.NewGaugeFloat64(metaStoreDiskUtilization),
	}
}

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
		Name: "mma.overloaded_store.lease_grace.success",
		Help: "Number of overloaded stores in the lease shedding grace period (first 2 min of " +
			"overload) where at least one lease or replica was successfully moved away during " +
			"this store's MMA rebalancing pass. During this grace period, MMA waits for the " +
			"overloaded store to shed its own leases before intervening. This metric should " +
			"normally be 0 since MMA skips shedding during the grace period.",
		Measurement: "Stores",
		Unit:        metric.Unit_COUNT,
		LabeledName: "mma.overloaded_store",
		StaticLabels: metric.MakeLabelPairs(
			metric.LabelType, "lease_grace", metric.LabelResult, "success"),
	}
	metaOverloadedStoreLeaseGraceFailure = metric.Metadata{
		Name: "mma.overloaded_store.lease_grace.failure",
		Help: "Number of overloaded stores in the lease shedding grace period (first 2 min of " +
			"overload) where all shedding attempts failed during this store's MMA rebalancing " +
			"pass. During this grace period, MMA waits for the overloaded store to shed its " +
			"own leases before intervening. A non-zero value indicates stores were observed " +
			"but intentionally skipped.",
		Measurement: "Stores",
		Unit:        metric.Unit_COUNT,
		LabeledName: "mma.overloaded_store",
		StaticLabels: metric.MakeLabelPairs(
			metric.LabelType, "lease_grace", metric.LabelResult, "failure"),
	}
	metaOverloadedStoreShortDurSuccess = metric.Metadata{
		Name: "mma.overloaded_store.short_dur.success",
		Help: "Number of stores overloaded for a short duration (2-5 min) where at least one " +
			"lease or replica was successfully moved away during this store's MMA rebalancing " +
			"pass. At this stage, MMA only considers lightly-loaded target stores to avoid " +
			"overloading them.",
		Measurement: "Stores",
		Unit:        metric.Unit_COUNT,
		LabeledName: "mma.overloaded_store",
		StaticLabels: metric.MakeLabelPairs(
			metric.LabelType, "short_dur", metric.LabelResult, "success"),
	}
	metaOverloadedStoreShortDurFailure = metric.Metadata{
		Name: "mma.overloaded_store.short_dur.failure",
		Help: "Number of stores overloaded for a short duration (2-5 min) where all shedding " +
			"attempts failed during this store's MMA rebalancing pass. Failures occur when no " +
			"suitable target store is found (e.g., all targets are too loaded, constraints " +
			"cannot be satisfied). At this stage, MMA only considers lightly-loaded targets.",
		Measurement: "Stores",
		Unit:        metric.Unit_COUNT,
		LabeledName: "mma.overloaded_store",
		StaticLabels: metric.MakeLabelPairs(
			metric.LabelType, "short_dur", metric.LabelResult, "failure"),
	}
	metaOverloadedStoreMediumDurSuccess = metric.Metadata{
		Name: "mma.overloaded_store.medium_dur.success",
		Help: "Number of stores overloaded for a medium duration (5-8 min) where at least one " +
			"lease or replica was successfully moved away during this store's MMA rebalancing " +
			"pass. At this stage, MMA becomes more aggressive and considers moderately-loaded " +
			"target stores that were previously excluded.",
		Measurement: "Stores",
		Unit:        metric.Unit_COUNT,
		LabeledName: "mma.overloaded_store",
		StaticLabels: metric.MakeLabelPairs(
			metric.LabelType, "medium_dur", metric.LabelResult, "success"),
	}
	metaOverloadedStoreMediumDurFailure = metric.Metadata{
		Name: "mma.overloaded_store.medium_dur.failure",
		Help: "Number of stores overloaded for a medium duration (5-8 min) where all shedding " +
			"attempts failed during this store's MMA rebalancing pass. Despite considering " +
			"moderately-loaded targets, no suitable store was found (e.g., constraints cannot " +
			"be satisfied, all candidates still too loaded).",
		Measurement: "Stores",
		Unit:        metric.Unit_COUNT,
		LabeledName: "mma.overloaded_store",
		StaticLabels: metric.MakeLabelPairs(
			metric.LabelType, "medium_dur", metric.LabelResult, "failure"),
	}
	metaOverloadedStoreLongDurSuccess = metric.Metadata{
		Name: "mma.overloaded_store.long_dur.success",
		Help: "Number of stores overloaded for a long duration (8+ min) where at least one " +
			"lease or replica was successfully moved away during this store's MMA rebalancing " +
			"pass. At this stage, MMA is most aggressive and considers any target store at or " +
			"below the load threshold.",
		Measurement: "Stores",
		Unit:        metric.Unit_COUNT,
		LabeledName: "mma.overloaded_store",
		StaticLabels: metric.MakeLabelPairs(
			metric.LabelType, "long_dur", metric.LabelResult, "success"),
	}
	metaOverloadedStoreLongDurFailure = metric.Metadata{
		Name: "mma.overloaded_store.long_dur.failure",
		Help: "Number of stores overloaded for a long duration (8+ min) where all shedding " +
			"attempts failed during this store's MMA rebalancing pass. Even with the most " +
			"aggressive target selection, no suitable store was found. This may indicate " +
			"cluster-wide capacity issues or overly restrictive constraints.",
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
