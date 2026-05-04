// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mmaprototype

import (
	"cmp"
	"context"
	"slices"

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
			"that completed successfully. MMA transfers leases away from CPU-overloaded stores " +
			"since leaseholders handle all reads and coordinate writes.",
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
		`),
		Measurement: "Bytes",
		Unit:        metric.Unit_BYTES,
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
	// overloadedWaitingForLeaseShedding represents when the store is still in
	// the grace period for shedding its own leases. NB: this applies to both
	// local and remote stores. For remote stores, MMA skips shedding during
	// this period. For the local store, MMA proceeds with shedding and the
	// lease_grace metrics track whether that self-shedding succeeds.
	overloadedWaitingForLeaseShedding overloadKind = iota
	// overloadedShortDuration corresponds to ignoreLoadNoChangeAndHigher.
	overloadedShortDuration
	// overloadedMediumDuration corresponds to ignoreLoadThresholdAndHigher.
	overloadedMediumDuration
	// overloadedLongDuration corresponds to ignoreHigherThanLoadThresholdGraceDuration
	overloadedLongDuration
	numOverloadKinds
)

func (kind overloadKind) String() string {
	return redact.StringWithoutMarkers(kind)
}

// SafeFormat implements the redact.SafeFormatter interface.
func (kind overloadKind) SafeFormat(w redact.SafePrinter, _ rune) {
	switch kind {
	case overloadedWaitingForLeaseShedding:
		w.SafeString("lease-grace")
	case overloadedShortDuration:
		w.SafeString("short")
	case overloadedMediumDuration:
		w.SafeString("medium")
	case overloadedLongDuration:
		w.SafeString("long")
	default:
		w.SafeString("unknown overloadKind")
	}
}

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
	// skipped is true when MMA recognized the store as overloaded but did not
	// shed from it this pass for a known reason — most commonly because of
	// pending decrease/increase. When set, shedCounts is empty and the store
	// is attributed to the `<bucket>.skipped` gauge instead of
	// success/failure.
	skipped bool
}

func (s *storePassState) summarize() storePassSummary {
	var sum storePassSummary
	for i := 0; i < int(numShedResults); i++ {
		sum.reasonCounts[i].reason = shedResult(i)
	}
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
				sum.reasonCounts[resultIdx].count += count
			}
		}
	}
	slices.SortFunc(sum.reasonCounts[:], func(a, b reasonCount) int {
		return cmp.Compare(-a.count, -b.count)
	})
	return sum
}

// storePassSummary aggregates shedding outcomes for a single overloaded
// store.
type storePassSummary struct {
	// Overloaded store ID.
	storeID          roachpb.StoreID
	numShedSuccesses int
	numShedFailures  int
	// reasonCounts tracks the counts of shedResult for this overloaded store
	// in the descending order based on counts, used in diagnostic logging.
	reasonCounts [numShedResults]reasonCount
}

// reasonCount tracks the count of how many ranges were rejected for a given
// reason.
type reasonCount struct {
	count  int
	reason shedResult
}

var (
	metaOverloadedStoreLeaseGraceSuccess = metric.Metadata{
		Name: "mma.overloaded_store.lease_grace.success",
		Help: "Number of overloaded stores in the lease shedding grace period (first 2 min of " +
			"overload) where at least one lease or replica was successfully moved away during " +
			"this store's MMA rebalancing pass. During this grace period, remote stores wait " +
			"for the overloaded store to shed its own leases before intervening with replica " +
			"moves. This metric tracks whether the local store's self-shedding is effective.",
		Measurement: "Stores",
		Unit:        metric.Unit_COUNT,
		LabeledName: "mma.overloaded_store",
		StaticLabels: metric.MakeLabelPairs(
			metric.LabelType, "lease_grace", metric.LabelResult, "success"),
	}
	metaOverloadedStoreLeaseGraceFailure = metric.Metadata{
		Name: "mma.overloaded_store.lease_grace.failure",
		Help: "Number of overloaded stores in the lease shedding grace period (first 2 min of " +
			"overload) where the local store failed to shed any leases or replicas during " +
			"this store's MMA rebalancing pass. During this grace period, remote stores wait " +
			"for the overloaded store to shed its own leases before intervening. A non-zero " +
			"value indicates the overloaded store is struggling to self-shed.",
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

	// The "skipped" gauges below cover the third per-bucket outcome: the
	// store is overloaded but MMA chose not to shed from it this pass for a
	// known reason — most commonly because it already has too much pending
	// work (pending-decrease saturated above the fraction threshold, or
	// pending-increase >= epsilon). For each duration bucket,
	// success + failure + skipped equals the count of overloaded stores
	// observed during the local MMA rebalancing pass.
	metaOverloadedStoreLeaseGraceSkipped = metric.Metadata{
		Name: "mma.overloaded_store.lease_grace.skipped",
		Help: "Number of overloaded stores in the lease shedding grace period (first 2 min of " +
			"overload) that MMA recognized as overloaded but did not shed from this pass. " +
			"The most common reason is pending work on the store (pending-decrease saturated " +
			"above the configured fraction threshold, or pending-increase >= epsilon, " +
			"indicating the load arithmetic is unreliable). For each duration bucket, " +
			"success + failure + skipped equals the total overloaded stores observed in " +
			"that bucket this pass.",
		Measurement: "Stores",
		Unit:        metric.Unit_COUNT,
		LabeledName: "mma.overloaded_store",
		StaticLabels: metric.MakeLabelPairs(
			metric.LabelType, "lease_grace", metric.LabelResult, "skipped"),
	}
	metaOverloadedStoreShortDurSkipped = metric.Metadata{
		Name: "mma.overloaded_store.short_dur.skipped",
		Help: "Number of stores overloaded for a short duration (2-5 min) that MMA recognized " +
			"as overloaded but did not shed from this pass. The most common reason is pending " +
			"work on the store (pending-decrease saturated above the configured fraction " +
			"threshold, or pending-increase >= epsilon). For each duration bucket, " +
			"success + failure + skipped equals the total overloaded stores observed in that " +
			"bucket this pass.",
		Measurement: "Stores",
		Unit:        metric.Unit_COUNT,
		LabeledName: "mma.overloaded_store",
		StaticLabels: metric.MakeLabelPairs(
			metric.LabelType, "short_dur", metric.LabelResult, "skipped"),
	}
	metaOverloadedStoreMediumDurSkipped = metric.Metadata{
		Name: "mma.overloaded_store.medium_dur.skipped",
		Help: "Number of stores overloaded for a medium duration (5-8 min) that MMA recognized " +
			"as overloaded but did not shed from this pass. The most common reason is pending " +
			"work on the store (pending-decrease saturated above the configured fraction " +
			"threshold, or pending-increase >= epsilon). For each duration bucket, " +
			"success + failure + skipped equals the total overloaded stores observed in that " +
			"bucket this pass.",
		Measurement: "Stores",
		Unit:        metric.Unit_COUNT,
		LabeledName: "mma.overloaded_store",
		StaticLabels: metric.MakeLabelPairs(
			metric.LabelType, "medium_dur", metric.LabelResult, "skipped"),
	}
	metaOverloadedStoreLongDurSkipped = metric.Metadata{
		Name: "mma.overloaded_store.long_dur.skipped",
		Help: "Number of stores overloaded for a long duration (8+ min) that MMA recognized as " +
			"overloaded but did not shed from this pass. The most common reason is pending " +
			"work on the store (pending-decrease saturated above the configured fraction " +
			"threshold, or pending-increase >= epsilon). A persistently non-zero value here " +
			"indicates an overloaded store that is repeatedly being deferred and may not be " +
			"receiving relief. For each duration bucket, success + failure + skipped equals " +
			"the total overloaded stores observed in that bucket this pass.",
		Measurement: "Stores",
		Unit:        metric.Unit_COUNT,
		LabeledName: "mma.overloaded_store",
		StaticLabels: metric.MakeLabelPairs(
			metric.LabelType, "long_dur", metric.LabelResult, "skipped"),
	}
)

type gaugeMetrics struct {
	OverloadedStoreLeaseGraceSuccess *metric.Gauge
	OverloadedStoreLeaseGraceFailure *metric.Gauge
	OverloadedStoreLeaseGraceSkipped *metric.Gauge
	OverloadedStoreShortDurSuccess   *metric.Gauge
	OverloadedStoreShortDurFailure   *metric.Gauge
	OverloadedStoreShortDurSkipped   *metric.Gauge
	OverloadedStoreMediumDurSuccess  *metric.Gauge
	OverloadedStoreMediumDurFailure  *metric.Gauge
	OverloadedStoreMediumDurSkipped  *metric.Gauge
	OverloadedStoreLongDurSuccess    *metric.Gauge
	OverloadedStoreLongDurFailure    *metric.Gauge
	OverloadedStoreLongDurSkipped    *metric.Gauge
}

func (m *gaugeMetrics) init() {
	*m = gaugeMetrics{
		OverloadedStoreLeaseGraceSuccess: metric.NewGauge(metaOverloadedStoreLeaseGraceSuccess),
		OverloadedStoreLeaseGraceFailure: metric.NewGauge(metaOverloadedStoreLeaseGraceFailure),
		OverloadedStoreLeaseGraceSkipped: metric.NewGauge(metaOverloadedStoreLeaseGraceSkipped),
		OverloadedStoreShortDurSuccess:   metric.NewGauge(metaOverloadedStoreShortDurSuccess),
		OverloadedStoreShortDurFailure:   metric.NewGauge(metaOverloadedStoreShortDurFailure),
		OverloadedStoreShortDurSkipped:   metric.NewGauge(metaOverloadedStoreShortDurSkipped),
		OverloadedStoreMediumDurSuccess:  metric.NewGauge(metaOverloadedStoreMediumDurSuccess),
		OverloadedStoreMediumDurFailure:  metric.NewGauge(metaOverloadedStoreMediumDurFailure),
		OverloadedStoreMediumDurSkipped:  metric.NewGauge(metaOverloadedStoreMediumDurSkipped),
		OverloadedStoreLongDurSuccess:    metric.NewGauge(metaOverloadedStoreLongDurSuccess),
		OverloadedStoreLongDurFailure:    metric.NewGauge(metaOverloadedStoreLongDurFailure),
		OverloadedStoreLongDurSkipped:    metric.NewGauge(metaOverloadedStoreLongDurSkipped),
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
	rangeConstraintsError
	rangeTransient
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
	case rangeConstraintsError:
		w.SafeString("constraint-error")
	case rangeTransient:
		w.SafeString("range-transient")
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

// skippedByPending is sandwiched between storeOverloaded and finishStore. It
// records that MMA recognized the store as overloaded but did not shed from
// it this pass because the store already has too much pending work.
func (g *rebalancingPassMetricsAndLogger) skippedByPending() {
	if g == nil {
		return
	}
	g.curState.skipped = true
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
	buf := redact.StringBuilder{}
	g.computePassSummary(&buf)
	log.KvDistribution.Infof(ctx, "%s", buf.RedactableString())
}

// computePassSummary performs the aggregation of the rebalancing pass summary,
// updates gauges and generates a log message with stores per category of
// successful rebalances, failed rebalances, skipped stores (overloaded but
// not shed from this pass — most commonly due to pending work), over-budget
// stores (cut off because the per-pass range-move budget was exhausted), and
// stores for which no ranges were evaluated. The list of stores is truncated
// upto a fixed number of stores (see `logStores`).
// Example output:
/*
rebalancing pass summary [local=s1]:
	overloaded:
		short: [s1, s10]
		medium: [s8]
		long: [s6]
	success: [s1]
	failure: [{s6, total: 2, no-cand-load:2}, {s8, total: 1, no-cand:1}]
	skipped: [s10]
	over-budget: [s5]
	no-ranges-evaluated: [s10]
*/
func (g *rebalancingPassMetricsAndLogger) computePassSummary(buf *redact.StringBuilder) {
	g.failedSummaries = g.failedSummaries[:0]
	g.successSummaries = g.successSummaries[:0]

	// For each overloadKind, collect the stores that belong to it and count
	// the stores that had shedding success, failure, or were skipped this pass.
	// NB: Each store is counted at most once based on whether it was skipped
	// or had any shedding success, even if it had shedding failures.
	var overloadSummaries [numOverloadKinds]struct {
		success, failure, skipped int64
		stores                    []roachpb.StoreID
	}

	// Collect the stores for which no ranges were evaluated.
	var noRangesEvaluated []roachpb.StoreID
	// Collect skipped stores for the per-pass log.
	var skippedStores []roachpb.StoreID

	for storeID, passState := range g.states {
		storeSummary := passState.summarize()
		storeSummary.storeID = storeID

		overloadSummary := &overloadSummaries[passState.overloadKind]
		overloadSummary.stores = append(overloadSummary.stores, storeID)

		switch {
		case passState.skipped:
			overloadSummary.skipped++
			skippedStores = append(skippedStores, storeID)
		case storeSummary.numShedSuccesses > 0:
			overloadSummary.success++
			g.successSummaries = append(g.successSummaries, storeSummary)
		case storeSummary.numShedFailures > 0:
			overloadSummary.failure++
			g.failedSummaries = append(g.failedSummaries, storeSummary)
		default:
			noRangesEvaluated = append(noRangesEvaluated, storeSummary.storeID)
		}
	}

	// Update gauge metrics using overloadSummaries aggregated from all
	// overloaded stores.
	for kind, counts := range overloadSummaries {
		switch overloadKind(kind) {
		case overloadedWaitingForLeaseShedding:
			g.m.OverloadedStoreLeaseGraceSuccess.Update(counts.success)
			g.m.OverloadedStoreLeaseGraceFailure.Update(counts.failure)
			g.m.OverloadedStoreLeaseGraceSkipped.Update(counts.skipped)
		case overloadedShortDuration:
			g.m.OverloadedStoreShortDurSuccess.Update(counts.success)
			g.m.OverloadedStoreShortDurFailure.Update(counts.failure)
			g.m.OverloadedStoreShortDurSkipped.Update(counts.skipped)
		case overloadedMediumDuration:
			g.m.OverloadedStoreMediumDurSuccess.Update(counts.success)
			g.m.OverloadedStoreMediumDurFailure.Update(counts.failure)
			g.m.OverloadedStoreMediumDurSkipped.Update(counts.skipped)
		case overloadedLongDuration:
			g.m.OverloadedStoreLongDurSuccess.Update(counts.success)
			g.m.OverloadedStoreLongDurFailure.Update(counts.failure)
			g.m.OverloadedStoreLongDurSkipped.Update(counts.skipped)
		}
	}

	buf.Printf("rebalancing pass summary [local=s%v]:", g.localStoreID)

	// Log the state of all the overloaded store by kind.
	empty := true
	for kind, summary := range overloadSummaries {
		if len(summary.stores) > 0 {
			if empty {
				buf.Printf("\n\toverloaded:")
				empty = false
			}
			slices.Sort(summary.stores)
			buf.Printf("\n\t\t%v: ", overloadKind(kind))
			logStores(buf, summary.stores, func(store roachpb.StoreID) redact.RedactableString {
				return redact.Sprintf("s%v", store)
			})
		}
	}

	// Log stores where shedding succeeded, sorted by store ID.
	if len(g.successSummaries) > 0 {
		empty = false
		slices.SortFunc(g.successSummaries, func(a, b storePassSummary) int {
			return cmp.Compare(a.storeID, b.storeID)
		})
		buf.SafeString("\n\tsuccess: ")
		logStores(buf, g.successSummaries, func(store storePassSummary) redact.RedactableString {
			return redact.Sprintf("s%v", store.storeID)
		})
	}

	// Log stores where shedding failed, sorted by failure count (descending).
	if len(g.failedSummaries) > 0 {
		empty = false
		slices.SortFunc(g.failedSummaries, func(a, b storePassSummary) int {
			return cmp.Or(cmp.Compare(-a.numShedFailures, -b.numShedFailures),
				cmp.Compare(a.storeID, b.storeID))
		})
		buf.SafeString("\n\tfailure: ")
		logStores(buf, g.failedSummaries, func(store storePassSummary) redact.RedactableString {
			inner := redact.StringBuilder{}
			inner.Printf("{s%v, total: %d", store.storeID,
				redact.SafeInt(store.numShedFailures))
			for _, reasonCount := range store.reasonCounts {
				if reasonCount.count == 0 {
					break
				}
				inner.Printf(", %v:%v", reasonCount.reason, redact.SafeInt(reasonCount.count))
			}
			inner.SafeRune('}')
			return inner.RedactableString()
		})
	}

	// Log stores that were overloaded but skipped from shedding (most
	// commonly due to pending decrease/increase), sorted by store ID. The
	// bucket affiliation for these stores is visible in the `overloaded:`
	// section above.
	if len(skippedStores) > 0 {
		empty = false
		slices.Sort(skippedStores)
		buf.SafeString("\n\tskipped: ")
		logStores(buf, skippedStores, func(store roachpb.StoreID) redact.RedactableString {
			return redact.Sprintf("s%v", store)
		})
	}

	// Log stores that were cut off because the per-pass range-move budget was
	// exhausted, sorted by store ID. These stores are not yet routed through
	// the per-bucket skipped accounting; a follow-up unifies them.
	if len(g.skippedStores) > 0 {
		empty = false
		slices.Sort(g.skippedStores)
		buf.SafeString("\n\tover-budget: ")
		logStores(buf, g.skippedStores, func(store roachpb.StoreID) redact.RedactableString {
			return redact.Sprintf("s%v", store)
		})
	}

	// Log stores for which no ranges were evaluated.
	if len(noRangesEvaluated) > 0 {
		empty = false
		slices.Sort(noRangesEvaluated)
		buf.SafeString("\n\tno-ranges-evaluated: ")
		logStores(buf, noRangesEvaluated, func(store roachpb.StoreID) redact.RedactableString {
			return redact.Sprintf("s%v", store)
		})
	}

	// Log if none of the conditions were met.
	if empty {
		buf.SafeString(" none")
	}
}

// logStores is a convenience function that prints the given slice of elements
// by first applying the printer function and then joining the strings together.
func logStores[T any](
	buf *redact.StringBuilder, stores []T, printer func(T) redact.RedactableString,
) {
	// maxStoresLogged defines the upper bound on how many stores' summary is
	// logged.
	const maxStoresLogged int = 20

	omitted := false
	n := len(stores)
	if n > maxStoresLogged {
		omitted = true
		n = maxStoresLogged
	}
	buf.Printf("[%s", printer(stores[0]))
	for i := 1; i < n; i++ {
		buf.Printf(", %s", printer(stores[i]))
	}
	if omitted {
		buf.Printf(",..., %d total]", len(stores))
	} else {
		buf.SafeRune(']')
	}
}
