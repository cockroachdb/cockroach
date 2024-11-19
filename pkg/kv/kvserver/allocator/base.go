// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package allocator

import (
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/load"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/constraint"
	"github.com/cockroachdb/cockroach/pkg/raft"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
)

const (
	// MinQPSThresholdDifference is the minimum QPS difference from the cluster
	// mean that this system should care about. In other words, we won't worry
	// about rebalancing for QPS reasons if a store's QPS differs from the mean by
	// less than this amount even if the amount is greater than the percentage
	// threshold. This avoids too many lease transfers / range rebalances in
	// lightly loaded clusters.
	MinQPSThresholdDifference = 100

	// MinCPUThresholdDifference is the minimum CPU difference from the cluster
	// mean that this system should care about. The system won't attempt to
	// take action if a store's CPU differs from the mean by less than this
	// amount even if it is greater than the percentage threshold. This
	// prevents too many lease transfers or range rebalances in lightly loaded
	// clusters.
	//
	// NB: This represents 5% (1/20) utilization of 1 cpu on average.  This
	// number was arrived at from testing to minimize thrashing. This number is
	// set independent of processor speed and assumes identical value of cpu
	// time across all stores. i.e. all cpu's are identical.
	MinCPUThresholdDifference = float64(50 * time.Millisecond)

	// MinCPUDifferenceForTransfers is the minimum CPU difference that a
	// store rebalncer would care about to reconcile (via lease or replica
	// rebalancing) between any two stores.
	//
	// NB: This is set to be two times the minimum threshold that a store needs
	// to be above or below the mean to be considered overfull or underfull
	// respectively. This is to make lease transfers and replica rebalances
	// less sensistive to jitters in any given workload by introducing
	// additional friction before taking these actions.
	MinCPUDifferenceForTransfers = 2 * MinCPUThresholdDifference

	// defaultLoadBasedRebalancingInterval is how frequently to check the store-level
	// balance of the cluster.
	defaultLoadBasedRebalancingInterval = time.Minute
)

// AllocationError is a simple interface used to indicate a replica processing
// error originating from the allocator.
type AllocationError interface {
	error
	AllocationErrorMarker() // dummy method for unique interface
}

// IsStoreValid returns true iff the provided store would be a valid in a
// range with the provided constraints.
func IsStoreValid(
	store roachpb.StoreDescriptor, constraints []roachpb.ConstraintsConjunction,
) bool {
	if len(constraints) == 0 {
		return true
	}
	for _, subConstraints := range constraints {
		if constraintsOK := constraint.CheckStoreConjunction(
			store, subConstraints.Constraints,
		); constraintsOK {
			return true
		}
	}
	return false
}

// TestingKnobs allows tests to override the behavior of `Allocator`.
type TestingKnobs struct {
	// AllowLeaseTransfersToReplicasNeedingSnapshots permits lease transfer
	// targets produced by the Allocator to include replicas that may be waiting
	// for snapshots.
	AllowLeaseTransfersToReplicasNeedingSnapshots bool
	RaftStatusFn                                  func(
		desc *roachpb.RangeDescriptor,
		storeID roachpb.StoreID,
	) *raft.Status
	// BlockTransferTarget can be used to block returning any transfer targets
	// from TransferLeaseTarget.
	BlockTransferTarget func(roachpb.RangeID) bool
}

// QPSRebalanceThreshold is much like rangeRebalanceThreshold, but for
// QPS rather than range count. This should be set higher than
// rangeRebalanceThreshold because QPS can naturally vary over time as
// workloads change and clients come and go, so we need to be a little more
// forgiving to avoid thrashing.
var QPSRebalanceThreshold = settings.RegisterFloatSetting(
	settings.SystemOnly,
	"kv.allocator.qps_rebalance_threshold",
	"minimum fraction away from the mean a store's QPS (such as queries per second) can be before it is considered overfull or underfull",
	0.10,
	settings.FloatWithMinimum(0.01),
	settings.WithPublic,
)

// CPURebalanceThreshold is the minimum ratio of a store's cpu time to the mean
// cpu time at which that store is considered overfull or underfull of cpu
// usage.
var CPURebalanceThreshold = settings.RegisterFloatSetting(
	settings.SystemOnly,
	"kv.allocator.store_cpu_rebalance_threshold",
	"minimum fraction away from the mean a store's cpu usage can be before it is considered overfull or underfull",
	0.10,
	settings.FloatWithMinimum(0.01),
	settings.WithPublic,
)

// LoadBasedRebalanceInterval controls how frequently each store checks for
// load-base lease/replica rebalancing opportunties.
var LoadBasedRebalanceInterval = settings.RegisterDurationSettingWithExplicitUnit(
	settings.SystemOnly,
	"kv.allocator.load_based_rebalancing_interval",
	"the rough interval at which each store will check for load-based lease / replica rebalancing opportunities",
	defaultLoadBasedRebalancingInterval,
	// Setting this interval to a very low duration is generally going to be a
	// bad idea without any real benefit, so let's disallow that.
	settings.DurationWithMinimum(10*time.Second),
	settings.WithPublic,
)

// MinQPSDifferenceForTransfers is the minimum QPS difference that the store
// rebalancer would care to reconcile (via lease or replica rebalancing) between
// any two stores.
//
// NB: This value is used to compare the QPS of two stores _without accounting_
// for the QPS of the replica or lease that is being considered for the
// transfer. This is set to be twice the minimum threshold that a store needs to
// be above or below the mean to be considered overfull or underfull
// respectively. This is to make lease and replica transfers less sensitive to
// the jitters in any given workload.
var MinQPSDifferenceForTransfers = settings.RegisterFloatSetting(
	settings.SystemOnly,
	"kv.allocator.min_qps_difference_for_transfers",
	"the minimum qps difference that must exist between any two stores"+
		" for the allocator to allow a lease or replica transfer between them",
	2*MinQPSThresholdDifference,
	settings.NonNegativeFloat,
	settings.WithVisibility(settings.Reserved),
)

// transferLeaseGoal dictates whether a call to TransferLeaseTarget should
// improve locality of access, convergence of lease counts or convergence of
// QPS.
type transferLeaseGoal int

const (
	// FollowTheWorkload moves leases towards where requests originate, to improve
	// locality of access.
	FollowTheWorkload transferLeaseGoal = iota
	// LeaseCountConvergence transfers leases such that lease counts converge
	// across stores.
	LeaseCountConvergence
	// LoadConvergence transfers leases such that load converges across stores.
	LoadConvergence
)

// TransferLeaseOptions is the set of options needed to evaluate a lease
// transfer.
type TransferLeaseOptions struct {
	Goal transferLeaseGoal
	// ExcludeLeaseRepl, when true, tells `TransferLeaseTarget` to exclude the
	// current leaseholder from consideration as a potential target (i.e. when the
	// caller explicitly wants to shed its lease away).
	ExcludeLeaseRepl bool
	// CheckCandidateFullness, when false, tells `TransferLeaseTarget`
	// to disregard the existing lease counts on candidates.
	CheckCandidateFullness bool
	// AllowUninitializedCandidates allows a lease transfer target to include
	// replicas which are not in the existing replica set.
	AllowUninitializedCandidates bool
	// LoadDimensions declares the load dimensions to use when the Goal is
	// LoadConvergence.
	LoadDimensions []load.Dimension
}

// LeaseTransferOutcome represents the result of shedLease().
type LeaseTransferOutcome int

const (
	// TransferErr indicates a lease transfer attempt that errored out.
	TransferErr LeaseTransferOutcome = iota
	// TransferOK indicates a successful lease transfer attempt.
	TransferOK
	// NoSuitableTarget indicates a lease transfer attempt that found no suitable
	// targets.
	NoSuitableTarget
)

func (o LeaseTransferOutcome) String() string {
	switch o {
	case TransferErr:
		return "err"
	case TransferOK:
		return "ok"
	case NoSuitableTarget:
		return "no suitable transfer target found"
	default:
		return fmt.Sprintf("unexpected status value: %d", o)
	}
}

// SafeValue implements the redact.SafeValue interface.
func (o LeaseTransferOutcome) SafeValue() {}
