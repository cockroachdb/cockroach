// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package allocator

import (
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/load"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/constraint"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/errors"
	"go.etcd.io/raft/v3"
)

const (
	// MaxFractionUsedThreshold controls the point at which the store cedes having
	// room for new replicas. If the fraction used of a store descriptor capacity
	// is greater than this value, it will never be used as a rebalance or
	// allocate target and we will actively try to move replicas off of it.
	MaxFractionUsedThreshold = 0.95

	// MinQPSThresholdDifference is the minimum QPS difference from the cluster
	// mean that this system should care about. In other words, we won't worry
	// about rebalancing for QPS reasons if a store's QPS differs from the mean by
	// less than this amount even if the amount is greater than the percentage
	// threshold. This avoids too many lease transfers / range rebalances in
	// lightly loaded clusters.
	MinQPSThresholdDifference = 100

	// defaultLoadBasedRebalancingInterval is how frequently to check the store-level
	// balance of the cluster.
	defaultLoadBasedRebalancingInterval = time.Minute
)

// MaxCapacityCheck returns true if the store has room for a new replica.
func MaxCapacityCheck(store roachpb.StoreDescriptor) bool {
	return store.Capacity.FractionUsed() < MaxFractionUsedThreshold
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
		if constraintsOK := constraint.ConjunctionsCheck(
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
	RaftStatusFn                                  func(r interface {
		Desc() *roachpb.RangeDescriptor
		StoreID() roachpb.StoreID
	}) *raft.Status
}

// QPSRebalanceThreshold is much like rangeRebalanceThreshold, but for
// QPS rather than range count. This should be set higher than
// rangeRebalanceThreshold because QPS can naturally vary over time as
// workloads change and clients come and go, so we need to be a little more
// forgiving to avoid thrashing.
var QPSRebalanceThreshold = func() *settings.FloatSetting {
	s := settings.RegisterFloatSetting(
		settings.SystemOnly,
		"kv.allocator.qps_rebalance_threshold",
		"minimum fraction away from the mean a store's QPS (such as queries per second) can be before it is considered overfull or underfull",
		0.10,
		settings.NonNegativeFloat,
		func(f float64) error {
			if f < 0.01 {
				return errors.Errorf("cannot set kv.allocator.qps_rebalance_threshold to less than 0.01")
			}
			return nil
		},
	)
	s.SetVisibility(settings.Public)
	return s
}()

// LoadBasedRebalanceInterval controls how frequently each store checks for
// load-base lease/replica rebalancing opportunties.
var LoadBasedRebalanceInterval = settings.RegisterPublicDurationSettingWithExplicitUnit(
	settings.SystemOnly,
	"kv.allocator.load_based_rebalancing_interval",
	"the rough interval at which each store will check for load-based lease / replica rebalancing opportunities",
	defaultLoadBasedRebalancingInterval,
	func(d time.Duration) error {
		// Setting this interval to a very low duration is generally going to be a
		// bad idea without any real benefit, so let's disallow that.
		const min = 10 * time.Second
		if d < min {
			return errors.Errorf("must specify a minimum of %s", min)
		}
		return nil
	},
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
var MinQPSDifferenceForTransfers = func() *settings.FloatSetting {
	s := settings.RegisterFloatSetting(
		settings.SystemOnly,
		"kv.allocator.min_qps_difference_for_transfers",
		"the minimum qps difference that must exist between any two stores"+
			" for the allocator to allow a lease or replica transfer between them",
		2*MinQPSThresholdDifference,
		settings.NonNegativeFloat,
	)
	s.SetVisibility(settings.Reserved)
	return s
}()

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
