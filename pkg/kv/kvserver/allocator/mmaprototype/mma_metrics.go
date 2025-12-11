// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mmaprototype

import "github.com/cockroachdb/cockroach/pkg/util/metric"

type counterMetrics struct {
	DroppedDueToStateInconsistency  *metric.Counter
	ExternalFailedToRegister        *metric.Counter
	ExternaRegisterSuccess          *metric.Counter
	ExternalReplicaRebalanceSuccess *metric.Counter
	ExternalReplicaRebalanceFailure *metric.Counter
	ExternalLeaseTransferSuccess    *metric.Counter
	ExternalLeaseTransferFailure    *metric.Counter
	MMAReplicaRebalanceSuccess      *metric.Counter
	MMAReplicaRebalanceFailure      *metric.Counter
	MMALeaseTransferSuccess         *metric.Counter
	MMALeaseTransferFailure         *metric.Counter
	MMARegisterLeaseSuccess         *metric.Counter
	MMARegisterRebalanceSuccess     *metric.Counter
}

func makeCounterMetrics() *counterMetrics {
	return &counterMetrics{
		DroppedDueToStateInconsistency:  metric.NewCounter(metaDroppedDueToStateInconsistency),
		ExternalFailedToRegister:        metric.NewCounter(metaExternalFailedToRegister),
		ExternaRegisterSuccess:          metric.NewCounter(metaExternaRegisterSuccess),
		MMARegisterLeaseSuccess:         metric.NewCounter(metaMMARegisterLeaseSuccess),
		MMARegisterRebalanceSuccess:     metric.NewCounter(metaMMARegisterRebalanceSuccess),
		ExternalReplicaRebalanceSuccess: metric.NewCounter(metaExternalReplicaRebalanceSuccess),
		ExternalReplicaRebalanceFailure: metric.NewCounter(metaExternalReplicaRebalanceFailure),
		ExternalLeaseTransferSuccess:    metric.NewCounter(metaExternalLeaseTransferSuccess),
		ExternalLeaseTransferFailure:    metric.NewCounter(metaExternalLeaseTransferFailure),
		MMAReplicaRebalanceSuccess:      metric.NewCounter(metaMMAReplicaRebalanceSuccess),
		MMAReplicaRebalanceFailure:      metric.NewCounter(metaMMAReplicaRebalanceFailure),
		MMALeaseTransferSuccess:         metric.NewCounter(metaMMALeaseTransferSuccess),
		MMALeaseTransferFailure:         metric.NewCounter(metaMMALeaseTransferFailure),
	}
}

var (
	metaDroppedDueToStateInconsistency = metric.Metadata{
		Name:        "mma.dropped",
		Help:        "Number of operations dropped due to MMA state inconsistency",
		Measurement: "Range Rebalances",
		Unit:        metric.Unit_COUNT,
	}
	metaExternalFailedToRegister = metric.Metadata{
		Name:        "mma.external.dropped",
		Help:        "Number of external operations that failed to register with MMA",
		Measurement: "Range Rebalances",
		Unit:        metric.Unit_COUNT,
	}
	metaExternaRegisterSuccess = metric.Metadata{
		Name:        "mma.external.success",
		Help:        "Number of external operations successfully registered with MMA",
		Measurement: "Range Rebalances",
		Unit:        metric.Unit_COUNT,
	}
	metaMMARegisterLeaseSuccess = metric.Metadata{
		Name:        "mma.lease.register.success",
		Help:        "Number of lease transfers successfully registered with MMA",
		Measurement: "Range Rebalances",
		Unit:        metric.Unit_COUNT,
	}
	metaMMARegisterRebalanceSuccess = metric.Metadata{
		Name:        "mma.rebalance.register.success",
		Help:        "Number of rebalance operations successfully registered with MMA",
		Measurement: "Range Rebalances",
		Unit:        metric.Unit_COUNT,
	}
	metaExternalReplicaRebalanceSuccess = metric.Metadata{
		Name:        "mma.rebalances.external.success",
		Help:        "Number of successful external replica rebalance operations",
		Measurement: "Range Rebalances",
		Unit:        metric.Unit_COUNT,
	}

	metaExternalLeaseTransferSuccess = metric.Metadata{
		Name:        "mma.lease.external.success",
		Help:        "Number of successful external lease transfer operations",
		Measurement: "Lease Transfers",
		Unit:        metric.Unit_COUNT,
	}

	metaExternalReplicaRebalanceFailure = metric.Metadata{
		Name:        "mma.rebalances.external.failure",
		Help:        "Number of failed external replica rebalance operations",
		Measurement: "Range Rebalances",
		Unit:        metric.Unit_COUNT,
	}

	metaExternalLeaseTransferFailure = metric.Metadata{
		Name:        "mma.lease.external.failure",
		Help:        "Number of failed external lease transfer operations",
		Measurement: "Lease Transfers",
		Unit:        metric.Unit_COUNT,
	}

	metaMMAReplicaRebalanceSuccess = metric.Metadata{
		Name:        "mma.rebalance.success",
		Help:        "Number of successful MMA-initiated replica rebalance operations",
		Measurement: "Range Rebalances",
		Unit:        metric.Unit_COUNT,
	}

	metaMMAReplicaRebalanceFailure = metric.Metadata{
		Name:        "mma.rebalance.failure",
		Help:        "Number of failed MMA-initiated replica rebalance operations",
		Measurement: "Range Rebalances",
		Unit:        metric.Unit_COUNT,
	}

	metaMMALeaseTransferSuccess = metric.Metadata{
		Name:        "mma.lease.success",
		Help:        "Number of successful MMA-initiated lease transfer operations",
		Measurement: "Lease Transfers",
		Unit:        metric.Unit_COUNT,
	}

	metaMMALeaseTransferFailure = metric.Metadata{
		Name:        "mma.lease.failure",
		Help:        "Number of failed MMA-initiated lease transfer operations",
		Measurement: "Lease Transfers",
		Unit:        metric.Unit_COUNT,
	}
)
