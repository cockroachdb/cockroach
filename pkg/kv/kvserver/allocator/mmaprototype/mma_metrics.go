// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mmaprototype

import "github.com/cockroachdb/cockroach/pkg/util/metric"

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
