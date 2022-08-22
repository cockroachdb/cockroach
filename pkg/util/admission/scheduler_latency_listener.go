// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package admission

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
)

var _ metric.Struct = &schedulerLatencyListenerMetrics{}

// TODO(irfansharif): There’s some discrepancy between what this struct observes
// as p99 scheduling latencies and what prometheus/client_golang computes. Worth
// investigating.

type schedulerLatencyListener struct {
	ewmaP99            float64
	elasticCPUAdjuster elasticCPUUtilizationAdjuster
	coord              *ElasticCPUGrantCoordinator
	metrics            *schedulerLatencyListenerMetrics
	settings           *cluster.Settings
}

func newSchedulerLatencyListener(
	st *cluster.Settings, metrics *schedulerLatencyListenerMetrics, e elasticCPUUtilizationAdjuster,
) *schedulerLatencyListener {
	return &schedulerLatencyListener{
		settings:           st,
		metrics:            metrics,
		elasticCPUAdjuster: e,
	}
}

func (e *schedulerLatencyListener) SchedulerLatency(p99, period time.Duration) {
	c := elasticCPUGranterEWMAConstant.Get(&e.settings.SV)
	e.ewmaP99 = c*float64(p99.Nanoseconds()) + (1-c)*e.ewmaP99

	e.metrics.ObservedP99SchedulerLatency.Update(p99.Nanoseconds())
	e.metrics.EWMAP99SchedulerLatency.Update(int64(e.ewmaP99))

	targetP99 := elasticCPUGranterSchedulerLatencyTarget.Get(&e.settings.SV)
	minUtilization := elasticCPUGranterMinUtilization.Get(&e.settings.SV)
	maxUtilization := elasticCPUGranterMaxUtilization.Get(&e.settings.SV)
	multiplicativeFactor := elasticCPUGranterMultiplicativeFactor.Get(&e.settings.SV)
	deltaPerMs := elasticCPUGranterAdditiveDeltaPerSecond.Get(&e.settings.SV) / float64(time.Second.Milliseconds())
	delta := deltaPerMs * float64(period.Milliseconds())
	utilizationFractionForAdditionalCPU := elasticCPUGranterUtilizationFractionForAdditionalCPU.Get(&e.settings.SV)

	currentTargetUtilization := e.elasticCPUAdjuster.getTargetUtilization()
	currentObservedUtilization := e.elasticCPUAdjuster.getObservedUtilization() // TODO(irfansharif): Does this need to be smoothed out?
	if int64(e.ewmaP99) > targetP99.Nanoseconds() && currentObservedUtilization > minUtilization {
		newTargetUtilization := currentTargetUtilization - (delta * multiplicativeFactor)
		if newTargetUtilization < minUtilization {
			newTargetUtilization = minUtilization
		}
		log.Infof(context.TODO(), "elastic cpu limiter; %f%% - %f%% => %f%%",
			100*currentTargetUtilization, 100*delta*multiplicativeFactor, 100*newTargetUtilization)
		e.elasticCPUAdjuster.setTargetUtilization(newTargetUtilization)
	} else {
		if currentObservedUtilization/currentTargetUtilization >= utilizationFractionForAdditionalCPU {
			newTargetUtilization := currentTargetUtilization + delta
			if newTargetUtilization > maxUtilization {
				newTargetUtilization = maxUtilization
			}
			log.Infof(context.TODO(), "elastic cpu limiter; %f%% + %f%% => %f%%",
				100*currentTargetUtilization, 100*delta, 100*newTargetUtilization)
			e.elasticCPUAdjuster.setTargetUtilization(newTargetUtilization)
		}
	}

	// XXX: This needs to get ticked a lot more frequently to drive high elastic
	// CPU utilization, else requests sit idle in the work queues.
	e.coord.tryGrant()
}

var ( // cluster settings to control how elastic CPU granter is adjusted
	elasticCPUGranterMaxUtilization = settings.RegisterFloatSetting(
		settings.SystemOnly,
		"elastic_cpu_granter.max_utilization",
		"sets the ceiling on per-node elastic work CPU % utilization",
		0.25, // 25%
	)

	elasticCPUGranterMinUtilization = settings.RegisterFloatSetting(
		settings.SystemOnly,
		"elastic_cpu_granter.min_utilization",
		"sets the floor on per-node elastic work CPU % utilization",
		0.05, // 5%
	)

	elasticCPUGranterEWMAConstant = settings.RegisterFloatSetting(
		settings.SystemOnly,
		"elastic_cpu_granter.ewma_constant",
		"ewma constant to use for scheduler latency smoothing",
		0.3,
	)

	elasticCPUGranterAdditiveDeltaPerSecond = settings.RegisterFloatSetting(
		settings.SystemOnly,
		"elastic_cpu_granter.additive_delta_per_second",
		"sets the per-second % adjustments used when when adapting elastic work CPU %s",
		0.001, // 0.1%, takes 10s to add 1% to elastic CPU limit.
	)

	elasticCPUGranterMultiplicativeFactor = settings.RegisterFloatSetting(
		settings.SystemOnly,
		"elastic_cpu_granter.multiplicative_factor_on_decrease",
		"sets the multiplier on negative adjustments to elastic work CPU %",
		2, // 2 * 0.1%, takes 5s to subtract 1% from elastic CPU limit.
	)

	// TODO(irfansharif): This setting is flawed. It can make for a very slow
	// rise (the observed utilization value is not smoothed) or be altogether
	// unreactive when operating at low elastic CPU % limits. Consider if the
	// target utilization is at 1% in an 8vCPU machine. The burst capacity of
	// the token bucket = 1% * 8s = 80ms. If we're relying on observing 90% of
	// that value being in use, i.e. 72ms to have been acquired by elastic work,
	// this is simply not possible when the smallest unit of acquisition is
	// larger, say 100ms.
	//
	// We really only want this for one reason: only increase CPU allotment if
	// there are active users of this quota that possibly benefit from a larger
	// allotment. So we could instead make this conditional on there being >= 1
	// waiters X% of time over some recent time window.
	elasticCPUGranterUtilizationFractionForAdditionalCPU = settings.RegisterFloatSetting(
		settings.SystemOnly,
		"elastic_cpu_granter.utilization_fraction_for_additional_cpu",
		"sets the minimum utilization of the current limit needed before increasing elastic CPU %",
		0.9, // 90%
	)

	elasticCPUGranterSchedulerLatencyTarget = settings.RegisterDurationSetting(
		settings.SystemOnly,
		"elastic_cpu_granter.scheduler_latency_target",
		"sets the p99 scheduling latency the elastic cpu granter aims for",
		time.Millisecond,
	)
)

var (
	instantaneousP99SchedulerLatency = metric.Metadata{
		Name:        "admission.scheduler_latency_listener.instantaneous_p99_nanos",
		Help:        "The scheduling latency at p99 as observed by the scheduler latency listener",
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
	}

	smoothedP99SchedulerLatency = metric.Metadata{
		Name:        "admission.scheduler_latency_listener.smoothed_p99_nanos",
		Help:        "The scheduling latency at p99 as observed by the scheduler latency listener, exponentially smoothed",
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
	}
)

// schedulerLatencyListenerMetrics are the metrics associated with an instance
// of the schedulerLatencyListener.
type schedulerLatencyListenerMetrics struct {
	ObservedP99SchedulerLatency *metric.Gauge
	EWMAP99SchedulerLatency     *metric.Gauge
}

func makeSchedulerLatencyListenerMetrics() *schedulerLatencyListenerMetrics {
	return &schedulerLatencyListenerMetrics{
		ObservedP99SchedulerLatency: metric.NewGauge(instantaneousP99SchedulerLatency),
		EWMAP99SchedulerLatency:     metric.NewGauge(smoothedP99SchedulerLatency),
	}
}

// MetricStruct implements the metric.Struct interface.
func (k *schedulerLatencyListenerMetrics) MetricStruct() {}
