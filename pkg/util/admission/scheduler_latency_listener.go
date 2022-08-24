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
	"github.com/cockroachdb/logtags"
)

var _ metric.Struct = &schedulerLatencyListenerMetrics{}

// TODO(irfansharif): There’s some discrepancy between what this struct observes
// as p99 scheduling latencies and what prometheus/client_golang computes. Worth
// investigating.

type schedulerLatencyListener struct {
	ctx                context.Context
	ewmaP99            float64
	elasticCPUAdjuster elasticCPUUtilizationAdjuster
	coord              *ElasticCPUGrantCoordinator
	metrics            *schedulerLatencyListenerMetrics
	settings           *cluster.Settings

	testingParams *schedulerLatencyListenerParams
}

func newSchedulerLatencyListener(
	ambientCtx log.AmbientContext,
	st *cluster.Settings,
	metrics *schedulerLatencyListenerMetrics,
	e elasticCPUUtilizationAdjuster,
) *schedulerLatencyListener {
	ctx := ambientCtx.AnnotateCtx(context.Background())
	ctx = logtags.AddTag(ctx, "scheduler-latency-listener", "")
	return &schedulerLatencyListener{
		ctx:                ctx,
		settings:           st,
		metrics:            metrics,
		elasticCPUAdjuster: e,
	}
}

func (e *schedulerLatencyListener) setCoord(coord *ElasticCPUGrantCoordinator) {
	e.coord = coord
}

// SchedulerLatency is part of the SchedulerLatencyListener interface. It
// controls the elastic CPU % limit based on scheduling latency and elastic CPU
// utilization data. The controller behaves as follows.
//
//   Every scheduler_latency_sample_period, measure scheduling_p99 and execute
//   the following:
//
//   	ewma_p99 = ewma_c * scheduling_p99 + (1 - ewma_c) * ewma_p99
//   	IF ewma_p99 > target_p99 AND observed_cpu_utilization > min_utilization:
//   		utilization_limit = max(utilization_limit – delta * factor, min_utilization)
//   	ELSE IF observed_cpu_utilization / utilization_limit > min_utilization_fraction:
//			utilization_limit = min(utilization_limit + delta, max_utilization)
//
// Where the input parameters are:
// - scheduler_latency_sample_period: Inverse of how frequently the CPU
//   scheduler's latencies are sampled.
// - scheduling_p99: Observed p99 scheduling latency over the last
//   scheduler_latency_sample_period.
// - ewma_c: Recency bias for EWMA smoothing of scheduling_p99.
// - target_p99: Target p99 scheduling latency.
// - observed_cpu_utilization: Observed CPU % attributed to elastic work.
// - min_utilization: Floor on per-node elastic work CPU % utilization
// - max_utilization: Ceiling on per-node elastic work CPU % utilization
// - delta: Additive adjustment of CPU %.
// - factor: Multiplicative factor for delta, used when decreasing utilization
// - min_utilization_fraction: Minimum utilization of CPU limit before raising
//   the limit
//
// And the output parameter is:
// - utilization_limit: CPU % utilization limit for elastic work
//
func (e *schedulerLatencyListener) SchedulerLatency(p99, period time.Duration) {
	if !e.enabled() {
		return // nothing to do
	}

	params := e.getParams(period)
	e.ewmaP99 = params.ewmaConstant*float64(p99.Nanoseconds()) + (1-params.ewmaConstant)*e.ewmaP99
	e.metrics.InstantaneousP99SchedulerLatency.Update(p99.Nanoseconds())
	e.metrics.EWMAP99SchedulerLatency.Update(int64(e.ewmaP99))

	// TODO(irfansharif): Does utilization need to be smoothed
	// out? Ripped out entirely?
	utilization := e.elasticCPUAdjuster.getUtilization()
	currentUtilizationLimit := e.elasticCPUAdjuster.getUtilizationLimit()

	if int64(e.ewmaP99) > params.targetP99.Nanoseconds() && utilization > params.minUtilization { // over latency target; decrease limit
		newUtilizationLimit := currentUtilizationLimit - (params.additiveDelta * params.multiplicativeFactorOnDecrease)
		if newUtilizationLimit < params.minUtilization {
			newUtilizationLimit = params.minUtilization // floor
		}
		if log.V(1) {
			log.Infof(e.ctx, "clamp(%0.2f%% - %0.2f%%) => %0.2f%%",
				100*currentUtilizationLimit, 100*params.additiveDelta*params.multiplicativeFactorOnDecrease,
				100*newUtilizationLimit)
		}
		e.elasticCPUAdjuster.setUtilizationLimit(newUtilizationLimit)
	} else { // under latency target; increase limit
		if utilizationFraction := utilization / currentUtilizationLimit; utilizationFraction >= params.utilizationFractionForAdditionalCPU {
			newUtilizationLimit := currentUtilizationLimit + params.additiveDelta
			if newUtilizationLimit > params.maxUtilization {
				newUtilizationLimit = params.maxUtilization // ceiling
			}
			if log.V(1) {
				log.Infof(e.ctx, "clamp(%0.2f%% + %0.2f%%) => %0.2f%%",
					100*currentUtilizationLimit, 100*params.additiveDelta,
					100*newUtilizationLimit)
			}
			e.elasticCPUAdjuster.setUtilizationLimit(newUtilizationLimit)
		} else {
			if log.V(1) {
				log.Infof(e.ctx, "insufficient utilization %0.2f%% / %0.2f%% = %0.2f (< %0.2f)",
					100*utilization, 100*currentUtilizationLimit, utilizationFraction,
					params.utilizationFractionForAdditionalCPU)
			}
		}
	}

	if e.coord != nil { // only nil in tests
		e.coord.tryGrant()
	}
}

func (e *schedulerLatencyListener) getParams(period time.Duration) schedulerLatencyListenerParams {
	if e.testingParams != nil {
		return *e.testingParams
	}

	ewmaConstant := elasticCPUGranterEWMAConstant.Get(&e.settings.SV)
	targetP99 := elasticCPUGranterSchedulerLatencyTarget.Get(&e.settings.SV)
	minUtilization := elasticCPUGranterMinUtilization.Get(&e.settings.SV)
	maxUtilization := elasticCPUGranterMaxUtilization.Get(&e.settings.SV)
	additiveDeltaPerSecond := elasticCPUGranterAdditiveDeltaPerSecond.Get(&e.settings.SV)
	additiveDelta := (additiveDeltaPerSecond * float64(period.Milliseconds())) / float64(time.Second.Milliseconds())
	multiplicativeFactorOnDecrease := elasticCPUGranterMultiplicativeFactorOnDecrease.Get(&e.settings.SV)
	utilizationFractionForAdditionalCPU := elasticCPUGranterUtilizationFractionForAdditionalCPU.Get(&e.settings.SV)

	return schedulerLatencyListenerParams{
		ewmaConstant:                        ewmaConstant,
		targetP99:                           targetP99,
		minUtilization:                      minUtilization,
		maxUtilization:                      maxUtilization,
		additiveDelta:                       additiveDelta,
		multiplicativeFactorOnDecrease:      multiplicativeFactorOnDecrease,
		utilizationFractionForAdditionalCPU: utilizationFractionForAdditionalCPU,
	}
}

func (e *schedulerLatencyListener) enabled() bool {
	if e.testingParams != nil {
		return e.testingParams.enabled
	}
	return elasticCPUControlEnabled.Get(&e.settings.SV)
}

type schedulerLatencyListenerParams struct {
	enabled                             bool
	ewmaConstant                        float64       // ewma constant for scheduler latency smoothing
	targetP99                           time.Duration // target p99 scheduling latency
	minUtilization, maxUtilization      float64       // {floor,ceiling} on per-node CPU % utilization for elastic work
	additiveDelta                       float64       // adjustment delta for CPU % limit applied elastic work
	multiplicativeFactorOnDecrease      float64       // multiplicative factor applied to additiveDelta when reducing limit
	utilizationFractionForAdditionalCPU float64       // minimum utilization of CPU limit needed before raising the limit
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

	elasticCPUGranterMultiplicativeFactorOnDecrease = settings.RegisterFloatSetting(
		settings.SystemOnly,
		"elastic_cpu_granter.multiplicative_factor_on_decrease",
		"sets the multiplier on negative adjustments to elastic work CPU %",
		2, // 2 * 0.1%, takes 5s to subtract 1% from elastic CPU limit.
	)

	// TODO(irfansharif): This setting is flawed. It can make for a very slow
	// rise (the observed utilization value is not smoothed) or be altogether
	// unreactive when operating at low elastic CPU % limits. Consider if the
	// utilization limit is at 1% in an 8vCPU machine. The burst capacity of the
	// token bucket = 1% * 8s = 80ms. If we're relying on observing 90% of that
	// value being in use, i.e. 72ms to have been acquired by elastic work, this
	// is simply not possible when the smallest unit of acquisition is larger,
	// say 100ms. (We can check for this explicitly since we know what the
	// largest unit of acquisition is and have visibility into how many tokens
	// we have left.)
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
	InstantaneousP99SchedulerLatency *metric.Gauge
	EWMAP99SchedulerLatency          *metric.Gauge
}

func makeSchedulerLatencyListenerMetrics() *schedulerLatencyListenerMetrics {
	return &schedulerLatencyListenerMetrics{
		InstantaneousP99SchedulerLatency: metric.NewGauge(instantaneousP99SchedulerLatency),
		EWMAP99SchedulerLatency:          metric.NewGauge(smoothedP99SchedulerLatency),
	}
}

// MetricStruct implements the metric.Struct interface.
func (k *schedulerLatencyListenerMetrics) MetricStruct() {}
