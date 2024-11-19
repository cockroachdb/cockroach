// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package admission

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/logtags"
)

var _ metric.Struct = &schedulerLatencyListenerMetrics{}

type schedulerLatencyListener struct {
	ctx               context.Context
	elasticCPULimiter elasticCPULimiter
	coord             *ElasticCPUGrantCoordinator
	metrics           *schedulerLatencyListenerMetrics
	settings          *cluster.Settings

	testingParams *schedulerLatencyListenerParams
}

func newSchedulerLatencyListener(
	ambientCtx log.AmbientContext,
	st *cluster.Settings,
	metrics *schedulerLatencyListenerMetrics,
	e elasticCPULimiter,
) *schedulerLatencyListener {
	ctx := ambientCtx.AnnotateCtx(context.Background())
	ctx = logtags.AddTag(ctx, "scheduler-latency-listener", "")
	return &schedulerLatencyListener{
		ctx:               ctx,
		settings:          st,
		metrics:           metrics,
		elasticCPULimiter: e,
	}
}

func (e *schedulerLatencyListener) setCoord(coord *ElasticCPUGrantCoordinator) {
	e.coord = coord
}

// SchedulerLatency is part of the SchedulerLatencyListener interface. It
// controls the elastic CPU % limit based on scheduling latency and elastic CPU
// utilization data. Every tick we measure scheduling_p99 and execute the
// following:
//
//		IF scheduling_p99 > target_p99:
//			utilization_limit = max(utilization_limit – delta * factor, min_utilization)
//		ELSE:
//			IF requests_waiting:
//					utilization_limit = min(utilization_limit + delta, max_utilization)
//	       ELSE:
//				utilization_limit = max(utilization_limit – delta * inactive_factor, inactive_utilization)
//
// Definitions:
//
//	 scheduling_p99        Observed p99 scheduling latency a recent time window.
//	 target_p99            Target p99 scheduling latency.
//	 min_utilization       Floor on per-node elastic work CPU % utilization.
//	 max_utilization       Ceiling on per-node elastic work CPU % utilization.
//	 inactive_utilization  The CPU % utilization we decrease to when there's no utilization.
//	 delta                 Per-tick adjustment of CPU %.
//	 factor                Multiplicative factor for delta, used when decreasing utilization.
//	 inactive_factor       Multiplicative factor for delta, used when decreasing utilization when inactive.
//	 requests_waiting      Whether there are requests waiting due to insufficient utilization limit.
//	 utilization_limit     CPU % utilization limit for elastic work.
//
//	The controller uses fixed deltas for adjustments, adjusting down a bit more
//	aggressively than adjusting up. This is due to the nature of the work being
//	paced — we care more about quickly introducing a ceiling rather than
//	staying near it (though experimentally we’re able to stay near it just
//	fine). It adjusts upwards only when seeing waiting requests that could use
//	more quota (assuming it’s under the p99 target). The adjustments are small
//	to reduce {over,under}shoot and controller instability at the cost of being
//	somewhat dampened. We use a relatively long duration for measuring scheduler
//	latency data; since the p99 is computed off of histogram data, we saw a lot
//	more jaggedness when taking p99s off of a smaller set of scheduler events
//	(last 50ms for ex.) compared to computing p99s over a larger set of
//	scheduler events (last 2500ms). This, with the small deltas used for
//	adjustments, can make for a dampened response, but assuming a stable-ish
//	foreground CPU load against a node, it works fine. The controller output is
//	limited to a well-defined range that can be tuned through cluster settings.
//	This controller can be made more involved if we find good reasons for
//	it; this is just the first version that worked well-enough.
func (e *schedulerLatencyListener) SchedulerLatency(p99, period time.Duration) {
	params := e.getParams(period)
	if !params.enabled {
		return // nothing to do
	}

	e.metrics.P99SchedulerLatency.Update(p99.Nanoseconds())

	hasWaitingRequests := e.elasticCPULimiter.hasWaitingRequests()
	oldUtilizationLimit := e.elasticCPULimiter.getUtilizationLimit()
	newUtilizationLimit := oldUtilizationLimit

	if p99 > params.targetP99 { // over latency target; decrease limit
		newUtilizationLimit = oldUtilizationLimit -
			(params.adjustmentDelta * params.multiplicativeFactorOnDecrease)
		newUtilizationLimit = clamp(params.minUtilization, params.maxUtilization, newUtilizationLimit)
		if log.V(1) {
			log.Infof(e.ctx, "clamp(%0.2f%% - %0.2f%%) => %0.2f%%",
				100*oldUtilizationLimit, 100*params.adjustmentDelta*params.multiplicativeFactorOnDecrease,
				100*newUtilizationLimit)
		}
	} else { // under latency target
		if hasWaitingRequests { // increase limit if there are waiting requests
			newUtilizationLimit = oldUtilizationLimit + params.adjustmentDelta
			newUtilizationLimit = clamp(params.minUtilization, params.maxUtilization, newUtilizationLimit)
			if log.V(1) {
				log.Infof(e.ctx, "clamp(%0.2f%% + %0.2f%%) => %0.2f%%",
					100*oldUtilizationLimit, 100*params.adjustmentDelta,
					100*newUtilizationLimit)
			}
		} else { // unused limit; slowly decrease it
			inactiveUtilizationLimit := params.minUtilization +
				params.inactivePoint*(params.maxUtilization-params.minUtilization)
			if oldUtilizationLimit > inactiveUtilizationLimit {
				newUtilizationLimit = oldUtilizationLimit -
					(params.adjustmentDelta * params.multiplicativeFactorOnInactiveDecrease)
				newUtilizationLimit = clamp(inactiveUtilizationLimit, params.maxUtilization, newUtilizationLimit)
				if log.V(1) {
					log.Infof(e.ctx, "clamp(%0.2f%% - %0.2f%%) => %0.2f%% (inactive)",
						100*oldUtilizationLimit, 100*params.adjustmentDelta*params.multiplicativeFactorOnInactiveDecrease,
						100*newUtilizationLimit)
				}
			}
		}
	}

	e.elasticCPULimiter.setUtilizationLimit(newUtilizationLimit)
	e.elasticCPULimiter.computeUtilizationMetric()
	if e.coord != nil { // only nil in tests
		// TODO(irfansharif): Right now this is the only ticking mechanism for
		// elastic CPU grants; consider some form of explicit ticking instead.
		// We have this need for fine-granularity explicit ticking for the IO
		// tokens too, where the 250ms granularity is too coarse. Ideally a 1ms
		// granularity would be good. We've had problems with that in unloaded
		// systems, see the samplePeriod{Short,Long} logic goschedstats, so
		// maybe we can generalize that period switching into a struct where the
		// coarser period is used only when some func indicates that the
		// relevant "resource" is underloaded -- for goschedstats this resource
		// is the CPU, and for these token buckets it will be based on how many
		// tokens are still available.
		e.coord.tryGrant()
	}
}

func (e *schedulerLatencyListener) getParams(period time.Duration) schedulerLatencyListenerParams {
	if e.testingParams != nil {
		return *e.testingParams
	}

	enabled := elasticCPUControlEnabled.Get(&e.settings.SV)
	targetP99 := elasticCPUSchedulerLatencyTarget.Get(&e.settings.SV)
	minUtilization := elasticCPUMinUtilization.Get(&e.settings.SV)
	maxUtilization := elasticCPUMaxUtilization.Get(&e.settings.SV)
	if minUtilization > maxUtilization { // user error
		defaultMinUtilization := elasticCPUMinUtilization.Default()
		defaultMaxUtilization := elasticCPUMaxUtilization.Default()
		log.Errorf(e.ctx, "min utilization (%0.2f%%) > max utilization (%0.2f%%); resetting to defaults [%0.2f%%, %0.2f%%]",
			minUtilization*100, maxUtilization*100, defaultMinUtilization*100, defaultMaxUtilization*100,
		)
		minUtilization, maxUtilization = defaultMinUtilization, defaultMaxUtilization
	}
	inactivePoint := elasticCPUInactivePoint.Get(&e.settings.SV)
	adjustmentDeltaPerSecond := elasticCPUAdjustmentDeltaPerSecond.Get(&e.settings.SV)
	adjustmentDelta := adjustmentDeltaPerSecond * period.Seconds()
	multiplicativeFactorOnDecrease := elasticCPUMultiplicativeFactorOnDecrease.Get(&e.settings.SV)
	multiplicativeFactorOnInactiveDecrease := elasticCPUMultiplicativeFactorOnInactiveDecrease.Get(&e.settings.SV)

	return schedulerLatencyListenerParams{
		enabled:                                enabled,
		targetP99:                              targetP99,
		minUtilization:                         minUtilization,
		maxUtilization:                         maxUtilization,
		inactivePoint:                          inactivePoint,
		adjustmentDelta:                        adjustmentDelta,
		multiplicativeFactorOnDecrease:         multiplicativeFactorOnDecrease,
		multiplicativeFactorOnInactiveDecrease: multiplicativeFactorOnInactiveDecrease,
	}
}

type schedulerLatencyListenerParams struct {
	enabled                                bool
	targetP99                              time.Duration // target p99 scheduling latency
	minUtilization, maxUtilization         float64       // {floor,ceiling} on per-node CPU % utilization for elastic work
	inactivePoint                          float64       // point between {min,max} utilization we'll decrease to when inactive
	adjustmentDelta                        float64       // adjustment delta for CPU % limit applied elastic work
	multiplicativeFactorOnDecrease         float64       // multiplicative factor applied to additive delta when reducing limit
	multiplicativeFactorOnInactiveDecrease float64       // multiplicative factor applied to additive delta when reducing limit due to inactivity
}

var ( // cluster settings to control how elastic CPU % is adjusted
	elasticCPUMaxUtilization = settings.RegisterFloatSetting(
		settings.SystemOnly,
		"admission.elastic_cpu.max_utilization",
		"sets the ceiling on per-node elastic work CPU % utilization",
		0.75, // 75%
		settings.FloatInRange(0.05, 1.0),
	)

	elasticCPUMinUtilization = settings.RegisterFloatSetting(
		settings.SystemOnly,
		"admission.elastic_cpu.min_utilization",
		"sets the floor on per-node elastic work CPU % utilization",
		0.05, // 5%
		settings.FloatInRange(0.01, 1.0),
	)

	elasticCPUInactivePoint = settings.RegisterFloatSetting(
		settings.SystemOnly,
		"admission.elastic_cpu.inactive_point",
		"the point between {min,max}_utilization the CPU % decreases to when there's no elastic work",
		0.10, // 10% of the way between {min,max} utilization -- 12% if [min,max] = [5%,75%]
		settings.Fraction,
	)

	elasticCPUAdjustmentDeltaPerSecond = settings.RegisterFloatSetting(
		settings.SystemOnly,
		"admission.elastic_cpu.adjustment_delta_per_second",
		"sets the per-second % adjustment used when when adapting elastic work CPU %s",
		0.001, // 0.1%, takes 10s to add 1% to elastic CPU limit
		settings.FloatInRange(0.0001, 1.0),
	)

	elasticCPUMultiplicativeFactorOnDecrease = settings.RegisterFloatSetting(
		settings.SystemOnly,
		"admission.elastic_cpu.multiplicative_factor_on_decrease",
		"sets the multiplier on negative adjustments to elastic work CPU %",
		2, // 2 * 0.1%, takes 5s to subtract 1% from elastic CPU limit
	)

	elasticCPUMultiplicativeFactorOnInactiveDecrease = settings.RegisterFloatSetting(
		settings.SystemOnly,
		"admission.elastic_cpu.multiplicative_factor_on_inactive_decrease",
		"sets the multiplier on negative adjustments to elastic work CPU % when inactive",
		0.25, // 0.25 * 0.1%, takes 40s to subtract 1% from elastic CPU limit
	)

	elasticCPUSchedulerLatencyTarget = settings.RegisterDurationSetting(
		settings.SystemOnly,
		"admission.elastic_cpu.scheduler_latency_target",
		"sets the p99 scheduling latency the elastic CPU controller aims for",
		time.Millisecond,
		settings.DurationInRange(50*time.Microsecond, time.Second),
	)
)

var (
	p99SchedulerLatency = metric.Metadata{
		Name:        "admission.scheduler_latency_listener.p99_nanos",
		Help:        "The scheduling latency at p99 as observed by the scheduler latency listener",
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
	}
)

// schedulerLatencyListenerMetrics are the metrics associated with an instance
// of the schedulerLatencyListener.
type schedulerLatencyListenerMetrics struct {
	P99SchedulerLatency *metric.Gauge
}

func makeSchedulerLatencyListenerMetrics() *schedulerLatencyListenerMetrics {
	return &schedulerLatencyListenerMetrics{
		P99SchedulerLatency: metric.NewGauge(p99SchedulerLatency),
	}
}

// MetricStruct implements the metric.Struct interface.
func (k *schedulerLatencyListenerMetrics) MetricStruct() {}

func clamp(min, max, val float64) float64 {
	if buildutil.CrdbTestBuild && min > max {
		log.Fatalf(context.Background(), "min (%f) > max (%f)", min, max)
	}
	if val < min {
		val = min // floor
	}
	if val > max {
		val = max // ceiling
	}
	return val
}
