// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package admission

import (
	"context"
	"runtime"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/tokenbucket"
)

// We don't want the ability for an admitted a request to be able to run
// indefinitely consuming arbitrary CPU. For long-running (~1s of CPU work per
// request) "elastic" (not latency sensitive) work like backups, this can have
// detrimental effects on foreground latencies – once such work is admitted, it
// can take up available CPU cores until completion, which prevents foreground
// work from running. The pieces here aim to improve this behavior; there are
// two components in play:
//
// - A token bucket that hands out slices of CPU time where the total amount
//   handed out is determined by a 'target utilization' – the max % of CPU it's
//   aiming to use (on a 8vCPU machine, if targeting 50% CPU, it can hand out
//   .50 * 8 = 4 seconds of CPU time per second).
// - A feedback controller that adjusts the CPU % used by the token bucket
//   periodically by measuring scheduling latency[1]. If over the limit (1ms at
//   p99, chosen experimentally), the % is reduced; if under the limit and we're
//   seeing substantial utilization, the % is increased.
//
// Elastic work acquires CPU tokens representing some predetermined slice of CPU
// time, blocking until these tokens become available. We found that 100ms of
// tokens work well enough experimentally. A larger value, say 250ms, would
// translate to less preemption and fewer RPCs. What's important is that it
// isn't "too much", like 2s of CPU time, since that would let a single request
// hog a core potentially for 2s and allow for a large build up of a runnable
// goroutines (serving foreground traffic) on that core, affecting
// scheduling/foreground latencies.
//
// The work preempts itself once the slice is used up (as a form of cooperative
// scheduling). Once preempted, the request returns to the caller with a
// resumption key. This scheme is effective in clamping down on scheduling
// latency that's due an excessive amount of elastic work. We have proof from
// direct trace captures and instrumentation that reducing scheduling latencies
// directly translates to reduced foreground latencies. They're primarily felt
// when straddling goroutines, typically around RPC boundaries (request/response
// handling goroutines); the effects multiplicative for statements that issue
// multiple requests.
//
// The controller uses fixed deltas for adjustments, adjusting down a bit more
// aggressively than adjusting up. This is due to the nature of the work being
// paced — we care more about quickly introducing a ceiling rather than staying
// near it (though experimentally we’re able to stay near it just fine). It
// adjusts upwards only when seeing a reasonably high % of utilization with the
// allotted CPU quota (assuming it’s under the p99 target). The adjustments are
// small to reduce {over,under}shoot and controller instability at the cost of
// being somewhat dampened. We use a smoothed form of the p99 latency captures
// to add stability to the controller input, which consequently affects the
// controller output. We use a relatively low frequency when sampling scheduler
// latencies; since the p99 is computed off of histogram data, we saw a lot more
// jaggedness when taking p99s off of a smaller set of scheduler events (every
// 50ms for ex.) compared to computing p99s over a larger set of scheduler
// events (every 2500ms). This, with the small deltas used for adjustments, can
// make for a dampened response, but assuming a stable-ish foreground CPU load
// against a node, it works fine. The controller output is limited to a
// well-defined range that can be tuned through cluster settings.
//
// [1]: Specifically the time between a goroutine being ready to run and when
//      it's scheduled to do so by the Go scheduler.

// elasticCPUGranter is used to limit CPU utilization for elastic work (not
// latency sensitive and can be throttled). This form of control helps clamp
// down on scheduling latencies observed by non-elastic work due to excessive
// CPU use by elastic work.
//
// This is modeled as a token bucket that fills with allotted CPU time and
// drains with used CPU time, for and by elastic work. This form of limiter is
// most applicable to CPU intensive elastic work that's somewhat long-running
// (10s of ms, typically embedded within is a tight loop).
//
//	allotted CPU time per-second = target CPU utilization * # of processors
//
// NB: This granter is slightly differing from the other ones in the admission
// package in that if the number of tokens are > 0 they will grant and let the
// tokens go into debt (though penalizing future requests, so this difference
// doesn't actually matter).
type elasticCPUGranter struct {
	ctx context.Context
	mu  struct {
		// NB: there's no lock ordering between this mutex and the one in
		// WorkQueue; neither holds a mutex while calling the other. This is
		// different from the other granters: the granters that are used by
		// GrantCoordinator don't have their own mutex, and rely on the one in
		// GrantCoordinator, which is ordered before the one in WorkQueue, since
		// requester.granted() is called while holding GrantCoordinator.mu.
		syncutil.Mutex
		tb               *tokenbucket.TokenBucket
		tbLastReset      time.Time
		utilizationLimit float64
	}
	requester requester
	metrics   *elasticCPUGranterMetrics
}

var _ granter = &elasticCPUGranter{}

func newElasticCPUGranter(
	ambientCtx log.AmbientContext, st *cluster.Settings, metrics *elasticCPUGranterMetrics,
) *elasticCPUGranter {
	tokenBucket := &tokenbucket.TokenBucket{}
	tokenBucket.InitWithNowFn(0, 0, timeutil.Now)
	return newElasticCPUGranterWithTokenBucket(ambientCtx, st, metrics, tokenBucket)
}

func newElasticCPUGranterWithTokenBucket(
	ambientCtx log.AmbientContext,
	st *cluster.Settings,
	metrics *elasticCPUGranterMetrics,
	tokenBucket *tokenbucket.TokenBucket,
) *elasticCPUGranter {
	e := &elasticCPUGranter{
		ctx:     ambientCtx.AnnotateCtx(context.Background()),
		metrics: metrics,
	}
	e.mu.tb = tokenBucket
	e.setUtilizationLimit(elasticCPUMinUtilization.Get(&st.SV))
	return e
}

func (e *elasticCPUGranter) setRequester(requester requester) {
	e.requester = requester
}

// grantKind implements granter.
func (e *elasticCPUGranter) grantKind() grantKind {
	return token
}

// tryGet implements granter.
func (e *elasticCPUGranter) tryGet(count int64) (granted bool) {
	e.mu.Lock()
	defer e.mu.Unlock()

	granted, _ = e.mu.tb.TryToFulfill(tokenbucket.Tokens(count))
	return granted
}

// returnGrant implements granter.
func (e *elasticCPUGranter) returnGrant(count int64) {
	e.returnGrantWithoutGrantingElsewhere(count)
	e.tryGrant()
}

func (e *elasticCPUGranter) returnGrantWithoutGrantingElsewhere(count int64) {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.mu.tb.Adjust(tokenbucket.Tokens(count))
}

// tookWithoutPermission implements granter.
func (e *elasticCPUGranter) tookWithoutPermission(count int64) {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.mu.tb.Adjust(tokenbucket.Tokens(-count))
}

// continueGrantChain implements granter.
func (e *elasticCPUGranter) continueGrantChain(grantChainID) {
	// Ignore since grant chains are not used for elastic CPU tokens.
}

// tryGrant is used to attempt to grant to waiting requests.
func (e *elasticCPUGranter) tryGrant() {
	for e.requester.hasWaitingRequests() && e.tryGet(1) {
		tokens := e.requester.granted(noGrantChain)
		if tokens == 0 {
			e.returnGrantWithoutGrantingElsewhere(1)
			return // requester didn't accept, nothing left to do; bow out
		} else if tokens > 1 {
			e.tookWithoutPermission(tokens - 1)
		}
	}
}

var _ elasticCPULimiter = &elasticCPUGranter{}

// setTargetUtilization is part of the elasticCPULimiter interface.
func (e *elasticCPUGranter) setUtilizationLimit(utilizationLimit float64) {
	e.mu.Lock()
	defer e.mu.Unlock()

	// Our rate limiter rate and burst limits are the same, are computed using:
	//
	//   allotted CPU time per-second = target CPU utilization * # of processors
	//
	rate := utilizationLimit * float64(int64(runtime.GOMAXPROCS(0))*time.Second.Nanoseconds())
	e.mu.utilizationLimit = utilizationLimit
	e.mu.tb.UpdateConfig(tokenbucket.TokensPerSecond(rate), tokenbucket.Tokens(rate))
	if now := timeutil.Now(); now.Sub(e.mu.tbLastReset) > 15*time.Second { // TODO(irfansharif): make this is a cluster setting?
		// Periodically reset the token bucket. This is just defense-in-depth
		// and at worst, over-admits. We've seen production clusters where the
		// token bucket was severely in debt and caused wait queue times of
		// minutes, which can be long enough to fail backups completely
		// (#102817).
		e.mu.tb.Reset()
		e.mu.tbLastReset = now
	}
	e.metrics.NanosExhaustedDuration.Update(e.mu.tb.Exhausted().Microseconds())
	e.metrics.AvailableNanos.Update(int64(e.mu.tb.Available()))

	e.metrics.UtilizationLimit.Update(utilizationLimit)
	if log.V(1) {
		log.Infof(e.ctx, "elastic cpu granter refill rate = %0.4f cpu seconds per second (utilization across %d procs = %0.2f%%)",
			time.Duration(rate).Seconds(), runtime.GOMAXPROCS(0), utilizationLimit*100)
	}
}

// getTargetUtilization is part of the elasticCPULimiter interface.
func (e *elasticCPUGranter) getUtilizationLimit() float64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.mu.utilizationLimit
}

// hasWaitingRequests is part of the elasticCPULimiter interface.
func (e *elasticCPUGranter) hasWaitingRequests() bool {
	return e.requester.hasWaitingRequests()
}

// computeUtilizationMetric is part of the elasticCPULimiter interface.
func (e *elasticCPUGranter) computeUtilizationMetric() {
	if !e.metrics.everyInterval.ShouldProcess(timeutil.Now()) {
		return // nothing to do
	}

	// NB: Read the returned-nanos atomic before the acquired-nanos one, to
	// avoid spurious negative values given these we don't read these two under
	// a mutex. It's still possible for returned-nanos > acquired-nanos,
	// resulting in a negative utilization -- it needs to be investigated
	// (#103359).
	currentCumReturnedNanos := e.metrics.ReturnedNanos.Count()
	currentCumAcquiredNanos := e.metrics.AcquiredNanos.Count()
	currentCumUsedNanos := currentCumAcquiredNanos - currentCumReturnedNanos

	if e.metrics.lastCumUsedNanos != 0 {
		intervalUsedNanos := currentCumUsedNanos - e.metrics.lastCumUsedNanos
		intervalUsedPercent := float64(intervalUsedNanos) /
			(float64(e.metrics.MaxAvailableNanos.Count()) * elasticCPUUtilizationMetricInterval.Seconds())
		e.metrics.Utilization.Update(intervalUsedPercent)
		e.metrics.lastCumUsedNanos = currentCumUsedNanos
	}
	e.metrics.lastCumUsedNanos = currentCumUsedNanos
}

// TODO(irfansharif): Provide separate enums for different elastic CPU token
// sizes? (1ms, 10ms, 100ms). Write up something about picking the right value.
// Can this value be auto-estimated?

var ( // granter-side metrics (some of these have parallels on the requester side, but are still useful to have)
	elasticCPUAcquiredNanos = metric.Metadata{
		Name:        "admission.elastic_cpu.acquired_nanos",
		Help:        "Total CPU nanoseconds acquired by elastic work",
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
	}

	elasticCPUReturnedNanos = metric.Metadata{
		Name:        "admission.elastic_cpu.returned_nanos",
		Help:        "Total CPU nanoseconds returned by elastic work",
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
	}

	elasticCPUPreWorkNanos = metric.Metadata{
		Name:        "admission.elastic_cpu.pre_work_nanos",
		Help:        "Total CPU nanoseconds spent doing pre-work, before doing elastic work",
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
	}

	// elasticCPUMaxAvailableNanos is a static metric, useful for computing the
	// % utilization: (acquired - returned)/max available.
	elasticCPUMaxAvailableNanos = metric.Metadata{
		Name:        "admission.elastic_cpu.max_available_nanos",
		Help:        "Maximum available CPU nanoseconds per second ignoring utilization limit",
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
	}

	elasticCPUNanosExhaustedDuration = metric.Metadata{
		Name:        "admission.elastic_cpu.nanos_exhausted_duration",
		Help:        "Total duration when elastic CPU nanoseconds were exhausted, in micros",
		Measurement: "Microseconds",
		Unit:        metric.Unit_COUNT,
	}

	elasticCPUOverLimitDurations = metric.Metadata{
		Name:        "admission.elastic_cpu.over_limit_durations",
		Help:        "Measurement of how much over the prescribed limit elastic requests ran (not recorded if requests don't run over)",
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
	}

	elasticCPUAvailableNanos = metric.Metadata{
		Name:        "admission.elastic_cpu.available_nanos",
		Help:        "Instantaneous available CPU nanoseconds per second ignoring utilization limit",
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
	}

	elasticCPUGranterUtilization = metric.Metadata{
		Name:        "admission.elastic_cpu.utilization",
		Help:        "CPU utilization by elastic work",
		Measurement: "CPU Time",
		Unit:        metric.Unit_PERCENT,
	}

	elasticCPUGranterUtilizationLimit = metric.Metadata{
		Name:        "admission.elastic_cpu.utilization_limit",
		Help:        "Utilization limit set for the elastic CPU work",
		Measurement: "CPU Time",
		Unit:        metric.Unit_PERCENT,
	}
)

// elasticCPUGranterMetrics are the metrics associated with an instance of the
// ElasticCPUGranter.
type elasticCPUGranterMetrics struct {
	AcquiredNanos          *metric.Counter
	ReturnedNanos          *metric.Counter
	PreWorkNanos           *metric.Counter
	MaxAvailableNanos      *metric.Counter
	AvailableNanos         *metric.Gauge
	UtilizationLimit       *metric.GaugeFloat64
	NanosExhaustedDuration *metric.Gauge
	OverLimitDuration      metric.IHistogram

	Utilization      *metric.GaugeFloat64 // updated every elasticCPUUtilizationMetricInterval, using fields below
	everyInterval    util.EveryN
	lastCumUsedNanos int64
}

const elasticCPUUtilizationMetricInterval = 10 * time.Second

func makeElasticCPUGranterMetrics() *elasticCPUGranterMetrics {
	metrics := &elasticCPUGranterMetrics{
		AcquiredNanos:          metric.NewCounter(elasticCPUAcquiredNanos),
		ReturnedNanos:          metric.NewCounter(elasticCPUReturnedNanos),
		PreWorkNanos:           metric.NewCounter(elasticCPUPreWorkNanos),
		MaxAvailableNanos:      metric.NewCounter(elasticCPUMaxAvailableNanos),
		AvailableNanos:         metric.NewGauge(elasticCPUAvailableNanos),
		NanosExhaustedDuration: metric.NewGauge(elasticCPUNanosExhaustedDuration),
		OverLimitDuration: metric.NewHistogram(metric.HistogramOptions{
			Mode:         metric.HistogramModePrometheus,
			Metadata:     elasticCPUOverLimitDurations,
			Duration:     base.DefaultHistogramWindowInterval(),
			BucketConfig: metric.IOLatencyBuckets,
		}),
		Utilization:      metric.NewGaugeFloat64(elasticCPUGranterUtilization),
		UtilizationLimit: metric.NewGaugeFloat64(elasticCPUGranterUtilizationLimit),
		everyInterval:    util.Every(elasticCPUUtilizationMetricInterval),
	}

	metrics.MaxAvailableNanos.Inc(int64(runtime.GOMAXPROCS(0)) * time.Second.Nanoseconds())
	return metrics
}

// MetricStruct implements the metric.Struct interface.
func (k *elasticCPUGranterMetrics) MetricStruct() {}

var _ metric.Struct = &elasticCPUGranterMetrics{}
