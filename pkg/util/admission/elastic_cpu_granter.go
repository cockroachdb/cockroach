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
	"runtime"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
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
		tb               *quotapool.TokenBucket
		utilizationLimit float64
	}
	requester requester
	metrics   *elasticCPUGranterMetrics
}

var _ granter = &elasticCPUGranter{}

func newElasticCPUGranter(
	ambientCtx log.AmbientContext, st *cluster.Settings, metrics *elasticCPUGranterMetrics,
) *elasticCPUGranter {
	tokenBucket := &quotapool.TokenBucket{}
	tokenBucket.Init(0, 0, timeutil.DefaultTimeSource{})
	return newElasticCPUGranterWithTokenBucket(ambientCtx, st, metrics, tokenBucket)
}

func newElasticCPUGranterWithTokenBucket(
	ambientCtx log.AmbientContext,
	st *cluster.Settings,
	metrics *elasticCPUGranterMetrics,
	tokenBucket *quotapool.TokenBucket,
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

	granted, _ = e.mu.tb.TryToFulfill(quotapool.Tokens(count))
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

	e.mu.tb.Adjust(quotapool.Tokens(count))
}

// tookWithoutPermission implements granter.
func (e *elasticCPUGranter) tookWithoutPermission(count int64) {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.mu.tb.Adjust(quotapool.Tokens(-count))
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
	e.mu.tb.UpdateConfig(quotapool.TokensPerSecond(rate), quotapool.Tokens(rate))

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

	currentCumAcquiredNanos := e.metrics.AcquiredNanos.Count()
	currentCumReturnedNanos := e.metrics.ReturnedNanos.Count()
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

	// elasticCPUMaxAvailableNanos is a static metric, useful for computing the
	// % utilization: (acquired - returned)/max available.
	elasticCPUMaxAvailableNanos = metric.Metadata{
		Name:        "admission.elastic_cpu.max_available_nanos",
		Help:        "Maximum available CPU nanoseconds per second ignoring utilization limit",
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
	}

	// TODO(irfansharif): Surface this metric in the "Overload" dashboard.
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
	AcquiredNanos     *metric.Counter
	ReturnedNanos     *metric.Counter
	MaxAvailableNanos *metric.Counter
	UtilizationLimit  *metric.GaugeFloat64

	Utilization      *metric.GaugeFloat64 // updated every elasticCPUUtilizationMetricInterval, using fields below
	everyInterval    util.EveryN
	lastCumUsedNanos int64
}

const elasticCPUUtilizationMetricInterval = 10 * time.Second

func makeElasticCPUGranterMetrics() *elasticCPUGranterMetrics {
	metrics := &elasticCPUGranterMetrics{
		AcquiredNanos:     metric.NewCounter(elasticCPUAcquiredNanos),
		ReturnedNanos:     metric.NewCounter(elasticCPUReturnedNanos),
		MaxAvailableNanos: metric.NewCounter(elasticCPUMaxAvailableNanos),
		Utilization:       metric.NewGaugeFloat64(elasticCPUGranterUtilization),
		UtilizationLimit:  metric.NewGaugeFloat64(elasticCPUGranterUtilizationLimit),
		everyInterval:     util.Every(elasticCPUUtilizationMetricInterval),
	}

	metrics.MaxAvailableNanos.Inc(int64(runtime.GOMAXPROCS(0)) * time.Second.Nanoseconds())
	return metrics
}

// MetricStruct implements the metric.Struct interface.
func (k *elasticCPUGranterMetrics) MetricStruct() {}

var _ metric.Struct = &elasticCPUGranterMetrics{}
