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

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// XXX: Need a top-level disabled setting and hooked into the right integration
// points.
// XXX: Need tests for everything.

// elasticCPUGranter is used to limit CPU utilization for elastic work (not
// latency sensitive and can be throttled). This form of control helps clamp
// down on scheduling latencies observed by non-elastic work due to excessive
// CPU use by elastic work. Examples of such work include export processing,
// pebble compactions, MVCC GC and consistency queue activity.
//
// This is modeled as a token bucket that fills with allotted CPU time and
// drains with used CPU time, for and by elastic work. This form of limiter is
// most applicable to CPU intensive elastic work that's somewhat long-running
// (10s of ms, typically embedded within is a tight loop).
//
//   allotted CPU time per-second = target CPU utilization * # of processors
//
// XXX: Document. We don't know upfront what the CPU cost will be, but we take
// something -- seeking admission. The smallest divisible unit of work. We
// do work up until that point, and re-seek. The empty portion of the bucket
// is the elastic work utilization we're driving ("in-use"). This is the
// same as returning a slot after a fixed period of time, and the slots
// being granted only at a fixed rate. Rather, only a fixed number of slots
// are allowed to be in-use at any point in time. TODO think more. TODO
// document.
type elasticCPUGranter struct {
	mu struct {
		syncutil.Mutex
		targetUtilization float64
	}
	rl      *quotapool.RateLimiter
	metrics *elasticCPUGranterMetrics
}

var _ granter = &elasticCPUGranter{}

func newElasticCPUGranter(
	st *cluster.Settings, metrics *elasticCPUGranterMetrics,
) *elasticCPUGranter {
	rateLimiter := quotapool.NewRateLimiter(
		"elastic-cpu-granter",
		0, 0,
		quotapool.OnAcquisition(func(ctx context.Context, _ string, r quotapool.Request, start time.Time) {
			metrics.Acquisitions.Inc(1)
		}),
		quotapool.OnWaitStart(func(ctx context.Context, poolName string, r quotapool.Request) {
			metrics.Waiters.Inc(1)
		}),
		quotapool.OnWaitFinish(func(ctx context.Context, poolName string, r quotapool.Request, start time.Time) {
			metrics.Waiters.Dec(1)
			metrics.WaitingNanos.Inc(timeutil.Since(start).Nanoseconds())
		}),
	)
	e := &elasticCPUGranter{
		rl:      rateLimiter,
		metrics: metrics,
	}
	metrics.ObservedUtilization = metric.NewFunctionalGaugeFloat64(elasticCPUGranterObservedUtilization, func() float64 {
		return e.getObservedUtilization()
	})
	e.setTargetUtilization(elasticCPUGranterInjectedTargetUtilization.Get(&st.SV))
	elasticCPUGranterInjectedTargetUtilization.SetOnChange(&st.SV, func(ctx context.Context) {
		e.setTargetUtilization(elasticCPUGranterInjectedTargetUtilization.Get(&st.SV))
	})
	return e
}

// grantKind implements granter.
func (e *elasticCPUGranter) grantKind() grantKind {
	return token
}

// tryGet implements granter.
func (e *elasticCPUGranter) tryGet(count int64) (granted bool) {
	return e.rl.AdmitN(count)
}

// returnGrant implements granter.
func (e *elasticCPUGranter) returnGrant(count int64) {
	e.rl.Adjust(quotapool.Tokens(count))
}

// tookWithoutPermission implements granter.
func (e *elasticCPUGranter) tookWithoutPermission(count int64) {
	e.rl.Adjust(quotapool.Tokens(-count))
}

// continueGrantChain implements granter.
func (e *elasticCPUGranter) continueGrantChain(grantChainID) {
	// Ignore since grant chains are not used for elastic CPU tokens.
}

// TODO(irfansharif): Provide separate enums for different elastic CPU token
// sizes? (1ms, 10ms, 100ms). Write up something about picking the right value.
// Can this value be auto-estimated?

var (
	elasticCPUAcquisitions = metric.Metadata{
		Name:        "admission.elastic_cpu.acquisitions",
		Help:        "Total CPU token acquisitions by elastic work",
		Measurement: "Count",
		Unit:        metric.Unit_COUNT,
	}

	elasticCPUWaiters = metric.Metadata{
		Name:        "admission.elastic_cpu.waiters",
		Help:        "Current number of waiters for elastic CPU tokens",
		Measurement: "Count",
		Unit:        metric.Unit_COUNT,
	}

	elasticCPUWaitingNanos = metric.Metadata{
		Name:        "admission.elastic_cpu.waiting_nanos",
		Help:        "Total nanoseconds spent waiting for elastic CPU tokens",
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
	}

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

	elasticCPUGranterTargetUtilization = metric.Metadata{
		Name:        "admission.elastic_cpu_granter.target_utilization",
		Help:        "Target utilization set for the elastic CPU granter",
		Measurement: "CPU Time",
		Unit:        metric.Unit_PERCENT,
	}

	elasticCPUGranterObservedUtilization = metric.Metadata{
		Name:        "admission.elastic_cpu_granter.observed_utilization",
		Help:        "Utilization observed by the elastic CPU granter",
		Measurement: "CPU Time",
		Unit:        metric.Unit_PERCENT,
	}
)

// elasticCPUGranterMetrics are the metrics associated with an instance of the
// ElasticCPUGranter.
type elasticCPUGranterMetrics struct {
	Waiters             *metric.Gauge
	WaitingNanos        *metric.Counter
	AcquiredNanos       *metric.Counter
	ReturnedNanos       *metric.Counter
	Acquisitions        *metric.Counter
	TargetUtilization   *metric.GaugeFloat64
	ObservedUtilization *metric.GaugeFloat64
}

func makeElasticCPUGranterMetrics() *elasticCPUGranterMetrics {
	return &elasticCPUGranterMetrics{
		Waiters:           metric.NewGauge(elasticCPUWaiters),
		WaitingNanos:      metric.NewCounter(elasticCPUWaitingNanos),
		AcquiredNanos:     metric.NewCounter(elasticCPUAcquiredNanos),
		ReturnedNanos:     metric.NewCounter(elasticCPUReturnedNanos),
		Acquisitions:      metric.NewCounter(elasticCPUAcquisitions),
		TargetUtilization: metric.NewGaugeFloat64(elasticCPUGranterTargetUtilization),
	}
}

// MetricStruct implements the metric.Struct interface.
func (k *elasticCPUGranterMetrics) MetricStruct() {}

var _ metric.Struct = &elasticCPUGranterMetrics{}

// ElasticCPUGrantCoordinator coordinates grants for elastic CPU tokens, it has
// a single granter-requester pair. Since it's used for elastic CPU work, and
// the total allotment of CPU available for such work is reduced before getting
// close to CPU saturation (we observe 1ms+ p99 scheduling latencies when
// running at 65% utilization on 8vCPU machines, enough to affect foreground
// latencies), we don't want it to serve as a gatekeeper for SQL-level
// admission. All this informs why its structured as a separate grant
// coordinator.
type ElasticCPUGrantCoordinator struct {
	Listener            SchedulerLatencyListener
	elasticCPUGranter   *elasticCPUGranter
	elasticCPUWorkQueue *elasticCPUWorkQueue
}

func makeElasticGrantCoordinator(
	elasticCPUGranter *elasticCPUGranter,
	elasticCPUWorkQueue *elasticCPUWorkQueue,
	listener *schedulerLatencyListener,
) *ElasticCPUGrantCoordinator {
	return &ElasticCPUGrantCoordinator{
		elasticCPUGranter:   elasticCPUGranter,
		elasticCPUWorkQueue: elasticCPUWorkQueue,
		Listener:            listener,
	}
}

func (e *ElasticCPUGrantCoordinator) close() {
	e.elasticCPUWorkQueue.close()
}
