// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package kvprober sends queries to KV in a loop, with configurable sleep
// times, in order to generate data about the healthiness or unhealthiness of
// kvclient & below.
//
// Prober increments metrics that SRE & other operators can use as alerting
// signals. It also writes to logs to help narrow down the problem (e.g. which
// range(s) are acting up).
package kvprober

import (
	"context"
	"math/rand"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/contextutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logcrash"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// Prober sends queries to KV in a loop. See package docstring for more.
type Prober struct {
	ambientCtx log.AmbientContext
	db         *kv.DB
	settings   *cluster.Settings
	// planner is an interface for selecting a range to probe.
	planner planner
	// metrics wraps up the set of prometheus metrics that the prober sets; the
	// goal of the prober IS to populate these metrics.
	metrics Metrics
}

// Opts provides knobs to control kvprober.Prober.
type Opts struct {
	AmbientCtx log.AmbientContext
	DB         *kv.DB
	Settings   *cluster.Settings
	// The windowed portion of the latency histogram retains values for
	// approximately histogramWindow. See metrics library for more.
	HistogramWindowInterval time.Duration
}

var (
	metaReadProbeAttempts = metric.Metadata{
		Name:        "kv.prober.read.attempts",
		Help:        "Number of attempts made to probe KV, regardless of outcome",
		Measurement: "Queries",
		Unit:        metric.Unit_COUNT,
	}
	metaReadProbeFailures = metric.Metadata{
		Name: "kv.prober.read.failures",
		Help: "Number of attempts made to probe KV that failed, " +
			"whether due to error or timeout",
		Measurement: "Queries",
		Unit:        metric.Unit_COUNT,
	}
	metaReadProbeLatency = metric.Metadata{
		Name:        "kv.prober.read.latency",
		Help:        "Latency of successful KV read probes",
		Measurement: "Latency",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaProbePlanAttempts = metric.Metadata{
		Name: "kv.prober.planning_attempts",
		Help: "Number of attempts at planning out probes made; " +
			"in order to probe KV we need to plan out which ranges to probe;",
		Measurement: "Runs",
		Unit:        metric.Unit_COUNT,
	}
	metaProbePlanFailures = metric.Metadata{
		Name: "kv.prober.planning_failures",
		Help: "Number of attempts at planning out probes that failed; " +
			"in order to probe KV we need to plan out which ranges to probe; " +
			"if planning fails, then kvprober is not able to send probes to " +
			"all ranges; consider alerting on this metric as a result",
		Measurement: "Runs",
		Unit:        metric.Unit_COUNT,
	}
	// TODO(josh): Add a histogram that captures where in the "rangespace" errors
	// are occurring. This will allow operators to see at a glance what percentage
	// of ranges are affected.
)

// Metrics groups together the metrics that kvprober exports.
type Metrics struct {
	ReadProbeAttempts *metric.Counter
	ReadProbeFailures *metric.Counter
	ReadProbeLatency  *metric.Histogram
	ProbePlanAttempts *metric.Counter
	ProbePlanFailures *metric.Counter
}

// NewProber creates a Prober from Opts.
func NewProber(opts Opts) *Prober {
	return &Prober{
		ambientCtx: opts.AmbientCtx,
		db:         opts.DB,
		settings:   opts.Settings,

		planner: newMeta2Planner(opts.DB, opts.Settings),
		metrics: Metrics{
			ReadProbeAttempts: metric.NewCounter(metaReadProbeAttempts),
			ReadProbeFailures: metric.NewCounter(metaReadProbeFailures),
			ReadProbeLatency:  metric.NewLatency(metaReadProbeLatency, opts.HistogramWindowInterval),
			ProbePlanAttempts: metric.NewCounter(metaProbePlanAttempts),
			ProbePlanFailures: metric.NewCounter(metaProbePlanFailures),
		},
	}
}

// Metrics returns a struct which contains the kvprober metrics.
func (p *Prober) Metrics() Metrics {
	return p.metrics
}

// Start causes kvprober to start probing KV. Start returns immediately. Start
// returns an error only if stopper.RunAsyncTask returns an error.
func (p *Prober) Start(ctx context.Context, stopper *stop.Stopper) error {
	return stopper.RunAsyncTask(ctx, "probe loop", func(ctx context.Context) {
		ambient := p.ambientCtx
		ambient.AddLogTag("kvprober", nil)

		ctx, sp := ambient.AnnotateCtxWithSpan(ctx, "probe loop")
		defer sp.Finish()

		ctx, cancel := stopper.WithCancelOnQuiesce(ctx)
		defer cancel()

		d := func() time.Duration {
			return withJitter(readInterval.Get(&p.settings.SV), rand.Int63n)
		}
		t := timeutil.NewTimer()
		t.Reset(d())
		defer t.Stop()

		for {
			select {
			case <-t.C:
				t.Read = true
				// Jitter added to de-synchronize different nodes' probe loops.
				t.Reset(d())
			case <-stopper.ShouldQuiesce():
				return
			}

			p.probe(ctx, p.db)
		}
	})
}

type dbGet interface {
	Get(ctx context.Context, key interface{}) (kv.KeyValue, error)
}

// Doesn't return an error. Instead increments error type specific metrics.
func (p *Prober) probe(ctx context.Context, db dbGet) {
	defer logcrash.RecoverAndReportNonfatalPanic(ctx, &p.settings.SV)

	if !readEnabled.Get(&p.settings.SV) {
		return
	}

	p.metrics.ProbePlanAttempts.Inc(1)

	step, err := p.planner.next(ctx)
	if err != nil {
		log.Health.Errorf(ctx, "can't make a plan: %v", err)
		p.metrics.ProbePlanFailures.Inc(1)
		return
	}

	// If errors above the KV scan, then this counter won't be incremented.
	// This means that ReadProbeErrors / ReadProbeAttempts captures the KV
	// error rate only as desired. It also means that operators can alert on
	// an unexpectedly low rate of ReadProbeAttempts or else a high rate of
	// ProbePlanFailures. This would probably be a ticket alerting as
	// the impact is more low visibility into possible failures than a high
	// impact production issue.
	p.metrics.ReadProbeAttempts.Inc(1)

	start := timeutil.Now()

	// Slow enough response times are not different than errors from the
	// perspective of the user.
	timeout := readTimeout.Get(&p.settings.SV)
	err = contextutil.RunWithTimeout(ctx, "db.Get", timeout, func(ctx context.Context) error {
		// We read the start key for the range. There may be no data at the key,
		// but that is okay. Even if there is no data at the key, the prober still
		// executes a basic read operation on the range.
		// TODO(josh): Trace the probes.
		_, err = db.Get(ctx, step.StartKey)
		return err
	})
	if err != nil {
		// TODO(josh): Write structured events with log.Structured.
		log.Health.Errorf(ctx, "kv.Get(%s), r=%v failed with: %v", step.StartKey, step.RangeID, err)
		p.metrics.ReadProbeFailures.Inc(1)
		return
	}

	d := timeutil.Since(start)
	log.Health.Infof(ctx, "kv.Get(%s), r=%v returned success in %v", step.StartKey, step.RangeID, d)

	// Latency of failures is not recorded. They are counted as failures tho.
	p.metrics.ReadProbeLatency.RecordValue(d.Nanoseconds())
}

// Returns a random duration pulled from the uniform distribution given below:
// [d - 0.2*d, d + 0.2*d]
func withJitter(d time.Duration, intn func(n int64) int64) time.Duration {
	jitter := time.Duration(intn(d.Milliseconds()/5)) * time.Millisecond
	if intn(2) == 1 {
		return d + jitter
	}
	return d - jitter
}
