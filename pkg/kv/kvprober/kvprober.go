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
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/contextutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logcrash"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

const putValue = "thekvproberwrotethis"

// Prober sends queries to KV in a loop. See package docstring for more.
type Prober struct {
	ambientCtx log.AmbientContext
	db         *kv.DB
	settings   *cluster.Settings
	// planner is an interface for selecting a range to probe. There are
	// separate planners for the read & write probe loops, so as to achieve
	// a balanced probing of the keyspace, regardless of differences in the rate
	// at which Prober sends different probes. Also note that planner is
	// NOT thread-safe.
	readPlanner  planner
	writePlanner planner
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
		Help:        "Number of attempts made to read probe KV, regardless of outcome",
		Measurement: "Queries",
		Unit:        metric.Unit_COUNT,
	}
	metaReadProbeFailures = metric.Metadata{
		Name: "kv.prober.read.failures",
		Help: "Number of attempts made to read probe KV that failed, " +
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
	metaWriteProbeAttempts = metric.Metadata{
		Name:        "kv.prober.write.attempts",
		Help:        "Number of attempts made to write probe KV, regardless of outcome",
		Measurement: "Queries",
		Unit:        metric.Unit_COUNT,
	}
	metaWriteProbeFailures = metric.Metadata{
		Name: "kv.prober.write.failures",
		Help: "Number of attempts made to write probe KV that failed, " +
			"whether due to error or timeout",
		Measurement: "Queries",
		Unit:        metric.Unit_COUNT,
	}
	metaWriteProbeLatency = metric.Metadata{
		Name:        "kv.prober.write.latency",
		Help:        "Latency of successful KV write probes",
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
	ReadProbeAttempts  *metric.Counter
	ReadProbeFailures  *metric.Counter
	ReadProbeLatency   *metric.Histogram
	WriteProbeAttempts *metric.Counter
	WriteProbeFailures *metric.Counter
	WriteProbeLatency  *metric.Histogram
	ProbePlanAttempts  *metric.Counter
	ProbePlanFailures  *metric.Counter
}

// NewProber creates a Prober from Opts.
func NewProber(opts Opts) *Prober {
	return &Prober{
		ambientCtx: opts.AmbientCtx,
		db:         opts.DB,
		settings:   opts.Settings,

		readPlanner:  newMeta2Planner(opts.DB, opts.Settings, func() time.Duration { return readInterval.Get(&opts.Settings.SV) }),
		writePlanner: newMeta2Planner(opts.DB, opts.Settings, func() time.Duration { return writeInterval.Get(&opts.Settings.SV) }),

		metrics: Metrics{
			ReadProbeAttempts:  metric.NewCounter(metaReadProbeAttempts),
			ReadProbeFailures:  metric.NewCounter(metaReadProbeFailures),
			ReadProbeLatency:   metric.NewLatency(metaReadProbeLatency, opts.HistogramWindowInterval),
			WriteProbeAttempts: metric.NewCounter(metaWriteProbeAttempts),
			WriteProbeFailures: metric.NewCounter(metaWriteProbeFailures),
			WriteProbeLatency:  metric.NewLatency(metaWriteProbeLatency, opts.HistogramWindowInterval),
			ProbePlanAttempts:  metric.NewCounter(metaProbePlanAttempts),
			ProbePlanFailures:  metric.NewCounter(metaProbePlanFailures),
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
	ambient := p.ambientCtx
	ambient.AddLogTag("kvprober", nil)

	startLoop := func(ctx context.Context, desc string, pf func(context.Context, *kv.DB, planner), pl planner, interval *settings.DurationSetting) error {
		return stopper.RunAsyncTask(ctx, desc, func(ctx context.Context) {
			defer logcrash.RecoverAndReportNonfatalPanic(ctx, &p.settings.SV)

			d := func() time.Duration {
				return withJitter(interval.Get(&p.settings.SV), rand.Int63n)
			}
			t := timeutil.NewTimer()
			defer t.Stop()
			t.Reset(d())

			ctx, sp := ambient.AnnotateCtxWithSpan(ctx, desc)
			defer sp.Finish()

			ctx, cancel := stopper.WithCancelOnQuiesce(ctx)
			defer cancel()

			for {
				select {
				case <-t.C:
					t.Read = true
					// Jitter added to de-synchronize different nodes' probe loops.
					t.Reset(d())
				case <-stopper.ShouldQuiesce():
					return
				}

				pf(ctx, p.db, pl)
			}
		})
	}

	if err := startLoop(ctx, "read probe loop", p.readProbe, p.readPlanner, readInterval); err != nil {
		return err
	}
	return startLoop(ctx, "write probe loop", p.writeProbe, p.writePlanner, writeInterval)
}

// Doesn't return an error. Instead increments error type specific metrics.
func (p *Prober) readProbe(ctx context.Context, db *kv.DB, pl planner) {
	p.readProbeImpl(ctx, db, pl)
}

type dbGetter interface {
	Get(ctx context.Context, key interface{}) (kv.KeyValue, error)
}

func (p *Prober) readProbeImpl(ctx context.Context, db dbGetter, pl planner) {
	if !readEnabled.Get(&p.settings.SV) {
		return
	}

	p.metrics.ProbePlanAttempts.Inc(1)

	step, err := pl.next(ctx)
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
	err = contextutil.RunWithTimeout(ctx, "read probe", timeout, func(ctx context.Context) error {
		// We read a "range-local" key dedicated to probing. See pkg/keys for more.
		// There is no data at the key, but that is okay. Even tho there is no data
		// at the key, the prober still executes a read operation on the range.
		// TODO(josh): Trace the probes.
		_, err = db.Get(ctx, step.Key)
		return err
	})
	if err != nil {
		// TODO(josh): Write structured events with log.Structured.
		log.Health.Errorf(ctx, "kv.Get(%s), r=%v failed with: %v", step.Key, step.RangeID, err)
		p.metrics.ReadProbeFailures.Inc(1)
		return
	}

	d := timeutil.Since(start)
	log.Health.Infof(ctx, "kv.Get(%s), r=%v returned success in %v", step.Key, step.RangeID, d)

	// Latency of failures is not recorded. They are counted as failures tho.
	p.metrics.ReadProbeLatency.RecordValue(d.Nanoseconds())
}

// Doesn't return an error. Instead increments error type specific metrics.
func (p *Prober) writeProbe(ctx context.Context, db *kv.DB, pl planner) {
	p.writeProbeImpl(ctx, db, pl)
}

type dbTxner interface {
	Txn(ctx context.Context, f func(ctx context.Context, txn *kv.Txn) error) error
}

func (p *Prober) writeProbeImpl(ctx context.Context, db dbTxner, pl planner) {
	if !writeEnabled.Get(&p.settings.SV) {
		return
	}

	p.metrics.ProbePlanAttempts.Inc(1)

	step, err := pl.next(ctx)
	if err != nil {
		log.Health.Errorf(ctx, "can't make a plan: %v", err)
		p.metrics.ProbePlanFailures.Inc(1)
		return
	}

	p.metrics.WriteProbeAttempts.Inc(1)

	start := timeutil.Now()

	// Slow enough response times are not different than errors from the
	// perspective of the user.
	timeout := writeTimeout.Get(&p.settings.SV)
	err = contextutil.RunWithTimeout(ctx, "write probe", timeout, func(ctx context.Context) error {
		// We attempt to commit a txn that puts some data at the key then
		// deletes it. The test of the write code paths is good: We get
		// a raft command that goes thru consensus and is written to the
		// pebble log. Importantly, no *live* data is left at the key,
		// which simplifies the kvprober, as then there is no need to clean
		// up data at the key post range split / merge. Note that MVCC
		// tombstones may be left by the probe, but this is okay, as GC will
		// clean it up.
		return db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			if err := txn.Put(ctx, step.Key, putValue); err != nil {
				return err
			}
			return txn.Del(ctx, step.Key)
		})
	})
	if err != nil {
		log.Health.Errorf(ctx, "kv.Txn(Put(%s); Del(-)), r=%v failed with: %v", step.Key, step.RangeID, err)
		p.metrics.WriteProbeFailures.Inc(1)
		return
	}

	d := timeutil.Since(start)
	log.Health.Infof(ctx, "kv.Txn(Put(%s); Del(-)), r=%v returned success in %v", step.Key, step.RangeID, d)

	// Latency of failures is not recorded. They are counted as failures tho.
	p.metrics.WriteProbeLatency.RecordValue(d.Nanoseconds())
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
