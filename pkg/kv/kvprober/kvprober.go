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
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
)

const putValue = "thekvproberwrotethis"

var ErrPlanEmpty = errors.New("no keys need to be probed")

// Prober sends queries to KV in a loop. See package docstring for more.
type Prober struct {
	db       *kv.DB
	settings *cluster.Settings
	tracer   *tracing.Tracer
	loops    []ProbeLoop
	metrics  []Metrics
}

type ProbeLoop struct {
	name   string
	prober func(context.Context, *kv.DB, planner, ProbeLoop)
	// planner is an interface for selecting a range to probe. There are
	// separate planners for the read & write probe loops, so as to achieve
	// a balanced probing of the keyspace, regardless of differences in the rate
	// at which Prober sends different probes. Also note that planner is
	planner        planner
	interval       *settings.DurationSetting
	problemTracker *problemTracker
	// metrics wraps up the set of prometheus metrics that the prober sets; the
	// goal of the prober IS to populate these metrics.
	metrics Metrics
}

type problemTracker struct {
	steps []Step
	size  int
}

func newProblemTracker() *problemTracker {
	return &problemTracker{size: 3}
}

func (pt *problemTracker) add(step Step) {
	if len(pt.steps) >= pt.size-1 {
		pt.steps = append(pt.steps[1:], step)
	} else {
		pt.steps = append(pt.steps, step)
	}
}

// Choose a random problem range, if any available
func (pt *problemTracker) next() (Step, error) {
	if len(pt.steps) > 0 {
		return pt.steps[rand.Intn(len(pt.steps))], nil
	} else {
		return Step{}, ErrPlanEmpty
	}
}

// Opts provides knobs to control kvprober.Prober.
type Opts struct {
	DB       *kv.DB
	Settings *cluster.Settings
	Tracer   *tracing.Tracer
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

	metaProblemReadProbeAttempts = metric.Metadata{
		Name:        "kv.prober.problem.read.attempts",
		Help:        "Number of attempts made to read probe KV, regardless of outcome",
		Measurement: "Queries",
		Unit:        metric.Unit_COUNT,
	}
	metaProblemReadProbeFailures = metric.Metadata{
		Name: "kv.prober.problem.read.failures",
		Help: "Number of attempts made to read probe KV that failed, " +
			"whether due to error or timeout",
		Measurement: "Queries",
		Unit:        metric.Unit_COUNT,
	}
	metaProblemReadProbeLatency = metric.Metadata{
		Name:        "kv.prober.problem.read.latency",
		Help:        "Latency of successful KV read probes",
		Measurement: "Latency",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaProblemWriteProbeAttempts = metric.Metadata{
		Name:        "kv.prober.problem.write.attempts",
		Help:        "Number of attempts made to write probe KV, regardless of outcome",
		Measurement: "Queries",
		Unit:        metric.Unit_COUNT,
	}
	metaProblemWriteProbeFailures = metric.Metadata{
		Name: "kv.prober.problem.write.failures",
		Help: "Number of attempts made to write probe KV that failed, " +
			"whether due to error or timeout",
		Measurement: "Queries",
		Unit:        metric.Unit_COUNT,
	}
	metaProblemWriteProbeLatency = metric.Metadata{
		Name:        "kv.prober.problem.write.latency",
		Help:        "Latency of successful KV write probes",
		Measurement: "Latency",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaProblemProbePlanAttempts = metric.Metadata{
		Name: "kv.prober.problem.planning_attempts",
		Help: "Number of attempts at planning out probes made; " +
			"in order to probe KV we need to plan out which ranges to probe;",
		Measurement: "Runs",
		Unit:        metric.Unit_COUNT,
	}
	metaProblemProbePlanFailures = metric.Metadata{
		Name: "kv.prober.problem.planning_failures",
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

// proberOps is an interface that the prober will use to run ops against some
// system. This interface exists so that ops can be mocked for tests.
type proberOps interface {
	Read(key interface{}) func(context.Context, *kv.Txn) error
	Write(key interface{}) func(context.Context, *kv.Txn) error
}

// proberTxn is an interface that the prober will use to run txns. This
// interface exists so that txn can be mocked for tests.
type proberTxn interface {
	// Txn runs the given function with a transaction having the admission
	// source in the header set to OTHER. Transaction work submitted from this
	// source currently bypassess admission control.
	Txn(context.Context, func(context.Context, *kv.Txn) error) error
	// TxnRootKV runs the given function with a transaction having the admission
	// source in the header set to ROOT KV. Transaction work submitted from this
	// source should not bypass admission control.
	TxnRootKV(context.Context, func(context.Context, *kv.Txn) error) error
}

// proberOpsImpl is used to probe the kv layer.
type proberOpsImpl struct {
}

// We attempt to commit a txn that reads some data at the key.
func (p *proberOpsImpl) Read(key interface{}) func(context.Context, *kv.Txn) error {
	return func(ctx context.Context, txn *kv.Txn) error {
		_, err := txn.Get(ctx, key)
		return err
	}
}

// We attempt to commit a txn that puts some data at the key then deletes
// it. The test of the write code paths is good: We get a raft command that
// goes thru consensus and is written to the pebble log. Importantly, no
// *live* data is left at the key, which simplifies the kvprober, as then
// there is no need to clean up data at the key post range split / merge.
// Note that MVCC tombstones may be left by the probe, but this is okay, as
// GC will clean it up.
func (p *proberOpsImpl) Write(key interface{}) func(context.Context, *kv.Txn) error {
	return func(ctx context.Context, txn *kv.Txn) error {
		if err := txn.Put(ctx, key, putValue); err != nil {
			return err
		}
		return txn.Del(ctx, key)
	}
}

// proberTxnImpl is used to run transactions.
type proberTxnImpl struct {
	db *kv.DB
}

func (p *proberTxnImpl) Txn(ctx context.Context, f func(context.Context, *kv.Txn) error) error {
	return p.db.Txn(ctx, f)
}

func (p *proberTxnImpl) TxnRootKV(
	ctx context.Context, f func(context.Context, *kv.Txn) error,
) error {
	return p.db.TxnRootKV(ctx, f)
}

// NewProber creates a Prober from Opts.
func NewProber(opts Opts) *Prober {
	readProblemTracker := newProblemTracker()
	writeProblemTracker := newProblemTracker()

	meta2Metrics := Metrics{
		ReadProbeAttempts:  metric.NewCounter(metaReadProbeAttempts),
		ReadProbeFailures:  metric.NewCounter(metaReadProbeFailures),
		ReadProbeLatency:   metric.NewLatency(metaReadProbeLatency, opts.HistogramWindowInterval),
		WriteProbeAttempts: metric.NewCounter(metaWriteProbeAttempts),
		WriteProbeFailures: metric.NewCounter(metaWriteProbeFailures),
		WriteProbeLatency:  metric.NewLatency(metaWriteProbeLatency, opts.HistogramWindowInterval),
		ProbePlanAttempts:  metric.NewCounter(metaProbePlanAttempts),
		ProbePlanFailures:  metric.NewCounter(metaProbePlanFailures),
	}
	problemMetrics := Metrics{
		ReadProbeAttempts:  metric.NewCounter(metaProblemReadProbeAttempts),
		ReadProbeFailures:  metric.NewCounter(metaProblemReadProbeFailures),
		ReadProbeLatency:   metric.NewLatency(metaProblemReadProbeLatency, opts.HistogramWindowInterval),
		WriteProbeAttempts: metric.NewCounter(metaProblemWriteProbeAttempts),
		WriteProbeFailures: metric.NewCounter(metaProblemWriteProbeFailures),
		WriteProbeLatency:  metric.NewLatency(metaProblemWriteProbeLatency, opts.HistogramWindowInterval),
		ProbePlanAttempts:  metric.NewCounter(metaProblemProbePlanAttempts),
		ProbePlanFailures:  metric.NewCounter(metaProblemProbePlanFailures),
	}

	prober := Prober{
		db:       opts.DB,
		settings: opts.Settings,
		tracer:   opts.Tracer,
	}

	prober.loops = []ProbeLoop{
		{
			name:           "read probe loop",
			planner:        newMeta2Planner(opts.DB, opts.Settings, func() time.Duration { return readInterval.Get(&opts.Settings.SV) }),
			interval:       readInterval,
			problemTracker: readProblemTracker,
			prober:         prober.readProbe,
			metrics:        meta2Metrics,
		},
		{
			name:           "write probe loop",
			planner:        newMeta2Planner(opts.DB, opts.Settings, func() time.Duration { return readInterval.Get(&opts.Settings.SV) }),
			interval:       writeInterval,
			problemTracker: writeProblemTracker,
			prober:         prober.writeProbe,
			metrics:        meta2Metrics,
		},
		{
			name:           "problem read probe loop",
			planner:        newProblemTrackerPlanner(readProblemTracker),
			interval:       readInterval,
			problemTracker: nil,
			prober:         prober.readProbe,
			metrics:        problemMetrics,
		},
		{
			name:           "problem write probe loop",
			planner:        newProblemTrackerPlanner(writeProblemTracker),
			interval:       writeInterval,
			problemTracker: nil,
			prober:         prober.writeProbe,
			metrics:        problemMetrics,
		},
	}

	return &prober
}

// Metrics returns a struct which contains the kvprober metrics.
func (p *Prober) Metrics() []Metrics {
	return p.metrics
}

// Start causes kvprober to start probing KV. Start returns immediately. Start
// returns an error only if stopper.RunAsyncTask returns an error.
func (p *Prober) Start(ctx context.Context, stopper *stop.Stopper) error {
	ctx = logtags.AddTag(ctx, "kvprober", nil /* value */)
	startLoop := func(ctx context.Context, loop ProbeLoop) error {
		return stopper.RunAsyncTaskEx(ctx, stop.TaskOpts{TaskName: loop.name, SpanOpt: stop.SterileRootSpan}, func(ctx context.Context) {
			defer logcrash.RecoverAndReportNonfatalPanic(ctx, &p.settings.SV)

			rnd, _ /* seed */ := randutil.NewPseudoRand()
			d := func() time.Duration {
				return withJitter(loop.interval.Get(&p.settings.SV), rnd)
			}
			t := timeutil.NewTimer()
			defer t.Stop()
			t.Reset(d())

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

				probeCtx, sp := tracing.EnsureChildSpan(ctx, p.tracer, loop.name+" - probe")
				loop.prober(probeCtx, p.db, loop.planner, loop)
				sp.Finish()
			}
		})
	}

	for _, loop := range p.loops {
		if err := startLoop(ctx, loop); err != nil {
			return err
		}
	}
	return nil
}

// Doesn't return an error. Instead increments error type specific metrics.
func (p *Prober) readProbe(ctx context.Context, db *kv.DB, pl planner, l ProbeLoop) {
	p.readProbeImpl(ctx, &proberOpsImpl{}, &proberTxnImpl{db: p.db}, pl, l)
}

func (p *Prober) readProbeImpl(ctx context.Context, ops proberOps, txns proberTxn, pl planner, l ProbeLoop) {
	if !readEnabled.Get(&p.settings.SV) {
		return
	}

	l.metrics.ProbePlanAttempts.Inc(1)

	step, err := pl.next(ctx)
	if err != nil {
		if errors.Is(err, ErrPlanEmpty) {
			log.Health.Infof(ctx, "skipping empty plan: %v", err)
		} else {
			log.Health.Errorf(ctx, "can't make a plan: %v", err)
			l.metrics.ProbePlanFailures.Inc(1)
			return
		}
	}

	// If errors above the KV scan, then this counter won't be incremented.
	// This means that ReadProbeErrors / ReadProbeAttempts captures the KV
	// error rate only as desired. It also means that operators can alert on
	// an unexpectedly low rate of ReadProbeAttempts or else a high rate of
	// ProbePlanFailures. This would probably be a ticket alerting as
	// the impact is more low visibility into possible failures than a high
	// impact production issue.
	l.metrics.ReadProbeAttempts.Inc(1)

	start := timeutil.Now()

	// Slow enough response times are not different than errors from the
	// perspective of the user.
	timeout := readTimeout.Get(&p.settings.SV)
	err = contextutil.RunWithTimeout(ctx, "read probe", timeout, func(ctx context.Context) error {
		// We read a "range-local" key dedicated to probing. See pkg/keys for more.
		// There is no data at the key, but that is okay. Even tho there is no data
		// at the key, the prober still executes a read operation on the range.
		// TODO(josh): Trace the probes.
		f := ops.Read(step.Key)
		if bypassAdmissionControl.Get(&p.settings.SV) {
			return txns.Txn(ctx, f)
		}
		return txns.TxnRootKV(ctx, f)
	})
	if err != nil {
		// TODO(josh): Write structured events with log.Structured.
		log.Health.Errorf(ctx, "kv.Get(%s), r=%v failed with: %v", step.Key, step.RangeID, err)
		if l.problemTracker != nil {
			l.problemTracker.add(step)
		}
		l.metrics.ReadProbeFailures.Inc(1)
		return
	}

	d := timeutil.Since(start)
	log.Health.Infof(ctx, "kv.Get(%s), r=%v returned success in %v", step.Key, step.RangeID, d)

	// Latency of failures is not recorded. They are counted as failures tho.
	l.metrics.ReadProbeLatency.RecordValue(d.Nanoseconds())
}

// Doesn't return an error. Instead increments error type specific metrics.
func (p *Prober) writeProbe(ctx context.Context, db *kv.DB, pl planner, l ProbeLoop) {
	p.writeProbeImpl(ctx, &proberOpsImpl{}, &proberTxnImpl{db: p.db}, pl, l)
}

func (p *Prober) writeProbeImpl(ctx context.Context, ops proberOps, txns proberTxn, pl planner, l ProbeLoop) {
	if !writeEnabled.Get(&p.settings.SV) {
		return
	}

	l.metrics.ProbePlanAttempts.Inc(1)

	step, err := pl.next(ctx)
	if err != nil {
		if errors.Is(err, ErrPlanEmpty) {
			log.Health.Infof(ctx, "skipping empty plan: %v", err)
		} else {
			log.Health.Errorf(ctx, "can't make a plan: %v", err)
			l.metrics.ProbePlanFailures.Inc(1)
			return
		}
	}

	l.metrics.WriteProbeAttempts.Inc(1)

	start := timeutil.Now()

	// Slow enough response times are not different than errors from the
	// perspective of the user.
	timeout := writeTimeout.Get(&p.settings.SV)
	err = contextutil.RunWithTimeout(ctx, "write probe", timeout, func(ctx context.Context) error {
		f := ops.Write(step.Key)
		if bypassAdmissionControl.Get(&p.settings.SV) {
			return txns.Txn(ctx, f)
		}
		return txns.TxnRootKV(ctx, f)
	})
	if err != nil {
		log.Health.Errorf(ctx, "kv.Txn(Put(%s); Del(-)), r=%v failed with: %v", step.Key, step.RangeID, err)
		if l.problemTracker != nil {
			l.problemTracker.add(step)
		}
		l.metrics.WriteProbeFailures.Inc(1)
		return
	}

	d := timeutil.Since(start)
	log.Health.Infof(ctx, "kv.Txn(Put(%s); Del(-)), r=%v returned success in %v", step.Key, step.RangeID, d)

	// Latency of failures is not recorded. They are counted as failures tho.
	l.metrics.WriteProbeLatency.RecordValue(d.Nanoseconds())
}

// Returns a random duration pulled from the uniform distribution given below:
// [d - 0.25*d, d + 0.25*d).
func withJitter(d time.Duration, rnd *rand.Rand) time.Duration {
	amplitudeNanos := d.Nanoseconds() / 4
	return d + time.Duration(randutil.RandInt63InRange(rnd, -amplitudeNanos, amplitudeNanos))
}
