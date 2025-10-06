// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
	"regexp"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logcrash"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	"github.com/cockroachdb/redact"
)

const putValue = "thekvproberwrotethis"

// Prober sends queries to KV in a loop. See package docstring for more.
type Prober struct {
	db       *kv.DB
	settings *cluster.Settings
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
	tracer  *tracing.Tracer
	// quarantineWritePool pool keeps track of ranges that timed out/errored when
	// probing and repeatedly probes those ranges until a probe succeeds.
	quarantineWritePool *quarantinePool
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
	metaWriteProbeQuarantineOldestDuration = metric.Metadata{
		Name: "kv.prober.write.quarantine.oldest_duration",
		Help: "The duration that the oldest range in the " +
			"write quarantine pool has remained",
		Measurement: "Seconds",
		Unit:        metric.Unit_SECONDS,
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
	ReadProbeAttempts                  *metric.Counter
	ReadProbeFailures                  *metric.Counter
	ReadProbeLatency                   metric.IHistogram
	WriteProbeAttempts                 *metric.Counter
	WriteProbeFailures                 *metric.Counter
	WriteProbeLatency                  metric.IHistogram
	WriteProbeQuarantineOldestDuration *metric.Gauge
	ProbePlanAttempts                  *metric.Counter
	ProbePlanFailures                  *metric.Counter
}

// proberOpsI is an interface that the prober will use to run ops against some
// system. This interface exists so that ops can be mocked for tests.
type proberOpsI interface {
	Read(key roachpb.Key) func(context.Context, *kv.Txn) error
	Write(key roachpb.Key) func(context.Context, *kv.Txn) error
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

// ProberOps collects the methods used to probe the KV layer.
type ProberOps struct{}

// We attempt to commit a txn that reads some data at the key.
func (p *ProberOps) Read(key roachpb.Key) func(context.Context, *kv.Txn) error {
	return func(ctx context.Context, txn *kv.Txn) error {
		if err := p.validateKey(key); err != nil {
			return err
		}
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
func (p *ProberOps) Write(key roachpb.Key) func(context.Context, *kv.Txn) error {
	return func(ctx context.Context, txn *kv.Txn) error {
		if err := p.validateKey(key); err != nil {
			return err
		}
		// Use a single batch so that the entire txn requires a single pass
		// through Raft. It's not strictly necessary that we Put before we
		// Del the key, because a Del is blind and leaves a tombstone even
		// when the key is not live, but this is not guaranteed by the API,
		// so we avoid depending on a subtlety of the implementation.
		b := txn.NewBatch()
		b.Put(key, putValue)
		b.Del(key)
		return txn.CommitInBatch(ctx, b)
	}
}

// errorIsExpectedDuringNormalOperation filters out errors that may be returned
// during normal operation of CRDB.
//
// One such example is the `was permanently removed from the cluster at` error
// that is returned to the kvclient of decommissioned nodes. This error does not
// affect user traffic, since such traffic is drained off the node by the time it
// becomes decommissioned.
//
// Since such errors do not indicate a problem with CRDB, kvprober does not report
// them as an error in its metrics.
func errorIsExpectedDuringNormalOperation(err error) bool {
	// Note that errors *other* than decommissioned status errors, such as
	// `use of closed network connection`, happen *occasionally* on the kvclient
	// of a decommissioned node. The full set of other errors is not known exactly,
	// and the errors mostly lack structure. Since they happen rarely, and since
	// the intended use of kvprober is to page on a sustained error rate, not a
	// single error, we choose to only filter out errors via the
	// kvpb.IsDecommissionedStatusErr function.
	return kvpb.IsDecommissionedStatusErr(err)
}

// validateKey returns an error if the key is not valid for use by the kvprober.
// This is a sanity check to ensure that the kvprober does not corrupt user data
// in the global keyspace or other system data in the local keyspace.
func (p *ProberOps) validateKey(key roachpb.Key) error {
	_, suffix, _, err := keys.DecodeRangeKey(key)
	if err != nil {
		return errors.NewAssertionErrorWithWrappedErrf(err,
			"key %q is not a valid probe key; could not decode range key", key)
	}
	if !suffix.Equal(keys.LocalRangeProbeSuffix.AsRawKey()) {
		return errors.AssertionFailedf(
			"key %q is not a valid probe key; incorrect range key suffix", key)
	}
	return nil
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
	qPool := newQuarantinePool(opts.Settings)
	return &Prober{
		db:       opts.DB,
		settings: opts.Settings,

		readPlanner:  newMeta2Planner(opts.DB, opts.Settings, func() time.Duration { return readInterval.Get(&opts.Settings.SV) }),
		writePlanner: newMeta2Planner(opts.DB, opts.Settings, func() time.Duration { return writeInterval.Get(&opts.Settings.SV) }),

		metrics: Metrics{
			ReadProbeAttempts: metric.NewCounter(metaReadProbeAttempts),
			ReadProbeFailures: metric.NewCounter(metaReadProbeFailures),
			ReadProbeLatency: metric.NewHistogram(metric.HistogramOptions{
				Mode:         metric.HistogramModePreferHdrLatency,
				Metadata:     metaReadProbeLatency,
				Duration:     opts.HistogramWindowInterval,
				BucketConfig: metric.IOLatencyBuckets,
			}),
			WriteProbeAttempts: metric.NewCounter(metaWriteProbeAttempts),
			WriteProbeFailures: metric.NewCounter(metaWriteProbeFailures),
			WriteProbeLatency: metric.NewHistogram(metric.HistogramOptions{
				Mode:         metric.HistogramModePreferHdrLatency,
				Metadata:     metaWriteProbeLatency,
				Duration:     opts.HistogramWindowInterval,
				BucketConfig: metric.IOLatencyBuckets,
			}),
			WriteProbeQuarantineOldestDuration: metric.NewFunctionalGauge(
				metaWriteProbeQuarantineOldestDuration,
				func() int64 { return qPool.oldestDuration().Nanoseconds() },
			),
			ProbePlanAttempts: metric.NewCounter(metaProbePlanAttempts),
			ProbePlanFailures: metric.NewCounter(metaProbePlanFailures),
		},
		tracer:              opts.Tracer,
		quarantineWritePool: qPool,
	}
}

// Metrics returns a struct which contains the kvprober metrics.
func (p *Prober) Metrics() Metrics {
	return p.metrics
}

// Start causes kvprober to start probing KV. Start returns immediately. Start
// returns an error only if stopper.RunAsyncTask returns an error.
func (p *Prober) Start(ctx context.Context, stopper *stop.Stopper) error {
	ctx = logtags.AddTag(ctx, "kvprober", nil /* value */)
	startLoop := func(ctx context.Context, opName string, probe func(context.Context, planner), pl planner, interval *settings.DurationSetting) error {
		return stopper.RunAsyncTaskEx(ctx, stop.TaskOpts{TaskName: opName, SpanOpt: stop.SterileRootSpan}, func(ctx context.Context) {
			defer logcrash.RecoverAndReportNonfatalPanic(ctx, &p.settings.SV)

			rnd, _ /* seed */ := randutil.NewPseudoRand()
			d := func() time.Duration {
				return withJitter(interval.Get(&p.settings.SV), rnd)
			}
			var t timeutil.Timer
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

				probeCtx, sp := tracing.EnsureChildSpan(ctx, p.tracer, opName+" - probe")
				probe(probeCtx, pl)
				sp.Finish()
			}
		})
	}

	if err := startLoop(ctx, "read probe loop", p.readProbe, p.readPlanner, readInterval); err != nil {
		return err
	}
	if err := startLoop(ctx, "write probe loop", p.writeProbe, p.writePlanner, writeInterval); err != nil {
		return err
	}
	// The purpose of the quarantine pool is to detect outages affecting a small number
	// of ranges but at a high confidence. The quarantine pool does this by repeatedly
	// probing ranges that are in the pool.
	return startLoop(ctx, "quarantine write probe loop", p.quarantineProbe, p.quarantineWritePool, quarantineWriteInterval)
}

// Doesn't return an error. Instead, increments error type specific metrics.
//
// TODO(tbg): db parameter is unused, remove it.
func (p *Prober) readProbe(ctx context.Context, pl planner) {
	p.readProbeImpl(ctx, &ProberOps{}, &proberTxnImpl{db: p.db}, pl)
}

func (p *Prober) readProbeImpl(ctx context.Context, ops proberOpsI, txns proberTxn, pl planner) {
	if !readEnabled.Get(&p.settings.SV) {
		return
	}

	p.metrics.ProbePlanAttempts.Inc(1)
	var finishAndGetRecording func() tracingpb.Recording
	var probeCtx = ctx

	isTracingEnabled := tracingEnabled.Get(&p.settings.SV)
	if isTracingEnabled {
		probeCtx, finishAndGetRecording = tracing.ContextWithRecordingSpan(ctx, p.tracer, "read probe")
	}

	step, err := pl.next(probeCtx)
	if err == nil && step.RangeID == 0 {
		return
	}
	if err != nil {
		if errorIsExpectedDuringNormalOperation(err) {
			log.Health.Warningf(ctx, "making a plan failed with expected error: %v", err)
		} else {
			log.Health.Errorf(ctx, "can't make a plan: %v", err)
			p.metrics.ProbePlanFailures.Inc(1)
		}
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

	ctx = logtags.AddTag(ctx, "range", step.RangeID)

	start := timeutil.Now()

	// Slow enough response times are not different than errors from the
	// perspective of the user.
	timeout := readTimeout.Get(&p.settings.SV)
	err = timeutil.RunWithTimeout(probeCtx, "read probe", timeout, func(ctx context.Context) error {
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
		if errorIsExpectedDuringNormalOperation(err) {
			log.Health.Warningf(ctx, "kv.Get(%s), r=%v failed with expected error: %v", step.Key, step.RangeID, err)
		} else {
			// TODO(josh): Write structured events with log.Structured.
			log.Health.Errorf(ctx, "kv.Get(%s), r=%v failed with: %v", step.Key, step.RangeID, err)
			p.metrics.ReadProbeFailures.Inc(1)
		}
		return
	}

	d := timeutil.Since(start)
	// Extract leaseholder information from the trace recording if enabled.
	if isTracingEnabled {
		leaseholder := p.returnLeaseholderInfo(finishAndGetRecording())
		ctx = logtags.AddTag(ctx, "leaseholder", leaseholder)
		log.Health.Infof(ctx, "kv.Get(%s), r=%v having likely leaseholder=%s returned success in %v", step.Key, step.RangeID, leaseholder, d)
	} else {
		log.Health.Infof(ctx, "kv.Get(%s), r=%v returned success in %v", step.Key, step.RangeID, d)
	}

	// Latency of failures is not recorded. They are counted as failures tho.
	p.metrics.ReadProbeLatency.RecordValue(d.Nanoseconds())
}

// Doesn't return an error. Instead increments error type specific metrics.
func (p *Prober) writeProbe(ctx context.Context, pl planner) {
	p.writeProbeImpl(ctx, &ProberOps{}, &proberTxnImpl{db: p.db}, pl)
}

func (p *Prober) writeProbeImpl(ctx context.Context, ops proberOpsI, txns proberTxn, pl planner) {
	if !writeEnabled.Get(&p.settings.SV) {
		return
	}

	p.metrics.ProbePlanAttempts.Inc(1)
	var finishAndGetRecording func() tracingpb.Recording
	var probeCtx = ctx

	isTracingEnabled := tracingEnabled.Get(&p.settings.SV)
	if isTracingEnabled {
		probeCtx, finishAndGetRecording = tracing.ContextWithRecordingSpan(ctx, p.tracer, "write probe")
	}

	step, err := pl.next(probeCtx)
	// In the case where the quarantine pool is empty don't record a planning failure since
	// it isn't an actual plan failure.
	if err == nil && step.RangeID == 0 {
		return
	}
	if err != nil {
		if errorIsExpectedDuringNormalOperation(err) {
			log.Health.Warningf(ctx, "making a plan failed with expected error: %v", err)
		} else {
			log.Health.Errorf(ctx, "can't make a plan: %v", err)
			p.metrics.ProbePlanFailures.Inc(1)
		}
		return
	}

	p.metrics.WriteProbeAttempts.Inc(1)

	ctx = logtags.AddTag(ctx, "range", step.RangeID)

	start := timeutil.Now()

	// Slow enough response times are not different than errors from the
	// perspective of the user.
	timeout := writeTimeout.Get(&p.settings.SV)
	err = timeutil.RunWithTimeout(probeCtx, "write probe", timeout, func(ctx context.Context) error {
		f := ops.Write(step.Key)
		if bypassAdmissionControl.Get(&p.settings.SV) {
			return txns.Txn(ctx, f)
		}
		return txns.TxnRootKV(ctx, f)
	})
	if err != nil {
		if errorIsExpectedDuringNormalOperation(err) {
			log.Health.Warningf(
				ctx, "kv.Txn(Put(%s); Del(-)), r=%v failed with expected error: %v", step.Key, step.RangeID, err,
			)
		} else {
			added := p.quarantineWritePool.maybeAdd(ctx, step)
			log.Health.Errorf(
				ctx, "kv.Txn(Put(%s); Del(-)), r=%v failed with: %v [quarantined=%t]", step.Key, step.RangeID, err, added,
			)
			p.metrics.WriteProbeFailures.Inc(1)
		}
		return
	}
	// This will no-op if not in the quarantine pool.
	p.quarantineWritePool.maybeRemove(ctx, step)

	d := timeutil.Since(start)
	// Extract leaseholder information from the trace recording if enabled.
	if isTracingEnabled {
		leaseholder := p.returnLeaseholderInfo(finishAndGetRecording())
		ctx = logtags.AddTag(ctx, "leaseholder", leaseholder)
		log.Health.Infof(ctx, "kv.Txn(Put(%s); Del(-)), r=%v having likely leaseholder=%s returned success in %v", step.Key, step.RangeID, leaseholder, d)
	} else {
		log.Health.Infof(ctx, "kv.Txn(Put(%s); Del(-)), r=%v returned success in %v", step.Key, step.RangeID, d)
	}

	// Latency of failures is not recorded. They are counted as failures tho.
	p.metrics.WriteProbeLatency.RecordValue(d.Nanoseconds())
}

// Wrapper function for probing the quarantine pool.
func (p *Prober) quarantineProbe(ctx context.Context, pl planner) {
	if !quarantineWriteEnabled.Get(&p.settings.SV) {
		return
	}

	p.writeProbe(ctx, pl)
}

// returnLeaseholderInfo searches through the given tracing recording for
// log messages indicating leaseholder information, extracts leaseholder
// node ID, and returns this information. Returns an empty string if
// leaseholder information is not found.
func (p *Prober) returnLeaseholderInfo(recording tracingpb.Recording) redact.SafeString {
	// The leaseholder is determined by the kvclient in dist_sender.go,
	// which decides the node to handle the request and sends it. The log
	// entry with "node received request" shows the leaseholder acknowledging
	// and processing the request. Earlier entries with the same phrase might
	// relate to planning in planner.go, not the execution by the leaseholder.
	//
	// Example trace snippet:
	// event:kv/kvclient/kvcoord/dist_sender.go:2644 [n2,kvprober,txn=‹9d8205b4›] r22: sending batch ‹1 Get› to (n3,s3):2
	// event:server/node.go:1407 [n3] node received request: ‹1 Get›
	informationLog, isPresent := p.findLastLogMessage(recording, "node received request")

	if isPresent {
		// Regular expression to extract leaseholder node ID.
		leaseRegex := regexp.MustCompile(`\[n(\d+)\]`)
		leaseholder := leaseRegex.FindStringSubmatch(informationLog)
		// Return leaseholder node ID if found.
		if len(leaseholder) == 2 {
			return redact.SafeString(leaseholder[1])
		}
	}
	return ""
}

// findLastLogMessage returns the last log message in the recording that matches
// the given regexp. The bool return value is true if such a message is found.
//
// This method strips the redaction markers from all the log messages, which is
// pretty inefficient.
func (p *Prober) findLastLogMessage(r tracingpb.Recording, pattern string) (string, bool) {
	re := regexp.MustCompile(pattern)
	for i := len(r) - 1; i >= 0; i-- {
		sp := r[i]
		for j := len(sp.Logs) - 1; j >= 0; j-- {
			msg := sp.Logs[j].Msg().StripMarkers()
			if re.MatchString(msg) {
				return msg, true
			}
		}
	}
	return "", false
}

// Returns a random duration pulled from the uniform distribution given below:
// [d - 0.25*d, d + 0.25*d).
func withJitter(d time.Duration, rnd *rand.Rand) time.Duration {
	amplitudeNanos := d.Nanoseconds() / 4
	return d + time.Duration(randutil.RandInt63InRange(rnd, -amplitudeNanos, amplitudeNanos))
}
