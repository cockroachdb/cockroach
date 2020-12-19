package kvprober

import (
	"context"
	"math/rand"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logcrash"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

// kvprober sends queries to KV in a loop, with configurable sleep times, in
// order to generate data about the healthiness or unhealthiness of kvclient &
// below.
//
// kvprober increments metrics that SRE & other operators can use as alerting
// signals. It also writes to logs to help narrow down the problem (e.g. which
// range(s) are acting up).
type Prober struct {
	ambientCtx log.AmbientContext
	db         *kv.DB
	settings   *cluster.Settings

	// Planner is an interface for selecting a range to probe.
	Planner Planner
	// Metrics wraps up a set of prometheus metrics that the prober sets. The main
	// side effect the prober is taking is setting these metrics.
	Metrics Metrics
	// The windowed portion of the latency histogram retains values for
	// approximately histogramWindow. See metrics library for more.
	histogramWindowInterval time.Duration
}

type ProberOpts struct {
	AmbientCtx log.AmbientContext
	DB *kv.DB
	Settings         *cluster.Settings
	// The windowed portion of the latency histogram retains values for
	// approximately histogramWindow. See metrics library for more.
	HistogramWindowInterval time.Duration
}

var ReadEnabled = settings.RegisterBoolSetting(
	"kv.prober.read.enabled",
	"whether the KV read prober is enabled",
	false)

// TODO(josh): Maybe the cluster setting shouldn't be a time between probes
// per node but instead a QPS target for the cluster as a whole. This sounds
// like what the operator cares about? I'm not sure. As the cluster gets
// bigger, do we want to do more probing (there are more resource available
// to probe with & (probably) a bigger keyspace to cover)?
var ReadInterval = settings.RegisterDurationSetting(
	"kv.prober.read.interval",
	"how often each node sends a read probe of the KV layer",
	2 * time.Second)

var ReadTimeout = settings.RegisterDurationSetting(
	"kv.prober.read.timeout",
	// Slow enough response times are not different than errors from the
	// perspective of the user.
	"if this much time elapses without success, a KV read probe will be treated as an error",
	10 * time.Second)

var (
	metaReadProbeAttempts = metric.Metadata{
		Name:        "kv.prober.read.attempts",
		Help:        "Number of KV read probes made",
		Measurement: "Queries",
		Unit:        metric.Unit_COUNT,
	}
	metaReadProbeFailures = metric.Metadata{
		Name:        "kv.prober.read.failures",
		Help:        "Number of KV read probes that failed",
		Measurement: "Queries",
		Unit:        metric.Unit_COUNT,
	}
	metaReadProbeLatency = metric.Metadata{
		Name:        "kv.prober.read.latency",
		Help:        "KV read probe latency (latency of failures not tracked)",
		Measurement: "Latency",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaProbePlanAttempts = metric.Metadata{
		Name:        "kv.prober.planning_attempts",
		Help:        "Number of attempts at planning out probes made",
		Measurement: "Queries",
		Unit:        metric.Unit_COUNT,
	}
	metaProbePlanFailures = metric.Metadata{
		Name:        "kv.prober.planning_failures",
		Help:        "Number of attempts at planning out probes that failed",
		Measurement: "Queries",
		Unit:        metric.Unit_COUNT,
	}
)

type Metrics struct {
	ReadProbeAttempts *metric.Counter
	ReadProbeFailures *metric.Counter
	ReadProbeLatency  *metric.Histogram
	ProbePlanAttempts *metric.Counter
	ProbePlanFailures *metric.Counter
}

func NewProber(opts ProberOpts) *Prober {
	return &Prober{
		ambientCtx: opts.AmbientCtx,
		db: opts.DB,
		settings: opts.Settings,

		Planner: newMeta2Planner(opts.DB, opts.Settings),
		Metrics: Metrics{
			ReadProbeAttempts: metric.NewCounter(metaReadProbeAttempts),
			ReadProbeFailures: metric.NewCounter(metaReadProbeFailures),
			ReadProbeLatency:  metric.NewLatency(metaReadProbeLatency, opts.HistogramWindowInterval),
			ProbePlanAttempts: metric.NewCounter(metaProbePlanAttempts),
			ProbePlanFailures: metric.NewCounter(metaProbePlanFailures),
		},
	}
}

func (p *Prober) Start(ctx context.Context, stopper *stop.Stopper) {
	stopper.RunAsyncTask(ctx, "probe loop", func(ctx context.Context) {
		ambient := p.ambientCtx
		ambient.AddLogTag("kvprober", nil)

		ctx, cancel := stopper.WithCancelOnQuiesce(context.Background())
		defer cancel()

		ctx, sp := ambient.AnnotateCtxWithSpan(ctx, "probe loop")
		defer sp.Finish()

		ticker := time.NewTicker(withJitter(ReadInterval.Get(&p.settings.SV), rand.Intn))
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
			case <-stopper.ShouldQuiesce():
				return
			}

			// Jitter added to de-synchronize different nodes' probe loops.
			ticker.Reset(withJitter(ReadInterval.Get(&p.settings.SV), rand.Intn))

			p.probe(ctx, p.db)
		}
	})
}

type db interface {
	Get(ctx context.Context, key interface{}) (kv.KeyValue, error)
}

// Doesn't return an error. Instead increments error type specific metrics.
func (p *Prober) probe(ctx context.Context, db db) {
	defer logcrash.RecoverAndReportNonfatalPanic(ctx, &p.settings.SV)

	if !ReadEnabled.Get(&p.settings.SV) {
		return
	}

	p.Metrics.ProbePlanAttempts.Inc(1)

	pl, err := p.Planner.Plan(ctx)
	if err != nil {
		log.Health.Errorf(ctx, "can't make a plan: %v", err)
		p.Metrics.ProbePlanFailures.Inc(1)
		return
	}

	// If errors above the KV scan, then this counter won't be incremented.
	// This means that ReadProbeErrors / ReadProbeAttempts captures the KV
	// error rate only as desired. It also means that operators can alert on
	// an unexpectedly low rate of ReadProbeAttempts or else a high rate of
	// ProbePlanFailures. This would probably be a ticket alerting as
	// the impact is more low visibility into possible failures than a high
	// impact production issue.
	p.Metrics.ReadProbeAttempts.Inc(1)

	start := time.Now()

	// Slow enough response times are not different than errors from the
	// perspective of the user.
	ctx, cancel := context.WithTimeout(ctx, ReadTimeout.Get(&p.settings.SV))
	defer cancel()

	// We read the start key for the range. There may be no data at the key,
	// but that is okay. Even if there is no data at the key, the prober still
	// executes a basic read operation on the range.
	// TODO(josh): Trace all probes in production.
	_, err = db.Get(ctx, pl.StartKey)
	if err != nil {
		log.Health.Errorf(ctx, "kv.Get(%v), r=%v failed with: %v", roachpb.PrettyPrintKey(nil, pl.StartKey.AsRawKey()), pl.RangeID, err)
		p.Metrics.ReadProbeFailures.Inc(1)
		return
	}

	runtime := time.Since(start)
	log.Health.Infof(ctx, "kv.Get(%v), r=%v returned success in %v", roachpb.PrettyPrintKey(nil, pl.StartKey.AsRawKey()), pl.RangeID, runtime)

	// Latency of failures is not recorded. They are counted as failures tho.
	p.Metrics.ReadProbeLatency.RecordValue(runtime.Nanoseconds())
}

// Returns a random duration pulled from the uniform distribution given below:
// [d - 0.2*d, d + 0.2*d]
func withJitter(d time.Duration, intn func(n int) int) time.Duration {
	jitter := time.Duration(intn(int(d.Milliseconds()/5)))*time.Millisecond
	if intn(2) == 1 {
		return d + jitter
	}
	return d - jitter
}
