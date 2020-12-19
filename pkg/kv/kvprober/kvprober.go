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
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

type Prober struct {
	ambientCtx log.AmbientContext
	db         *kv.DB
	settings   *cluster.Settings

	Planner Planner
	Metrics Metrics
	histogramWindowInterval time.Duration
}

type ProberOpts struct {
	AmbientCtx log.AmbientContext
	DB *kv.DB
	Settings         *cluster.Settings
	HistogramWindowInterval time.Duration
	PlanNProbesAtATime int // optional
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
	// Idea is: On each probe iteration a range is randomly selected for probing. Go
	// from range ID to a value in the interval [0, 1] where 0 means
	// the first range in the cluster and 1 means the last range
	// This lets us see the distribution of failures across the "rangespace" in a
	// heat map.
	// TODO(josh): Implement the metric!
	metaReadProbeFailureDistribution = metric.Metadata{
		Name:        "kv.prober.read.failure.distribution",
		Help:        "Distribution of KV read probe failures across rangespace",
		Measurement: "Normalized location in rangespace ([0, 1])",
		Unit:        metric.Unit_UNSET,
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
	planNProbesAtATime := opts.PlanNProbesAtATime
	if planNProbesAtATime == 0 {
		planNProbesAtATime = 100 // default value
	}

	return &Prober{
		ambientCtx: opts.AmbientCtx,
		db: opts.DB,
		settings: opts.Settings,

		Planner: newMeta2Planner(opts.DB, planNProbesAtATime),
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
	stopper.RunWorker(ctx, func(ctx context.Context) {
		ambient := p.ambientCtx
		ambient.AddLogTag("kvprober", nil)

		ctx, cancel := stopper.WithCancelOnStop(context.Background())
		defer cancel()

		ctx, sp := ambient.AnnotateCtxWithSpan(ctx, "read probe loop")
		defer sp.Finish()

		ticker := time.NewTicker(withJitter(ReadInterval.Get(&p.settings.SV), rand.Intn))
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
			case <-stopper.ShouldStop():
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
	defer func() {
		if r := recover(); r != nil {
			log.Errorf(ctx, "Recovered from kvprober panic: %v", r)
		}
	}()

	if !ReadEnabled.Get(&p.settings.SV) {
		return
	}

	p.Metrics.ProbePlanAttempts.Inc(1)

	pl, err := p.Planner.Plan(ctx)
	if err != nil {
		log.Errorf(ctx, "Can't make a plan: %v", err)
		p.Metrics.ProbePlanFailures.Inc(1)
		return
	}

	// TODO(josh): Is INFO a reasonable level to log at?
	log.Infof(ctx, "Sending a read probe: range=%v, start=%v", pl.RangeID, roachpb.PrettyPrintKey(nil, pl.StartKey.AsRawKey()))

	// If errors above the KV scan, then this counter won't be incremented.
	// This means that ReadProbeErrors / ReadProbeAttempts captures the KV
	// error rate only as desired. It also means that operators can alert on
	// an unexpectedly low rate of ReadProbeAttempts or else a high rate of
	// ProbePlanFailures. This would probably be a ticket alerting as
	// the impact is more low visibility into possible failures than a high
	// impact production issue.
	p.Metrics.ReadProbeAttempts.Inc(1)

	start := time.Now()

	// We read the start key for the range. There may be no data at the key,
	// but that is okay. Even if there is no data at the key, the prober still
	// executes a basic read operation on the range.
	// TODO(josh): We should add a timeout. If query takes long enough, it
	// should count as an error from an SLI perspective.
	// TODO(josh): Trace all probes in production.
	_, err = db.Get(ctx, pl.StartKey)
	if err != nil {
		log.Errorf(ctx, "Read probe failed with: %v", err)
		p.Metrics.ReadProbeFailures.Inc(1)
		return
	}

	runtime := time.Since(start)
	log.Infof(ctx, "Read probe returned success in %v", runtime)

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
