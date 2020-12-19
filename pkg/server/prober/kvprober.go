package prober

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

// TODO(josh): Should this live in pkg/kv instead of pkg/server? I put it in
// pkg/server since it depends on *sql.InternalExecutor. Well to be more exact,
// *one* of the proposed ways of planning out probes depends on sql.
type KVProber struct {
	ambientCtx log.AmbientContext
	db *kv.DB
	// TODO(josh): If we were to go with such an approach, would this process
	// live in the SQL pods or the KV pods? It feels wrong to me for this to live
	// in the SQL pods, as there is one of them per tenant and the code under test
	// is the KV layer. OTOH, do the KV pods have access to the SQL internal
	// executor?
	internalExecutor *sql.InternalExecutor
	settings *cluster.Settings

	// TODO(josh): Public to check from test. Better way?
	Metrics              Metrics
	histogramWindowInterval time.Duration

	indexIntoMeta2 roachpb.Key
}

type KVProberOpts struct {
	AmbientCtx log.AmbientContext
	DB *kv.DB
	InternalExecutor *sql.InternalExecutor
	Settings         *cluster.Settings
	HistogramWindowInterval time.Duration
}

// TODO(josh): Public to set from test. Better way?
var ReadEnabled = settings.RegisterBoolSetting(
	"kv.prober.read.enabled",
	"whether the KV read prober is enabled",
	false)

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
	// TODO(josh): I think something like the below two metrics will be useful.
	// Idea is: On each probe iteration a range is randomly selected for probing. Go
	// from start key for that range to a value in the interval [0, 1] where 0 means
	// the first addressable key in the cluster and 1 means the last addressable key.
	// This lets us see the distribution of failures across the keyspace in a
	// heat map. I have't implemented this; I wonder what folks think about the
	// idea. Is location in keyspace the right thing to measure or would location
	// in the "rangespace" be better?
	metaReadProbeAttemptsDistribution = metric.Metadata{
		Name:        "kv.prober.read.attempts.distribution",
		Help:        "Distribution of KV read probe attempts across keyspace",
		Measurement: "Normalized location in keyspace ([0, 1])",
		Unit:        metric.Unit_UNSET,
	}
	metaReadProbeFailureDistribution = metric.Metadata{
		Name:        "kv.prober.read.failure.distribution",
		Help:        "Distribution of KV read probe failures across keyspace",
		Measurement: "Normalized location in keyspace ([0, 1])",
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

func NewKVProber(opts KVProberOpts) *KVProber {
	return &KVProber{
		ambientCtx: opts.AmbientCtx,
		db: opts.DB,
		internalExecutor: opts.InternalExecutor,
		settings: opts.Settings,

		Metrics: Metrics{
			ReadProbeAttempts: metric.NewCounter(metaReadProbeAttempts),
			ReadProbeFailures: metric.NewCounter(metaReadProbeFailures),
			ReadProbeLatency:  metric.NewLatency(metaReadProbeLatency, opts.HistogramWindowInterval),
			ProbePlanAttempts: metric.NewCounter(metaProbePlanAttempts),
			ProbePlanFailures: metric.NewCounter(metaProbePlanFailures),
		},

		// TODO(josh): Scanning from start key of range ID 1 leads to following error:
		// start key in [/Min,/System/NodeLiveness) must be greater than LocalMax
		// I don't understand the situation exactly. Is this an okay way to avoid
		// the problem?
		indexIntoMeta2: keys.Meta2Prefix.Next(),
	}
}

// TODO(josh): Not sure what the standard way to recover from panics with
// Stopper is. I see stopper.Recover() but that panics after logging the
// panic IIUC.
func (p *KVProber) Start(ctx context.Context, stopper *stop.Stopper) {
	// TODO(josh): Make possible to turn on & off without requiring a CRDB node
	// restart.
	if !ReadEnabled.Get(&p.settings.SV) {
		return
	}
	log.Infof(ctx, "Starting KV prober")

	stopper.RunWorker(ctx, func(ctx context.Context) {
		ambient := p.ambientCtx
		ambient.AddLogTag("kv-prober-read-loop", nil)

		ctx, cancel := stopper.WithCancelOnStop(context.Background())
		defer cancel()

		ctx, sp := ambient.AnnotateCtxWithSpan(ctx, "kv prober probe loop")
		defer sp.Finish()

		// TODO(josh): Maybe the cluster setting shouldn't be a time between probes
		// per node but instead a QPS target for the cluster as a whole. This sounds
		// like what the operator cares? Well, I'm not sure. As the cluster gets
		// bigger, do we want to do more probing (there are more resource available
		// to probe with & (probably) a bigger keyspace to cover)?
		ticker := time.NewTicker(ReadInterval.Get(&p.settings.SV))
		defer ticker.Stop()

		var plan Plan

		for {
			select {
			case <-ticker.C:
			case <-stopper.ShouldStop():
				return
			}

			// TODO(josh): Is it cheap to read a cluster setting?
			ticker.Reset(ReadInterval.Get(&p.settings.SV))

			if len(plan) == 0 {
				log.Infof(ctx, "Making a plan")
				p.Metrics.ProbePlanAttempts.Inc(1)
				var err error
				plan, err = p.MakePlan(ctx)
				if err != nil {
					log.Errorf(ctx, "Can't make a plan : %v", err)
					p.Metrics.ProbePlanFailures.Inc(1)
					continue
				}
			}

			rangeID := plan[0].RangeID
			startKey := plan[0].StartKey
			endKey := plan[0].EndKey

			// TODO(josh): This is too noisy. Do folks think it's reasonable though
			// to log at a lower log level? Or else only log at info but only if a
			// cluster setting is set?
			log.Infof(ctx, "Sending a probe -> range=%v, start=%v, end=%v", rangeID, startKey, endKey)

			// If errors above the KV scan, then this counter won't be incremented.
			// This means that ReadProbeErrors / ReadProbeAttempts captures the KV
			// error rate only as desired. It also means that operators can alert on
			// an unexpectedly low rate of ReadProbeAttempts or else a high rate of
			// ReadProbeInternalFailures. This would probably be a ticket alerting as
			// the impact is more low visibility into failures than a high impact
			// production issue.
			p.Metrics.ReadProbeAttempts.Inc(1)

			plan = plan[1:]

			start := time.Now()

			// We want to read *some* key in the range. We don't know which keys are
			// present though. So we do a scan with a rows returned limit of 1. Note
			// that this latches the whole range. Tobias points out that this could
			// disrupt user traffic. Tobias also points out it leads the prober
			// to detect overly long latching done by users, which is (arguably?) a
			// good thing.
			// TODO(josh): We should add a timeout. If query takes long enough, it
			// should count as an error from an SLI perspective.
			// TODO(josh): Can we trace all probes in production? That sounds useful.
			_, err := p.db.Scan(ctx, startKey, endKey, 1)
			if err != nil {
				log.Errorf(ctx, "Probe failed with %v", err)
				p.Metrics.ReadProbeFailures.Inc(1)
				continue
			}

			runtime := time.Since(start)
			log.Infof(ctx, "Probe returned success in %v", runtime)

			// Latency of failures is not recorded. They are counted as failures tho.
			p.Metrics.ReadProbeLatency.RecordValue(runtime.Nanoseconds())
		}
	})
}
