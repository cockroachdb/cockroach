package prober

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

// TODO(josh): Should this live in pkg/kv instead of pkg/server? I put it in
// pkg/server since it depends on *sql.InternalExecutor.
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
	metaReadProbeInternalFailures = metric.Metadata{
		Name:        "kv.prober.read.internal_failures",
		Help:        "Number of attempts at sending a KV read probe not made due to internal failures",
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
	// in the "range space" be better?
	metaReadProbeAttemptsDistribution = metric.Metadata{
		Name:        "kv.prober.read.attempts.distribution",
		Help:        "Distribution of KV read probe attempts across keyspace (expect uniform)",
		Measurement: "Normalized location in keyspace ([0, 1])",
		Unit:        metric.Unit_UNSET,
	}
	metaReadProbeFailureDistribution = metric.Metadata{
		Name:        "kv.prober.read.failure.distribution",
		Help:        "Distribution of KV read probe failures across keyspace",
		Measurement: "Normalized location in keyspace ([0, 1])",
		Unit:        metric.Unit_UNSET,
	}
)

type Metrics struct {
	ReadProbeAttempts *metric.Counter
	ReadProbeFailures *metric.Counter
	ReadProbeInternalFailures *metric.Counter
	ReadProbeLatency  *metric.Histogram
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
			ReadProbeInternalFailures: metric.NewCounter(metaReadProbeInternalFailures),
			ReadProbeLatency:  metric.NewLatency(metaReadProbeLatency, opts.HistogramWindowInterval),
		},
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

		for {
			select {
			case <-ticker.C:
			case <-stopper.ShouldStop():
				return
			}

			// TODO(josh): Is it cheap to read a cluster setting?
			ticker.Reset(ReadInterval.Get(&p.settings.SV))

			// TODO(josh): This is too noisy. Do folks think it's reasonable though
			// to log at a lower log level? Or else only log at info but only if a
			// cluster setting is set?
			log.Infof(ctx, "Sending a probe")

			// TODO(josh): Beyond correctness, we have three design goals for our
			// approach to probabilistically selecting a place in the keyspace to probe:
			// 1. That the approach is efficient enough.
			// 2. That the approach is available enough in times of outage that the
			//    prober is able to generate useful signal when we need it most.
			// 3. That the approach doesn't unnecessarily make quiescent ranges active.
			//
			// 1: Calling crdb_internal.ranges_no_leases could be made more efficient;
			//    we could not materialize the whole table in memory; also we could
			//    decide on a random range to probe without the very inefficient
			//    ORDER BY random() construct; surely there is more.
			// 2: First, do we agree that 2 is a good goal? I can imagine a test
			//    in kvprober_test.go that attempts to make goal 2 concrete by
			//    breaking node liveness. Maybe certain failures (e.g. extended
			//    meta1/2 unavailability) will stop the prober from probing KV but it
			//    seems desirable to me to minimize how often this happens. Thoughts?
			// 3: Do we agree 3 is a goal? Is it a hard requirement? I expect it
			//    is.
			//
			// Raphael has suggested using rangecache instead of
			// crdb_internal.ranges_no_leases. Does that hit all the above
			// goals? I can't tell what the deal with rangecache & quiescent ranges.
			// rangecache looks more efficient & available than
			// crdb_internal.ranges_no_leases. I can't tell if rangecache depends on
			// nodes being live or not. Also is rangecache empty at server restart
			// time? I don't think that's desirable.
			row, err := p.internalExecutor.QueryRowEx(
				ctx, "kvprober", nil, /* txn */
				sessiondata.InternalExecutorOverride{User: security.NodeUserName()},
				// TODO(josh): Probing for range ID 1 leads to following error:
				// start key in [/Min,/System/NodeLiveness) must be greater than LocalMax
				// I don't understand the situation exactly. Is filtering out range ID
				// 1 sufficient to avoid the problem?
				"SELECT * FROM crdb_internal.ranges_no_leases WHERE range_id != 1 ORDER BY random() LIMIT 1;",
			)
			if err != nil {
				log.Errorf(ctx, "Can't select a range to probe: %v", err)
				p.Metrics.ReadProbeInternalFailures.Inc(1)
				continue
			}

			// TODO(josh): Should I use the versions of these functions that don't
			// panic instead? Assuming we will add recovery above this anyway.
			// Wondering what typical CRDB style is around panics.
			rangeID := tree.MustBeDInt(row[0])
			startKey := tree.MustBeDBytes(row[1])
			endKey := tree.MustBeDBytes(row[3])

			log.Infof(ctx, "Probe plan -> range=%v, start=%v, end=%v", rangeID, startKey.String(), endKey.String())

			// If errors above the KV scan, then this counter won't be incremented.
			// This means that ReadProbeErrors / ReadProbeAttempts captures the KV
			// error rate only as desired. It also means that operators can alert on
			// an unexpectedly low rate of ReadProbeAttempts or else a high rate of
			// ReadProbeInternalFailures. This would probably be a ticket alerting as
			// the impact is more low visibility into failures than a high impact
			// production issue.
			p.Metrics.ReadProbeAttempts.Inc(1)

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
			_, err = p.db.Scan(ctx, []byte(startKey), []byte(endKey), 1)
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
