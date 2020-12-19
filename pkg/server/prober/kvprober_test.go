package prober_test

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/security/securitytest"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/server/prober"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	security.SetAssetLoader(securitytest.EmbeddedAssets)
	randutil.SeedForTests()
	serverutils.InitTestServerFactory(server.TestServerFactory)
	serverutils.InitTestClusterFactory(testcluster.TestClusterFactory)
	os.Exit(m.Run())
}

// TODO(josh): In CC land, I might write two kinds of tests for something
// like this prober. One set would be integration tests like these; they may be focused
// only on the happy path. Another set would be more unit tests where deps
// such as internal SQL executor & *kv.DB are mocked out. What's the typical
// style in the CRDB repo? It's quite fun using serverutils.StartServer and the
// testing knobs. It's quite amazing the kinds of tests one can write.
func TestProberDoesReads(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	t.Run("probes are disabled", func(t *testing.T) {
		// Prober defaults to off.
		s, p, cleanup := initTestProber(t, base.TestingKnobs{})
		defer cleanup()

		p.Start(ctx, s.Stopper())

		// TODO(josh): How long of a test is this for CRDB standards?
		time.Sleep(1 * time.Second)

		require.Zero(t, p.Metrics.ReadProbeAttempts.Count())
		require.Zero(t, p.Metrics.ProbePlanAttempts.Count())
	})

	t.Run("happy path", func(t *testing.T) {
		s, p, cleanup := initTestProber(t, base.TestingKnobs{})
		defer cleanup()

		prober.ReadEnabled.Override(&s.ClusterSettings().SV, true)
		prober.ReadInterval.Override(&s.ClusterSettings().SV, 5 * time.Millisecond)

		p.Start(ctx, s.Stopper())

		time.Sleep(1 * time.Second)

		// 1 second / probes every 5 milliseconds -> ~200 probes. Require only 100
		// probes to reduce chance of test flakes.
		require.Greater(t, p.Metrics.ReadProbeAttempts.Count(), int64(100))
		require.Zero(t, p.Metrics.ReadProbeFailures.Count())
		require.Zero(t, p.Metrics.ProbePlanFailures.Count())
	})

	// If meta2 is unavailable, the prober can't plan out which ranges to probe.
	t.Run("meta2 is unavailable", func(t *testing.T) {
		mu := sync.Mutex{}
		meta2IsAvailable := true

		s, p, cleanup := initTestProber(t, base.TestingKnobs{
			Store: &kvserver.StoreTestingKnobs{
				TestingRequestFilter: func(i context.Context, ba roachpb.BatchRequest) *roachpb.Error {
					mu.Lock()
					defer mu.Unlock()
					if !meta2IsAvailable {
						for _, ru := range ba.Requests {
							key := ru.GetInner().Header().Key
							if bytes.HasPrefix(key, keys.Meta2Prefix) {
								return roachpb.NewError(fmt.Errorf("boom"))
							}
						}
					}
					return nil
				},
			},
		})
		defer cleanup()

		prober.ReadEnabled.Override(&s.ClusterSettings().SV, true)
		prober.ReadInterval.Override(&s.ClusterSettings().SV, 5 * time.Millisecond)

		p.Start(ctx, s.Stopper())

		// Wait 500 milliseconds then make meta2 unavailable.
		time.Sleep(500 * time.Millisecond)

		mu.Lock()
		meta2IsAvailable = false
		mu.Unlock()

		time.Sleep(500 * time.Millisecond)

		// Expect 50% error rate but require >25% errors to reduce chance of test flake.
		require.Greater(t, errorRate(p.Metrics.ProbePlanFailures, p.Metrics.ProbePlanAttempts), 0.25)
	})

	// If everything is unavailable except met2 & descriptors, nearly all
	// probes sent by the prober will fail (though planning will succeed).
	t.Run("everything is unavailable except meta2 & descriptors", func(t *testing.T) {
		mu := sync.Mutex{}
		dbIsAvailable := true

		s, p, cleanup := initTestProber(t, base.TestingKnobs{
			Store: &kvserver.StoreTestingKnobs{
				TestingRequestFilter: func(i context.Context, ba roachpb.BatchRequest) *roachpb.Error {
					mu.Lock()
					defer mu.Unlock()
					if !dbIsAvailable {
						for _, ru := range ba.Requests {
							key := ru.GetInner().Header().Key
							// These exclusions keep planning working. An above test breaks
							// planning; this test tests the actual probes failing.
							if bytes.HasPrefix(key, keys.Meta2Prefix) {
								return nil
							}
							// TODO(josh): crdb_internal.ranges_no_leases depends on this system
							// table. The prober doesn't actually need any data from the system
							// table. Directly querying meta2 via *kv.DB avoids this dep. If we
							// go with that approach, we should delete the below lines.
							if bytes.HasPrefix(key, keys.SystemSQLCodec.TablePrefix(keys.DescriptorTableID)) {
								return nil
							}
							return roachpb.NewError(fmt.Errorf("boom"))
						}
					}
					return nil
				},
			},
		})
		defer cleanup()

		prober.ReadEnabled.Override(&s.ClusterSettings().SV, true)
		prober.ReadInterval.Override(&s.ClusterSettings().SV, 5 * time.Millisecond)

		p.Start(ctx, s.Stopper())

		// Wait 500 milliseconds then make DB (mostly) unavailable.
		time.Sleep(500 * time.Millisecond)

		mu.Lock()
		dbIsAvailable = false
		mu.Unlock()

		time.Sleep(500 * time.Millisecond)

		// Expect 50% error rate but require >25% errors to reduce chance of test flake.
		require.Greater(t, errorRate(p.Metrics.ReadProbeFailures, p.Metrics.ReadProbeAttempts), 0.25)
		require.Zero(t, p.Metrics.ProbePlanFailures.Count())
	})
}

func errorRate(errorCount *metric.Counter, overallCount *metric.Counter) float64 {
	return float64(errorCount.Count()) / float64(overallCount.Count())
}

func TestPlannerMakesPlansEvenlyCoveringAllRanges(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	_, p, cleanup := initTestProber(t, base.TestingKnobs{})
	defer cleanup()

	rangeIDToTimesWouldBeProbed := make(map[int64]int)

	require.Eventually(t, func() bool {
		plan, err := p.MakePlan(ctx)
		require.NoError(t, err)

		for _, pl := range plan {
			rangeIDToTimesWouldBeProbed[int64(pl.RangeID)]++
		}

		log.Infof(ctx, "current rangeID to times would be probed map: %v", rangeIDToTimesWouldBeProbed)

		// TODO(josh): Query the DB to get number of ranges in the test cluster,
		// instead of hard coding.
		for i := int64(1); i < 35; i++ {
			// Expect all ranges to eventually be included in a plan a single time.
			if rangeIDToTimesWouldBeProbed[i] != 1 {
				return false
			}
		}
		return true
	}, time.Second, time.Millisecond)
}

func initTestProber(t * testing.T, knobs base.TestingKnobs) (serverutils.TestServerInterface, *prober.KVProber, func()) {
	s, _, kvDB := serverutils.StartServer(t, base.TestServerArgs{
		Settings: cluster.MakeClusterSettings(),
		Knobs: knobs,
	})
	p := prober.NewKVProber(prober.KVProberOpts{
		AmbientCtx: log.AmbientContext{
			Tracer:  tracing.NewTracer(),
		},
		DB: kvDB,
		InternalExecutor: s.InternalExecutor().(*sql.InternalExecutor),
		HistogramWindowInterval: time.Minute, // actual value not important to test
		Settings: s.ClusterSettings(),
	})

	p.Metrics.ReadProbeAttempts.Clear()
	p.Metrics.ReadProbeFailures.Clear()
	p.Metrics.ProbePlanAttempts.Clear()
	p.Metrics.ProbePlanFailures.Clear()

	return s, p, func() {
		s.Stopper().Stop(context.Background())
	}
}
