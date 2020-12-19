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

// TODO(josh): In CC land, I'd probably write two kinds of tests for something
// like this prober. One set would be integration tests like these; they may be focused
// only on the happy path. Another set would be more unit tests where deps
// such as internal SQL executor & *kv.DB are mocked out. What's the typical
// style in the CRDB repo? It's quite fun using serverutils.StartServer and the
// testing knobs. It's quite amazing the kinds of tests one can write.
func TestKVProber(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	t.Run("probes are disabled", func(t *testing.T) {
		// Prober defaults to off.
		s, p := initTestProber(t, base.TestingKnobs{})

		p.Start(ctx, s.Stopper())
		defer s.Stopper().Stop(ctx)

		// TODO(josh): How long of a test is this for CRDB standards?
		time.Sleep(1 * time.Second)

		require.Zero(t, p.Metrics.ReadProbeAttempts.Count())
	})

	t.Run("happy path", func(t *testing.T) {
		s, p := initTestProber(t, base.TestingKnobs{})

		prober.ReadEnabled.Override(&s.ClusterSettings().SV, true)
		prober.ReadInterval.Override(&s.ClusterSettings().SV, 5 * time.Millisecond)

		p.Start(ctx, s.Stopper())
		defer s.Stopper().Stop(ctx)

		time.Sleep(1 * time.Second)

		require.Greater(t, p.Metrics.ReadProbeAttempts.Count(), int64(0))
		require.Zero(t, p.Metrics.ReadProbeFailures.Count())
		require.Zero(t, p.Metrics.ReadProbeInternalFailures.Count())
	})

	// If meta2 is unavailable, the prober can't select ranges to probe.
	t.Run("meta2 is unavailable", func(t *testing.T) {
		mu := sync.Mutex{}
		meta2IsAvailable := true

		s, p := initTestProber(t, base.TestingKnobs{
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

		prober.ReadEnabled.Override(&s.ClusterSettings().SV, true)
		prober.ReadInterval.Override(&s.ClusterSettings().SV, 5 * time.Millisecond)

		p.Start(ctx, s.Stopper())
		defer s.Stopper().Stop(ctx)

		// Wait 500 milliseconds then make meta2 unavailable.
		time.Sleep(500 * time.Millisecond)

		mu.Lock()
		meta2IsAvailable = false
		mu.Unlock()

		time.Sleep(500 * time.Millisecond)

		// Once meta2 is made unavailable, all queries to
		// crdb_internal.ranges_no_leases will fail.
		require.Greater(t, p.Metrics.ReadProbeInternalFailures.Count(), int64(0))
		// Still, before that happens, some probes have been sent.
		require.Greater(t, p.Metrics.ReadProbeAttempts.Count(), int64(0))
	})

	// TODO(josh): Possibly the relevant range just won't get hit by
	// probes and this test will flake. Need to fix that.
	t.Run("web sessions table is unavailable", func(t *testing.T) {
		s, p := initTestProber(t, base.TestingKnobs{
			Store: &kvserver.StoreTestingKnobs{
				TestingRequestFilter: func(i context.Context, ba roachpb.BatchRequest) *roachpb.Error {
					for _, ru := range ba.Requests {
						key := ru.GetInner().Header().Key
						if bytes.HasPrefix(key, keys.SystemSQLCodec.TablePrefix(keys.WebSessionsTableID)) {
							return roachpb.NewError(fmt.Errorf("boom"))
						}
					}
					return nil
				},
			},
		})

		prober.ReadEnabled.Override(&s.ClusterSettings().SV, true)
		prober.ReadInterval.Override(&s.ClusterSettings().SV, 5 * time.Millisecond)

		p.Start(ctx, s.Stopper())
		defer s.Stopper().Stop(ctx)

		time.Sleep(1 * time.Second)

		require.Greater(t, p.Metrics.ReadProbeAttempts.Count(), int64(0))
		require.Greater(t, p.Metrics.ReadProbeFailures.Count(), int64(0))
		require.Zero(t, p.Metrics.ReadProbeInternalFailures.Count())
	})
}

func initTestProber(t * testing.T, knobs base.TestingKnobs) (serverutils.TestServerInterface, *prober.KVProber) {
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
	p.Metrics.ReadProbeInternalFailures.Clear()

	return s, p
}
