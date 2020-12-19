// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvprober_test

import (
	"bytes"
	"context"
	gosql "database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvprober"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/security/securitytest"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
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

func TestProberDoesReads(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	t.Run("happy path", func(t *testing.T) {
		s, _, p, cleanup := initTestProber(t, base.TestingKnobs{})
		defer cleanup()

		kvprober.ReadEnabled.Override(&s.ClusterSettings().SV, true)
		kvprober.ReadInterval.Override(&s.ClusterSettings().SV, 5*time.Millisecond)

		require.NoError(t, p.Start(ctx, s.Stopper()))

		time.Sleep(1 * time.Second)

		// 1 second / probes every 5 milliseconds -> ~200 probes. Require only 50
		// probes to reduce chance of test flakes, especially given the jitter added
		// in kvprober.go.
		require.Greater(t, p.Metrics.ReadProbeAttempts.Count(), int64(50))
		require.Zero(t, p.Metrics.ReadProbeFailures.Count())
		require.Zero(t, p.Metrics.ProbePlanFailures.Count())
	})

	t.Run("a single range is unavailable", func(t *testing.T) {
		s, _, p, cleanup := initTestProber(t, base.TestingKnobs{
			Store: &kvserver.StoreTestingKnobs{
				TestingRequestFilter: func(i context.Context, ba roachpb.BatchRequest) *roachpb.Error {
					for _, ru := range ba.Requests {
						key := ru.GetInner().Header().Key
						if bytes.HasPrefix(key, keys.TimeseriesPrefix) {
							return roachpb.NewError(fmt.Errorf("boom"))
						}
					}
					return nil
				},
			},
		})
		defer cleanup()

		kvprober.ReadEnabled.Override(&s.ClusterSettings().SV, true)
		kvprober.ReadInterval.Override(&s.ClusterSettings().SV, 5*time.Millisecond)

		require.NoError(t, p.Start(ctx, s.Stopper()))

		// This is enough time to hit the time-series range multiple times, thus
		// triggering multiple probe failures.
		time.Sleep(1 * time.Second)

		// Expect >1 probe failures due to the unavailable time-series range.
		require.Greater(t, p.Metrics.ReadProbeFailures.Count(), int64(1))
		require.Zero(t, p.Metrics.ProbePlanFailures.Count())
	})

	t.Run("all ranges are unavailable except meta2", func(t *testing.T) {
		mu := syncutil.Mutex{}
		dbIsAvailable := true

		s, _, p, cleanup := initTestProber(t, base.TestingKnobs{
			Store: &kvserver.StoreTestingKnobs{
				TestingRequestFilter: func(i context.Context, ba roachpb.BatchRequest) *roachpb.Error {
					mu.Lock()
					defer mu.Unlock()
					if !dbIsAvailable {
						for _, ru := range ba.Requests {
							key := ru.GetInner().Header().Key
							// This exclusion keep planning working.
							if !bytes.HasPrefix(key, keys.Meta2Prefix) {
								return roachpb.NewError(fmt.Errorf("boom"))
							}
						}
						return nil
					}
					return nil
				},
			},
		})
		defer cleanup()

		kvprober.ReadEnabled.Override(&s.ClusterSettings().SV, true)
		kvprober.ReadInterval.Override(&s.ClusterSettings().SV, 5*time.Millisecond)

		require.NoError(t, p.Start(ctx, s.Stopper()))

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

func TestPlannerMakesPlansCoveringAllRanges(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	_, sqlDB, p, cleanup := initTestProber(t, base.TestingKnobs{})
	defer cleanup()

	var numRanges int64
	require.NoError(t, sqlDB.QueryRow(
		"SELECT count(*) FROM crdb_internal.ranges").Scan(&numRanges))
	log.Infof(ctx, "want numRanges %v", numRanges)

	rangeIDToTimesWouldBeProbed := make(map[int64]int)

	test := func(n int) {
		require.Eventually(t, func() bool {
			plan, err := p.Planner.Plan(ctx)
			require.NoError(t, err)

			rangeIDToTimesWouldBeProbed[int64(plan.RangeID)]++

			log.Infof(ctx, "current rangeID to times would be probed map: %v", rangeIDToTimesWouldBeProbed)

			for i := int64(1); i <= numRanges; i++ {
				// Expect all ranges to eventually be returned by Plan n or n+1 times.
				// Can't expect all ranges to be returned by Plan exactly n times,
				// as the order in which the lowest ordinal ranges are returned by
				// Plan the nth+1 time and the highest ordinal ranges are returned by
				// Plan planned the nth time is NOT specified. The reason for this is
				// that we make plans in batches of a constant size and then randomize
				// the order of the batch. See plan.go for more.
				if rangeIDToTimesWouldBeProbed[i] != n && rangeIDToTimesWouldBeProbed[i] != n+1 {
					return false
				}
			}
			return true
		}, time.Second, time.Millisecond)
	}
	for i := 0; i < 20; i++ {
		test(i)
	}
}

func initTestProber(
	t *testing.T, knobs base.TestingKnobs,
) (serverutils.TestServerInterface, *gosql.DB, *kvprober.Prober, func()) {

	s, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{
		Settings: cluster.MakeClusterSettings(),
		Knobs:    knobs,
	})
	p := kvprober.NewProber(kvprober.ProberOpts{
		AmbientCtx: log.AmbientContext{
			Tracer: tracing.NewTracer(),
		},
		DB:                      kvDB,
		HistogramWindowInterval: time.Minute, // actual value not important to test
		Settings:                s.ClusterSettings(),
	})

	// Given small test cluster, this better exercises the planning logic.
	kvprober.PlanNProbesAtATime.Override(&s.ClusterSettings().SV, 10)

	p.Metrics.ReadProbeAttempts.Clear()
	p.Metrics.ReadProbeFailures.Clear()
	p.Metrics.ProbePlanAttempts.Clear()
	p.Metrics.ProbePlanFailures.Clear()

	return s, sqlDB, p, func() {
		s.Stopper().Stop(context.Background())
	}
}
