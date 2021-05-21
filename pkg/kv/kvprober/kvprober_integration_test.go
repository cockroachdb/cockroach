// Copyright 2021 The Cockroach Authors.
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
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvprober"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestProberDoesReads(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderShort(t)

	ctx := context.Background()

	t.Run("disabled by default", func(t *testing.T) {
		s, _, p, cleanup := initTestProber(t, base.TestingKnobs{})
		defer cleanup()

		kvprober.ReadInterval.Override(ctx, &s.ClusterSettings().SV, 5*time.Millisecond)

		require.NoError(t, p.Start(ctx, s.Stopper()))

		time.Sleep(100 * time.Millisecond)

		require.Zero(t, p.Metrics().ProbePlanAttempts.Count())
		require.Zero(t, p.Metrics().ReadProbeAttempts.Count())
	})

	t.Run("happy path", func(t *testing.T) {
		s, _, p, cleanup := initTestProber(t, base.TestingKnobs{})
		defer cleanup()

		kvprober.ReadEnabled.Override(ctx, &s.ClusterSettings().SV, true)
		kvprober.ReadInterval.Override(ctx, &s.ClusterSettings().SV, 5*time.Millisecond)

		require.NoError(t, p.Start(ctx, s.Stopper()))

		testutils.SucceedsSoon(t, func() error {
			if p.Metrics().ReadProbeAttempts.Count() < int64(50) {
				return errors.Newf("probe count too low: %v", p.Metrics().ReadProbeAttempts.Count())
			}
			return nil
		})
		require.Zero(t, p.Metrics().ReadProbeFailures.Count())
		require.Zero(t, p.Metrics().ProbePlanFailures.Count())
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

		kvprober.ReadEnabled.Override(ctx, &s.ClusterSettings().SV, true)
		kvprober.ReadInterval.Override(ctx, &s.ClusterSettings().SV, 5*time.Millisecond)

		require.NoError(t, p.Start(ctx, s.Stopper()))

		// Expect >=2 failures eventually due to unavailable time-series range.
		// TODO(josh): Once structured logging is in, can check that failures
		// involved only the time-series range.
		testutils.SucceedsSoon(t, func() error {
			if p.Metrics().ReadProbeFailures.Count() < int64(2) {
				return errors.Newf("error count too low: %v", p.Metrics().ReadProbeFailures.Count())
			}
			return nil
		})
		require.Zero(t, p.Metrics().ProbePlanFailures.Count())
	})

	t.Run("all ranges are unavailable for Gets", func(t *testing.T) {
		mu := syncutil.Mutex{}
		dbIsAvailable := true

		s, _, p, cleanup := initTestProber(t, base.TestingKnobs{
			Store: &kvserver.StoreTestingKnobs{
				TestingRequestFilter: func(i context.Context, ba roachpb.BatchRequest) *roachpb.Error {
					mu.Lock()
					defer mu.Unlock()
					if !dbIsAvailable {
						for _, ru := range ba.Requests {
							// Planning depends on Scan so only returning an error on Get
							// keeps planning working.
							if ru.GetGet() != nil {
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

		// Want server to startup successfully then become unavailable.
		mu.Lock()
		dbIsAvailable = false
		mu.Unlock()

		kvprober.ReadEnabled.Override(ctx, &s.ClusterSettings().SV, true)
		kvprober.ReadInterval.Override(ctx, &s.ClusterSettings().SV, 5*time.Millisecond)

		// Probe exactly ten times so we can make assertions below.
		for i := 0; i < 10; i++ {
			p.Probe(ctx, s.DB())
		}

		// Expect all probes to fail but planning to succeed.
		require.Equal(t, int64(10), p.Metrics().ReadProbeAttempts.Count())
		require.Equal(t, int64(10), p.Metrics().ReadProbeFailures.Count())
		require.Zero(t, p.Metrics().ProbePlanFailures.Count())
	})
}

func TestPlannerMakesPlansCoveringAllRanges(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderShort(t)

	ctx := context.Background()
	_, sqlDB, p, cleanup := initTestProber(t, base.TestingKnobs{})
	defer cleanup()

	rangeIDToTimesWouldBeProbed := make(map[int64]int)

	test := func(n int) {
		var numRanges int64
		if err := sqlDB.QueryRow(
			"SELECT count(*) FROM crdb_internal.ranges").Scan(&numRanges); err != nil {
			require.True(t, false)
		}
		log.Infof(ctx, "want numRanges %v", numRanges)

		require.Eventually(t, func() bool {
			step, err := p.PlannerNext(ctx)
			require.NoError(t, err)

			rangeIDToTimesWouldBeProbed[int64(step.RangeID)]++

			log.Infof(ctx, "current rangeID to times would be probed map: %v", rangeIDToTimesWouldBeProbed)

			for i := int64(1); i <= numRanges; i++ {
				// Expect all ranges to eventually be returned by Next n or n+1 times.
				// Can't expect all ranges to be returned by Next exactly n times,
				// as the order in which the lowest ordinal ranges are returned by
				// Next the nth+1 time and the highest ordinal ranges are returned by
				// Next the nth time is NOT specified. The reason for this is
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
	p := kvprober.NewProber(kvprober.Opts{
		AmbientCtx: log.AmbientContext{
			Tracer: tracing.NewTracer(),
		},
		DB:                      kvDB,
		HistogramWindowInterval: time.Minute, // actual value not important to test
		Settings:                s.ClusterSettings(),
	})

	// Given small test cluster, this better exercises the planning logic.
	kvprober.NumStepsToPlanAtOnce.Override(context.Background(), &s.ClusterSettings().SV, 10)
	// Want these tests to run as fast as possible; see planner_test.go for a
	// unit test of the rate limiting.
	p.SetPlanningRateLimit(0)

	return s, sqlDB, p, func() {
		s.Stopper().Stop(context.Background())
	}
}
