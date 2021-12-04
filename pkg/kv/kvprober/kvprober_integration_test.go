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

func TestProberDoesReadsAndWrites(t *testing.T) {
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
		require.Zero(t, p.Metrics().WriteProbeAttempts.Count())
	})

	t.Run("happy path", func(t *testing.T) {
		s, _, p, cleanup := initTestProber(t, base.TestingKnobs{})
		defer cleanup()

		kvprober.ReadEnabled.Override(ctx, &s.ClusterSettings().SV, true)
		kvprober.ReadInterval.Override(ctx, &s.ClusterSettings().SV, 5*time.Millisecond)

		kvprober.WriteEnabled.Override(ctx, &s.ClusterSettings().SV, true)
		kvprober.WriteInterval.Override(ctx, &s.ClusterSettings().SV, 5*time.Millisecond)

		require.NoError(t, p.Start(ctx, s.Stopper()))

		testutils.SucceedsSoon(t, func() error {
			if p.Metrics().ReadProbeAttempts.Count() < int64(50) {
				return errors.Newf("read count too low: %v", p.Metrics().ReadProbeAttempts.Count())
			}
			if p.Metrics().WriteProbeAttempts.Count() < int64(50) {
				return errors.Newf("write count too low: %v", p.Metrics().WriteProbeAttempts.Count())
			}
			return nil
		})
		require.Zero(t, p.Metrics().ReadProbeFailures.Count())
		require.Zero(t, p.Metrics().WriteProbeFailures.Count())
		require.Zero(t, p.Metrics().ProbePlanFailures.Count())
	})

	t.Run("a single range is unavailable for all KV ops", func(t *testing.T) {
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

		kvprober.WriteEnabled.Override(ctx, &s.ClusterSettings().SV, true)
		kvprober.WriteInterval.Override(ctx, &s.ClusterSettings().SV, 5*time.Millisecond)

		require.NoError(t, p.Start(ctx, s.Stopper()))

		// Expect >=2 failures eventually due to unavailable time-series range.
		// TODO(josh): Once structured logging is in, can check that failures
		// involved only the time-series range.
		testutils.SucceedsSoon(t, func() error {
			if p.Metrics().ReadProbeFailures.Count() < int64(2) {
				return errors.Newf("read error count too low: %v", p.Metrics().ReadProbeFailures.Count())
			}
			if p.Metrics().WriteProbeFailures.Count() < int64(2) {
				return errors.Newf("write error count too low: %v", p.Metrics().WriteProbeFailures.Count())
			}
			return nil
		})
		require.Zero(t, p.Metrics().ProbePlanFailures.Count())
	})

	t.Run("all ranges are unavailable for Gets only", func(t *testing.T) {
		var dbIsAvailable syncutil.AtomicBool
		dbIsAvailable.Set(true)

		s, _, p, cleanup := initTestProber(t, base.TestingKnobs{
			Store: &kvserver.StoreTestingKnobs{
				TestingRequestFilter: func(i context.Context, ba roachpb.BatchRequest) *roachpb.Error {
					if !dbIsAvailable.Get() {
						for _, ru := range ba.Requests {
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
		dbIsAvailable.Set(false)

		kvprober.ReadEnabled.Override(ctx, &s.ClusterSettings().SV, true)
		kvprober.ReadInterval.Override(ctx, &s.ClusterSettings().SV, 5*time.Millisecond)

		kvprober.WriteEnabled.Override(ctx, &s.ClusterSettings().SV, true)
		kvprober.WriteInterval.Override(ctx, &s.ClusterSettings().SV, 5*time.Millisecond)

		// Probe exactly ten times so we can make assertions below.
		for i := 0; i < 10; i++ {
			p.ReadProbe(ctx, s.DB())
			p.WriteProbe(ctx, s.DB())
		}

		// Expect all read probes to fail but write probes & planning to succeed.
		require.Equal(t, int64(10), p.Metrics().ReadProbeAttempts.Count())
		require.Equal(t, int64(10), p.Metrics().ReadProbeFailures.Count())

		require.Equal(t, int64(10), p.Metrics().WriteProbeAttempts.Count())
		require.Zero(t, p.Metrics().WriteProbeFailures.Count())

		require.Zero(t, p.Metrics().ProbePlanFailures.Count())
	})
	t.Run("all ranges are unavailable for Puts only", func(t *testing.T) {
		var dbIsAvailable syncutil.AtomicBool
		dbIsAvailable.Set(true)

		s, _, p, cleanup := initTestProber(t, base.TestingKnobs{
			Store: &kvserver.StoreTestingKnobs{
				TestingRequestFilter: func(i context.Context, ba roachpb.BatchRequest) *roachpb.Error {
					if !dbIsAvailable.Get() {
						for _, ru := range ba.Requests {
							if ru.GetPut() != nil {
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
		dbIsAvailable.Set(false)

		kvprober.ReadEnabled.Override(ctx, &s.ClusterSettings().SV, true)
		kvprober.ReadInterval.Override(ctx, &s.ClusterSettings().SV, 5*time.Millisecond)

		kvprober.WriteEnabled.Override(ctx, &s.ClusterSettings().SV, true)
		kvprober.WriteInterval.Override(ctx, &s.ClusterSettings().SV, 5*time.Millisecond)

		// Probe exactly ten times so we can make assertions below.
		for i := 0; i < 10; i++ {
			p.ReadProbe(ctx, s.DB())
			p.WriteProbe(ctx, s.DB())
		}

		// Expect all write probes to fail but read probes & planning to succeed.
		require.Equal(t, int64(10), p.Metrics().WriteProbeAttempts.Count())
		require.Equal(t, int64(10), p.Metrics().WriteProbeFailures.Count())

		require.Equal(t, int64(10), p.Metrics().ReadProbeAttempts.Count())
		require.Zero(t, p.Metrics().ReadProbeFailures.Count())

		require.Zero(t, p.Metrics().ProbePlanFailures.Count())
	})
}

func TestWriteProbeDoesNotLeaveLiveData(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderShort(t)

	ctx := context.Background()

	s, _, p, cleanup := initTestProber(t, base.TestingKnobs{})
	defer cleanup()

	kvprober.WriteEnabled.Override(ctx, &s.ClusterSettings().SV, true)

	lastStep := p.WriteProbeReturnLastStep(ctx, s.DB())

	// Expect write probe to succeed.
	require.Equal(t, int64(1), p.Metrics().WriteProbeAttempts.Count())
	require.Zero(t, p.Metrics().WriteProbeFailures.Count())
	require.Zero(t, p.Metrics().ProbePlanFailures.Count())

	// Expect no **live** data at the key kvprober writes at.
	// TODO(josh): One can imagine comparing a checksum of all the live data
	// in a range, before and after the write probe is sent. This would be a
	// better test than what is below, if one can guarantee the live data
	// in the range won't change for some reason other than kvprober. The
	// below test is too fragile, in that it relies on kvprober implementation
	// details to check for the presence of a live data, meaning it will succeed
	// if live data is not present (desirable) or if the kvprober implementation
	// details change (not desirable).
	got, err := s.DB().Get(ctx, lastStep.Key)
	require.NoError(t, err)
	require.False(t, got.Exists(), got.PrettyValue())
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
			step, err := p.ReadPlannerNext(ctx)
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
		Tracer:                  s.TracerI().(*tracing.Tracer),
		DB:                      kvDB,
		HistogramWindowInterval: time.Minute, // actual value not important to test
		Settings:                s.ClusterSettings(),
	})

	// Given small test cluster, this better exercises the planning logic.
	kvprober.NumStepsToPlanAtOnce.Override(context.Background(), &s.ClusterSettings().SV, 10)
	// Want these tests to run as fast as possible; see planner_test.go for a
	// unit test of the rate limiting.
	p.SetPlanningRateLimits(0)

	return s, sqlDB, p, func() {
		s.Stopper().Stop(context.Background())
	}
}
