// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package slsession_test

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness/slsession"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness/slstorage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestFactory_invokesSessionExpiryCallbacksInGoroutine(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx, stopper := context.Background(), stop.NewStopper()

	t0 := time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)
	mClock := hlc.NewManualClock(t0.UnixNano())
	clock := hlc.NewClock(mClock.UnixNano, time.Nanosecond)
	settings := cluster.MakeTestingClusterSettings()

	fakeStorage := slstorage.NewFakeStorage()
	sf := slsession.NewFactory(stopper, clock, fakeStorage, settings, nil)
	sf.Start(ctx)

	session, err := sf.Session(ctx)
	require.NoError(t, err)

	// Simulating what happens in `instanceprovider.shutdownSQLInstance`
	session.RegisterCallbackForSessionExpiry(func(ctx context.Context) {
		stopper.Stop(ctx)
	})

	// Removing the session will run the callback above, which will have to
	// wait for async tasks to stop. The async tasks include the
	// sf `heartbeatLoop` function.
	require.NoError(t, fakeStorage.Delete(ctx, session.ID()))

	// Clock needs to advance for expiry we trigger below to be valid
	mClock.Increment(int64(slsession.DefaultTTL.Get(&settings.SV)))

	testutils.SucceedsSoon(t, func() error {
		select {
		case <-stopper.IsStopped():
			return nil
		default:
			return errors.New("not stopped")
		}
	})
}

func TestFactory(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx, stopper := context.Background(), stop.NewStopper()
	defer stopper.Stop(ctx)

	mClock := hlc.NewManualClock(hlc.UnixNano())
	clock := hlc.NewClock(mClock.UnixNano, time.Nanosecond)
	settings := cluster.MakeTestingClusterSettingsWithVersions(
		clusterversion.TestingBinaryVersion,
		clusterversion.TestingBinaryMinSupportedVersion,
		true /* initializeVersion */)
	slsession.DefaultTTL.Override(ctx, &settings.SV, 2*time.Microsecond)
	slsession.DefaultHeartBeat.Override(ctx, &settings.SV, time.Microsecond)

	fakeStorage := slstorage.NewFakeStorage()
	sf := slsession.NewFactory(stopper, clock, fakeStorage, settings, nil)
	sf.Start(ctx)

	// Add one more factory to introduce concurrent access to storage.
	dummy := slsession.NewFactory(stopper, clock, fakeStorage, settings, nil)
	dummy.Start(ctx)

	s1, err := sf.Session(ctx)
	require.NoError(t, err)
	a, err := fakeStorage.IsAlive(ctx, s1.ID())
	require.NoError(t, err)
	require.True(t, a)

	s2, err := sf.Session(ctx)
	require.NoError(t, err)
	require.Equal(t, s1.ID(), s2.ID())

	_ = fakeStorage.Delete(ctx, s2.ID())
	t.Logf("deleted session %s", s2.ID())
	a, err = fakeStorage.IsAlive(ctx, s2.ID())
	require.NoError(t, err)
	require.False(t, a)

	var s3 sqlliveness.Session
	require.Eventually(
		t,
		func() bool {
			s3, err = sf.Session(ctx)
			if err != nil {
				t.Fatal(err)
			}
			return s3.ID().String() != s2.ID().String()
		},
		time.Second, 10*time.Millisecond,
	)

	a, err = fakeStorage.IsAlive(ctx, s3.ID())
	require.NoError(t, err)
	require.True(t, a)
	require.NotEqual(t, s2.ID(), s3.ID())

	// Force next call to Session to fail.
	stopper.Stop(ctx)
	sf.ClearSessionForTest(ctx)
	_, err = sf.Session(ctx)
	require.Error(t, err)
}

// We want to test that the GC runs a number of times but is jitterred.
func TestJitter(t *testing.T) {
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	t0 := time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)
	timeSource := timeutil.NewManualTime(t0)
	clock := hlc.NewClock(func() int64 { return timeSource.Now().UnixNano() }, 0)
	fs := slstorage.NewFakeStorage()
	settings := cluster.MakeTestingClusterSettings()
	sf := slsession.NewFactory(stopper, clock, fs, settings, &sqlliveness.TestingKnobs{
		NewTimer: timeSource.NewTimer,
	})
	sf.Start(ctx)
	waitForGCTimer := func() (timer time.Time) {
		testutils.SucceedsSoon(t, func() error {
			timers := timeSource.Timers()
			if len(timers) != 1 {
				return errors.Errorf("expected 1 timer, saw %d", len(timers))
			}
			timer = timers[0]
			return nil
		})
		return timer
	}
	const N = 10
	for i := 0; i < N; i++ {
		timer := waitForGCTimer()
		timeSource.Advance(timer.Sub(timeSource.Now()))
	}
	jitter := slsession.GCJitter.Get(&settings.SV)
	interval := slsession.GCInterval.Get(&settings.SV)
	minTime := t0.Add(time.Duration((1 - jitter) * float64(interval.Nanoseconds()) * N))
	maxTime := t0.Add(time.Duration((1 + jitter) * float64(interval.Nanoseconds()) * N))
	noJitterTime := t0.Add(interval * N)
	now := timeSource.Now()
	require.Truef(t, now.After(minTime), "%v > %v", now, minTime)
	require.Truef(t, now.Before(maxTime), "%v < %v", now, maxTime)
	require.Truef(t, !now.Equal(noJitterTime), "%v != %v", now, noJitterTime)
}
