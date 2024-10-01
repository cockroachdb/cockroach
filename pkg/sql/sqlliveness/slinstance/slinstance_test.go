// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package slinstance_test

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/enum"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness/slbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness/slinstance"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness/slstorage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestSQLInstance(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx, stopper := context.Background(), stop.NewStopper()
	defer stopper.Stop(ctx)

	var ambientCtx log.AmbientContext
	clock := hlc.NewClockForTesting(timeutil.NewManualTime(timeutil.Unix(0, 42)))
	settings := cluster.MakeTestingClusterSettingsWithVersions(
		clusterversion.Latest.Version(),
		clusterversion.MinSupported.Version(),
		true /* initializeVersion */)
	slbase.DefaultTTL.Override(ctx, &settings.SV, 20*time.Millisecond)
	slbase.DefaultHeartBeat.Override(ctx, &settings.SV, 10*time.Millisecond)

	fakeStorage := slstorage.NewFakeStorage()
	sqlInstance := slinstance.NewSQLInstance(ambientCtx, stopper, clock, fakeStorage, settings, nil, nil)
	// Ensure that the first iteration always fails with an artificial error, this
	// should lead to a retry. Which confirms that the retry logic works correctly.
	var failureMu struct {
		syncutil.Mutex
		numRetries       int
		initialTimestamp hlc.Timestamp
		nextTimestamp    hlc.Timestamp
		lastSessionID    sqlliveness.SessionID
	}
	fakeStorage.SetInjectedFailure(func(sid sqlliveness.SessionID, expiration hlc.Timestamp) error {
		failureMu.Lock()
		defer failureMu.Unlock()
		failureMu.numRetries++
		if failureMu.numRetries == 1 {
			failureMu.initialTimestamp = expiration
			return kvpb.NewReplicaUnavailableError(errors.Newf("fake injected error"), &roachpb.RangeDescriptor{}, roachpb.ReplicaDescriptor{})
		} else if failureMu.numRetries == 2 {
			failureMu.lastSessionID = sid
			return kvpb.NewAmbiguousResultError(errors.Newf("fake injected error"))
		}
		failureMu.nextTimestamp = expiration
		return nil
	})
	sqlInstance.Start(ctx, nil)
	// We expect three attempts to insert, since we inject a replica unavailable
	// error on the first attempt. On the second attempt we will inject an ambiguous
	// result error. The third and final attempt will be successful.
	testutils.SucceedsSoon(t, func() error {
		failureMu.Lock()
		defer failureMu.Unlock()
		if failureMu.numRetries < 3 {
			return errors.AssertionFailedf("unexpected number of retries on session insertion, "+
				"expected at least 2, got %d", failureMu.numRetries)
		}
		if !failureMu.nextTimestamp.After(failureMu.initialTimestamp) {
			return errors.AssertionFailedf("timestamp should move forward on each retry, "+
				"got %s. Previous timestamp was: %s", failureMu.nextTimestamp, failureMu.initialTimestamp)
		}
		session, err := sqlInstance.Session(ctx)
		if err != nil {
			return err
		}
		if session.ID() == failureMu.lastSessionID || len(failureMu.lastSessionID) == 0 {
			return errors.AssertionFailedf("new session ID should have been assigned after an ambiguous"+
				" result error. Current: %s  Previous: %s", session.ID(), failureMu.lastSessionID)
		}
		return nil
	})

	// Add one more instance to introduce concurrent access to storage.
	dummy := slinstance.NewSQLInstance(ambientCtx, stopper, clock, fakeStorage, settings, nil, nil)
	dummy.Start(ctx, nil)

	s1, err := sqlInstance.Session(ctx)
	require.NoError(t, err)
	a, err := fakeStorage.IsAlive(ctx, s1.ID())
	require.NoError(t, err)
	require.True(t, a)

	region, id, err := slstorage.UnsafeDecodeSessionID(s1.ID())
	require.NoError(t, err)
	require.Equal(t, enum.One, region)
	require.NotNil(t, id)

	s2, err := sqlInstance.Session(ctx)
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
			s3, err = sqlInstance.Session(ctx)
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

	// Stop the stopper and check that the heartbeat loop terminates
	// and causes further Session() calls to fail.
	stopper.Stop(ctx)
	_, err = sqlInstance.Session(ctx)
	require.Error(t, err)
}

func TestSQLInstanceRelease(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx, stopper := context.Background(), stop.NewStopper()
	defer stopper.Stop(ctx)

	clock := hlc.NewClockForTesting(timeutil.NewManualTime(timeutil.Unix(0, 42)))
	settings := cluster.MakeTestingClusterSettingsWithVersions(
		clusterversion.Latest.Version(),
		clusterversion.MinSupported.Version(),
		true /* initializeVersion */)
	slbase.DefaultTTL.Override(ctx, &settings.SV, 20*time.Millisecond)
	slbase.DefaultHeartBeat.Override(ctx, &settings.SV, 10*time.Millisecond)

	fakeStorage := slstorage.NewFakeStorage()
	var ambientCtx log.AmbientContext
	sqlInstance := slinstance.NewSQLInstance(ambientCtx, stopper, clock, fakeStorage, settings, nil, nil)
	sqlInstance.Start(ctx, enum.One)

	activeSession, err := sqlInstance.Session(ctx)
	require.NoError(t, err)
	activeSessionID := activeSession.ID()

	a, err := fakeStorage.IsAlive(ctx, activeSessionID)
	require.NoError(t, err)
	require.True(t, a)

	require.NotEqual(t, 0, stopper.NumTasks())

	// Make sure release is idempotent.
	for i := 0; i < 5; i++ {
		finalSession, err := sqlInstance.Release(ctx)
		require.NoError(t, err)

		// Release should always return the last active session.
		require.Equal(t, activeSessionID, finalSession)

		// Release should tear down the background heartbeat.
		testutils.SucceedsSoon(t, func() error {
			tasks := stopper.NumTasks()
			if tasks != 0 {
				return errors.Newf("expected zero runnings tasks, found: %d", tasks)
			}
			return nil
		})

		// Once the instance is shut down, it should return an error instead of
		// the session.
		_, err = sqlInstance.Session(ctx)
		require.ErrorIs(t, err, stop.ErrUnavailable)
	}
}
