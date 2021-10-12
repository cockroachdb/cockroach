// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package slinstance_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness/slinstance"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness/slstorage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/stretchr/testify/require"
)

func TestSQLInstance_invokesSessionExpiryCallbacksInGoroutine(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx, stopper := context.Background(), stop.NewStopper()

	t0 := time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)
	mClock := hlc.NewManualClock(t0.UnixNano())
	clock := hlc.NewClock(mClock.UnixNano, time.Nanosecond)
	settings := cluster.MakeTestingClusterSettings()

	fakeStorage := slstorage.NewFakeStorage()
	sqlInstance := slinstance.NewSQLInstance(stopper, clock, fakeStorage, settings, nil)
	sqlInstance.Start(ctx)

	session, err := sqlInstance.Session(ctx)
	require.NoError(t, err)

	// Simulating what happens in `instanceprovider.shutdownSQLInstance`
	session.RegisterCallbackForSessionExpiry(func(ctx context.Context) {
		stopper.Stop(ctx)
	})

	// Removing the session will run the callback above, which will have to
	// wait for async tasks to stop. The async tasks include the
	// sqlInstance `heartbeatLoop` function.
	require.NoError(t, fakeStorage.Delete(ctx, session.ID()))

	// Clock needs to advance for expiry we trigger below to be valid
	mClock.Increment(int64(slinstance.DefaultTTL.Get(&settings.SV)))

	testutils.SucceedsSoon(t, func() error {
		select {
		case <-stopper.IsStopped():
			return nil
		default:
			return errors.New("not stopped")
		}
	})
}

func TestSQLInstance(t *testing.T) {
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
	slinstance.DefaultTTL.Override(ctx, &settings.SV, 2*time.Microsecond)
	slinstance.DefaultHeartBeat.Override(ctx, &settings.SV, time.Microsecond)

	fakeStorage := slstorage.NewFakeStorage()
	sqlInstance := slinstance.NewSQLInstance(stopper, clock, fakeStorage, settings, nil)
	sqlInstance.Start(ctx)

	// Add one more instance to introduce concurrent access to storage.
	dummy := slinstance.NewSQLInstance(stopper, clock, fakeStorage, settings, nil)
	dummy.Start(ctx)

	s1, err := sqlInstance.Session(ctx)
	require.NoError(t, err)
	a, err := fakeStorage.IsAlive(ctx, s1.ID())
	require.NoError(t, err)
	require.True(t, a)

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

	// Force next call to Session to fail.
	stopper.Stop(ctx)
	sqlInstance.ClearSessionForTest(ctx)
	_, err = sqlInstance.Session(ctx)
	require.Error(t, err)
}
