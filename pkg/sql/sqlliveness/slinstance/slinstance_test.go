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
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/enum"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness/slinstance"
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

func TestSQLInstance(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx, stopper := context.Background(), stop.NewStopper()
	defer stopper.Stop(ctx)

	var ambientCtx log.AmbientContext
	clock := hlc.NewClockForTesting(timeutil.NewManualTime(timeutil.Unix(0, 42)))
	settings := cluster.MakeTestingClusterSettingsWithVersions(
		clusterversion.TestingBinaryVersion,
		clusterversion.TestingBinaryMinSupportedVersion,
		true /* initializeVersion */)
	slinstance.DefaultTTL.Override(ctx, &settings.SV, 20*time.Millisecond)
	slinstance.DefaultHeartBeat.Override(ctx, &settings.SV, 10*time.Millisecond)

	fakeStorage := slstorage.NewFakeStorage()
	sqlInstance := slinstance.NewSQLInstance(ambientCtx, stopper, clock, fakeStorage, settings, nil, nil)
	sqlInstance.Start(ctx, nil)

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
		clusterversion.TestingBinaryVersion,
		clusterversion.TestingBinaryMinSupportedVersion,
		true /* initializeVersion */)
	slinstance.DefaultTTL.Override(ctx, &settings.SV, 20*time.Millisecond)
	slinstance.DefaultHeartBeat.Override(ctx, &settings.SV, 10*time.Millisecond)

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
