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
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness/slinstance"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness/slstorage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

func TestSQLInstance(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx, stopper := context.Background(), stop.NewStopper()
	defer stopper.Stop(ctx)

	clock := hlc.NewClock(timeutil.NewManualTime(timeutil.Unix(0, 42)), time.Nanosecond /* maxOffset */)
	settings := cluster.MakeTestingClusterSettingsWithVersions(
		clusterversion.TestingBinaryVersion,
		clusterversion.TestingBinaryMinSupportedVersion,
		true /* initializeVersion */)
	slinstance.DefaultTTL.Override(ctx, &settings.SV, 2*time.Millisecond)
	slinstance.DefaultHeartBeat.Override(ctx, &settings.SV, 1*time.Millisecond)

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

// TestSQLInstanceDeadlines tests that we have proper deadlines set on the
// create and extend session operations. This is done by blocking the fake
// storage layer and ensuring that no sessions get created because the
// timeouts are constantly triggered.
func TestSQLInstanceDeadlines(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx, stopper := context.Background(), stop.NewStopper()
	defer stopper.Stop(ctx)

	clock := hlc.NewClock(timeutil.NewManualTime(timeutil.Unix(0, 42)), time.Nanosecond /* maxOffset */)
	settings := cluster.MakeTestingClusterSettingsWithVersions(
		clusterversion.TestingBinaryVersion,
		clusterversion.TestingBinaryMinSupportedVersion,
		true /* initializeVersion */)
	slinstance.DefaultTTL.Override(ctx, &settings.SV, 2*time.Millisecond)
	slinstance.DefaultHeartBeat.Override(ctx, &settings.SV, 1*time.Millisecond)

	fakeStorage := slstorage.NewFakeStorage()
	// block the fake storage
	fakeStorage.BlockCh = make(chan struct{})
	cleanUpFunc := func() {
		close(fakeStorage.BlockCh)
		fakeStorage.BlockCh = nil
	}
	defer cleanUpFunc()

	sqlInstance := slinstance.NewSQLInstance(stopper, clock, fakeStorage, settings, nil)
	sqlInstance.Start(ctx)

	// verify that we do not create a session
	require.Never(
		t,
		func() bool {
			_, err := sqlInstance.Session(ctx)
			if err != nil {
				return false
			}
			return true
		},
		10*time.Millisecond, 1*time.Millisecond,
	)
}

// TestSQLInstanceDeadlinesExtend tests that we have proper deadlines set on the
// create and extend session operations. This tests the case where the session is
// successfully created first and then blocks indefinitely.
func TestSQLInstanceDeadlinesExtend(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx, stopper := context.Background(), stop.NewStopper()
	defer stopper.Stop(ctx)

	clock := hlc.NewClock(timeutil.NewManualTime(timeutil.Unix(0, 42)), time.Nanosecond /* maxOffset */)
	settings := cluster.MakeTestingClusterSettingsWithVersions(
		clusterversion.TestingBinaryVersion,
		clusterversion.TestingBinaryMinSupportedVersion,
		true /* initializeVersion */)
	slinstance.DefaultTTL.Override(ctx, &settings.SV, 2*time.Millisecond)
	// Must be shorter than the storage sleep amount below
	slinstance.DefaultHeartBeat.Override(ctx, &settings.SV, 1*time.Millisecond)

	fakeStorage := slstorage.NewFakeStorage()
	sqlInstance := slinstance.NewSQLInstance(stopper, clock, fakeStorage, settings, nil)
	sqlInstance.Start(ctx)

	// verify that eventually session is created successfully
	require.Eventually(
		t,
		func() bool {
			_, err := sqlInstance.Session(ctx)
			if err != nil {
				return false
			}
			return true
		},
		10*time.Millisecond, 1*time.Millisecond,
	)

	// verify that session is also extended successfully a few times
	require.Never(
		t,
		func() bool {
			_, err := sqlInstance.Session(ctx)
			if err != nil {
				return true
			}
			return false
		},
		10*time.Millisecond, 1*time.Millisecond,
	)

	// block the fake storage
	fakeStorage.BlockCh = make(chan struct{})
	cleanUpFunc := func() {
		close(fakeStorage.BlockCh)
		fakeStorage.BlockCh = nil
	}
	defer cleanUpFunc()

	// expect subsequent create/extend calls to fail
	require.Eventually(
		t,
		func() bool {
			_, err := sqlInstance.Session(ctx)
			if err != nil {
				return true
			}
			return false
		},
		10*time.Millisecond, 1*time.Millisecond,
	)
}
