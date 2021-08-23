// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package instanceprovider_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlinstance/instanceprovider"
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

// TestInstanceProvider verifies that instance provider works as expected
// while creating and shutting down a new SQL pod.
func TestInstanceProvider(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	setup := func(t *testing.T) (
		*stop.Stopper, *slinstance.Instance, *hlc.Clock,
	) {
		timeSource := timeutil.NewTestTimeSource()
		clock := hlc.NewClock(func() int64 {
			return timeSource.Now().UnixNano()
		}, base.DefaultMaxClockOffset)
		settings := cluster.MakeTestingClusterSettingsWithVersions(
			clusterversion.TestingBinaryVersion,
			clusterversion.TestingBinaryMinSupportedVersion,
			true /* initializeVersion */)
		stopper := stop.NewStopper()
		fakeStorage := slstorage.NewFakeStorage()
		slInstance := slinstance.NewSQLInstance(stopper, clock, fakeStorage, settings, nil)
		return stopper, slInstance, clock
	}

	t.Run("test-init-shutdown", func(t *testing.T) {
		const addr = "addr"
		const expectedInstanceID = base.SQLInstanceID(1)
		stopper, slInstance, clock := setup(t)
		defer stopper.Stop(ctx)
		instanceProvider := instanceprovider.NewTestInstanceProvider(stopper, slInstance, addr)
		slInstance.Start(ctx)
		instanceID, err := instanceProvider.Instance(ctx)
		require.NoError(t, err)
		require.Equal(t, expectedInstanceID, instanceID)

		// Verify an additional call to Instance(), returns the same instance
		instanceID, err = instanceProvider.Instance(ctx)
		require.NoError(t, err)
		require.Equal(t, expectedInstanceID, instanceID)
		session, err := slInstance.Session(ctx)
		if err != nil {
			t.Fatal(err)
		}

		// Update clock time to move ahead of session expiry to ensure session expiry callback is invoked.
		newTime := session.Expiration().Add(1, 0).UnsafeToClockTimestamp()
		clock.Update(newTime)
		// Force call to clearSession by deleting the active session.
		slInstance.ClearSessionForTest(ctx)
		// Verify that the SQL instance is shutdown on session expiry.
		testutils.SucceedsSoon(t, func() error {
			if _, err = instanceProvider.Instance(ctx); err != stop.ErrUnavailable {
				return errors.Errorf("sql instance is not shutdown on session expiry")
			}
			return nil
		})
	})

	t.Run("test-shutdown-before-init", func(t *testing.T) {
		stopper, slInstance, _ := setup(t)
		defer stopper.Stop(ctx)
		instanceProvider := instanceprovider.NewTestInstanceProvider(stopper, slInstance, "addr")
		slInstance.Start(ctx)
		instanceProvider.ShutdownSQLInstanceForTest(ctx)
		_, err := instanceProvider.Instance(ctx)
		require.Error(t, err)
		require.Equal(t, "instance never initialized", err.Error())
	})
}
