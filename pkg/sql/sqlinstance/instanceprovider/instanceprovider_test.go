// Copyright 2020 The Cockroach Authors.
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
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlinstance/instanceprovider"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness/slinstance"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness/slstorage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/stretchr/testify/require"
)

// TestInstanceProvider verifies that instance provider works as expected
// while creating and shutting down a new SQL pod.
func TestInstanceProvider(t *testing.T) {
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
	expiration := time.Microsecond
	slinstance.DefaultTTL.Override(ctx, &settings.SV, expiration)
	slinstance.DefaultHeartBeat.Override(ctx, &settings.SV, 10*time.Microsecond)

	fakeStorage := slstorage.NewFakeStorage()
	const addr = "addr"
	const expectedInstanceID = base.SQLInstanceID(1)
	slInstance := slinstance.NewSQLInstance(stopper, clock, fakeStorage, settings)
	instanceProvider := instanceprovider.NewTestInstanceProvider(stopper, slInstance, addr)
	slInstance.Start(ctx)
	instanceID, err := instanceProvider.Instance(ctx)
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
	// Verify that the sqlinstance is shutdown on session expiry.
	_, err = instanceProvider.Instance(ctx)
	require.Error(t, err)
	require.ErrorIs(t, err, stop.ErrUnavailable)
}
