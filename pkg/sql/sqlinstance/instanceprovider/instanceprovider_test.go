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
	expiration := 2 * time.Microsecond
	slinstance.DefaultTTL.Override(ctx, &settings.SV, expiration)
	slinstance.DefaultHeartBeat.Override(ctx, &settings.SV, 1*time.Microsecond)

	fakeStorage := slstorage.NewFakeStorage()
	const httpAddr = "http_addr"
	sqlInstance := slinstance.NewSQLInstance(stopper, clock, fakeStorage, settings)
	instanceProvider := instanceprovider.NewTestInstanceProvider(stopper, sqlInstance, httpAddr)
	sqlInstance.Start(ctx)
	instanceProvider.Start(ctx)
	instance, err := instanceProvider.Instance(ctx)
	require.NoError(t, err)
	addr, err := instanceProvider.GetInstanceAddr(ctx, instance.InstanceID())
	require.NoError(t, err)
	require.Equal(t, httpAddr, addr)
	session, err := sqlInstance.Session(ctx)
	if err != nil {
		t.Fatal(err)
	}
	clock.Update(session.Expiration().Add(1, 0).UnsafeToClockTimestamp())
	// Force call to clearSession by deleting the active session.
	sqlInstance.ClearSessionForTest(ctx)
	// Verify that the sqlinstance is shutdown on session expiry.
	require.Eventually(
		t,
		func() bool {
			_, err := instanceProvider.Instance(ctx)
			return err == stop.ErrUnavailable
		},
		time.Second, 10*time.Millisecond,
	)
}
