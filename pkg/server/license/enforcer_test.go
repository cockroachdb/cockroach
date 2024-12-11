// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package license_test

import (
	"context"
	"fmt"
	"regexp"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/server/license"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/testutils/listenerutil"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

type mockTelemetryStatusReporter struct {
	lastPingTime *time.Time
}

func (m mockTelemetryStatusReporter) GetLastSuccessfulTelemetryPing() time.Time {
	return *m.lastPingTime
}

func TestClusterInitGracePeriod_NoOverwrite(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// This is the timestamp that we'll override the grace period init timestamp with.
	// This will be set when bringing up the server.
	ts1 := timeutil.Unix(1724329716, 0)
	ts1_30d := ts1.Add(30 * 24 * time.Hour)

	ctx := context.Background()
	srv := serverutils.StartServerOnly(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			LicenseTestingKnobs: &license.TestingKnobs{
				OverrideStartTime: &ts1,
			},
		},
	})
	defer srv.Stopper().Stop(ctx)

	// Create a new enforcer, to test that it won't overwrite the grace period init
	// timestamp that was already setup.
	ts2 := ts1.Add(1)
	ts2End := ts2.Add(7 * 24 * time.Hour) // Calculate the end of the grace period
	enforcer := license.NewEnforcer(
		&license.TestingKnobs{
			OverrideStartTime: &ts2,
		})
	// Ensure request for the grace period init ts1 before start just returns the start
	// time used when the enforcer was created.
	require.Equal(t, ts2End, enforcer.GetClusterInitGracePeriodEndTS())
	// Start the enforcer to read the timestamp from the KV.
	err := enforcer.Start(ctx, srv.ClusterSettings(),
		license.WithDB(srv.SystemLayer().InternalDB().(descs.DB)),
		license.WithSystemTenant(true),
		license.WithTelemetryStatusReporter(&mockTelemetryStatusReporter{lastPingTime: &ts1}),
	)
	require.NoError(t, err)
	require.Equal(t, ts1_30d, enforcer.GetClusterInitGracePeriodEndTS())

	// Access the enforcer that is cached in the executor config to make sure they
	// work for the system tenant and secondary tenant.
	require.Equal(t, ts1_30d, srv.SystemLayer().ExecutorConfig().(sql.ExecutorConfig).LicenseEnforcer.GetClusterInitGracePeriodEndTS())
	require.Equal(t, ts1_30d, srv.ApplicationLayer().ExecutorConfig().(sql.ExecutorConfig).LicenseEnforcer.GetClusterInitGracePeriodEndTS())
}

func TestClusterInitGracePeriod_NewClusterEstimation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// This will be the start time of the enforcer for each unit test
	ts1 := timeutil.Unix(1631494860, 0)

	ctx := context.Background()
	srv := serverutils.StartServerOnly(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			LicenseTestingKnobs: &license.TestingKnobs{
				OverrideStartTime: &ts1,
			},
		},
	})
	defer srv.Stopper().Stop(ctx)

	for _, tc := range []struct {
		desc                string
		minSysMigrationTime time.Time
		expGracePeriodEndTS time.Time
	}{
		{"init-2020", timeutil.Unix(1577836800, 0), ts1.Add(30 * 24 * time.Hour)},
		{"init-5min-ago", ts1.Add(-5 * time.Minute), ts1.Add(7 * 24 * time.Hour)},
		{"init-59min-ago", ts1.Add(-59 * time.Minute), ts1.Add(7 * 24 * time.Hour)},
		{"init-1h1min-ago", ts1.Add(-61 * time.Minute), ts1.Add(30 * 24 * time.Hour)},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			enforcer := license.NewEnforcer(
				&license.TestingKnobs{
					OverrideStartTime:                 &ts1,
					OverwriteClusterInitGracePeriodTS: true,
				})

			// Set up the min time in system.migrations. This is used by the enforcer
			// to figure out if the cluster is new or old. The grace period length is
			// adjusted based on this.
			db := srv.SystemLayer().InternalDB().(descs.DB)
			err := db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
				_, err := txn.Exec(ctx, "add new min to system.migrations", txn.KV(),
					"UPDATE system.migrations SET completed_at=$1 LIMIT 1",
					tc.minSysMigrationTime)
				return err
			})
			require.NoError(t, err)

			err = enforcer.Start(ctx, srv.ClusterSettings(),
				license.WithDB(db),
				license.WithSystemTenant(true),
				license.WithTestingKnobs(&license.TestingKnobs{
					OverrideStartTime:                 &ts1,
					OverwriteClusterInitGracePeriodTS: true,
				}),
			)
			require.NoError(t, err)
			require.Equal(t, tc.expGracePeriodEndTS, enforcer.GetClusterInitGracePeriodEndTS())
		})
	}
}

type mockMetadataAccessor struct {
	ts *int64
}

func (m *mockMetadataAccessor) GetClusterInitGracePeriodTS() int64 {
	return *m.ts
}

func TestClusterInitGracePeriod_DelayedTenantConnector(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ts0 := timeutil.Unix(1667764800, 0) // 2022-11-6 8pm
	ts1d := ts0.Add(24 * time.Hour)
	ts8d := ts0.Add(8 * 24 * time.Hour)
	ts9d := ts0.Add(9 * 24 * time.Hour)
	ts30d := ts0.Add(30 * 24 * time.Hour)

	// Start the system tenant
	ctx := context.Background()
	srv := serverutils.StartServerOnly(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			LicenseTestingKnobs: &license.TestingKnobs{
				OverrideStartTime: &ts0,
			},
		},
	})
	defer srv.Stopper().Stop(ctx)

	// This is a timestamp that we'll change to mock receiving the cluster
	// init timestamp from tenant connector.
	var connectTS int64

	// Start up the enforcer for the secondary tenant using a metadata accessor
	// that has not yet received the cluster init grace period timestamp.
	enforcer := license.NewEnforcer(&license.TestingKnobs{
		OverrideStartTime:         &ts1d,
		OverrideThrottleCheckTime: &ts9d,
	})
	err := enforcer.Start(ctx, srv.ClusterSettings(),
		license.WithSystemTenant(false),
		license.WithMetadataAccessor(&mockMetadataAccessor{ts: &connectTS}),
	)
	require.NoError(t, err)

	// The cluster init grace period timestamp hasn't been received yet.
	// Default to 7 days from the enforcer's start time.
	require.Equal(t, ts8d, enforcer.GetClusterInitGracePeriodEndTS())

	// We will be throttled because the check time is 9-days after start,
	// which is beyond the grace period.
	const beyondThreshold = 10
	_, err = enforcer.TestingMaybeFailIfThrottled(ctx, beyondThreshold)
	require.Error(t, err)

	// Now mock receiving the timestamp from the system tenant. It should now
	// be 30-days after the start time of the system tenant.
	connectTS = ts30d.Unix()
	// The check for throttling will refresh the value.
	_, err = enforcer.TestingMaybeFailIfThrottled(ctx, beyondThreshold)
	require.NoError(t, err)
	require.Equal(t, ts30d, enforcer.GetClusterInitGracePeriodEndTS())
	// Run the throttle again (from a different goroutine) to help flush
	// out data races. It should be the same response as before.
	_, err = enforcer.TestingMaybeFailIfThrottled(ctx, beyondThreshold)
	require.NoError(t, err)
}

func TestThrottle(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	const UnderTxnThreshold = 3
	const AtTxnThreshold = 5
	const OverTxnThreshold = 7

	t0 := timeutil.Unix(1724884362, 0) // 08/28/24 10:32:42 PM UTC
	t1d := t0.Add(24 * time.Hour)
	t8d := t0.Add(8 * 24 * time.Hour)
	t10d := t0.Add(10 * 24 * time.Hour)
	t15d := t0.Add(15 * 24 * time.Hour)
	t17d := t0.Add(17 * 24 * time.Hour)
	t18d := t0.Add(18 * 24 * time.Hour)
	t30d := t0.Add(30 * 24 * time.Hour)
	t45d := t0.Add(45 * 24 * time.Hour)
	t46d := t0.Add(46 * 24 * time.Hour)

	for i, tc := range []struct {
		openTxnsCount         int64
		licType               license.LicType
		gracePeriodInit       time.Time
		lastTelemetryPingTime time.Time
		licExpiry             time.Time
		checkTs               time.Time
		expectedErrRegex      string
		expectedNoticeRegex   string
	}{
		// Expired free license but under the transaction threshold
		{UnderTxnThreshold, license.LicTypeFree, t0, t1d, t8d, t45d, "", ""},
		// Expired trial license but at the transaction threshold
		{AtTxnThreshold, license.LicTypeTrial, t0, t30d, t8d, t45d, "", ""},
		// Over the transaction threshold but not expired
		{OverTxnThreshold, license.LicTypeFree, t0, t10d, t45d, t10d, "", ""},
		// Expired free license, past the grace period
		{OverTxnThreshold, license.LicTypeFree, t0, t30d, t10d, t45d, "License expired", ""},
		// Expired free license, but not past the grace period
		{OverTxnThreshold, license.LicTypeFree, t0, t30d, t10d, t17d, "", "license expired.*Throttling will begin"},
		// Valid free license, but telemetry ping hasn't been received in 7 days.
		{OverTxnThreshold, license.LicTypeFree, t0, t10d, t45d, t17d, "", "Your license requires diagnostic reporting.*Throttling will begin"},
		// Valid free license, but telemetry ping hasn't been received in 8 days.
		{OverTxnThreshold, license.LicTypeFree, t0, t10d, t45d, t18d, "diagnostic reporting", ""},
		// No license but within grace period still
		{OverTxnThreshold, license.LicTypeNone, t0, t0, t0, t1d, "", "No license.*Throttling will begin"},
		// No license but beyond grace period
		{OverTxnThreshold, license.LicTypeNone, t0, t0, t0, t8d, "No license installed", ""},
		// Trial license has expired but still within grace period
		{OverTxnThreshold, license.LicTypeTrial, t0, t30d, t10d, t15d, "", "license expired.*Throttling will begin"},
		// Trial license has expired and just at the edge of the grace period.
		{OverTxnThreshold, license.LicTypeTrial, t0, t45d, t10d, t17d, "", "license expired.*Throttling will begin"},
		// Trial license has expired and just beyond the grace period.
		{OverTxnThreshold, license.LicTypeTrial, t0, t45d, t10d, t18d, "License expired", ""},
		// No throttling if past the expiry of an enterprise license
		{OverTxnThreshold, license.LicTypeEnterprise, t0, t0, t8d, t46d, "", ""},
		// Telemetry isn't needed for enterprise license
		{OverTxnThreshold, license.LicTypeEnterprise, t0, t0, t45d, t30d, "", ""},
		// Telemetry isn't needed for evaluation license
		{OverTxnThreshold, license.LicTypeEvaluation, t0, t0, t45d, t30d, "", ""},
		// Evaluation license doesn't throttle if expired but within grace period.
		{OverTxnThreshold, license.LicTypeEvaluation, t0, t0, t15d, t30d, "", "license expired.*Throttling will begin"},
		// Evaluation license does throttle if expired and beyond grace period.
		{OverTxnThreshold, license.LicTypeEvaluation, t0, t0, t15d, t46d, "License expired", ""},
	} {
		t.Run(fmt.Sprintf("test %d", i), func(t *testing.T) {
			e := license.NewEnforcer(
				&license.TestingKnobs{
					OverrideStartTime:         &tc.gracePeriodInit,
					OverrideThrottleCheckTime: &tc.checkTs,
				})
			gracePeriodEnd := tc.gracePeriodInit.Add(7 * 24 * time.Hour).Unix()
			err := e.Start(ctx, nil,
				license.WithMetadataAccessor(&mockMetadataAccessor{ts: &gracePeriodEnd}),
				license.WithTelemetryStatusReporter(&mockTelemetryStatusReporter{lastPingTime: &tc.lastTelemetryPingTime}),
			)
			require.NoError(t, err)
			e.RefreshForLicenseChange(ctx, tc.licType, tc.licExpiry)
			notice, err := e.TestingMaybeFailIfThrottled(ctx, tc.openTxnsCount)
			if tc.expectedErrRegex == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				re := regexp.MustCompile(tc.expectedErrRegex)
				match := re.MatchString(err.Error())
				require.NotNil(t, match, "Error text %q doesn't match the expected regexp of %q",
					err.Error(), tc.expectedErrRegex)
			}
			if tc.expectedNoticeRegex == "" {
				require.NoError(t, notice)
			} else {
				require.Error(t, notice)
				re := regexp.MustCompile(tc.expectedNoticeRegex)
				match := re.MatchString(notice.Error())
				require.NotNil(t, match, "Notice text %q doesn't match the expected regexp of %q",
					notice.Error(), tc.expectedNoticeRegex)
			}
		})
	}
}

func TestThrottleErrorMsg(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Set of times that we'll use in the test.
	//
	// Initial time that other timestamps are derived from
	t0d := timeutil.Unix(1724329716, 0)
	// 10 days after start. License is valid.
	t10d := t0d.Add(10 * 24 * time.Hour)
	// 30 days after start. This is when the license will expire.
	t30d := t0d.Add(30 * 24 * time.Hour)
	// 55 days after initial time. This is still within grace period.
	t55d := t0d.Add(55 * 24 * time.Hour)
	// 60 days after initial time. This is the end of the grace period. Throttling
	// may happen anytime after this.
	t60d := t0d.Add(60 * 24 * time.Hour)
	// 1ms past the grace period end time.
	t60d1ms := t60d.Add(time.Millisecond)

	// Pointer to the timestamp that we'll use for the throttle check. This is
	// modified for every test unit.
	throttleCheckTS := &time.Time{}
	// And a pointer to the timestamp that we'll use for the last ping time in
	// the telemetry server. We modify this for each test unit.
	telemetryTS := &time.Time{}

	// Controls the maximum number of open transactions to simulate concurrency.
	// This value can be modified by individual tests through testing knobs based
	// on whether the open transactions are above or below the threshold.
	var maxOpenTransactions int64 = 5

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			LicenseTestingKnobs: &license.TestingKnobs{
				// The mock server we bring up is single-node, which disables all
				// throttling checks. We need to avoid that for this test to verify
				// the throttle message.
				SkipDisable: true,
				// We are going to modify the throttle check timestamp in each test
				// unit.
				OverrideThrottleCheckTime: throttleCheckTS,
				// Allow for setting the last ping time in each test unit.
				OverrideTelemetryStatusReporter: &mockTelemetryStatusReporter{
					lastPingTime: telemetryTS,
				},
				// And we will modify what is the max open transactions to force us
				// over the limit.
				OverrideMaxOpenTransactions: &maxOpenTransactions,
			},
		},
	})
	defer srv.Stopper().Stop(ctx)

	// Set up a free license that will expire in 30 days
	licenseEnforcer := srv.ApplicationLayer().ExecutorConfig().(sql.ExecutorConfig).LicenseEnforcer
	licenseEnforcer.RefreshForLicenseChange(ctx, license.LicTypeFree, t30d)

	for _, tc := range []struct {
		desc string
		// overThreshold will control if we are above the limit of max open transactions.
		overThreshold   bool
		throttleCheckTS time.Time
		telemetryTS     time.Time
		sql             string
		errRE           string
	}{
		// NB: license expires at t30d and grace period ends at t60d.
		{"at-threshold-valid-license-and-telemetry", true, t10d, t10d, "SELECT 1", ""},
		{"at-threshold-expired-license-in-grace-valid-telemetry", true, t60d, t55d, "SELECT 1", ""},
		{"at-threshold-expired-license-past-grace-valid-telemetry", true, t60d1ms, t55d,
			"SELECT 1", "License expired .*. The maximum number of .*"},
		{"below-threshold-expired-license-past-grace-valid-telemetry", false, t60d1ms, t55d, "SELECT 1", ""},
		{"at-threshold-expired-license-in-grace-invalid-telemetry", true, t55d, t10d, "SELECT 1",
			"The maximum number of concurrently open transactions has been reached because the license requires diagnostic reporting"},
		{"at-threshold-valid-license-invalid-telemetry", true, t10d, t0d, "SELECT 1",
			"The maximum number of concurrently open transactions has been reached because the license requires diagnostic reporting"},
		{"below-threshold-invalid-telemetry", false, t10d, t0d, "SELECT 1", ""},
		{"at-threshold-expired-license-past-grace-valid-telemetry-SET_CLUSTER", true, t60d1ms, t55d,
			"SET CLUSTER SETTING cluster.label = ''", ""},
		{"at-threshold-expired-license-past-grace-invalid-telemetry-SET_CLUSTER", true, t60d1ms, t10d,
			"SET CLUSTER SETTING cluster.label = ''", ""},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			// Adjust the time stamps for this test unit
			*throttleCheckTS = tc.throttleCheckTS
			*telemetryTS = tc.telemetryTS

			// Override the max open transactions based on whether we are above or
			// below the open transaction limit.
			if tc.overThreshold {
				maxOpenTransactions = 0
			} else {
				maxOpenTransactions = 5
			}

			// Open a transaction. First, check external connections.
			// It may or may not be throttled, depending on tc parms.
			tdb := sqlutils.MakeSQLRunner(sqlDB)
			if tc.errRE != "" {
				tdb.ExpectErr(t, tc.errRE, tc.sql)
			} else {
				tdb.Exec(t, tc.sql)
			}

			// Confirm that internal connections are never throttled
			idb := srv.InternalDB().(isql.DB)
			err := idb.Txn(ctx, func(ctx context.Context, tx isql.Txn) error {
				_, err := tx.ExecEx(ctx, "internal query throttle test", tx.KV(),
					sessiondata.NodeUserSessionDataOverride, "SELECT 1")
				return err
			})
			require.NoError(t, err)
		})
	}
}

func TestLicenseDisabledAfterRestart(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	listenerReg := listenerutil.NewListenerRegistry()
	defer listenerReg.Close()
	stickyVFSRegistry := fs.NewStickyRegistry()

	tc := serverutils.StartCluster(t, 1, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					// Sticky vfs is needed for restart
					StickyVFSRegistry: stickyVFSRegistry,
				},
			},
		},
		// A listener is required for restart
		ReusableListenerReg: listenerReg,
		StartSingleNode:     true,
	})
	defer tc.Stopper().Stop(context.Background())

	enforcer := tc.SystemLayer(0).ExecutorConfig().(sql.ExecutorConfig).LicenseEnforcer
	require.Equal(t, true, enforcer.GetIsDisabled(), "expected license enforcement to be disabled")

	require.NoError(t, tc.Restart())

	enforcer = tc.SystemLayer(0).ExecutorConfig().(sql.ExecutorConfig).LicenseEnforcer
	require.Equal(t, true, enforcer.GetIsDisabled(), "expected license enforcement to be disabled")
}
