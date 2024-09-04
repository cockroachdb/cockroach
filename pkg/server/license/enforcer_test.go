// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

type mockTelemetryStatusReporter struct {
	lastPingTime time.Time
}

func (m mockTelemetryStatusReporter) GetLastSuccessfulTelemetryPing() time.Time {
	return m.lastPingTime
}

func TestGracePeriodInitTSCache(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// This is the timestamp that we'll override the grace period init timestamp with.
	// This will be set when bringing up the server.
	ts1 := timeutil.Unix(1724329716, 0)
	ts1End := ts1.Add(7 * 24 * time.Hour) // Calculate the end of the grace period based on ts1

	ctx := context.Background()
	srv := serverutils.StartServerOnly(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			Server: &server.TestingKnobs{
				LicenseTestingKnobs: license.TestingKnobs{
					EnableGracePeriodInitTSWrite: true,
					OverrideStartTime:            &ts1,
				},
			},
		},
	})
	defer srv.Stopper().Stop(ctx)

	// Create a new enforcer, to test that it won't overwrite the grace period init
	// timestamp that was already setup.
	enforcer := &license.Enforcer{}
	ts2 := ts1.Add(1)
	ts2End := ts2.Add(7 * 24 * time.Hour) // Calculate the end of the grace period
	enforcer.TestingKnobs = &license.TestingKnobs{
		EnableGracePeriodInitTSWrite: true,
		OverrideStartTime:            &ts2,
	}
	// Ensure request for the grace period init ts1 before start just returns the start
	// time used when the enforcer was created.
	require.Equal(t, ts2End, enforcer.GetClusterInitGracePeriodEndTS())
	// Start the enforcer to read the timestamp from the KV.
	enforcer.SetTelemetryStatusReporter(&mockTelemetryStatusReporter{lastPingTime: ts1})
	err := enforcer.Start(ctx, srv.ClusterSettings(), srv.SystemLayer().InternalDB().(descs.DB), false /* initialStart */)
	require.NoError(t, err)
	require.Equal(t, ts1End, enforcer.GetClusterInitGracePeriodEndTS())

	// Access the enforcer that is cached in the executor config to make sure they
	// work for the system tenant and secondary tenant.
	require.Equal(t, ts1End, srv.SystemLayer().ExecutorConfig().(sql.ExecutorConfig).LicenseEnforcer.GetClusterInitGracePeriodEndTS())
	require.Equal(t, ts1End, srv.ApplicationLayer().ExecutorConfig().(sql.ExecutorConfig).LicenseEnforcer.GetClusterInitGracePeriodEndTS())
}

func TestThrottle(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	const UnderTxnThreshold = 3
	const OverTxnThreshold = 7

	t0 := time.Unix(1724884362, 0)
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
	}{
		// Expired free license but under the transaction threshold
		{UnderTxnThreshold, license.LicTypeFree, t0, t1d, t8d, t45d, ""},
		// Expired trial license but under the transaction threshold
		{UnderTxnThreshold, license.LicTypeTrial, t0, t30d, t8d, t45d, ""},
		// Over the transaction threshold but not expired
		{OverTxnThreshold, license.LicTypeFree, t0, t10d, t45d, t10d, ""},
		// Expired free license, past the grace period
		{OverTxnThreshold, license.LicTypeFree, t0, t30d, t10d, t45d, "License expired"},
		// Expired free license, but not past the grace period
		{OverTxnThreshold, license.LicTypeFree, t0, t30d, t10d, t17d, ""},
		// Valid free license, but telemetry ping hasn't been received in 7 days.
		{OverTxnThreshold, license.LicTypeFree, t0, t10d, t45d, t17d, ""},
		// Valid free license, but telemetry ping hasn't been received in 8 days.
		{OverTxnThreshold, license.LicTypeFree, t0, t10d, t45d, t18d, "diagnostic reporting"},
		// No license but within grace period still
		{OverTxnThreshold, license.LicTypeNone, t0, t0, t0, t1d, ""},
		// No license but beyond grace period
		{OverTxnThreshold, license.LicTypeNone, t0, t0, t0, t8d, "No license installed"},
		// Trial license has expired but still within grace period
		{OverTxnThreshold, license.LicTypeTrial, t0, t30d, t10d, t15d, ""},
		// Trial license has expired and just at the edge of the grace period.
		{OverTxnThreshold, license.LicTypeTrial, t0, t45d, t10d, t17d, ""},
		// Trial license has expired and just beyond the grace period.
		{OverTxnThreshold, license.LicTypeTrial, t0, t45d, t10d, t18d, "License expired"},
		// No throttling if past the expiry of an enterprise license
		{OverTxnThreshold, license.LicTypeEnterprise, t0, t0, t8d, t46d, ""},
		// Telemetry isn't needed for enterprise license
		{OverTxnThreshold, license.LicTypeEnterprise, t0, t0, t45d, t30d, ""},
		// Telemetry isn't needed for evaluation license
		{OverTxnThreshold, license.LicTypeEvaluation, t0, t0, t45d, t30d, ""},
		// Evaluation license doesn't throttle if expired but within grace period.
		{OverTxnThreshold, license.LicTypeEvaluation, t0, t0, t15d, t30d, ""},
		// Evaluation license does throttle if expired and beyond grace period.
		{OverTxnThreshold, license.LicTypeEvaluation, t0, t0, t15d, t46d, "License expired"},
	} {
		t.Run(fmt.Sprintf("test %d", i), func(t *testing.T) {
			e := license.Enforcer{
				TestingKnobs: &license.TestingKnobs{
					OverrideStartTime:         &tc.gracePeriodInit,
					OverrideThrottleCheckTime: &tc.checkTs,
				},
			}
			e.SetTelemetryStatusReporter(&mockTelemetryStatusReporter{
				lastPingTime: tc.lastTelemetryPingTime,
			})
			e.RefreshForLicenseChange(tc.licType, tc.licExpiry)
			err := e.MaybeFailIfThrottled(ctx, tc.openTxnsCount)
			if tc.expectedErrRegex == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				re := regexp.MustCompile(tc.expectedErrRegex)
				match := re.MatchString(err.Error())
				require.NotNil(t, match, "Error text %q doesn't match the expected regexp of %q",
					err.Error(), tc.expectedErrRegex)
			}
		})
	}
}
