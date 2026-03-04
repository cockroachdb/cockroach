// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package license_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/server/license"
	"github.com/cockroachdb/cockroach/pkg/server/license/licensepb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

func TestRefreshLicenseEnforcerOnLicenseChange(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ts1 := timeutil.Unix(1724329716, 0)

	ctx := context.Background()
	srv, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{
		// We are changing a cluster setting that can only be done at the
		// system tenant.
		DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
		Knobs: base.TestingKnobs{
			LicenseTestingKnobs: &license.TestingKnobs{
				SkipDisable:       true,
				OverrideStartTime: &ts1,
			},
		},
	})
	defer srv.Stopper().Stop(ctx)

	// All of the licenses that we install later depend on this org name.
	_, err := srv.SystemLayer().SQLConn(t).Exec(
		"SET CLUSTER SETTING cluster.organization = 'CRDB Unit Test'",
	)
	require.NoError(t, err)

	// Test to ensure that the state is correctly registered on startup
	// before changing the license.
	enforcer := srv.SystemLayer().ExecutorConfig().(sql.ExecutorConfig).LicenseEnforcer
	require.Equal(t, false, enforcer.GetHasLicense())
	gracePeriodTS, hasGracePeriod := enforcer.GetGracePeriodEndTS()
	require.True(t, hasGracePeriod)
	require.Equal(t, ts1.Add(30*24*time.Hour), gracePeriodTS)

	jan1st2000 := timeutil.Unix(946728000, 0)

	for i, tc := range []struct {
		// licenses is a list of licenses to be installed sequentially.
		// All licenses except the last one must be installed successfully.
		// The outcome of the final license is controlled by the errRE field.
		licenses []string
		// errRE specifies a regular expression that matches the expected
		// error message when installing the last license. If no error is
		// expected, this should be set to an empty string.
		errRE string
		// expectedGracePeriodEnd is the timestamp representing when the
		// grace period should end. This value is verified only if the last
		// license is installed successfully.
		expectedGracePeriodEnd time.Time
	}{
		// Note: all licenses below expire on Jan 1st 2000
		//
		// Free license - 30 days grace period
		{[]string{"crl-0-EMDYt8MDGAMiDkNSREIgVW5pdCBUZXN0"}, "",
			jan1st2000.Add(30 * 24 * time.Hour)},
		// Trial license - 7 days grace period
		{[]string{"crl-0-EMDYt8MDGAQiDkNSREIgVW5pdCBUZXN0"}, "",
			jan1st2000.Add(7 * 24 * time.Hour)},
		// Enterprise - no grace period
		{[]string{"crl-0-EMDYt8MDGAEiDkNSREIgVW5pdCBUZXN0KAM"}, "",
			timeutil.UnixEpoch},
		// No license - 7 days grace period
		{[]string{""}, "", ts1.Add(30 * 24 * time.Hour)},
		// Two trial license allowed if they both have the same expiry
		{[]string{
			"crl-0-EMDYt8MDGAQiDkNSREIgVW5pdCBUZXN0",
			"crl-0-EMDYt8MDGAQiDkNSREIgVW5pdCBUZXN0",
		}, "", jan1st2000.Add(7 * 24 * time.Hour)},
		// A second trial license is not allowed if it has a different
		// expiry (Jan 1st 2000 8:01 AST)
		{[]string{
			"crl-0-EMDYt8MDGAQiDkNSREIgVW5pdCBUZXN0",
			"crl-0-EPzYt8MDGAQiDkNSREIgVW5pdCBUZXN0",
		}, "a trial license has previously been installed on this cluster",
			timeutil.UnixEpoch},
	} {
		t.Run(fmt.Sprintf("test %d", i), func(t *testing.T) {
			// Reset from prior test unit.
			err := enforcer.TestingResetTrialUsage(ctx)
			require.NoError(t, err)

			tdb := sqlutils.MakeSQLRunner(sqlDB)

			// Loop through all but the last license. They should all
			// succeed.
			for i := 0; i < len(tc.licenses)-1; i++ {
				stmt := fmt.Sprintf(
					"SET CLUSTER SETTING enterprise.license = '%s'",
					tc.licenses[i])
				tdb.Exec(t, stmt)

				// If installing a trial license, we need to wait for the
				// callback to bump the expiry before continuing. We depend
				// on the expiry to cause an error if another trial license
				// is installed.
				l, err := licensepb.Decode(tc.licenses[i])
				require.NoError(t, err)
				if l.Type == licensepb.License_Trial {
					require.Eventually(t, func() bool {
						return enforcer.TestingGetTrialUsageExpiry() > 0
					}, 20*time.Second, time.Millisecond,
						"trial usage expiry not set in time")
				}
			}

			// Handle the last license separately
			lastLicense := tc.licenses[len(tc.licenses)-1]
			stmt := fmt.Sprintf(
				"SET CLUSTER SETTING enterprise.license = '%s'",
				lastLicense)

			if tc.errRE != "" {
				// The last license may result in an error
				tdb.ExpectErr(t, tc.errRE, stmt)
				return
			}

			tdb.Exec(t, stmt)

			// The SQL can return back before the callback has finished.
			// So, we wait a bit to see if the desired state is reached.
			var hasLicense bool
			require.Eventually(t, func() bool {
				hasLicense = enforcer.GetHasLicense()
				return (lastLicense != "") == hasLicense
			}, 20*time.Second, time.Millisecond,
				"GetHasLicense() did not return hasLicense of %t in time",
				lastLicense != "")
			var ts time.Time
			var hasGP bool
			require.Eventually(t, func() bool {
				ts, hasGP = enforcer.GetGracePeriodEndTS()
				if tc.expectedGracePeriodEnd.Equal(timeutil.UnixEpoch) {
					return !hasGP
				}
				return ts.Equal(tc.expectedGracePeriodEnd)
			}, 20*time.Second, time.Millisecond,
				"GetGracePeriodEndTS() did not return grace period of %s in time",
				tc.expectedGracePeriodEnd.String())

			// Perform the throttle check after the license change. We
			// expect an error if a grace period is set, since all licenses
			// expired long ago and any grace period would have already
			// ended.
			const aboveThreshold = 100
			_, err = enforcer.TestingMaybeFailIfThrottled(
				ctx, aboveThreshold)
			if tc.expectedGracePeriodEnd.Equal(timeutil.UnixEpoch) {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				require.Contains(t, err.Error(),
					"maximum number of concurrently open transactions has been reached")
			}
		})
	}
}
