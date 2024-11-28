// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package utilccl

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl/licenseccl"
	"github.com/cockroachdb/cockroach/pkg/server/license"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

func TestSettingAndCheckingLicense(t *testing.T) {
	defer leaktest.AfterTest(t)()

	idA, _ := uuid.FromString("A0000000-0000-0000-0000-00000000000A")

	ctx := context.Background()
	t0 := timeutil.Unix(0, 0)

	licA, _ := (&licenseccl.License{
		ClusterID:         []uuid.UUID{idA},
		Type:              licenseccl.License_Enterprise,
		ValidUntilUnixSec: t0.AddDate(0, 1, 0).Unix(),
	}).Encode()

	st := cluster.MakeTestingClusterSettings()

	for _, tc := range []struct {
		lic string
	}{
		// NB: we're observing the update manifest as changed behavior -- detailed
		// testing of that behavior is left to licenseccl's own tests.
		{""},
		// adding a valid lic.
		{licA},
		// clearing an existing lic.
		{""},
	} {
		updater := st.MakeUpdater()
		if err := setLicense(ctx, updater, tc.lic); err != nil {
			t.Fatal(err)
		}
		err := CheckEnterpriseEnabled(st, uuid.UUID{}, "")
		require.NoError(t, err)
	}
}

func TestGetLicenseTypePresent(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer func() {
		trialLicenseExpiryTimestamp.Store(0)
	}()

	ctx := context.Background()
	for _, tc := range []struct {
		licenseType licenseccl.License_Type
		expected    string
	}{
		{licenseccl.License_NonCommercial, "NonCommercial"},
		{licenseccl.License_Enterprise, "Enterprise"},
		{licenseccl.License_Evaluation, "Evaluation"},
		{licenseccl.License_Trial, "Trial"},
		{licenseccl.License_Free, "Free"},
	} {
		st := cluster.MakeTestingClusterSettings()
		updater := st.MakeUpdater()
		lic, _ := (&licenseccl.License{
			ClusterID:         []uuid.UUID{},
			Type:              tc.licenseType,
			ValidUntilUnixSec: 0,
		}).Encode()
		if err := setLicense(ctx, updater, lic); err != nil {
			t.Fatal(err)
		}
		actual, err := GetLicenseType(st)
		if err != nil {
			t.Fatal(err)
		}
		if actual != tc.expected {
			t.Fatalf("expected license type %s, got %s", tc.expected, actual)
		}
	}
}

func TestGetLicenseTypeAbsent(t *testing.T) {
	defer leaktest.AfterTest(t)()

	expected := "None"
	actual, err := GetLicenseType(cluster.MakeTestingClusterSettings())
	if err != nil {
		t.Fatal(err)
	}
	if actual != expected {
		t.Fatalf("expected license type %s, got %s", expected, actual)
	}
}

func TestSettingBadLicenseStrings(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	for _, tc := range []struct{ lic, err string }{
		{"blah", "invalid license string"},
		{"cl-0-blah", "invalid license string"},
	} {
		st := cluster.MakeTestingClusterSettings()
		u := st.MakeUpdater()

		if err := setLicense(ctx, u, tc.lic); !testutils.IsError(
			err, tc.err,
		) {
			t.Fatalf("%q: expected err %q, got %v", tc.lic, tc.err, err)
		}
	}
}

func TestTimeToEnterpriseLicenseExpiry(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer func() {
		trialLicenseExpiryTimestamp.Store(0)
	}()

	ctx := context.Background()
	id, _ := uuid.FromString("A0000000-0000-0000-0000-00000000000A")

	t0 := timeutil.Unix(1603926294, 0)

	lic1M, _ := (&licenseccl.License{
		ClusterID:         []uuid.UUID{id},
		Type:              licenseccl.License_Enterprise,
		ValidUntilUnixSec: t0.AddDate(0, 1, 0).Unix(),
	}).Encode()

	lic2M, _ := (&licenseccl.License{
		ClusterID:         []uuid.UUID{id},
		Type:              licenseccl.License_Evaluation,
		ValidUntilUnixSec: t0.AddDate(0, 2, 0).Unix(),
	}).Encode()

	lic0M, _ := (&licenseccl.License{
		ClusterID:         []uuid.UUID{id},
		Type:              licenseccl.License_Free,
		ValidUntilUnixSec: t0.AddDate(0, 0, 0).Unix(),
	}).Encode()

	licExpired, _ := (&licenseccl.License{
		ClusterID:         []uuid.UUID{id},
		Type:              licenseccl.License_Trial,
		ValidUntilUnixSec: t0.AddDate(0, -1, 0).Unix(),
	}).Encode()

	st := cluster.MakeTestingClusterSettings()
	updater := st.MakeUpdater()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	manualTime := timeutil.NewManualTime(t0)

	for _, tc := range []struct {
		desc       string
		lic        string
		ttlSeconds int64
	}{
		{"One Month", lic1M, 24 * 31 * 3600},
		{"Two Month", lic2M, (24*31 + 24*30) * 3600},
		{"Zero Month", lic0M, 0},
		{"Expired", licExpired, (-24 * 30) * 3600},
		{"No License", "", 0},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			if err := setLicense(ctx, updater, tc.lic); err != nil {
				t.Fatal(err)
			}
			actual := base.GetLicenseTTL(context.Background(), st, manualTime)
			require.Equal(t, tc.ttlSeconds, actual)
		})
	}
}

func setLicense(ctx context.Context, updater settings.Updater, val string) error {
	return updater.Set(ctx, "enterprise.license", settings.EncodedValue{
		Value: val,
		Type:  "s",
	})
}

func TestRefreshLicenseEnforcerOnLicenseChange(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ts1 := timeutil.Unix(1724329716, 0)

	ctx := context.Background()
	srv, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			LicenseTestingKnobs: &license.TestingKnobs{
				SkipDisable:       true,
				OverrideStartTime: &ts1,
			},
		},
	})
	defer srv.Stopper().Stop(ctx)

	// All of the licenses that we install later depend on this org name.
	_, err := sqlDB.Exec(
		"SET CLUSTER SETTING cluster.organization = 'CRDB Unit Test'",
	)
	require.NoError(t, err)

	// Test to ensure that the state is correctly registered on startup before
	// changing the license.
	enforcer := srv.ExecutorConfig().(sql.ExecutorConfig).LicenseEnforcer
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
		// errRE specifies a regular expression that matches the expected error message
		// when installing the last license. If no error is expected, this should be
		// set to an empty string.
		errRE string
		// expectedGracePeriodEnd is the timestamp representing when the grace period
		// should end. This value is verified only if the last license is installed
		// successfully.
		expectedGracePeriodEnd time.Time
	}{
		// Note: all licenses below expire on Jan 1st 2000
		//
		// Free license - 30 days grace period
		{[]string{"crl-0-EMDYt8MDGAMiDkNSREIgVW5pdCBUZXN0"}, "", jan1st2000.Add(30 * 24 * time.Hour)},
		// Trial license - 7 days grace period
		{[]string{"crl-0-EMDYt8MDGAQiDkNSREIgVW5pdCBUZXN0"}, "", jan1st2000.Add(7 * 24 * time.Hour)},
		// Enterprise - no grace period
		{[]string{"crl-0-EMDYt8MDGAEiDkNSREIgVW5pdCBUZXN0KAM"}, "", timeutil.UnixEpoch},
		// No license - 7 days grace period
		{[]string{""}, "", ts1.Add(30 * 24 * time.Hour)},
		// Two trial license allowed if they both have the same expiry
		{[]string{"crl-0-EMDYt8MDGAQiDkNSREIgVW5pdCBUZXN0", "crl-0-EMDYt8MDGAQiDkNSREIgVW5pdCBUZXN0"},
			"", jan1st2000.Add(7 * 24 * time.Hour)},
		// A second trial license is not allowed if it has a different expiry (Jan 1st 2000 8:01 AST)
		{[]string{"crl-0-EMDYt8MDGAQiDkNSREIgVW5pdCBUZXN0KAM", "crl-0-EPzYt8MDGAQiDkNSREIgVW5pdCBUZXN0"},
			"a trial license has previously been installed on this cluster", timeutil.UnixEpoch},
	} {
		t.Run(fmt.Sprintf("test %d", i), func(t *testing.T) {
			// Reset at the end of the test unit
			defer func() {
				err = enforcer.TestingResetTrialUsage(ctx)
				require.NoError(t, err)
				trialLicenseExpiryTimestamp.Store(0)
			}()

			tdb := sqlutils.MakeSQLRunner(sqlDB)

			// Loop through all but the last license. They should all succeed.
			for i := 0; i < len(tc.licenses)-1; i++ {
				sql := fmt.Sprintf("SET CLUSTER SETTING enterprise.license = '%s'", tc.licenses[i])
				tdb.Exec(t, sql)

				// If installing a trial license, we need to wait for the callback to
				// bump the expiry before continuing. We depend on the expiry to cause an
				// error if another trial license is installed.
				l, err := decode(tc.licenses[i])
				require.NoError(t, err)
				if l.Type == licenseccl.License_Trial {
					var expiry int64
					require.Eventually(t, func() bool {
						expiry = trialLicenseExpiryTimestamp.Load()
						return expiry > 0
					}, 20*time.Second, time.Millisecond,
						"trialLicenseExpiryTimestamp last returned %t", expiry)
				}
			}

			// Handle the last license separately
			lastLicense := tc.licenses[len(tc.licenses)-1]
			sql := fmt.Sprintf("SET CLUSTER SETTING enterprise.license = '%s'", lastLicense)

			if tc.errRE != "" {
				tdb.ExpectErr(t, tc.errRE, sql) // The last license may result in an error
				return
			}

			tdb.Exec(t, sql)

			// The SQL can return back before the callback has finished. So, we wait a
			// bit to see if the desired state is reached.
			var hasLicense bool
			require.Eventually(t, func() bool {
				hasLicense = enforcer.GetHasLicense()
				return (lastLicense != "") == hasLicense
			}, 20*time.Second, time.Millisecond,
				"GetHasLicense() did not return hasLicense of %t in time", lastLicense != "")
			var ts time.Time
			var hasGracePeriod bool
			require.Eventually(t, func() bool {
				ts, hasGracePeriod = enforcer.GetGracePeriodEndTS()
				if tc.expectedGracePeriodEnd.Equal(timeutil.UnixEpoch) {
					return !hasGracePeriod
				}
				return ts.Equal(tc.expectedGracePeriodEnd)
			}, 20*time.Second, time.Millisecond,
				"GetGracePeriodEndTS() did not return grace period of %s in time", tc.expectedGracePeriodEnd.String())
		})
	}
}
