// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package utilccl

import (
	"context"
	"fmt"
	"strconv"
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
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

func TestSettingAndCheckingLicense(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	t0 := timeutil.Unix(0, 0)

	licA, _ := (&licenseccl.License{
		Type:              licenseccl.License_Enterprise,
		ValidUntilUnixSec: t0.AddDate(0, 1, 0).Unix(),
	}).Encode()

	st := cluster.MakeTestingClusterSettings()

	for i, tc := range []struct {
		lic       string
		checkTime time.Time
		err       string
	}{
		// NB: we're observing the update manifest as changed behavior -- detailed
		// testing of that behavior is left to licenseccl's own tests.
		{"", t0, "requires an enterprise license"},
		// adding a valid lic.
		{licA, t0, ""},
		// clearing an existing lic.
		{"", t0, "requires an enterprise license"},
		// clearing an existing, invalid lic.
		{"", t0, "requires an enterprise license"},
	} {
		updater := st.MakeUpdater()
		if err := setLicense(ctx, updater, tc.lic); err != nil {
			t.Fatal(err)
		}
		err := checkEnterpriseEnabledAt(st, tc.checkTime, "", true)
		if !testutils.IsError(err, tc.err) {
			l, _ := decode(tc.lic)
			t.Fatalf("%d: lic %v, update by %T, checked at %s, got %q", i, l, updater, tc.checkTime, err)
		}
	}
}

// test setting a license with a specific diagnostics setting.
func TestSetLicenseWithDiagnosticsReporting(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	t0 := timeutil.Unix(0, 0)
	updater := st.MakeUpdater()
	var enterpriseLicense, err = (&licenseccl.License{
		Type:              licenseccl.License_Enterprise,
		ValidUntilUnixSec: t0.AddDate(0, 1, 0).Unix(),
	}).Encode()
	require.NoError(t, err)

	resetLicense := func() {
		err = setLicense(ctx, updater, enterpriseLicense)
		require.NoError(t, err)
	}

	for _, tc := range []struct {
		lit         licenseccl.License_Type
		diagnostics bool
		err         string
	}{
		{licenseccl.License_Free, false, "diagnostics.reporting.enabled must be true to use this license"},
		{licenseccl.License_Free, true, ""},
		{licenseccl.License_Trial, false, "diagnostics.reporting.enabled must be true to use this license"},
		{licenseccl.License_Trial, true, ""},
		{licenseccl.License_NonCommercial, false, ""},
		{licenseccl.License_NonCommercial, true, ""},
		{licenseccl.License_Enterprise, false, ""},
		{licenseccl.License_Enterprise, true, ""},
		{licenseccl.License_Evaluation, false, ""},
		{licenseccl.License_Evaluation, true, ""},
	} {
		// First set the license to enterprise so that we can change the diagnostics setting.
		resetLicense()
		// Then, set the diagnostics value for the test.
		if err := setDiagnosticsReporting(ctx, updater, tc.diagnostics); err != nil {
			t.Fatal(err)
		}
		// Finally, verify that trying to set the license gives the appropriate validation response.
		lic, _ := (&licenseccl.License{
			Type:              tc.lit,
			ValidUntilUnixSec: t0.AddDate(0, 1, 0).Unix(),
		}).Encode()
		if err := setLicense(ctx, updater, lic); !testutils.IsError(
			err, tc.err,
		) {
			t.Fatalf("%s %t: expected err %q, got %v", tc.lit, tc.diagnostics, tc.err, err)
		}

	}
}

// test setting the diagnostics setting with a specific license.
func TestSetDiagnosticsReportingWithLicense(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	t0 := timeutil.Unix(0, 0)
	updater := st.MakeUpdater()

	resetDiagnostics := func() {
		err := setDiagnosticsReporting(ctx, updater, true)
		require.NoError(t, err)
	}

	for _, tc := range []struct {
		lit         licenseccl.License_Type
		diagnostics bool
		err         string
	}{
		{licenseccl.License_Free, false, "unable to disable diagnostics with license type Free"},
		{licenseccl.License_Free, true, ""},
		{licenseccl.License_Trial, false, "unable to disable diagnostics with license type Trial"},
		{licenseccl.License_Trial, true, ""},
		{licenseccl.License_NonCommercial, false, ""},
		{licenseccl.License_NonCommercial, true, ""},
		{licenseccl.License_Enterprise, false, ""},
		{licenseccl.License_Enterprise, true, ""},
		{licenseccl.License_Evaluation, false, ""},
		{licenseccl.License_Evaluation, true, ""},
	} {
		// First set the diagnostics to true so that we can change the license
		resetDiagnostics()
		// Then change the license value for the test.
		lic, _ := (&licenseccl.License{
			Type:              tc.lit,
			ValidUntilUnixSec: t0.AddDate(0, 1, 0).Unix(),
		}).Encode()
		if err := setLicense(ctx, updater, lic); err != nil {
			t.Fatal(err)
		}
		// Finally, verify that trying to set the diagnostics value gives the appropriate validation response.
		if err := setDiagnosticsReporting(ctx, updater, tc.diagnostics); !testutils.IsError(
			err, tc.err,
		) {
			t.Fatalf("%s %t: expected err %q, got %v", tc.lit, tc.diagnostics, tc.err, err)
		}
	}
}

func TestGetLicenseTypePresent(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	for _, tc := range []struct {
		typ                 licenseccl.License_Type
		expectedType        string
		environment         licenseccl.License_Environment
		expectedEnvironment string
	}{
		{licenseccl.License_NonCommercial, "NonCommercial", licenseccl.PreProduction, "pre-production"},
		{licenseccl.License_Enterprise, "Enterprise", licenseccl.Production, "production"},
		{licenseccl.License_Evaluation, "Evaluation", licenseccl.Development, "development"},
		{licenseccl.License_Enterprise, "Enterprise", licenseccl.Unspecified, ""},
		{licenseccl.License_Free, "Free", licenseccl.Development, "development"},
		{licenseccl.License_Trial, "Trial", licenseccl.PreProduction, "pre-production"},
	} {
		st := cluster.MakeTestingClusterSettings()
		updater := st.MakeUpdater()
		lic, _ := (&licenseccl.License{
			Type:              tc.typ,
			ValidUntilUnixSec: 0,
			Environment:       tc.environment,
		}).Encode()
		if err := setLicense(ctx, updater, lic); err != nil {
			t.Fatal(err)
		}
		actualType, err := GetLicenseType(st)
		if err != nil {
			t.Fatal(err)
		}
		if actualType != tc.expectedType {
			t.Fatalf("expected license type %s, got %s", tc.expectedType, actualType)
		}
		actualEnvironment, err := GetLicenseEnvironment(st)
		if err != nil {
			t.Fatal(err)
		}
		if actualEnvironment != tc.expectedEnvironment {
			t.Fatalf("expected license environment %s, got %s", tc.expectedEnvironment, actualEnvironment)
		}
	}
}

func TestUnknownEnvironmentEnum(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// This literal was generated with an enum value of 100 for environment, to show
	// what happens if we add more environments later and then try to apply one to an
	// older node which does not include it.
	l, err := decode(`crl-0-GAIoZA`)
	if err != nil {
		t.Fatal(err)
	}
	if expected, got := "Evaluation", l.Type.String(); got != expected {
		t.Fatalf("expected license type %s, got %s", expected, got)
	}
	if expected, got := "other", l.Environment.String(); got != expected {
		t.Fatalf("expected license environment %q, got %q", expected, got)
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

	ctx := context.Background()

	t0 := timeutil.Unix(1603926294, 0)

	lic1M, _ := (&licenseccl.License{
		Type:              licenseccl.License_Enterprise,
		ValidUntilUnixSec: t0.AddDate(0, 1, 0).Unix(),
	}).Encode()

	lic2M, _ := (&licenseccl.License{
		Type:              licenseccl.License_Evaluation,
		ValidUntilUnixSec: t0.AddDate(0, 2, 0).Unix(),
	}).Encode()

	lic0M, _ := (&licenseccl.License{
		Type:              licenseccl.License_Free,
		ValidUntilUnixSec: t0.AddDate(0, 0, 0).Unix(),
	}).Encode()

	licExpired, _ := (&licenseccl.License{
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

func TestApplyTenantLicenseWithLicense(t *testing.T) {
	defer leaktest.AfterTest(t)()

	license, err := (&licenseccl.License{
		Type: licenseccl.License_Enterprise,
	}).Encode()
	require.NoError(t, err)

	defer TestingDisableEnterprise()()
	defer envutil.TestSetEnv(t, "COCKROACH_TENANT_LICENSE", license)()

	settings := cluster.MakeClusterSettings()

	require.Error(t, CheckEnterpriseEnabled(settings, ""))
	require.False(t, IsEnterpriseEnabled(settings, ""))
	require.NoError(t, ApplyTenantLicense())
	require.NoError(t, CheckEnterpriseEnabled(settings, ""))
	require.True(t, IsEnterpriseEnabled(settings, ""))
}

func TestApplyTenantLicenseWithoutLicense(t *testing.T) {
	defer leaktest.AfterTest(t)()

	defer TestingDisableEnterprise()()

	settings := cluster.MakeClusterSettings()
	_, ok := envutil.EnvString("COCKROACH_TENANT_LICENSE", 0)
	envutil.ClearEnvCache()
	require.False(t, ok)

	require.Error(t, CheckEnterpriseEnabled(settings, ""))
	require.False(t, IsEnterpriseEnabled(settings, ""))
	require.NoError(t, ApplyTenantLicense())
	require.Error(t, CheckEnterpriseEnabled(settings, ""))
	require.False(t, IsEnterpriseEnabled(settings, ""))
}

func TestApplyTenantLicenseWithInvalidLicense(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer envutil.TestSetEnv(t, "COCKROACH_TENANT_LICENSE", "THIS IS NOT A VALID LICENSE")()
	require.Error(t, ApplyTenantLicense())
}

func setLicense(ctx context.Context, updater settings.Updater, val string) error {
	return updater.Set(ctx, "enterprise.license", settings.EncodedValue{
		Value: val,
		Type:  "s",
	})
}

func setDiagnosticsReporting(ctx context.Context, updater settings.Updater, val bool) error {
	return updater.Set(ctx, "diagnostics.reporting.enabled", settings.EncodedValue{
		Value: strconv.FormatBool(val),
		Type:  "b",
	})
}

func TestRefreshLicenseEnforcerOnLicenseChange(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ts1 := timeutil.Unix(1724329716, 0)

	ctx := context.Background()
	srv, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{
		// We are changing a cluster setting that can only be done at the system tenant.
		DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
		Knobs: base.TestingKnobs{
			LicenseTestingKnobs: &license.TestingKnobs{
				Enable:            true,
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

	// Test to ensure that the state is correctly registered on startup before
	// changing the license.
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
		{[]string{"crl-0-EMDYt8MDGAQiDkNSREIgVW5pdCBUZXN0", "crl-0-EPzYt8MDGAQiDkNSREIgVW5pdCBUZXN0"},
			"a trial license has previously been installed on this cluster", timeutil.UnixEpoch},
	} {
		t.Run(fmt.Sprintf("test %d", i), func(t *testing.T) {
			// Reset from prior test unit.
			err := enforcer.TestingResetTrialUsage(ctx)
			require.NoError(t, err)

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
