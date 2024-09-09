// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package utilccl

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl/licenseccl"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/server/license"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

func TestSettingAndCheckingLicense(t *testing.T) {
	defer leaktest.AfterTest(t)()

	idA, _ := uuid.FromString("A0000000-0000-0000-0000-00000000000A")
	idB, _ := uuid.FromString("B0000000-0000-0000-0000-00000000000B")

	ctx := context.Background()
	t0 := timeutil.Unix(0, 0)

	licA, _ := (&licenseccl.License{
		ClusterID:         []uuid.UUID{idA},
		Type:              licenseccl.License_Enterprise,
		ValidUntilUnixSec: t0.AddDate(0, 1, 0).Unix(),
	}).Encode()

	licB, _ := (&licenseccl.License{
		ClusterID:         []uuid.UUID{idB},
		Type:              licenseccl.License_Evaluation,
		ValidUntilUnixSec: t0.AddDate(0, 2, 0).Unix(),
	}).Encode()

	st := cluster.MakeTestingClusterSettings()

	for i, tc := range []struct {
		lic          string
		checkCluster uuid.UUID
		checkTime    time.Time
		err          string
	}{
		// NB: we're observing the update manifest as changed behavior -- detailed
		// testing of that behavior is left to licenseccl's own tests.
		{"", idA, t0, "requires an enterprise license"},
		// adding a valid lic.
		{licA, idA, t0, ""},
		// clearing an existing lic.
		{"", idA, t0, "requires an enterprise license"},
		// adding invalid lic.
		{licB, idA, t0, "not valid for cluster"},
		// clearing an existing, invalid lic.
		{"", idA, t0, "requires an enterprise license"},
	} {
		updater := st.MakeUpdater()
		if err := setLicense(ctx, updater, tc.lic); err != nil {
			t.Fatal(err)
		}
		err := checkEnterpriseEnabledAt(st, tc.checkTime, tc.checkCluster, "", true)
		if !testutils.IsError(err, tc.err) {
			l, _ := decode(tc.lic)
			t.Fatalf("%d: lic %v, update by %T, checked by %s at %s, got %q", i, l, updater, tc.checkCluster, tc.checkTime, err)
		}
	}
}

func TestGetLicenseTypePresent(t *testing.T) {
	defer leaktest.AfterTest(t)()

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

	err := UpdateMetricOnLicenseChange(context.Background(), st, base.LicenseTTL, manualTime, stopper)
	require.NoError(t, err)

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
			actual := base.LicenseTTL.Value()
			require.Equal(t, tc.ttlSeconds, actual)
		})
	}
}

func TestApplyTenantLicenseWithLicense(t *testing.T) {
	defer leaktest.AfterTest(t)()

	license, _ := (&licenseccl.License{
		Type: licenseccl.License_Enterprise,
	}).Encode()

	defer TestingDisableEnterprise()()
	defer envutil.TestSetEnv(t, "COCKROACH_TENANT_LICENSE", license)()

	settings := cluster.MakeClusterSettings()

	require.Error(t, CheckEnterpriseEnabled(settings, uuid.MakeV4(), ""))
	require.False(t, IsEnterpriseEnabled(settings, uuid.MakeV4(), ""))
	require.NoError(t, ApplyTenantLicense())
	require.NoError(t, CheckEnterpriseEnabled(settings, uuid.MakeV4(), ""))
	require.True(t, IsEnterpriseEnabled(settings, uuid.MakeV4(), ""))
}

func TestApplyTenantLicenseWithoutLicense(t *testing.T) {
	defer leaktest.AfterTest(t)()

	defer TestingDisableEnterprise()()

	settings := cluster.MakeClusterSettings()
	_, ok := envutil.EnvString("COCKROACH_TENANT_LICENSE", 0)
	envutil.ClearEnvCache()
	require.False(t, ok)

	require.Error(t, CheckEnterpriseEnabled(settings, uuid.MakeV4(), ""))
	require.False(t, IsEnterpriseEnabled(settings, uuid.MakeV4(), ""))
	require.NoError(t, ApplyTenantLicense())
	require.Error(t, CheckEnterpriseEnabled(settings, uuid.MakeV4(), ""))
	require.False(t, IsEnterpriseEnabled(settings, uuid.MakeV4(), ""))
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

func TestRefreshLicenseEnforcerOnLicenseChange(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ts1 := timeutil.Unix(1724329716, 0)

	ctx := context.Background()
	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			Server: &server.TestingKnobs{
				LicenseTestingKnobs: license.TestingKnobs{
					OverrideStartTime: &ts1,
				},
			},
		},
	})
	defer srv.Stopper().Stop(ctx)

	// All of the licenses that we install later depend on this org name.
	_, err := db.Exec(
		"SET CLUSTER SETTING cluster.organization = 'CRDB Unit Test'",
	)
	require.NoError(t, err)

	// Test to ensure that the state is correctly registered on startup before
	// changing the license.
	enforcer := license.GetEnforcerInstance()
	require.Equal(t, false, enforcer.GetHasLicense())
	gracePeriodTS, hasGracePeriod := enforcer.GetGracePeriodEndTS()
	require.True(t, hasGracePeriod)
	require.Equal(t, ts1.Add(7*24*time.Hour), gracePeriodTS)

	jan1st2000 := timeutil.Unix(946728000, 0)

	for i, tc := range []struct {
		license                string
		expectedGracePeriodEnd time.Time
	}{
		// Note: all licenses below expire on Jan 1st 2000
		//
		// Free license - 30 days grace period
		{"crl-0-EMDYt8MDGAMiDkNSREIgVW5pdCBUZXN0", jan1st2000.Add(30 * 24 * time.Hour)},
		// Trial license - 7 days grace period
		{"crl-0-EMDYt8MDGAQiDkNSREIgVW5pdCBUZXN0", jan1st2000.Add(7 * 24 * time.Hour)},
		// Enterprise - no grace period
		{"crl-0-EMDYt8MDGAEiDkNSREIgVW5pdCBUZXN0KAM", timeutil.UnixEpoch},
		// No license - 7 days grace period
		{"", ts1.Add(7 * 24 * time.Hour)},
	} {
		t.Run(fmt.Sprintf("test %d", i), func(t *testing.T) {
			_, err := db.Exec(
				fmt.Sprintf("SET CLUSTER SETTING enterprise.license = '%s'", tc.license),
			)
			require.NoError(t, err)
			// The SQL can return back before the callback has finished. So, we wait a
			// bit to see if the desired state is reached.
			var hasLicense bool
			require.Eventually(t, func() bool {
				hasLicense = enforcer.GetHasLicense()
				return (tc.license != "") == hasLicense
			}, 20*time.Second, time.Millisecond,
				"GetHasLicense() last returned %t", hasLicense)
			var ts time.Time
			var hasGracePeriod bool
			require.Eventually(t, func() bool {
				ts, hasGracePeriod = enforcer.GetGracePeriodEndTS()
				if tc.expectedGracePeriodEnd.Equal(timeutil.UnixEpoch) {
					return !hasGracePeriod
				}
				return ts.Equal(tc.expectedGracePeriodEnd)
			}, 20*time.Second, time.Millisecond,
				"GetGracePeriodEndTS() last returned %v (%t)", ts, hasGracePeriod)
		})
	}
}
