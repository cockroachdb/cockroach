// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package license

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/server/license/licensepb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

func TestGetLicenseTypePresent(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	for _, tc := range []struct {
		typ                 licensepb.License_Type
		expectedType        string
		environment         licensepb.License_Environment
		expectedEnvironment string
	}{
		{licensepb.License_NonCommercial, "NonCommercial", licensepb.PreProduction, "pre-production"},
		{licensepb.License_Enterprise, "Enterprise", licensepb.Production, "production"},
		{licensepb.License_Evaluation, "Evaluation", licensepb.Development, "development"},
		{licensepb.License_Enterprise, "Enterprise", licensepb.Unspecified, ""},
		{licensepb.License_Free, "Free", licensepb.Development, "development"},
		{licensepb.License_Trial, "Trial", licensepb.PreProduction, "pre-production"},
	} {
		st := cluster.MakeTestingClusterSettings()
		updater := st.MakeUpdater()
		lic, _ := (&licensepb.License{
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
			t.Fatalf("expected license environment %s, got %s",
				tc.expectedEnvironment, actualEnvironment)
		}
	}
}

func TestUnknownEnvironmentEnum(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// This literal was generated with an enum value of 100 for environment,
	// to show what happens if we add more environments later and then try to
	// apply one to an older node which does not include it.
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

	lic1M, _ := (&licensepb.License{
		Type:              licensepb.License_Enterprise,
		ValidUntilUnixSec: t0.AddDate(0, 1, 0).Unix(),
	}).Encode()

	lic2M, _ := (&licensepb.License{
		Type:              licensepb.License_Evaluation,
		ValidUntilUnixSec: t0.AddDate(0, 2, 0).Unix(),
	}).Encode()

	lic0M, _ := (&licensepb.License{
		Type:              licensepb.License_Free,
		ValidUntilUnixSec: t0.AddDate(0, 0, 0).Unix(),
	}).Encode()

	licExpired, _ := (&licensepb.License{
		Type:              licensepb.License_Trial,
		ValidUntilUnixSec: t0.AddDate(0, -1, 0).Unix(),
	}).Encode()

	st := cluster.MakeTestingClusterSettings()
	updater := st.MakeUpdater()
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
			actual := GetLicenseTTL(context.Background(), st, manualTime)
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
