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
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl/licenseccl"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

func TestSettingAndCheckingLicense(t *testing.T) {
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
		if err := updater.Set(ctx, "enterprise.license", tc.lic, "s"); err != nil {
			t.Fatal(err)
		}
		err := checkEnterpriseEnabledAt(st, tc.checkTime, tc.checkCluster, "", "", true)
		if !testutils.IsError(err, tc.err) {
			l, _ := decode(tc.lic)
			t.Fatalf("%d: lic %v, update by %T, checked by %s at %s, got %q", i, l, updater, tc.checkCluster, tc.checkTime, err)
		}
	}
}

func TestGetLicenseTypePresent(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		licenseType licenseccl.License_Type
		expected    string
	}{
		{licenseccl.License_NonCommercial, "NonCommercial"},
		{licenseccl.License_Enterprise, "Enterprise"},
		{licenseccl.License_Evaluation, "Evaluation"},
	} {
		st := cluster.MakeTestingClusterSettings()
		updater := st.MakeUpdater()
		lic, _ := (&licenseccl.License{
			ClusterID:         []uuid.UUID{},
			Type:              tc.licenseType,
			ValidUntilUnixSec: 0,
		}).Encode()
		if err := updater.Set(ctx, "enterprise.license", lic, "s"); err != nil {
			t.Fatal(err)
		}
		actual, err := getLicenseType(st)
		if err != nil {
			t.Fatal(err)
		}
		if actual != tc.expected {
			t.Fatalf("expected license type %s, got %s", tc.expected, actual)
		}
	}
}

func TestGetLicenseTypeAbsent(t *testing.T) {
	expected := "None"
	actual, err := getLicenseType(cluster.MakeTestingClusterSettings())
	if err != nil {
		t.Fatal(err)
	}
	if actual != expected {
		t.Fatalf("expected license type %s, got %s", expected, actual)
	}
}

func TestSettingBadLicenseStrings(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct{ lic, err string }{
		{"blah", "invalid license string"},
		{"cl-0-blah", "invalid license string"},
	} {
		st := cluster.MakeTestingClusterSettings()
		u := st.MakeUpdater()

		if err := u.Set(ctx, "enterprise.license", tc.lic, "s"); !testutils.IsError(
			err, tc.err,
		) {
			t.Fatalf("%q: expected err %q, got %v", tc.lic, tc.err, err)
		}
	}
}

func TestTimeToEnterpriseLicenseExpiry(t *testing.T) {
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
		Type:              licenseccl.License_Evaluation,
		ValidUntilUnixSec: t0.AddDate(0, 0, 0).Unix(),
	}).Encode()

	licExpired, _ := (&licenseccl.License{
		ClusterID:         []uuid.UUID{id},
		Type:              licenseccl.License_Evaluation,
		ValidUntilUnixSec: t0.AddDate(0, -1, 0).Unix(),
	}).Encode()

	st := cluster.MakeTestingClusterSettings()
	updater := st.MakeUpdater()

	for _, tc := range []struct {
		desc     string
		lic      string
		ttlHours float64
	}{
		{"One Month", lic1M, 24 * 31},
		{"Two Month", lic2M, 24*31 + 24*30},
		{"Zero Month", lic0M, 0},
		{"Expired", licExpired, -24 * 30},
		{"No License", "", 0},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			if err := updater.Set(ctx, "enterprise.license", tc.lic, "s"); err != nil {
				t.Fatal(err)
			}

			actual, err := TimeToEnterpriseLicenseExpiry(context.Background(), st, t0)
			if err != nil {
				t.Fatal(err)
			}

			require.Equal(t, tc.ttlHours, actual.Hours())
		})
	}
}
