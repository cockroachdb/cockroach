// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/LICENSE

package utilccl

import (
	"testing"
	"time"

	"fmt"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

func TestLicense(t *testing.T) {
	idA, _ := uuid.FromString("A0000000-0000-0000-0000-00000000000A")
	idB, _ := uuid.FromString("B0000000-0000-0000-0000-00000000000B")
	idC, _ := uuid.FromString("C0000000-0000-0000-0000-00000000000C")
	grantToA := []uuid.UUID{idA}
	grantToB := []uuid.UUID{idB}

	epoch := time.Unix(0, 0)
	after := epoch.Add(time.Hour * 24)
	before := epoch.Add(time.Hour * -24)
	wayAfter := epoch.Add(time.Hour * 24 * 365 * 200)

	for i, tc := range []struct {
		licType      License_Type
		grantedTo    []uuid.UUID
		expiration   time.Time
		checkCluster uuid.UUID
		checkTime    time.Time
		err          string
	}{
		{licType: -1, err: "requires an enterprise license"},
		{License_Evaluation, grantToA, epoch, idA, epoch, ""},
		{License_Enterprise, grantToA, epoch, idA, epoch, ""},
		{License_NonCommercial, grantToA, epoch, idA, epoch, ""},
		{License_Evaluation, grantToA, after, idA, epoch, ""},
		{License_Evaluation, grantToA, epoch, idA, before, ""},
		{License_Evaluation, grantToA, wayAfter, idA, epoch, ""},

		// clear existing.
		{licType: -1, err: "requires an enterprise license"},

		// expirations.
		{License_Evaluation, grantToA, epoch, idA, after, "expired"},
		{License_Evaluation, grantToA, after, idA, wayAfter, "expired"},
		{License_NonCommercial, grantToA, after, idA, wayAfter, "expired"},

		// grace period.
		{License_Enterprise, grantToA, after, idA, wayAfter, ""},

		// cluster match and mismatch.
		{License_Enterprise, grantToA, epoch, idA, epoch, ""},
		{License_Enterprise, grantToA, epoch, idB, epoch, "not valid for cluster"},
		{License_Enterprise, grantToB, epoch, idB, epoch, ""},
		{License_Enterprise, grantToB, epoch, idA, epoch, "not valid for cluster"},
		{License_Enterprise, nil, epoch, idA, epoch, ""},
		{License_Enterprise, nil, epoch, idB, epoch, ""},
		{License_Enterprise, nil, epoch, idC, epoch, ""},
		{License_Enterprise, append(grantToA, idB), epoch, idA, epoch, ""},
		{License_Enterprise, append(grantToA, idB), epoch, idB, epoch, ""},
		{License_Enterprise, append(grantToA, idB), epoch, idC, epoch, "not valid for cluster"},

		// clear existing, even if invalid.
		{licType: -1, err: "requires an enterprise license"},
	} {
		licStr := ""
		if tc.licType != -1 {
			licStr, _ = License{
				tc.grantedTo,
				tc.expiration.Unix(),
				tc.licType,
				fmt.Sprintf("tc-%d", i),
			}.Encode()
		}
		if err := (settings.Updater{}).Set("enterprise.license", licStr, "s"); err != nil {
			t.Fatal(err)
		}

		err := checkEnterpriseEnabledAt(tc.checkCluster, tc.checkTime, "")
		if !testutils.IsError(err, tc.err) {
			t.Fatalf("%d: lic for %v to %s, checked by %s at %s.\n got %q", i,
				tc.grantedTo, tc.expiration, tc.checkCluster, tc.checkTime, err)
		}
	}
}

func TestBadLicenseStrings(t *testing.T) {
	for _, tc := range []struct{ lic, err string }{
		{"blah", "invalid license string"},
		{"cl-0-blah", "invalid license string"},
	} {
		if err := (settings.Updater{}).Set("enterprise.license", tc.lic, "s"); !testutils.IsError(
			err, tc.err,
		) {
			t.Fatalf("%q: expected err %q, got %v", tc.lic, tc.err, err)
		}
	}
}
