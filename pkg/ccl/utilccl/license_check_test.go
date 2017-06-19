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
	clusterA, _ := uuid.FromString("A0000000-0000-0000-0000-00000000000A")
	clusterB, _ := uuid.FromString("B0000000-0000-0000-0000-00000000000B")

	epoch := time.Unix(0, 0)
	after := epoch.Add(time.Hour * 24)
	before := epoch.Add(time.Hour * -24)
	wayAfter := epoch.Add(time.Hour * 24 * 365 * 200)

	for i, tc := range []struct {
		licType      License_Type
		grantedTo    uuid.UUID
		expiration   time.Time
		checkCluster uuid.UUID
		checkTime    time.Time
		err          string
	}{
		{licType: -1, err: "requires an enterprise license"},
		{License_Evaluation, clusterA, epoch, clusterA, epoch, ""},
		{License_Enterprise, clusterA, epoch, clusterA, epoch, ""},
		{License_NonCommercial, clusterA, epoch, clusterA, epoch, ""},
		{License_Evaluation, clusterA, after, clusterA, epoch, ""},
		{License_Evaluation, clusterA, epoch, clusterA, before, ""},
		{License_Evaluation, clusterA, wayAfter, clusterA, epoch, ""},

		// clear existing.
		{licType: -1, err: "requires an enterprise license"},

		// expirations.
		{License_Evaluation, clusterA, epoch, clusterA, after, "expired"},
		{License_Evaluation, clusterA, after, clusterA, wayAfter, "expired"},
		{License_NonCommercial, clusterA, after, clusterA, wayAfter, "expired"},

		// grace period.
		{License_Enterprise, clusterA, after, clusterA, wayAfter, ""},

		// mismatch.
		{License_Enterprise, clusterA, epoch, clusterB, epoch, "not valid for cluster"},
		{License_Enterprise, clusterB, epoch, clusterA, epoch, "not valid for cluster"},

		// clear existing, even if invalid.
		{licType: -1, err: "requires an enterprise license"},
	} {
		licStr := ""
		if tc.licType != -1 {
			licStr, _ = License{
				[]uuid.UUID{tc.grantedTo},
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
			t.Fatalf("%d: lic for %s to %s, checked by %s at %s.\n got %q", i,
				tc.grantedTo.Short(), tc.expiration, tc.checkCluster.Short(), tc.checkTime, err)
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
