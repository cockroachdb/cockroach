// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/LICENSE

package licenseccl

import (
	"testing"
	"time"

	"fmt"

	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

func TestLicense(t *testing.T) {
	clusterA, _ := uuid.FromString("A0000000-0000-0000-0000-00000000000A")
	clusterB, _ := uuid.FromString("B0000000-0000-0000-0000-00000000000B")

	clustersA, clustersB := []uuid.UUID{clusterA}, []uuid.UUID{clusterB}

	t0 := timeutil.Unix(0, 0)
	ts := t0.AddDate(40, 0, 0)
	after := ts.Add(time.Hour * 24)
	before := ts.Add(time.Hour * -24)
	wayAfter := ts.Add(time.Hour * 24 * 365 * 200)

	for i, tc := range []struct {
		licType      License_Type
		grantedTo    []uuid.UUID
		expiration   time.Time
		checkCluster uuid.UUID
		checkOrg     string
		checkTime    time.Time
		err          string
	}{
		{licType: -1, err: "requires an enterprise license"},
		{License_Evaluation, clustersA, ts, clusterA, "", ts, ""},
		{License_Enterprise, clustersA, ts, clusterA, "", ts, ""},
		{License_NonCommercial, clustersA, ts, clusterA, "", ts, ""},
		{License_Evaluation, clustersA, after, clusterA, "", ts, ""},
		{License_Evaluation, clustersA, ts, clusterA, "", before, ""},
		{License_Evaluation, clustersA, wayAfter, clusterA, "", ts, ""},

		// expirations.
		{License_Evaluation, clustersA, ts, clusterA, "", after, "expired"},
		{License_Evaluation, clustersA, after, clusterA, "", wayAfter, "expired"},
		{License_NonCommercial, clustersA, after, clusterA, "", wayAfter, "expired"},
		{License_NonCommercial, clustersA, t0, clusterA, "", wayAfter, ""},
		{License_Evaluation, clustersA, t0, clusterA, "", wayAfter, ""},

		// grace period.
		{License_Enterprise, clustersA, after, clusterA, "", wayAfter, ""},

		// mismatch.
		{License_Enterprise, clustersA, ts, clusterB, "", ts, "not valid for cluster"},
		{License_Enterprise, clustersB, ts, clusterA, "", ts, "not valid for cluster"},
		{License_Enterprise, append(clustersB, clusterA), ts, clusterA, "", ts, ""},
		{License_Enterprise, nil, ts, clusterA, "", ts, "license valid only for"},
		{License_Enterprise, nil, ts, clusterA, "tc-17", ts, ""},
	} {
		var lic *License
		if tc.licType != -1 {
			s, err := License{
				tc.grantedTo,
				tc.expiration.Unix(),
				tc.licType,
				fmt.Sprintf("tc-%d", i),
			}.Encode()
			if err != nil {
				t.Fatal(err)
			}

			lic, err = Decode(s)
			if err != nil {
				t.Fatal(err)
			}
		}
		if err := lic.Check(
			tc.checkTime, tc.checkCluster, tc.checkOrg, "",
		); !testutils.IsError(err, tc.err) {
			t.Fatalf("%d: lic for %s to %s, checked by %s at %s.\n got %q", i,
				tc.grantedTo, tc.expiration, tc.checkCluster, tc.checkTime, err)
		}
	}
}

func TestBadLicenseStrings(t *testing.T) {
	for _, tc := range []struct{ lic, err string }{
		{"blah", "invalid license string"},
		{"crl-0-&&&&&", "invalid license string"},
		{"crl-0-blah", "invalid license string"},
	} {
		if _, err := Decode(tc.lic); !testutils.IsError(err, tc.err) {
			t.Fatalf("%q: expected err %q, got %v", tc.lic, tc.err, err)
		}
	}
}
