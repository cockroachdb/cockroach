// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package utilccl

import (
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl/licenseccl"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

func TestLicense(t *testing.T) {
	t0 := timeutil.Unix(0, 0)
	ts := t0.AddDate(40, 0, 0)
	after := ts.Add(time.Hour * 24)
	before := ts.Add(time.Hour * -24)
	wayAfter := ts.Add(time.Hour * 24 * 365 * 200)

	for i, tc := range []struct {
		licType    licenseccl.License_Type
		expiration time.Time
		checkOrg   string
		checkTime  time.Time
		err        string
	}{
		{licType: -1, err: "requires an enterprise license"},
		{licenseccl.License_Evaluation, ts, "tc-1", ts, ""},
		{licenseccl.License_Enterprise, ts, "tc-2", ts, ""},
		{licenseccl.License_NonCommercial, ts, "tc-3", ts, ""},
		{licenseccl.License_Evaluation, after, "tc-4", ts, ""},
		{licenseccl.License_Evaluation, ts, "tc-5", before, ""},
		{licenseccl.License_Evaluation, wayAfter, "tc-6", ts, ""},

		// expirations.
		{licenseccl.License_Evaluation, ts, "tc-7", after, "expired"},
		{licenseccl.License_Evaluation, after, "tc-8", wayAfter, "expired"},
		{licenseccl.License_NonCommercial, after, "tc-9", wayAfter, "expired"},
		{licenseccl.License_NonCommercial, t0, "tc-10", wayAfter, ""},
		{licenseccl.License_Evaluation, t0, "tc-11", wayAfter, ""},

		// grace period.
		{licenseccl.License_Enterprise, after, "tc-12", wayAfter, ""},

		// mismatch.
		{licenseccl.License_Enterprise, ts, "tc-13", ts, ""},
		{licenseccl.License_Enterprise, ts, "", ts, "license valid only for"},
		{licenseccl.License_Enterprise, ts, "tc-15", ts, ""},
	} {
		t.Run("", func(t *testing.T) {
			var lic *licenseccl.License
			if tc.licType != -1 {
				s, err := (&licenseccl.License{
					ValidUntilUnixSec: tc.expiration.Unix(),
					Type:              tc.licType,
					OrganizationName:  fmt.Sprintf("tc-%d", i),
				}).Encode()
				if err != nil {
					t.Fatal(err)
				}

				lic, err = decode(s)
				if err != nil {
					t.Fatal(err)
				}
			}
			if err := check(
				lic, tc.checkTime, uuid.UUID{}, tc.checkOrg, "", true,
			); !testutils.IsError(err, tc.err) {
				t.Fatalf("%d: lic to %s, checked at %s.\n got %q", i,
					tc.expiration, tc.checkTime, err)
			}
		})
	}
}

func TestBadLicenseStrings(t *testing.T) {
	for _, tc := range []struct{ lic, err string }{
		{"blah", "invalid license string"},
		{"crl-0-&&&&&", "invalid license string"},
		{"crl-0-blah", "invalid license string"},
	} {
		if _, err := decode(tc.lic); !testutils.IsError(err, tc.err) {
			t.Fatalf("%q: expected err %q, got %v", tc.lic, tc.err, err)
		}
	}
}

func TestExpiredLicenseLanguage(t *testing.T) {
	lic := &licenseccl.License{
		Type:              licenseccl.License_Evaluation,
		ValidUntilUnixSec: 1,
	}
	err := check(lic, timeutil.Now(), uuid.MakeV4(), "", "RESTORE", true)
	expected := "Use of RESTORE requires an enterprise license. Your evaluation license expired on " +
		"January 1, 1970. If you're interested in getting a new license, please contact " +
		"subscriptions@cockroachlabs.com and we can help you out."
	if err == nil || err.Error() != expected {
		t.Fatalf("expected err:\n%s\ngot:\n%v", expected, err)
	}
}
