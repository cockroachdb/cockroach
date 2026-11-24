// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package license

import (
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/server/license/licensepb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

func TestLicense(t *testing.T) {
	defer leaktest.AfterTest(t)()

	t0 := timeutil.Unix(0, 0)
	ts := t0.AddDate(40, 0, 0)
	after := ts.Add(time.Hour * 24)
	before := ts.Add(time.Hour * -24)
	wayAfter := ts.Add(time.Hour * 24 * 365 * 200)

	// Generate random, yet deterministic, values for the two byte fields.
	// The first byte of each will be incremented after each test to ensure
	// variation.
	orgID := []byte{0}
	licenseID := []byte{0}

	for i, tc := range []struct {
		licType licensepb.License_Type
	}{
		{licensepb.License_Enterprise},
		{licensepb.License_NonCommercial},
		{licensepb.License_Evaluation},
		{licensepb.License_Free},
		{licensepb.License_Trial},
	} {
		t.Run("", func(t *testing.T) {
			for _, ts := range []time.Time{ts, t0, after, wayAfter, before} {
				s, err := (&licensepb.License{
					ValidUntilUnixSec: ts.Unix(),
					Type:              tc.licType,
					OrganizationName:  fmt.Sprintf("tc-%d", i),
					OrganizationId:    orgID,
					LicenseId:         licenseID,
				}).Encode()
				if err != nil {
					t.Fatal(err)
				}

				_, err = decode(s)
				if err != nil {
					t.Fatal(err)
				}
				orgID[0]++
				licenseID[0]++
			}
		})
	}
}

func TestBadLicenseStrings(t *testing.T) {
	defer leaktest.AfterTest(t)()

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
