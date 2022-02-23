// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package xform

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/partition"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"gopkg.in/yaml.v2"
)

func TestIsZoneLocal(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testCases := []struct {
		localRegion      string
		constraints      string
		voterConstraints string
		leasePrefs       string
		expected         bool
	}{
		{localRegion: "us", constraints: "[]", expected: false},
		{localRegion: "us", constraints: "[+region=eu,+dc=uk]", expected: false},
		{localRegion: "us", constraints: "[-region=us,+dc=east]", expected: false},
		{localRegion: "us", constraints: "[-region=eu]", expected: false},
		{localRegion: "us", constraints: "[+region=us]", expected: true},

		{localRegion: "us", voterConstraints: "[+region=us,-dc=east]", expected: true},
		{localRegion: "us", voterConstraints: "[+region=us,+dc=west]", expected: true},
		{localRegion: "us", voterConstraints: "[+dc=east]", expected: false},
		{localRegion: "us", voterConstraints: "[+dc=west,+ssd]", expected: false},
		{localRegion: "us", voterConstraints: "[-region=eu,+dc=east]", expected: false},
		{localRegion: "us", voterConstraints: "[+region=us,+dc=east,+rack=1,-ssd]", expected: true},

		{localRegion: "us", constraints: `{"+region=us,+dc=east":3,"-dc=east":2}`, expected: true},
		{localRegion: "us", constraints: `{"+region=us,+dc=east":3,"+region=us,+dc=west":2}`, expected: true},
		{localRegion: "us", constraints: `{"+region=us,+dc=east":3,"+region=eu":2}`, expected: false},

		{localRegion: "us", leasePrefs: "[[]]", expected: false},
		{localRegion: "us", leasePrefs: "[[+dc=west]]", expected: false},
		{localRegion: "us", leasePrefs: "[[+region=us]]", expected: true},
		{localRegion: "us", leasePrefs: "[[+region=us,+dc=east]]", expected: true},

		{localRegion: "us", constraints: "[+region=eu]", voterConstraints: "[+region=eu]",
			leasePrefs: "[[+dc=west]]", expected: false},
		{localRegion: "us", constraints: "[+region=eu]", voterConstraints: "[+region=eu]",
			leasePrefs: "[[+region=us]]", expected: false},
		{localRegion: "us", constraints: "[+region=us]", voterConstraints: "[+region=us]",
			leasePrefs: "[[+dc=west]]", expected: true},
		{localRegion: "us", constraints: "[+region=us]", voterConstraints: "[+region=us]",
			leasePrefs: "[[+region=us]]", expected: true},
		{localRegion: "us", constraints: "[+dc=east]", voterConstraints: "[+region=us]",
			leasePrefs: "[[+region=us]]", expected: true},
		{localRegion: "us", constraints: "[+dc=east]", voterConstraints: "[+dc=east]",
			leasePrefs: "[[+region=us]]", expected: true},
		{localRegion: "us", constraints: "[+dc=east]", voterConstraints: "[+dc=east]",
			leasePrefs: "[[+dc=east]]", expected: false},
		{localRegion: "us", constraints: "[+region=us,+dc=east]", voterConstraints: "[-region=eu]",
			leasePrefs: "[[+region=us,+dc=east]]", expected: true},
		{localRegion: "us", constraints: `{"+region=us":3,"+region=eu":2}`,
			voterConstraints: `[+region=us]`, expected: true},
		{localRegion: "us", constraints: `{"+region=us":3,"+region=eu":2}`,
			voterConstraints: `{"+region=us":1,"+region=eu":1}`, expected: false},
		{localRegion: "us", constraints: `{"+region=us":3,"+region=eu":2}`,
			voterConstraints: `{"+region=us":1,"+region=eu":1}`, leasePrefs: "[[+region=us]]", expected: true},
	}

	for _, tc := range testCases {
		zone := &zonepb.ZoneConfig{}

		if tc.constraints != "" {
			constraintsList := &zonepb.ConstraintsList{}
			if err := yaml.UnmarshalStrict([]byte(tc.constraints), constraintsList); err != nil {
				t.Fatal(err)
			}
			zone.Constraints = constraintsList.Constraints
		}

		if tc.voterConstraints != "" {
			constraintsList := &zonepb.ConstraintsList{}
			if err := yaml.UnmarshalStrict([]byte(tc.voterConstraints), constraintsList); err != nil {
				t.Fatal(err)
			}
			zone.VoterConstraints = constraintsList.Constraints
		}

		if tc.leasePrefs != "" {
			if err := yaml.UnmarshalStrict([]byte(tc.leasePrefs), &zone.LeasePreferences); err != nil {
				t.Fatal(err)
			}
		}

		actual := partition.IsZoneLocal(zone, tc.localRegion)
		if actual != tc.expected {
			t.Errorf("locality=%v, constraints=%v, voterConstraints=%v, leasePrefs=%v: expected %v, got %v",
				tc.localRegion, tc.constraints, tc.voterConstraints, tc.leasePrefs, tc.expected, actual)
		}
	}
}
