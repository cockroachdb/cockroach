// Copyright 2019 The Cockroach Authors.
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
	"math"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"gopkg.in/yaml.v2"
)

func TestLocalityMatchScore(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testCases := []struct {
		locality    string
		constraints string
		leasePrefs  string
		expected    float64
	}{
		{locality: "region=us,dc=east", constraints: "[]", expected: 0.0},
		{locality: "region=us,dc=east", constraints: "[+region=eu,+dc=uk,+dc=de]", expected: 0.0},
		{locality: "region=us,dc=east", constraints: "[-region=us,+dc=east]", expected: 0.0},
		{locality: "region=us,dc=east", constraints: "[+region=eu,+dc=east]", expected: 0.0},
		{locality: "region=us,dc=east", constraints: "[-region=eu]", expected: 0.0},
		{locality: "region=us,dc=east", constraints: "[+region=us]", expected: 0.5},
		{locality: "region=us,dc=east", constraints: "[+region=us,+region=eu]", expected: 0.5},
		{locality: "region=us,dc=east", constraints: "[+region=eu,+region=ap,+region=us]", expected: 0.5},
		{locality: "region=us,dc=east", constraints: "[+region=us,-dc=east]", expected: 0.5},
		{locality: "region=us,dc=east", constraints: "[+region=us,+dc=west]", expected: 0.5},
		{locality: "region=us,dc=east", constraints: "[+region=us,+dc=east]", expected: 1.0},
		{locality: "region=us,dc=east", constraints: "[+dc=east]", expected: 1.0},
		{locality: "region=us,dc=east", constraints: "[+dc=west,+dc=east]", expected: 1.0},
		{locality: "region=us,dc=east", constraints: "[-region=eu,+dc=east]", expected: 1.0},
		{locality: "region=us,dc=east", constraints: "[+region=eu,+dc=east,+region=us,+dc=west]", expected: 1.0},
		{locality: "region=us,dc=east", constraints: "[+region=us,+dc=east,+rack=1,-ssd]", expected: 1.0},

		{locality: "region=us,dc=east", constraints: `{"+region=us,+dc=east":3,"-dc=east":2}`, expected: 0.0},
		{locality: "region=us,dc=east", constraints: `{"+region=us,+dc=east":3,"+region=eu,+dc=east":2}`, expected: 0.0},
		{locality: "region=us,dc=east", constraints: `{"+region=us,+dc=east":3,"+region=us,+region=eu":2}`, expected: 0.5},
		{locality: "region=us,dc=east", constraints: `{"+region=us,+dc=east":3,"+dc=east,+dc=west":2}`, expected: 1.0},

		{locality: "region=us,dc=east", leasePrefs: "[[]]", expected: 0.0},
		{locality: "region=us,dc=east", leasePrefs: "[[+dc=west]]", expected: 0.0},
		{locality: "region=us,dc=east", leasePrefs: "[[+region=us]]", expected: 0.17},
		{locality: "region=us,dc=east", leasePrefs: "[[+region=us,+dc=east]]", expected: 0.33},

		{locality: "region=us,dc=east", constraints: "[+region=eu]", leasePrefs: "[[+dc=west]]", expected: 0.0},
		{locality: "region=us,dc=east", constraints: "[+region=eu]", leasePrefs: "[[+region=us]]", expected: 0.17},
		{locality: "region=us,dc=east", constraints: "[+region=eu]", leasePrefs: "[[+dc=east]]", expected: 0.33},
		{locality: "region=us,dc=east", constraints: "[+region=us]", leasePrefs: "[[+dc=west]]", expected: 0.33},
		{locality: "region=us,dc=east", constraints: "[+region=us]", leasePrefs: "[[+region=us]]", expected: 0.50},
		{locality: "region=us,dc=east", constraints: "[+region=us]", leasePrefs: "[[+dc=east]]", expected: 0.67},
		{locality: "region=us,dc=east", constraints: "[+dc=east]", leasePrefs: "[[+region=us]]", expected: 0.83},
		{locality: "region=us,dc=east", constraints: "[+dc=east]", leasePrefs: "[[+dc=east]]", expected: 1.0},
		{locality: "region=us,dc=east", constraints: "[+region=us,+dc=east]", leasePrefs: "[[+region=us,+dc=east]]", expected: 1.0},
	}

	for _, tc := range testCases {
		zone := &zonepb.ZoneConfig{}

		var locality roachpb.Locality
		if err := locality.Set(tc.locality); err != nil {
			t.Fatal(err)
		}

		if tc.constraints != "" {
			constraintsList := &zonepb.ConstraintsList{}
			if err := yaml.UnmarshalStrict([]byte(tc.constraints), constraintsList); err != nil {
				t.Fatal(err)
			}
			zone.Constraints = constraintsList.Constraints
		}

		if tc.leasePrefs != "" {
			if err := yaml.UnmarshalStrict([]byte(tc.leasePrefs), &zone.LeasePreferences); err != nil {
				t.Fatal(err)
			}
		}

		actual := math.Round(localityMatchScore(zone, locality)*100) / 100
		if actual != tc.expected {
			t.Errorf("locality=%v, constraints=%v, leasePrefs=%v: expected %v, got %v",
				tc.locality, tc.constraints, tc.leasePrefs, tc.expected, actual)
		}
	}
}
