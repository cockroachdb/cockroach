// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package physical

import (
	"fmt"
	"reflect"
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"gopkg.in/yaml.v2"
)

func TestUnion(t *testing.T) {
	testCases := []struct {
		leftDist  []string
		rightDist []string
		expected  []string
	}{
		{
			leftDist:  []string{},
			rightDist: []string{},
			expected:  []string{},
		},
		{
			leftDist:  []string{},
			rightDist: []string{"west"},
			expected:  []string{"west"},
		},
		{
			leftDist:  []string{"east"},
			rightDist: []string{"east"},
			expected:  []string{"east"},
		},
		{
			leftDist:  []string{"west"},
			rightDist: []string{"east"},
			expected:  []string{"east", "west"},
		},
		{
			leftDist:  []string{"central", "east", "west"},
			rightDist: []string{"central", "west"},
			expected:  []string{"central", "east", "west"},
		},
	}
	for tcIdx, tc := range testCases {
		t.Run(fmt.Sprintf("case%d", tcIdx+1), func(t *testing.T) {
			leftDist := Distribution{Regions: tc.leftDist}
			rightDist := Distribution{Regions: tc.rightDist}
			expected := Distribution{Regions: tc.expected}

			res := leftDist.Union(rightDist)
			if !res.Equals(expected) {
				t.Errorf("expected '%s', got '%s'", expected, res)
			}
		})
	}
}

func TestFromLocality(t *testing.T) {
	testCases := []struct {
		locality roachpb.Locality
		expected []string
	}{
		{
			locality: roachpb.Locality{},
			expected: []string{},
		},
		{
			locality: roachpb.Locality{Tiers: []roachpb.Tier{
				{Key: "region", Value: "west"},
				{Key: "zone", Value: "b"},
			}},
			expected: []string{"west"},
		},
		{
			locality: roachpb.Locality{Tiers: []roachpb.Tier{
				{Key: "zone", Value: "b"},
			}},
			expected: []string{},
		},
	}
	for tcIdx, tc := range testCases {
		t.Run(fmt.Sprintf("case%d", tcIdx+1), func(t *testing.T) {
			expected := Distribution{Regions: tc.expected}

			var res Distribution
			res.FromLocality(tc.locality)
			if !res.Equals(expected) {
				t.Errorf("expected '%s', got '%s'", expected, res)
			}
		})
	}
}

func TestGetRegionsFromZone(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testCases := []struct {
		constraints      string
		voterConstraints string
		leasePrefs       string
		expected         []string
	}{
		{constraints: "[]", expected: []string{}},
		{constraints: "[+region=eu,+dc=uk]", expected: []string{"eu"}},
		{constraints: "[-region=us,+dc=east]", expected: []string{}},
		{constraints: "[-region=eu]", expected: []string{}},
		{constraints: "[+region=us]", expected: []string{"us"}},
		{constraints: "[+region=us,-region=eu]", expected: []string{"us"}},

		{voterConstraints: "[+region=us,-dc=east]", expected: []string{"us"}},
		{voterConstraints: "[+region=us,+dc=west]", expected: []string{"us"}},
		{voterConstraints: "[+dc=east]", expected: []string{}},
		{voterConstraints: "[+dc=west,+ssd]", expected: []string{}},
		{voterConstraints: "[-region=eu,+dc=east]", expected: []string{}},
		{voterConstraints: "[+region=us,+dc=east,+rack=1,-ssd]", expected: []string{"us"}},

		{constraints: `{"+region=us,+dc=east":3,"-dc=east":2}`, expected: []string{"us"}},
		{constraints: `{"+region=us,+dc=east":3,"+region=us,+dc=west":2}`, expected: []string{"us"}},
		{constraints: `{"+region=us,+dc=east":3,"+region=eu":2}`, expected: []string{"eu", "us"}},

		{leasePrefs: "[[]]", expected: []string{}},
		{leasePrefs: "[[+dc=west]]", expected: []string{}},
		{leasePrefs: "[[+region=us]]", expected: []string{"us"}},
		{leasePrefs: "[[+region=us,+dc=east]]", expected: []string{"us"}},

		{constraints: "[+region=eu]", voterConstraints: "[+region=eu]",
			leasePrefs: "[[+dc=west]]", expected: []string{"eu"}},
		{constraints: "[+region=eu]", voterConstraints: "[+region=eu]",
			leasePrefs: "[[+region=us]]", expected: []string{"eu"}},
		{constraints: "[+region=us]", voterConstraints: "[+region=us]",
			leasePrefs: "[[+dc=west]]", expected: []string{"us"}},
		{constraints: "[+region=us]", voterConstraints: "[+region=us]",
			leasePrefs: "[[+region=us]]", expected: []string{"us"}},
		{constraints: "[+dc=east]", voterConstraints: "[+region=us]",
			leasePrefs: "[[+region=us]]", expected: []string{"us"}},
		{constraints: "[+dc=east]", voterConstraints: "[+dc=east]",
			leasePrefs: "[[+region=us]]", expected: []string{"us"}},
		{constraints: "[+dc=east]", voterConstraints: "[+dc=east]",
			leasePrefs: "[[+dc=east]]", expected: []string{}},
		{constraints: "[+region=us,+dc=east]", voterConstraints: "[-region=eu]",
			leasePrefs: "[[+region=us,+dc=east]]", expected: []string{"us"}},
		{constraints: `{"+region=us":3,"+region=eu":2}`,
			voterConstraints: `[+region=us]`, expected: []string{"us"}},
		{constraints: `{"+region=us":3,"+region=eu":2}`,
			voterConstraints: `{"+region=us":1,"+region=eu":1}`, expected: []string{"eu", "us"}},
		{constraints: `{"+region=us":3,"+region=eu":2}`,
			voterConstraints: `{"+region=us":1,"+region=eu":1}`, leasePrefs: "[[+region=us]]", expected: []string{"us"}},
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

		regions := getRegionsFromZone(cat.AsZone(zone))
		actual := make([]string, 0, len(regions))
		for r := range regions {
			actual = append(actual, r)
		}
		sort.Strings(actual)
		if !reflect.DeepEqual(actual, tc.expected) {
			t.Errorf("constraints=%v, voterConstraints=%v, leasePrefs=%v: expected %v, got %v",
				tc.constraints, tc.voterConstraints, tc.leasePrefs, tc.expected, actual)
		}
	}
}
