// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package roachpb

import (
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPercentilesFromData(t *testing.T) {
	testCases := []struct {
		data      []float64
		percents  []float64
		expecteds []float64
	}{
		{
			[]float64{},
			[]float64{-10, 0, 10, 25, 50, 75, 90, 100, 110},
			[]float64{0, 0, 0, 0, 0, 0, 0, 0, 0},
		},
		{
			[]float64{5},
			[]float64{-10, 0, 10, 25, 50, 75, 90, 100, 110},
			[]float64{5, 5, 5, 5, 5, 5, 5, 5, 5},
		},
		{
			[]float64{1, 2},
			[]float64{-10, 0, 10, 25, 50, 75, 90, 100, 110},
			[]float64{1, 1, 1, 1, 2, 2, 2, 2, 2},
		},
		{
			[]float64{1, 2, 3},
			[]float64{-10, 0, 10, 25, 50, 75, 90, 100, 110},
			[]float64{1, 1, 1, 1, 2, 3, 3, 3, 3},
		},
		{
			[]float64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
			[]float64{-10, 0, 10, 25, 50, 75, 90, 100, 110},
			[]float64{0, 0, 1, 2, 5, 8, 9, 10, 10},
		},
	}
	for _, tc := range testCases {
		for i := range tc.percents {
			if actual := percentileFromSortedData(tc.data, tc.percents[i]); actual != tc.expecteds[i] {
				t.Errorf("percentile(%v, %f) got %f, want %f", tc.data, tc.percents[i], actual, tc.expecteds[i])
			}
		}
	}
}

func TestRangeDescriptorFindReplica(t *testing.T) {
	desc := RangeDescriptor{
		InternalReplicas: []ReplicaDescriptor{
			{NodeID: 1, StoreID: 1},
			{NodeID: 2, StoreID: 2},
			{NodeID: 3, StoreID: 3},
		},
	}
	for i, e := range desc.InternalReplicas {
		if a, ok := desc.GetReplicaDescriptor(e.StoreID); !ok {
			t.Errorf("%d: expected to find %+v in %+v for store %d", i, e, desc, e.StoreID)
		} else if a != e {
			t.Errorf("%d: expected to find %+v in %+v for store %d; got %+v", i, e, desc, e.StoreID, a)
		}
	}
}

func TestRangeDescriptorMissingReplica(t *testing.T) {
	desc := RangeDescriptor{}
	r, ok := desc.GetReplicaDescriptor(0)
	if ok {
		t.Fatalf("unexpectedly found missing replica: %s", r)
	}
	if (r != ReplicaDescriptor{}) {
		t.Fatalf("unexpectedly got nontrivial return: %s", r)
	}
}

func TestNodeDescriptorAddressForLocality(t *testing.T) {
	addr := func(name string) util.UnresolvedAddr {
		return util.UnresolvedAddr{NetworkField: name}
	}
	desc := NodeDescriptor{
		Address: addr("1"),
		LocalityAddress: []LocalityAddress{
			{Address: addr("2"), LocalityTier: Tier{Key: "region", Value: "east"}},
			{Address: addr("3"), LocalityTier: Tier{Key: "zone", Value: "a"}},
		},
	}
	for _, tc := range []struct {
		locality Locality
		expected util.UnresolvedAddr
	}{
		{
			locality: Locality{},
			expected: addr("1"),
		},
		{
			locality: Locality{Tiers: []Tier{
				{Key: "region", Value: "west"},
				{Key: "zone", Value: "b"},
			}},
			expected: addr("1"),
		},
		{
			locality: Locality{Tiers: []Tier{
				{Key: "region", Value: "east"},
				{Key: "zone", Value: "b"},
			}},
			expected: addr("2"),
		},
		{
			locality: Locality{Tiers: []Tier{
				{Key: "region", Value: "west"},
				{Key: "zone", Value: "a"},
			}},
			expected: addr("3"),
		},
	} {
		t.Run(tc.locality.String(), func(t *testing.T) {
			require.Equal(t, tc.expected, *desc.AddressForLocality(tc.locality))
		})
	}
}

// TestLocalityConversions verifies that setting the value from the CLI short
// hand format works correctly.
func TestLocalityConversions(t *testing.T) {
	testCases := []struct {
		in       string
		expected Locality
		err      string
	}{
		{
			in: "a=b,c=d,e=f",
			expected: Locality{
				Tiers: []Tier{
					{Key: "a", Value: "b"},
					{Key: "c", Value: "d"},
					{Key: "e", Value: "f"},
				},
			},
		},
		{
			in:       "",
			expected: Locality{},
			err:      "empty locality",
		},
		{
			in:       "c=d=e",
			expected: Locality{},
			err:      "tier must be in the form",
		},
		{
			in:       "a",
			expected: Locality{},
			err:      "tier must be in the form",
		},
	}

	for i, tc := range testCases {
		var l Locality
		if err := l.Set(tc.in); tc.err == "" && err != nil {
			t.Error(err)
		} else if tc.err != "" && !strings.Contains(err.Error(), tc.err) {
			t.Error(errors.Wrapf(err, "%d: expected %q", i, tc.err))
		}

		if !reflect.DeepEqual(l, tc.expected) {
			t.Errorf("%d: Locality.Set(%q) = %+v; not %+v", i, tc.in, l, tc.expected)
		}
	}
}

func TestLocalityMatches(t *testing.T) {
	var empty Locality
	var l Locality
	require.NoError(t, l.Set("a=b,c=d,e=f"))
	for _, tc := range []struct {
		filter string
		miss   string
	}{
		{filter: "", miss: ""},
		{filter: "a=b", miss: ""},
		{filter: "a=b,c=d,e=f", miss: ""},
		{filter: "c=d,e=f,a=b", miss: ""},
		{filter: "a=z", miss: "a=z"},
		{filter: "a=b,c=x,e=f", miss: "c=x"},
		{filter: "a=b,x=y", miss: "x=y"},
	} {
		t.Run(fmt.Sprintf("%s-miss-%s", tc.filter, tc.miss), func(t *testing.T) {
			var filter Locality
			if tc.filter != "" {
				require.NoError(t, filter.Set(tc.filter))
			}
			matches, miss := l.Matches(filter)
			if tc.miss == "" {
				require.True(t, matches)
			} else {
				require.False(t, matches)
				require.Equal(t, tc.miss, miss.String())
			}

			emptyMatches, _ := empty.Matches(filter)
			require.Equal(t, tc.filter == "", emptyMatches)
		})
	}
}

func TestLocalityCompareWithLocality(t *testing.T) {
	firstRegionStr := "us-east"
	secRegionStr := "us-west"
	firstZoneStr := "us-east-1"
	secZoneStr := "us-west-1"

	makeLocalityStr := func(region string, zone string) string {
		result := ""
		intermediateStr := ""
		if region != "" {
			result = result + fmt.Sprintf("region=%s", region)
			intermediateStr = ","
		}
		if zone != "" {
			result = result + intermediateStr + fmt.Sprintf("zone=%s", zone)
		}
		if result == "" {
			// empty locality is not allowed.
			result = "invalid-key=invalid"
		}
		fmt.Println(result)
		return result
	}

	for _, tc := range []struct {
		l            string
		other        string
		localityType LocalityComparisonType
		regionValid  bool
		zoneValid    bool
	}{
		// -------- Part 1: check for different zone tier alternatives  --------
		// Valid tier keys, same regions and same zones.
		{l: "region=us-west,zone=us-west-1", other: "region=us-west,zone=us-west-1",
			localityType: LocalityComparisonType_SAME_REGION_SAME_ZONE, regionValid: true, zoneValid: true},
		// Valid tier keys, different regions and different zones.
		{l: "region=us-west,zone=us-west-1", other: "region=us-east,zone=us-west-2",
			localityType: LocalityComparisonType_CROSS_REGION, regionValid: true, zoneValid: true},
		// Valid tier keys, different regions and different zones.
		{l: "region=us-west,availability-zone=us-west-1", other: "region=us-east,availability-zone=us-east-1",
			localityType: LocalityComparisonType_CROSS_REGION, regionValid: true, zoneValid: true},
		// Valid tier keys, same regions and different zones.
		{l: "region=us-west,az=us-west-1", other: "region=us-west,other-keys=us,az=us-east-1",
			localityType: LocalityComparisonType_SAME_REGION_CROSS_ZONE, regionValid: true, zoneValid: true},
		// Invalid zone tier key and different regions.
		{l: "region=us-west,availability-zone=us-west-1", other: "region=us-east,zone=us-east-1",
			localityType: LocalityComparisonType_CROSS_REGION, regionValid: true, zoneValid: false},
		// Valid zone tier key (edge case), different zones and regions.
		{l: "region=us-west,zone=us-west-1", other: "region=us-east,zone=us-west-2,az=us-west-1",
			localityType: LocalityComparisonType_CROSS_REGION, regionValid: true, zoneValid: true},
		// Missing zone tier key and different regions.
		{l: "region=us-west,zone=us-west-1", other: "region=us-east",
			localityType: LocalityComparisonType_CROSS_REGION, regionValid: true, zoneValid: false},
		// Different region and different zones with non-unique & invalid zone tier key.
		{l: "region=us-west,zone=us-west-1,az=us-west-2", other: "az=us-west-1,region=us-west,zone=us-west-1",
			localityType: LocalityComparisonType_SAME_REGION_SAME_ZONE, regionValid: true, zoneValid: false},
		// Different regions and different zones with non-unique & valid zone tier key.
		{l: "region=us-west,az=us-west-2,zone=us-west-1", other: "region=us-west,az=us-west-1",
			localityType: LocalityComparisonType_SAME_REGION_CROSS_ZONE, regionValid: true, zoneValid: true},
		// Invalid region tier key and different zones.
		{l: "country=us,zone=us-west-1", other: "country=us,zone=us-west-2",
			localityType: LocalityComparisonType_SAME_REGION_CROSS_ZONE, regionValid: false, zoneValid: true},
		// Missing region tier key and different zones.
		{l: "az=us-west-1", other: "region=us-east,az=us-west-2",
			localityType: LocalityComparisonType_SAME_REGION_CROSS_ZONE, regionValid: false, zoneValid: true},
		// Invalid region and zone tier key.
		{l: "invalid-key=us-west,zone=us-west-1", other: "region=us-east,invalid-key=us-west-1",
			localityType: LocalityComparisonType_SAME_REGION_SAME_ZONE, regionValid: false, zoneValid: false},
		// Invalid region and zone tier key.
		{l: "country=us,dc=us-west-2", other: "country=us,dc=us-west-2",
			localityType: LocalityComparisonType_SAME_REGION_SAME_ZONE, regionValid: false, zoneValid: false},
		// -------- Part 2: single region, single zone  --------
		// One: (both) Two: (region)
		{l: makeLocalityStr(firstRegionStr, firstZoneStr), other: makeLocalityStr(secRegionStr, ""),
			localityType: LocalityComparisonType_CROSS_REGION, regionValid: true, zoneValid: false},
		// One: (both) Two: (zone)
		{l: makeLocalityStr(firstRegionStr, firstZoneStr), other: makeLocalityStr("", secZoneStr),
			localityType: LocalityComparisonType_SAME_REGION_CROSS_ZONE, regionValid: false, zoneValid: true},
		// One: (region) Two: (region)
		{l: makeLocalityStr(firstRegionStr, ""), other: makeLocalityStr(secRegionStr, ""),
			localityType: LocalityComparisonType_CROSS_REGION, regionValid: true, zoneValid: false},
		// One: (zone) Two: (zone)
		{l: makeLocalityStr("", firstZoneStr), other: makeLocalityStr("", secZoneStr),
			localityType: LocalityComparisonType_SAME_REGION_CROSS_ZONE, regionValid: false, zoneValid: true},
		// One: (region) Two: (zone)
		{l: makeLocalityStr(firstRegionStr, ""), other: makeLocalityStr("", secZoneStr),
			localityType: LocalityComparisonType_SAME_REGION_SAME_ZONE, regionValid: false, zoneValid: false},
		// One: (both) Two: (both)
		{l: makeLocalityStr(firstRegionStr, firstZoneStr), other: makeLocalityStr(secRegionStr, secZoneStr),
			localityType: LocalityComparisonType_CROSS_REGION, regionValid: true, zoneValid: true},
		// One: (none) Two: (none)
		{l: makeLocalityStr("", ""), other: makeLocalityStr("", ""),
			localityType: LocalityComparisonType_SAME_REGION_SAME_ZONE, regionValid: false, zoneValid: false},
		// One: (region) Two: (none)
		{l: makeLocalityStr(firstRegionStr, ""), other: makeLocalityStr("", ""),
			localityType: LocalityComparisonType_SAME_REGION_SAME_ZONE, regionValid: false, zoneValid: false},
		// One: (zone) Two: (none)
		{l: makeLocalityStr("", firstZoneStr), other: makeLocalityStr("", ""),
			localityType: LocalityComparisonType_SAME_REGION_SAME_ZONE, regionValid: false, zoneValid: false},
		// One: (both) Two: (none)
		{l: makeLocalityStr(firstRegionStr, firstZoneStr), other: makeLocalityStr("", ""),
			localityType: LocalityComparisonType_SAME_REGION_SAME_ZONE, regionValid: false, zoneValid: false},
	} {
		t.Run(fmt.Sprintf("%s-crosslocality-%s", tc.l, tc.other), func(t *testing.T) {
			var l Locality
			var other Locality
			require.NoError(t, l.Set(tc.l))
			require.NoError(t, other.Set(tc.other))
			type localities struct {
				localityType LocalityComparisonType
				regionValid  bool
				zoneValid    bool
			}
			localityType, regionValid, zoneValid := l.CompareWithLocality(other)
			actual := localities{localityType, regionValid, zoneValid}
			expected := localities{tc.localityType, tc.regionValid, tc.zoneValid}
			require.Equal(t, expected, actual)
		})
	}
}

func TestLocalitySharedPrefix(t *testing.T) {
	for _, tc := range []struct {
		a        string
		b        string
		expected int
	}{
		// Test basic match and mismatch cases.
		{"a=b", "a=b", 1},
		{"a=b,c=d", "a=b,c=d", 2},
		{"a=b", "a=b,c=d", 1},
		{"", "", 0},

		// Test cases with differing lengths.
		{"a=b", "x=y", 0},
		{"a=b,x=y", "", 0},
		{"a=b,x=y", "a=c", 0},
		{"a=b,x=y", "a=c,x=y", 0},

		// Test cases where the mismatch occurs in different positions.
		{"a=b,c=d,e=f", "a=z,c=d,e=f", 0},
		{"a=b,c=d,e=f", "a=b,c=z,e=f", 1},
		{"a=b,c=d,e=f", "a=b,c=d,e=z", 2},
		{"a=b,c=d,e=f", "a=b,c=d,e=f", 3},
	} {
		t.Run(fmt.Sprintf("%s_=_%s", tc.a, tc.b), func(t *testing.T) {
			var a, b Locality
			if tc.a != "" {
				require.NoError(t, a.Set(tc.a))
			}
			if tc.b != "" {
				require.NoError(t, b.Set(tc.b))
			}
			require.Equal(t, tc.expected, a.SharedPrefix(b))
			require.Equal(t, tc.expected, b.SharedPrefix(a))
		})
	}
}

func TestDiversityScore(t *testing.T) {
	// Keys are not considered for score, just the order, so we don't need to
	// specify them.
	generateLocality := func(values string) Locality {
		var locality Locality
		if len(values) > 0 {
			for i, value := range strings.Split(values, ",") {
				locality.Tiers = append(locality.Tiers, Tier{
					Key:   fmt.Sprintf("%d", i),
					Value: value,
				})
			}
		}
		return locality
	}

	testCases := []struct {
		left     string
		right    string
		expected float64
	}{
		{"", "", 0},
		{"a", "", 1},
		{"a,b", "", 1},
		{"a,b,c", "", 1},
		{"a", "b", 1},
		{"a,aa", "b,bb", 1},
		{"a,aa,aaa", "b,bb,bbb", 1},
		{"a,aa,aaa", "b,aa,aaa", 1},
		{"a", "a", 0},
		{"a,aa", "a,aa", 0},
		{"a,aa,aaa", "a,aa,aaa", 0},
		{"a,aa,aaa", "a,aa", 1.0 / 3.0},
		{"a,aa", "a,bb", 1.0 / 2.0},
		{"a,aa,aaa", "a,bb,bbb", 2.0 / 3.0},
		{"a,aa,aaa", "a,bb,aaa", 2.0 / 3.0},
		{"a,aa,aaa", "a,aa,bbb", 1.0 / 3.0},
		{"a,aa,aaa,aaaa", "b,aa,aaa,aaaa", 1},
		{"a,aa,aaa,aaaa", "a,bb,aaa,aaaa", 3.0 / 4.0},
		{"a,aa,aaa,aaaa", "a,aa,bbb,aaaa", 2.0 / 4.0},
		{"a,aa,aaa,aaaa", "a,aa,aaa,bbbb", 1.0 / 4.0},
		{"a,aa,aaa,aaaa", "a,aa,aaa", 1.0 / 4.0},
		{"a,aa,aaa,aaaa", "b,aa", 1},
		{"a,aa,aaa,aaaa", "a,bb", 1.0 / 2.0},
	}

	for _, testCase := range testCases {
		t.Run(fmt.Sprintf("%s:%s", testCase.left, testCase.right), func(t *testing.T) {
			left := generateLocality(testCase.left)
			right := generateLocality(testCase.right)
			if a := left.DiversityScore(right); a != testCase.expected {
				t.Fatalf("expected %f, got %f", testCase.expected, a)
			}
			if a := right.DiversityScore(left); a != testCase.expected {
				t.Fatalf("expected %f, got %f", testCase.expected, a)
			}
		})
	}
}

func TestAddTier(t *testing.T) {
	l1 := Locality{}
	l2 := Locality{
		Tiers: []Tier{{Key: "foo", Value: "bar"}},
	}
	l3 := Locality{
		Tiers: []Tier{{Key: "foo", Value: "bar"}, {Key: "bar", Value: "foo"}},
	}
	require.Equal(t, l2, l1.AddTier(Tier{Key: "foo", Value: "bar"}))
	require.Equal(t, l3, l2.AddTier(Tier{Key: "bar", Value: "foo"}))
}

func TestGCHint(t *testing.T) {
	var empty hlc.Timestamp
	ts1, ts2, ts3 := makeTS(1234, 2), makeTS(2345, 0), makeTS(3456, 10)
	hint := func(rangeT, minT, maxT hlc.Timestamp) GCHint {
		return GCHint{
			LatestRangeDeleteTimestamp: rangeT,
			GCTimestamp:                minT,
			GCTimestampNext:            maxT,
		}
	}

	// checkInvariants verifies that the GCHint is well-formed.
	checkInvariants := func(t *testing.T, hint GCHint) {
		t.Helper()
		if hint.GCTimestamp.IsEmpty() {
			require.True(t, hint.GCTimestampNext.IsEmpty())
		}
		if hint.GCTimestampNext.IsSet() {
			require.True(t, hint.GCTimestamp.Less(hint.GCTimestampNext))
		}
	}
	checkInvariants(t, GCHint{})

	// merge runs GCHint.Merge with the given parameters, and verifies that the
	// semantics of the returned "updated" bool are satisfied.
	merge := func(t *testing.T, lhs, rhs GCHint, leftEmtpy, rightEmpty bool) GCHint {
		t.Helper()
		before := lhs
		updated := lhs.Merge(&rhs, leftEmtpy, rightEmpty)
		require.Equal(t, !lhs.Equal(before), updated, "Merge return value incorrect")
		return lhs
	}
	// checkMerge runs GCHint.Merge, and verifies that:
	// 	- The result of merging 2 hints does not depend on the order.
	// 	- Merge returns true iff it modified the hint.
	// 	- Merge returns GCHint conforming with the invariants.
	checkMerge := func(t *testing.T, lhs, rhs GCHint, leftEmpty, rightEmpty bool) GCHint {
		t.Helper()
		res1 := merge(t, lhs, rhs, leftEmpty, rightEmpty)
		res2 := merge(t, rhs, lhs, rightEmpty, leftEmpty)
		require.Equal(t, res1, res2, "Merge is not commutative")
		checkInvariants(t, res1)
		return res1
	}

	// Test the effect of GCHint.Merge with an empty hint. Additionally, test that
	// the result of such a Merge does not depend on the order, i.e. Merge is
	// commutative; and that Merge returns a correct "updated" bool.
	for _, h := range []GCHint{
		hint(empty, empty, empty),
		hint(empty, ts1, empty),
		hint(empty, ts1, ts3),
		hint(ts1, empty, empty),
		hint(ts1, ts1, ts3),
		hint(ts2, ts1, ts3),
	} {
		t.Run("Merge-with-empty-hint", func(t *testing.T) {
			for _, lr := range [][]bool{
				{false, false},
				{false, true},
				{true, false},
				{true, true},
			} {
				t.Run(fmt.Sprintf("leftEmpty=%v/rightEmpty=%v", lr[0], lr[1]), func(t *testing.T) {
					leftEmpty, rightEmpty := lr[0], lr[1]
					checkInvariants(t, h)
					checkMerge(t, h, GCHint{}, leftEmpty, rightEmpty)
				})
			}
		})
	}

	ts4 := makeTS(4567, 5)
	for _, tc := range []struct {
		lhs    GCHint
		rhs    GCHint
		lEmpty bool
		rEmpty bool
		want   GCHint
	}{
		{lhs: hint(empty, ts1, ts3), rhs: hint(empty, ts1, empty), want: hint(empty, ts1, ts3)},
		{lhs: hint(empty, ts1, ts3), rhs: hint(empty, ts1, ts2), want: hint(empty, ts1, ts3)},
		{lhs: hint(empty, ts1, ts3), rhs: hint(empty, ts1, ts3), want: hint(empty, ts1, ts3)},
		{lhs: hint(empty, ts1, ts3), rhs: hint(empty, ts1, ts4), want: hint(empty, ts1, ts4)},
		{lhs: hint(empty, ts1, ts3), rhs: hint(empty, ts2, empty), want: hint(empty, ts1, ts3)},
		{lhs: hint(empty, ts1, ts3), rhs: hint(empty, ts2, ts3), want: hint(empty, ts1, ts3)},
		{lhs: hint(empty, ts1, ts3), rhs: hint(empty, ts2, ts4), want: hint(empty, ts1, ts4)},
		{lhs: hint(empty, ts1, ts3), rhs: hint(empty, ts3, empty), want: hint(empty, ts1, ts3)},
		{lhs: hint(empty, ts1, ts3), rhs: hint(empty, ts3, ts4), want: hint(empty, ts1, ts4)},

		{lhs: hint(ts1, empty, empty), rhs: hint(ts2, empty, empty), lEmpty: false, rEmpty: false,
			want: hint(ts2, empty, empty)},
		{lhs: hint(ts1, empty, empty), rhs: hint(ts2, empty, empty), lEmpty: true, rEmpty: false,
			want: hint(ts2, empty, empty)},
		{lhs: hint(ts1, empty, empty), rhs: hint(ts2, empty, empty), lEmpty: false, rEmpty: true,
			want: hint(ts2, empty, empty)},
		{lhs: hint(ts1, empty, empty), rhs: hint(ts2, empty, empty), lEmpty: true, rEmpty: true,
			want: hint(ts2, empty, empty)},

		{lhs: hint(ts2, empty, empty), rhs: hint(empty, ts1, ts3), lEmpty: false, rEmpty: false,
			want: hint(empty, ts1, ts3)},
		{lhs: hint(ts2, empty, empty), rhs: hint(empty, ts1, ts3), lEmpty: true, rEmpty: false,
			want: hint(empty, ts1, ts3)},
		{lhs: hint(ts2, empty, empty), rhs: hint(empty, ts1, ts3), lEmpty: false, rEmpty: true,
			want: hint(ts2, ts1, ts3)},
		{lhs: hint(ts2, empty, empty), rhs: hint(empty, ts1, ts3), lEmpty: true, rEmpty: true,
			want: hint(ts2, ts1, ts3)},
	} {
		t.Run("Merge", func(t *testing.T) {
			checkInvariants(t, tc.lhs)
			checkInvariants(t, tc.rhs)
			got := checkMerge(t, tc.lhs, tc.rhs, tc.lEmpty, tc.rEmpty)
			require.Equal(t, tc.want, got)
		})
	}

	for _, tc := range []struct {
		was  GCHint
		add  hlc.Timestamp
		want GCHint
	}{
		// Adding an empty timestamp is a no-op.
		{was: GCHint{}, add: empty, want: GCHint{}},
		{was: hint(empty, ts1, empty), add: empty, want: hint(empty, ts1, empty)},
		{was: hint(empty, ts1, ts2), add: empty, want: hint(empty, ts1, ts2)},
		{was: hint(ts1, ts1, ts2), add: empty, want: hint(ts1, ts1, ts2)},
		// Any timestamp is added to a hint with empty timestamps.
		{was: GCHint{}, add: ts1, want: hint(empty, ts1, empty)},
		{was: hint(ts1, empty, empty), add: ts1, want: hint(ts1, ts1, empty)},
		{was: hint(ts2, empty, empty), add: ts1, want: hint(ts2, ts1, empty)},
		// For a hint with only one timestamp, test all possible relative positions
		// of the newly added timestamp.
		{was: hint(empty, ts2, empty), add: ts1, want: hint(empty, ts1, ts2)},
		{was: hint(empty, ts2, empty), add: ts2, want: hint(empty, ts2, empty)}, // no-op
		{was: hint(empty, ts1, empty), add: ts2, want: hint(empty, ts1, ts2)},
		{was: hint(ts1, ts1, empty), add: ts2, want: hint(ts1, ts1, ts2)},
		// For a hint with both timestamps, test all possible relative positions of
		// the newly added timestamp.
		{was: hint(empty, ts2, ts3), add: ts1, want: hint(empty, ts1, ts3)},
		{was: hint(empty, ts2, ts3), add: ts2, want: hint(empty, ts2, ts3)}, // no-op
		{was: hint(empty, ts1, ts3), add: ts2, want: hint(empty, ts1, ts3)}, // no-op
		{was: hint(empty, ts1, ts3), add: ts3, want: hint(empty, ts1, ts3)}, // no-op
		{was: hint(empty, ts1, ts2), add: ts2, want: hint(empty, ts1, ts2)}, // no-op
		{was: hint(empty, ts1, ts2), add: ts3, want: hint(empty, ts1, ts3)},
		{was: hint(ts1, ts1, ts2), add: ts3, want: hint(ts1, ts1, ts3)},
		{was: hint(ts2, ts1, ts2), add: ts3, want: hint(ts2, ts1, ts3)},
		{was: hint(ts3, ts1, ts2), add: ts3, want: hint(ts3, ts1, ts3)},
	} {
		t.Run("ScheduleGCFor", func(t *testing.T) {
			hint := tc.was
			checkInvariants(t, hint)
			updated := hint.ScheduleGCFor(tc.add)
			checkInvariants(t, hint)
			assert.Equal(t, !hint.Equal(tc.was), updated, "returned incorrect 'updated' bit")
			assert.Equal(t, tc.want, hint)
		})
	}

	for _, tc := range []struct {
		was  GCHint
		gced hlc.Timestamp
		want GCHint
	}{
		// Check empty timestamp cases.
		{was: GCHint{}, gced: empty, want: GCHint{}},
		{was: GCHint{}, gced: ts1, want: GCHint{}},
		{was: hint(empty, ts1, empty), gced: empty, want: hint(empty, ts1, empty)},
		{was: hint(ts2, ts1, ts3), gced: empty, want: hint(ts2, ts1, ts3)},
		{was: hint(ts2, ts1, empty), gced: empty, want: hint(ts2, ts1, empty)},
		// Check that the GC hint is updated correctly with all relative positions
		// of the threshold.
		{was: hint(empty, ts2, ts3), gced: ts1, want: hint(empty, ts2, ts3)}, // no-op
		{was: hint(empty, ts2, ts3), gced: ts2, want: hint(empty, ts3, empty)},
		{was: hint(empty, ts1, ts3), gced: ts2, want: hint(empty, ts3, empty)},
		{was: hint(empty, ts1, ts3), gced: ts3, want: GCHint{}},
		{was: hint(empty, ts1, ts2), gced: ts3, want: GCHint{}},
		// Check that the entire-range hint is updated correctly with all relative
		// positions of the threshold.
		{was: hint(ts2, empty, empty), gced: ts1, want: hint(ts2, empty, empty)},
		{was: hint(ts2, empty, empty), gced: ts2, want: GCHint{}},
		{was: hint(ts2, empty, empty), gced: ts3, want: GCHint{}},
	} {
		t.Run("UpdateAfterGC", func(t *testing.T) {
			hint := tc.was
			checkInvariants(t, hint)
			updated := hint.UpdateAfterGC(tc.gced)
			checkInvariants(t, hint)
			assert.Equal(t, !hint.Equal(tc.was), updated, "returned incorrect 'updated' bit")
			assert.Equal(t, tc.want, hint)
		})
	}

	for _, tc := range []struct {
		was  GCHint
		add  hlc.Timestamp
		want GCHint
	}{
		{was: GCHint{}, add: empty, want: GCHint{}},
		{was: hint(ts2, empty, empty), add: empty, want: hint(ts2, empty, empty)},
		{was: hint(ts2, empty, empty), add: ts1, want: hint(ts2, empty, empty)},
		{was: hint(ts2, empty, empty), add: ts2, want: hint(ts2, empty, empty)},
		{was: hint(ts2, empty, empty), add: ts3, want: hint(ts3, empty, empty)},
	} {
		t.Run("ForwardLatestRangeDeleteTimestamp", func(t *testing.T) {
			hint := tc.was
			checkInvariants(t, hint)
			updated := hint.ForwardLatestRangeDeleteTimestamp(tc.add)
			checkInvariants(t, hint)
			assert.Equal(t, !hint.Equal(tc.was), updated, "returned incorrect 'updated' bit")
			assert.Equal(t, tc.want, hint)
		})
	}
}

func TestRangeDescriptorsByStartKey(t *testing.T) {
	// table-prefix-range-key
	tprk := func(t byte) RKey {
		return RKey(Key([]byte{t}))
	}
	ranges := []RangeDescriptor{
		{StartKey: tprk(2), EndKey: tprk(7)},
		{StartKey: tprk(5), EndKey: tprk(5)},
		{StartKey: tprk(7), EndKey: tprk(2)},
		{StartKey: tprk(1), EndKey: tprk(10)},
		{StartKey: tprk(5), EndKey: tprk(5)},
	}
	sort.Stable(RangeDescriptorsByStartKey(ranges))

	for i := 0; i < len(ranges)-1; i++ {
		if ranges[i+1].StartKey.AsRawKey().Less(ranges[i].StartKey.AsRawKey()) {
			t.Fatalf("expected ranges to be ordered increasing by start key, failed on %d, %d with keys %s, %s", i, i+1, ranges[i].StartKey.AsRawKey(), ranges[i+1].StartKey.AsRawKey())
		}
	}
}
