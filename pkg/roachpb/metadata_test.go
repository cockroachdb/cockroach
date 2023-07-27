// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package roachpb

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/errors"
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
