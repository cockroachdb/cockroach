// Copyright 2014 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package roachpb

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/pkg/errors"
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
		Replicas: []ReplicaDescriptor{
			{NodeID: 1, StoreID: 1},
			{NodeID: 2, StoreID: 2},
			{NodeID: 3, StoreID: 3},
		},
	}
	for i, e := range desc.Replicas {
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
