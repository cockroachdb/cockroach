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
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package roachpb

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/pkg/errors"
)

func TestAttributesIsSubset(t *testing.T) {
	a := Attributes{Attrs: []string{"a", "b", "c"}}
	b := Attributes{Attrs: []string{"a", "b"}}
	c := Attributes{Attrs: []string{"a"}}
	if !b.IsSubset(a) {
		t.Errorf("expected %+v to be a subset of %+v", b, a)
	}
	if !c.IsSubset(a) {
		t.Errorf("expected %+v to be a subset of %+v", c, a)
	}
	if !c.IsSubset(b) {
		t.Errorf("expected %+v to be a subset of %+v", c, b)
	}
	if a.IsSubset(b) {
		t.Errorf("%+v should not be a subset of %+v", a, b)
	}
	if a.IsSubset(c) {
		t.Errorf("%+v should not be a subset of %+v", a, c)
	}
	if b.IsSubset(c) {
		t.Errorf("%+v should not be a subset of %+v", b, c)
	}
}

func TestAttributesSortedString(t *testing.T) {
	a := Attributes{Attrs: []string{"a", "b", "c"}}
	if a.SortedString() != "a,b,c" {
		t.Errorf("sorted string of %+v (%s) != \"a,b,c\"", a, a.SortedString())
	}
	b := Attributes{Attrs: []string{"c", "a", "b"}}
	if b.SortedString() != "a,b,c" {
		t.Errorf("sorted string of %+v (%s) != \"a,b,c\"", b, b.SortedString())
	}
	// Duplicates.
	c := Attributes{Attrs: []string{"c", "c", "a", "a", "b", "b"}}
	if c.SortedString() != "a,b,c" {
		t.Errorf("sorted string of %+v (%s) != \"a,b,c\"", c, c.SortedString())
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
	generateLocality := func(values []string) Locality {
		var locality Locality
		for i, value := range values {
			locality.Tiers = append(locality.Tiers, Tier{
				Key:   fmt.Sprintf("%d", i),
				Value: value,
			})
		}
		return locality
	}

	getFraction := func(num, den int) float64 {
		return float64(num) / float64(den)
	}

	testCases := []struct {
		name     string
		left     []string // These will use generateLocality to produce a
		right    []string // Locality.
		expected float64
	}{
		{
			name:     "both empty",
			expected: 0,
		},
		{
			name:     "only one side",
			left:     []string{"a", "b", "c"},
			expected: 0,
		},
		{
			name:     "1 level, 1st different",
			left:     []string{"a"},
			right:    []string{"b"},
			expected: 1,
		},
		{
			name:     "2 levels, 1st different",
			left:     []string{"a", "aa"},
			right:    []string{"b", "bb"},
			expected: 1,
		},
		{
			name:     "3 levels, 1st different",
			left:     []string{"a", "aa", "aaa"},
			right:    []string{"b", "bb", "bbb"},
			expected: 1,
		},
		{
			name:     "3 levels, 1st different, all other values the same",
			left:     []string{"a", "aa", "aaa"},
			right:    []string{"b", "aa", "aaa"},
			expected: 1,
		},
		{
			name:     "1 level, all same",
			left:     []string{"a"},
			right:    []string{"a"},
			expected: 0,
		},
		{
			name:     "2 levels, all same",
			left:     []string{"a", "aa"},
			right:    []string{"a", "aa"},
			expected: 0,
		},
		{
			name:     "3 levels, all same",
			left:     []string{"a", "aa", "aaa"},
			right:    []string{"a", "aa", "aaa"},
			expected: 0,
		},
		{
			name:     "2 levels, 2nd different",
			left:     []string{"a", "aa"},
			right:    []string{"a", "bb"},
			expected: getFraction(1, 2),
		},
		{
			name:     "3 levels, 2nd different",
			left:     []string{"a", "aa", "aaa"},
			right:    []string{"a", "bb", "bbb"},
			expected: getFraction(2, 3),
		},
		{
			name:     "3 levels, 2nd different, rest same",
			left:     []string{"a", "aa", "aaa"},
			right:    []string{"a", "bb", "aaa"},
			expected: getFraction(2, 3),
		},
		{
			name:     "3 levels, 3rd different",
			left:     []string{"a", "aa", "aaa"},
			right:    []string{"a", "aa", "bbb"},
			expected: getFraction(1, 3),
		},
		{
			name:     "4 levels, 1st different, rest same",
			left:     []string{"a", "aa", "aaa", "aaaa"},
			right:    []string{"b", "aa", "aaa", "aaaa"},
			expected: 1,
		},
		{
			name:     "4 levels, 2nd different, rest same",
			left:     []string{"a", "aa", "aaa", "aaaa"},
			right:    []string{"a", "bb", "aaa", "aaaa"},
			expected: getFraction(3, 4),
		},
		{
			name:     "4 levels, 3nd different, rest same",
			left:     []string{"a", "aa", "aaa", "aaaa"},
			right:    []string{"a", "aa", "bbb", "aaaa"},
			expected: getFraction(2, 4),
		},
		{
			name:     "4 levels, 4th different",
			left:     []string{"a", "aa", "aaa", "aaaa"},
			right:    []string{"a", "aa", "aaa", "bbbb"},
			expected: getFraction(1, 4),
		},
		{
			name:     "2 and 4 levels, same",
			left:     []string{"a", "aa", "aaa", "aaaa"},
			right:    []string{"b", "aa"},
			expected: 1,
		},
		{
			name:     "2 and 4 levels, 1st different",
			left:     []string{"a", "aa", "aaa", "aaaa"},
			right:    []string{"b", "aa"},
			expected: 1,
		},
		{
			name:     "2 and 4 levels, 2nd different",
			left:     []string{"a", "aa", "aaa", "aaaa"},
			right:    []string{"a", "bb"},
			expected: getFraction(1, 2),
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
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
