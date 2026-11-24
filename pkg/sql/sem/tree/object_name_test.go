// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tree

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestUnresolvedObjectNameSet(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	name := func(parts ...string) *UnresolvedObjectName {
		var partsArr [3]string
		copy(partsArr[:], parts)
		n, err := NewUnresolvedObjectName(len(parts), partsArr, NoAnnotation)
		if err != nil {
			t.Fatal(err)
		}
		return n
	}

	testCases := []struct {
		add         *UnresolvedObjectName
		expectedLen int
	}{
		{name("foo"), 1},
		{name("bar"), 2},
		{name("foo", "bar"), 3},
		{name("bar", "foo"), 4},
		{name("foo", "bar", "baz"), 5},
		{name("foo", "bar"), 5},
		{name("bar"), 5},
		{name("foo"), 5},
		{name("baz"), 6},
	}

	var s UnresolvedObjectNameSet
	if s.Len() != 0 {
		t.Error("expected set to be empty")
	}

	// Add each test case to the set and check the length.
	for _, tc := range testCases {
		s.Add(tc.add)
		if l := s.Len(); l != tc.expectedLen {
			t.Errorf("after adding %v, expected length of %d, got %d", tc.add, tc.expectedLen, l)
		}
	}

	// Every name should be in the set.
	for _, tc := range testCases {
		inSet := false
		for i := 0; i < s.Len(); i++ {
			if s.Get(i).equals(tc.add) {
				inSet = true
				break
			}
		}
		if !inSet {
			t.Errorf("expected %v to be in the set", tc.add)
		}
	}

	// There should be no names in the set that were never added.
	for i := 0; i < s.Len(); i++ {
		added := false
		for _, tc := range testCases {
			if s.Get(i).equals(tc.add) {
				added = true
				break
			}
		}
		if !added {
			t.Errorf("%v was never added to the set", s.Get(i))
		}
	}
}
