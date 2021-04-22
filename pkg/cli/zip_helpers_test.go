// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cli

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestFileSelection(t *testing.T) {
	defer leaktest.AfterTest(t)()

	type s = string
	testCases := []struct {
		incl    []s
		excl    []s
		accepts []s
		rejects []s
	}{
		{nil, nil, []s{"anything", "really"}, nil},
		{nil, []s{"any*"}, []s{"really", "unrelated"}, []s{"anything"}},
		{nil, []s{"any*", "*lly"}, []s{"unrelated"}, []s{"anything", "really"}},
		{[]s{"any*"}, nil, []s{"anything"}, []s{"really", "unrelated"}},
		{[]s{"any*", "*lly"}, nil, []s{"anything", "really"}, []s{"unrelated"}},
		{[]s{"any*", "*lly"}, []s{"re*"}, []s{"anything", "oreilly"}, []s{"unrelated", "really"}},
	}

	for _, tc := range testCases {
		sel := fileSelection{includePatterns: tc.incl, excludePatterns: tc.excl}
		for _, f := range tc.accepts {
			if !sel.isIncluded(f) {
				t.Errorf("incl=%+v, excl=%+v: mistakenly does not include %q", sel.includePatterns, sel.excludePatterns, f)
			}
		}
		for _, f := range tc.rejects {
			if sel.isIncluded(f) {
				t.Errorf("incl=%+v, excl=%+v: mistakenly includes %q", sel.includePatterns, sel.excludePatterns, f)
			}
		}
	}
}

func TestNodeSelection(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		incl    string
		excl    string
		accepts []roachpb.NodeID
		rejects []roachpb.NodeID
	}{
		{"", "", []roachpb.NodeID{1, 2, 3, 100}, nil},
		{"1-10", "", []roachpb.NodeID{1, 2, 3}, []roachpb.NodeID{100}},
		{"1-2,4", "", []roachpb.NodeID{1, 2, 4}, []roachpb.NodeID{3, 5}},
		{"", "3-10", []roachpb.NodeID{1, 2, 100}, []roachpb.NodeID{4, 5}},
	}

	for _, tc := range testCases {
		var sel nodeSelection
		if tc.incl != "" {
			if err := sel.inclusive.Set(tc.incl); err != nil {
				t.Fatal(err)
			}
		}
		if tc.excl != "" {
			if err := sel.exclusive.Set(tc.excl); err != nil {
				t.Fatal(err)
			}
		}
		for _, n := range tc.accepts {
			if !sel.isIncluded(n) {
				t.Errorf("incl=%s, excl=%s: mistakenly does not include %d", &sel.inclusive, &sel.exclusive, n)
			}
		}
		for _, n := range tc.rejects {
			if sel.isIncluded(n) {
				t.Errorf("incl=%s, excl=%s: mistakenly includes %d", &sel.inclusive, &sel.exclusive, n)
			}
		}
	}
}
