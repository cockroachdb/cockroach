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
