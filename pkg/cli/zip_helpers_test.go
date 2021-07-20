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
	"time"

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

	var zt time.Time
	for _, tc := range testCases {
		sel := fileSelection{includePatterns: tc.incl, excludePatterns: tc.excl}
		for _, f := range tc.accepts {
			if !sel.isIncluded(f, zt, zt) {
				t.Errorf("incl=%+v, excl=%+v: mistakenly does not include %q", sel.includePatterns, sel.excludePatterns, f)
			}
		}
		for _, f := range tc.rejects {
			if sel.isIncluded(f, zt, zt) {
				t.Errorf("incl=%+v, excl=%+v: mistakenly includes %q", sel.includePatterns, sel.excludePatterns, f)
			}
		}
	}
}

func TestFileTimeBounds(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const tfmt = "2006-01-02 15:04"
	var fs fileSelection
	const startTs = "2021-04-21 10:00"
	const endTs = "2021-04-23 10:00"
	_ = fs.startTimestamp.Set(startTs)
	_ = fs.endTimestamp.Set(endTs)

	testCases := []struct {
		ctime    string
		mtime    string
		included bool
	}{
		// Completely before.
		{"2021-03-01 01:00", "2021-04-01 12:00", false},
		// Completely after.
		{"2021-04-25 01:00", "2021-04-30 12:00", false},
		// mtime exactly at the start of the range.
		{"2021-04-20 01:00", startTs, true},
		// ctime exactly at the end of the range.
		{endTs, "2021-04-25 15:00", true},
		// Straddles at the beginning.
		{"2021-04-20 01:00", "2021-04-21 15:00", true},
		// Straddles at the end.
		{"2021-04-21 14:00", "2021-04-25 15:00", true},
		// Straddles over the entire range.
		{"2021-04-20 01:00", "2021-04-25 15:00", true},
		// Straddles over the entire range, ctime at start timestamp.
		{startTs, "2021-04-21 11:00", true},
		// Straddles over the entire range, mtime at end timestamp.
		{"2021-04-20 10:00", endTs, true},
		// Coincides exactly.
		{startTs, endTs, true},
	}

	for i, tc := range testCases {
		ct, _ := time.ParseInLocation(tfmt, tc.ctime, time.UTC)
		mt, _ := time.ParseInLocation(tfmt, tc.mtime, time.UTC)
		if expected, actual := tc.included, fs.isIncluded("unused", ct, mt); expected != actual {
			t.Errorf("%d: file(ctime=%s,mtime=%s) expected included %v, got %v", i, ct, mt, expected, actual)
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

func TestTimestampFlag(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		in  string
		exp string
	}{
		{"", `parsing time "" as "2006-01-02": cannot parse "" as "2006"`},
		{"2011-01-02", "2011-01-02 00:00:00"},
		{"  2011-01-02", "2011-01-02 00:00:00"},
		{"2011-01-02  ", "2011-01-02 00:00:00"},
		{"2011-01-02 03", `parsing time "2011-01-02 03" as "2006-01-02 15:04": cannot parse "" as ":"`},
		{"2011-01-02 03:04", "2011-01-02 03:04:00"},
		{"  2011-01-02 03:04", "2011-01-02 03:04:00"},
		{"2011-01-02 03:04 ", "2011-01-02 03:04:00"},
		{"2011-01-02 03:04:06", "2011-01-02 03:04:06"},
		{"  2011-01-02 03:04:06", "2011-01-02 03:04:06"},
		{"2011-01-02 03:04:06 ", "2011-01-02 03:04:06"},
	}

	for _, tc := range testCases {
		var tt timestampValue
		err := tt.Set(tc.in)
		actual := tt.String()
		if err != nil {
			actual = err.Error()
		}
		if actual != tc.exp {
			t.Errorf("%s: expected %q, got %q", tc.in, tc.exp, actual)
		}
	}
}
