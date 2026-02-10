// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
	// Tests for base-name-only patterns (just filename, no directory).
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

func TestFileSelectionWithPaths(t *testing.T) {
	defer leaktest.AfterTest(t)()

	type s = string
	var zt time.Time

	testCases := []struct {
		name     string
		incl     []s
		excl     []s
		zipPath  s
		included bool
	}{
		{
			name:     "path include with wildcard node",
			incl:     []s{"debug/nodes/*/*.json"},
			zipPath:  "debug/nodes/1/details.json",
			included: true,
		},
		{
			name:     "path include rejects wrong node",
			incl:     []s{"debug/nodes/1/*.json"},
			zipPath:  "debug/nodes/2/details.json",
			included: false,
		},
		// Path exclude targets per-node files but spares the same
		// base name at a different location (cluster-wide).
		{
			name:     "path exclude scoped to per-node only",
			excl:     []s{"debug/nodes/*/ranges.json"},
			zipPath:  "debug/ranges.json",
			included: true,
		},
		// Base-name include + path exclude: the path exclude only
		// removes the targeted file, not other files with the same
		// base-name extension.
		{
			name:     "base include with path exclude",
			incl:     []s{"*.json"},
			excl:     []s{"debug/nodes/*/ranges.json"},
			zipPath:  "debug/nodes/1/ranges.json",
			included: false,
		},
		// Path include distinguishes cluster-wide vs per-node files
		// that share the same base name.
		{
			name:     "path include distinguishes location",
			incl:     []s{"debug/events.json"},
			zipPath:  "debug/nodes/1/events.json",
			included: false,
		},
		// When only a base name is provided (no directory prefix),
		// path patterns still match against the base component.
		{
			name:     "base-only zipPath with path pattern matches base",
			incl:     []s{"debug/nodes/*/cpu.pprof"},
			zipPath:  "cpu.pprof",
			included: false,
		},
		{
			name:     "base-only zipPath with base pattern",
			incl:     []s{"*.pprof"},
			zipPath:  "cpu.pprof",
			included: true,
		},
		// Mixed base-name + path include: path-specific pattern
		// limits which nodes match for json, while base-name pattern
		// includes all txt files.
		{
			name:     "mixed base and path include",
			incl:     []s{"*.txt", "debug/nodes/1/*.json"},
			zipPath:  "debug/nodes/2/details.json",
			included: false,
		},
		// Deep path patterns work for subdirectories like heapprof.
		{
			name:     "deep path include for heapprof",
			incl:     []s{"debug/nodes/*/heapprof/*.pprof"},
			zipPath:  "debug/nodes/1/heapprof/memprof.2026-02-10.pprof",
			included: true,
		},
		// Deep path exclude targets a subdirectory without affecting
		// files elsewhere.
		{
			name:     "deep path exclude spares other files",
			excl:     []s{"debug/nodes/*/heapprof/*.pprof"},
			zipPath:  "debug/nodes/1/heapprof/memmonitoring.2026-02-10.txt",
			included: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			sel := fileSelection{includePatterns: tc.incl, excludePatterns: tc.excl}
			actual := sel.isIncluded(tc.zipPath, zt, zt)
			if actual != tc.included {
				t.Errorf("incl=%+v, excl=%+v, path=%q: expected included=%v, got %v",
					sel.includePatterns, sel.excludePatterns, tc.zipPath, tc.included, actual)
			}
		})
	}
}

func TestRetrievalPatternsWithPaths(t *testing.T) {
	defer leaktest.AfterTest(t)()

	type s = string
	testCases := []struct {
		name     string
		incl     []s
		expected []s
	}{
		{
			name:     "no patterns returns wildcard",
			incl:     nil,
			expected: []s{"*"},
		},
		{
			name:     "base-name patterns returned as-is",
			incl:     []s{"*.json", "*.txt"},
			expected: []s{"*.json", "*.txt"},
		},
		{
			name:     "path patterns filtered out, only base-name sent to server",
			incl:     []s{"*.json", "debug/nodes/1/*.txt"},
			expected: []s{"*.json"},
		},
		{
			name:     "all path patterns returns wildcard",
			incl:     []s{"debug/nodes/1/*.json", "debug/nodes/*/stacks.txt"},
			expected: []s{"*"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			sel := fileSelection{includePatterns: tc.incl}
			actual := sel.retrievalPatterns()
			if len(actual) != len(tc.expected) {
				t.Fatalf("expected %v, got %v", tc.expected, actual)
			}
			for i := range actual {
				if actual[i] != tc.expected[i] {
					t.Errorf("expected %v, got %v", tc.expected, actual)
					break
				}
			}
		})
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
