// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package util

import "testing"

func TestGetSingleRune(t *testing.T) {
	tests := []struct {
		s        string
		expected rune
		err      bool
	}{
		{"a", 'a', false},
		{"", 0, false},
		{"🐛"[:1], 0, true},
		{"aa", 'a', true},
	}
	for _, tc := range tests {
		t.Run(tc.s, func(t *testing.T) {
			got, err := GetSingleRune(tc.s)
			if (err != nil) != tc.err {
				t.Fatalf("got unexpected err: %v", err)
			}
			if tc.expected != got {
				t.Fatalf("expected %v, got %v", tc.expected, got)
			}
		})
	}
}

func TestTruncateString(t *testing.T) {
	testCases := []struct {
		s string
		// res stores the expected results for maxRunes=0,1,2,3,etc.
		res []string
	}{
		{"", []string{"", ""}},
		{"abcd", []string{"", "a", "ab", "abc", "abcd", "abcd", "abcd"}},
		{"🐛🏠", []string{"", "🐛", "🐛🏠", "🐛🏠", "🐛🏠"}},
		{"a🐛b🏠c", []string{"", "a", "a🐛", "a🐛b", "a🐛b🏠", "a🐛b🏠c", "a🐛b🏠c"}},
		{
			// Test with an invalid UTF-8 sequence.
			"\xf0\x90\x28\xbc",
			[]string{"", "\xf0", "\xf0\x90", "\xf0\x90\x28", "\xf0\x90\x28\xbc", "\xf0\x90\x28\xbc"},
		},
	}

	for _, tc := range testCases {
		for i := range tc.res {
			if r := TruncateString(tc.s, i); r != tc.res[i] {
				t.Errorf("TruncateString(\"%q\", %d) = \"%q\"; expected \"%q\"", tc.s, i, r, tc.res[i])
			}
		}
	}
}
