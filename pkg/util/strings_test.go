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

import (
	"testing"

	"github.com/stretchr/testify/require"
)

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

func TestToLowerSingleByte(t *testing.T) {
	testCases := []struct {
		from     byte
		expected byte
	}{
		{'a', 'a'},
		{'A', 'a'},
		{'c', 'c'},
		{'C', 'c'},
		{'Z', 'z'},
		{'1', '1'},
		{'\n', '\n'},
	}

	for _, tc := range testCases {
		t.Run(string(tc.from), func(t *testing.T) {
			ret := ToLowerSingleByte(tc.from)
			require.Equal(t, tc.expected, ret)
		})
	}
}
