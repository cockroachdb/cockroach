// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

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
