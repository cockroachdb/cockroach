// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scanner

import "testing"

func TestHasMultipleStatements(t *testing.T) {
	testCases := []struct {
		in       string
		expected bool
	}{
		{`a b c`, false},
		{`a; b c`, true},
		{`a b; b c`, true},
		{`a b; b c;`, true},
		{`a b;`, false},
		{`SELECT 123; SELECT 123`, true},
		{`SELECT 123; SELECT 123;`, true},
	}

	for _, tc := range testCases {
		actual, err := HasMultipleStatements(tc.in)
		if err != nil {
			t.Error(err)
		}

		if actual != tc.expected {
			t.Errorf("%q: expected %v, got %v", tc.in, tc.expected, actual)
		}
	}
}
