// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package builtins

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestFloatWidthBucket(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testCases := []struct {
		operand  float64
		b1       float64
		b2       float64
		count    int
		expected int
	}{
		{0.5, 2, 3, 5, 0},
		{8, 2, 3, 5, 6},
		{1.5, 1, 3, 2, 1},
		{5.35, 0.024, 10.06, 5, 3},
		{-3.0, -5, 5, 10, 3},
		{1, 1, 10, 2, 1},  // minimum should be inclusive
		{10, 1, 10, 2, 3}, // maximum should be exclusive
		{4, 10, 1, 4, 3},
		{11, 10, 1, 4, 0},
		{0, 10, 1, 4, 5},
	}

	for _, tc := range testCases {
		got := widthBucket(tc.operand, tc.b1, tc.b2, tc.count)
		if got != tc.expected {
			t.Errorf("expected %d, found %d", tc.expected, got)
		}
	}
}
