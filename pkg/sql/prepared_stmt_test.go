// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestPlanCosts(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	type testCase struct {
		input       []memo.Cost
		expectedNum int
		expectedAvg memo.Cost
	}
	testCases := []testCase{
		{input: []memo.Cost{}, expectedNum: 0, expectedAvg: 0},
		{input: []memo.Cost{0, 0}, expectedNum: 2, expectedAvg: 0},
		{input: []memo.Cost{1, 1}, expectedNum: 2, expectedAvg: 1},
		{input: []memo.Cost{1, 2, 3, 4, 5}, expectedNum: 5, expectedAvg: 3},
		{input: []memo.Cost{1, 2, 3, 4, 5, 6}, expectedNum: 5, expectedAvg: 4},
		{input: []memo.Cost{9, 9, 9, 9, 9, 1, 2, 3, 4, 5}, expectedNum: 5, expectedAvg: 3},
	}
	var pc planCosts
	for _, tc := range testCases {
		pc.ClearCustom()
		for _, cost := range tc.input {
			pc.AddCustom(cost)
		}
		if pc.NumCustom() != tc.expectedNum {
			t.Errorf("expected Len() to be %d, got %d", tc.expectedNum, pc.NumCustom())
		}
		if pc.AvgCustom() != tc.expectedAvg {
			t.Errorf("expected Avg() to be %f, got %f", tc.expectedAvg, pc.AvgCustom())
		}
	}
}
