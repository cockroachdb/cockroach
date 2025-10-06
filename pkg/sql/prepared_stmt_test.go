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
		input       []float64
		expectedNum int
		expectedAvg float64
	}
	testCases := []testCase{
		{input: []float64{}, expectedNum: 0, expectedAvg: 0},
		{input: []float64{0, 0}, expectedNum: 2, expectedAvg: 0},
		{input: []float64{1, 1}, expectedNum: 2, expectedAvg: 1},
		{input: []float64{1, 2, 3, 4, 5}, expectedNum: 5, expectedAvg: 3},
		{input: []float64{1, 2, 3, 4, 5, 6}, expectedNum: 5, expectedAvg: 4},
		{input: []float64{9, 9, 9, 9, 9, 1, 2, 3, 4, 5}, expectedNum: 5, expectedAvg: 3},
	}
	var pc planCosts
	for _, tc := range testCases {
		pc.ClearCustom()
		for _, cost := range tc.input {
			pc.AddCustom(memo.Cost{C: cost})
		}
		if pc.NumCustom() != tc.expectedNum {
			t.Errorf("expected Len() to be %d, got %d", tc.expectedNum, pc.NumCustom())
		}
		expectedAvgCost := memo.Cost{C: tc.expectedAvg}
		if pc.AvgCustom() != expectedAvgCost {
			t.Errorf("expected Avg() to be %f, got %f", tc.expectedAvg, pc.AvgCustom().C)
		}
	}
}
