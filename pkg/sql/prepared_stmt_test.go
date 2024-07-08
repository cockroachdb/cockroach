// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestPlanCostStats(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	type testCase struct {
		input       []memo.Cost
		expectedLen int
		expectedAvg memo.Cost
	}
	testCases := []testCase{
		{input: []memo.Cost{}, expectedLen: 0, expectedAvg: 0},
		{input: []memo.Cost{0, 0}, expectedLen: 2, expectedAvg: 0},
		{input: []memo.Cost{1, 1}, expectedLen: 2, expectedAvg: 1},
		{input: []memo.Cost{1, 2, 3, 4, 5}, expectedLen: 5, expectedAvg: 3},
		{input: []memo.Cost{1, 2, 3, 4, 5, 6}, expectedLen: 5, expectedAvg: 4},
		{input: []memo.Cost{9, 9, 9, 9, 9, 1, 2, 3, 4, 5}, expectedLen: 5, expectedAvg: 3},
	}
	for _, tc := range testCases {
		var pc planCostStats
		for _, cost := range tc.input {
			pc.Add(cost)
		}
		if pc.Len() != tc.expectedLen {
			t.Errorf("expected Len() to be %d, got %d", tc.expectedLen, pc.Len())
		}
		if pc.Avg() != tc.expectedAvg {
			t.Errorf("expected Avg() to be %f, got %f", tc.expectedAvg, pc.Avg())
		}
	}

}
