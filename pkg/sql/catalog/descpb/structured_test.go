// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package descpb

import "testing"

func TestColumnIDsPermutationOf(t *testing.T) {
	type testCase struct {
		name                       string
		columnIDsOne, columnIDsTwo ColumnIDs
		expectedResult             bool
	}

	testCases := []testCase{
		{
			name:           "different IDs",
			columnIDsOne:   ColumnIDs{ColumnID(1), ColumnID(2)},
			columnIDsTwo:   ColumnIDs{ColumnID(3), ColumnID(4)},
			expectedResult: false,
		},
		{
			name:           "one element extra",
			columnIDsOne:   ColumnIDs{ColumnID(1), ColumnID(2)},
			columnIDsTwo:   ColumnIDs{ColumnID(1), ColumnID(2), ColumnID(3)},
			expectedResult: false,
		},
		{
			name:           "one element less",
			columnIDsOne:   ColumnIDs{ColumnID(1), ColumnID(2), ColumnID(3)},
			columnIDsTwo:   ColumnIDs{ColumnID(1), ColumnID(2)},
			expectedResult: false,
		},
		{
			name:           "same elements, in different order",
			columnIDsOne:   ColumnIDs{ColumnID(1), ColumnID(2), ColumnID(3)},
			columnIDsTwo:   ColumnIDs{ColumnID(3), ColumnID(2), ColumnID(1)},
			expectedResult: true,
		},
		{
			name:           "when duplicate is in both, returns true",
			columnIDsOne:   ColumnIDs{ColumnID(1), ColumnID(2), ColumnID(1), ColumnID(3)},
			columnIDsTwo:   ColumnIDs{ColumnID(3), ColumnID(1), ColumnID(2), ColumnID(1)},
			expectedResult: true,
		},
		{
			name:           "when duplicate in first, returns true",
			columnIDsOne:   ColumnIDs{ColumnID(1), ColumnID(2), ColumnID(1), ColumnID(3)},
			columnIDsTwo:   ColumnIDs{ColumnID(3), ColumnID(2), ColumnID(1)},
			expectedResult: true,
		},
		{
			name:           "when duplicate in second, returns true",
			columnIDsOne:   ColumnIDs{ColumnID(2), ColumnID(1), ColumnID(3)},
			columnIDsTwo:   ColumnIDs{ColumnID(3), ColumnID(1), ColumnID(2), ColumnID(1)},
			expectedResult: true,
		},
		{
			name:           "when each list has a different duplicate, same length, returns true",
			columnIDsOne:   ColumnIDs{ColumnID(2), ColumnID(1), ColumnID(2)},
			columnIDsTwo:   ColumnIDs{ColumnID(1), ColumnID(2), ColumnID(1)},
			expectedResult: true,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.columnIDsOne.PermutationOf(tt.columnIDsTwo)
			if result != tt.expectedResult {
				t.Errorf("PermuationOf() %s: got %v, want %v", tt.name, result, tt.expectedResult)
			}
		})
	}
}
