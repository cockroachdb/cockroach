// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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

func TestColumnIDsIsNonEmptySubsetOf(t *testing.T) {
	type testCase struct {
		name           string
		subject, input ColumnIDs
		expectedResult bool
	}

	testCases := []testCase{
		{
			name:           "empty subject is rejected even against any input",
			subject:        ColumnIDs{},
			input:          ColumnIDs{ColumnID(1), ColumnID(2)},
			expectedResult: false,
		},
		{
			name:           "empty subject and empty input is rejected",
			subject:        ColumnIDs{},
			input:          ColumnIDs{},
			expectedResult: false,
		},
		{
			name:           "non-empty subject, empty input",
			subject:        ColumnIDs{ColumnID(1)},
			input:          ColumnIDs{},
			expectedResult: false,
		},
		{
			name:           "strict subset",
			subject:        ColumnIDs{ColumnID(1)},
			input:          ColumnIDs{ColumnID(1), ColumnID(2), ColumnID(3)},
			expectedResult: true,
		},
		{
			name:           "exact match counts as subset",
			subject:        ColumnIDs{ColumnID(1), ColumnID(2)},
			input:          ColumnIDs{ColumnID(1), ColumnID(2)},
			expectedResult: true,
		},
		{
			name:           "permutation counts as subset",
			subject:        ColumnIDs{ColumnID(2), ColumnID(1)},
			input:          ColumnIDs{ColumnID(1), ColumnID(2)},
			expectedResult: true,
		},
		{
			name:           "subject not contained at all",
			subject:        ColumnIDs{ColumnID(1), ColumnID(2)},
			input:          ColumnIDs{ColumnID(3), ColumnID(4)},
			expectedResult: false,
		},
		{
			name:           "subject partially contained",
			subject:        ColumnIDs{ColumnID(1), ColumnID(2)},
			input:          ColumnIDs{ColumnID(1), ColumnID(3)},
			expectedResult: false,
		},
		{
			name:           "subject is superset",
			subject:        ColumnIDs{ColumnID(1), ColumnID(2), ColumnID(3)},
			input:          ColumnIDs{ColumnID(1), ColumnID(2)},
			expectedResult: false,
		},
		{
			name:           "duplicates in subject all present in input",
			subject:        ColumnIDs{ColumnID(1), ColumnID(2), ColumnID(1)},
			input:          ColumnIDs{ColumnID(1), ColumnID(2), ColumnID(3)},
			expectedResult: true,
		},
		{
			name:           "duplicates in subject, one not in input",
			subject:        ColumnIDs{ColumnID(1), ColumnID(2), ColumnID(1), ColumnID(4)},
			input:          ColumnIDs{ColumnID(1), ColumnID(2), ColumnID(3)},
			expectedResult: false,
		},
		{
			name:           "duplicates in input do not affect result",
			subject:        ColumnIDs{ColumnID(1), ColumnID(2)},
			input:          ColumnIDs{ColumnID(1), ColumnID(1), ColumnID(2), ColumnID(2)},
			expectedResult: true,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.subject.IsNonEmptySubsetOf(tt.input)
			if result != tt.expectedResult {
				t.Errorf("IsNonEmptySubsetOf() %s: got %v, want %v", tt.name, result, tt.expectedResult)
			}
		})
	}
}

func TestEffectiveCacheSize(t *testing.T) {
	type testCase struct {
		name           string
		session, node  int64
		expectedResult int64
	}

	testCases := []testCase{
		{
			name:           "both unset",
			session:        0,
			node:           0,
			expectedResult: 1,
		},
		{
			name:           "session unset",
			session:        0,
			node:           5,
			expectedResult: 5,
		},
		{
			name:           "node unset",
			session:        5,
			node:           0,
			expectedResult: 5,
		},

		{
			name:           "both disabled",
			session:        0,
			node:           0,
			expectedResult: 1,
		},
		{
			name:           "session disabled",
			session:        1,
			node:           5,
			expectedResult: 5,
		},
		{
			name:           "session disabled",
			session:        5,
			node:           1,
			expectedResult: 5,
		},

		{
			name:           "both enabled",
			session:        11,
			node:           13,
			expectedResult: 11,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			opts := TableDescriptor_SequenceOpts{
				SessionCacheSize: tt.session,
				NodeCacheSize:    tt.node,
			}
			result := opts.EffectiveCacheSize()
			if result != tt.expectedResult {
				t.Errorf("EffectiveCacheSize() %s: got %v, want %v", tt.name, result, tt.expectedResult)
			}
		})
	}

}
