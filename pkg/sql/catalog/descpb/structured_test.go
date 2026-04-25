// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package descpb

import (
	"testing"

	"github.com/stretchr/testify/require"
)

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
	for name, tc := range map[string]struct {
		candidate, base ColumnIDs
		expectedResult  bool
	}{
		"empty candidate set and empty base set": {
			candidate:      ColumnIDs{},
			base:           ColumnIDs{},
			expectedResult: false,
		},
		"empty candidate set and non-empty base set": {
			candidate:      ColumnIDs{},
			base:           ColumnIDs{ColumnID(1), ColumnID(2)},
			expectedResult: false,
		},
		"non-empty candidate set and empty base set": {
			candidate:      ColumnIDs{ColumnID(1)},
			base:           ColumnIDs{},
			expectedResult: false,
		},
		"strict subset": {
			candidate:      ColumnIDs{ColumnID(1)},
			base:           ColumnIDs{ColumnID(1), ColumnID(2), ColumnID(3)},
			expectedResult: true,
		},
		"exact match counts as subset": {
			candidate:      ColumnIDs{ColumnID(1), ColumnID(2)},
			base:           ColumnIDs{ColumnID(1), ColumnID(2)},
			expectedResult: true,
		},
		"permutation counts as subset": {
			candidate:      ColumnIDs{ColumnID(2), ColumnID(1)},
			base:           ColumnIDs{ColumnID(1), ColumnID(2)},
			expectedResult: true,
		},
		"candidate set not contained at all": {
			candidate:      ColumnIDs{ColumnID(1), ColumnID(2)},
			base:           ColumnIDs{ColumnID(3), ColumnID(4)},
			expectedResult: false,
		},
		"candidate set partially contained": {
			candidate:      ColumnIDs{ColumnID(1), ColumnID(2)},
			base:           ColumnIDs{ColumnID(1), ColumnID(3)},
			expectedResult: false,
		},
		"candidate set is actually a superset": {
			candidate:      ColumnIDs{ColumnID(1), ColumnID(2), ColumnID(3)},
			base:           ColumnIDs{ColumnID(1), ColumnID(2)},
			expectedResult: false,
		},
		"duplicates in candidate set all present in base set": {
			candidate:      ColumnIDs{ColumnID(1), ColumnID(2), ColumnID(1)},
			base:           ColumnIDs{ColumnID(1), ColumnID(2), ColumnID(3)},
			expectedResult: true,
		},
		"duplicates in candidate set and one not in base set": {
			candidate:      ColumnIDs{ColumnID(1), ColumnID(2), ColumnID(1), ColumnID(4)},
			base:           ColumnIDs{ColumnID(1), ColumnID(2), ColumnID(3)},
			expectedResult: false,
		},
		"duplicates in base set do not affect result": {
			candidate:      ColumnIDs{ColumnID(1), ColumnID(2)},
			base:           ColumnIDs{ColumnID(1), ColumnID(1), ColumnID(2), ColumnID(2)},
			expectedResult: true,
		},
	} {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, tc.expectedResult, tc.candidate.IsNonEmptySubsetOf(tc.base))
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
