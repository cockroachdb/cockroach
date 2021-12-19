// Copyright 2021 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestShouldValidateCheckConstraint(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tableDesc := &tabledesc.Mutable{}
	tableDesc.TableDescriptor = descpb.TableDescriptor{
		ID:            2,
		ParentID:      1,
		Name:          "foo",
		FormatVersion: descpb.InterleavedFormatVersion,
		Columns: []descpb.ColumnDescriptor{
			{ID: 1, Name: "c1"},
			{ID: 2, Name: "c2"},
			{ID: 3, Name: "c3", Virtual: true},
		},
		Families: []descpb.ColumnFamilyDescriptor{
			{ID: 0, Name: "primary", ColumnIDs: []descpb.ColumnID{1, 2}, ColumnNames: []string{"c1", "c2"}},
		},
		PrimaryIndex: descpb.IndexDescriptor{
			ID: 1, Name: "pri", KeyColumnIDs: []descpb.ColumnID{1},
			KeyColumnNames:      []string{"c1"},
			KeyColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
			EncodingType:        descpb.PrimaryIndexEncoding,
			Version:             descpb.LatestPrimaryIndexDescriptorVersion,
			Sharded: descpb.ShardedDescriptor{
				IsSharded:    true,
				Name:         "c3",
				ShardBuckets: 8,
				ColumnNames:  []string{"c1"},
			},
		},
	}

	type testCase struct {
		name            string
		checkConstraint descpb.TableDescriptor_CheckConstraint
		expectedResult  bool
	}

	tcs := []testCase{
		{
			name: "test_shard_col_check_constraint_validating",
			checkConstraint: descpb.TableDescriptor_CheckConstraint{
				Expr:      "some fake expr",
				Name:      "some fake name",
				Validity:  descpb.ConstraintValidity_Validating,
				ColumnIDs: []descpb.ColumnID{3},
				Hidden:    true,
			},
			expectedResult: false,
		},
		{
			name: "test_shard_col_check_constraint_not_hidden",
			checkConstraint: descpb.TableDescriptor_CheckConstraint{
				Expr:      "some fake expr",
				Name:      "some fake name",
				Validity:  descpb.ConstraintValidity_Validating,
				ColumnIDs: []descpb.ColumnID{3},
				Hidden:    false,
			},
			expectedResult: true,
		},
		{
			name: "test_check_constraint_more_than_one_col",
			checkConstraint: descpb.TableDescriptor_CheckConstraint{
				Expr:      "some fake expr",
				Name:      "some fake name",
				Validity:  descpb.ConstraintValidity_Validating,
				ColumnIDs: []descpb.ColumnID{1, 3},
				Hidden:    false,
			},
			expectedResult: true,
		},
		{
			name: "test_check_constraint_validated",
			checkConstraint: descpb.TableDescriptor_CheckConstraint{
				Expr:      "some fake expr",
				Name:      "some fake name",
				Validity:  descpb.ConstraintValidity_Validated,
				ColumnIDs: []descpb.ColumnID{1, 3},
				Hidden:    false,
			},
			expectedResult: false,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			isValidating, err := shouldValidateCheckConstraint(tableDesc, tc.checkConstraint)
			if err != nil {
				t.Fatal("Failed to run function being tested:", err)
			}
			require.Equal(t, tc.expectedResult, isValidating)
		})
	}

}
