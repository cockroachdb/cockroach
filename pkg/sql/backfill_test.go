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
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// constraintToUpdateForTest implements the catalog.ConstraintToUpdate interface.
// It's only used for testing
type constraintToUpdateForTest struct {
	desc *descpb.ConstraintToUpdate
}

func (c constraintToUpdateForTest) IsMutation() bool {
	panic(errors.AssertionFailedf("unimplemented"))
}

// IsRollback returns true iff the table element is in a rollback mutation.
func (c constraintToUpdateForTest) IsRollback() bool {
	panic(errors.AssertionFailedf("unimplemented"))
}

// MutationID returns the table element's mutationID if applicable,
// descpb.InvalidMutationID otherwise.
func (c constraintToUpdateForTest) MutationID() descpb.MutationID {
	panic(errors.AssertionFailedf("unimplemented"))
}

// WriteAndDeleteOnly returns true iff the table element is in a mutation in
// the delete-and-write-only state.
func (c constraintToUpdateForTest) WriteAndDeleteOnly() bool {
	panic(errors.AssertionFailedf("unimplemented"))
}

// DeleteOnly returns true iff the table element is in a mutation in the
// delete-only state.
func (c constraintToUpdateForTest) DeleteOnly() bool {
	panic(errors.AssertionFailedf("unimplemented"))
}

// Adding returns true iff the table element is in an add mutation.
func (c constraintToUpdateForTest) Adding() bool {
	panic(errors.AssertionFailedf("unimplemented"))
}

// Dropped returns true iff the table element is in a drop mutation.
func (c constraintToUpdateForTest) Dropped() bool {
	panic(errors.AssertionFailedf("unimplemented"))
}

// ConstraintToUpdateDesc returns the underlying protobuf descriptor.
func (c constraintToUpdateForTest) ConstraintToUpdateDesc() *descpb.ConstraintToUpdate {
	panic(errors.AssertionFailedf("unimplemented"))
}

// GetName returns the name of this constraint update mutation.
func (c constraintToUpdateForTest) GetName() string {
	panic(errors.AssertionFailedf("unimplemented"))
}

// IsCheck returns true iff this is an update for a check constraint.
func (c constraintToUpdateForTest) IsCheck() bool {
	return c.desc.ConstraintType == descpb.ConstraintToUpdate_CHECK
}

// Check returns the underlying check constraint, if there is one.
func (c constraintToUpdateForTest) Check() descpb.TableDescriptor_CheckConstraint {
	return c.desc.Check
}

// IsForeignKey returns true iff this is an update for a fk constraint.
func (c constraintToUpdateForTest) IsForeignKey() bool {
	panic(errors.AssertionFailedf("unimplemented"))
}

// ForeignKey returns the underlying fk constraint, if there is one.
func (c constraintToUpdateForTest) ForeignKey() descpb.ForeignKeyConstraint {
	panic(errors.AssertionFailedf("unimplemented"))
}

// IsNotNull returns true iff this is an update for a not-null constraint.
func (c constraintToUpdateForTest) IsNotNull() bool {
	panic(errors.AssertionFailedf("unimplemented"))
}

// NotNullColumnID returns the underlying not-null column ID, if there is one.
func (c constraintToUpdateForTest) NotNullColumnID() descpb.ColumnID {
	panic(errors.AssertionFailedf("unimplemented"))
}

// IsUniqueWithoutIndex returns true iff this is an update for a unique without
// index constraint.
func (c constraintToUpdateForTest) IsUniqueWithoutIndex() bool {
	panic(errors.AssertionFailedf("unimplemented"))
}

// UniqueWithoutIndex returns the underlying unique without index constraint, if
// there is one.
func (c constraintToUpdateForTest) UniqueWithoutIndex() descpb.UniqueWithoutIndexConstraint {
	panic(errors.AssertionFailedf("unimplemented"))
}

func TestShouldSkipConstraintValidation(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tableDesc := &tabledesc.Mutable{}
	tableDesc.TableDescriptor = descpb.TableDescriptor{
		ID:            2,
		ParentID:      1,
		Name:          "foo",
		FormatVersion: descpb.InterleavedFormatVersion,
		Columns: []descpb.ColumnDescriptor{
			{ID: 1, Name: "c1"},
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
		},
		Mutations: []descpb.DescriptorMutation{
			{
				Descriptor_: &descpb.DescriptorMutation_Index{
					Index: &descpb.IndexDescriptor{
						ID: 2, Name: "new_hash_index", KeyColumnIDs: []descpb.ColumnID{2, 3},
						KeyColumnNames: []string{"c2", "c3"},
						KeyColumnDirections: []descpb.IndexDescriptor_Direction{
							descpb.IndexDescriptor_ASC,
							descpb.IndexDescriptor_ASC,
						},
						EncodingType: descpb.PrimaryIndexEncoding,
						Version:      descpb.LatestPrimaryIndexDescriptorVersion,
						Sharded: descpb.ShardedDescriptor{
							IsSharded:    true,
							Name:         "c3",
							ShardBuckets: 8,
							ColumnNames:  []string{"c2"},
						},
					},
				},
				Direction: descpb.DescriptorMutation_ADD,
			},
			{
				Descriptor_: &descpb.DescriptorMutation_Column{
					Column: &descpb.ColumnDescriptor{
						ID:      2,
						Name:    "c2",
						Virtual: true,
					},
				},
				Direction: descpb.DescriptorMutation_ADD,
			},
			{
				Descriptor_: &descpb.DescriptorMutation_Column{
					Column: &descpb.ColumnDescriptor{
						ID:      3,
						Name:    "c3",
						Virtual: true,
					},
				},
				Direction: descpb.DescriptorMutation_ADD,
			},
		},
	}

	type testCase struct {
		name           string
		constraint     constraintToUpdateForTest
		expectedResult bool
	}

	tcs := []testCase{
		{
			name: "test_adding_shard_col_check_constraint",
			constraint: constraintToUpdateForTest{
				desc: &descpb.ConstraintToUpdate{
					ConstraintType: descpb.ConstraintToUpdate_CHECK,
					Check: descpb.TableDescriptor_CheckConstraint{
						Expr:      "some fake expr",
						Name:      "some fake name",
						Validity:  descpb.ConstraintValidity_Validating,
						ColumnIDs: []descpb.ColumnID{3},
						Hidden:    true,
					},
				},
			},
			expectedResult: true,
		},
		{
			name: "test_adding_non_shard_col_check_constraint",
			constraint: constraintToUpdateForTest{
				desc: &descpb.ConstraintToUpdate{
					ConstraintType: descpb.ConstraintToUpdate_CHECK,
					Check: descpb.TableDescriptor_CheckConstraint{
						Expr:      "some fake expr",
						Name:      "some fake name",
						Validity:  descpb.ConstraintValidity_Validating,
						ColumnIDs: []descpb.ColumnID{2},
						Hidden:    false,
					},
				},
			},
			expectedResult: false,
		},
		{
			name: "test_adding_multi_col_check_constraint",
			constraint: constraintToUpdateForTest{
				desc: &descpb.ConstraintToUpdate{
					ConstraintType: descpb.ConstraintToUpdate_CHECK,
					Check: descpb.TableDescriptor_CheckConstraint{
						Expr:      "some fake expr",
						Name:      "some fake name",
						Validity:  descpb.ConstraintValidity_Validating,
						ColumnIDs: []descpb.ColumnID{2, 3},
						Hidden:    false,
					},
				},
			},
			expectedResult: false,
		},
		{
			name: "test_adding_non_check_constraint",
			constraint: constraintToUpdateForTest{
				desc: &descpb.ConstraintToUpdate{
					ConstraintType: descpb.ConstraintToUpdate_FOREIGN_KEY,
				},
			},
			expectedResult: false,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			isSkipping, err := shouldSkipConstraintValidation(tableDesc, tc.constraint)
			if err != nil {
				t.Fatal("Failed to run function being tested:", err)
			}
			require.Equal(t, tc.expectedResult, isSkipping)
		})
	}

}
