// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catenumpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// constraintToUpdateForTest implements the catalog.Constraint interface.
// It's only used for testing
type constraintToUpdateForTest struct {
	catalog.Constraint
	desc *descpb.ConstraintToUpdate
}

var _ catalog.CheckConstraint = (*constraintToUpdateForTest)(nil)

// AsCheck implements the catalog.Constraint interface.
func (c constraintToUpdateForTest) AsCheck() catalog.CheckConstraint {
	if c.desc.ConstraintType != descpb.ConstraintToUpdate_CHECK {
		return nil
	}
	return c
}

// CheckDesc implements the catalog.CheckConstraint interface.
func (c constraintToUpdateForTest) CheckDesc() *descpb.TableDescriptor_CheckConstraint {
	return &c.desc.Check
}

// GetExpr implements the catalog.CheckConstraint interface.
func (c constraintToUpdateForTest) GetExpr() string {
	return c.desc.Check.Expr
}

// NumReferencedColumns implements the catalog.CheckConstraint interface.
func (c constraintToUpdateForTest) NumReferencedColumns() int {
	return len(c.desc.Check.ColumnIDs)
}

// GetReferencedColumnID implements the catalog.CheckConstraint interface.
func (c constraintToUpdateForTest) GetReferencedColumnID(columnOrdinal int) descpb.ColumnID {
	return c.desc.Check.ColumnIDs[columnOrdinal]
}

// CollectReferencedColumnIDs implements the catalog.CheckConstraint interface.
func (c constraintToUpdateForTest) CollectReferencedColumnIDs() catalog.TableColSet {
	return catalog.MakeTableColSet(c.desc.Check.ColumnIDs...)
}

// IsHashShardingConstraint implements the catalog.CheckConstraint interface.
func (c constraintToUpdateForTest) IsHashShardingConstraint() bool {
	return c.desc.Check.FromHashShardedColumn
}

// IsNotNullColumnConstraint implements the catalog.CheckConstraint interface.
func (c constraintToUpdateForTest) IsNotNullColumnConstraint() bool {
	return c.desc.NotNullColumn != 0
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
			KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC},
			EncodingType:        catenumpb.PrimaryIndexEncoding,
			Version:             descpb.LatestIndexDescriptorVersion,
		},
		Mutations: []descpb.DescriptorMutation{
			{
				Descriptor_: &descpb.DescriptorMutation_Index{
					Index: &descpb.IndexDescriptor{
						ID: 2, Name: "new_hash_index", KeyColumnIDs: []descpb.ColumnID{2, 3},
						KeyColumnNames: []string{"c2", "c3"},
						KeyColumnDirections: []catenumpb.IndexColumn_Direction{
							catenumpb.IndexColumn_ASC,
							catenumpb.IndexColumn_ASC,
						},
						EncodingType: catenumpb.PrimaryIndexEncoding,
						Version:      descpb.LatestIndexDescriptorVersion,
						Sharded: catpb.ShardedDescriptor{
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
						Expr:                  "some fake expr",
						Name:                  "some fake name",
						Validity:              descpb.ConstraintValidity_Validating,
						ColumnIDs:             []descpb.ColumnID{3},
						FromHashShardedColumn: true,
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
						Expr:                  "some fake expr",
						Name:                  "some fake name",
						Validity:              descpb.ConstraintValidity_Validating,
						ColumnIDs:             []descpb.ColumnID{2},
						FromHashShardedColumn: false,
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
						Expr:                  "some fake expr",
						Name:                  "some fake name",
						Validity:              descpb.ConstraintValidity_Validating,
						ColumnIDs:             []descpb.ColumnID{2, 3},
						FromHashShardedColumn: false,
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

func TestMultiStageFractionScaler(t *testing.T) {
	defer leaktest.AfterTest(t)()

	stages := []float32{0.4, 0.8, 1.0}
	t.Run("returns a fraction within the range of the given stage", func(t *testing.T) {
		s := multiStageFractionScaler{initial: 0.0, stages: stages}
		f, err := s.fractionCompleteFromStageFraction(0, 0.0)
		require.NoError(t, err)
		assert.Equal(t, float32(0.0), f)

		f, err = s.fractionCompleteFromStageFraction(0, 0.5)
		require.NoError(t, err)
		assert.Equal(t, float32(0.20), f)

		f, err = s.fractionCompleteFromStageFraction(0, 1.0)
		require.NoError(t, err)
		assert.Equal(t, float32(0.40), f)

		f, err = s.fractionCompleteFromStageFraction(1, 0.0)
		require.NoError(t, err)
		assert.Equal(t, float32(0.40), f)
	})
	t.Run("returns a fraction that is always >= initial", func(t *testing.T) {
		s := multiStageFractionScaler{initial: float32(0.60), stages: stages}
		f, err := s.fractionCompleteFromStageFraction(0, 0.0)
		require.NoError(t, err)
		assert.Equal(t, float32(0.60), f)

		f, err = s.fractionCompleteFromStageFraction(0, 1.0)
		require.NoError(t, err)
		assert.Equal(t, float32(0.60), f)
	})
	t.Run("errors if given an unknown stage", func(t *testing.T) {
		s := multiStageFractionScaler{initial: float32(0.60), stages: stages}
		_, err := s.fractionCompleteFromStageFraction(5, 0.0)
		require.Error(t, err)
	})
	t.Run("errors if given a negative fraction", func(t *testing.T) {
		s := multiStageFractionScaler{initial: float32(0.60), stages: stages}
		_, err := s.fractionCompleteFromStageFraction(0, -float32(0.60))
		require.Error(t, err)
	})
	t.Run("errors if given a fraction above 1", func(t *testing.T) {
		s := multiStageFractionScaler{initial: float32(0.60), stages: stages}
		_, err := s.fractionCompleteFromStageFraction(0, 1.60)
		require.Error(t, err)
	})
}
