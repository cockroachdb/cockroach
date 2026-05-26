// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tabledesc

import (
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catenumpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/stretchr/testify/require"
)

// TestConstraintRetrieval tests the AllConstraints and EnforcedConstraints
// methods of the catalog.TableDescriptor interface.
//
// This test constructs a table with the following elements:
//   - Primary key, index ID #1, constraint ID #1.
//   - Non-unique index, index ID #2.
//   - Unique index, index ID #3, constraint ID #4.
//   - Check, constraint ID #2.
//   - Check, constraint ID #3, unvalidated, adding.
//   - Outbound foreign key, constraint ID #6.
//   - Inbound foreign key, constraint ID #5.
//   - Unique-without-index, constraint ID #7, dropping.
//   - Unique index, index ID #4, constraint ID #8, adding.
func TestConstraintRetrieval(t *testing.T) {
	primaryIndex := descpb.IndexDescriptor{
		ID:           1,
		Unique:       true,
		KeyColumnIDs: []descpb.ColumnID{1},
		ConstraintID: 1,
		EncodingType: catenumpb.PrimaryIndexEncoding,
	}

	indexes := []descpb.IndexDescriptor{
		{
			ID:           2,
			Unique:       false,
			KeyColumnIDs: []descpb.ColumnID{1},
		},
		{
			ID:           3,
			Unique:       true,
			ConstraintID: 4,
			KeyColumnIDs: []descpb.ColumnID{1},
		},
	}

	checks := []*descpb.TableDescriptor_CheckConstraint{
		{
			Validity:     descpb.ConstraintValidity_Validated,
			ConstraintID: 2,
		},
		{
			Validity:     descpb.ConstraintValidity_Unvalidated,
			ConstraintID: 3,
		},
	}

	outboundFKs := []descpb.ForeignKeyConstraint{
		{
			Validity:     descpb.ConstraintValidity_Validated,
			ConstraintID: 6,
		},
	}

	inboundFKs := []descpb.ForeignKeyConstraint{
		{
			Validity:     descpb.ConstraintValidity_Validated,
			ConstraintID: 5,
		},
	}

	uniqueWithoutIndexConstraints := []descpb.UniqueWithoutIndexConstraint{
		{
			Validity:     descpb.ConstraintValidity_Dropping,
			ConstraintID: 7,
			ColumnIDs:    []descpb.ColumnID{1},
		},
	}

	mutations := []descpb.DescriptorMutation{
		{
			Descriptor_: &descpb.DescriptorMutation_Constraint{
				Constraint: &descpb.ConstraintToUpdate{
					ConstraintType:               descpb.ConstraintToUpdate_UNIQUE_WITHOUT_INDEX,
					UniqueWithoutIndexConstraint: uniqueWithoutIndexConstraints[0],
				},
			},
			State:     descpb.DescriptorMutation_WRITE_ONLY,
			Direction: descpb.DescriptorMutation_DROP,
		},
		{
			Descriptor_: &descpb.DescriptorMutation_Constraint{
				Constraint: &descpb.ConstraintToUpdate{
					ConstraintType: descpb.ConstraintToUpdate_CHECK,
					Check:          *checks[1],
				}},
			State:     descpb.DescriptorMutation_DELETE_ONLY,
			Direction: descpb.DescriptorMutation_ADD,
		},
		{
			Descriptor_: &descpb.DescriptorMutation_Index{
				Index: &descpb.IndexDescriptor{
					ID:           4,
					Unique:       true,
					ConstraintID: 8,
					KeyColumnIDs: []descpb.ColumnID{1},
				}},
			State:     descpb.DescriptorMutation_DELETE_ONLY,
			Direction: descpb.DescriptorMutation_ADD,
		},
		{
			Descriptor_: &descpb.DescriptorMutation_Index{
				Index: &descpb.IndexDescriptor{
					ID:           5,
					Unique:       false,
					KeyColumnIDs: []descpb.ColumnID{1},
				}},
			State:     descpb.DescriptorMutation_DELETE_ONLY,
			Direction: descpb.DescriptorMutation_ADD,
		},
	}

	tableDesc := NewBuilder(&descpb.TableDescriptor{
		PrimaryIndex:                  primaryIndex,
		Indexes:                       indexes,
		Checks:                        checks,
		OutboundFKs:                   outboundFKs,
		InboundFKs:                    inboundFKs,
		UniqueWithoutIndexConstraints: uniqueWithoutIndexConstraints,
		Mutations:                     mutations,
	}).BuildImmutableTable()

	t.Run("test-AllConstraints", func(t *testing.T) {
		all := tableDesc.AllConstraints()
		sort.Slice(all, func(i, j int) bool {
			return all[i].GetConstraintID() < all[j].GetConstraintID()
		})
		require.Len(t, all, 7)
		checkIndexBackedConstraint(t, all[0], 1, descpb.ConstraintValidity_Validated, catenumpb.PrimaryIndexEncoding)
		checkNonIndexBackedConstraint(t, all[1], 2, descpb.ConstraintValidity_Validated, descpb.ConstraintToUpdate_CHECK)
		checkNonIndexBackedConstraint(t, all[2], 3, descpb.ConstraintValidity_Unvalidated, descpb.ConstraintToUpdate_CHECK)
		checkIndexBackedConstraint(t, all[3], 4, descpb.ConstraintValidity_Validated, catenumpb.SecondaryIndexEncoding)
		// ID 5 is missing: inbound foreign keys are not constraints.
		checkNonIndexBackedConstraint(t, all[4], 6, descpb.ConstraintValidity_Validated, descpb.ConstraintToUpdate_FOREIGN_KEY)
		checkNonIndexBackedConstraint(t, all[5], 7, descpb.ConstraintValidity_Dropping, descpb.ConstraintToUpdate_UNIQUE_WITHOUT_INDEX)
		checkIndexBackedConstraint(t, all[6], 8, descpb.ConstraintValidity_Validating, catenumpb.SecondaryIndexEncoding)
	})
	t.Run("test-EnforcedConstraints", func(t *testing.T) {
		enforced := tableDesc.EnforcedConstraints()
		sort.Slice(enforced, func(i, j int) bool {
			return enforced[i].GetConstraintID() < enforced[j].GetConstraintID()
		})
		require.Len(t, enforced, 6)
		checkIndexBackedConstraint(t, enforced[0], 1, descpb.ConstraintValidity_Validated, catenumpb.PrimaryIndexEncoding)
		checkNonIndexBackedConstraint(t, enforced[1], 2, descpb.ConstraintValidity_Validated, descpb.ConstraintToUpdate_CHECK)
		checkNonIndexBackedConstraint(t, enforced[2], 3, descpb.ConstraintValidity_Unvalidated, descpb.ConstraintToUpdate_CHECK)
		checkIndexBackedConstraint(t, enforced[3], 4, descpb.ConstraintValidity_Validated, catenumpb.SecondaryIndexEncoding)
		// ID 5 is missing: inbound foreign keys are not constraints.
		checkNonIndexBackedConstraint(t, enforced[4], 6, descpb.ConstraintValidity_Validated, descpb.ConstraintToUpdate_FOREIGN_KEY)
		checkNonIndexBackedConstraint(t, enforced[5], 7, descpb.ConstraintValidity_Dropping, descpb.ConstraintToUpdate_UNIQUE_WITHOUT_INDEX)
	})
}

// checkIndexBackedConstraint ensures `c` (PRIMARY KEY or UNIQUE)
// has the expected ID, validity, and type.
//
// `expectedEncodingType = secondaryIndexEncoding` is used for
// UNIQUE constraint type.
func checkIndexBackedConstraint(
	t *testing.T,
	c catalog.Constraint,
	expectedID descpb.ConstraintID,
	expectedValidity descpb.ConstraintValidity,
	expectedEncodingType catenumpb.IndexDescriptorEncodingType,
) {
	require.Equal(t, expectedID, c.GetConstraintID())
	require.Equal(t, expectedValidity, c.GetConstraintValidity())
	idx := c.AsUniqueWithIndex()
	require.NotNil(t, idx)
	require.Equal(t, expectedEncodingType, idx.IndexDesc().EncodingType)
	if expectedEncodingType == catenumpb.SecondaryIndexEncoding {
		require.Equal(t, true, idx.IndexDesc().Unique)
	}
}

// checkNonIndexBackedConstraint ensures `c` (CHECK, FK, or UNIQUE_WITHOUT_INDEX)
// has the expected ID, validity, and type.
func checkNonIndexBackedConstraint(
	t *testing.T,
	c catalog.Constraint,
	expectedID descpb.ConstraintID,
	expectedValidity descpb.ConstraintValidity,
	expectedType descpb.ConstraintToUpdate_ConstraintType,
) {
	require.Equal(t, expectedID, c.GetConstraintID())
	require.Equal(t, expectedValidity, c.GetConstraintValidity())
	switch expectedType {
	case descpb.ConstraintToUpdate_CHECK:
		require.NotNil(t, c.AsCheck())
		require.False(t, c.AsCheck().IsNotNullColumnConstraint())
		require.Nil(t, c.AsForeignKey())
		require.Nil(t, c.AsUniqueWithoutIndex())
	case descpb.ConstraintToUpdate_NOT_NULL:
		require.NotNil(t, c.AsCheck())
		require.True(t, c.AsCheck().IsNotNullColumnConstraint())
		require.Nil(t, c.AsForeignKey())
		require.Nil(t, c.AsUniqueWithoutIndex())
	case descpb.ConstraintToUpdate_FOREIGN_KEY:
		require.Nil(t, c.AsCheck())
		require.NotNil(t, c.AsForeignKey())
		require.Nil(t, c.AsUniqueWithoutIndex())
	case descpb.ConstraintToUpdate_UNIQUE_WITHOUT_INDEX:
		require.Nil(t, c.AsCheck())
		require.Nil(t, c.AsForeignKey())
		require.NotNil(t, c.AsUniqueWithoutIndex())
	default:
		t.Fatalf("unexpected constraint type %d", expectedType)
	}
}
