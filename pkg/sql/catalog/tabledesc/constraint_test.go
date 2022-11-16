// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tabledesc

import (
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/stretchr/testify/require"
)

// TestConstraintRetrieval tests the following three method inside catalog.TableDescriptor interface.
// - AllConstraints() []Constraint
// - AllActiveAndInactiveConstraints() []Constraint
// - AllActiveConstraints() []Constraint
func TestConstraintRetrieval(t *testing.T) {
	// Construct a table with the following constraints:
	//  - Primary Index: [ID_1:validated]
	//  - Indexes: [a non-unique index, ID_4:validated]
	//  - Checks: [ID_2:validated], [ID_3:unvalidated]
	//  - OutboundFKs: [ID_6:validated]
	//  - InboundFKs: [ID_5:validated]
	//  - UniqueWithoutIndexConstraints: [ID_7:dropping]
	//  - mutation slice: [ID_7:dropping:UniqueWithoutIndex, ID_8:validating:Check, ID_9:validating:UniqueIndex, a non-unique index]
	primaryIndex := descpb.IndexDescriptor{
		Unique:       true,
		ConstraintID: 1,
		EncodingType: descpb.PrimaryIndexEncoding,
	}

	indexes := make([]descpb.IndexDescriptor, 2)
	indexes[0] = descpb.IndexDescriptor{
		Unique: false,
	}
	indexes[1] = descpb.IndexDescriptor{
		Unique:       true,
		ConstraintID: 4,
	}

	checks := make([]*descpb.TableDescriptor_CheckConstraint, 2)
	checks[0] = &descpb.TableDescriptor_CheckConstraint{
		Validity:     descpb.ConstraintValidity_Validated,
		ConstraintID: 2,
	}
	checks[1] = &descpb.TableDescriptor_CheckConstraint{
		Validity:     descpb.ConstraintValidity_Unvalidated,
		ConstraintID: 3,
	}

	outboundFKs := make([]descpb.ForeignKeyConstraint, 1)
	outboundFKs[0] = descpb.ForeignKeyConstraint{
		Validity:     descpb.ConstraintValidity_Validated,
		ConstraintID: 6,
	}

	inboundFKs := make([]descpb.ForeignKeyConstraint, 1)
	inboundFKs[0] = descpb.ForeignKeyConstraint{
		Validity:     descpb.ConstraintValidity_Validated,
		ConstraintID: 5,
	}

	uniqueWithoutIndexConstraints := make([]descpb.UniqueWithoutIndexConstraint, 1)
	uniqueWithoutIndexConstraints[0] = descpb.UniqueWithoutIndexConstraint{
		Validity:     descpb.ConstraintValidity_Dropping,
		ConstraintID: 7,
	}

	mutations := make([]descpb.DescriptorMutation, 4)
	mutations[0] = descpb.DescriptorMutation{
		Descriptor_: &descpb.DescriptorMutation_Constraint{
			Constraint: &descpb.ConstraintToUpdate{
				ConstraintType:               descpb.ConstraintToUpdate_UNIQUE_WITHOUT_INDEX,
				Name:                         "unique_on_k_without_index",
				UniqueWithoutIndexConstraint: uniqueWithoutIndexConstraints[0],
			},
		},
		State:     descpb.DescriptorMutation_DELETE_ONLY,
		Direction: descpb.DescriptorMutation_DROP,
	}
	mutations[1] = descpb.DescriptorMutation{
		Descriptor_: &descpb.DescriptorMutation_Constraint{
			Constraint: &descpb.ConstraintToUpdate{
				ConstraintType: descpb.ConstraintToUpdate_CHECK,
				Check: descpb.TableDescriptor_CheckConstraint{
					Validity:     descpb.ConstraintValidity_Validating,
					ConstraintID: 8,
				},
			}},
		State:     descpb.DescriptorMutation_DELETE_ONLY,
		Direction: descpb.DescriptorMutation_ADD,
	}
	mutations[2] = descpb.DescriptorMutation{
		Descriptor_: &descpb.DescriptorMutation_Index{
			Index: &descpb.IndexDescriptor{
				Unique:       true,
				ConstraintID: 9,
			}},
		State:     descpb.DescriptorMutation_DELETE_ONLY,
		Direction: descpb.DescriptorMutation_ADD,
	}
	mutations[3] = descpb.DescriptorMutation{
		Descriptor_: &descpb.DescriptorMutation_Index{
			Index: &descpb.IndexDescriptor{
				Unique: false,
			}},
		State:     descpb.DescriptorMutation_DELETE_ONLY,
		Direction: descpb.DescriptorMutation_ADD,
	}

	tableDesc := NewBuilder(&descpb.TableDescriptor{
		PrimaryIndex:                  primaryIndex,
		Indexes:                       indexes,
		Checks:                        checks,
		OutboundFKs:                   outboundFKs,
		InboundFKs:                    inboundFKs,
		UniqueWithoutIndexConstraints: uniqueWithoutIndexConstraints,
		Mutations:                     mutations,
	}).BuildImmutable().(catalog.TableDescriptor)

	t.Run("test-AllConstraints", func(t *testing.T) {
		all := tableDesc.AllConstraints()
		sort.Slice(all, func(i, j int) bool {
			return all[i].GetConstraintID() < all[j].GetConstraintID()
		})
		require.Equal(t, len(all), 9)
		checkIndexBackedConstraint(t, all[0], 1, descpb.ConstraintValidity_Validated, descpb.PrimaryIndexEncoding)
		checkNonIndexBackedConstraint(t, all[1], 2, descpb.ConstraintValidity_Validated, descpb.ConstraintToUpdate_CHECK)
		checkNonIndexBackedConstraint(t, all[2], 3, descpb.ConstraintValidity_Unvalidated, descpb.ConstraintToUpdate_CHECK)
		checkIndexBackedConstraint(t, all[3], 4, descpb.ConstraintValidity_Validated, descpb.SecondaryIndexEncoding)
		checkNonIndexBackedConstraint(t, all[4], 5, descpb.ConstraintValidity_Validated, descpb.ConstraintToUpdate_FOREIGN_KEY)
		checkNonIndexBackedConstraint(t, all[5], 6, descpb.ConstraintValidity_Validated, descpb.ConstraintToUpdate_FOREIGN_KEY)
		checkNonIndexBackedConstraint(t, all[6], 7, descpb.ConstraintValidity_Dropping, descpb.ConstraintToUpdate_UNIQUE_WITHOUT_INDEX)
		checkNonIndexBackedConstraint(t, all[7], 8, descpb.ConstraintValidity_Validating, descpb.ConstraintToUpdate_CHECK)
		checkIndexBackedConstraint(t, all[8], 9, descpb.ConstraintValidity_Validating, descpb.SecondaryIndexEncoding)
	})

	t.Run("test-AllActiveAndInactiveConstraints", func(t *testing.T) {
		allActiveAndInactive := tableDesc.AllActiveAndInactiveConstraints()
		sort.Slice(allActiveAndInactive, func(i, j int) bool {
			return allActiveAndInactive[i].GetConstraintID() < allActiveAndInactive[j].GetConstraintID()
		})
		require.Equal(t, len(allActiveAndInactive), 8)
		checkIndexBackedConstraint(t, allActiveAndInactive[0], 1, descpb.ConstraintValidity_Validated, descpb.PrimaryIndexEncoding)
		checkNonIndexBackedConstraint(t, allActiveAndInactive[1], 2, descpb.ConstraintValidity_Validated, descpb.ConstraintToUpdate_CHECK)
		checkNonIndexBackedConstraint(t, allActiveAndInactive[2], 3, descpb.ConstraintValidity_Unvalidated, descpb.ConstraintToUpdate_CHECK)
		checkIndexBackedConstraint(t, allActiveAndInactive[3], 4, descpb.ConstraintValidity_Validated, descpb.SecondaryIndexEncoding)
		checkNonIndexBackedConstraint(t, allActiveAndInactive[4], 5, descpb.ConstraintValidity_Validated, descpb.ConstraintToUpdate_FOREIGN_KEY)
		checkNonIndexBackedConstraint(t, allActiveAndInactive[5], 6, descpb.ConstraintValidity_Validated, descpb.ConstraintToUpdate_FOREIGN_KEY)
		checkNonIndexBackedConstraint(t, allActiveAndInactive[6], 8, descpb.ConstraintValidity_Validating, descpb.ConstraintToUpdate_CHECK)
		checkIndexBackedConstraint(t, allActiveAndInactive[7], 9, descpb.ConstraintValidity_Validating, descpb.SecondaryIndexEncoding)
	})

	t.Run("test-AllActiveConstraints", func(t *testing.T) {
		allActive := tableDesc.AllActiveConstraints()
		sort.Slice(allActive, func(i, j int) bool {
			return allActive[i].GetConstraintID() < allActive[j].GetConstraintID()
		})
		require.Equal(t, len(allActive), 5)
		checkIndexBackedConstraint(t, allActive[0], 1, descpb.ConstraintValidity_Validated, descpb.PrimaryIndexEncoding)
		checkNonIndexBackedConstraint(t, allActive[1], 2, descpb.ConstraintValidity_Validated, descpb.ConstraintToUpdate_CHECK)
		checkIndexBackedConstraint(t, allActive[2], 4, descpb.ConstraintValidity_Validated, descpb.SecondaryIndexEncoding)
		checkNonIndexBackedConstraint(t, allActive[3], 5, descpb.ConstraintValidity_Validated, descpb.ConstraintToUpdate_FOREIGN_KEY)
		checkNonIndexBackedConstraint(t, allActive[4], 6, descpb.ConstraintValidity_Validated, descpb.ConstraintToUpdate_FOREIGN_KEY)
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
	expectedEncodingType descpb.IndexDescriptorEncodingType,
) {
	require.Equal(t, expectedID, c.GetConstraintID())
	require.Equal(t, expectedValidity, c.GetConstraintValidity())
	idx := c.AsUnique()
	if idx != nil {
		idx = c.AsPrimaryKey()
	}
	require.NotNil(t, idx)
	require.Equal(t, expectedEncodingType, idx.IndexDesc().EncodingType)
	if expectedEncodingType == descpb.SecondaryIndexEncoding {
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
		require.Zero(t, c.NotNullColumnID())
		require.Nil(t, c.AsForeignKey())
		require.Nil(t, c.AsUniqueWithoutIndex())
	case descpb.ConstraintToUpdate_NOT_NULL:
		require.NotNil(t, c.AsCheck())
		require.NotZero(t, c.NotNullColumnID())
		require.Nil(t, c.AsForeignKey())
		require.Nil(t, c.AsUniqueWithoutIndex())
	case descpb.ConstraintToUpdate_FOREIGN_KEY:
		require.Nil(t, c.AsCheck())
		require.Zero(t, c.NotNullColumnID())
		require.NotNil(t, c.AsForeignKey())
		require.Nil(t, c.AsUniqueWithoutIndex())
	case descpb.ConstraintToUpdate_UNIQUE_WITHOUT_INDEX:
		require.Nil(t, c.AsCheck())
		require.Zero(t, c.NotNullColumnID())
		require.Nil(t, c.AsForeignKey())
		require.NotNil(t, c.AsUniqueWithoutIndex())
	default:
		t.Fatalf("unexpected constraint type %d", expectedType)
	}
}
