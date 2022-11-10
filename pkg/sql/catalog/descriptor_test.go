// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package catalog_test

import (
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/redact"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
)

func TestFormatSafeDescriptorProperties(t *testing.T) {
	for _, tc := range []struct {
		desc catalog.Descriptor
		exp  string
	}{
		{
			desc: tabledesc.NewBuilder(&descpb.TableDescriptor{
				ID:       27,
				Version:  2,
				ParentID: 12,
				State:    descpb.DescriptorState_ADD,
			}).BuildImmutable(),
			exp: "ID: 27, Version: 2, ModificationTime: \"0,0\", ParentID: 12, ParentSchemaID: 29, State: ADD",
		},
		{
			desc: schemadesc.NewBuilder(&descpb.SchemaDescriptor{
				ID:            12,
				Version:       1,
				ParentID:      2,
				State:         descpb.DescriptorState_OFFLINE,
				OfflineReason: "foo",
			}).BuildImmutable(),
			exp: "ID: 12, Version: 1, ModificationTime: \"0,0\", ParentID: 2, State: OFFLINE, OfflineReason: \"foo\"",
		},
		{
			desc: dbdesc.NewBuilder(&descpb.DatabaseDescriptor{
				ID:      12,
				Version: 1,
				State:   descpb.DescriptorState_PUBLIC,
			}).BuildCreatedMutable(),
			exp: "ID: 12, Version: 1, IsUncommitted: true, ModificationTime: \"0,0\", State: PUBLIC",
		},
	} {
		t.Run("", func(t *testing.T) {
			var buf redact.StringBuilder
			catalog.FormatSafeDescriptorProperties(&buf, tc.desc)
			redacted := string(buf.RedactableString().Redact())
			require.Equal(t, tc.exp, redacted)
			var m map[string]interface{}
			require.NoError(t, yaml.UnmarshalStrict([]byte("{"+redacted+"}"), &m))
		})
	}
}

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

	tableDesc := tabledesc.NewBuilder(&descpb.TableDescriptor{
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
		checkConstraintIsAsExpected(t, all[0],
			1, descpb.ConstraintToUpdate_PRIMARY_KEY, descpb.ConstraintValidity_Validated)
		checkConstraintIsAsExpected(t, all[1],
			2, descpb.ConstraintToUpdate_CHECK, descpb.ConstraintValidity_Validated)
		checkConstraintIsAsExpected(t, all[2],
			3, descpb.ConstraintToUpdate_CHECK, descpb.ConstraintValidity_Unvalidated)
		checkConstraintIsAsExpected(t, all[3],
			4, descpb.ConstraintToUpdate_UNIQUE, descpb.ConstraintValidity_Validated)
		checkConstraintIsAsExpected(t, all[4],
			5, descpb.ConstraintToUpdate_FOREIGN_KEY, descpb.ConstraintValidity_Validated)
		checkConstraintIsAsExpected(t, all[5],
			6, descpb.ConstraintToUpdate_FOREIGN_KEY, descpb.ConstraintValidity_Validated)
		checkConstraintIsAsExpected(t, all[6],
			7, descpb.ConstraintToUpdate_UNIQUE_WITHOUT_INDEX, descpb.ConstraintValidity_Dropping)
		checkConstraintIsAsExpected(t, all[7],
			8, descpb.ConstraintToUpdate_CHECK, descpb.ConstraintValidity_Validating)
		checkConstraintIsAsExpected(t, all[8],
			9, descpb.ConstraintToUpdate_UNIQUE, descpb.ConstraintValidity_Validating)
	})

	t.Run("test-AllActiveAndInactiveConstraints", func(t *testing.T) {
		allActiveAndInactive := tableDesc.AllActiveAndInactiveConstraints()
		sort.Slice(allActiveAndInactive, func(i, j int) bool {
			return allActiveAndInactive[i].GetConstraintID() < allActiveAndInactive[j].GetConstraintID()
		})
		require.Equal(t, len(allActiveAndInactive), 8)
		checkConstraintIsAsExpected(t, allActiveAndInactive[0],
			1, descpb.ConstraintToUpdate_PRIMARY_KEY, descpb.ConstraintValidity_Validated)
		checkConstraintIsAsExpected(t, allActiveAndInactive[1],
			2, descpb.ConstraintToUpdate_CHECK, descpb.ConstraintValidity_Validated)
		checkConstraintIsAsExpected(t, allActiveAndInactive[2],
			3, descpb.ConstraintToUpdate_CHECK, descpb.ConstraintValidity_Unvalidated)
		checkConstraintIsAsExpected(t, allActiveAndInactive[3],
			4, descpb.ConstraintToUpdate_UNIQUE, descpb.ConstraintValidity_Validated)
		checkConstraintIsAsExpected(t, allActiveAndInactive[4],
			5, descpb.ConstraintToUpdate_FOREIGN_KEY, descpb.ConstraintValidity_Validated)
		checkConstraintIsAsExpected(t, allActiveAndInactive[5],
			6, descpb.ConstraintToUpdate_FOREIGN_KEY, descpb.ConstraintValidity_Validated)
		checkConstraintIsAsExpected(t, allActiveAndInactive[6],
			8, descpb.ConstraintToUpdate_CHECK, descpb.ConstraintValidity_Validating)
		checkConstraintIsAsExpected(t, allActiveAndInactive[7],
			9, descpb.ConstraintToUpdate_UNIQUE, descpb.ConstraintValidity_Validating)
	})

	t.Run("test-AllActiveConstraints", func(t *testing.T) {
		allActive := tableDesc.AllActiveConstraints()
		sort.Slice(allActive, func(i, j int) bool {
			return allActive[i].GetConstraintID() < allActive[j].GetConstraintID()
		})
		require.Equal(t, len(allActive), 5)
		checkConstraintIsAsExpected(t, allActive[0],
			1, descpb.ConstraintToUpdate_PRIMARY_KEY, descpb.ConstraintValidity_Validated)
		checkConstraintIsAsExpected(t, allActive[1],
			2, descpb.ConstraintToUpdate_CHECK, descpb.ConstraintValidity_Validated)
		checkConstraintIsAsExpected(t, allActive[2],
			4, descpb.ConstraintToUpdate_UNIQUE, descpb.ConstraintValidity_Validated)
		checkConstraintIsAsExpected(t, allActive[3],
			5, descpb.ConstraintToUpdate_FOREIGN_KEY, descpb.ConstraintValidity_Validated)
		checkConstraintIsAsExpected(t, allActive[4],
			6, descpb.ConstraintToUpdate_FOREIGN_KEY, descpb.ConstraintValidity_Validated)
	})
}

func checkConstraintIsAsExpected(
	t *testing.T,
	c catalog.Constraint,
	expectedID descpb.ConstraintID,
	expectedType descpb.ConstraintToUpdate_ConstraintType,
	expectedValidity descpb.ConstraintValidity,
) {
	require.Equal(t, expectedID, c.GetConstraintID())
	require.Equal(t, expectedType, c.ConstraintToUpdateDesc().ConstraintType)
	require.Equal(t, expectedValidity, c.GetConstraintValidity())
}
