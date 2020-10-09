// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tabledesc_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/redact"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
)

func TestSafeMessage(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// TODO(ajwerner): Finish testing all of the cases.
	ctx := context.Background()
	for _, tc := range []struct {
		id       descpb.ID
		parentID descpb.ID
		schema   string
		f        func(mutable *tabledesc.Mutable) catalog.TableDescriptor
		exp      string
	}{
		{
			id:       12,
			parentID: 21,
			schema:   "CREATE TABLE foo ()",
			exp: "tabledesc.Mutable: {" +
				"ID: 12, Version: 1, IsUncommitted: true, " +
				"ModificationTime: \"0,0\", " +
				"ParentID: 21, ParentSchemaID: 29, " +
				"State: PUBLIC, " +
				"NextColumnID: 2, " +
				"Columns: [{ID: 1, TypeID: 20, Null: false, Hidden: true, HasDefault: true}], " +
				"NextFamilyID: 1, " +
				"Families: [{ID: 0, Columns: [1]}], " +
				"PrimaryIndex: 1, " +
				"NextIndexID: 2, " +
				"Indexes: [{ID: 1, Unique: true, Columns: [{ID: 1, Dir: ASC}]}]" +
				"}",
		},
		{
			id:       12,
			parentID: 21,
			schema:   "CREATE TABLE foo (i INT PRIMARY KEY, j INT, j_str STRING AS (j::STRING) STORED, INDEX (j_str))",
			exp: `tabledesc.Immutable: {` +
				`ID: 12, Version: 1, ModificationTime: "1.000000000,0", ` +
				`ParentID: 21, ParentSchemaID: 29, State: PUBLIC, ` +
				`NextColumnID: 6, ` +
				`Columns: [` +
				`{ID: 1, TypeID: 20, Null: false}, ` +
				`{ID: 2, TypeID: 20, Null: true}, ` +
				`{ID: 3, TypeID: 25, Null: true, IsComputed: true}` +
				`], ` +
				`NextFamilyID: 1, ` +
				`Families: [{ID: 0, Columns: [1, 2, 3, 5]}], ` +
				`MutationJobs: [` +
				`{MutationID: 1, JobID: 12345}, ` +
				`{MutationID: 2, JobID: 67890}, ` +
				`{MutationID: 3, JobID: 1234}` +
				`], ` +
				`Mutations: [` +
				`{MutationID: 1, Direction: ADD, State: DELETE_AND_WRITE_ONLY, ConstraintType: FOREIGN_KEY, ForeignKey: {OriginTableID: 12, OriginColumns: [2], ReferencedTableID: 2, ReferencedColumnIDs: [3], Validity: Unvalidated, State: ADD, MutationID: 1}}, ` +
				`{MutationID: 2, Direction: ADD, State: DELETE_ONLY, Column: {ID: 5, TypeID: 20, Null: false, State: ADD, MutationID: 2}}, ` +
				`{MutationID: 3, Direction: ADD, State: DELETE_ONLY, ConstraintType: CHECK, NotNullColumn: 2, Check: {Columns: [2], Validity: Unvalidated, State: ADD, MutationID: 3}}, ` +
				`{MutationID: 3, Direction: ADD, State: DELETE_ONLY, Index: {ID: 3, Unique: false, Columns: [{ID: 3, Dir: ASC}, {ID: 2, Dir: DESC}], ExtraColumns: [1], StoreColumns: [5], State: ADD, MutationID: 3}}` +
				`], ` +
				`PrimaryIndex: 1, ` +
				`NextIndexID: 4, ` +
				`Indexes: [` +
				`{ID: 1, Unique: true, Columns: [{ID: 1, Dir: ASC}]}, ` +
				`{ID: 2, Unique: false, Columns: [{ID: 3, Dir: ASC}], ExtraColumns: [1]}` +
				`], ` +
				`Checks: [` +
				`{Columns: [2], Validity: Validated}` +
				`], ` +
				`InboundFKs: [` +
				`{OriginTableID: 2, OriginColumns: [3], ReferencedTableID: 12, ReferencedColumnIDs: [2], Validity: Validated}` +
				`], ` +
				`OutboundFKs: [` +
				`{OriginTableID: 12, OriginColumns: [2], ReferencedTableID: 3, ReferencedColumnIDs: [1], Validity: Validated}` +
				`]}`,
			f: func(mutable *tabledesc.Mutable) catalog.TableDescriptor {
				// Add foreign key constraints and foreign key constraints and
				// various mutations.
				mutable.Checks = append(mutable.Checks, &descpb.TableDescriptor_CheckConstraint{
					Name:      "check",
					Expr:      "j > 0",
					Validity:  descpb.ConstraintValidity_Validated,
					ColumnIDs: []descpb.ColumnID{2},
				})
				mutable.InboundFKs = append(mutable.InboundFKs, descpb.ForeignKeyConstraint{
					Name:                "inbound_fk",
					OriginTableID:       2,
					OriginColumnIDs:     []descpb.ColumnID{3},
					ReferencedColumnIDs: []descpb.ColumnID{2},
					ReferencedTableID:   12,
					Validity:            descpb.ConstraintValidity_Validated,
					OnDelete:            descpb.ForeignKeyReference_CASCADE,
					Match:               descpb.ForeignKeyReference_PARTIAL,
				})
				mutable.OutboundFKs = append(mutable.OutboundFKs, descpb.ForeignKeyConstraint{
					Name:                "outbound_fk",
					OriginTableID:       12,
					OriginColumnIDs:     []descpb.ColumnID{2},
					ReferencedColumnIDs: []descpb.ColumnID{1},
					ReferencedTableID:   3,
					Validity:            descpb.ConstraintValidity_Validated,
					OnDelete:            descpb.ForeignKeyReference_SET_DEFAULT,
					Match:               descpb.ForeignKeyReference_SIMPLE,
				})

				mutable.Mutations = append(mutable.Mutations, descpb.DescriptorMutation{
					State: descpb.DescriptorMutation_DELETE_AND_WRITE_ONLY,
					Descriptor_: &descpb.DescriptorMutation_Constraint{
						Constraint: &descpb.ConstraintToUpdate{
							ConstraintType: descpb.ConstraintToUpdate_FOREIGN_KEY,
							Name:           "outbound_fk_mutation",
							ForeignKey: descpb.ForeignKeyConstraint{
								Name:                "outbound_fk_mutation",
								OriginTableID:       12,
								OriginColumnIDs:     []descpb.ColumnID{2},
								ReferencedTableID:   2,
								ReferencedColumnIDs: []descpb.ColumnID{3},
								Validity:            descpb.ConstraintValidity_Unvalidated,
								OnDelete:            descpb.ForeignKeyReference_SET_NULL,
								Match:               descpb.ForeignKeyReference_FULL,
							},
						},
					},
					Direction:  descpb.DescriptorMutation_ADD,
					MutationID: 1,
				},
					descpb.DescriptorMutation{
						State: descpb.DescriptorMutation_DELETE_ONLY,
						Descriptor_: &descpb.DescriptorMutation_Column{
							Column: &descpb.ColumnDescriptor{
								ID:   5,
								Name: "c",
								Type: types.Int,
							},
						},
						Direction:  descpb.DescriptorMutation_ADD,
						MutationID: 2,
					},
					descpb.DescriptorMutation{
						State: descpb.DescriptorMutation_DELETE_ONLY,
						Descriptor_: &descpb.DescriptorMutation_Constraint{
							Constraint: &descpb.ConstraintToUpdate{
								ConstraintType: descpb.ConstraintToUpdate_CHECK,
								Name:           "check_not_null",
								Check: descpb.TableDescriptor_CheckConstraint{
									Name:                "check_not_null",
									Validity:            descpb.ConstraintValidity_Unvalidated,
									ColumnIDs:           []descpb.ColumnID{2},
									IsNonNullConstraint: true,
								},
								NotNullColumn: 2,
							},
						},
						Direction:  descpb.DescriptorMutation_ADD,
						MutationID: 3,
					},
					descpb.DescriptorMutation{
						State: descpb.DescriptorMutation_DELETE_ONLY,
						Descriptor_: &descpb.DescriptorMutation_Index{
							Index: &descpb.IndexDescriptor{
								ID:             3,
								Name:           "check_not_null",
								ColumnIDs:      []descpb.ColumnID{3, 2},
								ExtraColumnIDs: []descpb.ColumnID{1},
								StoreColumnIDs: []descpb.ColumnID{5},
								ColumnNames:    []string{"j_str", "j"},
								ColumnDirections: []descpb.IndexDescriptor_Direction{
									descpb.IndexDescriptor_ASC,
									descpb.IndexDescriptor_DESC,
								},
								StoreColumnNames: []string{"c"},
							},
						},
						Direction:  descpb.DescriptorMutation_ADD,
						MutationID: 3,
					})
				mutable.MutationJobs = append(mutable.MutationJobs,
					descpb.TableDescriptor_MutationJob{
						MutationID: 1,
						JobID:      12345,
					},
					descpb.TableDescriptor_MutationJob{
						MutationID: 2,
						JobID:      67890,
					},
					descpb.TableDescriptor_MutationJob{
						MutationID: 3,
						JobID:      1234,
					},
				)
				mutable.NextColumnID = 6
				mutable.NextIndexID = 4
				mutable.Families[0].ColumnNames = append(mutable.Families[0].ColumnNames, "c")
				mutable.Families[0].ColumnIDs = append(mutable.Families[0].ColumnIDs, 5)
				mutable.ModificationTime = hlc.Timestamp{WallTime: 1e9}
				mutable.ClusterVersion = *mutable.TableDesc()
				return mutable.ImmutableCopy().(catalog.TableDescriptor)
			},
		},
		{
			id:       12,
			parentID: 21,
			schema:   "CREATE TABLE foo ()",
			exp: "tabledesc.Immutable: {" +
				"ID: 12, Version: 1, " +
				"ModificationTime: \"0,0\", " +
				"ParentID: 21, ParentSchemaID: 29, " +
				"State: PUBLIC, " +
				"NextColumnID: 2, " +
				"Columns: [{ID: 1, TypeID: 20, Null: false, Hidden: true, HasDefault: true}], " +
				"NextFamilyID: 1, " +
				"Families: [{ID: 0, Columns: [1]}], " +
				"PrimaryIndex: 1, " +
				"NextIndexID: 2, " +
				"Indexes: [{ID: 1, Unique: true, Columns: [{ID: 1, Dir: ASC}]}]" +
				"}",
			f: func(mutable *tabledesc.Mutable) catalog.TableDescriptor {
				mutable.ClusterVersion = *mutable.TableDesc()
				return mutable.ImmutableCopy().(catalog.TableDescriptor)
			},
		},
	} {
		t.Run("", func(t *testing.T) {
			desc, err := sql.CreateTestTableDescriptor(ctx, tc.parentID, tc.id, tc.schema, &descpb.PrivilegeDescriptor{})
			require.NoError(t, err)
			var td catalog.TableDescriptor
			if tc.f != nil {
				td = tc.f(desc)
			} else {
				td = desc
			}
			redacted := string(redact.Sprint(td).Redact())
			require.NoError(t, desc.ValidateTable(ctx))
			require.Equal(t, tc.exp, redacted)
			var m map[string]interface{}
			require.NoError(t, yaml.UnmarshalStrict([]byte(redacted), &m), redacted)
		})
	}
}
