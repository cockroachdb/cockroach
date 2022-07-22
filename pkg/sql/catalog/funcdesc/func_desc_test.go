// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package funcdesc_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/bootstrap"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/funcdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/nstree"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestValidateFuncDesc(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	const (
		dbID                      = 1000
		schemaID                  = dbID + 1
		typeID                    = dbID + 2
		tableID                   = dbID + 3
		schemaWithFuncRefID       = dbID + 4
		tableWithFuncBackRefID    = dbID + 5
		tableWithFuncForwardRefID = dbID + 6
		typeWithFuncRefID         = dbID + 7
	)
	funcDescID := descpb.ID(bootstrap.TestingUserDescID(0))

	var cb nstree.MutableCatalog
	cb.UpsertDescriptorEntry(dbdesc.NewBuilder(&descpb.DatabaseDescriptor{
		Name: "db",
		ID:   dbID,
	}).BuildImmutable())
	cb.UpsertDescriptorEntry(schemadesc.NewBuilder(&descpb.SchemaDescriptor{
		ID:       schemaID,
		ParentID: dbID,
		Name:     "schema",
	}).BuildImmutable())
	cb.UpsertDescriptorEntry(typedesc.NewBuilder(&descpb.TypeDescriptor{
		ID:   typeID,
		Name: "type",
	}).BuildImmutable())
	cb.UpsertDescriptorEntry(tabledesc.NewBuilder(&descpb.TableDescriptor{
		ID:   tableID,
		Name: "tbl",
	}).BuildImmutable())
	cb.UpsertDescriptorEntry(schemadesc.NewBuilder(&descpb.SchemaDescriptor{
		ID:       schemaWithFuncRefID,
		ParentID: dbID,
		Name:     "schema",
		Functions: map[string]descpb.SchemaDescriptor_Function{
			"f": {Overloads: []descpb.SchemaDescriptor_FunctionOverload{{ID: funcDescID}}},
		},
	}).BuildImmutable())
	cb.UpsertDescriptorEntry(typedesc.NewBuilder(&descpb.TypeDescriptor{
		ID:                       typeWithFuncRefID,
		Name:                     "type",
		ReferencingDescriptorIDs: []descpb.ID{funcDescID},
	}).BuildImmutable())
	cb.UpsertDescriptorEntry(tabledesc.NewBuilder(&descpb.TableDescriptor{
		ID:           tableWithFuncBackRefID,
		Name:         "tbl",
		DependedOnBy: []descpb.TableDescriptor_Reference{{ID: funcDescID}},
	}).BuildImmutable())
	cb.UpsertDescriptorEntry(tabledesc.NewBuilder(&descpb.TableDescriptor{
		ID:        tableWithFuncForwardRefID,
		Name:      "tbl",
		DependsOn: []descpb.ID{funcDescID},
	}).BuildImmutable())

	defaultPrivileges := catpb.NewBasePrivilegeDescriptor(username.RootUserName())
	invalidPrivileges := catpb.NewBasePrivilegeDescriptor(username.RootUserName())
	// Make the PrivilegeDescriptor invalid by granting SELECT to a function.
	invalidPrivileges.Grant(username.TestUserName(), privilege.List{privilege.SELECT}, false)

	testData := []struct {
		err  string
		desc descpb.FunctionDescriptor
	}{
		{
			"empty function name",
			descpb.FunctionDescriptor{},
		},
		{
			"invalid ID 0",
			descpb.FunctionDescriptor{
				Name: "f",
			},
		},
		{
			"invalid parentID 0",
			descpb.FunctionDescriptor{
				Name: "f",
				ID:   funcDescID,
			},
		},
		{
			"invalid parentSchemaID 0",
			descpb.FunctionDescriptor{
				Name:     "f",
				ID:       funcDescID,
				ParentID: dbID,
			},
		},
		{
			"privileges not set",
			descpb.FunctionDescriptor{
				Name:           "f",
				ID:             funcDescID,
				ParentID:       dbID,
				ParentSchemaID: schemaID,
			},
		},
		{
			"user testuser must not have SELECT privileges on function \"f\"",
			descpb.FunctionDescriptor{
				Name:           "f",
				ID:             funcDescID,
				ParentID:       dbID,
				ParentSchemaID: schemaID,
				Privileges:     invalidPrivileges,
			},
		},
		{
			"return type not set",
			descpb.FunctionDescriptor{
				Name:           "f",
				ID:             funcDescID,
				ParentID:       dbID,
				ParentSchemaID: schemaID,
				Privileges:     defaultPrivileges,
			},
		},
		{
			"type not set for arg 0",
			descpb.FunctionDescriptor{
				Name:           "f",
				ID:             funcDescID,
				ParentID:       dbID,
				ParentSchemaID: schemaID,
				Privileges:     defaultPrivileges,
				ReturnType: descpb.FunctionDescriptor_ReturnType{
					Type: types.Int,
				},
				Args: []descpb.FunctionDescriptor_Argument{
					{
						Name: "arg1",
					},
				},
			},
		},
		{
			"leakproof is set for non-immutable function",
			descpb.FunctionDescriptor{
				Name:           "f",
				ID:             funcDescID,
				ParentID:       dbID,
				ParentSchemaID: schemaID,
				Privileges:     defaultPrivileges,
				ReturnType: descpb.FunctionDescriptor_ReturnType{
					Type: types.Int,
				},
				Args: []descpb.FunctionDescriptor_Argument{
					{
						Name: "arg1",
						Type: types.Int,
					},
				},
				LeakProof:  true,
				Volatility: catpb.Function_VOLATILE,
			},
		},
		{
			"invalid relation id 0 in depended-on-by references #0",
			descpb.FunctionDescriptor{
				Name:           "f",
				ID:             funcDescID,
				ParentID:       dbID,
				ParentSchemaID: schemaID,
				Privileges:     defaultPrivileges,
				ReturnType: descpb.FunctionDescriptor_ReturnType{
					Type: types.Int,
				},
				Args: []descpb.FunctionDescriptor_Argument{
					{
						Name: "arg1",
						Type: types.Int,
					},
				},
				LeakProof:  true,
				Volatility: catpb.Function_IMMUTABLE,
				DependedOnBy: []descpb.FunctionDescriptor_Reference{
					{},
				},
			},
		},
		{
			"invalid relation id 0 in depends-on references #0",
			descpb.FunctionDescriptor{
				Name:           "f",
				ID:             funcDescID,
				ParentID:       dbID,
				ParentSchemaID: schemaID,
				Privileges:     defaultPrivileges,
				ReturnType: descpb.FunctionDescriptor_ReturnType{
					Type: types.Int,
				},
				Args: []descpb.FunctionDescriptor_Argument{
					{
						Name: "arg1",
						Type: types.Int,
					},
				},
				LeakProof:  true,
				Volatility: catpb.Function_IMMUTABLE,
				DependedOnBy: []descpb.FunctionDescriptor_Reference{
					{ID: tableID},
				},
				DependsOn: []descpb.ID{0},
			},
		},
		{
			"invalid type id 0 in depends-on-types references #0",
			descpb.FunctionDescriptor{
				Name:           "f",
				ID:             funcDescID,
				ParentID:       dbID,
				ParentSchemaID: schemaID,
				Privileges:     defaultPrivileges,
				ReturnType: descpb.FunctionDescriptor_ReturnType{
					Type: types.Int,
				},
				Args: []descpb.FunctionDescriptor_Argument{
					{
						Name: "arg1",
						Type: types.Int,
					},
				},
				LeakProof:  true,
				Volatility: catpb.Function_IMMUTABLE,
				DependedOnBy: []descpb.FunctionDescriptor_Reference{
					{ID: tableID},
				},
				DependsOn:      []descpb.ID{tableID},
				DependsOnTypes: []descpb.ID{0},
			},
		},
		{
			"function does not exist in schema \"schema\" (1001)",
			descpb.FunctionDescriptor{
				Name:           "f",
				ID:             funcDescID,
				ParentID:       dbID,
				ParentSchemaID: schemaID,
				Privileges:     defaultPrivileges,
				ReturnType: descpb.FunctionDescriptor_ReturnType{
					Type: types.Int,
				},
				Args: []descpb.FunctionDescriptor_Argument{
					{
						Name: "arg1",
						Type: types.Int,
					},
				},
				LeakProof:  true,
				Volatility: catpb.Function_IMMUTABLE,
				DependedOnBy: []descpb.FunctionDescriptor_Reference{
					{ID: tableID},
				},
				DependsOn:      []descpb.ID{tableID},
				DependsOnTypes: []descpb.ID{typeID},
			},
		},
		{
			"depends-on relation \"tbl\" (1003) has no corresponding depended-on-by back reference",
			descpb.FunctionDescriptor{
				Name:           "f",
				ID:             funcDescID,
				ParentID:       dbID,
				ParentSchemaID: schemaWithFuncRefID,
				Privileges:     defaultPrivileges,
				ReturnType: descpb.FunctionDescriptor_ReturnType{
					Type: types.Int,
				},
				Args: []descpb.FunctionDescriptor_Argument{
					{
						Name: "arg1",
						Type: types.Int,
					},
				},
				LeakProof:  true,
				Volatility: catpb.Function_IMMUTABLE,
				DependedOnBy: []descpb.FunctionDescriptor_Reference{
					{ID: tableID},
				},
				DependsOn:      []descpb.ID{tableID},
				DependsOnTypes: []descpb.ID{typeID},
			},
		},
		{
			"depends-on type \"type\" (1002) has no corresponding referencing-descriptor back references",
			descpb.FunctionDescriptor{
				Name:           "f",
				ID:             funcDescID,
				ParentID:       dbID,
				ParentSchemaID: schemaWithFuncRefID,
				Privileges:     defaultPrivileges,
				ReturnType: descpb.FunctionDescriptor_ReturnType{
					Type: types.Int,
				},
				Args: []descpb.FunctionDescriptor_Argument{
					{
						Name: "arg1",
						Type: types.Int,
					},
				},
				LeakProof:  true,
				Volatility: catpb.Function_IMMUTABLE,
				DependedOnBy: []descpb.FunctionDescriptor_Reference{
					{ID: tableID},
				},
				DependsOn:      []descpb.ID{tableWithFuncBackRefID},
				DependsOnTypes: []descpb.ID{typeID},
			},
		},
		{
			"depended-on-by table \"tbl\" (1003) has no corresponding depends-on forward reference",
			descpb.FunctionDescriptor{
				Name:           "f",
				ID:             funcDescID,
				ParentID:       dbID,
				ParentSchemaID: schemaWithFuncRefID,
				Privileges:     defaultPrivileges,
				ReturnType: descpb.FunctionDescriptor_ReturnType{
					Type: types.Int,
				},
				Args: []descpb.FunctionDescriptor_Argument{
					{
						Name: "arg1",
						Type: types.Int,
					},
				},
				LeakProof:  true,
				Volatility: catpb.Function_IMMUTABLE,
				DependedOnBy: []descpb.FunctionDescriptor_Reference{
					{ID: tableID},
				},
				DependsOn:      []descpb.ID{tableWithFuncBackRefID},
				DependsOnTypes: []descpb.ID{typeWithFuncRefID},
			},
		},
		{
			"depended-on-by relation \"tbl\" (1006) does not have a column with ID 1",
			descpb.FunctionDescriptor{
				Name:           "f",
				ID:             funcDescID,
				ParentID:       dbID,
				ParentSchemaID: schemaWithFuncRefID,
				Privileges:     defaultPrivileges,
				ReturnType: descpb.FunctionDescriptor_ReturnType{
					Type: types.Int,
				},
				Args: []descpb.FunctionDescriptor_Argument{
					{
						Name: "arg1",
						Type: types.Int,
					},
				},
				LeakProof:  true,
				Volatility: catpb.Function_IMMUTABLE,
				DependedOnBy: []descpb.FunctionDescriptor_Reference{
					{ID: tableWithFuncForwardRefID, ColumnIDs: []descpb.ColumnID{1}},
				},
				DependsOn:      []descpb.ID{tableWithFuncBackRefID},
				DependsOnTypes: []descpb.ID{typeWithFuncRefID},
			},
		},
		{
			"depended-on-by relation \"tbl\" (1006) does not have an index with ID 1",
			descpb.FunctionDescriptor{
				Name:           "f",
				ID:             funcDescID,
				ParentID:       dbID,
				ParentSchemaID: schemaWithFuncRefID,
				Privileges:     defaultPrivileges,
				ReturnType: descpb.FunctionDescriptor_ReturnType{
					Type: types.Int,
				},
				Args: []descpb.FunctionDescriptor_Argument{
					{
						Name: "arg1",
						Type: types.Int,
					},
				},
				LeakProof:  true,
				Volatility: catpb.Function_IMMUTABLE,
				DependedOnBy: []descpb.FunctionDescriptor_Reference{
					{ID: tableWithFuncForwardRefID, IndexIDs: []descpb.IndexID{1}},
				},
				DependsOn:      []descpb.ID{tableWithFuncBackRefID},
				DependsOnTypes: []descpb.ID{typeWithFuncRefID},
			},
		},
		{
			"depended-on-by relation \"tbl\" (1006) does not have a constraint with ID 1",
			descpb.FunctionDescriptor{
				Name:           "f",
				ID:             funcDescID,
				ParentID:       dbID,
				ParentSchemaID: schemaWithFuncRefID,
				Privileges:     defaultPrivileges,
				ReturnType: descpb.FunctionDescriptor_ReturnType{
					Type: types.Int,
				},
				Args: []descpb.FunctionDescriptor_Argument{
					{
						Name: "arg1",
						Type: types.Int,
					},
				},
				LeakProof:  true,
				Volatility: catpb.Function_IMMUTABLE,
				DependedOnBy: []descpb.FunctionDescriptor_Reference{
					{ID: tableWithFuncForwardRefID, ConstraintIDs: []descpb.ConstraintID{1}},
				},
				DependsOn:      []descpb.ID{tableWithFuncBackRefID},
				DependsOnTypes: []descpb.ID{typeWithFuncRefID},
			},
		},
	}

	for i, test := range testData {
		desc := funcdesc.NewBuilder(&test.desc).BuildImmutable()
		expectedErr := fmt.Sprintf("%s %q (%d): %s", desc.DescriptorType(), desc.GetName(), desc.GetID(), test.err)
		ve := cb.Validate(ctx, clusterversion.TestingClusterVersion, catalog.NoValidationTelemetry, catalog.ValidationLevelCrossReferences, desc)
		if err := ve.CombinedError(); err == nil {
			t.Errorf("#%d expected err: %s, but found nil: %v", i, expectedErr, test.desc)
		} else if expectedErr != err.Error() {
			t.Errorf("#%d expected err: %s, but found %s", i, expectedErr, err)
		}
	}
}
