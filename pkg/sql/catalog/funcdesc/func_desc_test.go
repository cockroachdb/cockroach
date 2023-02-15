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
	"strconv"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/bootstrap"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/funcdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/internal/validate"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/nstree"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/volatility"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/lib/pq/oid"
	"github.com/stretchr/testify/require"
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
	cb.UpsertDescriptor(dbdesc.NewBuilder(&descpb.DatabaseDescriptor{
		Name: "db",
		ID:   dbID,
	}).BuildImmutable())
	cb.UpsertDescriptor(schemadesc.NewBuilder(&descpb.SchemaDescriptor{
		ID:       schemaID,
		ParentID: dbID,
		Name:     "schema",
	}).BuildImmutable())
	cb.UpsertDescriptor(typedesc.NewBuilder(&descpb.TypeDescriptor{
		ID:   typeID,
		Name: "type",
	}).BuildImmutable())
	cb.UpsertDescriptor(tabledesc.NewBuilder(&descpb.TableDescriptor{
		ID:   tableID,
		Name: "tbl",
	}).BuildImmutable())
	cb.UpsertDescriptor(schemadesc.NewBuilder(&descpb.SchemaDescriptor{
		ID:       schemaWithFuncRefID,
		ParentID: dbID,
		Name:     "schema",
		Functions: map[string]descpb.SchemaDescriptor_Function{
			"f": {Signatures: []descpb.SchemaDescriptor_FunctionSignature{{ID: funcDescID}}},
		},
	}).BuildImmutable())
	cb.UpsertDescriptor(typedesc.NewBuilder(&descpb.TypeDescriptor{
		ID:                       typeWithFuncRefID,
		Name:                     "type",
		ReferencingDescriptorIDs: []descpb.ID{funcDescID},
	}).BuildImmutable())
	cb.UpsertDescriptor(tabledesc.NewBuilder(&descpb.TableDescriptor{
		ID:           tableWithFuncBackRefID,
		Name:         "tbl",
		DependedOnBy: []descpb.TableDescriptor_Reference{{ID: funcDescID}},
	}).BuildImmutable())
	cb.UpsertDescriptor(tabledesc.NewBuilder(&descpb.TableDescriptor{
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
				Params: []descpb.FunctionDescriptor_Parameter{
					{
						Name: "arg1",
					},
				},
			},
		},
		{
			"cannot set leakproof on function with non-immutable volatility: VOLATILE",
			descpb.FunctionDescriptor{
				Name:           "f",
				ID:             funcDescID,
				ParentID:       dbID,
				ParentSchemaID: schemaID,
				Privileges:     defaultPrivileges,
				ReturnType: descpb.FunctionDescriptor_ReturnType{
					Type: types.Int,
				},
				Params: []descpb.FunctionDescriptor_Parameter{
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
				Params: []descpb.FunctionDescriptor_Parameter{
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
				Params: []descpb.FunctionDescriptor_Parameter{
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
				Params: []descpb.FunctionDescriptor_Parameter{
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
				Params: []descpb.FunctionDescriptor_Parameter{
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
				Params: []descpb.FunctionDescriptor_Parameter{
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
				Params: []descpb.FunctionDescriptor_Parameter{
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
				Params: []descpb.FunctionDescriptor_Parameter{
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
				Params: []descpb.FunctionDescriptor_Parameter{
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
				Params: []descpb.FunctionDescriptor_Parameter{
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
				Params: []descpb.FunctionDescriptor_Parameter{
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
		ve := cb.Validate(ctx, clusterversion.TestingClusterVersion, catalog.NoValidationTelemetry, validate.Write, desc)
		if err := ve.CombinedError(); err == nil {
			t.Errorf("#%d expected err: %s, but found nil: %v", i, expectedErr, test.desc)
		} else if expectedErr != err.Error() {
			t.Errorf("#%d expected err: %s, but found %s", i, expectedErr, err)
		}
	}
}

func TestToOverload(t *testing.T) {
	testCases := []struct {
		desc     descpb.FunctionDescriptor
		expected tree.Overload
		err      string
	}{
		{
			// Test all fields are properly valued.
			desc: descpb.FunctionDescriptor{
				ID:                1,
				Params:            []descpb.FunctionDescriptor_Parameter{{Name: "arg1", Type: types.Int}},
				ReturnType:        descpb.FunctionDescriptor_ReturnType{Type: types.Int, ReturnSet: true},
				LeakProof:         true,
				Volatility:        catpb.Function_IMMUTABLE,
				NullInputBehavior: catpb.Function_RETURNS_NULL_ON_NULL_INPUT,
				FunctionBody:      "ANY QUERIES",
			},
			expected: tree.Overload{
				Oid: oid.Oid(100001),
				Types: tree.ParamTypes{
					{Name: "arg1", Typ: types.Int},
				},
				ReturnType: tree.FixedReturnType(types.Int),
				ReturnSet:  true,
				Class:      tree.GeneratorClass,
				Volatility: volatility.Leakproof,
				Body:       "ANY QUERIES",
				IsUDF:      true,
			},
		},
		{
			// Test ReturnSet matters.
			desc: descpb.FunctionDescriptor{
				ID:                1,
				Params:            []descpb.FunctionDescriptor_Parameter{{Name: "arg1", Type: types.Int}},
				ReturnType:        descpb.FunctionDescriptor_ReturnType{Type: types.Int, ReturnSet: false},
				LeakProof:         true,
				Volatility:        catpb.Function_IMMUTABLE,
				NullInputBehavior: catpb.Function_RETURNS_NULL_ON_NULL_INPUT,
				FunctionBody:      "ANY QUERIES",
			},
			expected: tree.Overload{
				Oid: oid.Oid(100001),
				Types: tree.ParamTypes{
					{Name: "arg1", Typ: types.Int},
				},
				ReturnType: tree.FixedReturnType(types.Int),
				ReturnSet:  false,
				Volatility: volatility.Leakproof,
				Body:       "ANY QUERIES",
				IsUDF:      true,
			},
		},
		{
			// Test Volatility matters.
			desc: descpb.FunctionDescriptor{
				ID:                1,
				Params:            []descpb.FunctionDescriptor_Parameter{{Name: "arg1", Type: types.Int}},
				ReturnType:        descpb.FunctionDescriptor_ReturnType{Type: types.Int, ReturnSet: true},
				LeakProof:         false,
				Volatility:        catpb.Function_STABLE,
				NullInputBehavior: catpb.Function_RETURNS_NULL_ON_NULL_INPUT,
				FunctionBody:      "ANY QUERIES",
			},
			expected: tree.Overload{
				Oid: oid.Oid(100001),
				Types: tree.ParamTypes{
					{Name: "arg1", Typ: types.Int},
				},
				ReturnType: tree.FixedReturnType(types.Int),
				Class:      tree.GeneratorClass,
				ReturnSet:  true,
				Volatility: volatility.Stable,
				Body:       "ANY QUERIES",
				IsUDF:      true,
			},
		},
		{
			// Test CalledOnNullInput matters.
			desc: descpb.FunctionDescriptor{
				ID:                1,
				Params:            []descpb.FunctionDescriptor_Parameter{{Name: "arg1", Type: types.Int}},
				ReturnType:        descpb.FunctionDescriptor_ReturnType{Type: types.Int, ReturnSet: true},
				LeakProof:         true,
				Volatility:        catpb.Function_IMMUTABLE,
				NullInputBehavior: catpb.Function_CALLED_ON_NULL_INPUT,
				FunctionBody:      "ANY QUERIES",
			},
			expected: tree.Overload{
				Oid: oid.Oid(100001),
				Types: tree.ParamTypes{
					{Name: "arg1", Typ: types.Int},
				},
				ReturnType:        tree.FixedReturnType(types.Int),
				Class:             tree.GeneratorClass,
				ReturnSet:         true,
				Volatility:        volatility.Leakproof,
				Body:              "ANY QUERIES",
				IsUDF:             true,
				CalledOnNullInput: true,
			},
		},
		{
			// Test failure on non-immutable but leakproof function.
			desc: descpb.FunctionDescriptor{
				ID:                1,
				Params:            []descpb.FunctionDescriptor_Parameter{{Name: "arg1", Type: types.Int}},
				ReturnType:        descpb.FunctionDescriptor_ReturnType{Type: types.Int, ReturnSet: true},
				LeakProof:         true,
				Volatility:        catpb.Function_STABLE,
				NullInputBehavior: catpb.Function_RETURNS_NULL_ON_NULL_INPUT,
				FunctionBody:      "ANY QUERIES",
			},
			expected: tree.Overload{
				Oid: oid.Oid(100001),
				Types: tree.ParamTypes{
					{Name: "arg1", Typ: types.Int},
				},
				ReturnType: tree.FixedReturnType(types.Int),
				ReturnSet:  true,
				Volatility: volatility.Leakproof,
				Body:       "ANY QUERIES",
				IsUDF:      true,
			},
			err: "function 1 is leakproof but not immutable",
		},
	}

	for i, tc := range testCases {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			desc := funcdesc.NewBuilder(&tc.desc).BuildImmutable().(catalog.FunctionDescriptor)
			overload, err := desc.ToOverload()
			if tc.err == "" {
				require.NoError(t, err)
			} else {
				require.Equal(t, tc.err, err.Error())
				return
			}

			returnType := overload.ReturnType([]tree.TypedExpr{})
			expectedReturnType := tc.expected.ReturnType([]tree.TypedExpr{})
			require.Equal(t, expectedReturnType, returnType)
			// Set ReturnType(which is function) to nil for easier equality check.
			overload.ReturnType = nil
			tc.expected.ReturnType = nil
			require.Equal(t, tc.expected, *overload)
		})
	}
}
