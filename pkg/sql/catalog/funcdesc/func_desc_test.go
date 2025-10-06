// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package funcdesc_test

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
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
		viewID                    = dbID + 8
		tableWithBadConstraint    = dbID + 9
		tableWithGoodConstraint   = dbID + 10
		tableWithBadColumn        = dbID + 11
		tableWIthGoodColumn       = dbID + 12
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
	cb.UpsertDescriptor(tabledesc.NewBuilder(&descpb.TableDescriptor{
		ID:        viewID,
		Name:      "v",
		ViewQuery: "some query",
	}).BuildImmutable())
	cb.UpsertDescriptor(tabledesc.NewBuilder(&descpb.TableDescriptor{
		ID:   tableWithBadConstraint,
		Name: "tbl_bad_constraint",
		Checks: []*descpb.TableDescriptor_CheckConstraint{
			{
				Expr:         "[FUNCTION 100101] ()",
				ConstraintID: 1,
			},
		},
	}).BuildImmutable())
	cb.UpsertDescriptor(tabledesc.NewBuilder(&descpb.TableDescriptor{
		ID:   tableWithGoodConstraint,
		Name: "tbl_good_constraint",
		Checks: []*descpb.TableDescriptor_CheckConstraint{
			{
				Expr:         "[FUNCTION 100100] ()",
				ConstraintID: 1,
			},
		},
	}).BuildImmutable())
	cb.UpsertDescriptor(tabledesc.NewBuilder(&descpb.TableDescriptor{
		ID:   tableWithBadColumn,
		Name: "tbl_bad_col",
		Columns: []descpb.ColumnDescriptor{
			{
				ID:              1,
				UsesFunctionIds: []descpb.ID{101},
			},
		},
	}).BuildImmutable())
	cb.UpsertDescriptor(tabledesc.NewBuilder(&descpb.TableDescriptor{
		ID:   tableWIthGoodColumn,
		Name: "tbl_good_col",
		Columns: []descpb.ColumnDescriptor{
			{
				ID:              1,
				UsesFunctionIds: []descpb.ID{100},
			},
		},
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
			"user testuser must not have [SELECT] privileges on routine \"f\"",
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
			"leak proof function must be immutable, but got volatility: VOLATILE",
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
			"depended-on-by view \"v\" (1008) has no corresponding depends-on forward reference",
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
					{ID: viewID},
				},
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
		{
			`constraint 1 in depended-on-by relation "tbl_bad_constraint" (1009) does not have reference to function "f" (100)`,
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
					{ID: tableWithBadConstraint, ConstraintIDs: []descpb.ConstraintID{1}},
				},
				DependsOn:      []descpb.ID{tableWithFuncBackRefID},
				DependsOnTypes: []descpb.ID{typeWithFuncRefID},
			},
		},
		{
			``,
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
					{ID: tableWithGoodConstraint, ConstraintIDs: []descpb.ConstraintID{1}},
				},
				DependsOn:      []descpb.ID{tableWithFuncBackRefID},
				DependsOnTypes: []descpb.ID{typeWithFuncRefID},
			},
		},
		{
			`column 1 in depended-on-by relation "tbl_bad_col" (1011) does not have reference to function "f" (100)`,
			descpb.FunctionDescriptor{
				Name:           "f",
				ID:             funcDescID,
				ParentID:       dbID,
				ParentSchemaID: schemaWithFuncRefID,
				Privileges:     defaultPrivileges,
				ReturnType: descpb.FunctionDescriptor_ReturnType{
					Type: types.Int,
				},
				Volatility: catpb.Function_IMMUTABLE,
				DependedOnBy: []descpb.FunctionDescriptor_Reference{
					{ID: tableWithBadColumn, ColumnIDs: []descpb.ColumnID{1}},
				},
				DependsOn:      []descpb.ID{tableWithFuncBackRefID},
				DependsOnTypes: []descpb.ID{typeWithFuncRefID},
			},
		},
		{
			``,
			descpb.FunctionDescriptor{
				Name:           "f",
				ID:             funcDescID,
				ParentID:       dbID,
				ParentSchemaID: schemaWithFuncRefID,
				Privileges:     defaultPrivileges,
				ReturnType: descpb.FunctionDescriptor_ReturnType{
					Type: types.Int,
				},
				Volatility: catpb.Function_IMMUTABLE,
				DependedOnBy: []descpb.FunctionDescriptor_Reference{
					{ID: tableWIthGoodColumn, ColumnIDs: []descpb.ColumnID{1}},
				},
				DependsOn:      []descpb.ID{tableWithFuncBackRefID},
				DependsOnTypes: []descpb.ID{typeWithFuncRefID},
			},
		},
	}

	for i, test := range testData {
		desc := funcdesc.NewBuilder(&test.desc).BuildImmutable()
		var expectedErr string
		if test.err != "" {
			expectedErr = fmt.Sprintf("%s %q (%d): %s", desc.DescriptorType(), desc.GetName(), desc.GetID(), test.err)
		}
		ve := cb.Validate(ctx, clusterversion.TestingClusterVersion, catalog.NoValidationTelemetry, validate.Write, desc)
		err := ve.CombinedError()
		if expectedErr == "" {
			if err != nil {
				t.Errorf("#%d expected no err, but found %s", i, err)
			}
		} else {
			if err == nil {
				t.Errorf("#%d expected err: %s, but found nil: %v", i, expectedErr, test.desc)
			} else if expectedErr != err.Error() {
				t.Errorf("#%d expected err: %s, but found %s", i, expectedErr, err)
			}
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
				Lang:              catpb.Function_SQL,
			},
			expected: tree.Overload{
				Oid: oid.Oid(100001),
				Types: tree.ParamTypes{
					{Name: "arg1", Typ: types.Int},
				},
				ReturnType: tree.FixedReturnType(types.Int),
				Class:      tree.GeneratorClass,
				Volatility: volatility.Leakproof,
				Body:       "ANY QUERIES",
				Type:       tree.UDFRoutine,
				Language:   tree.RoutineLangSQL,
				RoutineParams: tree.RoutineParams{
					{Name: "arg1", Type: types.Int},
				},
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
				Lang:              catpb.Function_SQL,
			},
			expected: tree.Overload{
				Oid: oid.Oid(100001),
				Types: tree.ParamTypes{
					{Name: "arg1", Typ: types.Int},
				},
				ReturnType: tree.FixedReturnType(types.Int),
				Volatility: volatility.Leakproof,
				Body:       "ANY QUERIES",
				Type:       tree.UDFRoutine,
				Language:   tree.RoutineLangSQL,
				RoutineParams: tree.RoutineParams{
					{Name: "arg1", Type: types.Int},
				},
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
				Lang:              catpb.Function_SQL,
			},
			expected: tree.Overload{
				Oid: oid.Oid(100001),
				Types: tree.ParamTypes{
					{Name: "arg1", Typ: types.Int},
				},
				ReturnType: tree.FixedReturnType(types.Int),
				Class:      tree.GeneratorClass,
				Volatility: volatility.Stable,
				Body:       "ANY QUERIES",
				Type:       tree.UDFRoutine,
				Language:   tree.RoutineLangSQL,
				RoutineParams: tree.RoutineParams{
					{Name: "arg1", Type: types.Int},
				},
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
				Lang:              catpb.Function_SQL,
			},
			expected: tree.Overload{
				Oid: oid.Oid(100001),
				Types: tree.ParamTypes{
					{Name: "arg1", Typ: types.Int},
				},
				ReturnType:        tree.FixedReturnType(types.Int),
				Class:             tree.GeneratorClass,
				Volatility:        volatility.Leakproof,
				Body:              "ANY QUERIES",
				Type:              tree.UDFRoutine,
				CalledOnNullInput: true,
				Language:          tree.RoutineLangSQL,
				RoutineParams: tree.RoutineParams{
					{Name: "arg1", Type: types.Int},
				},
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
				Lang:              catpb.Function_SQL,
			},
			expected: tree.Overload{
				Oid: oid.Oid(100001),
				Types: tree.ParamTypes{
					{Name: "arg1", Typ: types.Int},
				},
				ReturnType: tree.FixedReturnType(types.Int),
				Volatility: volatility.Leakproof,
				Body:       "ANY QUERIES",
				Type:       tree.UDFRoutine,
				Language:   tree.RoutineLangSQL,
				RoutineParams: tree.RoutineParams{
					{Name: "arg1", Type: types.Int},
				},
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

func TestStripDanglingBackReferencesAndRoles(t *testing.T) {
	type testCase struct {
		name                           string
		input, expectedOutput          descpb.FunctionDescriptor
		validIDs                       catalog.DescriptorIDSet
		strippedDanglingBackReferences bool
		strippedNonExistentRoles       bool
	}

	badOwnerPrivilege := catpb.NewBaseDatabasePrivilegeDescriptor(username.MakeSQLUsernameFromPreNormalizedString("dropped_user"))
	goodOwnerPrivilege := catpb.NewBaseDatabasePrivilegeDescriptor(username.AdminRoleName())
	badPrivilege := catpb.NewBaseDatabasePrivilegeDescriptor(username.RootUserName())
	goodPrivilege := catpb.NewBaseDatabasePrivilegeDescriptor(username.RootUserName())
	badPrivilege.Users = append(badPrivilege.Users, catpb.UserPrivileges{
		UserProto: username.TestUserName().EncodeProto(),
	})
	testData := []testCase{
		{
			name: "depended on by",
			input: descpb.FunctionDescriptor{
				Name: "foo",
				ID:   105,
				DependedOnBy: []descpb.FunctionDescriptor_Reference{
					{
						ID:        104,
						ColumnIDs: []descpb.ColumnID{1},
					},
					{
						ID:        12345,
						ColumnIDs: []descpb.ColumnID{1},
					},
				},
				Privileges: badPrivilege,
			},
			expectedOutput: descpb.FunctionDescriptor{
				Name: "foo",
				ID:   105,
				DependedOnBy: []descpb.FunctionDescriptor_Reference{
					{
						ID:        104,
						ColumnIDs: []descpb.ColumnID{1},
					},
				},
				Privileges: goodPrivilege,
			},
			validIDs:                       catalog.MakeDescriptorIDSet(104, 105),
			strippedDanglingBackReferences: true,
			strippedNonExistentRoles:       true,
		},
		{
			name: "missing owner",
			input: descpb.FunctionDescriptor{
				Name: "foo",
				ID:   105,
				DependedOnBy: []descpb.FunctionDescriptor_Reference{
					{
						ID:        104,
						ColumnIDs: []descpb.ColumnID{1},
					},
				},
				Privileges: badOwnerPrivilege,
			},
			expectedOutput: descpb.FunctionDescriptor{
				Name: "foo",
				ID:   105,
				DependedOnBy: []descpb.FunctionDescriptor_Reference{
					{
						ID:        104,
						ColumnIDs: []descpb.ColumnID{1},
					},
				},
				Privileges: goodOwnerPrivilege,
			},
			validIDs:                 catalog.MakeDescriptorIDSet(104, 105),
			strippedNonExistentRoles: true,
		},
	}

	for _, test := range testData {
		t.Run(test.name, func(t *testing.T) {
			b := funcdesc.NewBuilder(&test.input)
			require.NoError(t, b.RunPostDeserializationChanges())
			out := funcdesc.NewBuilder(&test.expectedOutput)
			require.NoError(t, out.RunPostDeserializationChanges())
			require.NoError(t, b.StripDanglingBackReferences(test.validIDs.Contains, func(id jobspb.JobID) bool {
				return false
			}))
			require.NoError(t, b.StripNonExistentRoles(func(role username.SQLUsername) bool {
				return role.IsAdminRole() || role.IsPublicRole() || role.IsRootUser() || role.IsNodeUser()
			}))
			desc := b.BuildCreatedMutableFunction()
			require.Equal(t, test.strippedDanglingBackReferences, desc.GetPostDeserializationChanges().Contains(catalog.StrippedDanglingBackReferences))
			require.Equal(t, test.strippedNonExistentRoles, desc.GetPostDeserializationChanges().Contains(catalog.StrippedNonExistentRoles))
			require.Equal(t, out.BuildCreatedMutableFunction().FuncDesc(), desc.FuncDesc())
		})
	}
}
