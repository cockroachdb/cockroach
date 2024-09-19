// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tree_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/lib/pq/oid"
	"github.com/stretchr/testify/require"
)

func TestBuiltinFunctionResolver(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testCases := []struct {
		testName       string
		fnName         tree.UnresolvedName
		expectedSchema string
		expectNoFound  bool
	}{
		{
			testName:      "not found",
			fnName:        tree.UnresolvedName{NumParts: 1, Parts: tree.NameParts{"whathmm", "", "", ""}},
			expectNoFound: true,
		},
		{
			testName:       "default to use pg_catalog schema",
			fnName:         tree.UnresolvedName{NumParts: 1, Parts: tree.NameParts{"lower", "", "", ""}},
			expectedSchema: "pg_catalog",
		},
		{
			testName:       "explicit to use pg_catalog schema",
			fnName:         tree.UnresolvedName{NumParts: 2, Parts: tree.NameParts{"lower", "pg_catalog", "", ""}},
			expectedSchema: "pg_catalog",
		},
		{
			testName:       "explicit to use public schema",
			fnName:         tree.UnresolvedName{NumParts: 2, Parts: tree.NameParts{"st_makeline", "public", "", ""}},
			expectedSchema: "public",
		},
		{
			testName:      "explicit to use public schema but not available",
			fnName:        tree.UnresolvedName{NumParts: 2, Parts: tree.NameParts{"lower", "public", "", ""}},
			expectNoFound: true,
		},
		{
			testName:       "explicit to use crdb_internal",
			fnName:         tree.UnresolvedName{NumParts: 2, Parts: tree.NameParts{"json_to_pb", "crdb_internal", "", ""}},
			expectedSchema: "crdb_internal",
		},
		{
			testName:       "implicit to use crdb_internal",
			fnName:         tree.UnresolvedName{NumParts: 1, Parts: tree.NameParts{"json_to_pb", "", "", ""}},
			expectedSchema: "crdb_internal",
		},
	}

	path := sessiondata.MakeSearchPath([]string{"crdb_internal"})

	for _, tc := range testCases {
		t.Run(tc.testName, func(t *testing.T) {
			fnName, err := tc.fnName.ToRoutineName()
			require.NoError(t, err)
			funcDef, err := tree.GetBuiltinFuncDefinition(fnName, &path)
			require.NoError(t, err)
			if tc.expectNoFound {
				require.Nil(t, funcDef)
				return
			}
			for _, o := range funcDef.Overloads {
				require.Equal(t, tc.expectedSchema, o.Schema)
			}
		})
	}
}

func TestMatchOverload(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	fd := &tree.ResolvedFunctionDefinition{
		Name: "f",
		Overloads: []tree.QualifiedOverload{
			{
				Schema:   "pg_catalog",
				Overload: &tree.Overload{Oid: 1, Type: tree.UDFRoutine, Types: tree.ParamTypes{tree.ParamType{Typ: types.Int}}},
			},
			{
				Schema:   "sc1",
				Overload: &tree.Overload{Oid: 2, Type: tree.UDFRoutine, Types: tree.ParamTypes{tree.ParamType{Typ: types.Int}}},
			},
			{
				Schema:   "sc1",
				Overload: &tree.Overload{Oid: 3, Type: tree.UDFRoutine, Types: tree.ParamTypes{}},
			},
			{
				Schema: "sc2",
				Overload: &tree.Overload{
					Oid:          4,
					Type:         tree.UDFRoutine,
					Types:        tree.ParamTypes{tree.ParamType{Typ: types.Int}},
					DefaultExprs: tree.Exprs{tree.DZero},
				},
			},
			{
				Schema: "sc3",
				Overload: &tree.Overload{Oid: 5, Type: tree.UDFRoutine,
					Types: tree.ParamTypes{
						tree.ParamType{Typ: types.Int2}, tree.ParamType{Typ: types.Int4},
					},
				},
			},
			{
				Schema: "sc1",
				Overload: &tree.Overload{Oid: 6, Type: tree.ProcedureRoutine,
					Types: tree.ParamTypes{
						tree.ParamType{Typ: types.Bool}},
				},
			},
		},
	}

	testCase := []struct {
		testName        string
		argTypes        []*types.T
		explicitSchema  string
		path            []string
		routineType     tree.RoutineType
		expectedOid     oid.Oid
		expectedErr     string
		tryDefaultExprs bool
	}{
		{
			testName:    "nil arg types implicit pg_catalog in path",
			argTypes:    nil,
			path:        []string{"sc1", "sc2"},
			routineType: tree.UDFRoutine,
			expectedOid: 1,
		},
		{
			testName:    "match a procedure",
			argTypes:    []*types.T{types.Bool},
			path:        []string{"sc1", "sc2"},
			routineType: tree.ProcedureRoutine,
			expectedOid: 6,
		},
		{
			testName:    "nil arg types explicit pg_catalog in path",
			argTypes:    nil,
			path:        []string{"sc2", "sc1", "pg_catalog"},
			routineType: tree.UDFRoutine,
			expectedOid: 4,
		},
		{
			testName:    "nil arg types explicit pg_catalog in path not unique",
			argTypes:    nil,
			path:        []string{"sc1", "sc2", "pg_catalog"},
			routineType: tree.UDFRoutine,
			expectedErr: `function name "f" is not unique`,
		},
		{
			testName:    "int arg type implicit pg_catalog in path",
			argTypes:    []*types.T{types.Int},
			path:        []string{"sc1", "sc2"},
			routineType: tree.UDFRoutine,
			expectedOid: 1,
		},
		{
			testName:    "empty arg type implicit pg_catalog in path",
			argTypes:    []*types.T{},
			path:        []string{"sc1", "sc2"},
			routineType: tree.UDFRoutine,
			expectedOid: 3,
		},
		{
			testName:    "int arg types explicit pg_catalog in path",
			argTypes:    []*types.T{types.Int},
			path:        []string{"sc1", "sc2", "pg_catalog"},
			routineType: tree.UDFRoutine,
			expectedOid: 2,
		},
		{
			testName:    "int arg types explicit pg_catalog in path schema order matters",
			argTypes:    []*types.T{types.Int},
			path:        []string{"sc2", "sc1", "pg_catalog"},
			routineType: tree.UDFRoutine,
			expectedOid: 4,
		},
		{
			testName:       "explicit schema in search path",
			argTypes:       []*types.T{types.Int},
			explicitSchema: "sc2",
			path:           []string{"sc2", "sc1", "pg_catalog"},
			routineType:    tree.UDFRoutine,
			expectedOid:    4,
		},
		{
			testName:       "explicit schema not in search path",
			argTypes:       []*types.T{types.Int},
			explicitSchema: "sc2",
			path:           []string{"sc1", "pg_catalog"},
			routineType:    tree.UDFRoutine,
			expectedOid:    4,
		},
		{
			testName:       "explicit schema not in search path not found",
			argTypes:       []*types.T{types.Int},
			explicitSchema: "sc3",
			path:           []string{"s2", "sc1", "pg_catalog"},
			routineType:    tree.UDFRoutine,
			expectedErr:    `function f\(int\) does not exist`,
		},
		{
			testName:    "signature not found",
			argTypes:    []*types.T{types.String},
			path:        []string{"sc2", "sc1", "pg_catalog"},
			routineType: tree.UDFRoutine,
			expectedErr: `function f\(string\) does not exist`,
		},
		{
			testName:    "multiple parameters exact match on same family type",
			argTypes:    []*types.T{types.Int2, types.Int4},
			path:        []string{"sc3", "sc2", "pg_catalog"},
			routineType: tree.UDFRoutine,
			expectedOid: 5,
		},
		{
			testName:    "multiple parameters exact match on same family type not exists",
			argTypes:    []*types.T{types.Int, types.Int4},
			path:        []string{"sc3", "sc2", "pg_catalog"},
			routineType: tree.UDFRoutine,
			expectedErr: `function f\(int,int4\) does not exist`,
		},
		{
			testName:        "using DEFAULT expr",
			argTypes:        []*types.T{},
			explicitSchema:  "sc2",
			path:            []string{"sc2", "sc1", "pg_catalog"},
			routineType:     tree.UDFRoutine,
			expectedOid:     4,
			tryDefaultExprs: true,
		},
		{
			testName:        "not using DEFAULT expr",
			argTypes:        []*types.T{},
			explicitSchema:  "sc2",
			path:            []string{"sc2", "sc1", "pg_catalog"},
			routineType:     tree.UDFRoutine,
			expectedErr:     `function f\(\) does not exist`,
			tryDefaultExprs: false,
		},
	}

	for _, tc := range testCase {
		t.Run(tc.testName, func(t *testing.T) {
			routineObj := tree.RoutineObj{
				FuncName: tree.MakeQualifiedRoutineName("", tc.explicitSchema, "f"),
			}
			if tc.argTypes != nil {
				routineObj.Params = make(tree.RoutineParams, 0, len(tc.argTypes))
				for _, typ := range tc.argTypes {
					routineObj.Params = append(routineObj.Params, tree.RoutineParam{
						Type:  typ,
						Class: tree.RoutineParamIn,
					})
				}
			}
			path := sessiondata.MakeSearchPath(tc.path)
			ol, err := fd.MatchOverload(
				context.Background(),
				nil, /* typeRes */
				&routineObj,
				&path,
				tc.routineType,
				false, /* inDropContext */
				tc.tryDefaultExprs,
			)
			if tc.expectedErr != "" {
				require.Error(t, err)
				require.Regexp(t, tc.expectedErr, err.Error())
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.expectedOid, ol.Oid)
		})
	}
}
