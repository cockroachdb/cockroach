// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tree_test

import (
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
				Overload: &tree.Overload{Oid: 1, IsUDF: false, Types: tree.ParamTypes{tree.ParamType{Typ: types.Int}}},
			},
			{
				Schema:   "sc1",
				Overload: &tree.Overload{Oid: 2, IsUDF: true, Types: tree.ParamTypes{tree.ParamType{Typ: types.Int}}},
			},
			{
				Schema:   "sc1",
				Overload: &tree.Overload{Oid: 3, IsUDF: true, Types: tree.ParamTypes{}},
			},
			{
				Schema:   "sc2",
				Overload: &tree.Overload{Oid: 4, IsUDF: true, Types: tree.ParamTypes{tree.ParamType{Typ: types.Int}}},
			},
			{
				Schema: "sc3",
				Overload: &tree.Overload{Oid: 5, IsUDF: true,
					Types: tree.ParamTypes{
						tree.ParamType{Typ: types.Int2}, tree.ParamType{Typ: types.Int4},
					},
				},
			},
		},
	}

	testCase := []struct {
		testName       string
		argTypes       []*types.T
		explicitSchema string
		path           []string
		expectedOid    oid.Oid
		expectedErr    string
	}{
		{
			testName:    "nil arg types implicit pg_catalog in path",
			argTypes:    nil,
			path:        []string{"sc1", "sc2"},
			expectedOid: 1,
		},
		{
			testName:    "nil arg types explicit pg_catalog in path",
			argTypes:    nil,
			path:        []string{"sc2", "sc1", "pg_catalog"},
			expectedOid: 4,
		},
		{
			testName:    "nil arg types explicit pg_catalog in path not unique",
			argTypes:    nil,
			path:        []string{"sc1", "sc2", "pg_catalog"},
			expectedErr: `function name "f" is not unique`,
		},
		{
			testName:    "int arg type implicit pg_catalog in path",
			argTypes:    []*types.T{types.Int},
			path:        []string{"sc1", "sc2"},
			expectedOid: 1,
		},
		{
			testName:    "empty arg type implicit pg_catalog in path",
			argTypes:    []*types.T{},
			path:        []string{"sc1", "sc2"},
			expectedOid: 3,
		},
		{
			testName:    "int arg types explicit pg_catalog in path",
			argTypes:    []*types.T{types.Int},
			path:        []string{"sc1", "sc2", "pg_catalog"},
			expectedOid: 2,
		},
		{
			testName:    "int arg types explicit pg_catalog in path schema order matters",
			argTypes:    []*types.T{types.Int},
			path:        []string{"sc2", "sc1", "pg_catalog"},
			expectedOid: 4,
		},
		{
			testName:       "explicit schema in search path",
			argTypes:       []*types.T{types.Int},
			explicitSchema: "sc2",
			path:           []string{"sc2", "sc1", "pg_catalog"},
			expectedOid:    4,
		},
		{
			testName:       "explicit schema not in search path",
			argTypes:       []*types.T{types.Int},
			explicitSchema: "sc2",
			path:           []string{"sc1", "pg_catalog"},
			expectedOid:    4,
		},
		{
			testName:       "explicit schema not in search path not found",
			argTypes:       []*types.T{types.Int},
			explicitSchema: "sc3",
			path:           []string{"s2", "sc1", "pg_catalog"},
			expectedErr:    `function f\(int\) does not exist`,
		},
		{
			testName:    "signature not found",
			argTypes:    []*types.T{types.String},
			path:        []string{"sc2", "sc1", "pg_catalog"},
			expectedErr: `function f\(string\) does not exist`,
		},
		{
			testName:    "multiple parameters exact match on same family type",
			argTypes:    []*types.T{types.Int2, types.Int4},
			path:        []string{"sc3", "sc3", "pg_catalog"},
			expectedOid: 5,
		},
		{
			testName:    "multiple parameters exact match on same family type not exists",
			argTypes:    []*types.T{types.Int, types.Int4},
			path:        []string{"sc3", "sc3", "pg_catalog"},
			expectedErr: `function f\(int,int4\) does not exist: function undefined`,
		},
	}

	for _, tc := range testCase {
		t.Run(tc.testName, func(t *testing.T) {
			path := sessiondata.MakeSearchPath(tc.path)
			ol, err := fd.MatchOverload(tc.argTypes, tc.explicitSchema, &path)
			if tc.expectedErr != "" {
				require.Regexp(t, tc.expectedErr, err.Error())
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.expectedOid, ol.Oid)
		})
	}
}
