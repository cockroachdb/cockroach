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
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
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
			fnName, err := tc.fnName.ToFunctionName()
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
