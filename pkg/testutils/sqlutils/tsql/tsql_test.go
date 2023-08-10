// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tsql_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils/tsql"
	"github.com/stretchr/testify/require"
)

func TestToString(t *testing.T) {
	testCases := []struct {
		In  tree.NodeFormatter
		Out string
	}{
		{
			In:  tsql.PBToJSON("cockroach.sql.sqlbase.Descriptor", tree.NewUnresolvedName("some_col")),
			Out: `crdb_internal.pb_to_json('cockroach.sql.sqlbase.Descriptor', some_col)`,
		},
		{
			In: tsql.And(
				tsql.Func("levenshtein", "str1", "str2"),
				tsql.Cmp(tree.NewDInt(1), tsql.LT, tree.NewDInt(2)),
			),
			Out: `levenshtein('str1', 'str2') AND (1 < 2)`,
		},
	}

	for _, tc := range testCases {
		require.Equal(t, tc.Out, tsql.ToString(tc.In))
	}
}
