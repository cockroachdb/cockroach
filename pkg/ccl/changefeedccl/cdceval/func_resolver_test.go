// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package cdceval_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdceval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestResolveFunction(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testCases := []struct {
		testName       string
		fnName         tree.UnresolvedName
		expectedSchema string
		err            string
	}{
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
			testName: "explicit to use pg_catalog schema but cdc name",
			fnName:   tree.UnresolvedName{NumParts: 2, Parts: tree.NameParts{"cdc_prev", "pg_catalog", "", ""}},
			err:      "function pg_catalog.cdc_prev does not exist",
		},
		{
			testName:       "cdc name without schema",
			fnName:         tree.UnresolvedName{NumParts: 1, Parts: tree.NameParts{"cdc_prev", "", "", ""}},
			expectedSchema: "public",
		},
		{
			testName:       "uppercase cdc name without schema",
			fnName:         tree.UnresolvedName{NumParts: 1, Parts: tree.NameParts{"cdc_PREV", "", "", ""}},
			expectedSchema: "public",
		},
	}

	path := sessiondata.MakeSearchPath([]string{})
	resolver := cdceval.CDCFunctionResolver{}

	for _, tc := range testCases {
		t.Run(tc.testName, func(t *testing.T) {
			funcDef, err := resolver.ResolveFunction(context.Background(), &tc.fnName, &path)
			if tc.err != "" {
				require.Equal(t, tc.err, err.Error())
				return
			}
			require.NoError(t, err)
			for _, o := range funcDef.Overloads {
				require.Equal(t, tc.expectedSchema, o.Schema)
			}
		})
	}
}
