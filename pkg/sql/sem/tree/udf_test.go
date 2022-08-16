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
	"strconv"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestConflictingFunctionOptions(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testCases := []struct {
		options     tree.FunctionOptions
		expectedErr string
	}{
		{
			options: tree.FunctionOptions{
				tree.FunctionVolatile, tree.FunctionLeakproof(true), tree.FunctionCalledOnNullInput, tree.FunctionLangSQL, tree.FunctionBodyStr("hi"),
			},
			expectedErr: "",
		},
		{
			options: tree.FunctionOptions{
				tree.FunctionVolatile, tree.FunctionStable,
			},
			expectedErr: "STABLE: conflicting or redundant options",
		},
		{
			options: tree.FunctionOptions{
				tree.FunctionCalledOnNullInput, tree.FunctionReturnsNullOnNullInput,
			},
			expectedErr: "RETURNS NULL ON NULL INPUT: conflicting or redundant options",
		},
		{
			options: tree.FunctionOptions{
				tree.FunctionCalledOnNullInput, tree.FunctionStrict,
			},
			expectedErr: "STRICT: conflicting or redundant options",
		},
		{
			options: tree.FunctionOptions{
				tree.FunctionLeakproof(true), tree.FunctionLeakproof(false),
			},
			expectedErr: "NOT LEAKPROOF: conflicting or redundant options",
		},
		{
			options: tree.FunctionOptions{
				tree.FunctionLangSQL, tree.FunctionLangSQL,
			},
			expectedErr: "LANGUAGE SQL: conflicting or redundant options",
		},
		{
			options: tree.FunctionOptions{
				tree.FunctionBodyStr("queries"), tree.FunctionBodyStr("others"),
			},
			expectedErr: "AS $$others$$: conflicting or redundant options",
		},
	}

	for i, tc := range testCases {
		t.Run(strconv.Itoa(i+1), func(t *testing.T) {
			err := tree.ValidateFuncOptions(tc.options)
			if tc.expectedErr == "" {
				require.NoError(t, err)
				return
			}
			require.Equal(t, tc.expectedErr, err.Error())
		})
	}
}
