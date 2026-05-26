// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tree_test

import (
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
		testName    string
		options     tree.RoutineOptions
		isProc      bool
		expectedErr string
	}{
		{
			testName: "no conflict",
			options: tree.RoutineOptions{
				tree.RoutineVolatile, tree.RoutineLeakproof(true), tree.RoutineCalledOnNullInput, tree.RoutineLangSQL, tree.RoutineBodyStr("hi"),
			},
			expectedErr: "",
		},
		{
			testName: "volatility conflict",
			options: tree.RoutineOptions{
				tree.RoutineVolatile, tree.RoutineStable,
			},
			expectedErr: "STABLE: conflicting or redundant options",
		},
		{
			testName: "null input behavior conflict 1",
			options: tree.RoutineOptions{
				tree.RoutineCalledOnNullInput, tree.RoutineReturnsNullOnNullInput,
			},
			expectedErr: "RETURNS NULL ON NULL INPUT: conflicting or redundant options",
		},
		{
			testName: "null input behavior conflict 2",
			options: tree.RoutineOptions{
				tree.RoutineCalledOnNullInput, tree.RoutineStrict,
			},
			expectedErr: "STRICT: conflicting or redundant options",
		},
		{
			testName: "leakproof conflict",
			options: tree.RoutineOptions{
				tree.RoutineLeakproof(true), tree.RoutineLeakproof(false),
			},
			expectedErr: "NOT LEAKPROOF: conflicting or redundant options",
		},
		{
			testName: "language conflict",
			options: tree.RoutineOptions{
				tree.RoutineLangSQL, tree.RoutineLangSQL,
			},
			expectedErr: "LANGUAGE SQL: conflicting or redundant options",
		},
		{
			testName: "function body conflict",
			options: tree.RoutineOptions{
				tree.RoutineBodyStr("queries"), tree.RoutineBodyStr("others"),
			},
			expectedErr: "AS $$others$$: conflicting or redundant options",
		},
		{
			testName: "proc volatility",
			options: tree.RoutineOptions{
				tree.RoutineVolatile,
			},
			isProc:      true,
			expectedErr: "volatility attribute not allowed in procedure definition",
		},
		{
			testName: "proc leakproof",
			options: tree.RoutineOptions{
				tree.RoutineLeakproof(true),
			},
			isProc:      true,
			expectedErr: "leakproof attribute not allowed in procedure definition",
		},
		{
			testName: "proc null input",
			options: tree.RoutineOptions{
				tree.RoutineStrict,
			},
			isProc:      true,
			expectedErr: "null input attribute not allowed in procedure definition",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.testName, func(t *testing.T) {
			err := tree.ValidateRoutineOptions(tc.options, tc.isProc)
			if tc.expectedErr == "" {
				require.NoError(t, err)
				return
			}
			require.Equal(t, tc.expectedErr, err.Error())
		})
	}
}
