// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/stretchr/testify/require"
)

func TestParseQueryXML(t *testing.T) {
	expectedDataMap := map[string][]string{
		"small": {
			"//pkg/sql/sem/eval/cast_test:cast_test_test",
			"//pkg/sql/sem/eval/eval_test:eval_test_test",
		},
		"medium": {
			"//pkg/sql/sem/builtins:builtins_test",
			"//pkg/sql/sem/builtins/pgformat:pgformat_test",
			"//pkg/sql/sem/tree:tree_test",
		},
		"large": {
			"//pkg/sql/sem/cast:cast_test",
		},
		"enormous": {
			"//pkg/sql/sem/eval:eval_test",
			"//pkg/sql/sem/normalize:normalize_test",
		},
	}
	xmlData, err := os.ReadFile(filepath.Join(datapathutils.TestDataPath(t, "TestParseQueryXML"), "tc1.xml"))
	require.NoError(t, err)
	sizeToTargets, err := parseQueryXML(xmlData)
	require.NoError(t, err)
	for size := range expectedDataMap {
		require.ElementsMatch(t, expectedDataMap[size], sizeToTargets[size])
	}
}

func TestExcludeReallyEnormousTests(t *testing.T) {
	for _, tc := range []struct {
		in  []string
		out []string
	}{
		{
			in: []string{
				"//pkg/jobs:jobs_test",
				"//pkg/sql/sem/eval:eval_test",
				"//pkg/sql/sqlitelogictest:sqlitelogictest_test",
			},
			out: []string{
				"//pkg/jobs:jobs_test",
				"//pkg/sql/sem/eval:eval_test",
			},
		},
		{
			in: []string{
				"//pkg/sql/colexec:colexec_test",
				"//pkg/sql/sem/eval:eval_test",
			},
			out: []string{
				"//pkg/sql/colexec:colexec_test",
				"//pkg/sql/sem/eval:eval_test",
			},
		},
		{
			in: []string{
				"//pkg/ccl/sqlitelogictestccl:sqlitelogictestccl_test",
				"//pkg/sql/sqlitelogictest:sqlitelogictest_test",
			},
			out: []string{},
		},
		{
			in: []string{
				"//pkg/ccl/sqlitelogictestccl:sqlitelogictestccl_test",
				"//pkg/jobs:jobs_test",
				"//pkg/sql/colexec:colexec_test",
				"//pkg/sql/sem/eval:eval_test",
				"//pkg/sql/sqlitelogictest:sqlitelogictest_test",
			},
			out: []string{
				"//pkg/jobs:jobs_test",
				"//pkg/sql/colexec:colexec_test",
				"//pkg/sql/sem/eval:eval_test",
			},
		},
	} {
		require.Equal(t, tc.out, excludeReallyEnormousTargets(tc.in))
	}
}
