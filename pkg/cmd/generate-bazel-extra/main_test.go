// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestExcludeReallyEnormousTests(t *testing.T) {
	for _, tc := range []struct {
		in  []string
		out []string
	}{
		{
			in: []string{
				"//pkg/jobs:jobs_test",
				"//pkg/ccl/backupccl:backupccl_test",
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
				"//pkg/ccl/backupccl:backupccl_test",
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
