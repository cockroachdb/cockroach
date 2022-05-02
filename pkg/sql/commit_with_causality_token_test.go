// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

// TestIsSelectWithCausalityToken tests whether given sql statements match the
// criteria for isSelectWithCausalityToken.
func TestIsSelectWithCausalityToken(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for _, tc := range []struct {
		in  string
		exp bool
	}{
		{"Select crdb_internal.commit_with_causality_token()", true},
		{"select crdb_inteRnal.comMit_wiTh_cauSality_toKen()", true},
		{`select "crdb_internal".comMit_wiTh_cauSality_toKen()`, true},
		{"select crdb_inteRnal.comMit_wiTh_cauSality_toKen(), 1", false},
		{"select crdb_internal.commit_with_causality_token() from crdb_internal.ranges_no_leases", false},
		{"select crdb_internal.commit_with_causality_token() from generate_series(1, 100)", false},
		{`select distinct "crdb_internal".comMit_wiTh_cauSality_toKen()`, false},
		{`select "crdb_inteRnal".comMit_wiTh_cauSality_toKen()`, false},
		{"(select crdb_internal.commit_with_causality_token())", false},
		{"(select crdb_inteRnal.comMit_wiTh_cauSality_toKen())", false},
		{`(select "crdb_internal".comMit_wiTh_cauSality_toKen())`, false},
		{`(select "crdb_inteRnal".comMit_wiTh_cauSality_toKen())`, false},
		{`((select "crdb_internal".comMit_wiTh_cauSality_toKen()))`, false},
		{`SELECT ((select "crdb_internal".comMit_wiTh_cauSality_toKen()))`, false},
		{"SELECT crdb_internal.commit_with_causality_token() FOR UPDATE ", false},
		{"Select crdb_internal.commit_with_causality_token", false},
		{"with a as (select 1) select crdb_internal.commit_with_causality_token()", false},
		{"(select crdb_internal.commit_with_causality_token() limit 0)", false},
		{"(select crdb_internal.commit_with_causality_token()) limit 0", false},
		{"select crdb_internal.commit_with_causality_token() limit 0", false},
		{"(select crdb_internal.commit_with_causality_token() where true)", false},
		{"select crdb_internal.commit_with_causality_token() where true", false},
		{"select crdb_internal.commit_with_causality_token() having true", false},
		{"select crdb_internal.commit_with_causality_token() order by 1", false},
		{"(select crdb_internal.commit_with_causality_token()) order by 1", false},
		{"(select crdb_internal.commit_with_causality_token() order by 1)", false},
	} {
		t.Run(tc.in, func(t *testing.T) {
			stmts, err := parser.Parse(tc.in)
			require.NoError(t, err)
			require.Len(t, stmts, 1)
			require.Equalf(
				t, tc.exp, isSelectCommitWithCausalityToken(stmts[0].AST),
				"%s", stmts[0].SQL,
			)
		})
	}
}
