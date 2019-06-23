// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package ordering

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props/physical"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils/testexpr"
)

func TestProject(t *testing.T) {
	var fds props.FuncDepSet
	fds.AddEquivalency(2, 3)
	fds.AddConstants(opt.MakeColSet(4))

	project := &memo.ProjectExpr{
		Input: &testexpr.Instance{
			Rel: &props.Relational{
				OutputCols: opt.MakeColSet(1, 2, 3, 4),
				FuncDeps:   fds,
			},
		},
	}

	type testCase struct {
		req string
		exp string
	}
	testCases := []testCase{
		{
			req: "",
			exp: "",
		},
		{
			req: "+1,+2,+3,+4",
			exp: "+1,+(2|3) opt(4)",
		},
		{
			req: "+3 opt(5)",
			exp: "+(2|3) opt(4)",
		},
		{
			req: "+1 opt(2,5)",
			exp: "+1 opt(2-4)",
		},
		{
			req: "+1,+2,+3,+4,+5",
			exp: "no",
		},
		{
			req: "+5",
			exp: "no",
		},
	}
	for _, tc := range testCases {
		req := physical.ParseOrderingChoice(tc.req)
		res := "no"
		if projectCanProvideOrdering(project, &req) {
			res = projectBuildChildReqOrdering(project, &req, 0).String()
		}
		if res != tc.exp {
			t.Errorf("req: %s  expected: %s  got: %s\n", tc.req, tc.exp, res)
		}
	}
}
