// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package ordering

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props/physical"
	"github.com/cockroachdb/cockroach/pkg/util"
)

func TestProject(t *testing.T) {
	var inProps props.Relational
	inProps.OutputCols = util.MakeFastIntSet(1, 2, 3, 4)
	fd := &inProps.FuncDeps
	fd.AddEquivalency(2, 3)
	fd.AddConstants(util.MakeFastIntSet(4))

	project := &memo.ProjectExpr{
		Input: newDummyRelExpr(inProps),
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
