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
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/norm"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props/physical"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils/testexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

func TestOrdinalityProvided(t *testing.T) {
	emptyFD, equivFD, constFD := testFDs()
	// The ordinality column is 10.
	testCases := []struct {
		required string
		input    string
		fds      props.FuncDepSet
		provided string
	}{
		{ // case 1
			required: "+10",
			input:    "+1,+2",
			fds:      emptyFD,
			provided: "+10",
		},
		{ // case 2
			required: "+1,+10",
			input:    "+1,+2,+3",
			fds:      emptyFD,
			provided: "+1,+10",
		},
		{ // case 3
			required: "+1,+10,+5",
			input:    "+1,+2",
			fds:      emptyFD,
			provided: "+1,+10",
		},
		{ // case 4
			required: "+(1|2),+(3|10)",
			input:    "+1,+4,+5",
			fds:      emptyFD,
			provided: "+1,+10",
		},
		{ // case 5
			required: "+1",
			input:    "",
			fds:      constFD,
			provided: "",
		},
		{ // case 6
			required: "-1,+10",
			input:    "-2",
			fds:      equivFD,
			provided: "-2,+10",
		},
	}

	for tcIdx, tc := range testCases {
		t.Run(fmt.Sprintf("case%d", tcIdx+1), func(t *testing.T) {
			evalCtx := tree.NewTestingEvalContext(nil /* st */)
			var f norm.Factory
			f.Init(evalCtx, nil /* catalog */)
			input := &testexpr.Instance{
				Rel: &props.Relational{OutputCols: opt.MakeColSet(1, 2, 3, 4, 5)},
				Provided: &physical.Provided{
					Ordering: props.ParseOrdering(tc.input),
				},
			}
			r := f.Memo().MemoizeOrdinality(input, &memo.OrdinalityPrivate{ColID: 10})
			r.Relational().FuncDeps = tc.fds
			req := props.ParseOrderingChoice(tc.required)
			res := ordinalityBuildProvided(r, &req).String()
			if res != tc.provided {
				t.Errorf("expected '%s', got '%s'", tc.provided, res)
			}
		})
	}
}
