// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package physical

import (
	"bytes"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestPlanGramFormatPretty(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	scan := &planGramProduction{
		name: "scan",
		rules: []planGramTerm{
			&planGramExpr{
				op: opt.ScanOp,
				fields: []planGramExprField{
					{key: "Index", val: "abc_b_idx"},
				},
			},
			&planGramExpr{
				op: opt.ScanOp,
				fields: []planGramExprField{
					{key: "Index", val: "abc_c_idx"},
				},
			},
		},
	}

	cycle := &planGramProduction{name: "cycle"}
	cycle.rules = []planGramTerm{
		&planGramExpr{
			op: opt.InnerJoinOp,
			children: []planGramTerm{
				cycle,
				cycle,
			},
		},
		scan,
	}

	tests := []struct {
		name             string
		plangram         PlanGram
		expectedOneLine  string
		expectedNewlines string
	}{
		{
			name:             "zero value",
			plangram:         PlanGram{},
			expectedOneLine:  "root: any;",
			expectedNewlines: "root: any;\n",
		},
		{
			name:             "any",
			plangram:         AnyPlanGram,
			expectedOneLine:  "root: any;",
			expectedNewlines: "root: any;\n",
		},
		{
			name:             "none",
			plangram:         NonePlanGram,
			expectedOneLine:  "root: none;",
			expectedNewlines: "root: none;\n",
		},
		{
			name: "simple terminal",
			plangram: PlanGram{root: &planGramExpr{
				op: opt.ScanOp,
				fields: []planGramExprField{
					{key: "Index", val: "abc_a_idx"},
				},
			}},
			expectedOneLine:  "root: (Scan Index=\"abc_a_idx\");",
			expectedNewlines: "root: (Scan Index=\"abc_a_idx\");\n",
		},
		{
			name: "expr with children",
			plangram: PlanGram{root: &planGramExpr{
				op: opt.SelectOp,
				children: []planGramTerm{
					&planGramExpr{op: opt.ScanOp},
				},
			}},
			expectedOneLine:  "root: (Select (Scan));",
			expectedNewlines: "root: (Select (Scan));\n",
		},
		{
			name: "child referencing nil",
			plangram: PlanGram{root: &planGramExpr{
				op:       opt.SelectOp,
				children: []planGramTerm{nil},
			}},
			expectedOneLine:  "root: (Select any);",
			expectedNewlines: "root: (Select any);\n",
		},
		{
			name: "child referencing any",
			plangram: PlanGram{root: &planGramExpr{
				op:       opt.SelectOp,
				children: []planGramTerm{anyPlanGramTerm},
			}},
			expectedOneLine:  "root: (Select any);",
			expectedNewlines: "root: (Select any);\n",
		},
		{
			name: "child referencing none",
			plangram: PlanGram{root: &planGramExpr{
				op:       opt.SelectOp,
				children: []planGramTerm{nonePlanGramTerm},
			}},
			expectedOneLine:  "root: (Select none);",
			expectedNewlines: "root: (Select none);\n",
		},
		{
			name: "with nonterminal production",
			plangram: PlanGram{root: &planGramExpr{
				op: opt.SelectOp,
				children: []planGramTerm{
					&planGramExpr{
						op:       opt.IndexJoinOp,
						children: []planGramTerm{scan},
					},
				},
			}},
			expectedOneLine: "root: (Select (IndexJoin scan)); scan: (Scan Index=\"abc_b_idx\") | (Scan Index=\"abc_c_idx\");",
			expectedNewlines: "root: (Select (IndexJoin scan));\n" +
				"scan: (Scan Index=\"abc_b_idx\") | (Scan Index=\"abc_c_idx\");\n",
		},
		{
			name: "multiple fields",
			plangram: PlanGram{root: &planGramExpr{
				op: opt.ScanOp,
				fields: []planGramExprField{
					{key: "Table", val: "abc"},
					{key: "Index", val: "abc_b_idx"},
				},
			}},
			expectedOneLine:  "root: (Scan Table=\"abc\" Index=\"abc_b_idx\");",
			expectedNewlines: "root: (Scan Table=\"abc\" Index=\"abc_b_idx\");\n",
		},
		{
			name: "field value with special characters",
			plangram: PlanGram{root: &planGramExpr{
				op: opt.ScanOp,
				fields: []planGramExprField{
					{key: "Index", val: `has "quotes" and spaces`},
				},
			}},
			expectedOneLine:  `root: (Scan Index="has \"quotes\" and spaces");`,
			expectedNewlines: "root: (Scan Index=\"has \\\"quotes\\\" and spaces\");\n",
		},
		{
			name:             "cyclical productions",
			plangram:         PlanGram{root: cycle},
			expectedOneLine:  "root: cycle; cycle: (InnerJoin cycle cycle) | scan; scan: (Scan Index=\"abc_b_idx\") | (Scan Index=\"abc_c_idx\");",
			expectedNewlines: "root: cycle;\ncycle: (InnerJoin cycle cycle) | scan;\nscan: (Scan Index=\"abc_b_idx\") | (Scan Index=\"abc_c_idx\");\n",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var b bytes.Buffer
			tc.plangram.FormatPretty(&b, false /* newlines */)
			require.Equal(t, tc.expectedOneLine, b.String())
			b.Reset()
			tc.plangram.FormatPretty(&b, true /* newlines */)
			require.Equal(t, tc.expectedNewlines, b.String())
		})
	}
}
