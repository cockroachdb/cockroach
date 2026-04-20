// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package physical

import (
	"bytes"
	"strings"
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

func TestPlanGramParse(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Round-trip tests: for each format test case, format the plangram, parse it
	// back, and re-format it. The two formatted strings should be identical.
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

	roundTripTests := []struct {
		name     string
		plangram PlanGram
	}{
		{name: "any", plangram: AnyPlanGram},
		{name: "none", plangram: NonePlanGram},
		{
			name: "simple terminal",
			plangram: PlanGram{root: &planGramExpr{
				op: opt.ScanOp,
				fields: []planGramExprField{
					{key: "Index", val: "abc_a_idx"},
				},
			}},
		},
		{
			name: "expr with children",
			plangram: PlanGram{root: &planGramExpr{
				op: opt.SelectOp,
				children: []planGramTerm{
					&planGramExpr{op: opt.ScanOp},
				},
			}},
		},
		{
			name: "child referencing any",
			plangram: PlanGram{root: &planGramExpr{
				op:       opt.SelectOp,
				children: []planGramTerm{anyPlanGramTerm},
			}},
		},
		{
			name: "child referencing none",
			plangram: PlanGram{root: &planGramExpr{
				op:       opt.SelectOp,
				children: []planGramTerm{nonePlanGramTerm},
			}},
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
		},
		{
			name: "field value with special characters",
			plangram: PlanGram{root: &planGramExpr{
				op: opt.ScanOp,
				fields: []planGramExprField{
					{key: "Index", val: `has "quotes" and spaces`},
				},
			}},
		},
		{
			name:     "cyclical productions",
			plangram: PlanGram{root: cycle},
		},
	}

	for _, tc := range roundTripTests {
		t.Run(tc.name, func(t *testing.T) {
			// Format the original plangram.
			var b bytes.Buffer
			tc.plangram.FormatPretty(&b, false /* newlines */)
			formatted1 := b.String()

			// Parse it back.
			parsed, err := ParsePlanGram(strings.NewReader(formatted1))
			require.NoError(t, err)

			// Re-format the parsed plangram.
			b.Reset()
			parsed.FormatPretty(&b, false /* newlines */)
			formatted2 := b.String()

			require.Equal(t, formatted1, formatted2)
		})
	}

	// Also test parsing from multi-line format.
	for _, tc := range roundTripTests {
		t.Run(tc.name+"/newlines", func(t *testing.T) {
			var b bytes.Buffer
			tc.plangram.FormatPretty(&b, true /* newlines */)
			formatted1 := b.String()

			parsed, err := ParsePlanGram(strings.NewReader(formatted1))
			require.NoError(t, err)

			// Compare with single-line format (canonical).
			b.Reset()
			tc.plangram.FormatPretty(&b, false /* newlines */)
			expected := b.String()

			b.Reset()
			parsed.FormatPretty(&b, false /* newlines */)
			require.Equal(t, expected, b.String())
		})
	}

	// Error cases.
	errorTests := []struct {
		name        string
		input       string
		expectedErr string
	}{
		{
			name:        "empty input",
			input:       "",
			expectedErr: "unexpected end of input",
		},
		{
			name:        "missing root",
			input:       "scan: (Scan);",
			expectedErr: `expected "root", got "scan"`,
		},
		{
			name:        "root alternates",
			input:       "root: (Scan) | (Select);",
			expectedErr: "root must have exactly one term",
		},
		{
			name:        "undefined nonterminal",
			input:       "root: (Select missing);",
			expectedErr: `undefined nonterminal "missing"`,
		},
		{
			name:        "duplicate production",
			input:       "root: s; s: (Scan); s: (Select);",
			expectedErr: `duplicate production "s"`,
		},
		{
			name:        "unknown operator",
			input:       "root: (Bogus);",
			expectedErr: `unknown operator "Bogus"`,
		},
		{
			name:        "root as nonterminal",
			input:       "root: (Select root);",
			expectedErr: `"root" cannot be used as a nonterminal reference`,
		},
		{
			name:        "any as production name",
			input:       "root: (Select any); any: (Scan);",
			expectedErr: `cannot be used as a production name`,
		},
		{
			name:        "none as production name",
			input:       "root: (Select none); none: (Scan);",
			expectedErr: `cannot be used as a production name`,
		},
		{
			name:        "missing semicolon",
			input:       "root: (Scan)",
			expectedErr: `expected ";"`,
		},
	}

	for _, tc := range errorTests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParsePlanGram(strings.NewReader(tc.input))
			require.Error(t, err)
			require.ErrorContains(t, err, tc.expectedErr)
		})
	}
}
