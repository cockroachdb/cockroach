// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package physical

import (
	"bufio"
	"bytes"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestPlanGramAny(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	require.True(t, AnyPlanGram.Any())
	require.True(t, PlanGram{}.Any())
	require.False(t, NonePlanGram.Any())
}

func TestPlanGramStringAndFormat(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	pg, err := ParsePlanGram(strings.NewReader(`root: (Scan Index="abc_a_idx");`))
	require.NoError(t, err)

	require.Equal(t, `root: (Scan Index="abc_a_idx");`, pg.String())

	var b bytes.Buffer
	pg.Format(&b)
	require.Equal(t, `root: (Scan Index="abc_a_idx");`, b.String())
}

func TestTokenizePlanGramSmallBuffer(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tests := []struct {
		name     string
		input    string
		expected []string
	}{
		{
			name:     "short tokens",
			input:    `root: (Scan Index="abc_a_idx");`,
			expected: []string{"root", ":", "(", "Scan", "Index", "=", `"abc_a_idx"`, ")", ";"},
		},
		{
			name:     "word longer than buffer",
			input:    "root: (InnerJoin (Scan) (Scan));",
			expected: []string{"root", ":", "(", "InnerJoin", "(", "Scan", ")", "(", "Scan", ")", ")", ";"},
		},
		{
			name:     "whitespace spanning buffer boundary",
			input:    "root:          (Scan);",
			expected: []string{"root", ":", "(", "Scan", ")", ";"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			scanner := bufio.NewScanner(strings.NewReader(tc.input))
			scanner.Buffer(make([]byte, 8), 64)
			scanner.Split(tokenizePlanGram)

			var tokens []string
			for scanner.Scan() {
				tokens = append(tokens, scanner.Text())
			}
			require.NoError(t, scanner.Err())
			require.Equal(t, tc.expected, tokens)
		})
	}
}

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

type mockExpr struct {
	op       opt.Operator
	children []opt.Expr
}

func (m *mockExpr) Op() opt.Operator       { return m.op }
func (m *mockExpr) ChildCount() int        { return len(m.children) }
func (m *mockExpr) Child(nth int) opt.Expr { return m.children[nth] }
func (m *mockExpr) Private() interface{}   { return nil }

func TestPlanGramMatches(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	scanExpr := &mockExpr{op: opt.ScanOp}
	selectExpr := &mockExpr{op: opt.SelectOp, children: []opt.Expr{scanExpr}}
	twoChildExpr := &mockExpr{
		op:       opt.InnerJoinOp,
		children: []opt.Expr{scanExpr, scanExpr},
	}

	tests := []struct {
		name     string
		pg       PlanGram
		expr     opt.Expr
		expected bool
	}{
		{
			name:     "any matches any expr",
			pg:       AnyPlanGram,
			expr:     scanExpr,
			expected: true,
		},
		{
			name:     "none matches nothing",
			pg:       NonePlanGram,
			expr:     scanExpr,
			expected: false,
		},
		{
			name:     "operator match",
			pg:       PlanGram{root: &planGramExpr{op: opt.ScanOp}},
			expr:     scanExpr,
			expected: true,
		},
		{
			name:     "operator mismatch",
			pg:       PlanGram{root: &planGramExpr{op: opt.ScanOp}},
			expr:     selectExpr,
			expected: false,
		},
		{
			name:     "wildcard op matches any expr",
			pg:       PlanGram{root: &planGramExpr{op: opt.UnknownOp}},
			expr:     scanExpr,
			expected: true,
		},
		{
			name: "fewer PG children than expr children",
			pg: PlanGram{root: &planGramExpr{
				op:       opt.InnerJoinOp,
				children: []planGramTerm{&planGramExpr{op: opt.ScanOp}},
			}},
			expr:     twoChildExpr,
			expected: true,
		},
		{
			name: "more PG children than expr children",
			pg: PlanGram{root: &planGramExpr{
				op: opt.InnerJoinOp,
				children: []planGramTerm{
					&planGramExpr{op: opt.ScanOp},
					&planGramExpr{op: opt.ScanOp},
					&planGramExpr{op: opt.ScanOp},
				},
			}},
			expr:     twoChildExpr,
			expected: false,
		},
		{
			name: "equal PG and expr children",
			pg: PlanGram{root: &planGramExpr{
				op: opt.InnerJoinOp,
				children: []planGramTerm{
					&planGramExpr{op: opt.ScanOp},
					&planGramExpr{op: opt.ScanOp},
				},
			}},
			expr:     twoChildExpr,
			expected: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, tc.pg.Matches(tc.expr))
		})
	}
}

func TestPlanGramChild(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	scanTerm := &planGramExpr{op: opt.ScanOp}
	selectTerm := &planGramExpr{op: opt.SelectOp}
	parentPG := PlanGram{root: &planGramExpr{
		op:       opt.InnerJoinOp,
		children: []planGramTerm{scanTerm, selectTerm},
	}}

	tests := []struct {
		name          string
		pg            PlanGram
		childIdx      int
		expectedChild PlanGram
	}{
		{
			name:          "any child 0",
			pg:            AnyPlanGram,
			childIdx:      0,
			expectedChild: AnyPlanGram,
		},
		{
			name:          "any child 99",
			pg:            AnyPlanGram,
			childIdx:      99,
			expectedChild: AnyPlanGram,
		},
		{
			name:          "concrete child 0",
			pg:            parentPG,
			childIdx:      0,
			expectedChild: PlanGram{root: scanTerm},
		},
		{
			name:          "concrete child 1",
			pg:            parentPG,
			childIdx:      1,
			expectedChild: PlanGram{root: selectTerm},
		},
		{
			name:          "out of range returns any",
			pg:            parentPG,
			childIdx:      5,
			expectedChild: AnyPlanGram,
		},
		{
			name:          "none child 0 returns none",
			pg:            NonePlanGram,
			childIdx:      0,
			expectedChild: NonePlanGram,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			child := tc.pg.Child(tc.childIdx)
			require.True(t, child.Equals(tc.expectedChild),
				"expected %s, got %s", tc.expectedChild, child)
		})
	}
}

func TestPlanGramHasAlternates(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tests := []struct {
		name     string
		pg       PlanGram
		expected bool
	}{
		{
			name:     "any",
			pg:       AnyPlanGram,
			expected: false,
		},
		{
			name:     "none",
			pg:       NonePlanGram,
			expected: false,
		},
		{
			name:     "concrete expr",
			pg:       PlanGram{root: &planGramExpr{op: opt.ScanOp}},
			expected: false,
		},
		{
			name: "production with rules",
			pg: PlanGram{root: &planGramProduction{
				name: "p",
				rules: []planGramTerm{
					&planGramExpr{op: opt.ScanOp},
					&planGramExpr{op: opt.SelectOp},
				},
			}},
			expected: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, tc.pg.HasAlternates())
		})
	}
}

func TestPlanGramVisitAlternates(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	scanTerm := &planGramExpr{op: opt.ScanOp}
	selectTerm := &planGramExpr{op: opt.SelectOp}

	tests := []struct {
		name          string
		pg            PlanGram
		expectedCount int
		verify        func(t *testing.T, alternates []PlanGram)
	}{
		{
			name:          "any yields one visit",
			pg:            AnyPlanGram,
			expectedCount: 1,
			verify: func(t *testing.T, alternates []PlanGram) {
				require.True(t, alternates[0].Any())
			},
		},
		{
			name:          "none yields one visit",
			pg:            NonePlanGram,
			expectedCount: 1,
			verify: func(t *testing.T, alternates []PlanGram) {
				require.True(t, alternates[0].Equals(NonePlanGram))
			},
		},
		{
			name:          "concrete expr yields one visit",
			pg:            PlanGram{root: scanTerm},
			expectedCount: 1,
			verify: func(t *testing.T, alternates []PlanGram) {
				require.True(t, alternates[0].Equals(PlanGram{root: scanTerm}))
			},
		},
		{
			name: "production with 2 rules yields 2 visits",
			pg: PlanGram{root: &planGramProduction{
				name:  "p",
				rules: []planGramTerm{scanTerm, selectTerm},
			}},
			expectedCount: 2,
			verify: func(t *testing.T, alternates []PlanGram) {
				require.False(t, alternates[0].HasAlternates())
				require.False(t, alternates[1].HasAlternates())
			},
		},
		{
			name: "production with any alternate",
			pg: PlanGram{root: &planGramProduction{
				name:  "p",
				rules: []planGramTerm{scanTerm, anyPlanGramTerm},
			}},
			expectedCount: 2,
			verify: func(t *testing.T, alternates []PlanGram) {
				hasAny := false
				for _, a := range alternates {
					if a.Any() {
						hasAny = true
					}
				}
				require.True(t, hasAny)
			},
		},
		{
			name: "production with none alternate",
			pg: PlanGram{root: &planGramProduction{
				name:  "p",
				rules: []planGramTerm{scanTerm, nonePlanGramTerm},
			}},
			expectedCount: 2,
			verify: func(t *testing.T, alternates []PlanGram) {
				hasNone := false
				for _, a := range alternates {
					if a.Equals(NonePlanGram) {
						hasNone = true
					}
				}
				require.True(t, hasNone)
			},
		},
		{
			name: "nested production flattened",
			pg: PlanGram{root: &planGramProduction{
				name: "a",
				rules: []planGramTerm{
					&planGramProduction{
						name:  "b",
						rules: []planGramTerm{scanTerm, selectTerm},
					},
				},
			}},
			expectedCount: 2,
			verify: func(t *testing.T, alternates []PlanGram) {
				for _, a := range alternates {
					require.False(t, a.HasAlternates())
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var alternates []PlanGram
			tc.pg.VisitAlternates(func(alt PlanGram) {
				alternates = append(alternates, alt)
			})
			require.Len(t, alternates, tc.expectedCount)
			if tc.verify != nil {
				tc.verify(t, alternates)
			}
		})
	}
}

func TestPlanGramWithNoneFallback(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	t.Run("any is idempotent", func(t *testing.T) {
		result := AnyPlanGram.WithNoneFallback()
		require.True(t, result.Any())
	})

	t.Run("none is idempotent", func(t *testing.T) {
		result := NonePlanGram.WithNoneFallback()
		require.True(t, result.Equals(NonePlanGram))
	})

	t.Run("concrete gets fallback", func(t *testing.T) {
		pg := PlanGram{root: &planGramExpr{op: opt.ScanOp}}
		result := pg.WithNoneFallback()
		require.True(t, result.HasAlternates())

		var alternates []PlanGram
		result.VisitAlternates(func(alt PlanGram) {
			alternates = append(alternates, alt)
		})
		require.Len(t, alternates, 2)

		hasNone := false
		for _, a := range alternates {
			if a.Equals(NonePlanGram) {
				hasNone = true
			}
		}
		require.True(t, hasNone)
	})

	t.Run("double wrap still works", func(t *testing.T) {
		pg := PlanGram{root: &planGramExpr{op: opt.ScanOp}}
		result := pg.WithNoneFallback().WithNoneFallback()
		require.True(t, result.HasAlternates())

		var alternates []PlanGram
		result.VisitAlternates(func(alt PlanGram) {
			alternates = append(alternates, alt)
		})
		require.GreaterOrEqual(t, len(alternates), 2)
	})
}

func TestPlanGramParse(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Round-trip tests: parse the input, format it, parse again, and re-format.
	// The two formatted strings should be identical.
	roundTripTests := []struct {
		name  string
		input string
	}{
		// Simple root-only grammars.
		{
			name:  "any",
			input: "root: any;",
		},
		{
			name:  "none",
			input: "root: none;",
		},
		{
			name:  "nullary expression",
			input: "root: (Scan);",
		},
		// Expressions with fields.
		{
			name:  "one field",
			input: `root: (Scan Index="abc_a_idx");`,
		},
		{
			name:  "multiple fields",
			input: `root: (Scan Table="abc" Index="abc_b_idx");`,
		},
		{
			name:  "field value with special characters",
			input: `root: (Scan Index="has \"quotes\" and spaces");`,
		},
		// Expressions with children.
		{
			name:  "child referencing any",
			input: "root: (Select any);",
		},
		{
			name:  "child referencing none",
			input: "root: (Select none);",
		},
		{
			name:  "inline child expression",
			input: "root: (Select (Scan));",
		},
		{
			name:  "multiple children",
			input: "root: (InnerJoin (Scan) (Scan));",
		},
		{
			name:  "fields and children",
			input: `root: (IndexJoin Table="abc" (Scan));`,
		},
		// Productions with nonterminal references.
		{
			name:  "root referencing nonterminal",
			input: `root: scan; scan: (Scan Index="abc_a_idx");`,
		},
		{
			name:  "single rule production",
			input: "root: (Select scan); scan: (Scan);",
		},
		{
			name:  "production with alternates",
			input: `root: (Select (IndexJoin scan)); scan: (Scan Index="abc_b_idx") | (Scan Index="abc_c_idx");`,
		},
		// Productions with any and none.
		{
			name:  "production rule is any",
			input: "root: (Select s); s: any;",
		},
		{
			name:  "production rule is none",
			input: "root: (Select s); s: none;",
		},
		{
			name:  "any as alternate",
			input: "root: (Select s); s: (Scan) | any;",
		},
		{
			name:  "none as alternate",
			input: "root: (Select s); s: (Scan) | none;",
		},
		// Production ordering and references.
		{
			name:  "productions in non-root-first order",
			input: `scan: (Scan Index="abc_a_idx"); root: (Select scan);`,
		},
		{
			name:  "forward and back references",
			input: "a: (Scan); root: (InnerJoin a b); b: (Scan);",
		},
		// Cycles.
		{
			name:  "mutual cycle",
			input: "root: a; a: (Select b); b: (Select a) | (Scan);",
		},
		{
			name:  "self-referencing cycle",
			input: `root: cycle; cycle: (InnerJoin cycle cycle) | scan; scan: (Scan Index="abc_b_idx") | (Scan Index="abc_c_idx");`,
		},
	}

	for _, tc := range roundTripTests {
		t.Run(tc.name, func(t *testing.T) {
			parsed1, err := ParsePlanGram(strings.NewReader(tc.input))
			require.NoError(t, err)

			var b bytes.Buffer
			parsed1.FormatPretty(&b, false /* newlines */)
			formatted1 := b.String()

			parsed2, err := ParsePlanGram(strings.NewReader(formatted1))
			require.NoError(t, err)

			b.Reset()
			parsed2.FormatPretty(&b, false /* newlines */)
			require.Equal(t, formatted1, b.String())
		})
	}

	// Also test parsing from multi-line format.
	for _, tc := range roundTripTests {
		t.Run(tc.name+"/newlines", func(t *testing.T) {
			// Parse multi-line version.
			parsed1, err := ParsePlanGram(strings.NewReader(tc.input))
			require.NoError(t, err)

			var b bytes.Buffer
			parsed1.FormatPretty(&b, true /* newlines */)
			multiLine := b.String()

			parsed2, err := ParsePlanGram(strings.NewReader(multiLine))
			require.NoError(t, err)

			// Compare with single-line format (canonical).
			b.Reset()
			parsed1.FormatPretty(&b, false /* newlines */)
			expected := b.String()

			b.Reset()
			parsed2.FormatPretty(&b, false /* newlines */)
			require.Equal(t, expected, b.String())
		})
	}

	// Extra whitespace: parse input with extra whitespace and verify it produces
	// the expected canonical output.
	whitespaceTests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "extra spaces around tokens",
			input:    `root :  ( Scan  Index = "abc_a_idx" ) ;`,
			expected: `root: (Scan Index="abc_a_idx");`,
		},
		{
			name:     "tabs and newlines",
			input:    "root:\n\t(\n\t\tScan\n\t);\n",
			expected: "root: (Scan);",
		},
		{
			name:     "leading and trailing whitespace",
			input:    "   root: (Scan);   ",
			expected: "root: (Scan);",
		},
		{
			name:     "whitespace around alternates",
			input:    `root: (Select scan) ; scan: ( Scan Index="b" )  |  ( Scan Index="c" ) ;`,
			expected: `root: (Select scan); scan: (Scan Index="b") | (Scan Index="c");`,
		},
	}

	for _, tc := range whitespaceTests {
		t.Run(tc.name, func(t *testing.T) {
			parsed, err := ParsePlanGram(strings.NewReader(tc.input))
			require.NoError(t, err)

			var b bytes.Buffer
			parsed.FormatPretty(&b, false /* newlines */)
			require.Equal(t, tc.expected, b.String())
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
			expectedErr: `missing "root" production`,
		},
		{
			name:        "missing root",
			input:       "scan: (Scan);",
			expectedErr: `missing "root" production`,
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
		{
			name:        "fields after children",
			input:       `root: (IndexJoin (Scan) Table="abc");`,
			expectedErr: "fields must come before children",
		},
		{
			name:        "apostrophe in production name",
			input:       "root: s; s'bad: (Scan);",
			expectedErr: `contains invalid character`,
		},
		{
			name:        "backslash in production name",
			input:       `root: s; s\bad: (Scan);`,
			expectedErr: `contains invalid character`,
		},
		{
			name:        "apostrophe in nonterminal reference",
			input:       `root: (Select s'bad); s'bad: (Scan);`,
			expectedErr: `contains invalid character`,
		},
		{
			name:        "backslash in nonterminal reference",
			input:       `root: (Select s\bad); s\bad: (Scan);`,
			expectedErr: `contains invalid character`,
		},
		{
			name:        "punctuation as production name",
			input:       "root: (Select ;);",
			expectedErr: `unexpected ";" in expression`,
		},
		{
			name:        "punctuation as nonterminal reference",
			input:       `root: (Select =);`,
			expectedErr: `unexpected "=" in expression`,
		},
		{
			name:        "missing closing paren",
			input:       "root: (Select (Scan);",
			expectedErr: `unexpected ";" in expression`,
		},
		{
			name:        "invalid field value",
			input:       "root: (Scan Index=unquoted);",
			expectedErr: "expected quoted string for field value",
		},
		{
			name:        "missing field value",
			input:       `root: (Scan Index=);`,
			expectedErr: "expected quoted string for field value",
		},
		{
			name:        "truncated input after colon",
			input:       "root:",
			expectedErr: "unexpected end of input: expected term",
		},
		{
			name:        "unclosed paren at EOF",
			input:       "root: (Scan",
			expectedErr: `unexpected end of input: expected ")"`,
		},
		{
			name:        "production name starts with digit",
			input:       "root: s; 1scan: (Scan);",
			expectedErr: "must not start with a digit",
		},
		{
			name:        "comma between children",
			input:       "root: (InnerJoin (Scan), (Scan));",
			expectedErr: "contains invalid character",
		},
		{
			name:        "comma in production name",
			input:       "root: s; s,bad: (Scan);",
			expectedErr: "contains invalid character",
		},
		{
			name:        "invalid UTF-8",
			input:       string([]byte{0xff}),
			expectedErr: "invalid UTF-8",
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
