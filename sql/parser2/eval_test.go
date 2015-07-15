// Copyright 2015 The Cockroach Authors.
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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Peter Mattis (peter@cockroachlabs.com)

package parser2

import (
	"testing"

	"github.com/cockroachdb/cockroach/testutils"
)

func TestEvalExpr(t *testing.T) {
	testData := []struct {
		expr     string
		expected string
		env      Env
	}{
		// Bitwise operators.
		{`1 & 3`, `1`, nil},
		{`1 | 3`, `3`, nil},
		{`1 # 3`, `2`, nil},
		// Arithmetic operators.
		{`1 + 1`, `2`, nil},
		{`1 - 2`, `-1`, nil},
		{`3 * 4`, `12`, nil},
		{`3.1 % 2.0`, `1.1`, nil},
		{`5 % 3`, `2`, nil},
		// Division is always done on floats.
		{`4 / 5`, `0.8`, nil},
		// Grouping
		{`1 + 2 + (3 * 4)`, `15`, nil},
		// Unary operators.
		{`-3`, `-3`, nil},
		{`-4.1`, `-4.1`, nil},
		// Ones complement operates on signed integers.
		{`~0`, `-1`, nil},
		{`~0 - 1`, `-2`, nil},
		// Hexadecimal numbers.
		// TODO(pmattis): {`0xa`, `10`, nil},
		// Octal numbers.
		// TODO(pmattis): {`0755`, `493`, nil},
		// String concatenation.
		{`'a' || 'b'`, `ab`, nil},
		{`'a' || (1 + 2)`, `a3`, nil},
		// Column lookup.
		{`a`, `1`, mapEnv{"a": dint(1)}},
		{`a`, `3.1`, mapEnv{"a": dfloat(3.1)}},
		{`a`, `c`, mapEnv{"a": dstring("c")}},
		{`a.b + 1`, `2`, mapEnv{"a.b": dint(1)}},
		// Boolean expressions.
		{`false AND true`, `false`, nil},
		{`true AND true`, `true`, nil},
		{`true AND false`, `false`, nil},
		{`false AND false`, `false`, nil},
		{`false OR true`, `true`, nil},
		{`true OR true`, `true`, nil},
		{`true OR false`, `true`, nil},
		{`false OR false`, `false`, nil},
		{`NOT false`, `true`, nil},
		{`NOT true`, `false`, nil},
		// Boolean expressions short-circuit the evaluation.
		{`false AND (a = 1)`, `false`, nil},
		{`true OR (a = 1)`, `true`, nil},
		// Comparisons.
		{`0 = 1`, `false`, nil},
		{`0 != 1`, `true`, nil},
		{`0 < 1`, `true`, nil},
		{`0 <= 1`, `true`, nil},
		{`0 > 1`, `false`, nil},
		{`0 >= 1`, `false`, nil},
		{`true = false`, `false`, nil},
		{`true != false`, `true`, nil},
		{`true < false`, `false`, nil},
		{`true <= false`, `false`, nil},
		{`true > false`, `true`, nil},
		{`true >= false`, `true`, nil},
		{`'a' = 'b'`, `false`, nil},
		{`'a' != 'b'`, `true`, nil},
		{`'a' < 'b'`, `true`, nil},
		{`'a' <= 'b'`, `true`, nil},
		{`'a' > 'b'`, `false`, nil},
		{`'a' >= 'b'`, `false`, nil},
		{`'a' >= 'b'`, `false`, nil},
		{`'10' > '2'`, `false`, nil},
		// Comparisons against NULL result in NULL.
		{`0 = NULL`, `NULL`, nil},
		{`NULL = NULL`, `NULL`, nil},
		// NULL checks.
		{`0 IS NULL`, `false`, nil},
		{`0 IS NOT NULL`, `true`, nil},
		{`NULL IS NULL`, `true`, nil},
		{`NULL IS NOT NULL`, `false`, nil},
		// Range conditions.
		{`2 BETWEEN 1 AND 3`, `true`, nil},
		{`1 NOT BETWEEN 2 AND 3`, `true`, nil},
		{`'foo' BETWEEN 'a' AND 'z'`, `true`, nil},
		// Case operator.
		{`CASE WHEN true THEN 1 END`, `1`, nil},
		{`CASE WHEN false THEN 1 END`, `NULL`, nil},
		{`CASE WHEN false THEN 1 ELSE 2 END`, `2`, nil},
		{`CASE WHEN false THEN 1 WHEN false THEN 2 END`, `NULL`, nil},
	}
	for _, d := range testData {
		q, err := Parse("SELECT " + d.expr)
		if err != nil {
			t.Fatalf("%s: %v: %s", d.expr, err, d.expr)
		}
		expr := q[0].(*Select).Exprs[0].(*NonStarExpr).Expr
		r, err := EvalExpr(expr, d.env)
		if err != nil {
			t.Fatalf("%s: %v", d.expr, err)
		}
		if s := r.String(); d.expected != s {
			t.Errorf("%s: expected %s, but found %s: %s", d.expr, d.expected, s, d.expr)
		}
	}
}

func TestEvalExprError(t *testing.T) {
	testData := []struct {
		expr     string
		expected string
	}{
		{`'1' + '2'`, `unsupported binary operator:`},
		{`'a' + 0`, `unsupported binary operator:`},
		{`1.1 # 3.1`, `unsupported binary operator:`},
		{`~0.1`, `unsupported unary operator:`},
		{`'10' > 2`, `unsupported comparison operator:`},
		{`a`, `column \"a\" not found`},
		// TODO(pmattis): Check for overflow.
		// {`~0 + 1`, `0`, nil},
	}
	for _, d := range testData {
		q, err := Parse("SELECT " + d.expr)
		if err != nil {
			t.Fatalf("%s: %v: %s", d.expr, err, d.expr)
		}
		expr := q[0].(*Select).Exprs[0].(*NonStarExpr).Expr
		if _, err := EvalExpr(expr, mapEnv{}); !testutils.IsError(err, d.expected) {
			t.Errorf("%s: expected %s, but found %v", d.expr, d.expected, err)
		}
	}
}
