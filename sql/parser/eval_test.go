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

package parser

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
		{`1 / 0`, `+Inf`, nil},
		{`-1.0 * (1.0 / 0.0)`, `-Inf`, nil},
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
		{`a`, `1`, mapEnv{"a": DInt(1)}},
		{`a`, `3.1`, mapEnv{"a": DFloat(3.1)}},
		{`a`, `c`, mapEnv{"a": DString("c")}},
		{`a.b + 1`, `2`, mapEnv{"a.b": DInt(1)}},
		{`a OR b`, `true`, mapEnv{"a": DBool(false), "b": DBool(true)}},
		// Boolean expressions.
		{`false AND true`, `false`, nil},
		{`false AND NULL`, `false`, nil},
		{`false AND false`, `false`, nil},
		{`true AND true`, `true`, nil},
		{`true AND false`, `false`, nil},
		{`true AND NULL`, `NULL`, nil},
		{`NULL AND true`, `NULL`, nil},
		{`NULL AND false`, `NULL`, nil},
		{`NULL AND NULL`, `NULL`, nil},
		{`false OR true`, `true`, nil},
		{`false OR NULL`, `NULL`, nil},
		{`false OR false`, `false`, nil},
		{`true OR true`, `true`, nil},
		{`true OR false`, `true`, nil},
		{`true OR NULL`, `true`, nil},
		{`NULL OR true`, `true`, nil},
		{`NULL OR false`, `NULL`, nil},
		{`NULL OR NULL`, `NULL`, nil},
		{`NOT false`, `true`, nil},
		{`NOT true`, `false`, nil},
		{`NOT NULL`, `NULL`, nil},
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
		{`CASE 1+1 WHEN 1 THEN 1 WHEN 2 THEN 2 END`, `2`, nil},
		{`CASE 1+2 WHEN 1 THEN 1 WHEN 2 THEN 2 ELSE 'doh' END`, `doh`, nil},
		// Row (tuple) comparisons.
		{`ROW(1) = ROW(1)`, `true`, nil},
		{`ROW(1, true) = (1, NOT false)`, `true`, nil},
		{`(1, 'a') = (1, 'a')`, `true`, nil},
		{`(1, 'a' || 1) = (1, 'a1')`, `true`, nil},
		{`(1+1, (2+2, (3+3))) = (2, (4, (6)))`, `true`, nil},
		{`(1, 'a') != (1, 'a')`, `false`, nil},
		{`(1, 'a') != (1, 'b')`, `true`, nil},
		// IN and NOT IN expressions.
		{`1 NOT IN (2, 3, 4)`, `true`, nil},
		{`1+1 IN (2, 3, 4)`, `true`, nil},
		{`'a0' IN ('a'||0, 'b'||1, 'c'||2)`, `true`, nil},
		{`(1,2) IN ((0+1,1+1), (3,4), (5,6))`, `true`, nil},
		// Func expressions.
		{`length('hel'||'lo')`, `5`, nil},
		{`lower('HELLO')`, `hello`, nil},
		{`UPPER('hello')`, `HELLO`, nil},
		// Cast expressions.
		{`true::boolean`, `true`, nil},
		{`true::int`, `1`, nil},
		{`true::float`, `1`, nil},
		{`length(true::text)`, `4`, nil},
		{`false::boolean`, `false`, nil},
		{`false::int`, `0`, nil},
		{`false::float`, `0`, nil},
		{`length(false::text)`, `5`, nil},
		{`1::boolean`, `true`, nil},
		{`0::boolean`, `false`, nil},
		{`1::int`, `1`, nil},
		{`1::float`, `1`, nil},
		{`length(123::text)`, `3`, nil},
		{`1.1::boolean`, `true`, nil},
		{`0.0::boolean`, `false`, nil},
		{`1.1::int`, `1`, nil},
		{`1.1::float`, `1.1`, nil},
		{`length(1.23::text)`, `4`, nil},
		{`'t'::boolean`, `true`, nil},
		{`'T'::boolean`, `true`, nil},
		{`'true'::boolean`, `true`, nil},
		{`'True'::boolean`, `true`, nil},
		{`'TRUE'::boolean`, `true`, nil},
		{`'1'::boolean`, `true`, nil},
		{`'f'::boolean`, `false`, nil},
		{`'F'::boolean`, `false`, nil},
		{`'false'::boolean`, `false`, nil},
		{`'False'::boolean`, `false`, nil},
		{`'FALSE'::boolean`, `false`, nil},
		{`'0'::boolean`, `false`, nil},
		{`'123'::int + 1`, `124`, nil},
		{`'0x123'::int + 1`, `292`, nil},
		{`'0123'::int + 1`, `84`, nil}, // TODO(pmattis): Should we support octal notation?
		{`'1.23'::float + 1.0`, `2.23`, nil},
		{`'hello'::text`, `hello`, nil},
		{`CAST('123' AS int) + 1`, `124`, nil},
		{`'hello'::char(2)`, `he`, nil},
	}
	for _, d := range testData {
		q, err := Parse("SELECT " + d.expr)
		if err != nil {
			t.Fatalf("%s: %v", d.expr, err)
		}
		expr := q[0].(*Select).Exprs[0].(*NonStarExpr).Expr
		r, err := EvalExpr(expr, d.env)
		if err != nil {
			t.Fatalf("%s: %v", d.expr, err)
		}
		if s := r.String(); d.expected != s {
			t.Errorf("%s: expected %s, but found %s", d.expr, d.expected, s)
		}
	}
}

func TestEvalExprError(t *testing.T) {
	testData := []struct {
		expr     string
		expected string
	}{
		{`1 % 0`, `zero modulus`},
		{`'1' + '2'`, `unsupported binary operator:`},
		{`'a' + 0`, `unsupported binary operator:`},
		{`1.1 # 3.1`, `unsupported binary operator:`},
		{`1/0.0`, `unsupported binary operator:`},
		{`~0.1`, `unsupported unary operator:`},
		{`'10' > 2`, `unsupported comparison operator:`},
		{`1 IN ('a', 'b')`, `unsupported comparison operator:`},
		{`a`, `column \"a\" not found`},
		{`1 AND true`, `cannot convert int to bool`},
		{`1.0 AND true`, `cannot convert float to bool`},
		{`'a' AND true`, `cannot convert string to bool`},
		{`(1, 2) AND true`, `cannot convert tuple to bool`},
		{`lower()`, `incorrect number of arguments`},
		{`lower(1, 2)`, `incorrect number of arguments`},
		{`lower(1)`, `argument type mismatch`},
		{`1::bit`, `invalid cast: int -> BIT`},
		{`1::decimal`, `invalid cast: int -> DECIMAL`},
		{`1::date`, `invalid cast: int -> DATE`},
		{`1::time`, `invalid cast: int -> TIME`},
		{`1::timestamp`, `invalid cast: int -> TIMESTAMP`},
		// TODO(pmattis): Check for overflow.
		// {`~0 + 1`, `0`, nil},
	}
	for _, d := range testData {
		q, err := Parse("SELECT " + d.expr)
		if err != nil {
			t.Fatalf("%s: %v", d.expr, err)
		}
		expr := q[0].(*Select).Exprs[0].(*NonStarExpr).Expr
		if _, err := EvalExpr(expr, mapEnv{}); !testutils.IsError(err, d.expected) {
			t.Errorf("%s: expected %s, but found %v", d.expr, d.expected, err)
		}
	}
}
