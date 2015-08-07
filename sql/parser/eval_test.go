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
	}{
		// Bitwise operators.
		{`1 & 3`, `1`},
		{`1 | 3`, `3`},
		{`1 # 3`, `2`},
		// Arithmetic operators.
		{`1 + 1`, `2`},
		{`1 - 2`, `-1`},
		{`3 * 4`, `12`},
		{`3.1 % 2.0`, `1.1`},
		{`5 % 3`, `2`},
		// Division is always done on floats.
		{`4 / 5`, `0.8`},
		{`1 / 0`, `+Inf`},
		{`-1.0 * (1.0 / 0.0)`, `-Inf`},
		// Grouping
		{`1 + 2 + (3 * 4)`, `15`},
		// Unary operators.
		{`-3`, `-3`},
		{`-4.1`, `-4.1`},
		// Ones complement operates on signed integers.
		{`~0`, `-1`},
		{`~0 - 1`, `-2`},
		// Hexadecimal numbers.
		// TODO(pmattis): {`0xa`, `10`},
		// Octal numbers.
		// TODO(pmattis): {`0755`, `493`},
		// String concatenation.
		{`'a' || 'b'`, `'ab'`},
		{`'a' || (1 + 2)`, `'a3'`},
		// Boolean expressions.
		{`false AND true`, `false`},
		{`false AND NULL`, `false`},
		{`false AND false`, `false`},
		{`true AND true`, `true`},
		{`true AND false`, `false`},
		{`true AND NULL`, `NULL`},
		{`NULL AND true`, `NULL`},
		{`NULL AND false`, `NULL`},
		{`NULL AND NULL`, `NULL`},
		{`false OR true`, `true`},
		{`false OR NULL`, `NULL`},
		{`false OR false`, `false`},
		{`true OR true`, `true`},
		{`true OR false`, `true`},
		{`true OR NULL`, `true`},
		{`NULL OR true`, `true`},
		{`NULL OR false`, `NULL`},
		{`NULL OR NULL`, `NULL`},
		{`NOT false`, `true`},
		{`NOT true`, `false`},
		{`NOT NULL`, `NULL`},
		// Boolean expressions short-circuit the evaluation.
		{`false AND (a = 1)`, `false`},
		{`true OR (a = 1)`, `true`},
		// Comparisons.
		{`0 = 1`, `false`},
		{`0 != 1`, `true`},
		{`0 < 1`, `true`},
		{`0 <= 1`, `true`},
		{`0 > 1`, `false`},
		{`0 >= 1`, `false`},
		{`true = false`, `false`},
		{`true != false`, `true`},
		{`true < false`, `false`},
		{`true <= false`, `false`},
		{`true > false`, `true`},
		{`true >= false`, `true`},
		{`'a' = 'b'`, `false`},
		{`'a' != 'b'`, `true`},
		{`'a' < 'b'`, `true`},
		{`'a' <= 'b'`, `true`},
		{`'a' > 'b'`, `false`},
		{`'a' >= 'b'`, `false`},
		{`'a' >= 'b'`, `false`},
		{`'10' > '2'`, `false`},
		// Comparisons against NULL result in NULL.
		{`0 = NULL`, `NULL`},
		{`NULL = NULL`, `NULL`},
		// NULL checks.
		{`0 IS NULL`, `false`},
		{`0 IS NOT NULL`, `true`},
		{`NULL IS NULL`, `true`},
		{`NULL IS NOT NULL`, `false`},
		// Range conditions.
		{`2 BETWEEN 1 AND 3`, `true`},
		{`1 NOT BETWEEN 2 AND 3`, `true`},
		{`'foo' BETWEEN 'a' AND 'z'`, `true`},
		// Case operator.
		{`CASE WHEN true THEN 1 END`, `1`},
		{`CASE WHEN false THEN 1 END`, `NULL`},
		{`CASE WHEN false THEN 1 ELSE 2 END`, `2`},
		{`CASE WHEN false THEN 1 WHEN false THEN 2 END`, `NULL`},
		{`CASE 1+1 WHEN 1 THEN 1 WHEN 2 THEN 2 END`, `2`},
		{`CASE 1+2 WHEN 1 THEN 1 WHEN 2 THEN 2 ELSE 'doh' END`, `'doh'`},
		// Row (tuple) comparisons.
		{`ROW(1) = ROW(1)`, `true`},
		{`ROW(1, true) = (1, NOT false)`, `true`},
		{`(1, 'a') = (1, 'a')`, `true`},
		{`(1, 'a' || 1) = (1, 'a1')`, `true`},
		{`(1+1, (2+2, (3+3))) = (2, (4, (6)))`, `true`},
		{`(1, 'a') != (1, 'a')`, `false`},
		{`(1, 'a') != (1, 'b')`, `true`},
		// IN and NOT IN expressions.
		{`1 NOT IN (2, 3, 4)`, `true`},
		{`1+1 IN (2, 3, 4)`, `true`},
		{`'a0' IN ('a'||0, 'b'||1, 'c'||2)`, `true`},
		{`(1,2) IN ((0+1,1+1), (3,4), (5,6))`, `true`},
		// Func expressions.
		{`length('hel'||'lo')`, `5`},
		{`lower('HELLO')`, `'hello'`},
		{`UPPER('hello')`, `'HELLO'`},
		// Cast expressions.
		{`true::boolean`, `true`},
		{`true::int`, `1`},
		{`true::float`, `1`},
		{`length(true::text)`, `4`},
		{`false::boolean`, `false`},
		{`false::int`, `0`},
		{`false::float`, `0`},
		{`length(false::text)`, `5`},
		{`1::boolean`, `true`},
		{`0::boolean`, `false`},
		{`1::int`, `1`},
		{`1::float`, `1`},
		{`length(123::text)`, `3`},
		{`1.1::boolean`, `true`},
		{`0.0::boolean`, `false`},
		{`1.1::int`, `1`},
		{`1.1::float`, `1.1`},
		{`length(1.23::text)`, `4`},
		{`'t'::boolean`, `true`},
		{`'T'::boolean`, `true`},
		{`'true'::boolean`, `true`},
		{`'True'::boolean`, `true`},
		{`'TRUE'::boolean`, `true`},
		{`'1'::boolean`, `true`},
		{`'f'::boolean`, `false`},
		{`'F'::boolean`, `false`},
		{`'false'::boolean`, `false`},
		{`'False'::boolean`, `false`},
		{`'FALSE'::boolean`, `false`},
		{`'0'::boolean`, `false`},
		{`'123'::int + 1`, `124`},
		{`'0x123'::int + 1`, `292`},
		{`'0123'::int + 1`, `84`}, // TODO(pmattis): Should we support octal notation?
		{`'1.23'::float + 1.0`, `2.23`},
		{`'hello'::text`, `'hello'`},
		{`CAST('123' AS int) + 1`, `124`},
		{`'hello'::char(2)`, `'he'`},
	}
	for _, d := range testData {
		q, err := Parse("SELECT " + d.expr)
		if err != nil {
			t.Fatalf("%s: %v", d.expr, err)
		}
		expr := q[0].(*Select).Exprs[0].(*NonStarExpr).Expr
		r, err := EvalExpr(expr)
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
		{`a`, `qualified name "a" not found`},
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
		// {`~0 + 1`, `0`},
	}
	for _, d := range testData {
		q, err := Parse("SELECT " + d.expr)
		if err != nil {
			t.Fatalf("%s: %v", d.expr, err)
		}
		expr := q[0].(*Select).Exprs[0].(*NonStarExpr).Expr
		if _, err := EvalExpr(expr); !testutils.IsError(err, d.expected) {
			t.Errorf("%s: expected %s, but found %v", d.expr, d.expected, err)
		}
	}
}
