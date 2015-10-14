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
	"reflect"
	"regexp"
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
		{`1 ^ 3`, `2`},
		// Arithmetic operators.
		{`1 + 1`, `2`},
		{`1 - 2`, `-1`},
		{`3 * 4`, `12`},
		{`3.1 % 2.0`, `1.1`},
		{`5 % 3`, `2`},
		// Division is always done on floats.
		{`4 / 5`, `0.8`},
		{`1.0 / 0.0`, `+Inf`},
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
		{`0xa`, `10`},
		// String concatenation.
		{`'a' || 'b'`, `'ab'`},
		{`'a' || (1 + 2)::char`, `'a3'`},
		// Bit shift operators.
		{`1 << 2`, `4`},
		{`4 >> 2`, `1`},
		// Boolean expressions.
		{`false AND true`, `false`},
		{`false AND NULL`, `false`},
		{`false AND false`, `false`},
		{`true AND true`, `true`},
		{`true AND false`, `false`},
		{`true AND NULL`, `NULL`},
		{`NULL AND true`, `NULL`},
		{`NULL AND false`, `false`},
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
		// LIKE and NOT LIKE
		{`'TEST' LIKE 'TEST'`, `true`},
		{`'TEST' LIKE 'TE%'`, `true`},
		{`'TEST' LIKE '%E%'`, `true`},
		{`'TEST' LIKE 'TES_'`, `true`},
		{`'TEST' LIKE 'TE_'`, `false`},
		{`'TEST' LIKE '%R'`, `false`},
		{`'TEST' LIKE 'TESTER'`, `false`},
		{`'TEST' NOT LIKE '%E%'`, `false`},
		{`'TEST' NOT LIKE 'TES_'`, `false`},
		{`'TEST' NOT LIKE 'TE_'`, `true`},
		// SIMILAR TO and NOT SIMILAR TO
		{`'abc' SIMILAR TO 'abc'`, `true`},
		{`'abc' SIMILAR TO 'a'`, `false`},
		{`'abc' SIMILAR TO '%(b|d)%'`, `true`},
		{`'abc' SIMILAR TO '(b|c)%'`, `false`},
		{`'abc' NOT SIMILAR TO '%(b|d)%'`, `false`},
		{`'abc' NOT SIMILAR TO '(b|c)%'`, `true`},
		// IS DISTINCT FROM can be used to compare NULLs "safely".
		{`0 IS DISTINCT FROM 0`, `false`},
		{`0 IS DISTINCT FROM 1`, `true`},
		{`0 IS DISTINCT FROM NULL`, `true`},
		{`NULL IS DISTINCT FROM NULL`, `false`},
		{`NULL IS DISTINCT FROM 1`, `true`},
		{`0 IS NOT DISTINCT FROM 0`, `true`},
		{`0 IS NOT DISTINCT FROM 1`, `false`},
		{`0 IS NOT DISTINCT FROM NULL`, `false`},
		{`NULL IS NOT DISTINCT FROM NULL`, `true`},
		{`NULL IS NOT DISTINCT FROM 1`, `false`},
		// IS expressions.
		{`0 IS NULL`, `false`},
		{`0 IS NOT NULL`, `true`},
		{`NULL IS NULL`, `true`},
		{`NULL IS NOT NULL`, `false`},
		{`NULL IS UNKNOWN`, `true`},
		{`NULL IS NOT UNKNOWN`, `false`},
		{`TRUE IS TRUE`, `true`},
		{`TRUE IS NOT TRUE`, `false`},
		{`FALSE IS TRUE`, `false`},
		{`FALSE IS NOT TRUE`, `true`},
		{`NULL IS TRUE`, `false`},
		{`NULL IS NOT TRUE`, `true`},
		{`TRUE IS FALSE`, `false`},
		{`TRUE IS NOT FALSE`, `true`},
		{`FALSE IS FALSE`, `true`},
		{`FALSE IS NOT FALSE`, `false`},
		{`NULL IS FALSE`, `false`},
		{`NULL IS NOT FALSE`, `true`},
		// IS OF expressions.
		{`TRUE IS OF (BOOL)`, `true`},
		{`1 IS OF (INT)`, `true`},
		{`1.0 IS OF (FLOAT)`, `true`},
		{`'hello' IS OF (STRING)`, `true`},
		{`'hello' IS OF (BYTES)`, `false`},
		{`b'hello' IS OF (STRING)`, `false`},
		{`b'hello' IS OF (BYTES)`, `true`},
		{`'2012-09-21'::date IS OF (DATE)`, `true`},
		{`'2010-09-28 12:00:00.1'::timestamp IS OF (TIMESTAMP)`, `true`},
		{`'34h'::interval IS OF (INTERVAL)`, `true`},
		{`1 IS OF (STRING)`, `false`},
		{`1 IS OF (BOOL, INT)`, `true`},
		{`1 IS NOT OF (INT)`, `false`},
		{`1 IS NOT OF (STRING)`, `true`},
		{`1 IS NOT OF (BOOL, INT)`, `false`},
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
		{`CASE 1+2 WHEN 1 THEN 1 WHEN 2 THEN 2 ELSE 5 END`, `5`},
		// Row (tuple) comparisons.
		{`ROW(1) = ROW(1)`, `true`},
		{`ROW(1, true) = (1, NOT false)`, `true`},
		{`(1, 'a') = (1, 'a')`, `true`},
		{`(1, 'a' || 1::char) = (1, 'a1')`, `true`},
		{`(1+1, (2+2, (3+3))) = (2, (4, (6)))`, `true`},
		{`(1, 'a') != (1, 'a')`, `false`},
		{`(1, 'a') != (1, 'b')`, `true`},
		// IN and NOT IN expressions.
		{`1 NOT IN (2, 3, 4)`, `true`},
		{`1+1 IN (2, 3, 4)`, `true`},
		{`'a0' IN ('a'||0::char, 'b'||1::char, 'c'||2::char)`, `true`},
		{`'2012-09-21'::date IN ('2012-09-21'::date)`, `true`},
		{`'2010-09-28 12:00:00.1'::timestamp IN ('2010-09-28 12:00:00.1'::timestamp)`, `true`},
		{`'34h'::interval IN ('34h'::interval)`, `true`},
		{`(1,2) IN ((0+1,1+1), (3,4), (5,6))`, `true`},
		{`(1, 2) IN ((2, 1), (3, 4))`, `false`},
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
		{`'0123'::int + 1`, `84`},
		{`'1.23'::float + 1.0`, `2.23`},
		{`'hello'::text`, `'hello'`},
		{`CAST('123' AS int) + 1`, `124`},
		{`'hello'::char(2)`, `'he'`},
		{`'hello'::bytes`, `b'hello'`},
		{`b'hello'::string`, `'hello'`},
		{`b'\xff'`, `b'\xff'`},
		{`123::text`, `'123'`},
		{`'2010-09-28'::date`, `2010-09-28`},
		{`'2010-09-28'::timestamp`, `2010-09-28 00:00:00+00:00`},
		{`('2010-09-28 12:00:00.1'::timestamp)::date`, `2010-09-28`},
		{`'2010-09-28 12:00:00.1'::timestamp`, `2010-09-28 12:00:00.1+00:00`},
		{`'2010-09-28 12:00:00.1+02:00'::timestamp`, `2010-09-28 10:00:00.1+00:00`},
		{`'2010-09-28 12:00:00.1-07:00'::timestamp`, `2010-09-28 19:00:00.1+00:00`},
		{`('2010-09-28'::date)::timestamp`, `2010-09-28 00:00:00+00:00`},
		{`'12h2m1s23ms'::interval`, `12h2m1.023s`},
		{`1::interval`, `1ns`},
		{`'2010-09-28'::date + 3`, `2010-10-01`},
		{`3 + '2010-09-28'::date`, `2010-10-01`},
		{`'2010-09-28'::date - 3`, `2010-09-25`},
		{`'2010-09-28'::date - '2010-10-21'::date`, `-552h0m0s`},
		{`'2010-09-28 12:00:00.1-04:00'::timestamp + '12h2m'::interval`, `2010-09-29 04:02:00.1+00:00`},
		{`'12h2m'::interval + '2010-09-28 12:00:00.1-04:00'::timestamp`, `2010-09-29 04:02:00.1+00:00`},
		{`'2010-09-28 12:00:00.1-04:00'::timestamp - '12h2m'::interval`, `2010-09-28 03:58:00.1+00:00`},
		{`'2010-09-28 12:00:00.1-04:00'::timestamp - '2010-09-28 12:00:00.1+00:00'::timestamp`, `4h0m0s`},
		{`'12h2m1s23ms'::interval + '1h'::interval`, `13h2m1.023s`},
		{`'12h2m1s23ms'::interval - '1h'::interval`, `11h2m1.023s`},
		{`'1h'::interval - '12h2m1s23ms'::interval`, `-11h2m1.023s`},
		{`3 * '1h2m'::interval * 3`, `9h18m0s`},
		{`'3h'::interval / 2`, `1h30m0s`},
		// Conditional expressions.
		{`IF(true, 1, 2/0)`, `1`},
		{`IF(false, 1/0, 2)`, `2`},
		{`IF(NULL, 1/0, 2)`, `2`},
		{`NULLIF(1, 1)`, `NULL`},
		{`NULLIF(1, 2)`, `1`},
		{`NULLIF(2, 1)`, `2`},
		{`IFNULL(1, 2/0)`, `1`},
		{`IFNULL(NULL, 2)`, `2`},
		{`IFNULL(1, NULL)`, `1`},
		{`IFNULL(NULL, NULL)`, `NULL`},
		{`COALESCE(1, 2, 3, 4/0)`, `1`},
		{`COALESCE(NULL, 2, 3, 4/0)`, `2`},
		{`COALESCE(NULL, NULL, NULL, 4)`, `4`},
		{`COALESCE(NULL, NULL, NULL, NULL)`, `NULL`},
	}
	for _, d := range testData {
		q, err := ParseTraditional("SELECT " + d.expr)
		if err != nil {
			t.Fatalf("%s: %v", d.expr, err)
		}
		expr := q[0].(*Select).Exprs[0].Expr
		if expr, err = defaultContext.NormalizeExpr(expr); err != nil {
			t.Fatalf("%s: %v", d.expr, err)
		}
		r, err := defaultContext.EvalExpr(expr)
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
		{`1 / 0`, `division by zero`},
		{`'2010-09-28 12:00:00.1'::date`, `parsing time "2010-09-28 12:00:00.1": extra text`},
		{`'2010-09-28 12:00.1 MST'::timestamp`, `parsing time "2010-09-28 12:00.1 MST" as "2006-01-02 15:04:05.999999999 MST": cannot parse ".1 MST" as ":"`},
		{`'11h2m'::interval / 0`, `division by zero`},
		{`'hello' || b'world'`, `unsupported binary operator: <string> || <bytes>`},
		{`b'\xff\xfe\xfd'::string`, `invalid utf8: "\xff\xfe\xfd"`},
		// TODO(pmattis): Check for overflow.
		// {`~0 + 1`, `0`},
	}
	for _, d := range testData {
		q, err := ParseTraditional("SELECT " + d.expr)
		if err != nil {
			t.Fatalf("%s: %v", d.expr, err)
		}
		expr := q[0].(*Select).Exprs[0].Expr
		if _, err := defaultContext.EvalExpr(expr); !testutils.IsError(err, regexp.QuoteMeta(d.expected)) {
			t.Errorf("%s: expected %s, but found %v", d.expr, d.expected, err)
		}
	}
}

func TestEvalComparisonExprCaching(t *testing.T) {
	regexpType := reflect.TypeOf(&regexp.Regexp{})
	testExprs := []struct {
		op          ComparisonOp
		left, right string
		cacheType   reflect.Type
	}{
		// Comparisons.
		{EQ, `0`, `1`, nil},
		{LT, `0`, `1`, nil},
		// LIKE and NOT LIKE
		{Like, `TEST`, `T%T`, regexpType},
		{NotLike, `TEST`, `%E%`, regexpType},
		// SIMILAR TO and NOT SIMILAR TO
		{SimilarTo, `abc`, `(b|c)%`, regexpType},
		{NotSimilarTo, `abc`, `%(b|d)%`, regexpType},
	}
	for _, d := range testExprs {
		expr := &ComparisonExpr{
			Operator: d.op,
			Left:     DString(d.left),
			Right:    DString(d.right),
		}
		if _, err := defaultContext.EvalExpr(expr); err != nil {
			t.Fatalf("%s: %v", d, err)
		}
		if expr.fn.fn == nil {
			t.Errorf("%s: expected the comparison function to be looked up and memoized, but it wasn't", expr)
		}
		if d.cacheType != nil {
			if expr.cache == nil {
				t.Errorf("%s: expected expression cache population, but found an empty cache", expr)
			} else if ty := reflect.TypeOf(expr.cache); ty != d.cacheType {
				t.Errorf("%s: expected expression cache to have type %v, but found %v", expr, d.cacheType, ty)
			}
		} else {
			if expr.cache != nil {
				t.Errorf("%s: expected no expression cache population, but found %v", expr, expr.cache)
			}
		}
	}
}

func TestSimilarEscape(t *testing.T) {
	testData := []struct {
		expr     string
		expected string
	}{
		{`test`, `test`},
		{`test%`, `test.*`},
		{`_test_`, `.test.`},
		{`_%*`, `..**`},
		{`[_%]*`, `[_%]*`},
		{`.^$`, `\.\^\$`},
		{`%(b|d)%`, `.*(?:b|d).*`},
		{`se\"arch\"[\"]`, `se(arch)[\"]`},
	}
	for _, d := range testData {
		s := SimilarEscape(d.expr)
		if s != d.expected {
			t.Errorf("%s: expected %s, but found %v", d.expr, d.expected, s)
		}
	}
}
