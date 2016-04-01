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
// permissions and limitations under the License.
//
// Author: Peter Mattis (peter@cockroachlabs.com)

package parser

import (
	"regexp"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/testutils"
)

func TestEval(t *testing.T) {
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
		{`1 + NULL`, `NULL`},
		{`1.1::decimal + 2.4::decimal`, `3.5`},
		{`1.1::decimal - 2.4::decimal`, `-1.3`},
		{`1.1::decimal * 2.4::decimal`, `2.64`},
		{`1.1::decimal % 2.4::decimal`, `1.1`},
		// Division is always done on floats or decimals.
		{`4 / 5`, `0.8`},
		{`1.0 / 0.0`, `+Inf`},
		{`-1.0 * (1.0 / 0.0)`, `-Inf`},
		{`1.1::decimal / 2.2::decimal`, `0.5000000000000000`},
		// Grouping
		{`1 + 2 + (3 * 4)`, `15`},
		// Unary operators.
		{`-3`, `-3`},
		{`-4.1`, `-4.1`},
		{`-6.1::decimal`, `-6.1`},
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
		{`1.1 = 1.2`, `false`},
		{`1.1 != 1.2`, `true`},
		{`1.1 < 1.2`, `true`},
		{`1.1 <= 1.2`, `true`},
		{`1.1 > 1.2`, `false`},
		{`1.1 >= 1.2`, `false`},
		{`1.1::decimal = 1.2::decimal`, `false`},
		{`1.1::decimal != 1.2::decimal`, `true`},
		{`1.1::decimal < 1.2::decimal`, `true`},
		{`1.1::decimal <= 1.2::decimal`, `true`},
		{`1.1::decimal > 1.2::decimal`, `false`},
		{`1.1::decimal >= 1.2::decimal`, `false`},
		{`'2015-10-01'::date = '2015-10-02'::date`, `false`},
		{`'2015-10-01'::date != '2015-10-02'::date`, `true`},
		{`'2015-10-01'::date < '2015-10-02'::date`, `true`},
		{`'2015-10-01'::date <= '2015-10-02'::date`, `true`},
		{`'2015-10-01'::date > '2015-10-02'::date`, `false`},
		{`'2015-10-01'::date >= '2015-10-02'::date`, `false`},
		{`'2015-10-01'::timestamp = '2015-10-02'::timestamp`, `false`},
		{`'2015-10-01'::timestamp != '2015-10-02'::timestamp`, `true`},
		{`'2015-10-01'::timestamp < '2015-10-02'::timestamp`, `true`},
		{`'2015-10-01'::timestamp <= '2015-10-02'::timestamp`, `true`},
		{`'2015-10-01'::timestamp > '2015-10-02'::timestamp`, `false`},
		{`'2015-10-01'::timestamp >= '2015-10-02'::timestamp`, `false`},
		{`'12h2m1s23ms'::interval = '12h2m1s24ms'::interval`, `false`},
		{`'12h2m1s23ms'::interval != '12h2m1s24ms'::interval`, `true`},
		{`'12h2m1s23ms'::interval < '12h2m1s24ms'::interval`, `true`},
		{`'12h2m1s23ms'::interval <= '12h2m1s24ms'::interval`, `true`},
		{`'12h2m1s23ms'::interval > '12h2m1s24ms'::interval`, `false`},
		{`'12h2m1s23ms'::interval >= '12h2m1s24ms'::interval`, `false`},
		// Comparisons against NULL result in NULL.
		{`0 = NULL`, `NULL`},
		{`NULL = NULL`, `NULL`},
		// LIKE and NOT LIKE
		{`'TEST' LIKE 'TEST'`, `true`},
		{`'TEST' LIKE 'TE%'`, `true`},
		{`'TEST' LIKE '%E%'`, `true`},
		{`'TEST' LIKE 'TES_'`, `true`},
		{`'TEST' LIKE 'TE_%'`, `true`},
		{`'TEST' LIKE 'TE_'`, `false`},
		{`'TEST' LIKE '%'`, `true`},
		{`'TEST' LIKE '%R'`, `false`},
		{`'TEST' LIKE 'TESTER'`, `false`},
		{`'TEST' LIKE ''`, `false`},
		{`'' LIKE ''`, `true`},
		{`'T' LIKE '_'`, `true`},
		{`'TE' LIKE '_'`, `false`},
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
		{`1.0::decimal IS OF (DECIMAL)`, `true`},
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
		{`true::float`, `1.0`},
		{`length(true::text)`, `4`},
		{`false::boolean`, `false`},
		{`false::int`, `0`},
		{`false::float`, `0.0`},
		{`true::decimal`, `1`},
		{`false::decimal`, `0`},
		{`length(false::text)`, `5`},
		{`1::boolean`, `true`},
		{`0::boolean`, `false`},
		{`1::int`, `1`},
		{`1::float`, `1.0`},
		{`1::decimal`, `1`},
		{`length(123::text)`, `3`},
		{`1.1::boolean`, `true`},
		{`0.0::boolean`, `false`},
		{`(1.1::decimal)::int`, `1`},
		{`(1.9::decimal)::int`, `2`},
		{`(1.1::decimal)::float`, `1.1`},
		{`(1.1::decimal)::boolean`, `true`},
		{`(0.0::decimal)::boolean`, `false`},
		{`1.1::int`, `1`},
		{`1.5::int`, `2`},
		{`1.9::int`, `2`},
		{`2.5::int`, `2`},
		{`-1.5::int`, `-2`},
		{`-2.5::int`, `-2`},
		{`1.1::float`, `1.1`},
		{`-1e+06`, `-1e+06`},
		{`-9.99999e+05`, `-999999.0`},
		{`999999.0`, `999999.0`},
		{`1000000.0`, `1e+06`},
		{`-1e+06::decimal`, `-1000000`},
		{`-9.99999e+05::decimal`, `-999999`},
		{`999999.0::decimal`, `999999`},
		{`1000000.0::decimal`, `1000000`},
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
		{`NULL::int`, `NULL`},
		{`'0x123'::int + 1`, `292`},
		{`'0123'::int + 1`, `84`},
		{`'1.23'::float + 1.0`, `2.23`},
		{`'hello'::text`, `'hello'`},
		{`CAST('123' AS int) + 1`, `124`},
		{`CAST(NULL AS int)`, `NULL`},
		{`'hello'::char(2)`, `'he'`},
		{`'hello'::bytes`, `b'hello'`},
		{`b'hello'::string`, `'hello'`},
		{`b'\xff'`, `b'\xff'`},
		{`123::text`, `'123'`},
		{`'2010-09-28'::date`, `2010-09-28`},
		{`'2010-09-28T12:00:00Z'::date`, `2010-09-28`},
		{`'2010-09-28'::timestamp`, `2010-09-28 00:00:00+00:00`},
		{`('2010-09-28 12:00:00.1'::timestamp)::date`, `2010-09-28`},
		{`'2010-09-28 12:00:00.1'::timestamp`, `2010-09-28 12:00:00.1+00:00`},
		{`'2010-09-28 12:00:00.1+02:00'::timestamp`, `2010-09-28 10:00:00.1+00:00`},
		{`'2010-09-28 12:00:00.1-07:00'::timestamp`, `2010-09-28 19:00:00.1+00:00`},
		{`'2010-09-28T12:00:00'::timestamp`, `2010-09-28 12:00:00+00:00`},
		{`'2010-09-28T12:00:00Z'::timestamp`, `2010-09-28 12:00:00+00:00`},
		{`'2010-09-28T12:00:00.1'::timestamp`, `2010-09-28 12:00:00.1+00:00`},
		{`('2010-09-28'::date)::timestamp`, `2010-09-28 00:00:00+00:00`},
		{`'12h2m1s23ms'::interval`, `12h2m1.023s`},
		{`1::interval`, `1ns`},
		{`'2010-09-28'::date + 3`, `2010-10-01`},
		{`3 + '2010-09-28'::date`, `2010-10-01`},
		{`'2010-09-28'::date - 3`, `2010-09-25`},
		{`'2010-09-28'::date - '2010-10-21'::date`, `-23`},
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
		// Infinities
		{`'Infinity'::float`, `+Inf`},
		{`'+Infinity'::float`, `+Inf`},
		{`'-Infinity'::float`, `-Inf`},
		{`'Inf'::float`, `+Inf`},
		{`'+Inf'::float`, `+Inf`},
		{`'-Inf'::float`, `-Inf`},
		{`'Inf'::float(4)`, `+Inf`},
		{`'-Inf'::real`, `-Inf`},
		{`'+Infinity'::double precision`, `+Inf`},
		// NaN
		{`'NaN'::float`, `NaN`},
		{`'NaN'::float(4)`, `NaN`},
		{`'NaN'::real`, `NaN`},
		{`'NaN'::double precision`, `NaN`},
	}
	for _, d := range testData {
		expr, err := ParseExprTraditional(d.expr)
		if err != nil {
			t.Fatalf("%s: %v", d.expr, err)
		}
		if expr, err = defaultContext.NormalizeExpr(expr); err != nil {
			t.Fatalf("%s: %v", d.expr, err)
		}
		r, err := expr.Eval(defaultContext)
		if err != nil {
			t.Fatalf("%s: %v", d.expr, err)
		}
		if s := r.String(); d.expected != s {
			t.Errorf("%s: expected %s, but found %s", d.expr, d.expected, s)
		}
	}
}

func TestEvalError(t *testing.T) {
	testData := []struct {
		expr     string
		expected string
	}{
		{`1 % 0`, `zero modulus`},
		{`1 / 0`, `division by zero`},
		{`'2010-09-28 12:00:00.1'::date`,
			`could not parse '2010-09-28 12:00:00.1' in any supported date format`},
		{`'2010-09-28 12:00.1 MST'::timestamp`,
			`could not parse '2010-09-28 12:00.1 MST' in any supported timestamp format`},
		{`'11h2m'::interval / 0`, `division by zero`},
		{`'hello' || b'world'`, `unsupported binary operator: <string> || <bytes>`},
		{`b'\xff\xfe\xfd'::string`, `invalid utf8: "\xff\xfe\xfd"`},
		{`'' LIKE ` + string([]byte{0x27, 0xc2, 0x30, 0x7a, 0xd5, 0x25, 0x30, 0x27}),
			`LIKE regexp compilation failed: error parsing regexp: invalid UTF-8: .*`},
		// TODO(pmattis): Check for overflow.
		// {`~0 + 1`, `0`},
	}
	for _, d := range testData {
		expr, err := ParseExprTraditional(d.expr)
		if err != nil {
			t.Fatalf("%s: %v", d.expr, err)
		}
		if _, err := expr.Eval(defaultContext); !testutils.IsError(err, strings.Replace(regexp.QuoteMeta(d.expected), `\.\*`, `.*`, -1)) {
			t.Errorf("%s: expected %s, but found %v", d.expr, d.expected, err)
		}
	}
}

func TestEvalComparisonExprCaching(t *testing.T) {
	testExprs := []struct {
		op          ComparisonOp
		left, right string
		cacheCount  int
	}{
		// Comparisons.
		{EQ, `0`, `1`, 0},
		{LT, `0`, `1`, 0},
		// LIKE and NOT LIKE
		{Like, `TEST`, `T%T`, 1},
		{NotLike, `TEST`, `%E%T`, 1},
		// SIMILAR TO and NOT SIMILAR TO
		{SimilarTo, `abc`, `(b|c)%`, 1},
		{NotSimilarTo, `abc`, `%(b|d)%`, 1},
	}
	for _, d := range testExprs {
		expr := &ComparisonExpr{
			Operator: d.op,
			Left:     DString(d.left),
			Right:    DString(d.right),
		}
		ctx := defaultContext
		ctx.ReCache = NewRegexpCache(8)
		if _, err := expr.Eval(ctx); err != nil {
			t.Fatalf("%v: %v", d, err)
		}
		if expr.fn.fn == nil {
			t.Errorf("%s: expected the comparison function to be looked up and memoized, but it wasn't", expr)
		}
		if count := ctx.ReCache.Len(); count != d.cacheCount {
			t.Errorf("%s: expected regular expression cache to contain %d compiled patterns, but found %d", expr, d.cacheCount, count)
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

func TestClusterTimestampConversion(t *testing.T) {
	testData := []struct {
		walltime int64
		logical  int32
		expected string
	}{
		{0, 0, "0.0000000000"},
		{42, 0, "42.0000000000"},
		{-42, 0, "-42.0000000000"},
		{42, 69, "42.0000000069"},
		{42, 2147483647, "42.2147483647"},
		{9223372036854775807, 2147483647, "9223372036854775807.2147483647"},
	}

	ctx := defaultContext
	ctx.PrepareOnly = true
	for _, d := range testData {
		ts := roachpb.Timestamp{WallTime: d.walltime, Logical: d.logical}
		ctx.SetClusterTimestamp(ts)
		dec := ctx.GetClusterTimestamp()
		final := dec.String()
		if final != d.expected {
			t.Errorf("expected %s, but found %s", d.expected, final)
		}
	}
}

var benchmarkLikePatterns = []string{
	`test%`,
	`%test%`,
	`%test`,
	``,
	`%`,
	`_`,
	`test`,
	`bad`,
	`also\%`,
}

func benchmarkLike(b *testing.B, ctx EvalContext) {
	likeFn := CmpOps[CmpArgs{Like, stringType, stringType}].fn
	iter := func() {
		for _, p := range benchmarkLikePatterns {
			if _, err := likeFn(ctx, DString("test"), DString(p)); err != nil {
				b.Fatalf("LIKE evaluation failed with error: %v", err)
			}
		}
	}
	// Warm up cache, if applicable
	iter()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		iter()
	}
}

func BenchmarkLikeWithCache(b *testing.B) {
	benchmarkLike(b, EvalContext{ReCache: NewRegexpCache(len(benchmarkLikePatterns))})
}

func BenchmarkLikeWithoutCache(b *testing.B) {
	benchmarkLike(b, EvalContext{})
}
