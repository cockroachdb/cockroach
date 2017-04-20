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
	"fmt"
	"regexp"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

func TestEval(t *testing.T) {
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
		{`9 // 2`, `4`},
		{`-5 // 3`, `-1`},
		{`4.5 // 2`, `2`},
		{`-4.5 // 1.2`, `-3`},
		{`3.1 % 2.0`, `1.1`},
		{`5 % 3`, `2`},
		{`1 + NULL`, `NULL`},
		{`1.1 + 2.4`, `3.5`},
		{`1.1 - 2.4`, `-1.3`},
		{`1.1 * 2.4`, `2.64`},
		{`1.1 % 2.4`, `1.1`},
		{`4.1 // 2.4`, `1`},
		{`-4.5:::float // 1.2:::float`, `-3.0`},
		{`2 ^ 3`, `8`},
		{`2:::float ^ 3:::float`, `8.0`},
		{`2:::decimal ^ 3:::decimal`, `8`},
		{`2:::int ^ 62:::int`, `4611686018427387904`},
		// Various near-edge cases for overflow.
		{`0:::int * 0:::int`, `0`},
		{`0:::int * 1:::int`, `0`},
		{`1:::int * 0:::int`, `0`},
		{`1:::int * 1:::int`, `1`},
		{`4611686018427387904:::int * 1:::int`, `4611686018427387904`},
		{`-4611686018427387905:::int * 1:::int`, `-4611686018427387905`},
		// Heterogeneous int/decimal arithmetic is valid.
		{`1.1:::decimal + 2:::int`, `3.1`},
		{`1.1:::decimal - 2:::int`, `-0.9`},
		{`1.1:::decimal * 2:::int`, `2.2`},
		{`1.1:::decimal % 2:::int`, `1.1`},
		{`4.1:::decimal // 2:::int`, `2`},
		{`1.1:::decimal ^ 2:::int`, `1.21`},
		{`2:::int +  2.1:::decimal`, `4.1`},
		{`2:::int -  2.1:::decimal`, `-0.1`},
		{`2:::int *  2.1:::decimal`, `4.2`},
		{`2:::int %  2.1:::decimal`, `2.0`},
		{`4:::int // 2.1:::decimal`, `1`},
		{`2:::int ^ 2.1:::decimal`, `4.2870938501451726569`},
		// Division is always done on floats or decimals.
		{`4 / 5`, `0.8`},
		{`1.1:::decimal / 2.2:::decimal`, `0.5`},
		{`1:::int / 2.2:::decimal`, `0.45454545454545454545`},
		{`1.1:::decimal / 2:::int`, `0.55`},
		// Verify INT_MIN / -1 = -INT_MIN.
		{`(-9223372036854775807:::int - 1) / -1`, `9223372036854775808`},
		// Infinity.
		{`1.0:::float / 0.0`, `+Inf`},
		{`-1.0:::float * (1.0:::float / 0.0)`, `-Inf`},
		{`power(0::decimal, -1)`, `Infinity`},
		// Grouping
		{`1 + 2 + (3 * 4)`, `15`},
		// Unary operators.
		{`-3`, `-3`},
		{`-4.1`, `-4.1`},
		{`-6.1:::float`, `-6.1`},
		// Ones complement operates on signed integers.
		{`~0`, `-1`},
		{`~0 - 1`, `-2`},
		// Hexadecimal numbers.
		{`0xa`, `10`},
		{`0xcafe1111`, `3405648145`},
		// Hexadecimal bytes literals.
		{`x'636174'`, `b'cat'`},
		{`X'636174'`, `b'cat'`},
		{`x'636174'::string`, `'cat'`},
		// String concatenation.
		{`'a' || 'b'`, `'ab'`},
		{`'a' || (1 + 2)::char`, `'a3'`},
		{`b'hello' || 'world'`, `b'helloworld'`},
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
		{`false AND (2 = 1)`, `false`},
		{`true OR (3 = 1)`, `true`},
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
		{`'2015-10-01'::date = '2015-10-01'::date`, `true`},
		{`'2016-07-19 +0:0:0'::date = '2016-07-19'::date`, `true`},
		{`'2016-7-19 +0:0:0'::date = '2016-07-19'::date`, `true`},
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
		{`'P1Y2M10DT2H30M'::interval = 'P1Y2M10DT2H31M'::interval`, `false`},
		{`'P1Y2M10DT2H30M'::interval != 'P1Y2M10DT2H31M'::interval`, `true`},
		{`'P1Y2M10DT2H29M'::interval < 'P1Y2M10DT2H30M'::interval`, `true`},
		{`'P1Y2M10DT2H29M'::interval <= 'P1Y2M10DT2H30M'::interval`, `true`},
		{`'P1Y2M10DT2H29M'::interval > 'P1Y2M10DT2H30M'::interval`, `false`},
		{`'P1Y2M10DT2H29M'::interval >= 'P1Y2M10DT2H30M'::interval`, `false`},
		{`'1-2 10 2:30'::interval = 'P1Y2M10DT2H31M'::interval`, `false`},
		{`'1-2 10 2:30'::interval != 'P1Y2M10DT2H31M'::interval`, `true`},
		{`'1-2 10 2:29'::interval < 'P1Y2M10DT2H30M'::interval`, `true`},
		{`'1-2 10 2:29'::interval <= 'P1Y2M10DT2H30M'::interval`, `true`},
		{`'1-2 10 2:29'::interval > 'P1Y2M10DT2H30M'::interval`, `false`},
		{`'1-2 10 2:29'::interval >= 'P1Y2M10DT2H30M'::interval`, `false`},
		{`'1 year 2 months 3 days 4 hours 5 minutes 6 seconds'::interval = '1 year 2 months 3 days 4 hours 5 minutes 7 seconds'::interval`, `false`},
		{`'1 year 2 months 3 days 4 hours 5 minutes 6 seconds'::interval != '1 year 2 months 3 days 4 hours 5 minutes 7 seconds'::interval`, `true`},
		{`'1 year 2 months 3 days 4 hours 5 minutes 6 seconds'::interval < '1 year 2 months 3 days 4 hours 5 minutes 7 seconds'::interval`, `true`},
		{`'1 year 2 months 3 days 4 hours 5 minutes 6 seconds'::interval <= '1 year 2 months 3 days 4 hours 5 minutes 7 seconds'::interval`, `true`},
		{`'1 year 2 months 3 days 4 hours 5 minutes 6 seconds'::interval > '1 year 2 months 3 days 4 hours 5 minutes 7 seconds'::interval`, `false`},
		{`'1 year 2 months 3 days 4 hours 5 minutes 6 seconds'::interval >= '1 year 2 months 3 days 4 hours 5 minutes 7 seconds'::interval`, `false`},
		{`'5 minutes 6 seconds'::interval = '5 minutes 6 seconds'::interval`, `true`},
		{`'PT2H30M'::interval = 'PT2H30M'::interval`, `true`},
		// Comparisons against NULL result in NULL.
		{`0 = NULL`, `NULL`},
		{`0 < NULL`, `NULL`},
		{`0 <= NULL`, `NULL`},
		{`NULL = 0`, `NULL`},
		{`NULL < 0`, `NULL`},
		{`NULL <= 0`, `NULL`},
		{`0.1 = NULL`, `NULL`},
		{`0.1 < NULL`, `NULL`},
		{`0.1 <= NULL`, `NULL`},
		{`NULL = 0.1`, `NULL`},
		{`NULL < 0.1`, `NULL`},
		{`NULL <= 0.1`, `NULL`},
		{`0.1::float = NULL`, `NULL`},
		{`0.1::float < NULL`, `NULL`},
		{`0.1::float <= NULL`, `NULL`},
		{`NULL = 0.1::float`, `NULL`},
		{`NULL < 0.1::float`, `NULL`},
		{`NULL <= 0.1::float`, `NULL`},
		{`true = NULL`, `NULL`},
		{`true < NULL`, `NULL`},
		{`true <= NULL`, `NULL`},
		{`NULL = true`, `NULL`},
		{`NULL < true`, `NULL`},
		{`NULL <= true`, `NULL`},
		{`'a' = NULL`, `NULL`},
		{`'a' < NULL`, `NULL`},
		{`'a' <= NULL`, `NULL`},
		{`NULL = 'a'`, `NULL`},
		{`NULL < 'a'`, `NULL`},
		{`NULL <= 'a'`, `NULL`},
		{`'2015-10-01'::date = NULL`, `NULL`},
		{`'2015-10-01'::date < NULL`, `NULL`},
		{`'2015-10-01'::date <= NULL`, `NULL`},
		{`NULL = '2015-10-01'::date`, `NULL`},
		{`NULL < '2015-10-01'::date`, `NULL`},
		{`NULL <= '2015-10-01'::date`, `NULL`},
		{`'2015-10-01'::timestamp = NULL`, `NULL`},
		{`'2015-10-01'::timestamp < NULL`, `NULL`},
		{`'2015-10-01'::timestamp <= NULL`, `NULL`},
		{`NULL = '2015-10-01'::timestamp`, `NULL`},
		{`NULL < '2015-10-01'::timestamp`, `NULL`},
		{`NULL <= '2015-10-01'::timestamp`, `NULL`},
		{`'2015-10-01'::timestamptz = NULL`, `NULL`},
		{`'2015-10-01'::timestamptz < NULL`, `NULL`},
		{`'2015-10-01'::timestamptz <= NULL`, `NULL`},
		{`NULL = '2015-10-01'::timestamptz`, `NULL`},
		{`NULL < '2015-10-01'::timestamptz`, `NULL`},
		{`NULL <= '2015-10-01'::timestamptz`, `NULL`},
		{`'1-2 10 2:30'::interval = NULL`, `NULL`},
		{`'1-2 10 2:30'::interval < NULL`, `NULL`},
		{`'1-2 10 2:30'::interval <= NULL`, `NULL`},
		{`NULL = '1-2 10 2:30'::interval`, `NULL`},
		{`NULL < '1-2 10 2:30'::interval`, `NULL`},
		{`NULL <= '1-2 10 2:30'::interval`, `NULL`},
		{`NULL = NULL`, `NULL`},
		{`NULL < NULL`, `NULL`},
		{`NULL <= NULL`, `NULL`},
		// LIKE and NOT LIKE
		{`'TEST' LIKE 'TEST'`, `true`},
		{`'TEST' LIKE 'test'`, `false`},
		{`'TEST' LIKE 'TE%'`, `true`},
		{`'TEST' LIKE '%E%'`, `true`},
		{`'TEST' LIKE '%e%'`, `false`},
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
		{`'TEST' NOT LIKE 'TeS_'`, `true`},
		{`'TEST' NOT LIKE 'TE_'`, `true`},
		// ILIKE and NOT ILIKE
		{`'TEST' ILIKE 'TEST'`, `true`},
		{`'TEST' ILIKE 'test'`, `true`},
		{`'TEST' ILIKE 'TE%'`, `true`},
		{`'TEST' ILIKE '%E%'`, `true`},
		{`'TEST' ILIKE '%e%'`, `true`},
		{`'TEST' ILIKE 'TES_'`, `true`},
		{`'TEST' ILIKE 'TE_%'`, `true`},
		{`'TEST' ILIKE 'TE_'`, `false`},
		{`'TEST' ILIKE '%'`, `true`},
		{`'TEST' ILIKE '%R'`, `false`},
		{`'TEST' ILIKE 'TESTER'`, `false`},
		{`'TEST' ILIKE 'tester'`, `false`},
		{`'TEST' ILIKE ''`, `false`},
		{`'' ILIKE ''`, `true`},
		{`'T' ILIKE '_'`, `true`},
		{`'TE' ILIKE '_'`, `false`},
		{`'TEST' NOT ILIKE '%E%'`, `false`},
		{`'TEST' NOT ILIKE 'TES_'`, `false`},
		{`'TEST' NOT ILIKE 'TeS_'`, `false`},
		{`'TEST' NOT ILIKE 'TE_'`, `true`},
		// SIMILAR TO and NOT SIMILAR TO
		{`'abc' SIMILAR TO 'abc'`, `true`},
		{`'abc' SIMILAR TO 'a'`, `false`},
		{`'abc' SIMILAR TO '%(b|d)%'`, `true`},
		{`'abc' SIMILAR TO '(b|c)%'`, `false`},
		{`'abc' NOT SIMILAR TO '%(b|d)%'`, `false`},
		{`'abc' NOT SIMILAR TO '(b|c)%'`, `true`},
		// ~ and !~
		{`'TEST' ~ 'TEST'`, `true`},
		{`'TEST' ~ 'test'`, `false`},
		{`'TEST' ~ 'TE.*'`, `true`},
		{`'TEST' ~ '.*E.*'`, `true`},
		{`'TEST' ~ '.*e.*'`, `false`},
		{`'TEST' ~ 'TES.'`, `true`},
		{`'TEST' ~ '^TE[a-z]{2}$'`, `false`},
		{`'TEST' ~ 'TE.+'`, `true`},
		{`'TEST' ~ 'TE.$'`, `false`},
		{`'TEST' ~ '.*'`, `true`},
		{`'TEST' ~ '.+'`, `true`},
		{`'' ~ '.+'`, `false`},
		{`'TEST' ~ '.*R'`, `false`},
		{`'TEST' ~ 'TESTER'`, `false`},
		{`'TEST' ~ ''`, `true`},
		{`'TEST' ~ '^$'`, `false`},
		{`'' ~ ''`, `true`},
		{`'T' ~ '^.$'`, `true`},
		{`'TE' ~ '^.$'`, `false`},
		{`'T' ~ '^.*$'`, `true`},
		{`'TE' ~ '^.*$'`, `true`},
		{`'T' ~ '[a-z]'`, `false`},
		{`'T' ~ '[a-zA-Z]'`, `true`},
		{`'TEST' !~ '.E.{2}'`, `false`},
		{`'TEST' !~ '.e.{2}'`, `true`},
		{`'TEST' !~ 'TES.'`, `false`},
		{`'TEST' !~ 'TeST'`, `true`},
		{`'TEST' !~ 'TESV'`, `true`},
		{`'TEST' !~ 'TE.'`, `false`},
		// ~* and !~*
		{`'TEST' ~* 'TEST'`, `true`},
		{`'TEST' ~* 'test'`, `true`},
		{`'TEST' ~* 'Te'`, `true`},
		{`'TEST' ~* '^Te$'`, `false`},
		{`'TEST' ~* '^Te.*$'`, `true`},
		{`'TEST' ~* '.*E.*'`, `true`},
		{`'TEST' ~* '.*e.*'`, `true`},
		{`'TEST' ~* 'TES'`, `true`},
		{`'TEST' ~* '^TE[a-z]{2}$'`, `true`},
		{`'TEST' ~* '.*'`, `true`},
		{`'TEST' ~* '.*R'`, `false`},
		{`'TEST' ~* 'TESTER'`, `false`},
		{`'TEST' ~* 'tester'`, `false`},
		{`'TEST' ~* ''`, `true`},
		{`'TEST' ~* '^$'`, `false`},
		{`'' ~* ''`, `true`},
		{`'T' ~* '[a-z]'`, `true`},
		{`'T' ~* '[a-zA-Z]'`, `true`},
		{`'TE' ~* '.'`, `true`},
		{`'TEST' !~* '.E.{2}'`, `false`},
		{`'TEST' !~* '.e.{2}'`, `false`},
		{`'TEST' !~* 'TES.'`, `false`},
		{`'TEST' !~* 'TeST'`, `false`},
		{`'TEST' !~* 'TESV'`, `true`},
		{`'TEST' !~* 'TE.'`, `false`},
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
		{`1.0 IS OF (FLOAT)`, `false`},
		{`1.0 IS OF (DECIMAL)`, `true`},
		{`1.0::float IS OF (FLOAT)`, `true`},
		{`1.0::decimal IS OF (DECIMAL)`, `true`},
		{`'hello' IS OF (STRING)`, `true`},
		{`'hello' IS OF (BYTES)`, `false`},
		{`b'hello' IS OF (STRING)`, `false`},
		{`b'hello' IS OF (BYTES)`, `true`},
		{`'2012-09-21'::date IS OF (DATE)`, `true`},
		{`'2010-09-28 12:00:00.1'::timestamp IS OF (TIMESTAMP)`, `true`},
		{`'34h'::interval IS OF (INTERVAL)`, `true`},
		{`'P1Y2M10DT2H29M'::interval IS OF (INTERVAL)`, `true`},
		{`'1-2'::interval IS OF (INTERVAL)`, `true`},
		{`'1-2 3'::interval IS OF (INTERVAL)`, `true`},
		{`'2 4:09'::interval IS OF (INTERVAL)`, `true`},
		{`'2 4:09:57'::interval IS OF (INTERVAL)`, `true`},
		{`'1-2 4:09'::interval IS OF (INTERVAL)`, `true`},
		{`'1-2 3 4:09'::interval IS OF (INTERVAL)`, `true`},
		{`'1-2 4:09:55'::interval IS OF (INTERVAL)`, `true`},
		{`'1-2 3 4:09:55'::interval IS OF (INTERVAL)`, `true`},
		{`'1 hour 2 minutes'::interval IS OF (INTERVAL)`, `true`},
		// #13716
		{`'1 HOUR 2 MINutES'::interval IS OF (INTERVAL)`, `true`},
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
		{`2.2 IN (2.1, 2.2, 2.3)`, `true`},
		{`2.0 IN (2.1, 2.2, 2.3)`, `false`},
		{`'a0' IN ('a'||0::char, 'b'||1::char, 'c'||2::char)`, `true`},
		{`'2012-09-21'::date IN ('2012-09-21'::date)`, `true`},
		{`'2010-09-28 12:00:00.1'::timestamp IN ('2010-09-28 12:00:00.1'::timestamp)`, `true`},
		{`'34h'::interval IN ('34h'::interval)`, `true`},
		{`(1,2) IN ((0+1,1+1), (3,4), (5,6))`, `true`},
		{`(1, 2) IN ((2, 1), (3, 4))`, `false`},
		// ANY, SOME, and ALL expressions.
		{`1   = ANY ARRAY[]`, `false`},
		{`1   = ANY (ARRAY[2, 3, 4])`, `false`},
		{`1   = ANY (ARRAY[1, 3, 4])`, `true`},
		{`1+1 = ANY (ARRAY[2, 3, 4])`, `true`},
		{`1+1 = ANY  (ARRAY[1, 3, 4])`, `false`},
		{`1+1 = SOME (ARRAY[1, 3, 4])`, `false`},
		{`1   = ALL (ARRAY[])`, `true`},
		{`1   = ALL (ARRAY[1, 1, 1])`, `true`},
		{`1+1 = ALL (ARRAY[2, 3, 4])`, `false`},
		{`1+1 = ALL (ARRAY[2, 2, 2])`, `true`},
		{`1 = ANY (ARRAY[1, 2, NULL])`, `true`},
		{`1 = ANY (ARRAY[2, 3, NULL])`, `NULL`},
		{`1 = ANY (ARRAY[NULL])`, `NULL`},
		{`1 = ALL (ARRAY[1, 1, NULL])`, `NULL`},
		{`1 = ALL (ARRAY[1, 2, NULL])`, `false`},
		{`1 = ALL (ARRAY[NULL])`, `NULL`},
		{`1 =  ANY (ARRAY[1, 3, 5])`, `true`},
		{`1 <  ANY (ARRAY[1, 3, 5])`, `true`},
		{`1 >  ANY (ARRAY[1, 3, 5])`, `false`},
		{`1 <= ANY (ARRAY[1, 3, 5])`, `true`},
		{`1 >= ANY (ARRAY[1, 3, 5])`, `true`},
		{`5 =  ANY (ARRAY[1, 3, 5])`, `true`},
		{`5 <  ANY (ARRAY[1, 3, 5])`, `false`},
		{`5 >  ANY (ARRAY[1, 3, 5])`, `true`},
		{`5 <= ANY (ARRAY[1, 3, 5])`, `true`},
		{`5 >= ANY (ARRAY[1, 3, 5])`, `true`},
		{`'AAA' LIKE ANY (ARRAY['%A%', '%B%'])`, `true`},
		{`'CCC' LIKE ANY (ARRAY['%A%', '%B%'])`, `false`},
		{`'AAA' NOT LIKE ANY (ARRAY['%A%', '%B%'])`, `true`},
		{`'AAA' NOT LIKE ANY (ARRAY['%A%', '%A%'])`, `false`},
		{`'aaa' ILIKE ANY (ARRAY['%A%', '%B%'])`, `true`},
		{`'ccc' ILIKE ANY (ARRAY['%A%', '%B%'])`, `false`},
		{`'aaa' NOT ILIKE ANY (ARRAY['%A%', '%B%'])`, `true`},
		{`'aaa' NOT ILIKE ANY (ARRAY['%A%', '%A%'])`, `false`},
		// Func expressions.
		{`length('hel'||'lo')`, `5`},
		{`lower('HELLO')`, `'hello'`},
		{`UPPER('hello')`, `'HELLO'`},
		// Array constructors.
		{`ARRAY[]:::int[]`, `{}`},
		{`ARRAY[NULL]`, `{NULL}`},
		{`ARRAY[1, 2, 3]`, `{1,2,3}`},
		{`ARRAY['a', 'b', 'c']`, `{'a','b','c'}`},
		{`ARRAY[ARRAY[1, 2], ARRAY[2, 3]]`, `{{1,2},{2,3}}`},
		{`ARRAY[1, NULL]`, `{1,NULL}`},
		// Array sizes.
		{`array_length(ARRAY[1, 2, 3], 1)`, `3`},
		{`array_length(ARRAY[1, 2, 3], 2)`, `NULL`},
		{`array_length(ARRAY[1, 2, 3], 0)`, `NULL`},
		{`array_length(ARRAY[1, 2, 3], -10)`, `NULL`},
		{`array_length(ARRAY[ARRAY[1, 2, 3], ARRAY[1, 2, 3]], 1)`, `2`},
		{`array_length(ARRAY[ARRAY[1, 2, 3], ARRAY[1, 2, 3]], 2)`, `3`},
		{`array_length(ARRAY[ARRAY[1, 2, 3], ARRAY[1, 2, 3]], 3)`, `NULL`},
		{`array_lower(ARRAY[1, 2, 3], 1)`, `1`},
		{`array_lower(ARRAY[1, 2, 3], 2)`, `NULL`},
		{`array_lower(ARRAY[1, 2, 3], 0)`, `NULL`},
		{`array_lower(ARRAY[1, 2, 3], -10)`, `NULL`},
		{`array_lower(ARRAY[ARRAY[1, 2, 3], ARRAY[1, 2, 3]], 1)`, `1`},
		{`array_lower(ARRAY[ARRAY[1, 2, 3], ARRAY[1, 2, 3]], 2)`, `1`},
		{`array_lower(ARRAY[ARRAY[1, 2, 3], ARRAY[1, 2, 3]], 3)`, `NULL`},
		{`array_upper(ARRAY[1, 2, 3], 1)`, `3`},
		{`array_upper(ARRAY[1, 2, 3], 2)`, `NULL`},
		{`array_upper(ARRAY[1, 2, 3], 0)`, `NULL`},
		{`array_upper(ARRAY[1, 2, 3], -10)`, `NULL`},
		{`array_upper(ARRAY[ARRAY[1, 2, 3], ARRAY[1, 2, 3]], 1)`, `2`},
		{`array_upper(ARRAY[ARRAY[1, 2, 3], ARRAY[1, 2, 3]], 2)`, `3`},
		{`array_upper(ARRAY[ARRAY[1, 2, 3], ARRAY[1, 2, 3]], 3)`, `NULL`},
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
		{`(1e300::decimal)::float`, `1e+300`},
		{`(9223372036854775807::decimal)::int`, `9223372036854775807`},
		// The two largest floats that can be converted to an int, albeit inexactly.
		{`9223372036854775295::float::int`, `9223372036854774784`},
		{`-9223372036854775295::float::int`, `-9223372036854774784`},
		{`1.1::int`, `1`},
		{`1.5::int`, `2`},
		{`1.9::int`, `2`},
		{`2.5::int`, `3`},
		{`3.5::int`, `4`},
		{`-1.5::int`, `-2`},
		{`-2.5::int`, `-3`},
		{`1.1::float`, `1.1`},
		{`-1e+06::float`, `-1e+06`},
		{`-9.99999e+05`, `-999999`},
		{`999999.0`, `999999.0`},
		{`1000000.0`, `1000000.0`},
		{`-1e+06`, `-1000000`},
		{`-9.99999e+05::decimal`, `-999999`},
		{`999999.0::decimal`, `999999.0`},
		{`1000000.0::decimal`, `1000000.0`},
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
		{`date '2010-09-28'`, `'2010-09-28'`},
		{`CAST('2010-09-28' AS date)`, `'2010-09-28'`},
		{`'2010-09-28'::date`, `'2010-09-28'`},
		{`'2010-09-28'::date::text`, `'2010-09-28'`},
		{`('2010-09-28'::date)::date`, `'2010-09-28'`},
		{`'2010-09-28T12:00:00Z'::date`, `'2010-09-28'`},
		{`timestamp '2010-09-28'`, `'2010-09-28 00:00:00+00:00'`},
		{`CAST('2010-09-28' AS timestamp)`, `'2010-09-28 00:00:00+00:00'`},
		{`'2010-09-28'::timestamp`, `'2010-09-28 00:00:00+00:00'`},
		{`timestamptz '2010-09-28'`, `'2010-09-28 00:00:00+00:00'`},
		{`CAST('2010-09-28' AS timestamptz)`, `'2010-09-28 00:00:00+00:00'`},
		{`'2010-09-28'::timestamptz`, `'2010-09-28 00:00:00+00:00'`},
		{`('2010-09-28 12:00:00.1'::timestamp)::date`, `'2010-09-28'`},
		{`'2010-09-28 12:00:00.1'::timestamp`, `'2010-09-28 12:00:00.1+00:00'`},
		{`'2010-09-28 12:00:00.1+02:00'::timestamp`, `'2010-09-28 10:00:00.1+00:00'`},
		{`'2010-09-28 12:00:00.524000 +02:00:00'::timestamp`, `'2010-09-28 10:00:00.524+00:00'`},
		{`'2010-09-28 12:00:00.1-07:00'::timestamp`, `'2010-09-28 19:00:00.1+00:00'`},
		{`'2010-09-28T12:00:00'::timestamp`, `'2010-09-28 12:00:00+00:00'`},
		{`'2010-09-28T12:00:00Z'::timestamp`, `'2010-09-28 12:00:00+00:00'`},
		{`'2010-09-28T12:00:00.1'::timestamp`, `'2010-09-28 12:00:00.1+00:00'`},
		{`('2010-09-28'::date)::timestamp`, `'2010-09-28 00:00:00+00:00'`},
		{`'2010-09-28 12:00:00.1-04'::timestamp`, `'2010-09-28 16:00:00.1+00:00'`},
		{`'2010-09-28 12:00:00.1-04'::timestamp::text`, `'2010-09-28 16:00:00.1+00:00'`},
		{`'2010-09-28 12:00:00.1-04'::timestamptz::text`, `'2010-09-28 16:00:00.1+00:00'`},
		{`'12h2m1s23ms'::interval`, `'12h2m1s23ms'`},
		{`'12h2m1s23ms'::interval::text`, `'12h2m1s23ms'`},
		{`interval '1'`, `'1s'`},
		{`CAST('1' AS interval)`, `'1s'`},
		{`'1'::interval`, `'1s'`},
		{`1::interval`, `'1µs'`},
		{`(1::interval)::interval`, `'1µs'`},
		{`'2010-09-28'::date + 3`, `'2010-10-01'`},
		{`3 + '2010-09-28'::date`, `'2010-10-01'`},
		{`'2010-09-28'::date - 3`, `'2010-09-25'`},
		{`'2010-09-28'::date - '2010-10-21'::date`, `-23`},
		{`'2010-09-28 12:00:00.1-04:00'::timestamp + '12h2m'::interval`, `'2010-09-29 04:02:00.1+00:00'`},
		{`'12h2m'::interval + '2010-09-28 12:00:00.1-04:00'::timestamp`, `'2010-09-29 04:02:00.1+00:00'`},
		{`'12 hours 2 minutes'::interval + '2010-09-28 12:00:00.1-04:00'::timestamp`, `'2010-09-29 04:02:00.1+00:00'`},
		{`'PT12H2M'::interval + '2010-09-28 12:00:00.1-04:00'::timestamp`, `'2010-09-29 04:02:00.1+00:00'`},
		{`'12:2'::interval + '2010-09-28 12:00:00.1-04:00'::timestamp`, `'2010-09-29 04:02:00.1+00:00'`},
		{`'2010-09-28 12:00:00.1-04:00'::timestamp - '12h2m'::interval`, `'2010-09-28 03:58:00.1+00:00'`},
		{`'2010-09-28 12:00:00.1-04:00'::timestamp - '12 hours 2 minutes'::interval`, `'2010-09-28 03:58:00.1+00:00'`},
		{`'2010-09-28 12:00:00.1-04:00'::timestamp - 'PT12H2M'::interval`, `'2010-09-28 03:58:00.1+00:00'`},
		{`'2010-09-28 12:00:00.1-04:00'::timestamp - '12:2'::interval`, `'2010-09-28 03:58:00.1+00:00'`},
		{`'2010-09-28 12:00:00.1-04:00'::timestamp - '2010-09-28 12:00:00.1+00:00'::timestamp`, `'4h'`},
		{`'1970-01-01 00:01:00.123456-00:00'::timestamp::int`, `60`},
		{`'1970-01-01 00:01:00.123456-00:00'::timestamptz::int`, `60`},
		{`'1970-01-10'::date::int`, `9`},
		{`'3h3us'::interval::int`, `10800`},
		{`'1970-01-01 00:01:00.123456-00:00'::timestamp::decimal`, `60.123456`},
		{`'1970-01-01 00:01:00.123456-00:00'::timestamptz::decimal`, `60.123456`},
		{`'1970-01-10'::date::decimal`, `9`},
		{`'3h3us'::interval::decimal`, `10800.000003`},
		{`'1970-01-01 00:01:00.123456-00:00'::timestamp::float`, `60.123456`},
		{`'1970-01-01 00:01:00.123456-00:00'::timestamptz::float`, `60.123456`},
		{`'1970-01-10'::date::float`, `9.0`},
		{`'3h3us'::interval::float`, `1.0800000003e+16`},
		{`10::int::date`, `'1970-01-11'`},
		{`10::int::timestamp`, `'1970-01-01 00:00:10+00:00'`},
		{`10::int::timestamptz`, `'1970-01-01 00:00:10+00:00'`},
		{`10123456::int::interval`, `'10s123ms456µs'`},
		// Type annotation expressions.
		{`ANNOTATE_TYPE('s', string)`, `'s'`},
		{`ANNOTATE_TYPE('s', bytes)`, `b's'`},
		{`ANNOTATE_TYPE('2010-09-28', date)`, `'2010-09-28'`},
		{`ANNOTATE_TYPE('PT12H2M', interval)`, `'12h2m'`},
		{`ANNOTATE_TYPE('2 02:12', interval)`, `'2d2h12m'`},
		{`ANNOTATE_TYPE('2 02:12:34', interval)`, `'2d2h12m34s'`},
		{`ANNOTATE_TYPE('1-2 02:12', interval)`, `'1y2mon2h12m'`},
		{`ANNOTATE_TYPE('2010-09-28', timestamp)`, `'2010-09-28 00:00:00+00:00'`},
		{`ANNOTATE_TYPE('2010-09-28', timestamptz)`, `'2010-09-28 00:00:00+00:00'`},
		{`ANNOTATE_TYPE(123, int) + 1`, `124`},
		{`ANNOTATE_TYPE(123, float) + 1`, `124.0`},
		{`ANNOTATE_TYPE(123, decimal) + 1`, `124`},
		{`ANNOTATE_TYPE(123.5, float) + 1`, `124.5`},
		{`ANNOTATE_TYPE(123.5, decimal) + 1`, `124.5`},
		{`ANNOTATE_TYPE(NULL, int)`, `NULL`},
		{`ANNOTATE_TYPE(NULL, string)`, `NULL`},
		{`ANNOTATE_TYPE(NULL, timestamp)`, `NULL`},
		// Shorthand type annotation notation.
		{`123:::int + 1`, `124`},
		{`123:::float + 1`, `124.0`},
		{`(123 + 1):::int`, `124`},
		{`(123 + 1):::float`, `124.0`},
		// Extract from dates.
		{`extract(year from '2010-09-28'::date)`, `2010`},
		{`extract(year from '2010-09-28'::date)`, `2010`},
		{`extract(month from '2010-09-28'::date)`, `9`},
		{`extract(day from '2010-09-28'::date)`, `28`},
		{`extract(dayofyear from '2010-09-28'::date)`, `271`},
		{`extract(week from '2010-01-14'::date)`, `2`},
		{`extract(dayofweek from '2010-09-28'::date)`, `2`},
		{`extract(quarter from '2010-09-28'::date)`, `3`},
		// Extract from timestamps.
		{`extract(year from '2010-09-28 12:13:14.1+00:00'::timestamp)`, `2010`},
		{`extract(year from '2010-09-28 12:13:14.1+00:00'::timestamp)`, `2010`},
		{`extract(month from '2010-09-28 12:13:14.1+00:00'::timestamp)`, `9`},
		{`extract(day from '2010-09-28 12:13:14.1+00:00'::timestamp)`, `28`},
		{`extract(dayofyear from '2010-09-28 12:13:14.1+00:00'::timestamp)`, `271`},
		{`extract(week from '2010-01-14 12:13:14.1+00:00'::timestamp)`, `2`},
		{`extract(dayofweek from '2010-09-28 12:13:14.1+00:00'::timestamp)`, `2`},
		{`extract(quarter from '2010-09-28 12:13:14.1+00:00'::timestamp)`, `3`},
		{`extract(hour from '2010-01-10 12:13:14.1+00:00'::timestamp)`, `12`},
		{`extract(minute from '2010-01-10 12:13:14.1+00:00'::timestamp)`, `13`},
		{`extract(second from '2010-01-10 12:13:14.1+00:00'::timestamp)`, `14`},
		{`extract(millisecond from '2010-01-10 12:13:14.123456+00:00'::timestamp)`, `123`},
		{`extract(microsecond from '2010-01-10 12:13:14.123456+00:00'::timestamp)`, `123456`},
		{`extract(epoch from '2010-01-10 12:13:14.1+00:00'::timestamp)`, `1263125594`},
		// Extract from intervals.
		{`extract_duration(hour from '123m')`, `2`},
		{`extract_duration(hour from '123m'::interval)`, `2`},
		{`extract_duration(minute from '123m10s'::interval)`, `123`},
		{`extract_duration(second from '10m20s30ms'::interval)`, `620`},
		{`extract_duration(millisecond from '20s30ms40µs'::interval)`, `20030`},
		{`extract_duration(microsecond from '12345ns'::interval)`, `12`},
		// Need two interval ops to verify the return type matches the return struct type.
		{`'2010-09-28 12:00:00.1-04:00'::timestamptz - '0s'::interval - '0s'::interval`, `'2010-09-28 16:00:00.1+00:00'`},
		{`'12h2m1s23ms'::interval + '1h'::interval`, `'13h2m1s23ms'`},
		{`'12 hours 2 minutes 1 second'::interval + '1h'::interval`, `'13h2m1s'`},
		{`'PT12H2M1S'::interval + '1h'::interval`, `'13h2m1s'`},
		{`'12:02:01'::interval + '1h'::interval`, `'13h2m1s'`},
		{`'12h2m1s23ms'::interval - '1h'::interval`, `'11h2m1s23ms'`},
		{`'12 hours 2 minutes 1 second'::interval - '1h'::interval`, `'11h2m1s'`},
		{`'PT12H2M1S'::interval - '1h'::interval`, `'11h2m1s'`},
		{`'1h'::interval - '12h2m1s23ms'::interval`, `'-11h-2m-1s-23ms'`},
		{`'PT1H'::interval - '12h2m1s23ms'::interval`, `'-11h-2m-1s-23ms'`},
		{`'1 hour'::interval - '12h2m1s23ms'::interval`, `'-11h-2m-1s-23ms'`},
		{`3 * '1h2m'::interval * 3`, `'9h18m'`},
		{`3 * '1 hour 2 minutes'::interval * 3`, `'9h18m'`},
		{`3 * 'PT1H2M'::interval * 3`, `'9h18m'`},
		{`'3h'::interval / 2`, `'1h30m'`},
		{`'PT3H'::interval / 2`, `'1h30m'`},
		{`'3:00'::interval / 2`, `'1h30m'`},
		{`'3 hours'::interval / 2`, `'1h30m'`},
		{`'3 hours'::interval * 2.5`, `'7h30m'`},
		{`'3 hours'::interval / 2.5`, `'1h12m'`},
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
		{`'+Inf'::float < 1.0`, `false`},
		{`'+Inf'::float <= 1.0`, `false`},
		{`'+Inf'::float = 1.0`, `false`},
		{`'+Inf'::float > 1.0`, `true`},
		{`'+Inf'::float >= 1.0`, `true`},
		{`'-Inf'::float < 1.0`, `true`},
		{`'-Inf'::float <= 1.0`, `true`},
		{`'-Inf'::float = 1.0`, `false`},
		{`'-Inf'::float > 1.0`, `false`},
		{`'-Inf'::float >= 1.0`, `false`},
		{`'-Inf'::float < '+Inf'::float`, `true`},
		{`'-Inf'::float <= '+Inf'::float`, `true`},
		{`'-Inf'::float = '+Inf'::float`, `false`},
		{`'-Inf'::float > '+Inf'::float`, `false`},
		{`'-Inf'::float >= '+Inf'::float`, `false`},
		{`'Infinity'::decimal`, `Infinity`},
		{`'+Infinity'::decimal`, `Infinity`},
		{`'-Infinity'::decimal`, `-Infinity`},
		{`'Inf'::decimal`, `Infinity`},
		{`'+Inf'::decimal`, `Infinity`},
		{`'-Inf'::decimal`, `-Infinity`},
		{`'Inf'::decimal(4)`, `Infinity`},
		{`'+Inf'::decimal < 1.0`, `false`},
		{`'+Inf'::decimal <= 1.0`, `false`},
		{`'+Inf'::decimal = 1.0`, `false`},
		{`'+Inf'::decimal > 1.0`, `true`},
		{`'+Inf'::decimal >= 1.0`, `true`},
		{`'-Inf'::decimal < 1.0`, `true`},
		{`'-Inf'::decimal <= 1.0`, `true`},
		{`'-Inf'::decimal = 1.0`, `false`},
		{`'-Inf'::decimal > 1.0`, `false`},
		{`'-Inf'::decimal >= 1.0`, `false`},
		{`'-Inf'::decimal < '+Inf'::decimal`, `true`},
		{`'-Inf'::decimal <= '+Inf'::decimal`, `true`},
		{`'-Inf'::decimal = '+Inf'::decimal`, `false`},
		{`'-Inf'::decimal > '+Inf'::decimal`, `false`},
		{`'-Inf'::decimal >= '+Inf'::decimal`, `false`},
		{`'Inf'::decimal::float`, `+Inf`},
		{`'Inf'::float::decimal`, `Infinity`},
		// NaN
		{`'NaN'::float`, `NaN`},
		{`'NaN'::float(4)`, `NaN`},
		{`'NaN'::real`, `NaN`},
		{`'NaN'::double precision`, `NaN`},
		{`'NaN'::float < 1.0`, `false`},
		{`'NaN'::float <= 1.0`, `false`},
		{`'NaN'::float = 1.0`, `false`},
		{`'NaN'::float > 1.0`, `false`},
		{`'NaN'::float >= 1.0`, `false`},
		{`'NaN'::float < '+Inf'::float`, `false`},
		{`'NaN'::float <= '+Inf'::float`, `false`},
		{`'NaN'::float = '+Inf'::float`, `false`},
		{`'NaN'::float > '+Inf'::float`, `false`},
		{`'NaN'::float >= '+Inf'::float`, `false`},
		{`'NaN'::float < '-Inf'::float`, `false`},
		{`'NaN'::float <= '-Inf'::float`, `false`},
		{`'NaN'::float = '-Inf'::float`, `false`},
		{`'NaN'::float > '-Inf'::float`, `false`},
		{`'NaN'::float >= '-Inf'::float`, `false`},
		{`'NaN'::float < 'NaN'::float`, `false`},
		{`'NaN'::float <= 'NaN'::float`, `false`},
		{`'NaN'::float = 'NaN'::float`, `false`},
		{`'NaN'::float > 'NaN'::float`, `false`},
		{`'NaN'::float >= 'NaN'::float`, `false`},
		{`'NaN'::decimal`, `NaN`},
		{`'NaN'::decimal(4)`, `NaN`},
		{`'NaN'::decimal < 1.0`, `false`},
		{`'NaN'::decimal <= 1.0`, `false`},
		{`'NaN'::decimal = 1.0`, `false`},
		{`'NaN'::decimal > 1.0`, `false`},
		{`'NaN'::decimal >= 1.0`, `false`},
		{`'NaN'::decimal < '+Inf'::decimal`, `false`},
		{`'NaN'::decimal <= '+Inf'::decimal`, `false`},
		{`'NaN'::decimal = '+Inf'::decimal`, `false`},
		{`'NaN'::decimal > '+Inf'::decimal`, `false`},
		{`'NaN'::decimal >= '+Inf'::decimal`, `false`},
		{`'NaN'::decimal < '-Inf'::decimal`, `false`},
		{`'NaN'::decimal <= '-Inf'::decimal`, `false`},
		{`'NaN'::decimal = '-Inf'::decimal`, `false`},
		{`'NaN'::decimal > '-Inf'::decimal`, `false`},
		{`'NaN'::decimal >= '-Inf'::decimal`, `false`},
		{`'NaN'::decimal < 'NaN'::decimal`, `false`},
		{`'NaN'::decimal <= 'NaN'::decimal`, `false`},
		{`'NaN'::decimal = 'NaN'::decimal`, `false`},
		{`'NaN'::decimal > 'NaN'::decimal`, `false`},
		{`'NaN'::decimal >= 'NaN'::decimal`, `false`},
		{`'NaN'::decimal::float`, `NaN`},
		{`'NaN'::float::decimal`, `NaN`},
	}
	for _, d := range testData {
		expr, err := ParseExpr(d.expr)
		if err != nil {
			t.Fatalf("%s: %v", d.expr, err)
		}
		// expr.TypeCheck to avoid constant folding.
		typedExpr, err := expr.TypeCheck(nil, TypeAny)
		if err != nil {
			t.Fatalf("%s: %v", d.expr, err)
		}
		ctx := &EvalContext{}
		if typedExpr, err = ctx.NormalizeExpr(typedExpr); err != nil {
			t.Fatalf("%s: %v", d.expr, err)
		}
		r, err := typedExpr.Eval(ctx)
		if err != nil {
			t.Fatalf("%s: %v", d.expr, err)
		}
		if s := r.String(); d.expected != s {
			t.Errorf("%s: expected %s, but found %s", d.expr, d.expected, s)
		}
	}
}

func TestTimeConversion(t *testing.T) {
	tests := []struct {
		start     string
		format    string
		tm        string
		revformat string
		reverse   string
	}{
		// %a %A %b %B (+ %Y)
		{`Wed Oct 05 2016`, `%a %b %d %Y`, `2016-10-05 00:00:00+00:00`, ``, ``},
		{`Wednesday October 05 2016`, `%A %B %d %Y`, `2016-10-05 00:00:00+00:00`, ``, ``},
		// %c
		{`Wed Oct 5 01:02:03 2016`, `%c`, `2016-10-05 01:02:03+00:00`, ``, ``},
		// %C %d (+ %m %y)
		{`20 06 10 12`, `%C %y %m %d`, `2006-10-12 00:00:00+00:00`, ``, ``},
		// %D
		{`10/12/06`, `%D`, `2006-10-12 00:00:00+00:00`, ``, ``},
		// %e (+ %Y %m)
		{`2006 10  3`, `%Y %m %e`, `2006-10-03 00:00:00+00:00`, ``, ``},
		// %f (+ %c)
		{`Wed Oct 5 01:02:03 2016 .123`, `%c .%f`, `2016-10-05 01:02:03.123+00:00`, `.%f`, `.123000000`},
		{`Wed Oct 5 01:02:03 2016 .123456`, `%c .%f`, `2016-10-05 01:02:03.123456+00:00`, `.%f`, `.123456000`},
		{`Wed Oct 5 01:02:03 2016 .123456789`, `%c .%f`, `2016-10-05 01:02:03.123457+00:00`, `.%f`, `.123457000`},
		{`Wed Oct 5 01:02:03 2016 .999999999`, `%c .%f`, `2016-10-05 01:02:04+00:00`, `.%f`, `.000000000`},
		// %F
		{`2006-10-03`, `%F`, `2006-10-03 00:00:00+00:00`, ``, ``},
		// %h (+ %Y %d)
		{`2006 Oct 03`, `%Y %h %d`, `2006-10-03 00:00:00+00:00`, ``, ``},
		// %H (+ %S %M)
		{`20061012 01:03:02`, `%Y%m%d %H:%S:%M`, `2006-10-12 01:02:03+00:00`, ``, ``},
		// %I (+ %Y %m %d)
		{`20161012 11`, `%Y%m%d %I`, `2016-10-12 11:00:00+00:00`, ``, ``},
		// %j (+ %Y)
		{`2016 286`, `%Y %j`, `2016-10-12 00:00:00+00:00`, ``, ``},
		// %k (+ %Y %m %d)
		{`20061012 23`, `%Y%m%d %k`, `2006-10-12 23:00:00+00:00`, ``, ``},
		// %l (+ %Y %m %d %p)
		{`20061012  5 PM`, `%Y%m%d %l %p`, `2006-10-12 17:00:00+00:00`, ``, ``},
		// %n (+ %Y %m %d)
		{"2006\n10\n03", `%Y%n%m%n%d`, `2006-10-03 00:00:00+00:00`, ``, ``},
		// %p cannot be parsed before hour specifiers, so be sure that
		// they appear in this order.
		{`20161012 11 PM`, `%Y%m%d %I %p`, `2016-10-12 23:00:00+00:00`, ``, ``},
		{`20161012 11 AM`, `%Y%m%d %I %p`, `2016-10-12 11:00:00+00:00`, ``, ``},
		// %r
		{`20161012 11:02:03 PM`, `%Y%m%d %r`, `2016-10-12 23:02:03+00:00`, ``, ``},
		// %R
		{`20161012 11:02`, `%Y%m%d %R`, `2016-10-12 11:02:00+00:00`, ``, ``},
		// %s
		{`1491920586`, `%s`, `2017-04-11 14:23:06+00:00`, ``, ``},
		// %t (+ %Y %m %d)
		{"2006\t10\t03", `%Y%t%m%t%d`, `2006-10-03 00:00:00+00:00`, ``, ``},
		// %T (+ %Y %m %d)
		{`20061012 01:02:03`, `%Y%m%d %T`, `2006-10-12 01:02:03+00:00`, ``, ``},
		// %U %u (+ %Y)
		{`2018 10 4`, `%Y %U %u`, `2018-03-15 00:00:00+00:00`, ``, ``},
		// %W %w (+ %Y)
		{`2018 10 4`, `%Y %W %w`, `2018-03-08 00:00:00+00:00`, ``, ``},
		// %x
		{`10/12/06`, `%x`, `2006-10-12 00:00:00+00:00`, ``, ``},
		// %X
		{`20061012 01:02:03`, `%Y%m%d %X`, `2006-10-12 01:02:03+00:00`, ``, ``},
		// %y (+ %m %d)
		{`000101`, `%y%m%d`, `2000-01-01 00:00:00+00:00`, ``, ``},
		{`680101`, `%y%m%d`, `2068-01-01 00:00:00+00:00`, ``, ``},
		{`690101`, `%y%m%d`, `1969-01-01 00:00:00+00:00`, ``, ``},
		{`990101`, `%y%m%d`, `1999-01-01 00:00:00+00:00`, ``, ``},
		// %Y
		{`19000101`, `%Y%m%d`, `1900-01-01 00:00:00+00:00`, ``, ``},
		{`20000101`, `%Y%m%d`, `2000-01-01 00:00:00+00:00`, ``, ``},
		{`30000101`, `%Y%m%d`, `3000-01-01 00:00:00+00:00`, ``, ``},
		// %z causes the time zone to adjust the time when parsing, but the time zone information
		// is not retained when printing the timestamp out back.
		{`20160101 13:00 +0655`, `%Y%m%d %H:%M %z`, `2016-01-01 06:05:00+00:00`, `%Y%m%d %H:%M %z`, `20160101 06:05 +0000`},
	}

	for _, test := range tests {
		ctx := &EvalContext{}
		exprStr := fmt.Sprintf("experimental_strptime('%s', '%s')", test.start, test.format)
		expr, err := ParseExpr(exprStr)
		if err != nil {
			t.Errorf("%s: %v", exprStr, err)
			continue
		}
		typedExpr, err := expr.TypeCheck(nil, TypeTimestamp)
		if err != nil {
			t.Errorf("%s: %v", exprStr, err)
			continue
		}
		r, err := typedExpr.Eval(ctx)
		if err != nil {
			t.Errorf("%s: %v", exprStr, err)
			continue
		}
		ts, ok := r.(*DTimestampTZ)
		if !ok {
			t.Errorf("%s: result not a timestamp: %s", exprStr, r)
			continue
		}

		tmS := ts.String()
		tmS = tmS[1 : len(tmS)-1] // strip the quote delimiters
		if tmS != test.tm {
			t.Errorf("%s: got %q, expected %q", exprStr, tmS, test.tm)
			continue
		}

		revfmt := test.format
		if test.revformat != "" {
			revfmt = test.revformat
		}

		ref := test.start
		if test.reverse != "" {
			ref = test.reverse
		}

		exprStr = fmt.Sprintf("experimental_strftime('%s'::timestamp, '%s')", tmS, revfmt)
		expr, err = ParseExpr(exprStr)
		if err != nil {
			t.Errorf("%s: %v", exprStr, err)
			continue
		}
		typedExpr, err = expr.TypeCheck(nil, TypeTimestamp)
		if err != nil {
			t.Errorf("%s: %v", exprStr, err)
			continue
		}
		r, err = typedExpr.Eval(ctx)
		if err != nil {
			t.Errorf("%s: %v", exprStr, err)
			continue
		}
		rs, ok := r.(*DString)
		if !ok {
			t.Errorf("%s: result not a string: %s", exprStr, r)
			continue
		}
		revS := string(*rs)
		if ref != revS {
			t.Errorf("%s: got %q, expected %q", exprStr, revS, ref)
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
		{`1 // 0`, `division by zero`},
		{`1.5 / 0`, `division by zero`},
		{`'11h2m'::interval / 0`, `division by zero`},
		{`'11h2m'::interval / 0.0::float`, `division by zero`},
		{`'???'::bool`,
			`could not parse '???' as type bool: strconv.ParseBool: parsing "???": invalid syntax`},
		{`'foo'::int`,
			`could not parse 'foo' as type int: strconv.ParseInt: parsing "foo": invalid syntax`},
		{`'bar'::float`,
			`could not parse 'bar' as type float: strconv.ParseFloat: parsing "bar": invalid syntax`},
		{`'baz'::decimal`,
			`could not parse 'baz' as type decimal`},
		{`'2010-09-28 12:00:00.1'::date`,
			`could not parse '2010-09-28 12:00:00.1' as type date`},
		{`'2010-09-28 12:00.1 MST'::timestamp`,
			`could not parse '2010-09-28 12:00.1 MST' as type timestamp`},
		{`'abcd'::interval`,
			`could not parse 'abcd' as type interval: interval: missing unit`},
		{`'1- 2:3:4 9'::interval`,
			`could not parse '1- 2:3:4 9' as type interval: invalid input syntax for type interval 1- 2:3:4 9`},
		{`b'\xff\xfe\xfd'::string`, `invalid utf8: "\xff\xfe\xfd"`},
		{`ARRAY[NULL, ARRAY[1, 2]]`, `multidimensional arrays must have array expressions with matching dimensions`},
		{`ARRAY[ARRAY[1, 2], NULL]`, `multidimensional arrays must have array expressions with matching dimensions`},
		{`ARRAY[ARRAY[1, 2], ARRAY[1]]`, `multidimensional arrays must have array expressions with matching dimensions`},
		// TODO(pmattis): Check for overflow.
		// {`~0 + 1`, `0`},
		{`9223372036854775807::int + 1::int`, `integer out of range`},
		{`-9223372036854775807::int + -2::int`, `integer out of range`},
		{`-9223372036854775807::int + -9223372036854775807::int`, `integer out of range`},
		{`9223372036854775807::int + 9223372036854775807::int`, `integer out of range`},
		{`9223372036854775807::int - -1::int`, `integer out of range`},
		{`-9223372036854775807::int - 2::int`, `integer out of range`},
		{`-9223372036854775807::int - 9223372036854775807::int`, `integer out of range`},
		{`9223372036854775807::int - -9223372036854775807::int`, `integer out of range`},
		{`4611686018427387904::int * 2::int`, `integer out of range`},
		{`4611686018427387904::int * 2::int`, `integer out of range`},
		{`(-9223372036854775807:::int - 1) * -1:::int`, `integer out of range`},
		{`123 ^ 100`, `integer out of range`},
		{`power(123, 100)`, `integer out of range`},
		// Although these next two tests are valid integers, a float cannot represent
		// them exactly, and so rounds them to a larger number that is out of bounds
		// for an int. Thus, they should fail during this conversion.
		{`9223372036854775807::float::int`, `integer out of range`},
		{`-9223372036854775808::float::int`, `integer out of range`},
		// The two smallest floats that cannot be converted to an int.
		{`9223372036854775296::float::int`, `integer out of range`},
		{`-9223372036854775296::float::int`, `integer out of range`},
		{`1e500::decimal::int`, `integer out of range`},
		{`1e500::decimal::float`, `float out of range`},
		{`1e300::decimal::float::int`, `integer out of range`},
		{`'Inf'::decimal::int`, `integer out of range`},
		{`'NaN'::decimal::int`, `integer out of range`},
		{`'Inf'::float::int`, `integer out of range`},
		{`'NaN'::float::int`, `integer out of range`},
	}
	for _, d := range testData {
		expr, err := ParseExpr(d.expr)
		if err != nil {
			t.Fatalf("%s: %v", d.expr, err)
		}
		typedExpr, err := TypeCheck(expr, nil, TypeAny)
		if err == nil {
			_, err = typedExpr.Eval(&EvalContext{})
		}
		if !testutils.IsError(err, strings.Replace(regexp.QuoteMeta(d.expected), `\.\*`, `.*`, -1)) {
			t.Errorf("%s: expected %s, but found %v", d.expr, d.expected, err)
		}
	}
}

func TestEvalComparisonExprCaching(t *testing.T) {
	testExprs := []struct {
		op          ComparisonOperator
		left, right string
		cacheCount  int
	}{
		// Comparisons.
		{EQ, `0`, `1`, 0},
		{LT, `0`, `1`, 0},
		// LIKE and NOT LIKE
		{Like, `TEST`, `T%T`, 1},
		{NotLike, `TEST`, `%E%T`, 1},
		// ILIKE and NOT ILIKE
		{ILike, `TEST`, `T%T`, 1},
		{NotILike, `TEST`, `%E%T`, 1},
		// SIMILAR TO and NOT SIMILAR TO
		{SimilarTo, `abc`, `(b|c)%`, 1},
		{NotSimilarTo, `abc`, `%(b|d)%`, 1},
		// ~, !~, ~*, and !~*
		{RegMatch, `abc`, `(b|c).`, 1},
		{NotRegMatch, `abc`, `(b|c).`, 1},
		{RegIMatch, `abc`, `(b|c).`, 1},
		{NotRegIMatch, `abc`, `(b|c).`, 1},
	}
	for _, d := range testExprs {
		expr := &ComparisonExpr{
			Operator: d.op,
			Left:     NewDString(d.left),
			Right:    NewDString(d.right),
		}
		ctx := &EvalContext{}
		ctx.ReCache = NewRegexpCache(8)
		typedExpr, err := TypeCheck(expr, nil, TypeAny)
		if err != nil {
			t.Fatalf("%v: %v", d, err)
		}
		if _, err := typedExpr.Eval(ctx); err != nil {
			t.Fatalf("%v: %v", d, err)
		}
		if typedExpr.(*ComparisonExpr).fn.fn == nil {
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

	ctx := &EvalContext{}
	ctx.PrepareOnly = true
	for _, d := range testData {
		ts := hlc.Timestamp{WallTime: d.walltime, Logical: d.logical}
		ctx.SetClusterTimestamp(ts)
		dec := ctx.GetClusterTimestamp()
		final := dec.ToStandard()
		if final != d.expected {
			t.Errorf("expected %s, but found %s", d.expected, final)
		}
	}
}

func TestCastToCollatedString(t *testing.T) {
	cases := []struct {
		typ      CollatedStringColType
		contents string
	}{
		{CollatedStringColType{Locale: "de"}, "test"},
		{CollatedStringColType{Locale: "en"}, "test"},
		{CollatedStringColType{Locale: "en", N: 5}, "test"},
		{CollatedStringColType{Locale: "en", N: 4}, "test"},
		{CollatedStringColType{Locale: "en", N: 3}, "tes"},
	}
	for _, cas := range cases {
		t.Run("", func(t *testing.T) {
			expr := &CastExpr{Expr: NewDString("test"), Type: &cas.typ, syntaxMode: castShort}
			typedexpr, err := expr.TypeCheck(&SemaContext{}, TypeAny)
			if err != nil {
				t.Fatal(err)
			}
			val, err := typedexpr.Eval(&EvalContext{})
			if err != nil {
				t.Fatal(err)
			}
			switch v := val.(type) {
			case *DCollatedString:
				if v.Locale != cas.typ.Locale {
					t.Errorf("expected locale %q but got %q", cas.typ.Locale, v.Locale)
				}
				if v.Contents != cas.contents {
					t.Errorf("expected contents %q but got %q", cas.contents, v.Contents)
				}
			default:
				t.Errorf("expected type *DCollatedString but got %T", v)
			}
		})
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

func benchmarkLike(b *testing.B, ctx *EvalContext, caseInsensitive bool) {
	op := Like
	if caseInsensitive {
		op = ILike
	}
	likeFn, _ := CmpOps[op].lookupImpl(TypeString, TypeString)
	iter := func() {
		for _, p := range benchmarkLikePatterns {
			if _, err := likeFn.fn(ctx, NewDString("test"), NewDString(p)); err != nil {
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
	benchmarkLike(b, &EvalContext{ReCache: NewRegexpCache(len(benchmarkLikePatterns))}, false)
}

func BenchmarkLikeWithoutCache(b *testing.B) {
	benchmarkLike(b, &EvalContext{}, false)
}

func BenchmarkILikeWithCache(b *testing.B) {
	benchmarkLike(b, &EvalContext{ReCache: NewRegexpCache(len(benchmarkLikePatterns))}, true)
}

func BenchmarkILikeWithoutCache(b *testing.B) {
	benchmarkLike(b, &EvalContext{}, true)
}
