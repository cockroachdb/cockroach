// Copyright 2017 The Cockroach Authors.
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

package tree_test

import (
	"regexp"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	_ "github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/testutils"
)

// The following tests need both the type checking infrastructure and also
// all the built-in function definitions to be active.

func TestTypeCheck(t *testing.T) {
	testData := []struct {
		expr string
		// The expected serialized expression after type-checking. This tests both
		// the serialization and that the constants are resolved to the expected
		// types.
		expected string
	}{
		{`NULL + 1`, `NULL`},
		{`NULL + 1.1`, `NULL`},
		{`NULL + '2006-09-23'::date`, `NULL`},
		{`NULL + '1h'::interval`, `NULL`},
		{`NULL || 'hello'`, `NULL`},
		{`NULL || 'hello'::bytes`, `NULL`},
		{`NULL::int`, `NULL::INT`},
		{`INTERVAL '1s'`, `'1s':::INTERVAL`},
		{`(1.1::decimal)::decimal`, `1.1:::DECIMAL::DECIMAL::DECIMAL`},
		{`NULL = 1`, `NULL`},
		{`1 = NULL`, `NULL`},
		{`true AND NULL`, `true AND NULL`},
		{`NULL OR false`, `NULL OR false`},
		{`1 IN (1, 2, 3)`, `1:::INT IN (1:::INT, 2:::INT, 3:::INT)`},
		{`IF(true, 2, 3)`, `IF(true, 2:::INT, 3:::INT)`},
		{`IF(false, 2, 3)`, `IF(false, 2:::INT, 3:::INT)`},
		{`IF(NULL, 2, 3)`, `IF(NULL, 2:::INT, 3:::INT)`},
		{`IF(NULL, 2, 3.0)`, `IF(NULL, 2:::DECIMAL, 3.0:::DECIMAL)`},
		{`IF(true, (1, 2), (1, 3))`, `IF(true, (1:::INT, 2:::INT), (1:::INT, 3:::INT))`},
		{`IFNULL(1, 2)`, `IFNULL(1:::INT, 2:::INT)`},
		{`IFNULL(1, 2.0)`, `IFNULL(1:::DECIMAL, 2.0:::DECIMAL)`},
		{`IFNULL(NULL, 2)`, `IFNULL(NULL, 2:::INT)`},
		{`IFNULL(2, NULL)`, `IFNULL(2:::INT, NULL)`},
		{`IFNULL((1, 2), (1, 3))`, `IFNULL((1:::INT, 2:::INT), (1:::INT, 3:::INT))`},
		{`NULLIF(1, 2)`, `NULLIF(1:::INT, 2:::INT)`},
		{`NULLIF(1, 2.0)`, `NULLIF(1:::DECIMAL, 2.0:::DECIMAL)`},
		{`NULLIF(NULL, 2)`, `NULLIF(NULL, 2:::INT)`},
		{`NULLIF(2, NULL)`, `NULLIF(2:::INT, NULL)`},
		{`NULLIF((1, 2), (1, 3))`, `NULLIF((1:::INT, 2:::INT), (1:::INT, 3:::INT))`},
		{`COALESCE(1, 2, 3, 4, 5)`, `COALESCE(1:::INT, 2:::INT, 3:::INT, 4:::INT, 5:::INT)`},
		{`COALESCE(1, 2.0)`, `COALESCE(1:::DECIMAL, 2.0:::DECIMAL)`},
		{`COALESCE(NULL, 2)`, `COALESCE(NULL, 2:::INT)`},
		{`COALESCE(2, NULL)`, `COALESCE(2:::INT, NULL)`},
		{`COALESCE((1, 2), (1, 3))`, `COALESCE((1:::INT, 2:::INT), (1:::INT, 3:::INT))`},
		{`true IS NULL`, `true IS NULL`},
		{`true IS NOT NULL`, `true IS NOT NULL`},
		{`true IS TRUE`, `true IS true`},
		{`true IS NOT TRUE`, `true IS NOT true`},
		{`true IS FALSE`, `true IS false`},
		{`true IS NOT FALSE`, `true IS NOT false`},
		{`CASE 1 WHEN 1 THEN (1, 2) ELSE (1, 3) END`, `CASE 1:::INT WHEN 1:::INT THEN (1:::INT, 2:::INT) ELSE (1:::INT, 3:::INT) END`},
		{`1 BETWEEN 2 AND 3`, `1:::INT BETWEEN 2:::INT AND 3:::INT`},
		{`4 BETWEEN 2.4 AND 5.5::float`, `4:::INT BETWEEN 2.4:::DECIMAL AND 5.5:::FLOAT::FLOAT`},
		{`COUNT(3)`, `count(3:::INT)`},
		{`ARRAY['a', 'b', 'c']`, `ARRAY['a':::STRING, 'b':::STRING, 'c':::STRING]`},
		{`ARRAY[1.5, 2.5, 3.5]`, `ARRAY[1.5:::DECIMAL, 2.5:::DECIMAL, 3.5:::DECIMAL]`},
		{`ARRAY[NULL]`, `ARRAY[NULL]`},
		{`1 = ANY ARRAY[1.5, 2.5, 3.5]`, `1:::DECIMAL = ANY ARRAY[1.5:::DECIMAL, 2.5:::DECIMAL, 3.5:::DECIMAL]`},
		{`true = SOME (ARRAY[true, false])`, `true = SOME ARRAY[true, false]`},
		{`1.3 = ALL ARRAY[1, 2, 3]`, `1.3:::DECIMAL = ALL ARRAY[1:::DECIMAL, 2:::DECIMAL, 3:::DECIMAL]`},
		{`1.3 = ALL ((ARRAY[]))`, `1.3:::DECIMAL = ALL ARRAY[]`},
		{`NULL = ALL ARRAY[1.5, 2.5, 3.5]`, `NULL`},
		{`NULL = ALL ARRAY[NULL, NULL]`, `NULL`},
		{`1 = ALL NULL`, `NULL`},
		{`'a' = ALL CURRENT_SCHEMAS(true)`, `'a':::STRING = ALL current_schemas(true)`},
		{`NULL = ALL CURRENT_SCHEMAS(true)`, `NULL`},

		{`INTERVAL '1'`, `'1s':::INTERVAL`},
		{`DECIMAL '1.0'`, `1.0:::DECIMAL::DECIMAL`},

		{`1 + 2`, `3:::INT`},
		{`1:::decimal + 2`, `1:::DECIMAL + 2:::DECIMAL`},
		{`1:::float + 2`, `1.0:::FLOAT + 2.0:::FLOAT`},
		{`INTERVAL '1.5s' * 2`, `'1s500ms':::INTERVAL * 2:::INT`},
		{`2 * INTERVAL '1.5s'`, `2:::INT * '1s500ms':::INTERVAL`},

		{`1 + $1`, `1:::INT + $1:::INT`},
		{`1:::DECIMAL + $1`, `1:::DECIMAL + $1:::DECIMAL`},
		{`$1:::INT`, `$1:::INT`},

		// These outputs, while bizarre looking, are correct and expected. The
		// type annotation is caused by the call to tree.Serialize, which formats the
		// output using the Parseable formatter which inserts type annotations
		// at the end of all well-typed datums. And the second cast is caused by
		// the test itself.
		{`'NaN'::decimal`, `'NaN':::DECIMAL::DECIMAL`},
		{`'-NaN'::decimal`, `'NaN':::DECIMAL::DECIMAL`},
		{`'Inf'::decimal`, `'Infinity':::DECIMAL::DECIMAL`},
		{`'-Inf'::decimal`, `'-Infinity':::DECIMAL::DECIMAL`},
	}
	for _, d := range testData {
		t.Run(d.expr, func(t *testing.T) {
			expr, err := parser.ParseExpr(d.expr)
			if err != nil {
				t.Fatalf("%s: %v", d.expr, err)
			}
			ctx := tree.MakeSemaContext(false)
			typeChecked, err := tree.TypeCheck(expr, &ctx, types.Any)
			if err != nil {
				t.Fatalf("%s: unexpected error %s", d.expr, err)
			}
			if s := tree.Serialize(typeChecked); s != d.expected {
				t.Errorf("%s: expected %s, but found %s", d.expr, d.expected, s)
			}
		})
	}
}

func TestTypeCheckError(t *testing.T) {
	testData := []struct {
		expr     string
		expected string
	}{
		{`'1' + '2'`, `unsupported binary operator:`},
		{`'a' + 0`, `unsupported binary operator:`},
		{`1.1 # 3.1`, `unsupported binary operator:`},
		{`~0.1`, `unsupported unary operator:`},
		{`'a' > 2`, `unsupported comparison operator:`},
		{`a`, `name "a" is not defined`},
		{`COS(*)`, `cannot use "*" in this context`},
		{`a.*`, `cannot use "a.*" in this context`},
		{`1 AND true`, `incompatible AND argument type: int`},
		{`1.0 AND true`, `incompatible AND argument type: decimal`},
		{`'a' OR true`, `could not parse "a" as type bool`},
		{`(1, 2) OR true`, `incompatible OR argument type: tuple`},
		{`NOT 1`, `incompatible NOT argument type: int`},
		{`lower()`, `unknown signature: lower()`},
		{`lower(1, 2)`, `unknown signature: lower(int, int)`},
		{`lower(1)`, `unknown signature: lower(int)`},
		{`lower('FOO') OVER ()`, `OVER specified, but lower() is neither a window function nor an aggregate function`},
		{`count(1) FILTER (WHERE true) OVER ()`, `FILTER within a window function call is not yet supported`},
		{`CASE 'one' WHEN 1 THEN 1 WHEN 'two' THEN 2 END`, `incompatible condition type`},
		{`CASE 1 WHEN 1 THEN 'one' WHEN 2 THEN 2 END`, `incompatible value type`},
		{`CASE 1 WHEN 1 THEN 'one' ELSE 2 END`, `incompatible value type`},
		{`(1, 2, 3) = (1, 2)`, `expected tuple (1, 2) to have a length of 3`},
		{`(1, 2) = (1, 'a')`, `tuples (1, 2), (1, 'a') are not comparable at index 2: unsupported comparison operator:`},
		{`1 IN ('a', 'b')`, `unsupported comparison operator: 1 IN ('a', 'b'): could not parse "a" as type int`},
		{`1 IN (1, 'a')`, `unsupported comparison operator: 1 IN (1, 'a'): could not parse "a" as type int`},
		{`1 = ANY 2`, `unsupported comparison operator: 1 = ANY 2: op ANY <right> requires array, tuple or subquery on right side`},
		{`1 = ANY ARRAY[2, 'a']`, `unsupported comparison operator: 1 = ANY ARRAY[2, 'a']: could not parse "a" as type int`},
		{`1 = ALL CURRENT_SCHEMAS(true)`, `unsupported comparison operator: <int> = ALL <string[]>`},
		{`1.0 BETWEEN 2 AND 'a'`, `unsupported comparison operator: <decimal> < <string>`},
		{`IF(1, 2, 3)`, `incompatible IF condition type: int`},
		{`IF(true, 'a', 2)`, `incompatible IF expressions: could not parse "a" as type int`},
		{`IF(true, 2, 'a')`, `incompatible IF expressions: could not parse "a" as type int`},
		{`IFNULL(1, 'a')`, `incompatible IFNULL expressions: could not parse "a" as type int`},
		{`NULLIF(1, 'a')`, `incompatible NULLIF expressions: could not parse "a" as type int`},
		{`COALESCE(1, 2, 3, 4, 'a')`, `incompatible COALESCE expressions: could not parse "a" as type int`},
		{`ARRAY[]`, `cannot determine type of empty array`},
		{`ANNOTATE_TYPE('a', int)`, `could not parse "a" as type int`},
		{`ANNOTATE_TYPE(ANNOTATE_TYPE(1, int), decimal)`, `incompatible type annotation for ANNOTATE_TYPE(1, INT) as decimal, found type: int`},
		{`3:::int[]`, `incompatible type annotation for 3 as int[], found type: int`},
	}
	for _, d := range testData {
		expr, err := parser.ParseExpr(d.expr)
		if err != nil {
			t.Fatalf("%s: %v", d.expr, err)
		}
		if _, err := tree.TypeCheck(expr, nil, types.Any); !testutils.IsError(err, regexp.QuoteMeta(d.expected)) {
			t.Errorf("%s: expected %s, but found %v", d.expr, d.expected, err)
		}
	}
}
