// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tree_test

import (
	"context"
	"regexp"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	_ "github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// The following tests need both the type checking infrastructure and also
// all the built-in function definitions to be active.

func TestTypeCheck(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	typeMap := map[string]*types.T{
		"d.t1":   types.Int,
		"t2":     types.String,
		"d.s.t3": types.Decimal,
	}
	mapResolver := tree.MakeTestingMapTypeResolver(typeMap)

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
		{`NULL::int4`, `NULL::INT4`},
		{`NULL::int8`, `NULL::INT8`},
		{`INTERVAL '1s'`, `'00:00:01':::INTERVAL`},
		{`(1.1::decimal)::decimal`, `1.1:::DECIMAL`},
		{`NULL = 1`, `NULL`},
		{`1 = NULL`, `NULL`},
		{`true AND NULL`, `true AND NULL`},
		{`NULL OR false`, `NULL OR false`},
		{`1 IN (1, 2, 3)`, `1:::INT8 IN (1:::INT8, 2:::INT8, 3:::INT8)`},
		{`IF(true, 2, 3)`, `IF(true, 2:::INT8, 3:::INT8)`},
		{`IF(false, 2, 3)`, `IF(false, 2:::INT8, 3:::INT8)`},
		{`IF(NULL, 2, 3)`, `IF(NULL, 2:::INT8, 3:::INT8)`},
		{`IF(NULL, 2, 3.0)`, `IF(NULL, 2:::DECIMAL, 3.0:::DECIMAL)`},
		{`IF(true, (1, 2), (1, 3))`, `IF(true, (1:::INT8, 2:::INT8), (1:::INT8, 3:::INT8))`},
		{`IFERROR(1, 1.2, '')`, `IFERROR(1:::DECIMAL, 1.2:::DECIMAL, '':::STRING)`},
		{`IFERROR(NULL, 3, '')`, `IFERROR(NULL, 3:::INT8, '':::STRING)`},
		{`ISERROR(123)`, `ISERROR(123:::INT8)`},
		{`ISERROR(NULL)`, `ISERROR(NULL)`},
		{`IFNULL(1, 2)`, `IFNULL(1:::INT8, 2:::INT8)`},
		{`IFNULL(1, 2.0)`, `IFNULL(1:::DECIMAL, 2.0:::DECIMAL)`},
		{`IFNULL(NULL, 2)`, `IFNULL(NULL, 2:::INT8)`},
		{`IFNULL(2, NULL)`, `IFNULL(2:::INT8, NULL)`},
		{`IFNULL((1, 2), (1, 3))`, `IFNULL((1:::INT8, 2:::INT8), (1:::INT8, 3:::INT8))`},
		{`NULLIF(1, 2)`, `NULLIF(1:::INT8, 2:::INT8)`},
		{`NULLIF(1, 2.0)`, `NULLIF(1:::DECIMAL, 2.0:::DECIMAL)`},
		{`NULLIF(NULL, 2)`, `NULLIF(NULL, 2:::INT8)`},
		{`NULLIF(2, NULL)`, `NULLIF(2:::INT8, NULL)`},
		{`NULLIF((1, 2), (1, 3))`, `NULLIF((1:::INT8, 2:::INT8), (1:::INT8, 3:::INT8))`},
		{`COALESCE(1, 2, 3, 4, 5)`, `COALESCE(1:::INT8, 2:::INT8, 3:::INT8, 4:::INT8, 5:::INT8)`},
		{`COALESCE(1, 2.0)`, `COALESCE(1:::DECIMAL, 2.0:::DECIMAL)`},
		{`COALESCE(NULL, 2)`, `COALESCE(NULL, 2:::INT8)`},
		{`COALESCE(2, NULL)`, `COALESCE(2:::INT8, NULL)`},
		{`COALESCE((1, 2), (1, 3))`, `COALESCE((1:::INT8, 2:::INT8), (1:::INT8, 3:::INT8))`},
		{`true IS NULL`, `true IS NULL`},
		{`true IS NOT NULL`, `true IS NOT NULL`},
		{`true IS TRUE`, `true IS true`},
		{`true IS NOT TRUE`, `true IS NOT true`},
		{`true IS FALSE`, `true IS false`},
		{`true IS NOT FALSE`, `true IS NOT false`},
		{`CASE 1 WHEN 1 THEN (1, 2) ELSE (1, 3) END`, `CASE 1:::INT8 WHEN 1:::INT8 THEN (1:::INT8, 2:::INT8) ELSE (1:::INT8, 3:::INT8) END`},
		{`1 BETWEEN 2 AND 3`, `1:::INT8 BETWEEN 2:::INT8 AND 3:::INT8`},
		{`4 BETWEEN 2.4 AND 5.5::float`, `4:::INT8 BETWEEN 2.4:::DECIMAL AND 5.5:::FLOAT8`},
		{`count(3)`, `count(3:::INT8)`},
		{`ARRAY['a', 'b', 'c']`, `ARRAY['a':::STRING, 'b':::STRING, 'c':::STRING]:::STRING[]`},
		{`ARRAY[1.5, 2.5, 3.5]`, `ARRAY[1.5:::DECIMAL, 2.5:::DECIMAL, 3.5:::DECIMAL]:::DECIMAL[]`},
		{`ARRAY[NULL]`, `ARRAY[NULL]:::STRING[]`},
		{`ARRAY[NULL]:::int[]`, `ARRAY[NULL]:::INT8[]`},
		{`ARRAY[NULL, NULL]:::int[]`, `ARRAY[NULL, NULL]:::INT8[]`},
		{`ARRAY[]::INT8[]`, `ARRAY[]:::INT8[]`},
		{`ARRAY[]:::INT8[]`, `ARRAY[]:::INT8[]`},
		{`1 = ANY ARRAY[1.5, 2.5, 3.5]`, `1:::DECIMAL = ANY ARRAY[1.5:::DECIMAL, 2.5:::DECIMAL, 3.5:::DECIMAL]:::DECIMAL[]`},
		{`true = SOME (ARRAY[true, false])`, `true = SOME ARRAY[true, false]:::BOOL[]`},
		{`1.3 = ALL ARRAY[1, 2, 3]`, `1.3:::DECIMAL = ALL ARRAY[1:::DECIMAL, 2:::DECIMAL, 3:::DECIMAL]:::DECIMAL[]`},
		{`1.3 = ALL ((ARRAY[]))`, `1.3:::DECIMAL = ALL ARRAY[]:::DECIMAL[]`},
		{`NULL = ALL ARRAY[1.5, 2.5, 3.5]`, `NULL`},
		{`NULL = ALL ARRAY[NULL, NULL]`, `NULL`},
		{`1 = ALL NULL`, `NULL`},
		{`'a' = ALL current_schemas(true)`, `'a':::STRING = ALL current_schemas(true)`},
		{`NULL = ALL current_schemas(true)`, `NULL`},

		{`INTERVAL '1'`, `'00:00:01':::INTERVAL`},
		{`DECIMAL '1.0'`, `1.0:::DECIMAL`},

		{`1 + 2`, `1:::INT8 + 2:::INT8`},
		{`1:::decimal + 2`, `1:::DECIMAL + 2:::DECIMAL`},
		{`1:::float + 2`, `1.0:::FLOAT8 + 2.0:::FLOAT8`},
		{`INTERVAL '1.5s' * 2`, `'00:00:01.5':::INTERVAL * 2:::INT8`},
		{`2 * INTERVAL '1.5s'`, `2:::INT8 * '00:00:01.5':::INTERVAL`},

		{`1 + $1`, `1:::INT8 + $1:::INT8`},
		{`1:::DECIMAL + $1`, `1:::DECIMAL + $1:::DECIMAL`},
		{`$1:::INT8`, `$1:::INT8`},
		{`2::DECIMAL(10,2) + $1`, `2:::DECIMAL::DECIMAL(10,2) + $1:::DECIMAL`},
		{`2::DECIMAL(10,0) + $1`, `2:::DECIMAL::DECIMAL(10) + $1:::DECIMAL`},

		// Tuples with labels
		{`(ROW (1) AS a)`, `((1:::INT8,) AS a)`},
		{`(ROW(1:::INT8) AS a)`, `((1:::INT8,) AS a)`},
		{`((1,2) AS a,b)`, `((1:::INT8, 2:::INT8) AS a, b)`},
		{`((1:::INT8, 2:::INT8) AS a, b)`, `((1:::INT8, 2:::INT8) AS a, b)`},
		{`(ROW (1,2) AS a,b)`, `((1:::INT8, 2:::INT8) AS a, b)`},
		{`(ROW(1:::INT8, 2:::INT8) AS a, b)`, `((1:::INT8, 2:::INT8) AS a, b)`},
		{
			`((1,2,3) AS "One","Two","Three")`,
			`((1:::INT8, 2:::INT8, 3:::INT8) AS "One", "Two", "Three")`,
		},
		{
			`((1:::INT8, 2:::INT8, 3:::INT8) AS "One", "Two", "Three")`,
			`((1:::INT8, 2:::INT8, 3:::INT8) AS "One", "Two", "Three")`,
		},
		{
			`(ROW (1,2,3) AS "One",Two,"Three")`,
			`((1:::INT8, 2:::INT8, 3:::INT8) AS "One", two, "Three")`,
		},
		{
			`(ROW(1:::INT8, 2:::INT8, 3:::INT8) AS "One", two, "Three")`,
			`((1:::INT8, 2:::INT8, 3:::INT8) AS "One", two, "Three")`,
		},
		// Tuples with duplicate labels are allowed, but raise error when accessed by a duplicate label name.
		// This satisfies a postgres-compatible implementation of unnest(array, array...), where all columns
		// have the same label (unnest).
		{`((1,2) AS a,a)`, `((1:::INT8, 2:::INT8) AS a, a)`},
		{`((1,2,3) AS a,a,a)`, `((1:::INT8, 2:::INT8, 3:::INT8) AS a, a, a)`},
		// And tuples without labels still work as advertized
		{`(ROW (1))`, `(1:::INT8,)`},
		{`ROW(1:::INT8)`, `(1:::INT8,)`},
		{`((1,2))`, `(1:::INT8, 2:::INT8)`},
		{`(1:::INT8, 2:::INT8)`, `(1:::INT8, 2:::INT8)`},
		{`(ROW (1,2))`, `(1:::INT8, 2:::INT8)`},
		{`ROW(1:::INT8, 2:::INT8)`, `(1:::INT8, 2:::INT8)`},

		{`((ROW (1) AS a)).a`, `1:::INT8`},
		{`((('1', 2) AS a, b)).a`, `'1':::STRING`},
		{`((('1', 2) AS a, b)).b`, `2:::INT8`},
		{`((('1', 2) AS a, b)).@2`, `2:::INT8`},
		{`(pg_get_keywords()).word`, `(pg_get_keywords()).word`},
		{
			`(information_schema._pg_expandarray(ARRAY[1,3])).x`,
			`(information_schema._pg_expandarray(ARRAY[1:::INT8, 3:::INT8]:::INT8[])).x`,
		},
		{
			`(information_schema._pg_expandarray(ARRAY[1:::INT8, 3:::INT8])).*`,
			`information_schema._pg_expandarray(ARRAY[1:::INT8, 3:::INT8]:::INT8[])`,
		},
		{`((ROW (1) AS a)).*`, `((1:::INT8,) AS a)`},
		{`((('1'||'', 1+1) AS a, b)).*`, `(('1':::STRING || '':::STRING, 1:::INT8 + 1:::INT8) AS a, b)`},

		{`'{"x": "bar"}' -> 'x'`, `'{"x": "bar"}':::JSONB->'x':::STRING`},
		{`('{"x": "bar"}') -> 'x'`, `'{"x": "bar"}':::JSONB->'x':::STRING`},

		// These outputs, while bizarre looking, are correct and expected. The
		// type annotation is caused by the call to tree.Serialize, which formats the
		// output using the Parseable formatter which inserts type annotations
		// at the end of all well-typed datums. And the second cast is caused by
		// the test itself.
		{`'NaN'::decimal`, `'NaN':::DECIMAL`},
		{`'-NaN'::decimal`, `'NaN':::DECIMAL`},
		{`'Inf'::decimal`, `'Infinity':::DECIMAL`},
		{`'-Inf'::decimal`, `'-Infinity':::DECIMAL`},

		// Test type checking with some types to resolve.
		// Because the resolvable types right now are just aliases, the
		// pre-resolution name is not going to get formatted.
		{`1:::d.t1`, `1:::INT8`},
		{`1:::d.s.t3 + 1.4`, `1:::DECIMAL + 1.4:::DECIMAL`},
		{`1 IS OF (d.t1, t2)`, `1:::INT8 IS OF (INT8, STRING)`},
		{`1::d.t1`, `1:::INT8`},

		{`(('{' || 'a' ||'}')::STRING[])[1]::STRING`, `((('{':::STRING || 'a':::STRING) || '}':::STRING)::STRING[])[1:::INT8]`},
		{`(('{' || '1' ||'}')::INT[])[1]`, `((('{':::STRING || '1':::STRING) || '}':::STRING)::INT8[])[1:::INT8]`},
		{`(ARRAY[1, 2, 3]::int[])[2]`, `(ARRAY[1:::INT8, 2:::INT8, 3:::INT8]:::INT8[])[2:::INT8]`},

		// String preference.
		{`st_geomfromgeojson($1)`, `st_geomfromgeojson($1:::STRING):::GEOMETRY`},
	}
	ctx := context.Background()
	for _, d := range testData {
		t.Run(d.expr, func(t *testing.T) {
			expr, err := parser.ParseExpr(d.expr)
			if err != nil {
				t.Fatalf("%s: %v", d.expr, err)
			}
			semaCtx := tree.MakeSemaContext()
			if err := semaCtx.Placeholders.Init(1 /* numPlaceholders */, nil /* typeHints */); err != nil {
				t.Fatal(err)
			}
			semaCtx.TypeResolver = mapResolver
			typeChecked, err := tree.TypeCheck(ctx, expr, &semaCtx, types.Any)
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
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	typeMap := map[string]*types.T{
		"d.t1":   types.Int,
		"t2":     types.String,
		"d.s.t3": types.Decimal,
	}
	mapResolver := tree.MakeTestingMapTypeResolver(typeMap)

	testData := []struct {
		expr     string
		expected string
	}{
		{`'1' + '2'`, `ambiguous binary operator:`},
		{`'a' + 0`, `unsupported binary operator:`},
		{`1.1 # 3.1`, `unsupported binary operator:`},
		{`~0.1`, `unsupported unary operator:`},
		{`'a' > 2`, `unsupported comparison operator:`},
		{`a`, `column "a" does not exist`},
		{`cos(*)`, `cannot use "*" in this context`},
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
		{`count(1) FILTER (WHERE 1) OVER ()`, `incompatible FILTER expression type: int`},
		{`CASE 'one' WHEN 1 THEN 1 WHEN 'two' THEN 2 END`, `incompatible condition type`},
		{`CASE 1 WHEN 1 THEN 'one' WHEN 2 THEN 2 END`, `incompatible value type`},
		{`CASE 1 WHEN 1 THEN 'one' ELSE 2 END`, `incompatible value type`},
		{`(1, 2, 3) = (1, 2)`, `expected tuple (1, 2) to have a length of 3`},
		{`(1, 2) = (1, 'a')`, `tuples (1, 2), (1, 'a') are not comparable at index 2: unsupported comparison operator:`},
		{`1 IN ('a', 'b')`, `unsupported comparison operator: 1 IN ('a', 'b'): could not parse "a" as type int`},
		{`1 IN (1, 'a')`, `unsupported comparison operator: 1 IN (1, 'a'): could not parse "a" as type int`},
		{`1 = ANY 2`, `unsupported comparison operator: 1 = ANY 2: op ANY <right> requires array, tuple or subquery on right side`},
		{`1 = ANY ARRAY[2, 'a']`, `unsupported comparison operator: 1 = ANY ARRAY[2, 'a']: could not parse "a" as type int`},
		{`1 = ALL current_schemas(true)`, `unsupported comparison operator: <int> = ALL <string[]>`},
		{`1.0 BETWEEN 2 AND 'a'`, `unsupported comparison operator: <decimal> < <string>`},
		{`NULL BETWEEN 2 AND 'a'`, `unsupported comparison operator: <int> < <string>`},
		{`IF(1, 2, 3)`, `incompatible IF condition type: int`},
		{`IF(true, 'a', 2)`, `incompatible IF expressions: could not parse "a" as type int`},
		{`IF(true, 2, 'a')`, `incompatible IF expressions: could not parse "a" as type int`},
		{`IFNULL(1, 'a')`, `incompatible IFNULL expressions: could not parse "a" as type int`},
		{`NULLIF(1, 'a')`, `incompatible NULLIF expressions: could not parse "a" as type int`},
		{`COALESCE(1, 2, 3, 4, 'a')`, `incompatible COALESCE expressions: could not parse "a" as type int`},
		{`ARRAY[]`, `cannot determine type of empty array`},
		{`ANNOTATE_TYPE('a', int)`, `could not parse "a" as type int`},
		{`ANNOTATE_TYPE(ANNOTATE_TYPE(1, int8), decimal)`, `incompatible type annotation for ANNOTATE_TYPE(1, INT8) as decimal, found type: int`},
		{`3:::int[]`, `incompatible type annotation for 3 as int[], found type: int`},
		{`B'1001'::decimal`, `invalid cast: varbit -> decimal`},
		{`101.3::bit`, `invalid cast: decimal -> bit`},
		{`ARRAY[1] = ARRAY['foo']`, `could not parse "foo" as type int`},
		{`ARRAY[1]::int[] = ARRAY[1.0]::decimal[]`, `unsupported comparison operator: <int[]> = <decimal[]>`},
		{`ARRAY[1] @> ARRAY['foo']`, `unsupported comparison operator: <int[]> @> <string[]>`},
		{`ARRAY[1]::int[] @> ARRAY[1.0]::decimal[]`, `unsupported comparison operator: <int[]> @> <decimal[]>`},
		{
			`((1,2) AS a)`,
			`mismatch in tuple definition: 2 expressions, 1 labels`,
		},
		{
			`(ROW (1) AS a,b)`,
			`mismatch in tuple definition: 1 expressions, 2 labels`,
		},
		{
			`(((1,2) AS a,a)).a`,
			`column reference "a" is ambiguous`,
		},
		{
			`((ROW (1, '2') AS b,b)).b`,
			`column reference "b" is ambiguous`,
		},
		{
			`((ROW (1, '2') AS a,b)).x`,
			`could not identify column "x" in tuple{int AS a, string AS b}`,
		},
		{
			`(((1, '2') AS a,b)).x`,
			`could not identify column "x" in tuple{int AS a, string AS b}`,
		},
		{
			`(pg_get_keywords()).foo`,
			`could not identify column "foo" in tuple{string AS word, string AS catcode, string AS catdesc}`,
		},
		{
			`((1,2,3)).foo`,
			`could not identify column "foo" in record data type`,
		},
		{
			`1::d.notatype`,
			`type "d.notatype" does not exist`,
		},
		{
			`1 + 2::d.s.typ`,
			`type "d.s.typ" does not exist`,
		},
	}
	ctx := context.Background()
	for _, d := range testData {
		t.Run(d.expr, func(t *testing.T) {
			// Test with a nil and non-nil semaCtx.
			t.Run("semaCtx not nil", func(t *testing.T) {
				semaCtx := tree.MakeSemaContext()
				semaCtx.TypeResolver = mapResolver
				expr, err := parser.ParseExpr(d.expr)
				if err != nil {
					t.Fatalf("%s: %v", d.expr, err)
				}
				if _, err := tree.TypeCheck(ctx, expr, &semaCtx, types.Any); !testutils.IsError(err, regexp.QuoteMeta(d.expected)) {
					t.Errorf("%s: expected %s, but found %v", d.expr, d.expected, err)
				}
			})
			t.Run("semaCtx is nil", func(t *testing.T) {
				expr, err := parser.ParseExpr(d.expr)
				if err != nil {
					t.Fatalf("%s: %v", d.expr, err)
				}
				if _, err := tree.TypeCheck(ctx, expr, nil, types.Any); !testutils.IsError(err, regexp.QuoteMeta(d.expected)) {
					t.Errorf("%s: expected %s, but found %v", d.expr, d.expected, err)
				}
			})
		})
	}
}

func TestTypeCheckVolatility(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// $1 has type timestamptz.
	placeholderTypes := []*types.T{types.TimestampTZ}

	testCases := []struct {
		expr       string
		volatility tree.Volatility
	}{
		{"1 + 1", tree.VolatilityImmutable},
		{"random()", tree.VolatilityVolatile},
		{"now()", tree.VolatilityStable},
		{"'2020-01-01 01:02:03'::timestamp", tree.VolatilityImmutable},
		{"'now'::timestamp", tree.VolatilityStable},
		{"'2020-01-01 01:02:03'::timestamptz", tree.VolatilityStable},
		{"'1 hour'::interval", tree.VolatilityImmutable},
		{"$1", tree.VolatilityImmutable},

		// Stable cast with immutable input.
		{"$1::string", tree.VolatilityStable},

		// Stable binary operator with immutable inputs.
		{"$1 + '1 hour'::interval", tree.VolatilityStable},

		// Stable comparison operator with immutable inputs.
		{"$1 = '2020-01-01 01:02:03'::timestamp", tree.VolatilityStable},
	}

	ctx := context.Background()
	semaCtx := tree.MakeSemaContext()
	if err := semaCtx.Placeholders.Init(len(placeholderTypes), placeholderTypes); err != nil {
		t.Fatal(err)
	}

	typeCheck := func(sql string) error {
		expr, err := parser.ParseExpr(sql)
		if err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
		_, err = tree.TypeCheck(ctx, expr, &semaCtx, types.Any)
		return err
	}

	for _, tc := range testCases {
		// First, typecheck without any restrictions.
		semaCtx.Properties.Require("", 0 /* flags */)
		if err := typeCheck(tc.expr); err != nil {
			t.Fatalf("%s: %v", tc.expr, err)
		}

		semaCtx.Properties.Require("", tree.RejectVolatileFunctions)
		expectErr := tc.volatility == tree.VolatilityVolatile
		if err := typeCheck(tc.expr); err == nil && expectErr {
			t.Fatalf("%s: should have rejected volatile function", tc.expr)
		} else if err != nil && !expectErr {
			t.Fatalf("%s: %v", tc.expr, err)
		}

		semaCtx.Properties.Require("", tree.RejectStableOperators|tree.RejectVolatileFunctions)
		expectErr = tc.volatility >= tree.VolatilityStable
		if err := typeCheck(tc.expr); err == nil && expectErr {
			t.Fatalf("%s: should have rejected stable operator", tc.expr)
		} else if err != nil && !expectErr {
			t.Fatalf("%s: %v", tc.expr, err)
		}
	}
}
