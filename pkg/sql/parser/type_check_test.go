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
// Author: Tamir Duberstein (tamird@gmail.com)

package parser

import (
	"go/constant"
	"go/token"
	"reflect"
	"regexp"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils"
)

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
		{`NULL + 'hello'`, `NULL`},
		{`NULL::int`, `NULL::INT`},
		{`NULL + 'hello'::bytes`, `NULL`},
		{`INTERVAL '1s'`, `'1s':::INTERVAL`},
		{`(1.1::decimal)::decimal`, `(1.1:::DECIMAL::DECIMAL)::DECIMAL`},
		{`NULL = 1`, `NULL = 1:::INT`},
		{`1 = NULL`, `1:::INT = NULL`},
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
		{`true = SOME (ARRAY[true, false])`, `true = SOME (ARRAY[true, false])`},
		{`1.3 = ALL ARRAY[1, 2, 3]`, `1.3:::DECIMAL = ALL ARRAY[1:::DECIMAL, 2:::DECIMAL, 3:::DECIMAL]`},
		{`1.3 = ALL ((ARRAY[]))`, `1.3:::DECIMAL = ALL ((ARRAY[]))`},
		{`NULL = ALL ARRAY[1.5, 2.5, 3.5]`, `NULL = ALL ARRAY[1.5:::DECIMAL, 2.5:::DECIMAL, 3.5:::DECIMAL]`},
		{`NULL = ALL ARRAY[NULL, NULL]`, `NULL = ALL ARRAY[NULL, NULL]`},
		{`1 = ALL NULL`, `1:::INT = ALL NULL`},
		{`'a' = ALL CURRENT_SCHEMAS(true)`, `'a':::STRING = ALL current_schemas(true)`},
		{`NULL = ALL CURRENT_SCHEMAS(true)`, `NULL = ALL current_schemas(true)`},

		{`INTERVAL '1'`, `'1s':::INTERVAL`},
		{`DECIMAL '1.0'`, `'1.0':::STRING::DECIMAL`},

		{`1 + 2`, `3:::INT`},
		{`1:::decimal + 2`, `1:::DECIMAL + 2:::DECIMAL`},
		{`1:::float + 2`, `1.0:::FLOAT + 2.0:::FLOAT`},
		{`INTERVAL '1.5s' * 2`, `'1s500ms':::INTERVAL * 2:::INT`},
		{`2 * INTERVAL '1.5s'`, `2:::INT * '1s500ms':::INTERVAL`},

		{`1 + $1`, `1:::INT + $1:::INT`},
		{`1:::DECIMAL + $1`, `1:::DECIMAL + $1:::DECIMAL`},
		{`$1:::INT`, `$1:::INT`},

		{`'NaN'::decimal`, `'NaN':::STRING::DECIMAL`},
		{`'-NaN'::decimal`, `'-NaN':::STRING::DECIMAL`},
		{`'Inf'::decimal`, `'Inf':::STRING::DECIMAL`},
		{`'-Inf'::decimal`, `'-Inf':::STRING::DECIMAL`},
	}
	for _, d := range testData {
		expr, err := ParseExpr(d.expr)
		if err != nil {
			t.Errorf("%s: %v", d.expr, err)
			continue
		}
		ctx := MakeSemaContext()
		typeChecked, err := TypeCheck(expr, &ctx, TypeAny)
		if err != nil {
			t.Errorf("%s: unexpected error %s", d.expr, err)
		} else if s := Serialize(typeChecked); s != d.expected {
			t.Errorf("%s: expected %s, but found %s", d.expr, d.expected, s)
		}
	}
}

func TestTypeCheckNormalize(t *testing.T) {
	testData := []struct {
		expr     string
		expected string
	}{
		{`'NaN'::decimal`, `'NaN'::DECIMAL:::DECIMAL`},
		{`'-NaN'::decimal`, `'NaN'::DECIMAL:::DECIMAL`},
		{`'Inf'::decimal`, `'Infinity'::DECIMAL:::DECIMAL`},
		{`'-Inf'::decimal`, `'-Infinity'::DECIMAL:::DECIMAL`},
	}
	for _, d := range testData {
		t.Run(d.expr, func(t *testing.T) {
			expr, err := ParseExpr(d.expr)
			if err != nil {
				t.Fatal(err)
			}
			ctx := MakeSemaContext()
			typeChecked, err := TypeCheck(expr, &ctx, TypeAny)
			if err != nil {
				t.Fatal(err)
			}
			evalCtx := &EvalContext{}
			typedExpr, err := evalCtx.NormalizeExpr(typeChecked)
			if err != nil {
				t.Fatal(err)
			}
			if s := Serialize(typedExpr); s != d.expected {
				t.Errorf("expected %s, but found %s", d.expected, s)
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
		// This strange error is a result of the <date> + <int> overload and because
		// of the limitation described on StrVal.AvailableTypes.
		{`'a' + 0`, `could not parse 'a' as type date`},
		{`1.1 # 3.1`, `unsupported binary operator:`},
		{`~0.1`, `unsupported unary operator:`},
		{`'10' > 2`, `unsupported comparison operator:`},
		{`a`, `name "a" is not defined`},
		{`COS(*)`, `cannot use "*" in this context`},
		{`a.*`, `cannot use "a.*" in this context`},
		{`1 AND true`, `incompatible AND argument type: int`},
		{`1.0 AND true`, `incompatible AND argument type: decimal`},
		{`'a' OR true`, `could not parse 'a' as type bool`},
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
		{`(1, 2) = (1, 'a')`, `tuples (1, 2), (1, 'a') are not comparable at index 2: unsupported comparison operator: <int> = <string>`},
		{`1 IN ('a', 'b')`, `unsupported comparison operator: 1 IN ('a', 'b'): expected 'a' to be of type int, found type string`},
		{`1 IN (1, 'a')`, `unsupported comparison operator: 1 IN (1, 'a'): expected 'a' to be of type int, found type string`},
		{`1 = ANY 2`, `unsupported comparison operator: 1 = ANY 2: op ANY array requires array on right side`},
		{`1 = ANY ARRAY[2, '3']`, `unsupported comparison operator: 1 ANY = ARRAY[2, '3']: expected '3' to be of type int, found type string`},
		{`1 = ALL CURRENT_SCHEMAS(true)`, `unsupported comparison operator: <int> = ALL <string[]>`},
		{`1.0 BETWEEN 2 AND '5'`, `unsupported comparison operator: <decimal> < <string>`},
		{`IF(1, 2, 3)`, `incompatible IF condition type: int`},
		{`IF(true, '5', 2)`, `incompatible IF expressions: expected 2 to be of type string, found type int`},
		{`IF(true, 2, '5')`, `incompatible IF expressions: expected '5' to be of type int, found type string`},
		{`IFNULL(1, '5')`, `incompatible IFNULL expressions: expected '5' to be of type int, found type string`},
		{`NULLIF(1, '5')`, `incompatible NULLIF expressions: expected '5' to be of type int, found type string`},
		{`COALESCE(1, 2, 3, 4, '5')`, `incompatible COALESCE expressions: expected '5' to be of type int, found type string`},
		{`ARRAY[]`, `cannot determine type of empty array`},
		{`ANNOTATE_TYPE('a', int)`, `incompatible type annotation for 'a' as int, found type: string`},
		{`ANNOTATE_TYPE(ANNOTATE_TYPE(1, int), decimal)`, `incompatible type annotation for ANNOTATE_TYPE(1, INT) as decimal, found type: int`},
		{`3:::int[]`, `incompatible type annotation for 3 as int[], found type: int`},
	}
	for _, d := range testData {
		expr, err := ParseExpr(d.expr)
		if err != nil {
			t.Fatalf("%s: %v", d.expr, err)
		}
		if _, err := TypeCheck(expr, nil, TypeAny); !testutils.IsError(err, regexp.QuoteMeta(d.expected)) {
			t.Errorf("%s: expected %s, but found %v", d.expr, d.expected, err)
		}
	}
}

var (
	mapPTypesInt               = PlaceholderTypes{"a": TypeInt}
	mapPTypesDecimal           = PlaceholderTypes{"a": TypeDecimal}
	mapPTypesIntAndInt         = PlaceholderTypes{"a": TypeInt, "b": TypeInt}
	mapPTypesIntAndDecimal     = PlaceholderTypes{"a": TypeInt, "b": TypeDecimal}
	mapPTypesDecimalAndDecimal = PlaceholderTypes{"a": TypeDecimal, "b": TypeDecimal}
)

// copyableExpr can provide each test permutation with a deep copy of the expression tree.
type copyableExpr func() Expr

func buildExprs(exprs []copyableExpr) []Expr {
	freshExprs := make([]Expr, 0, len(exprs))
	for _, expr := range exprs {
		freshExprs = append(freshExprs, expr())
	}
	return freshExprs
}

var dnull = func() Expr {
	return DNull
}

func exprs(fns ...copyableExpr) []copyableExpr {
	return fns
}
func intConst(s string) copyableExpr {
	return func() Expr {
		return &NumVal{Value: constant.MakeFromLiteral(s, token.INT, 0), OrigString: s}
	}
}
func decConst(s string) copyableExpr {
	return func() Expr {
		return &NumVal{Value: constant.MakeFromLiteral(s, token.FLOAT, 0), OrigString: s}
	}
}
func dint(i DInt) copyableExpr {
	return func() Expr {
		return NewDInt(i)
	}
}
func ddecimal(f float64) copyableExpr {
	return func() Expr {
		dd := &DDecimal{}
		if _, err := dd.SetFloat64(f); err != nil {
			panic(err)
		}
		return dd
	}
}
func placeholder(name string) copyableExpr {
	return func() Expr {
		return NewPlaceholder(name)
	}
}
func tuple(exprs ...copyableExpr) copyableExpr {
	return func() Expr {
		return &Tuple{Exprs: buildExprs(exprs)}
	}
}
func ttuple(types ...Type) TTuple {
	return TTuple(types)
}

func forEachPerm(exprs []copyableExpr, i int, fn func([]copyableExpr)) {
	if i == len(exprs)-1 {
		fn(exprs)
	}
	for j := i; j < len(exprs); j++ {
		exprs[i], exprs[j] = exprs[j], exprs[i]
		forEachPerm(exprs, i+1, fn)
		exprs[i], exprs[j] = exprs[j], exprs[i]
	}
}

func clonePlaceholderTypes(args PlaceholderTypes) PlaceholderTypes {
	clone := make(PlaceholderTypes)
	for k, v := range args {
		clone[k] = v
	}
	return clone
}

type sameTypedExprsTestCase struct {
	ptypes  PlaceholderTypes
	desired Type
	exprs   []copyableExpr

	expectedType   Type
	expectedPTypes PlaceholderTypes
}

func attemptTypeCheckSameTypedExprs(t *testing.T, idx int, test sameTypedExprsTestCase) {
	if test.expectedPTypes == nil {
		test.expectedPTypes = make(PlaceholderTypes)
	}
	forEachPerm(test.exprs, 0, func(exprs []copyableExpr) {
		ctx := MakeSemaContext()
		ctx.Placeholders.SetTypes(clonePlaceholderTypes(test.ptypes))
		desired := TypeAny
		if test.desired != nil {
			desired = test.desired
		}
		_, typ, err := typeCheckSameTypedExprs(&ctx, desired, buildExprs(exprs)...)
		if err != nil {
			t.Errorf("%d: unexpected error returned from typeCheckSameTypedExprs: %v", idx, err)
		} else {
			if !typ.Equivalent(test.expectedType) {
				t.Errorf("%d: expected type %s when type checking %s, found %s",
					idx, test.expectedType, buildExprs(exprs), typ)
			}
			if !reflect.DeepEqual(ctx.Placeholders.Types, test.expectedPTypes) {
				t.Errorf("%d: expected placeholder types %v after typeCheckSameTypedExprs for %v, found %v", idx, test.expectedPTypes, buildExprs(exprs), ctx.Placeholders.Types)
			}
		}
	})
}

func TestTypeCheckSameTypedExprs(t *testing.T) {
	for i, d := range []sameTypedExprsTestCase{
		// Constants.
		{nil, nil, exprs(intConst("1")), TypeInt, nil},
		{nil, nil, exprs(decConst("1.1")), TypeDecimal, nil},
		{nil, nil, exprs(intConst("1"), decConst("1.0")), TypeDecimal, nil},
		{nil, nil, exprs(intConst("1"), decConst("1.1")), TypeDecimal, nil},
		// Resolved exprs.
		{nil, nil, exprs(dint(1)), TypeInt, nil},
		{nil, nil, exprs(ddecimal(1)), TypeDecimal, nil},
		// Mixing constants and resolved exprs.
		{nil, nil, exprs(dint(1), intConst("1")), TypeInt, nil},
		{nil, nil, exprs(dint(1), decConst("1.0")), TypeInt, nil}, // This is what the AST would look like after folding (0.6 + 0.4).
		{nil, nil, exprs(dint(1), dint(1)), TypeInt, nil},
		{nil, nil, exprs(ddecimal(1), intConst("1")), TypeDecimal, nil},
		{nil, nil, exprs(ddecimal(1), decConst("1.1")), TypeDecimal, nil},
		{nil, nil, exprs(ddecimal(1), ddecimal(1)), TypeDecimal, nil},
		// Mixing resolved placeholders with constants and resolved exprs.
		{mapPTypesDecimal, nil, exprs(ddecimal(1), placeholder("a")), TypeDecimal, mapPTypesDecimal},
		{mapPTypesDecimal, nil, exprs(intConst("1"), placeholder("a")), TypeDecimal, mapPTypesDecimal},
		{mapPTypesDecimal, nil, exprs(decConst("1.1"), placeholder("a")), TypeDecimal, mapPTypesDecimal},
		{mapPTypesInt, nil, exprs(intConst("1"), placeholder("a")), TypeInt, mapPTypesInt},
		{mapPTypesInt, nil, exprs(decConst("1.0"), placeholder("a")), TypeInt, mapPTypesInt},
		{mapPTypesDecimalAndDecimal, nil, exprs(placeholder("b"), placeholder("a")), TypeDecimal, mapPTypesDecimalAndDecimal},
		// Mixing unresolved placeholders with constants and resolved exprs.
		{nil, nil, exprs(ddecimal(1), placeholder("a")), TypeDecimal, mapPTypesDecimal},
		{nil, nil, exprs(intConst("1"), placeholder("a")), TypeInt, mapPTypesInt},
		{nil, nil, exprs(decConst("1.1"), placeholder("a")), TypeDecimal, mapPTypesDecimal},
		// Verify dealing with Null.
		{nil, nil, exprs(dnull), TypeNull, nil},
		{nil, nil, exprs(dnull, dnull), TypeNull, nil},
		{nil, nil, exprs(dnull, intConst("1")), TypeInt, nil},
		{nil, nil, exprs(dnull, decConst("1.1")), TypeDecimal, nil},
		{nil, nil, exprs(dnull, dint(1)), TypeInt, nil},
		{nil, nil, exprs(dnull, ddecimal(1)), TypeDecimal, nil},
		{nil, nil, exprs(dnull, ddecimal(1), intConst("1")), TypeDecimal, nil},
		{nil, nil, exprs(dnull, ddecimal(1), decConst("1.1")), TypeDecimal, nil},
		{nil, nil, exprs(dnull, ddecimal(1), decConst("1.1")), TypeDecimal, nil},
		{nil, nil, exprs(dnull, intConst("1"), decConst("1.1")), TypeDecimal, nil},
		// Verify desired type when possible.
		{nil, TypeInt, exprs(intConst("1")), TypeInt, nil},
		{nil, TypeInt, exprs(dint(1)), TypeInt, nil},
		{nil, TypeInt, exprs(decConst("1.0")), TypeInt, nil},
		{nil, TypeInt, exprs(decConst("1.1")), TypeDecimal, nil},
		{nil, TypeInt, exprs(ddecimal(1)), TypeDecimal, nil},
		{nil, TypeDecimal, exprs(intConst("1")), TypeDecimal, nil},
		{nil, TypeDecimal, exprs(dint(1)), TypeInt, nil},
		{nil, TypeInt, exprs(intConst("1"), decConst("1.0")), TypeInt, nil},
		{nil, TypeInt, exprs(intConst("1"), decConst("1.1")), TypeDecimal, nil},
		{nil, TypeDecimal, exprs(intConst("1"), decConst("1.1")), TypeDecimal, nil},
		// Verify desired type when possible with unresolved placeholders.
		{nil, TypeDecimal, exprs(placeholder("a")), TypeDecimal, mapPTypesDecimal},
		{nil, TypeDecimal, exprs(intConst("1"), placeholder("a")), TypeDecimal, mapPTypesDecimal},
		{nil, TypeDecimal, exprs(decConst("1.1"), placeholder("a")), TypeDecimal, mapPTypesDecimal},
	} {
		attemptTypeCheckSameTypedExprs(t, i, d)
	}
}

func TestTypeCheckSameTypedTupleExprs(t *testing.T) {
	for i, d := range []sameTypedExprsTestCase{
		// // Constants.
		{nil, nil, exprs(tuple(intConst("1"))), ttuple(TypeInt), nil},
		{nil, nil, exprs(tuple(intConst("1"), intConst("1"))), ttuple(TypeInt, TypeInt), nil},
		{nil, nil, exprs(tuple(intConst("1")), tuple(intConst("1"))), ttuple(TypeInt), nil},
		{nil, nil, exprs(tuple(intConst("1")), tuple(decConst("1.0"))), ttuple(TypeDecimal), nil},
		{nil, nil, exprs(tuple(intConst("1")), tuple(decConst("1.1"))), ttuple(TypeDecimal), nil},
		// Resolved exprs.
		{nil, nil, exprs(tuple(dint(1)), tuple(dint(1))), ttuple(TypeInt), nil},
		{nil, nil, exprs(tuple(dint(1), ddecimal(1)), tuple(dint(1), ddecimal(1))), ttuple(TypeInt, TypeDecimal), nil},
		// Mixing constants and resolved exprs.
		{nil, nil, exprs(tuple(dint(1), decConst("1.1")), tuple(intConst("1"), ddecimal(1))), ttuple(TypeInt, TypeDecimal), nil},
		{nil, nil, exprs(tuple(dint(1), decConst("1.0")), tuple(intConst("1"), dint(1))), ttuple(TypeInt, TypeInt), nil},
		// Mixing resolved placeholders with constants and resolved exprs.
		{mapPTypesDecimal, nil, exprs(tuple(ddecimal(1), intConst("1")), tuple(placeholder("a"), placeholder("a"))), ttuple(TypeDecimal, TypeDecimal), mapPTypesDecimal},
		{mapPTypesDecimalAndDecimal, nil, exprs(tuple(placeholder("b"), intConst("1")), tuple(placeholder("a"), placeholder("a"))), ttuple(TypeDecimal, TypeDecimal), mapPTypesDecimalAndDecimal},
		{mapPTypesIntAndDecimal, nil, exprs(tuple(intConst("1"), intConst("1")), tuple(placeholder("a"), placeholder("b"))), ttuple(TypeInt, TypeDecimal), mapPTypesIntAndDecimal},
		// Mixing unresolved placeholders with constants and resolved exprs.
		{nil, nil, exprs(tuple(ddecimal(1), intConst("1")), tuple(placeholder("a"), placeholder("a"))), ttuple(TypeDecimal, TypeDecimal), mapPTypesDecimal},
		{nil, nil, exprs(tuple(intConst("1"), intConst("1")), tuple(placeholder("a"), placeholder("b"))), ttuple(TypeInt, TypeInt), mapPTypesIntAndInt},
		// Verify dealing with Null.
		{nil, nil, exprs(tuple(intConst("1"), dnull), tuple(dnull, decConst("1"))), ttuple(TypeInt, TypeDecimal), nil},
		{nil, nil, exprs(tuple(dint(1), dnull), tuple(dnull, ddecimal(1))), ttuple(TypeInt, TypeDecimal), nil},
		// Verify desired type when possible.
		{nil, ttuple(TypeInt, TypeDecimal), exprs(tuple(intConst("1"), intConst("1")), tuple(intConst("1"), intConst("1"))), ttuple(TypeInt, TypeDecimal), nil},
		// Verify desired type when possible with unresolved constants.
		{nil, ttuple(TypeInt, TypeDecimal), exprs(tuple(placeholder("a"), intConst("1")), tuple(intConst("1"), placeholder("b"))), ttuple(TypeInt, TypeDecimal), mapPTypesIntAndDecimal},
	} {
		attemptTypeCheckSameTypedExprs(t, i, d)
	}
}

func TestTypeCheckSameTypedExprsError(t *testing.T) {
	decimalIntMismatchErr := `expected .* to be of type (decimal|int), found type (decimal|int)`
	tupleFloatIntMismatchErr := `tuples .* are not the same type: ` + decimalIntMismatchErr
	tupleIntMismatchErr := `expected .* to be of type (tuple|int), found type (tuple|int)`
	tupleLenErr := `expected tuple .* to have a length of .*`
	placeholderErr := `could not determine data type of placeholder .*`

	testData := []struct {
		ptypes  PlaceholderTypes
		desired Type
		exprs   []copyableExpr

		expectedErr string
	}{
		// Single type mismatches.
		{nil, nil, exprs(dint(1), decConst("1.1")), decimalIntMismatchErr},
		{nil, nil, exprs(dint(1), ddecimal(1)), decimalIntMismatchErr},
		{mapPTypesInt, nil, exprs(ddecimal(1.1), placeholder("a")), decimalIntMismatchErr},
		{mapPTypesInt, nil, exprs(decConst("1.1"), placeholder("a")), decimalIntMismatchErr},
		{mapPTypesIntAndDecimal, nil, exprs(placeholder("b"), placeholder("a")), decimalIntMismatchErr},
		// Tuple type mismatches.
		{nil, nil, exprs(tuple(dint(1)), tuple(ddecimal(1))), tupleFloatIntMismatchErr},
		{nil, nil, exprs(tuple(dint(1)), dint(1), dint(1)), tupleIntMismatchErr},
		{nil, nil, exprs(tuple(dint(1)), tuple(dint(1), dint(1))), tupleLenErr},
		// Placeholder ambiguity.
		{nil, nil, exprs(placeholder("b"), placeholder("a")), placeholderErr},
	}
	for i, d := range testData {
		ctx := MakeSemaContext()
		ctx.Placeholders.SetTypes(d.ptypes)
		desired := TypeAny
		if d.desired != nil {
			desired = d.desired
		}
		forEachPerm(d.exprs, 0, func(exprs []copyableExpr) {
			if _, _, err := typeCheckSameTypedExprs(&ctx, desired, buildExprs(exprs)...); !testutils.IsError(err, d.expectedErr) {
				t.Errorf("%d: expected %s, but found %v", i, d.expectedErr, err)
			}
		})
	}
}

func cast(p *Placeholder, typ ColumnType) Expr {
	return &CastExpr{Expr: p, Type: typ}
}
func annot(p *Placeholder, typ ColumnType) Expr {
	return &AnnotateTypeExpr{Expr: p, Type: typ}
}

func TestProcessPlaceholderAnnotations(t *testing.T) {
	intType := intColTypeInt
	boolType := boolColTypeBoolean

	testData := []struct {
		initArgs  PlaceholderTypes
		stmtExprs []Expr
		desired   PlaceholderTypes
	}{
		{
			PlaceholderTypes{},
			[]Expr{NewPlaceholder("a")},
			PlaceholderTypes{},
		},
		{
			PlaceholderTypes{},
			[]Expr{NewPlaceholder("a"), NewPlaceholder("b")},
			PlaceholderTypes{},
		},
		{
			PlaceholderTypes{"b": TypeBool},
			[]Expr{NewPlaceholder("a"), NewPlaceholder("b")},
			PlaceholderTypes{"b": TypeBool},
		},
		{
			PlaceholderTypes{"c": TypeFloat},
			[]Expr{NewPlaceholder("a"), NewPlaceholder("b")},
			PlaceholderTypes{"c": TypeFloat},
		},
		{
			PlaceholderTypes{},
			[]Expr{
				cast(NewPlaceholder("a"), intType),
				cast(NewPlaceholder("a"), boolType),
			},
			PlaceholderTypes{},
		},
		{
			PlaceholderTypes{"a": TypeFloat},
			[]Expr{
				cast(NewPlaceholder("a"), intType),
				cast(NewPlaceholder("a"), boolType),
			},
			PlaceholderTypes{"a": TypeFloat},
		},
		{
			PlaceholderTypes{},
			[]Expr{
				cast(NewPlaceholder("a"), intType),
				cast(NewPlaceholder("b"), boolType),
			},
			PlaceholderTypes{"a": TypeInt, "b": TypeBool},
		},
		{
			PlaceholderTypes{},
			[]Expr{
				annot(NewPlaceholder("a"), intType),
				annot(NewPlaceholder("b"), boolType),
			},
			PlaceholderTypes{"a": TypeInt, "b": TypeBool},
		},
		{
			PlaceholderTypes{"b": TypeBool},
			[]Expr{
				annot(NewPlaceholder("a"), intType),
			},
			PlaceholderTypes{"a": TypeInt, "b": TypeBool},
		},
		{
			PlaceholderTypes{},
			[]Expr{
				cast(NewPlaceholder("a"), intType),
				cast(NewPlaceholder("b"), boolType),
				cast(NewPlaceholder("a"), intType),
				cast(NewPlaceholder("b"), intType),
			},
			PlaceholderTypes{"a": TypeInt},
		},
		{
			PlaceholderTypes{},
			[]Expr{
				cast(NewPlaceholder("a"), intType),
				annot(NewPlaceholder("b"), boolType),
				cast(NewPlaceholder("a"), intType),
				cast(NewPlaceholder("b"), intType),
			},
			PlaceholderTypes{"a": TypeInt, "b": TypeBool},
		},
		{
			PlaceholderTypes{},
			[]Expr{
				cast(NewPlaceholder("a"), intType),
				cast(NewPlaceholder("b"), boolType),
				NewPlaceholder("a"),
			},
			PlaceholderTypes{"b": TypeBool},
		},
		{
			PlaceholderTypes{},
			[]Expr{
				NewPlaceholder("a"),
				cast(NewPlaceholder("a"), intType),
				cast(NewPlaceholder("b"), boolType),
			},
			PlaceholderTypes{"b": TypeBool},
		},
		{
			PlaceholderTypes{},
			[]Expr{
				annot(NewPlaceholder("a"), intType),
				annot(NewPlaceholder("b"), boolType),
				NewPlaceholder("a"),
			},
			PlaceholderTypes{"a": TypeInt, "b": TypeBool},
		},
		{
			PlaceholderTypes{},
			[]Expr{
				NewPlaceholder("a"),
				annot(NewPlaceholder("a"), intType),
				annot(NewPlaceholder("b"), boolType),
			},
			PlaceholderTypes{"a": TypeInt, "b": TypeBool},
		},
		{
			PlaceholderTypes{"a": TypeFloat, "b": TypeBool},
			[]Expr{
				NewPlaceholder("a"),
				cast(NewPlaceholder("a"), intType),
				cast(NewPlaceholder("b"), boolType),
			},
			PlaceholderTypes{"a": TypeFloat, "b": TypeBool},
		},
		{
			PlaceholderTypes{"a": TypeFloat, "b": TypeFloat},
			[]Expr{
				NewPlaceholder("a"),
				cast(NewPlaceholder("a"), intType),
				cast(NewPlaceholder("b"), boolType),
			},
			PlaceholderTypes{"a": TypeFloat, "b": TypeFloat},
		},
		{
			PlaceholderTypes{},
			[]Expr{
				cast(NewPlaceholder("a"), intType),
				annot(NewPlaceholder("a"), intType),
				cast(NewPlaceholder("a"), intType),
			},
			PlaceholderTypes{"a": TypeInt},
		},
		{
			PlaceholderTypes{},
			[]Expr{
				cast(NewPlaceholder("a"), intType),
				annot(NewPlaceholder("a"), boolType),
				cast(NewPlaceholder("a"), intType),
			},
			PlaceholderTypes{"a": TypeBool},
		},
	}
	for i, d := range testData {
		args := d.initArgs
		stmt := &ValuesClause{Tuples: []*Tuple{{Exprs: d.stmtExprs}}}
		if err := args.ProcessPlaceholderAnnotations(stmt); err != nil {
			t.Errorf("%d: unexpected error returned from ProcessPlaceholderAnnotations: %v", i, err)
		} else if !reflect.DeepEqual(args, d.desired) {
			t.Errorf("%d: expected args %v after processing placeholder annotations for %v, found %v", i, d.desired, stmt, args)
		}
	}
}

func TestProcessPlaceholderAnnotationsError(t *testing.T) {
	intType := intColTypeInt
	floatType := floatColTypeFloat

	testData := []struct {
		initArgs  PlaceholderTypes
		stmtExprs []Expr
		expected  string
	}{
		{
			PlaceholderTypes{},
			[]Expr{
				annot(NewPlaceholder("a"), floatType),
				annot(NewPlaceholder("a"), intType),
			},
			"multiple conflicting type annotations around a",
		},
		{
			PlaceholderTypes{},
			[]Expr{
				annot(NewPlaceholder("a"), floatType),
				cast(NewPlaceholder("a"), floatType),
				cast(NewPlaceholder("b"), floatType),
				annot(NewPlaceholder("a"), intType),
			},
			"multiple conflicting type annotations around a",
		},
		{
			PlaceholderTypes{"a": TypeFloat},
			[]Expr{
				annot(NewPlaceholder("a"), intType),
			},
			"type annotation around a that conflicts with previously inferred type float",
		},
		{
			PlaceholderTypes{"a": TypeFloat},
			[]Expr{
				cast(NewPlaceholder("a"), intType),
				annot(NewPlaceholder("a"), intType),
			},
			"type annotation around a that conflicts with previously inferred type float",
		},
		{
			PlaceholderTypes{"a": TypeFloat},
			[]Expr{
				annot(NewPlaceholder("a"), floatType),
				annot(NewPlaceholder("a"), intType),
			},
			"type annotation around a that conflicts with previously inferred type float",
		},
		{
			PlaceholderTypes{"a": TypeFloat},
			[]Expr{
				annot(NewPlaceholder("a"), intType),
				annot(NewPlaceholder("a"), floatType),
			},
			"type annotation around a that conflicts with previously inferred type float",
		},
	}
	for i, d := range testData {
		args := d.initArgs
		stmt := &ValuesClause{Tuples: []*Tuple{{Exprs: d.stmtExprs}}}
		if err := args.ProcessPlaceholderAnnotations(stmt); !testutils.IsError(err, d.expected) {
			t.Errorf("%d: expected %s, but found %v", i, d.expected, err)
		}
	}
}
