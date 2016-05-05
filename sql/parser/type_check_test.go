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

	"github.com/cockroachdb/cockroach/testutils"
)

func TestTypeCheck(t *testing.T) {
	testData := []string{
		`NULL + 1`,
		`NULL + 1.1`,
		`NULL + '2006-09-23'::date`,
		`NULL + '1h'::interval`,
		`NULL + 'hello'`,
		`NULL::int`,
		`NULL + 'hello'::bytes`,
		`(1.1::decimal)::decimal`,
		`NULL = 1`,
		`1 = NULL`,
		`true AND NULL`,
		`NULL OR false`,
		`1 IN (SELECT 1)`,
		`IF(true, 2, 3)`,
		`IF(false, 2, 3)`,
		`IF(NULL, 2, 3)`,
		`IF(NULL, 2, 3.0)`,
		`IF(true, (1, 2), (1, 3))`,
		`IFNULL(1, 2)`,
		`IFNULL(1, 2.0)`,
		`IFNULL(NULL, 2)`,
		`IFNULL(2, NULL)`,
		`IFNULL((1, 2), (1, 3))`,
		`NULLIF(1, 2)`,
		`NULLIF(1, 2.0)`,
		`NULLIF(NULL, 2)`,
		`NULLIF(2, NULL)`,
		`NULLIF((1, 2), (1, 3))`,
		`COALESCE(1, 2, 3, 4, 5)`,
		`COALESCE(1, 2.0)`,
		`COALESCE(NULL, 2)`,
		`COALESCE(2, NULL)`,
		`COALESCE((1, 2), (1, 3))`,
		`true IS NULL`,
		`true IS NOT NULL`,
		`true IS TRUE`,
		`true IS NOT TRUE`,
		`true IS FALSE`,
		`true IS NOT FALSE`,
		`CASE 1 WHEN 1 THEN (1, 2) ELSE (1, 3) END`,
		`1 BETWEEN 2 AND 3`,
		`COUNT(3)`,
	}
	for _, d := range testData {
		expr, err := ParseExprTraditional(d)
		if err != nil {
			t.Fatalf("%s: %v", d, err)
		}
		if _, err := TypeCheck(expr, nil, NoTypePreference); err != nil {
			t.Errorf("%s: unexpected error %s", d, err)
		}
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
		{`'10' > 2`, `unsupported comparison operator:`},
		{`a`, `qualified name "a" not found`},
		{`1 AND true`, `incompatible AND argument type: int`},
		{`1.0 AND true`, `incompatible AND argument type: float`},
		{`'a' OR true`, `incompatible OR argument type: string`},
		{`(1, 2) OR true`, `incompatible OR argument type: tuple`},
		{`NOT 1`, `incompatible NOT argument type: int`},
		{`lower()`, `unknown signature for lower: lower()`},
		{`lower(1, 2)`, `unknown signature for lower: lower(int, int)`},
		{`lower(1)`, `unknown signature for lower: lower(int)`},
		{`1::date`, `invalid cast: int -> DATE`},
		{`1::timestamp`, `invalid cast: int -> TIMESTAMP`},
		{`CASE 'one' WHEN 1 THEN 1 WHEN 'two' THEN 2 END`, `incompatible condition type`},
		{`CASE 1 WHEN 1 THEN 'one' WHEN 2 THEN 2 END`, `incompatible value type`},
		{`CASE 1 WHEN 1 THEN 'one' ELSE 2 END`, `incompatible value type`},
		{`(1, 2, 3) = (1, 2)`, `expected tuple (1, 2) to have a length of 3`},
		{`(1, 2) = (1, 'a')`, `tuples (1, 2), (1, 'a') are not the same type: expected 2 to be of type string, found type int`},
		{`1 IN ('a', 'b')`, `unsupported comparison operator: 1 IN ('a', 'b'): expected 1 to be of type string, found type int`},
		{`1 IN (1, 'a')`, `unsupported comparison operator: 1 IN (1, 'a'): expected 1 to be of type string, found type int`},
		{`1.0 BETWEEN 2 AND '5'`, `expected 1.0 to be of type string, found type float`},
		{`IF(1, 2, 3)`, `incompatible IF condition type: int`},
		{`IF(true, 2, '5')`, `incompatible IF expressions: expected 2 to be of type string, found type int`},
		{`IFNULL(1, '5')`, `incompatible IFNULL expressions: expected 1 to be of type string, found type int`},
		{`NULLIF(1, '5')`, `incompatible NULLIF expressions: expected 1 to be of type string, found type int`},
		{`COALESCE(1, 2, 3, 4, '5')`, `incompatible COALESCE expressions: expected 1 to be of type string, found type int`},
	}
	for _, d := range testData {
		expr, err := ParseExprTraditional(d.expr)
		if err != nil {
			t.Fatalf("%s: %v", d.expr, err)
		}
		if _, err := TypeCheck(expr, nil, NoTypePreference); !testutils.IsError(err, regexp.QuoteMeta(d.expected)) {
			t.Errorf("%s: expected %s, but found %v", d.expr, d.expected, err)
		}
	}
}

var (
	mapArgsInt           = MapArgs{"a": DummyInt}
	mapArgsFloat         = MapArgs{"a": DummyFloat}
	mapArgsIntAndInt     = MapArgs{"a": DummyInt, "b": DummyInt}
	mapArgsIntAndFloat   = MapArgs{"a": DummyInt, "b": DummyFloat}
	mapArgsFloatAndFloat = MapArgs{"a": DummyFloat, "b": DummyFloat}
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
func floatConst(s string) copyableExpr {
	return func() Expr {
		return &NumVal{Value: constant.MakeFromLiteral(s, token.FLOAT, 0), OrigString: s}
	}
}
func dint(i DInt) copyableExpr {
	return func() Expr {
		return NewDInt(i)
	}
}
func dfloat(f DFloat) copyableExpr {
	return func() Expr {
		return NewDFloat(f)
	}
}
func valArg(name string) copyableExpr {
	return func() Expr {
		return ValArg{name}
	}
}
func tuple(exprs ...copyableExpr) copyableExpr {
	return func() Expr {
		return &Tuple{Exprs: buildExprs(exprs)}
	}
}
func dtuple(datums ...Datum) *DTuple {
	dt := DTuple(datums)
	return &dt
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

func cloneMapArgs(args MapArgs) MapArgs {
	clone := make(MapArgs)
	for k, v := range args {
		clone[k] = v
	}
	return clone
}

type sameTypedExprsTestCase struct {
	args    MapArgs
	desired Datum
	exprs   []copyableExpr

	expectedType Datum
	expectedArgs MapArgs
}

func attemptTypeCheckSameTypedExprs(t *testing.T, idx int, test sameTypedExprsTestCase) {
	if test.expectedArgs == nil {
		test.expectedArgs = make(MapArgs)
	}
	forEachPerm(test.exprs, 0, func(exprs []copyableExpr) {
		args := cloneMapArgs(test.args)
		_, typ, err := typeCheckSameTypedExprs(args, test.desired, buildExprs(exprs)...)
		if err != nil {
			t.Errorf("%d: unexpected error returned from typeCheckSameTypedExprs: %v", idx, err)
		} else {
			if !typ.TypeEqual(test.expectedType) {
				t.Errorf("%d: expected type %s:%s when type checking %s:%s, found %s", idx, test.expectedType, test.expectedType.Type(), buildExprs(exprs), typ, typ.Type())
			}
			if !reflect.DeepEqual(args, test.expectedArgs) {
				t.Errorf("%d: expected args %v after typeCheckSameTypedExprs for %v, found %v", idx, test.expectedArgs, buildExprs(exprs), args)
			}
		}
	})
}

func TestTypeCheckSameTypedExprs(t *testing.T) {
	for i, d := range []sameTypedExprsTestCase{
		// Constants.
		{nil, nil, exprs(intConst("1")), DummyInt, nil},
		{nil, nil, exprs(floatConst("1.1")), DummyFloat, nil},
		{nil, nil, exprs(intConst("1"), floatConst("1.0")), DummyFloat, nil},
		{nil, nil, exprs(intConst("1"), floatConst("1.1")), DummyFloat, nil},
		// Resolved exprs.
		{nil, nil, exprs(dint(1)), DummyInt, nil},
		{nil, nil, exprs(dfloat(1)), DummyFloat, nil},
		// Mixing constants and resolved exprs.
		{nil, nil, exprs(dint(1), intConst("1")), DummyInt, nil},
		{nil, nil, exprs(dint(1), floatConst("1.0")), DummyInt, nil}, // This is what the AST would look like after folding (0.6 + 0.4).
		{nil, nil, exprs(dint(1), dint(1)), DummyInt, nil},
		{nil, nil, exprs(dfloat(1), intConst("1")), DummyFloat, nil},
		{nil, nil, exprs(dfloat(1), floatConst("1.1")), DummyFloat, nil},
		{nil, nil, exprs(dfloat(1), dfloat(1)), DummyFloat, nil},
		// Mixing resolved params with constants and resolved exprs.
		{mapArgsFloat, nil, exprs(dfloat(1), valArg("a")), DummyFloat, mapArgsFloat},
		{mapArgsFloat, nil, exprs(intConst("1"), valArg("a")), DummyFloat, mapArgsFloat},
		{mapArgsFloat, nil, exprs(floatConst("1.1"), valArg("a")), DummyFloat, mapArgsFloat},
		{mapArgsInt, nil, exprs(intConst("1"), valArg("a")), DummyInt, mapArgsInt},
		{mapArgsInt, nil, exprs(floatConst("1.0"), valArg("a")), DummyInt, mapArgsInt},
		{mapArgsFloatAndFloat, nil, exprs(valArg("b"), valArg("a")), DummyFloat, mapArgsFloatAndFloat},
		// Mixing unresolved params with constants and resolved exprs.
		{nil, nil, exprs(dfloat(1), valArg("a")), DummyFloat, mapArgsFloat},
		{nil, nil, exprs(intConst("1"), valArg("a")), DummyInt, mapArgsInt},
		{nil, nil, exprs(floatConst("1.1"), valArg("a")), DummyFloat, mapArgsFloat},
		// Verify dealing with Null.
		{nil, nil, exprs(dnull), DNull, nil},
		{nil, nil, exprs(dnull, dnull), DNull, nil},
		{nil, nil, exprs(dnull, intConst("1")), DummyInt, nil},
		{nil, nil, exprs(dnull, floatConst("1.1")), DummyFloat, nil},
		{nil, nil, exprs(dnull, dint(1)), DummyInt, nil},
		{nil, nil, exprs(dnull, dfloat(1)), DummyFloat, nil},
		{nil, nil, exprs(dnull, dfloat(1), intConst("1")), DummyFloat, nil},
		{nil, nil, exprs(dnull, dfloat(1), floatConst("1.1")), DummyFloat, nil},
		{nil, nil, exprs(dnull, dfloat(1), floatConst("1.1")), DummyFloat, nil},
		{nil, nil, exprs(dnull, intConst("1"), floatConst("1.1")), DummyFloat, nil},
		// Verify desired type when possible.
		{nil, DummyInt, exprs(intConst("1")), DummyInt, nil},
		{nil, DummyInt, exprs(dint(1)), DummyInt, nil},
		{nil, DummyInt, exprs(floatConst("1.0")), DummyInt, nil},
		{nil, DummyInt, exprs(floatConst("1.1")), DummyFloat, nil},
		{nil, DummyInt, exprs(dfloat(1)), DummyFloat, nil},
		{nil, DummyFloat, exprs(intConst("1")), DummyFloat, nil},
		{nil, DummyFloat, exprs(dint(1)), DummyInt, nil},
		{nil, DummyInt, exprs(intConst("1"), floatConst("1.0")), DummyInt, nil},
		{nil, DummyInt, exprs(intConst("1"), floatConst("1.1")), DummyFloat, nil},
		{nil, DummyFloat, exprs(intConst("1"), floatConst("1.1")), DummyFloat, nil},
		// Verify desired type when possible with unresolved placeholders.
		{nil, DummyFloat, exprs(valArg("a")), DummyFloat, mapArgsFloat},
		{nil, DummyFloat, exprs(intConst("1"), valArg("a")), DummyFloat, mapArgsFloat},
		{nil, DummyFloat, exprs(floatConst("1.1"), valArg("a")), DummyFloat, mapArgsFloat},
	} {
		attemptTypeCheckSameTypedExprs(t, i, d)
	}
}

func TestTypeCheckSameTypedTupleExprs(t *testing.T) {
	for i, d := range []sameTypedExprsTestCase{
		// // Constants.
		{nil, nil, exprs(tuple(intConst("1"))), dtuple(DummyInt), nil},
		{nil, nil, exprs(tuple(intConst("1"), intConst("1"))), dtuple(DummyInt, DummyInt), nil},
		{nil, nil, exprs(tuple(intConst("1")), tuple(intConst("1"))), dtuple(DummyInt), nil},
		{nil, nil, exprs(tuple(intConst("1")), tuple(floatConst("1.0"))), dtuple(DummyFloat), nil},
		{nil, nil, exprs(tuple(intConst("1")), tuple(floatConst("1.1"))), dtuple(DummyFloat), nil},
		// Resolved exprs.
		{nil, nil, exprs(tuple(dint(1)), tuple(dint(1))), dtuple(DummyInt), nil},
		{nil, nil, exprs(tuple(dint(1), dfloat(1)), tuple(dint(1), dfloat(1))), dtuple(DummyInt, DummyFloat), nil},
		// Mixing constants and resolved exprs.
		{nil, nil, exprs(tuple(dint(1), floatConst("1.1")), tuple(intConst("1"), dfloat(1))), dtuple(DummyInt, DummyFloat), nil},
		{nil, nil, exprs(tuple(dint(1), floatConst("1.0")), tuple(intConst("1"), dint(1))), dtuple(DummyInt, DummyInt), nil},
		// Mixing resolved params with constants and resolved exprs.
		{mapArgsFloat, nil, exprs(tuple(dfloat(1), intConst("1")), tuple(valArg("a"), valArg("a"))), dtuple(DummyFloat, DummyFloat), mapArgsFloat},
		{mapArgsFloatAndFloat, nil, exprs(tuple(valArg("b"), intConst("1")), tuple(valArg("a"), valArg("a"))), dtuple(DummyFloat, DummyFloat), mapArgsFloatAndFloat},
		{mapArgsIntAndFloat, nil, exprs(tuple(intConst("1"), intConst("1")), tuple(valArg("a"), valArg("b"))), dtuple(DummyInt, DummyFloat), mapArgsIntAndFloat},
		// Mixing unresolved params with constants and resolved exprs.
		{nil, nil, exprs(tuple(dfloat(1), intConst("1")), tuple(valArg("a"), valArg("a"))), dtuple(DummyFloat, DummyFloat), mapArgsFloat},
		{nil, nil, exprs(tuple(intConst("1"), intConst("1")), tuple(valArg("a"), valArg("b"))), dtuple(DummyInt, DummyInt), mapArgsIntAndInt},
		// Verify dealing with Null.
		{nil, nil, exprs(tuple(intConst("1"), dnull), tuple(dnull, floatConst("1"))), dtuple(DummyInt, DummyFloat), nil},
		{nil, nil, exprs(tuple(dint(1), dnull), tuple(dnull, dfloat(1))), dtuple(DummyInt, DummyFloat), nil},
		// Verify desired type when possible.
		{nil, dtuple(DummyInt, DummyFloat), exprs(tuple(intConst("1"), intConst("1")), tuple(intConst("1"), intConst("1"))), dtuple(DummyInt, DummyFloat), nil},
		// Verify desired type when possible with unresolved constants.
		{nil, dtuple(DummyInt, DummyFloat), exprs(tuple(valArg("a"), intConst("1")), tuple(intConst("1"), valArg("b"))), dtuple(DummyInt, DummyFloat), mapArgsIntAndFloat},
	} {
		attemptTypeCheckSameTypedExprs(t, i, d)
	}
}

func TestTypeCheckSameTypedExprsError(t *testing.T) {
	floatIntMismatchErr := `expected .* to be of type (float|int), found type (float|int)`
	tupleFloatIntMismatchErr := `tuples .* are not the same type: ` + floatIntMismatchErr
	tupleIntMismatchErr := `expected .* to be of type (tuple|int), found type (tuple|int)`
	tupleLenErr := `expected tuple .* to have a length of .*`
	paramErr := `could not determine data type of parameter .*`

	testData := []struct {
		args    MapArgs
		desired Datum
		exprs   []copyableExpr

		expectedErr string
	}{
		// Single type mismatches.
		{nil, nil, exprs(dint(1), floatConst("1.1")), floatIntMismatchErr},
		{nil, nil, exprs(dint(1), dfloat(1)), floatIntMismatchErr},
		{mapArgsInt, nil, exprs(dfloat(1.1), valArg("a")), floatIntMismatchErr},
		{mapArgsInt, nil, exprs(floatConst("1.1"), valArg("a")), floatIntMismatchErr},
		{mapArgsIntAndFloat, nil, exprs(valArg("b"), valArg("a")), floatIntMismatchErr},
		// Tuple type mismatches.
		{nil, nil, exprs(tuple(dint(1)), tuple(dfloat(1))), tupleFloatIntMismatchErr},
		{nil, nil, exprs(tuple(dint(1)), dint(1), dint(1)), tupleIntMismatchErr},
		{nil, nil, exprs(tuple(dint(1)), tuple(dint(1), dint(1))), tupleLenErr},
		// Parameter ambiguity.
		{nil, nil, exprs(valArg("b"), valArg("a")), paramErr},
	}
	for i, d := range testData {
		forEachPerm(d.exprs, 0, func(exprs []copyableExpr) {
			if _, _, err := typeCheckSameTypedExprs(d.args, d.desired, buildExprs(exprs)...); !testutils.IsError(err, d.expectedErr) {
				t.Errorf("%d: expected %s, but found %v", i, d.expectedErr, err)
			}
		})
	}
}
