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
	"github.com/cockroachdb/cockroach/util/decimal"
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
		{`1.0 AND true`, `incompatible AND argument type: decimal`},
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
		{`1.0 BETWEEN 2 AND '5'`, `expected 1.0 to be of type string, found type decimal`},
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
	mapArgsInt               = MapArgs{"a": TypeInt}
	mapArgsDecimal           = MapArgs{"a": TypeDecimal}
	mapArgsIntAndInt         = MapArgs{"a": TypeInt, "b": TypeInt}
	mapArgsIntAndDecimal     = MapArgs{"a": TypeInt, "b": TypeDecimal}
	mapArgsDecimalAndDecimal = MapArgs{"a": TypeDecimal, "b": TypeDecimal}
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
		decimal.SetFromFloat(&dd.Dec, f)
		return dd
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
		// Mixing resolved params with constants and resolved exprs.
		{mapArgsDecimal, nil, exprs(ddecimal(1), valArg("a")), TypeDecimal, mapArgsDecimal},
		{mapArgsDecimal, nil, exprs(intConst("1"), valArg("a")), TypeDecimal, mapArgsDecimal},
		{mapArgsDecimal, nil, exprs(decConst("1.1"), valArg("a")), TypeDecimal, mapArgsDecimal},
		{mapArgsInt, nil, exprs(intConst("1"), valArg("a")), TypeInt, mapArgsInt},
		{mapArgsInt, nil, exprs(decConst("1.0"), valArg("a")), TypeInt, mapArgsInt},
		{mapArgsDecimalAndDecimal, nil, exprs(valArg("b"), valArg("a")), TypeDecimal, mapArgsDecimalAndDecimal},
		// Mixing unresolved params with constants and resolved exprs.
		{nil, nil, exprs(ddecimal(1), valArg("a")), TypeDecimal, mapArgsDecimal},
		{nil, nil, exprs(intConst("1"), valArg("a")), TypeInt, mapArgsInt},
		{nil, nil, exprs(decConst("1.1"), valArg("a")), TypeDecimal, mapArgsDecimal},
		// Verify dealing with Null.
		{nil, nil, exprs(dnull), DNull, nil},
		{nil, nil, exprs(dnull, dnull), DNull, nil},
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
		{nil, TypeDecimal, exprs(valArg("a")), TypeDecimal, mapArgsDecimal},
		{nil, TypeDecimal, exprs(intConst("1"), valArg("a")), TypeDecimal, mapArgsDecimal},
		{nil, TypeDecimal, exprs(decConst("1.1"), valArg("a")), TypeDecimal, mapArgsDecimal},
	} {
		attemptTypeCheckSameTypedExprs(t, i, d)
	}
}

func TestTypeCheckSameTypedTupleExprs(t *testing.T) {
	for i, d := range []sameTypedExprsTestCase{
		// // Constants.
		{nil, nil, exprs(tuple(intConst("1"))), dtuple(TypeInt), nil},
		{nil, nil, exprs(tuple(intConst("1"), intConst("1"))), dtuple(TypeInt, TypeInt), nil},
		{nil, nil, exprs(tuple(intConst("1")), tuple(intConst("1"))), dtuple(TypeInt), nil},
		{nil, nil, exprs(tuple(intConst("1")), tuple(decConst("1.0"))), dtuple(TypeDecimal), nil},
		{nil, nil, exprs(tuple(intConst("1")), tuple(decConst("1.1"))), dtuple(TypeDecimal), nil},
		// Resolved exprs.
		{nil, nil, exprs(tuple(dint(1)), tuple(dint(1))), dtuple(TypeInt), nil},
		{nil, nil, exprs(tuple(dint(1), ddecimal(1)), tuple(dint(1), ddecimal(1))), dtuple(TypeInt, TypeDecimal), nil},
		// Mixing constants and resolved exprs.
		{nil, nil, exprs(tuple(dint(1), decConst("1.1")), tuple(intConst("1"), ddecimal(1))), dtuple(TypeInt, TypeDecimal), nil},
		{nil, nil, exprs(tuple(dint(1), decConst("1.0")), tuple(intConst("1"), dint(1))), dtuple(TypeInt, TypeInt), nil},
		// Mixing resolved params with constants and resolved exprs.
		{mapArgsDecimal, nil, exprs(tuple(ddecimal(1), intConst("1")), tuple(valArg("a"), valArg("a"))), dtuple(TypeDecimal, TypeDecimal), mapArgsDecimal},
		{mapArgsDecimalAndDecimal, nil, exprs(tuple(valArg("b"), intConst("1")), tuple(valArg("a"), valArg("a"))), dtuple(TypeDecimal, TypeDecimal), mapArgsDecimalAndDecimal},
		{mapArgsIntAndDecimal, nil, exprs(tuple(intConst("1"), intConst("1")), tuple(valArg("a"), valArg("b"))), dtuple(TypeInt, TypeDecimal), mapArgsIntAndDecimal},
		// Mixing unresolved params with constants and resolved exprs.
		{nil, nil, exprs(tuple(ddecimal(1), intConst("1")), tuple(valArg("a"), valArg("a"))), dtuple(TypeDecimal, TypeDecimal), mapArgsDecimal},
		{nil, nil, exprs(tuple(intConst("1"), intConst("1")), tuple(valArg("a"), valArg("b"))), dtuple(TypeInt, TypeInt), mapArgsIntAndInt},
		// Verify dealing with Null.
		{nil, nil, exprs(tuple(intConst("1"), dnull), tuple(dnull, decConst("1"))), dtuple(TypeInt, TypeDecimal), nil},
		{nil, nil, exprs(tuple(dint(1), dnull), tuple(dnull, ddecimal(1))), dtuple(TypeInt, TypeDecimal), nil},
		// Verify desired type when possible.
		{nil, dtuple(TypeInt, TypeDecimal), exprs(tuple(intConst("1"), intConst("1")), tuple(intConst("1"), intConst("1"))), dtuple(TypeInt, TypeDecimal), nil},
		// Verify desired type when possible with unresolved constants.
		{nil, dtuple(TypeInt, TypeDecimal), exprs(tuple(valArg("a"), intConst("1")), tuple(intConst("1"), valArg("b"))), dtuple(TypeInt, TypeDecimal), mapArgsIntAndDecimal},
	} {
		attemptTypeCheckSameTypedExprs(t, i, d)
	}
}

func TestTypeCheckSameTypedExprsError(t *testing.T) {
	decimalIntMismatchErr := `expected .* to be of type (decimal|int), found type (decimal|int)`
	tupleFloatIntMismatchErr := `tuples .* are not the same type: ` + decimalIntMismatchErr
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
		{nil, nil, exprs(dint(1), decConst("1.1")), decimalIntMismatchErr},
		{nil, nil, exprs(dint(1), ddecimal(1)), decimalIntMismatchErr},
		{mapArgsInt, nil, exprs(ddecimal(1.1), valArg("a")), decimalIntMismatchErr},
		{mapArgsInt, nil, exprs(decConst("1.1"), valArg("a")), decimalIntMismatchErr},
		{mapArgsIntAndDecimal, nil, exprs(valArg("b"), valArg("a")), decimalIntMismatchErr},
		// Tuple type mismatches.
		{nil, nil, exprs(tuple(dint(1)), tuple(ddecimal(1))), tupleFloatIntMismatchErr},
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

func TestProcessPlaceholderAnnotations(t *testing.T) {
	intType := &IntColType{Name: "INT"}
	boolType := &BoolColType{Name: "BOOLEAN"}

	testData := []struct {
		stmtExprs []Expr
		desired   MapArgs
	}{
		{[]Expr{ValArg{"a"}}, MapArgs{}},
		{[]Expr{ValArg{"a"}, ValArg{"b"}}, MapArgs{}},
		{[]Expr{
			&CastExpr{Expr: ValArg{"a"}, Type: intType},
			&CastExpr{Expr: ValArg{"a"}, Type: boolType},
		}, MapArgs{}},
		{[]Expr{
			&CastExpr{Expr: ValArg{"a"}, Type: intType},
			&CastExpr{Expr: ValArg{"b"}, Type: boolType},
		}, MapArgs{
			"a": TypeInt,
			"b": TypeBool,
		}},
		{[]Expr{
			&CastExpr{Expr: ValArg{"a"}, Type: intType},
			&CastExpr{Expr: ValArg{"b"}, Type: boolType},
			&CastExpr{Expr: ValArg{"a"}, Type: intType},
			&CastExpr{Expr: ValArg{"b"}, Type: intType},
		}, MapArgs{
			"a": TypeInt,
		}},
		{[]Expr{
			&CastExpr{Expr: ValArg{"a"}, Type: intType},
			&CastExpr{Expr: ValArg{"b"}, Type: boolType},
			ValArg{"a"},
		}, MapArgs{
			"b": TypeBool,
		}},
		{[]Expr{
			ValArg{"a"},
			&CastExpr{Expr: ValArg{"a"}, Type: intType},
			&CastExpr{Expr: ValArg{"b"}, Type: boolType},
		}, MapArgs{
			"b": TypeBool,
		}},
	}
	for i, d := range testData {
		args := make(MapArgs)
		stmt := &ValuesClause{Tuples: []*Tuple{{Exprs: d.stmtExprs}}}
		if err := ProcessPlaceholderAnnotations(stmt, args); err != nil {
			t.Errorf("%d: unexpected error returned from ProcessPlaceholderAnnotations: %v", i, err)
		} else if !reflect.DeepEqual(args, d.desired) {
			t.Errorf("%d: expected args %v after processing placeholder annotations for %v, found %v", i, d.desired, stmt, args)
		}
	}
}
