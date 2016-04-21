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
		if _, err := TypeCheck(expr, nil, nil); err != nil {
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
		{`(1, 2, 3) = (1, 2)`, `unequal number of entries in tuple expressions`},
		{`(1, 2) = (1, 'a')`, `unsupported comparison operator`},
		{`1 IN ('a', 'b')`, `unsupported comparison operator:`},
		{`1 IN (1, 'a')`, `unsupported comparison operator`},
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
		if _, err := TypeCheck(expr, nil, nil); !testutils.IsError(err, regexp.QuoteMeta(d.expected)) {
			t.Errorf("%s: expected %s, but found %v", d.expr, d.expected, err)
		}
	}
}

func forEachPerm(exprs []Expr, i int, fn func([]Expr)) {
	if i == len(exprs)-1 {
		fn(exprs)
	}
	for j := i; j < len(exprs); j++ {
		exprs[i], exprs[j] = exprs[j], exprs[i]
		forEachPerm(exprs, i+1, fn)
		exprs[i], exprs[j] = exprs[j], exprs[i]
	}
}

func TestTypeCheckSameTypedExprs(t *testing.T) {
	intConst := func(s string) Expr {
		return &NumVal{Value: constant.MakeFromLiteral(s, token.INT, 0), OrigString: s}
	}
	floatConst := func(s string) Expr {
		return &NumVal{Value: constant.MakeFromLiteral(s, token.FLOAT, 0), OrigString: s}
	}

	testData := []struct {
		args         MapArgs
		desired      Datum
		exprs        []Expr
		expectedType Datum
	}{
		// Constants.
		{nil, nil, []Expr{intConst("1")}, DummyInt},
		{nil, nil, []Expr{floatConst("1")}, DummyFloat},
		{nil, nil, []Expr{intConst("1"), floatConst("1")}, DummyFloat},
		// Resolved exprs.
		{nil, nil, []Expr{NewDInt(1)}, DummyInt},
		{nil, nil, []Expr{NewDFloat(1)}, DummyFloat},
		// Mixing constants and resolved exprs.
		{nil, nil, []Expr{NewDInt(1), intConst("1")}, DummyInt},
		{nil, nil, []Expr{NewDInt(1), floatConst("1")}, DummyInt}, // This is what the AST would look like after folding (0.6 + 0.4).
		{nil, nil, []Expr{NewDInt(1), NewDInt(1)}, DummyInt},
		{nil, nil, []Expr{NewDFloat(1), intConst("1")}, DummyFloat},
		{nil, nil, []Expr{NewDFloat(1), floatConst("1")}, DummyFloat},
		{nil, nil, []Expr{NewDFloat(1), NewDFloat(1)}, DummyFloat},
		// Mixing resolved constants and resolved exprs with MapArgs.
		{MapArgs{"a": DummyFloat}, nil, []Expr{NewDFloat(1), ValArg{"a"}}, DummyFloat},
		{MapArgs{"a": DummyFloat}, nil, []Expr{intConst("1"), ValArg{"a"}}, DummyFloat},
		{MapArgs{"a": DummyFloat}, nil, []Expr{floatConst("1"), ValArg{"a"}}, DummyFloat},
		{MapArgs{"a": DummyInt}, nil, []Expr{intConst("1"), ValArg{"a"}}, DummyInt},
		{MapArgs{"a": DummyInt}, nil, []Expr{floatConst("1"), ValArg{"a"}}, DummyInt},
		{MapArgs{"a": DummyFloat, "b": DummyFloat}, nil, []Expr{ValArg{"b"}, ValArg{"a"}}, DummyFloat},
		// Mixing unresolved constants and resolved exprs with MapArgs.
		{nil, nil, []Expr{NewDFloat(1), ValArg{"a"}}, DummyFloat},
		{nil, nil, []Expr{intConst("1"), ValArg{"a"}}, DummyInt},
		{nil, nil, []Expr{floatConst("1"), ValArg{"a"}}, DummyFloat},
		// Verify dealing with Null.
		{nil, nil, []Expr{DNull}, DNull},
		{nil, nil, []Expr{DNull, DNull}, DNull},
		{nil, nil, []Expr{DNull, intConst("1")}, DummyInt},
		{nil, nil, []Expr{DNull, floatConst("1")}, DummyFloat},
		{nil, nil, []Expr{DNull, NewDInt(1)}, DummyInt},
		{nil, nil, []Expr{DNull, NewDFloat(1)}, DummyFloat},
		{nil, nil, []Expr{DNull, NewDFloat(1), intConst("1")}, DummyFloat},
		{nil, nil, []Expr{DNull, NewDFloat(1), floatConst("1")}, DummyFloat},
		{nil, nil, []Expr{DNull, NewDFloat(1), floatConst("1")}, DummyFloat},
		{nil, nil, []Expr{DNull, intConst("1"), floatConst("1")}, DummyFloat},
		// Verify desired type when possible.
		{nil, DummyInt, []Expr{intConst("1")}, DummyInt},
		{nil, DummyInt, []Expr{NewDInt(1)}, DummyInt},
		{nil, DummyInt, []Expr{floatConst("1")}, DummyInt},
		{nil, DummyInt, []Expr{NewDFloat(1)}, DummyFloat},
		{nil, DummyFloat, []Expr{intConst("1")}, DummyFloat},
		{nil, DummyFloat, []Expr{NewDInt(1)}, DummyInt},
		{nil, DummyInt, []Expr{intConst("1"), floatConst("1")}, DummyInt},
		{nil, DummyInt, []Expr{intConst("1"), floatConst("1.1")}, DummyFloat},
		{nil, DummyFloat, []Expr{intConst("1"), floatConst("1")}, DummyFloat},
		// Verify desired type when possible with unresolved constants.
		{nil, DummyFloat, []Expr{ValArg{"a"}}, DummyFloat},
		{nil, DummyFloat, []Expr{intConst("1"), ValArg{"a"}}, DummyFloat},
		{nil, DummyFloat, []Expr{floatConst("1"), ValArg{"a"}}, DummyFloat},
	}
	for i, d := range testData {
		forEachPerm(d.exprs, 0, func(exprs []Expr) {
			_, typ, err := typeCheckSameTypedExprs(d.args, d.desired, exprs...)
			if err != nil {
				t.Errorf("%d: unexpected error returned from typeCheckSameTypedExprs: %v", i, err)
			} else if !typ.TypeEqual(d.expectedType) {
				t.Errorf("%d: expected type %s when type checking %s, found %s", i, d.expectedType.Type(), exprs, typ.Type())
			}
		})
	}
}

func TestTypeCheckSameTypedExprsError(t *testing.T) {
	floatConst := func(s string) Expr {
		return &NumVal{Value: constant.MakeFromLiteral(s, token.FLOAT, 0), OrigString: s}
	}

	floatIntMismatchErr := `expected .* to be of type (float|int), found type (float|int)`
	paramErr := `could not determine data type of parameter .*`

	testData := []struct {
		args        MapArgs
		desired     Datum
		exprs       []Expr
		expectedErr string
	}{
		{nil, nil, []Expr{NewDInt(1), floatConst("1.1")}, floatIntMismatchErr},
		{nil, nil, []Expr{NewDInt(1), NewDFloat(1)}, floatIntMismatchErr},
		{MapArgs{"a": DummyInt}, nil, []Expr{NewDFloat(1.1), ValArg{"a"}}, floatIntMismatchErr},
		{MapArgs{"a": DummyInt}, nil, []Expr{floatConst("1.1"), ValArg{"a"}}, floatIntMismatchErr},
		{MapArgs{"a": DummyFloat, "b": DummyInt}, nil, []Expr{ValArg{"b"}, ValArg{"a"}}, floatIntMismatchErr},
		{nil, nil, []Expr{ValArg{"b"}, ValArg{"a"}}, paramErr},
	}
	for i, d := range testData {
		forEachPerm(d.exprs, 0, func(exprs []Expr) {
			if _, _, err := typeCheckSameTypedExprs(d.args, d.desired, exprs...); !testutils.IsError(err, d.expectedErr) {
				t.Errorf("%d: expected %s, but found %v", i, d.expectedErr, err)
			}
		})
	}
}
