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

package tree_test

import (
	"context"
	"go/constant"
	"go/token"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/testutils"
)

func BenchmarkTypeCheck(b *testing.B) {
	// random example from TPCC
	sql := `CASE 1 >= $1 + 10 WHEN true THEN 1-$1 ELSE (1-$1)+91 END`
	expr, err := parser.ParseExpr(sql)
	if err != nil {
		b.Fatalf("%s: %v", expr, err)
	}
	ctx := tree.MakeSemaContext(false)
	for i := 0; i < b.N; i++ {
		_, err := tree.TypeCheck(expr, &ctx, types.Int)
		if err != nil {
			b.Fatalf("unexpected error: %s", err)
		}
	}
}

func TestTypeCheckNormalize(t *testing.T) {
	testData := []struct {
		expr     string
		expected string
	}{
		{`'NaN'::decimal`, `'NaN':::DECIMAL`},
		{`'-NaN'::decimal`, `'NaN':::DECIMAL`},
		{`'Inf'::decimal`, `'Infinity':::DECIMAL`},
		{`'-Inf'::decimal`, `'-Infinity':::DECIMAL`},
	}
	for _, d := range testData {
		t.Run(d.expr, func(t *testing.T) {
			expr, err := parser.ParseExpr(d.expr)
			if err != nil {
				t.Fatal(err)
			}
			ctx := tree.MakeSemaContext(false)
			typeChecked, err := tree.TypeCheck(expr, &ctx, types.Any)
			if err != nil {
				t.Fatal(err)
			}
			evalCtx := tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
			defer evalCtx.Stop(context.Background())
			typedExpr, err := evalCtx.NormalizeExpr(typeChecked)
			if err != nil {
				t.Fatal(err)
			}
			if s := tree.Serialize(typedExpr); s != d.expected {
				t.Errorf("expected %s, but found %s", d.expected, s)
			}
		})
	}
}

var (
	mapPTypesInt               = tree.PlaceholderTypes{1: types.Int}
	mapPTypesDecimal           = tree.PlaceholderTypes{1: types.Decimal}
	mapPTypesIntAndInt         = tree.PlaceholderTypes{1: types.Int, 2: types.Int}
	mapPTypesIntAndDecimal     = tree.PlaceholderTypes{1: types.Int, 2: types.Decimal}
	mapPTypesDecimalAndDecimal = tree.PlaceholderTypes{1: types.Decimal, 2: types.Decimal}
)

// copyableExpr can provide each test permutation with a deep copy of the expression tree.
type copyableExpr func() tree.Expr

func buildExprs(exprs []copyableExpr) []tree.Expr {
	freshExprs := make([]tree.Expr, 0, len(exprs))
	for _, expr := range exprs {
		freshExprs = append(freshExprs, expr())
	}
	return freshExprs
}

var dnull = func() tree.Expr {
	return tree.DNull
}

func exprs(fns ...copyableExpr) []copyableExpr {
	return fns
}
func intConst(s string) copyableExpr {
	return func() tree.Expr {
		return &tree.NumVal{Value: constant.MakeFromLiteral(s, token.INT, 0), OrigString: s}
	}
}
func decConst(s string) copyableExpr {
	return func() tree.Expr {
		return &tree.NumVal{Value: constant.MakeFromLiteral(s, token.FLOAT, 0), OrigString: s}
	}
}
func dint(i tree.DInt) copyableExpr {
	return func() tree.Expr {
		return tree.NewDInt(i)
	}
}
func ddecimal(f float64) copyableExpr {
	return func() tree.Expr {
		dd := &tree.DDecimal{}
		if _, err := dd.SetFloat64(f); err != nil {
			panic(err)
		}
		return dd
	}
}
func placeholder(id types.PlaceholderIdx) copyableExpr {
	return func() tree.Expr {
		return newPlaceholder(id)
	}
}
func tuple(exprs ...copyableExpr) copyableExpr {
	return func() tree.Expr {
		return &tree.Tuple{Exprs: buildExprs(exprs)}
	}
}
func ttuple(tys ...types.T) types.TTuple {
	return types.TTuple{Types: tys}
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

func clonePlaceholderTypes(args tree.PlaceholderTypes) tree.PlaceholderTypes {
	clone := make(tree.PlaceholderTypes)
	for k, v := range args {
		clone[k] = v
	}
	return clone
}

type sameTypedExprsTestCase struct {
	ptypes  tree.PlaceholderTypes
	desired types.T
	exprs   []copyableExpr

	expectedType   types.T
	expectedPTypes tree.PlaceholderTypes
}

func attemptTypeCheckSameTypedExprs(t *testing.T, idx int, test sameTypedExprsTestCase) {
	if test.expectedPTypes == nil {
		test.expectedPTypes = make(tree.PlaceholderTypes)
	}
	forEachPerm(test.exprs, 0, func(exprs []copyableExpr) {
		ctx := tree.MakeSemaContext(false)
		ctx.Placeholders.SetTypeHints(clonePlaceholderTypes(test.ptypes))
		desired := types.Any
		if test.desired != nil {
			desired = test.desired
		}
		_, typ, err := tree.TypeCheckSameTypedExprs(&ctx, desired, buildExprs(exprs)...)
		if err != nil {
			t.Errorf("%d: unexpected error returned from typeCheckSameTypedExprs: %v", idx, err)
		} else {
			if !typ.Equivalent(test.expectedType) {
				t.Errorf("%d: expected type %s when type checking %s, found %s",
					idx, test.expectedType, buildExprs(exprs), typ)
			}
			if !reflect.DeepEqual(ctx.Placeholders.Types, test.expectedPTypes) {
				t.Errorf("%d: expected placeholder types %v after TypeCheckSameTypedExprs for %v, found %v", idx, test.expectedPTypes, buildExprs(exprs), ctx.Placeholders.Types)
			}
		}
	})
}

func TestTypeCheckSameTypedExprs(t *testing.T) {
	for i, d := range []sameTypedExprsTestCase{
		// Constants.
		{nil, nil, exprs(intConst("1")), types.Int, nil},
		{nil, nil, exprs(decConst("1.1")), types.Decimal, nil},
		{nil, nil, exprs(intConst("1"), decConst("1.0")), types.Decimal, nil},
		{nil, nil, exprs(intConst("1"), decConst("1.1")), types.Decimal, nil},
		// Resolved exprs.
		{nil, nil, exprs(dint(1)), types.Int, nil},
		{nil, nil, exprs(ddecimal(1)), types.Decimal, nil},
		// Mixing constants and resolved exprs.
		{nil, nil, exprs(dint(1), intConst("1")), types.Int, nil},
		{nil, nil, exprs(dint(1), decConst("1.0")), types.Int, nil}, // This is what the AST would look like after folding (0.6 + 0.4).
		{nil, nil, exprs(dint(1), dint(1)), types.Int, nil},
		{nil, nil, exprs(ddecimal(1), intConst("1")), types.Decimal, nil},
		{nil, nil, exprs(ddecimal(1), decConst("1.1")), types.Decimal, nil},
		{nil, nil, exprs(ddecimal(1), ddecimal(1)), types.Decimal, nil},
		// Mixing resolved placeholders with constants and resolved exprs.
		{mapPTypesDecimal, nil, exprs(ddecimal(1), placeholder(1)), types.Decimal, mapPTypesDecimal},
		{mapPTypesDecimal, nil, exprs(intConst("1"), placeholder(1)), types.Decimal, mapPTypesDecimal},
		{mapPTypesDecimal, nil, exprs(decConst("1.1"), placeholder(1)), types.Decimal, mapPTypesDecimal},
		{mapPTypesInt, nil, exprs(intConst("1"), placeholder(1)), types.Int, mapPTypesInt},
		{mapPTypesInt, nil, exprs(decConst("1.0"), placeholder(1)), types.Int, mapPTypesInt},
		{mapPTypesDecimalAndDecimal, nil, exprs(placeholder(2), placeholder(1)), types.Decimal, mapPTypesDecimalAndDecimal},
		// Mixing unresolved placeholders with constants and resolved exprs.
		{nil, nil, exprs(ddecimal(1), placeholder(1)), types.Decimal, mapPTypesDecimal},
		{nil, nil, exprs(intConst("1"), placeholder(1)), types.Int, mapPTypesInt},
		{nil, nil, exprs(decConst("1.1"), placeholder(1)), types.Decimal, mapPTypesDecimal},
		// Verify dealing with Null.
		{nil, nil, exprs(dnull), types.Unknown, nil},
		{nil, nil, exprs(dnull, dnull), types.Unknown, nil},
		{nil, nil, exprs(dnull, intConst("1")), types.Int, nil},
		{nil, nil, exprs(dnull, decConst("1.1")), types.Decimal, nil},
		{nil, nil, exprs(dnull, dint(1)), types.Int, nil},
		{nil, nil, exprs(dnull, ddecimal(1)), types.Decimal, nil},
		{nil, nil, exprs(dnull, ddecimal(1), intConst("1")), types.Decimal, nil},
		{nil, nil, exprs(dnull, ddecimal(1), decConst("1.1")), types.Decimal, nil},
		{nil, nil, exprs(dnull, ddecimal(1), decConst("1.1")), types.Decimal, nil},
		{nil, nil, exprs(dnull, intConst("1"), decConst("1.1")), types.Decimal, nil},
		// Verify desired type when possible.
		{nil, types.Int, exprs(intConst("1")), types.Int, nil},
		{nil, types.Int, exprs(dint(1)), types.Int, nil},
		{nil, types.Int, exprs(decConst("1.0")), types.Int, nil},
		{nil, types.Int, exprs(decConst("1.1")), types.Decimal, nil},
		{nil, types.Int, exprs(ddecimal(1)), types.Decimal, nil},
		{nil, types.Decimal, exprs(intConst("1")), types.Decimal, nil},
		{nil, types.Decimal, exprs(dint(1)), types.Int, nil},
		{nil, types.Int, exprs(intConst("1"), decConst("1.0")), types.Int, nil},
		{nil, types.Int, exprs(intConst("1"), decConst("1.1")), types.Decimal, nil},
		{nil, types.Decimal, exprs(intConst("1"), decConst("1.1")), types.Decimal, nil},
		// Verify desired type when possible with unresolved placeholders.
		{nil, types.Decimal, exprs(placeholder(1)), types.Decimal, mapPTypesDecimal},
		{nil, types.Decimal, exprs(intConst("1"), placeholder(1)), types.Decimal, mapPTypesDecimal},
		{nil, types.Decimal, exprs(decConst("1.1"), placeholder(1)), types.Decimal, mapPTypesDecimal},
	} {
		attemptTypeCheckSameTypedExprs(t, i, d)
	}
}

func TestTypeCheckSameTypedTupleExprs(t *testing.T) {
	for i, d := range []sameTypedExprsTestCase{
		// // Constants.
		{nil, nil, exprs(tuple(intConst("1"))), ttuple(types.Int), nil},
		{nil, nil, exprs(tuple(intConst("1"), intConst("1"))), ttuple(types.Int, types.Int), nil},
		{nil, nil, exprs(tuple(intConst("1")), tuple(intConst("1"))), ttuple(types.Int), nil},
		{nil, nil, exprs(tuple(intConst("1")), tuple(decConst("1.0"))), ttuple(types.Decimal), nil},
		{nil, nil, exprs(tuple(intConst("1")), tuple(decConst("1.1"))), ttuple(types.Decimal), nil},
		// Resolved exprs.
		{nil, nil, exprs(tuple(dint(1)), tuple(dint(1))), ttuple(types.Int), nil},
		{nil, nil, exprs(tuple(dint(1), ddecimal(1)), tuple(dint(1), ddecimal(1))), ttuple(types.Int, types.Decimal), nil},
		// Mixing constants and resolved exprs.
		{nil, nil, exprs(tuple(dint(1), decConst("1.1")), tuple(intConst("1"), ddecimal(1))), ttuple(types.Int, types.Decimal), nil},
		{nil, nil, exprs(tuple(dint(1), decConst("1.0")), tuple(intConst("1"), dint(1))), ttuple(types.Int, types.Int), nil},
		// Mixing resolved placeholders with constants and resolved exprs.
		{mapPTypesDecimal, nil, exprs(tuple(ddecimal(1), intConst("1")), tuple(placeholder(1), placeholder(1))), ttuple(types.Decimal, types.Decimal), mapPTypesDecimal},
		{mapPTypesDecimalAndDecimal, nil, exprs(tuple(placeholder(2), intConst("1")), tuple(placeholder(1), placeholder(1))), ttuple(types.Decimal, types.Decimal), mapPTypesDecimalAndDecimal},
		{mapPTypesIntAndDecimal, nil, exprs(tuple(intConst("1"), intConst("1")), tuple(placeholder(1), placeholder(2))), ttuple(types.Int, types.Decimal), mapPTypesIntAndDecimal},
		// Mixing unresolved placeholders with constants and resolved exprs.
		{nil, nil, exprs(tuple(ddecimal(1), intConst("1")), tuple(placeholder(1), placeholder(1))), ttuple(types.Decimal, types.Decimal), mapPTypesDecimal},
		{nil, nil, exprs(tuple(intConst("1"), intConst("1")), tuple(placeholder(1), placeholder(2))), ttuple(types.Int, types.Int), mapPTypesIntAndInt},
		// Verify dealing with Null.
		{nil, nil, exprs(tuple(intConst("1"), dnull), tuple(dnull, decConst("1"))), ttuple(types.Int, types.Decimal), nil},
		{nil, nil, exprs(tuple(dint(1), dnull), tuple(dnull, ddecimal(1))), ttuple(types.Int, types.Decimal), nil},
		// Verify desired type when possible.
		{nil, ttuple(types.Int, types.Decimal), exprs(tuple(intConst("1"), intConst("1")), tuple(intConst("1"), intConst("1"))), ttuple(types.Int, types.Decimal), nil},
		// Verify desired type when possible with unresolved constants.
		{nil, ttuple(types.Int, types.Decimal), exprs(tuple(placeholder(1), intConst("1")), tuple(intConst("1"), placeholder(2))), ttuple(types.Int, types.Decimal), mapPTypesIntAndDecimal},
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
		ptypes  tree.PlaceholderTypes
		desired types.T
		exprs   []copyableExpr

		expectedErr string
	}{
		// Single type mismatches.
		{nil, nil, exprs(dint(1), decConst("1.1")), decimalIntMismatchErr},
		{nil, nil, exprs(dint(1), ddecimal(1)), decimalIntMismatchErr},
		{mapPTypesInt, nil, exprs(decConst("1.1"), placeholder(1)), decimalIntMismatchErr},
		// Tuple type mismatches.
		{nil, nil, exprs(tuple(dint(1)), tuple(ddecimal(1))), tupleFloatIntMismatchErr},
		{nil, nil, exprs(tuple(dint(1)), dint(1), dint(1)), tupleIntMismatchErr},
		{nil, nil, exprs(tuple(dint(1)), tuple(dint(1), dint(1))), tupleLenErr},
		// Placeholder ambiguity.
		{nil, nil, exprs(placeholder(2), placeholder(1)), placeholderErr},
	}
	for i, d := range testData {
		ctx := tree.MakeSemaContext(false)
		ctx.Placeholders.SetTypeHints(d.ptypes)
		desired := types.Any
		if d.desired != nil {
			desired = d.desired
		}
		forEachPerm(d.exprs, 0, func(exprs []copyableExpr) {
			if _, _, err := tree.TypeCheckSameTypedExprs(&ctx, desired, buildExprs(exprs)...); !testutils.IsError(err, d.expectedErr) {
				t.Errorf("%d: expected %s, but found %v", i, d.expectedErr, err)
			}
		})
	}
}

func cast(p *tree.Placeholder, typ coltypes.T) tree.Expr {
	return &tree.CastExpr{Expr: p, Type: typ}
}
func annot(p *tree.Placeholder, typ coltypes.T) tree.Expr {
	return &tree.AnnotateTypeExpr{Expr: p, Type: typ}
}

func TestProcessPlaceholderAnnotations(t *testing.T) {
	intType := coltypes.Int8
	boolType := coltypes.Bool

	testData := []struct {
		initArgs  tree.PlaceholderTypes
		stmtExprs []tree.Expr
		desired   tree.PlaceholderTypes
	}{
		{
			tree.PlaceholderTypes{},
			[]tree.Expr{newPlaceholder(1)},
			tree.PlaceholderTypes{},
		},
		{
			tree.PlaceholderTypes{},
			[]tree.Expr{newPlaceholder(1), newPlaceholder(2)},
			tree.PlaceholderTypes{},
		},
		{
			tree.PlaceholderTypes{2: types.Bool},
			[]tree.Expr{newPlaceholder(1), newPlaceholder(2)},
			tree.PlaceholderTypes{2: types.Bool},
		},
		{
			tree.PlaceholderTypes{3: types.Float},
			[]tree.Expr{newPlaceholder(1), newPlaceholder(2)},
			tree.PlaceholderTypes{3: types.Float},
		},
		{
			tree.PlaceholderTypes{},
			[]tree.Expr{
				cast(newPlaceholder(1), intType),
				cast(newPlaceholder(1), boolType),
			},
			tree.PlaceholderTypes{},
		},
		{
			tree.PlaceholderTypes{1: types.Float},
			[]tree.Expr{
				cast(newPlaceholder(1), intType),
				cast(newPlaceholder(1), boolType),
			},
			tree.PlaceholderTypes{1: types.Float},
		},
		{
			tree.PlaceholderTypes{},
			[]tree.Expr{
				cast(newPlaceholder(1), intType),
				cast(newPlaceholder(2), boolType),
			},
			tree.PlaceholderTypes{1: types.Int, 2: types.Bool},
		},
		{
			tree.PlaceholderTypes{},
			[]tree.Expr{
				annot(newPlaceholder(1), intType),
				annot(newPlaceholder(2), boolType),
			},
			tree.PlaceholderTypes{1: types.Int, 2: types.Bool},
		},
		{
			tree.PlaceholderTypes{2: types.Bool},
			[]tree.Expr{
				annot(newPlaceholder(1), intType),
			},
			tree.PlaceholderTypes{1: types.Int, 2: types.Bool},
		},
		{
			tree.PlaceholderTypes{},
			[]tree.Expr{
				cast(newPlaceholder(1), intType),
				cast(newPlaceholder(2), boolType),
				cast(newPlaceholder(1), intType),
				cast(newPlaceholder(2), intType),
			},
			tree.PlaceholderTypes{1: types.Int},
		},
		{
			tree.PlaceholderTypes{},
			[]tree.Expr{
				cast(newPlaceholder(1), intType),
				annot(newPlaceholder(2), boolType),
				cast(newPlaceholder(1), intType),
				cast(newPlaceholder(2), intType),
			},
			tree.PlaceholderTypes{1: types.Int, 2: types.Bool},
		},
		{
			tree.PlaceholderTypes{},
			[]tree.Expr{
				cast(newPlaceholder(1), intType),
				cast(newPlaceholder(2), boolType),
				newPlaceholder(1),
			},
			tree.PlaceholderTypes{2: types.Bool},
		},
		{
			tree.PlaceholderTypes{},
			[]tree.Expr{
				newPlaceholder(1),
				cast(newPlaceholder(1), intType),
				cast(newPlaceholder(2), boolType),
			},
			tree.PlaceholderTypes{2: types.Bool},
		},
		{
			tree.PlaceholderTypes{},
			[]tree.Expr{
				annot(newPlaceholder(1), intType),
				annot(newPlaceholder(2), boolType),
				newPlaceholder(1),
			},
			tree.PlaceholderTypes{1: types.Int, 2: types.Bool},
		},
		{
			tree.PlaceholderTypes{},
			[]tree.Expr{
				newPlaceholder(1),
				annot(newPlaceholder(1), intType),
				annot(newPlaceholder(2), boolType),
			},
			tree.PlaceholderTypes{1: types.Int, 2: types.Bool},
		},
		{
			tree.PlaceholderTypes{1: types.Float, 2: types.Bool},
			[]tree.Expr{
				newPlaceholder(1),
				cast(newPlaceholder(1), intType),
				cast(newPlaceholder(2), boolType),
			},
			tree.PlaceholderTypes{1: types.Float, 2: types.Bool},
		},
		{
			tree.PlaceholderTypes{1: types.Float, 2: types.Float},
			[]tree.Expr{
				newPlaceholder(1),
				cast(newPlaceholder(1), intType),
				cast(newPlaceholder(2), boolType),
			},
			tree.PlaceholderTypes{1: types.Float, 2: types.Float},
		},
		{
			tree.PlaceholderTypes{},
			[]tree.Expr{
				cast(newPlaceholder(1), intType),
				annot(newPlaceholder(1), intType),
				cast(newPlaceholder(1), intType),
			},
			tree.PlaceholderTypes{1: types.Int},
		},
		{
			tree.PlaceholderTypes{},
			[]tree.Expr{
				cast(newPlaceholder(1), intType),
				annot(newPlaceholder(1), boolType),
				cast(newPlaceholder(1), intType),
			},
			tree.PlaceholderTypes{1: types.Bool},
		},
	}
	for i, d := range testData {
		args := d.initArgs
		stmt := &tree.ValuesClause{Rows: []tree.Exprs{d.stmtExprs}}
		if err := args.ProcessPlaceholderAnnotations(stmt); err != nil {
			t.Errorf("%d: unexpected error returned from ProcessPlaceholderAnnotations: %v", i, err)
		} else if !reflect.DeepEqual(args, d.desired) {
			t.Errorf("%d: expected args %v after processing placeholder annotations for %v, found %v", i, d.desired, stmt, args)
		}
	}
}

func TestProcessPlaceholderAnnotationsError(t *testing.T) {
	intType := coltypes.Int8
	floatType := coltypes.Float8

	testData := []struct {
		initArgs  tree.PlaceholderTypes
		stmtExprs []tree.Expr
		expected  string
	}{
		{
			tree.PlaceholderTypes{},
			[]tree.Expr{
				annot(newPlaceholder(1), floatType),
				annot(newPlaceholder(1), intType),
			},
			"multiple conflicting type annotations around 1",
		},
		{
			tree.PlaceholderTypes{},
			[]tree.Expr{
				annot(newPlaceholder(1), floatType),
				cast(newPlaceholder(1), floatType),
				cast(newPlaceholder(2), floatType),
				annot(newPlaceholder(1), intType),
			},
			"multiple conflicting type annotations around 1",
		},
		{
			tree.PlaceholderTypes{1: types.Float},
			[]tree.Expr{
				annot(newPlaceholder(1), intType),
			},
			"type annotation around 1 that conflicts with previously inferred type float",
		},
		{
			tree.PlaceholderTypes{1: types.Float},
			[]tree.Expr{
				cast(newPlaceholder(1), intType),
				annot(newPlaceholder(1), intType),
			},
			"type annotation around 1 that conflicts with previously inferred type float",
		},
		{
			tree.PlaceholderTypes{1: types.Float},
			[]tree.Expr{
				annot(newPlaceholder(1), floatType),
				annot(newPlaceholder(1), intType),
			},
			"type annotation around 1 that conflicts with previously inferred type float",
		},
		{
			tree.PlaceholderTypes{1: types.Float},
			[]tree.Expr{
				annot(newPlaceholder(1), intType),
				annot(newPlaceholder(1), floatType),
			},
			"type annotation around 1 that conflicts with previously inferred type float",
		},
	}
	for i, d := range testData {
		args := d.initArgs
		stmt := &tree.ValuesClause{Rows: []tree.Exprs{d.stmtExprs}}
		if err := args.ProcessPlaceholderAnnotations(stmt); !testutils.IsError(err, d.expected) {
			t.Errorf("%d: expected '%s', got '%v'", i, d.expected, err)
		}
	}
}

func newPlaceholder(id types.PlaceholderIdx) *tree.Placeholder {
	return &tree.Placeholder{Idx: id}
}
