// Copyright 2015 The Cockroach Authors.
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
	"fmt"
	"go/constant"
	"go/token"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func BenchmarkTypeCheck(b *testing.B) {
	// random example from TPCC
	sql := `CASE 1 >= $1 + 10 WHEN true THEN 1-$1 ELSE (1-$1)+91 END`
	expr, err := parser.ParseExpr(sql)
	if err != nil {
		b.Fatalf("%s: %v", expr, err)
	}
	ctx := tree.MakeSemaContext()
	if err := ctx.Placeholders.Init(1 /* numPlaceholders */, nil /* typeHints */); err != nil {
		b.Fatal(err)
	}
	for i := 0; i < b.N; i++ {
		_, err := tree.TypeCheck(context.Background(), expr, &ctx, types.Int)
		if err != nil {
			b.Fatalf("unexpected error: %s", err)
		}
	}
}

func TestTypeCheckNormalize(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	testData := []struct {
		expr     string
		expected string
	}{
		{`'NaN'::decimal`, `'NaN':::DECIMAL`},
		{`'-NaN'::decimal`, `'NaN':::DECIMAL`},
		{`'Inf'::decimal`, `'Infinity':::DECIMAL`},
		{`'-Inf'::decimal`, `'-Infinity':::DECIMAL`},
	}
	ctx := context.Background()
	for _, d := range testData {
		t.Run(d.expr, func(t *testing.T) {
			expr, err := parser.ParseExpr(d.expr)
			if err != nil {
				t.Fatal(err)
			}
			semaCtx := tree.MakeSemaContext()
			typeChecked, err := tree.TypeCheck(ctx, expr, &semaCtx, types.Any)
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
	ptypesNone              = tree.PlaceholderTypes{nil, nil}
	ptypesInt               = tree.PlaceholderTypes{types.Int, nil}
	ptypesDecimal           = tree.PlaceholderTypes{types.Decimal, nil}
	ptypesIntAndInt         = tree.PlaceholderTypes{types.Int, types.Int}
	ptypesIntAndDecimal     = tree.PlaceholderTypes{types.Int, types.Decimal}
	ptypesDecimalAndDecimal = tree.PlaceholderTypes{types.Decimal, types.Decimal}
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
		return tree.NewNumVal(constant.MakeFromLiteral(s, token.INT, 0), s, false /* negative */)
	}
}
func decConst(s string) copyableExpr {
	return func() tree.Expr {
		return tree.NewNumVal(constant.MakeFromLiteral(s, token.FLOAT, 0), s, false /* negative */)
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
func placeholder(id tree.PlaceholderIdx) copyableExpr {
	return func() tree.Expr {
		return newPlaceholder(id)
	}
}
func tuple(exprs ...copyableExpr) copyableExpr {
	return func() tree.Expr {
		return &tree.Tuple{Exprs: buildExprs(exprs)}
	}
}
func ttuple(tys ...*types.T) *types.T {
	return types.MakeTuple(tys)
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
	clone := make(tree.PlaceholderTypes, len(args))
	copy(clone, args)
	return clone
}

type sameTypedExprsTestCase struct {
	ptypes  tree.PlaceholderTypes
	desired *types.T
	exprs   []copyableExpr

	expectedType   *types.T
	expectedPTypes tree.PlaceholderTypes
}

func attemptTypeCheckSameTypedExprs(t *testing.T, idx int, test sameTypedExprsTestCase) {
	if test.expectedPTypes == nil {
		test.expectedPTypes = tree.PlaceholderTypes{}
	}
	ctx := context.Background()
	forEachPerm(test.exprs, 0, func(exprs []copyableExpr) {
		semaCtx := tree.MakeSemaContext()
		if err := semaCtx.Placeholders.Init(len(test.ptypes), clonePlaceholderTypes(test.ptypes)); err != nil {
			t.Fatal(err)
		}
		desired := types.Any
		if test.desired != nil {
			desired = test.desired
		}
		_, typ, err := tree.TypeCheckSameTypedExprs(ctx, &semaCtx, desired, buildExprs(exprs)...)
		if err != nil {
			t.Errorf("%d: unexpected error returned from typeCheckSameTypedExprs: %v", idx, err)
		} else {
			if !typ.Equivalent(test.expectedType) {
				t.Errorf("%d: expected type %s when type checking %s, found %s",
					idx, test.expectedType, buildExprs(exprs), typ)
			}
			if !reflect.DeepEqual(semaCtx.Placeholders.Types, test.expectedPTypes) {
				t.Errorf("%d: expected placeholder types %v after TypeCheckSameTypedExprs for %v, found %v", idx, test.expectedPTypes, buildExprs(exprs), semaCtx.Placeholders.Types)
			}
		}
	})
}

func TestTypeCheckSameTypedExprs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
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
		{ptypesDecimal, nil, exprs(ddecimal(1), placeholder(0)), types.Decimal, ptypesDecimal},
		{ptypesDecimal, nil, exprs(intConst("1"), placeholder(0)), types.Decimal, ptypesDecimal},
		{ptypesDecimal, nil, exprs(decConst("1.1"), placeholder(0)), types.Decimal, ptypesDecimal},
		{ptypesInt, nil, exprs(intConst("1"), placeholder(0)), types.Int, ptypesInt},
		{ptypesInt, nil, exprs(decConst("1.0"), placeholder(0)), types.Int, ptypesInt},
		{ptypesDecimalAndDecimal, nil, exprs(placeholder(1), placeholder(0)), types.Decimal, ptypesDecimalAndDecimal},
		// Mixing unresolved placeholders with constants and resolved exprs.
		{ptypesNone, nil, exprs(ddecimal(1), placeholder(0)), types.Decimal, ptypesDecimal},
		{ptypesNone, nil, exprs(intConst("1"), placeholder(0)), types.Int, ptypesInt},
		{ptypesNone, nil, exprs(decConst("1.1"), placeholder(0)), types.Decimal, ptypesDecimal},
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
		{ptypesNone, types.Decimal, exprs(placeholder(0)), types.Decimal, ptypesDecimal},
		{ptypesNone, types.Decimal, exprs(intConst("1"), placeholder(0)), types.Decimal, ptypesDecimal},
		{ptypesNone, types.Decimal, exprs(decConst("1.1"), placeholder(0)), types.Decimal, ptypesDecimal},
	} {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			attemptTypeCheckSameTypedExprs(t, i, d)
		})
	}
}

func TestTypeCheckSameTypedTupleExprs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
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
		{ptypesDecimal, nil, exprs(tuple(ddecimal(1), intConst("1")), tuple(placeholder(0), placeholder(0))), ttuple(types.Decimal, types.Decimal), ptypesDecimal},
		{ptypesDecimalAndDecimal, nil, exprs(tuple(placeholder(1), intConst("1")), tuple(placeholder(0), placeholder(0))), ttuple(types.Decimal, types.Decimal), ptypesDecimalAndDecimal},
		{ptypesIntAndDecimal, nil, exprs(tuple(intConst("1"), intConst("1")), tuple(placeholder(0), placeholder(1))), ttuple(types.Int, types.Decimal), ptypesIntAndDecimal},
		// Mixing unresolved placeholders with constants and resolved exprs.
		{ptypesNone, nil, exprs(tuple(ddecimal(1), intConst("1")), tuple(placeholder(0), placeholder(0))), ttuple(types.Decimal, types.Decimal), ptypesDecimal},
		{ptypesNone, nil, exprs(tuple(intConst("1"), intConst("1")), tuple(placeholder(0), placeholder(1))), ttuple(types.Int, types.Int), ptypesIntAndInt},
		// Verify dealing with Null.
		{nil, nil, exprs(tuple(intConst("1"), dnull), tuple(dnull, decConst("1"))), ttuple(types.Int, types.Decimal), nil},
		{nil, nil, exprs(tuple(dint(1), dnull), tuple(dnull, ddecimal(1))), ttuple(types.Int, types.Decimal), nil},
		{nil, nil, exprs(tuple(dint(1), dnull), dnull, tuple(dint(1), dnull), dnull), ttuple(types.Int, types.Unknown), nil},
		// Verify desired type when possible.
		{nil, ttuple(types.Int, types.Decimal), exprs(tuple(intConst("1"), intConst("1")), tuple(intConst("1"), intConst("1"))), ttuple(types.Int, types.Decimal), nil},
		// Verify desired type when possible with unresolved constants.
		{ptypesNone, ttuple(types.Int, types.Decimal), exprs(tuple(placeholder(0), intConst("1")), tuple(intConst("1"), placeholder(1))), ttuple(types.Int, types.Decimal), ptypesIntAndDecimal},
	} {
		attemptTypeCheckSameTypedExprs(t, i, d)
	}
}

func TestTypeCheckSameTypedExprsError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	decimalIntMismatchErr := `expected .* to be of type (decimal|int), found type (decimal|int)`
	tupleFloatIntMismatchErr := `tuples .* are not the same type: ` + decimalIntMismatchErr
	tupleIntMismatchErr := `expected .* to be of type (tuple|int), found type (tuple|int)`
	tupleLenErr := `expected tuple .* to have a length of .*`
	placeholderErr := `could not determine data type of placeholder .*`

	testData := []struct {
		ptypes  tree.PlaceholderTypes
		desired *types.T
		exprs   []copyableExpr

		expectedErr string
	}{
		// Single type mismatches.
		{nil, nil, exprs(dint(1), decConst("1.1")), decimalIntMismatchErr},
		{nil, nil, exprs(dint(1), ddecimal(1)), decimalIntMismatchErr},
		{ptypesInt, nil, exprs(decConst("1.1"), placeholder(0)), decimalIntMismatchErr},
		// Tuple type mismatches.
		{nil, nil, exprs(tuple(dint(1)), tuple(ddecimal(1))), tupleFloatIntMismatchErr},
		{nil, nil, exprs(tuple(dint(1)), dint(1), dint(1)), tupleIntMismatchErr},
		{nil, nil, exprs(tuple(dint(1)), tuple(dint(1), dint(1))), tupleLenErr},
		// Placeholder ambiguity.
		{ptypesNone, nil, exprs(placeholder(1), placeholder(0)), placeholderErr},
	}
	ctx := context.Background()
	for i, d := range testData {
		semaCtx := tree.MakeSemaContext()
		if err := semaCtx.Placeholders.Init(len(d.ptypes), d.ptypes); err != nil {
			t.Error(err)
			continue
		}
		desired := types.Any
		if d.desired != nil {
			desired = d.desired
		}
		forEachPerm(d.exprs, 0, func(exprs []copyableExpr) {
			if _, _, err := tree.TypeCheckSameTypedExprs(ctx, &semaCtx, desired, buildExprs(exprs)...); !testutils.IsError(err, d.expectedErr) {
				t.Errorf("%d: expected %s, but found %v", i, d.expectedErr, err)
			}
		})
	}
}

func cast(p *tree.Placeholder, typ *types.T) tree.Expr {
	return &tree.CastExpr{Expr: p, Type: typ}
}
func annot(p *tree.Placeholder, typ *types.T) tree.Expr {
	return &tree.AnnotateTypeExpr{Expr: p, Type: typ}
}

func TestProcessPlaceholderAnnotations(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	intType := types.Int
	boolType := types.Bool
	semaCtx := tree.MakeSemaContext()

	testData := []struct {
		initArgs  tree.PlaceholderTypes
		stmtExprs []tree.Expr
		desired   tree.PlaceholderTypes
	}{
		{ // 0
			tree.PlaceholderTypes{nil},
			[]tree.Expr{newPlaceholder(0)},
			tree.PlaceholderTypes{nil},
		},
		{ // 1
			tree.PlaceholderTypes{nil, nil},
			[]tree.Expr{newPlaceholder(0), newPlaceholder(1)},
			tree.PlaceholderTypes{nil, nil},
		},
		{ // 2
			tree.PlaceholderTypes{nil, nil, types.Bool},
			[]tree.Expr{newPlaceholder(0), newPlaceholder(1)},
			tree.PlaceholderTypes{nil, nil, types.Bool},
		},
		{ // 3
			tree.PlaceholderTypes{nil, nil, types.Float},
			[]tree.Expr{newPlaceholder(0), newPlaceholder(1)},
			tree.PlaceholderTypes{nil, nil, types.Float},
		},
		{ // 4
			tree.PlaceholderTypes{nil, nil},
			[]tree.Expr{
				cast(newPlaceholder(0), intType),
				cast(newPlaceholder(0), boolType),
			},
			tree.PlaceholderTypes{nil, nil},
		},
		{ // 5
			tree.PlaceholderTypes{types.Float},
			[]tree.Expr{
				cast(newPlaceholder(0), intType),
				cast(newPlaceholder(0), boolType),
			},
			tree.PlaceholderTypes{types.Float},
		},
		{ // 6
			tree.PlaceholderTypes{nil, nil},
			[]tree.Expr{
				cast(newPlaceholder(0), intType),
				cast(newPlaceholder(1), boolType),
			},
			tree.PlaceholderTypes{types.Int, types.Bool},
		},
		{ // 7
			tree.PlaceholderTypes{nil, nil},
			[]tree.Expr{
				annot(newPlaceholder(0), intType),
				annot(newPlaceholder(1), boolType),
			},
			tree.PlaceholderTypes{types.Int, types.Bool},
		},
		{ // 8
			tree.PlaceholderTypes{nil, types.Bool},
			[]tree.Expr{
				annot(newPlaceholder(0), intType),
			},
			tree.PlaceholderTypes{types.Int, types.Bool},
		},
		{ // 9
			tree.PlaceholderTypes{nil, nil},
			[]tree.Expr{
				cast(newPlaceholder(0), intType),
				cast(newPlaceholder(1), boolType),
				cast(newPlaceholder(0), intType),
				cast(newPlaceholder(1), intType),
			},
			tree.PlaceholderTypes{types.Int, nil},
		},
		{ // 10
			tree.PlaceholderTypes{nil, nil},
			[]tree.Expr{
				cast(newPlaceholder(0), intType),
				annot(newPlaceholder(1), boolType),
				cast(newPlaceholder(0), intType),
				cast(newPlaceholder(1), intType),
			},
			tree.PlaceholderTypes{types.Int, types.Bool},
		},
		{ // 11
			tree.PlaceholderTypes{nil, nil},
			[]tree.Expr{
				cast(newPlaceholder(0), intType),
				cast(newPlaceholder(1), boolType),
				newPlaceholder(0),
			},
			tree.PlaceholderTypes{nil, types.Bool},
		},
		{ // 12
			tree.PlaceholderTypes{nil, nil},
			[]tree.Expr{
				newPlaceholder(0),
				cast(newPlaceholder(0), intType),
				cast(newPlaceholder(1), boolType),
			},
			tree.PlaceholderTypes{nil, types.Bool},
		},
		{ // 13
			tree.PlaceholderTypes{nil, nil},
			[]tree.Expr{
				annot(newPlaceholder(0), intType),
				annot(newPlaceholder(1), boolType),
				newPlaceholder(0),
			},
			tree.PlaceholderTypes{types.Int, types.Bool},
		},
		{ // 14
			tree.PlaceholderTypes{nil, nil},
			[]tree.Expr{
				newPlaceholder(0),
				annot(newPlaceholder(0), intType),
				annot(newPlaceholder(1), boolType),
			},
			tree.PlaceholderTypes{types.Int, types.Bool},
		},
		{ // 15
			tree.PlaceholderTypes{types.Float, types.Bool},
			[]tree.Expr{
				newPlaceholder(0),
				cast(newPlaceholder(0), intType),
				cast(newPlaceholder(1), boolType),
			},
			tree.PlaceholderTypes{types.Float, types.Bool},
		},
		{ // 16
			tree.PlaceholderTypes{types.Float, types.Float},
			[]tree.Expr{
				newPlaceholder(0),
				cast(newPlaceholder(0), intType),
				cast(newPlaceholder(1), boolType),
			},
			tree.PlaceholderTypes{types.Float, types.Float},
		},
		{ // 17
			tree.PlaceholderTypes{nil},
			[]tree.Expr{
				cast(newPlaceholder(0), intType),
				annot(newPlaceholder(0), intType),
				cast(newPlaceholder(0), intType),
			},
			tree.PlaceholderTypes{types.Int},
		},
		{ // 18
			tree.PlaceholderTypes{nil},
			[]tree.Expr{
				cast(newPlaceholder(0), intType),
				annot(newPlaceholder(0), boolType),
				cast(newPlaceholder(0), intType),
			},
			tree.PlaceholderTypes{types.Bool},
		},
	}
	for i, d := range testData {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			args := d.initArgs
			stmt := &tree.ValuesClause{Rows: []tree.Exprs{d.stmtExprs}}
			if err := tree.ProcessPlaceholderAnnotations(&semaCtx, stmt, args); err != nil {
				t.Errorf("%d: unexpected error returned from ProcessPlaceholderAnnotations: %v", i, err)
			} else if !reflect.DeepEqual(args, d.desired) {
				t.Errorf(
					"%d: expected args %v after processing placeholder annotations for %v, found %v",
					i, d.desired, stmt, args,
				)
			}
		})
	}
}

func TestProcessPlaceholderAnnotationsError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	intType := types.Int
	floatType := types.Float
	semaCtx := tree.MakeSemaContext()

	testData := []struct {
		initArgs  tree.PlaceholderTypes
		stmtExprs []tree.Expr
		expected  string
	}{
		{
			tree.PlaceholderTypes{nil},
			[]tree.Expr{
				annot(newPlaceholder(0), floatType),
				annot(newPlaceholder(0), intType),
			},
			"multiple conflicting type annotations around \\$1",
		},
		{
			tree.PlaceholderTypes{nil, nil},
			[]tree.Expr{
				annot(newPlaceholder(0), floatType),
				cast(newPlaceholder(0), floatType),
				cast(newPlaceholder(1), floatType),
				annot(newPlaceholder(0), intType),
			},
			"multiple conflicting type annotations around \\$1",
		},
		{
			tree.PlaceholderTypes{nil, nil},
			[]tree.Expr{
				annot(newPlaceholder(0), floatType),
				annot(newPlaceholder(1), floatType),
				annot(newPlaceholder(1), intType),
				annot(newPlaceholder(0), intType),
			},
			"multiple conflicting type annotations around \\$1",
		},
		{
			tree.PlaceholderTypes{types.Float},
			[]tree.Expr{
				annot(newPlaceholder(0), intType),
			},
			"type annotation around \\$1 conflicts with specified type float",
		},
		{
			tree.PlaceholderTypes{types.Float},
			[]tree.Expr{
				cast(newPlaceholder(0), intType),
				annot(newPlaceholder(0), intType),
			},
			"type annotation around \\$1 conflicts with specified type float",
		},
		{
			tree.PlaceholderTypes{types.Float},
			[]tree.Expr{
				annot(newPlaceholder(0), floatType),
				annot(newPlaceholder(0), intType),
			},
			"type annotation around \\$1 conflicts with specified type float",
		},
		{
			tree.PlaceholderTypes{types.Float},
			[]tree.Expr{
				annot(newPlaceholder(0), intType),
				annot(newPlaceholder(0), floatType),
			},
			"type annotation around \\$1 conflicts with specified type float",
		},
	}
	for i, d := range testData {
		args := d.initArgs
		stmt := &tree.ValuesClause{Rows: []tree.Exprs{d.stmtExprs}}
		if err := tree.ProcessPlaceholderAnnotations(&semaCtx, stmt, args); !testutils.IsError(err, d.expected) {
			t.Errorf("%d: expected '%s', got '%v'", i, d.expected, err)
		}
	}
}

func newPlaceholder(id tree.PlaceholderIdx) *tree.Placeholder {
	return &tree.Placeholder{Idx: id}
}
