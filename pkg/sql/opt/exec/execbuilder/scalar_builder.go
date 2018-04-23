// Copyright 2018 The Cockroach Authors.
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

package execbuilder

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

type buildScalarCtx struct {
	ivh tree.IndexedVarHelper

	// ivarMap is a map from opt.ColumnID to the index of an IndexedVar.
	// If a ColumnID is not in the map, it cannot appear in the expression.
	ivarMap opt.ColMap
}

type buildFunc func(b *Builder, ctx *buildScalarCtx, ev memo.ExprView) (tree.TypedExpr, error)

var scalarBuildFuncMap [opt.NumOperators]buildFunc

func init() {
	// This code is not inline to avoid an initialization loop error (some of
	// the functions depend on scalarBuildFuncMap which in turn depends on the
	// functions).
	scalarBuildFuncMap = [opt.NumOperators]buildFunc{
		opt.VariableOp:        (*Builder).buildVariable,
		opt.ConstOp:           (*Builder).buildTypedExpr,
		opt.NullOp:            (*Builder).buildNull,
		opt.PlaceholderOp:     (*Builder).buildTypedExpr,
		opt.TupleOp:           (*Builder).buildTuple,
		opt.FunctionOp:        (*Builder).buildFunction,
		opt.CaseOp:            (*Builder).buildCase,
		opt.CastOp:            (*Builder).buildCast,
		opt.CoalesceOp:        (*Builder).buildCoalesce,
		opt.ArrayOp:           (*Builder).buildArray,
		opt.UnsupportedExprOp: (*Builder).buildUnsupportedExpr,
	}

	for _, op := range opt.BooleanOperators {
		scalarBuildFuncMap[op] = (*Builder).buildBoolean
	}

	for _, op := range opt.ComparisonOperators {
		scalarBuildFuncMap[op] = (*Builder).buildComparison
	}

	for _, op := range opt.BinaryOperators {
		scalarBuildFuncMap[op] = (*Builder).buildBinary
	}

	for _, op := range opt.UnaryOperators {
		scalarBuildFuncMap[op] = (*Builder).buildUnary
	}
}

// buildScalar converts a scalar expression to a TypedExpr. Variables are mapped
// according to ctx.
func (b *Builder) buildScalar(ctx *buildScalarCtx, ev memo.ExprView) (tree.TypedExpr, error) {
	if fn := scalarBuildFuncMap[ev.Operator()]; fn != nil {
		return fn(b, ctx, ev)
	}
	panic(fmt.Sprintf("unsupported op %s", ev.Operator()))
}

func (b *Builder) buildTypedExpr(ctx *buildScalarCtx, ev memo.ExprView) (tree.TypedExpr, error) {
	return ev.Private().(tree.TypedExpr), nil
}

func (b *Builder) buildNull(ctx *buildScalarCtx, ev memo.ExprView) (tree.TypedExpr, error) {
	return tree.DNull, nil
}

func (b *Builder) buildVariable(ctx *buildScalarCtx, ev memo.ExprView) (tree.TypedExpr, error) {
	colID := ev.Private().(opt.ColumnID)
	idx, ok := ctx.ivarMap.Get(int(colID))
	if !ok {
		panic(fmt.Sprintf("cannot map variable %d to an indexed var", colID))
	}
	return ctx.ivh.IndexedVarWithType(idx, ev.Metadata().ColumnType(colID)), nil
}

func (b *Builder) buildTuple(ctx *buildScalarCtx, ev memo.ExprView) (tree.TypedExpr, error) {
	if memo.MatchesTupleOfConstants(ev) {
		datums := make(tree.Datums, ev.ChildCount())
		for i := range datums {
			datums[i] = memo.ExtractConstDatum(ev.Child(i))
		}
		return tree.NewDTuple(datums...), nil
	}

	typedExprs := make([]tree.TypedExpr, ev.ChildCount())
	var err error
	for i := 0; i < ev.ChildCount(); i++ {
		typedExprs[i], err = b.buildScalar(ctx, ev.Child(i))
		if err != nil {
			return nil, err
		}
	}
	return tree.NewTypedTuple(typedExprs), nil
}

func (b *Builder) buildBoolean(ctx *buildScalarCtx, ev memo.ExprView) (tree.TypedExpr, error) {
	switch ev.Operator() {
	case opt.AndOp, opt.OrOp, opt.FiltersOp:
		expr, err := b.buildScalar(ctx, ev.Child(0))
		if err != nil {
			return nil, err
		}
		for i, n := 1, ev.ChildCount(); i < n; i++ {
			right, err := b.buildScalar(ctx, ev.Child(i))
			if err != nil {
				return nil, err
			}
			if ev.Operator() == opt.OrOp {
				expr = tree.NewTypedOrExpr(expr, right)
			} else {
				expr = tree.NewTypedAndExpr(expr, right)
			}
		}
		return expr, nil

	case opt.NotOp:
		expr, err := b.buildScalar(ctx, ev.Child(0))
		if err != nil {
			return nil, err
		}
		return tree.NewTypedNotExpr(expr), nil

	case opt.TrueOp:
		return tree.DBoolTrue, nil

	case opt.FalseOp:
		return tree.DBoolFalse, nil

	default:
		panic(fmt.Sprintf("invalid op %s", ev.Operator()))
	}
}

func (b *Builder) buildComparison(ctx *buildScalarCtx, ev memo.ExprView) (tree.TypedExpr, error) {
	left, err := b.buildScalar(ctx, ev.Child(0))
	if err != nil {
		return nil, err
	}
	right, err := b.buildScalar(ctx, ev.Child(1))
	if err != nil {
		return nil, err
	}
	operator := opt.ComparisonOpReverseMap[ev.Operator()]
	return tree.NewTypedComparisonExpr(operator, left, right), nil
}

func (b *Builder) buildUnary(ctx *buildScalarCtx, ev memo.ExprView) (tree.TypedExpr, error) {
	input, err := b.buildScalar(ctx, ev.Child(0))
	if err != nil {
		return nil, err
	}
	operator := opt.UnaryOpReverseMap[ev.Operator()]
	return tree.NewTypedUnaryExpr(operator, input, ev.Logical().Scalar.Type), nil
}

func (b *Builder) buildBinary(ctx *buildScalarCtx, ev memo.ExprView) (tree.TypedExpr, error) {
	left, err := b.buildScalar(ctx, ev.Child(0))
	if err != nil {
		return nil, err
	}
	right, err := b.buildScalar(ctx, ev.Child(1))
	if err != nil {
		return nil, err
	}
	operator := opt.BinaryOpReverseMap[ev.Operator()]
	return tree.NewTypedBinaryExpr(operator, left, right, ev.Logical().Scalar.Type), nil
}

func (b *Builder) buildFunction(ctx *buildScalarCtx, ev memo.ExprView) (tree.TypedExpr, error) {
	exprs := make(tree.TypedExprs, ev.ChildCount())
	var err error
	for i := range exprs {
		exprs[i], err = b.buildScalar(ctx, ev.Child(i))
		if err != nil {
			return nil, err
		}
	}
	funcDef := ev.Private().(*memo.FuncOpDef)
	funcRef := tree.WrapFunction(funcDef.Name)
	return tree.NewTypedFuncExpr(
		funcRef,
		0, /* aggQualifier */
		exprs,
		nil, /* filter */
		nil, /* windowDef */
		ev.Logical().Scalar.Type,
		funcDef.Overload,
	), nil
}

func (b *Builder) buildCase(ctx *buildScalarCtx, ev memo.ExprView) (tree.TypedExpr, error) {
	input, err := b.buildScalar(ctx, ev.Child(0))
	if err != nil {
		return nil, err
	}

	// A searched CASE statement is represented by the optimizer with input=True.
	// The executor expects searched CASE statements to have nil inputs.
	if input == tree.DBoolTrue {
		input = nil
	}

	// Extract the list of WHEN ... THEN ... clauses.
	whens := make([]*tree.When, ev.ChildCount()-2)
	for i := 1; i < ev.ChildCount()-1; i++ {
		whenEv := ev.Child(i)
		cond, err := b.buildScalar(ctx, whenEv.Child(0))
		if err != nil {
			return nil, err
		}
		val, err := b.buildScalar(ctx, whenEv.Child(1))
		if err != nil {
			return nil, err
		}
		whens[i-1] = &tree.When{Cond: cond, Val: val}
	}

	// The last child in ev is the ELSE expression.
	elseExpr, err := b.buildScalar(ctx, ev.Child(ev.ChildCount()-1))
	if err != nil {
		return nil, err
	}
	if elseExpr == tree.DNull {
		elseExpr = nil
	}

	return tree.NewTypedCaseExpr(input, whens, elseExpr, ev.Logical().Scalar.Type)
}

func (b *Builder) buildCast(ctx *buildScalarCtx, ev memo.ExprView) (tree.TypedExpr, error) {
	input, err := b.buildScalar(ctx, ev.Child(0))
	if err != nil {
		return nil, err
	}
	return tree.NewTypedCastExpr(input, ev.Logical().Scalar.Type)
}

func (b *Builder) buildCoalesce(ctx *buildScalarCtx, ev memo.ExprView) (tree.TypedExpr, error) {
	exprs := make(tree.TypedExprs, ev.ChildCount())
	var err error
	for i := range exprs {
		exprs[i], err = b.buildScalar(ctx, ev.Child(i))
		if err != nil {
			return nil, err
		}
	}
	return tree.NewTypedCoalesceExpr(exprs, ev.Logical().Scalar.Type), nil
}

func (b *Builder) buildArray(ctx *buildScalarCtx, ev memo.ExprView) (tree.TypedExpr, error) {
	exprs := make(tree.TypedExprs, ev.ChildCount())
	var err error
	for i := range exprs {
		exprs[i], err = b.buildScalar(ctx, ev.Child(i))
		if err != nil {
			return nil, err
		}
	}
	return tree.NewTypedArray(exprs, ev.Logical().Scalar.Type), nil
}

func (b *Builder) buildUnsupportedExpr(
	ctx *buildScalarCtx, ev memo.ExprView,
) (tree.TypedExpr, error) {
	return ev.Private().(tree.TypedExpr), nil
}
