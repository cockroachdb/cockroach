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

type buildFunc func(b *Builder, ctx *buildScalarCtx, ev memo.ExprView) tree.TypedExpr

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
func (b *Builder) buildScalar(ctx *buildScalarCtx, ev memo.ExprView) tree.TypedExpr {
	if fn := scalarBuildFuncMap[ev.Operator()]; fn != nil {
		return fn(b, ctx, ev)
	}
	panic(fmt.Sprintf("unsupported op %s", ev.Operator()))
}

func (b *Builder) buildTypedExpr(ctx *buildScalarCtx, ev memo.ExprView) tree.TypedExpr {
	return ev.Private().(tree.TypedExpr)
}

func (b *Builder) buildNull(ctx *buildScalarCtx, ev memo.ExprView) tree.TypedExpr {
	return tree.DNull
}

func (b *Builder) buildVariable(ctx *buildScalarCtx, ev memo.ExprView) tree.TypedExpr {
	colID := ev.Private().(opt.ColumnID)
	idx, ok := ctx.ivarMap.Get(int(colID))
	if !ok {
		panic(fmt.Sprintf("cannot map variable %d to an indexed var", colID))
	}
	return ctx.ivh.IndexedVarWithType(idx, ev.Metadata().ColumnType(colID))
}

func (b *Builder) buildTuple(ctx *buildScalarCtx, ev memo.ExprView) tree.TypedExpr {
	if memo.MatchesTupleOfConstants(ev) {
		datums := make(tree.Datums, ev.ChildCount())
		for i := range datums {
			datums[i] = memo.ExtractConstDatum(ev.Child(i))
		}
		return tree.NewDTuple(datums...)
	}

	typedExprs := make([]tree.TypedExpr, ev.ChildCount())
	for i := 0; i < ev.ChildCount(); i++ {
		typedExprs[i] = b.buildScalar(ctx, ev.Child(i))
	}
	return tree.NewTypedTuple(typedExprs)
}

func (b *Builder) buildBoolean(ctx *buildScalarCtx, ev memo.ExprView) tree.TypedExpr {
	switch ev.Operator() {
	case opt.AndOp, opt.OrOp, opt.FiltersOp:
		expr := b.buildScalar(ctx, ev.Child(0))
		for i, n := 1, ev.ChildCount(); i < n; i++ {
			right := b.buildScalar(ctx, ev.Child(i))
			if ev.Operator() == opt.OrOp {
				expr = tree.NewTypedOrExpr(expr, right)
			} else {
				expr = tree.NewTypedAndExpr(expr, right)
			}
		}
		return expr

	case opt.NotOp:
		return tree.NewTypedNotExpr(b.buildScalar(ctx, ev.Child(0)))

	case opt.TrueOp:
		return tree.DBoolTrue

	case opt.FalseOp:
		return tree.DBoolFalse

	default:
		panic(fmt.Sprintf("invalid op %s", ev.Operator()))
	}
}

func (b *Builder) buildComparison(ctx *buildScalarCtx, ev memo.ExprView) tree.TypedExpr {
	return tree.NewTypedComparisonExpr(
		opt.ComparisonOpReverseMap[ev.Operator()],
		b.buildScalar(ctx, ev.Child(0)),
		b.buildScalar(ctx, ev.Child(1)),
	)
}

func (b *Builder) buildUnary(ctx *buildScalarCtx, ev memo.ExprView) tree.TypedExpr {
	return tree.NewTypedUnaryExpr(
		opt.UnaryOpReverseMap[ev.Operator()],
		b.buildScalar(ctx, ev.Child(0)),
		ev.Logical().Scalar.Type,
	)
}

func (b *Builder) buildBinary(ctx *buildScalarCtx, ev memo.ExprView) tree.TypedExpr {
	return tree.NewTypedBinaryExpr(
		opt.BinaryOpReverseMap[ev.Operator()],
		b.buildScalar(ctx, ev.Child(0)),
		b.buildScalar(ctx, ev.Child(1)),
		ev.Logical().Scalar.Type,
	)
}

func (b *Builder) buildFunction(ctx *buildScalarCtx, ev memo.ExprView) tree.TypedExpr {
	exprs := make(tree.TypedExprs, ev.ChildCount())
	for i := range exprs {
		exprs[i] = b.buildScalar(ctx, ev.Child(i))
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
	)
}

func (b *Builder) buildCase(ctx *buildScalarCtx, ev memo.ExprView) tree.TypedExpr {
	input := b.buildScalar(ctx, ev.Child(0))

	// A searched CASE statement is represented by the optimizer with input=True.
	// The executor expects searched CASE statements to have nil inputs.
	if input == tree.DBoolTrue {
		input = nil
	}

	// Extract the list of WHEN ... THEN ... clauses.
	whens := make([]*tree.When, ev.ChildCount()-2)
	for i := 1; i < ev.ChildCount()-1; i++ {
		whenEv := ev.Child(i)
		cond := b.buildScalar(ctx, whenEv.Child(0))
		val := b.buildScalar(ctx, whenEv.Child(1))
		whens[i-1] = &tree.When{Cond: cond, Val: val}
	}

	// The last child in ev is the ELSE expression.
	elseExpr := b.buildScalar(ctx, ev.Child(ev.ChildCount()-1))
	if elseExpr == tree.DNull {
		elseExpr = nil
	}

	expr, err := tree.NewTypedCaseExpr(input, whens, elseExpr, ev.Logical().Scalar.Type)
	if err != nil {
		panic(err)
	}
	return expr
}

func (b *Builder) buildCast(ctx *buildScalarCtx, ev memo.ExprView) tree.TypedExpr {
	expr, err := tree.NewTypedCastExpr(
		b.buildScalar(ctx, ev.Child(0)),
		ev.Logical().Scalar.Type,
	)
	if err != nil {
		panic(err)
	}
	return expr
}

func (b *Builder) buildCoalesce(ctx *buildScalarCtx, ev memo.ExprView) tree.TypedExpr {
	exprs := make(tree.TypedExprs, ev.ChildCount())
	for i := range exprs {
		exprs[i] = b.buildScalar(ctx, ev.Child(i))
	}
	return tree.NewTypedCoalesceExpr(exprs, ev.Logical().Scalar.Type)
}

func (b *Builder) buildArray(ctx *buildScalarCtx, ev memo.ExprView) tree.TypedExpr {
	exprs := make(tree.TypedExprs, ev.ChildCount())
	for i := range exprs {
		exprs[i] = b.buildScalar(ctx, ev.Child(i))
	}
	return tree.NewTypedArray(exprs, ev.Logical().Scalar.Type)
}

func (b *Builder) buildUnsupportedExpr(ctx *buildScalarCtx, ev memo.ExprView) tree.TypedExpr {
	return ev.Private().(tree.TypedExpr)
}
