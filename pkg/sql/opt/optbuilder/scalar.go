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

package optbuilder

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/opt/opt"

	"github.com/cockroachdb/cockroach/pkg/sql/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
)

type unaryFactoryFunc func(f opt.Factory, input opt.GroupID) opt.GroupID
type binaryFactoryFunc func(f opt.Factory, left, right opt.GroupID) opt.GroupID

// Map from tree.ComparisonOperator to Factory constructor function.
var comparisonOpMap = [tree.NumComparisonOperators]binaryFactoryFunc{
	tree.EQ:                (opt.Factory).ConstructEq,
	tree.LT:                (opt.Factory).ConstructLt,
	tree.GT:                (opt.Factory).ConstructGt,
	tree.LE:                (opt.Factory).ConstructLe,
	tree.GE:                (opt.Factory).ConstructGe,
	tree.NE:                (opt.Factory).ConstructNe,
	tree.In:                (opt.Factory).ConstructIn,
	tree.NotIn:             (opt.Factory).ConstructNotIn,
	tree.Like:              (opt.Factory).ConstructLike,
	tree.NotLike:           (opt.Factory).ConstructNotLike,
	tree.ILike:             (opt.Factory).ConstructILike,
	tree.NotILike:          (opt.Factory).ConstructNotILike,
	tree.SimilarTo:         (opt.Factory).ConstructSimilarTo,
	tree.NotSimilarTo:      (opt.Factory).ConstructNotSimilarTo,
	tree.RegMatch:          (opt.Factory).ConstructRegMatch,
	tree.NotRegMatch:       (opt.Factory).ConstructNotRegMatch,
	tree.RegIMatch:         (opt.Factory).ConstructRegIMatch,
	tree.NotRegIMatch:      (opt.Factory).ConstructNotRegIMatch,
	tree.IsDistinctFrom:    (opt.Factory).ConstructIsNot,
	tree.IsNotDistinctFrom: (opt.Factory).ConstructIs,
	tree.Contains:          (opt.Factory).ConstructContains,
	tree.ContainedBy: func(f opt.Factory, left, right opt.GroupID) opt.GroupID {
		// This is just syntatic sugar that reverses the operands.
		return f.ConstructContains(right, left)
	},
}

// Map from tree.BinaryOperator to Factory constructor function.
var binaryOpMap = [tree.NumBinaryOperators]binaryFactoryFunc{
	tree.Bitand:   (opt.Factory).ConstructBitand,
	tree.Bitor:    (opt.Factory).ConstructBitor,
	tree.Bitxor:   (opt.Factory).ConstructBitxor,
	tree.Plus:     (opt.Factory).ConstructPlus,
	tree.Minus:    (opt.Factory).ConstructMinus,
	tree.Mult:     (opt.Factory).ConstructMult,
	tree.Div:      (opt.Factory).ConstructDiv,
	tree.FloorDiv: (opt.Factory).ConstructFloorDiv,
	tree.Mod:      (opt.Factory).ConstructMod,
	tree.Pow:      (opt.Factory).ConstructPow,
	tree.Concat:   (opt.Factory).ConstructConcat,
	tree.LShift:   (opt.Factory).ConstructLShift,
	tree.RShift:   (opt.Factory).ConstructRShift,
}

// Map from tree.UnaryOperator to Factory constructor function.
var unaryOpMap = [tree.NumUnaryOperators]unaryFactoryFunc{
	tree.UnaryPlus:       (opt.Factory).ConstructUnaryPlus,
	tree.UnaryMinus:      (opt.Factory).ConstructUnaryMinus,
	tree.UnaryComplement: (opt.Factory).ConstructUnaryComplement,
}

// buildScalar builds a set of memo groups that represent the given scalar
// expression.
//
// See Builder.buildStmt for a description of the remaining input and
// return values.
func (b *Builder) buildScalar(scalar tree.TypedExpr, inScope *scope) (out opt.GroupID) {
	inGroupingContext := inScope.inGroupingContext() && !inScope.groupby.inAgg
	varsUsedIn := len(inScope.groupby.varsUsed)
	switch t := scalar.(type) {
	case *columnProps:
		if inGroupingContext && !inScope.groupby.aggInScope.hasColumn(t.index) {
			inScope.groupby.varsUsed = append(inScope.groupby.varsUsed, t.index)
		}
		return b.factory.ConstructVariable(b.factory.InternPrivate(t.index))

	case *tree.AndExpr:
		left := b.buildScalar(t.TypedLeft(), inScope)
		right := b.buildScalar(t.TypedRight(), inScope)
		conditions := b.factory.InternList([]opt.GroupID{left, right})
		out = b.factory.ConstructAnd(conditions)

	case *tree.BinaryExpr:
		fn := binaryOpMap[t.Operator]
		if fn != nil {
			out = fn(b.factory,
				b.buildScalar(t.TypedLeft(), inScope),
				b.buildScalar(t.TypedRight(), inScope),
			)
		} else if b.AllowUnsupportedExpr {
			out = b.factory.ConstructUnsupportedExpr(b.factory.InternPrivate(scalar))
		} else {
			panic(errorf("not yet implemented: operator %s", t.Operator.String()))
		}

	case *tree.CastExpr:
		arg := b.buildScalar(inScope.resolveType(t.Expr, types.Any), inScope)
		typ := coltypes.CastTargetToDatumType(t.Type)
		out = b.factory.ConstructCast(arg, b.factory.InternPrivate(typ))

	case *tree.CoalesceExpr:
		args := make([]opt.GroupID, len(t.Exprs))
		for i := range args {
			args[i] = b.buildScalar(t.TypedExprAt(i), inScope)
		}
		out = b.factory.ConstructCoalesce(b.factory.InternList(args))

	case *tree.ComparisonExpr:
		left := b.buildScalar(t.TypedLeft(), inScope)
		right := b.buildScalar(t.TypedRight(), inScope)
		// TODO(andyk): handle t.SubOperator. Do this by mapping Any, Some,
		// and All to various formulations of the opt Exists operator.
		fn := comparisonOpMap[t.Operator]
		if fn != nil {
			// Most comparison ops map directly to a factory method.
			out = fn(b.factory, left, right)
		} else if b.AllowUnsupportedExpr {
			out = b.factory.ConstructUnsupportedExpr(b.factory.InternPrivate(scalar))
		} else {
			// TODO(rytaft): remove this check when we are confident that
			// all operators are included in comparisonOpMap.
			panic(errorf("not yet implemented: operator %s", t.Operator.String()))
		}

	case *tree.DTuple:
		list := make([]opt.GroupID, len(t.D))
		for i := range t.D {
			list[i] = b.buildScalar(t.D[i], inScope)
		}
		out = b.factory.ConstructTuple(b.factory.InternList(list))

	case *tree.FuncExpr:
		out, _ = b.buildFunction(t, "", inScope)

	case *tree.IndexedVar:
		if t.Idx < 0 || t.Idx >= len(inScope.cols) {
			panic(errorf("invalid column ordinal @%d", t.Idx))
		}
		out = b.factory.ConstructVariable(b.factory.InternPrivate(inScope.cols[t.Idx].index))
		// TODO(rytaft): Do we need to update varsUsed here?

	case *tree.NotExpr:
		out = b.factory.ConstructNot(b.buildScalar(t.TypedInnerExpr(), inScope))

	case *tree.OrExpr:
		left := b.buildScalar(t.TypedLeft(), inScope)
		right := b.buildScalar(t.TypedRight(), inScope)
		conditions := b.factory.InternList([]opt.GroupID{left, right})
		out = b.factory.ConstructOr(conditions)

	case *tree.ParenExpr:
		out = b.buildScalar(t.TypedInnerExpr(), inScope)

	case *tree.Placeholder:
		out = b.factory.ConstructPlaceholder(b.factory.InternPrivate(t))
		// TODO(rytaft): Do we need to update varsUsed here?

	case *tree.Tuple:
		list := make([]opt.GroupID, len(t.Exprs))
		for i := range t.Exprs {
			list[i] = b.buildScalar(t.Exprs[i].(tree.TypedExpr), inScope)
		}
		out = b.factory.ConstructTuple(b.factory.InternList(list))

	case *tree.UnaryExpr:
		out = unaryOpMap[t.Operator](b.factory, b.buildScalar(t.TypedInnerExpr(), inScope))

	// NB: this is the exception to the sorting of the case statements. The
	// tree.Datum case needs to occur after *tree.Placeholder which implements
	// Datum.
	case tree.Datum:
		// Map True/False datums to True/False operator.
		if t == tree.DBoolTrue {
			out = b.factory.ConstructTrue()
		} else if t == tree.DBoolFalse {
			out = b.factory.ConstructFalse()
		} else {
			out = b.factory.ConstructConst(b.factory.InternPrivate(t))
		}

	default:
		if b.AllowUnsupportedExpr {
			out = b.factory.ConstructUnsupportedExpr(b.factory.InternPrivate(scalar))
		} else {
			panic(errorf("not yet implemented: scalar expr: %T", scalar))
		}
	}

	// If we are in a grouping context and this expression corresponds to
	// a GROUP BY expression, truncate varsUsed.
	if inGroupingContext {
		// TODO(rytaft): This currently regenerates a string for each subexpression.
		// Change this to generate the string once for the top-level expression and
		// check the relevant slice for this subexpression.
		if _, ok := inScope.groupby.groupStrs[symbolicExprStr(scalar)]; ok {
			inScope.groupby.varsUsed = inScope.groupby.varsUsed[:varsUsedIn]
		}
	}

	return out
}

// buildFunction builds a set of memo groups that represent a function
// expression.
//
// f       The given function expression.
// label   If a new column is synthesized, it will be labeled with this
//         string.
//
// If the function is an aggregate, the second return value, col,
// corresponds to the columnProps that represents the aggregate.
// See Builder.buildStmt for a description of the remaining input and
// return values.
func (b *Builder) buildFunction(
	f *tree.FuncExpr, label string, inScope *scope,
) (out opt.GroupID, col *columnProps) {
	def, err := f.Func.Resolve(b.semaCtx.SearchPath)
	if err != nil {
		panic(builderError{err})
	}

	funcDef := opt.FuncDef{Name: def.Name, Type: f.ResolvedType(), Overload: f.ResolvedBuiltin()}

	if isAggregate(def) {
		return b.buildAggregateFunction(f, funcDef, label, inScope)
	}

	argList := make([]opt.GroupID, len(f.Exprs))
	for i, pexpr := range f.Exprs {
		argList[i] = b.buildScalar(pexpr.(tree.TypedExpr), inScope)
	}

	// Construct a private FuncDef that refers to a resolved function overload.
	return b.factory.ConstructFunction(
		b.factory.InternList(argList), b.factory.InternPrivate(funcDef),
	), nil
}

// ScalarBuilder is a specialized variant of Builder that can be used to create
// a scalar from a TypedExpr. This is used to build scalar expressions for
// testing. It is also used temporarily to interface with the old planning code.
//
// TypedExprs can refer to columns in the current scope using IndexedVars (@1,
// @2, etc). When we build a scalar, we have to provide information about these
// columns.
type ScalarBuilder struct {
	Builder
	scope scope
}

// NewScalar creates a new ScalarBuilder. The provided columns are accessible
// from scalar expressions via IndexedVars.
// columnNames and columnTypes must have the same length.
func NewScalar(
	ctx context.Context,
	semaCtx *tree.SemaContext,
	evalCtx *tree.EvalContext,
	factory opt.Factory,
	columnNames []string,
	columnTypes []types.T,
) *ScalarBuilder {
	sb := &ScalarBuilder{
		Builder: Builder{
			factory: factory,
			colMap:  make([]columnProps, 1),
			ctx:     ctx,
			semaCtx: semaCtx,
			evalCtx: evalCtx,
		},
	}
	sb.scope.builder = &sb.Builder
	for i := range columnNames {
		sb.synthesizeColumn(&sb.scope, columnNames[i], columnTypes[i])
	}
	return sb
}

// Build a memo structure from a TypedExpr: the root group represents a scalar
// expression equivalent to expr.
func (sb *ScalarBuilder) Build(expr tree.TypedExpr) (root opt.GroupID, err error) {
	defer func() {
		if r := recover(); r != nil {
			// This code allows us to propagate builder errors without adding
			// lots of checks for `if err != nil` throughout the code. This is
			// only possible because the code does not update shared state and does
			// not manipulate locks.
			if bldErr, ok := r.(builderError); ok {
				err = bldErr
			} else {
				panic(r)
			}
		}
	}()

	return sb.buildScalar(expr, &sb.scope), nil
}
