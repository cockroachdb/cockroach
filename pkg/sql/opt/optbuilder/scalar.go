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

	"github.com/cockroachdb/cockroach/pkg/sql/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/norm"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
)

type unaryFactoryFunc func(f *norm.Factory, input memo.GroupID) memo.GroupID
type binaryFactoryFunc func(f *norm.Factory, left, right memo.GroupID) memo.GroupID

// Map from tree.ComparisonOperator to Factory constructor function.
var comparisonOpMap = [tree.NumComparisonOperators]binaryFactoryFunc{
	tree.EQ:                (*norm.Factory).ConstructEq,
	tree.LT:                (*norm.Factory).ConstructLt,
	tree.GT:                (*norm.Factory).ConstructGt,
	tree.LE:                (*norm.Factory).ConstructLe,
	tree.GE:                (*norm.Factory).ConstructGe,
	tree.NE:                (*norm.Factory).ConstructNe,
	tree.In:                (*norm.Factory).ConstructIn,
	tree.NotIn:             (*norm.Factory).ConstructNotIn,
	tree.Like:              (*norm.Factory).ConstructLike,
	tree.NotLike:           (*norm.Factory).ConstructNotLike,
	tree.ILike:             (*norm.Factory).ConstructILike,
	tree.NotILike:          (*norm.Factory).ConstructNotILike,
	tree.SimilarTo:         (*norm.Factory).ConstructSimilarTo,
	tree.NotSimilarTo:      (*norm.Factory).ConstructNotSimilarTo,
	tree.RegMatch:          (*norm.Factory).ConstructRegMatch,
	tree.NotRegMatch:       (*norm.Factory).ConstructNotRegMatch,
	tree.RegIMatch:         (*norm.Factory).ConstructRegIMatch,
	tree.NotRegIMatch:      (*norm.Factory).ConstructNotRegIMatch,
	tree.IsDistinctFrom:    (*norm.Factory).ConstructIsNot,
	tree.IsNotDistinctFrom: (*norm.Factory).ConstructIs,
	tree.Contains:          (*norm.Factory).ConstructContains,
	tree.ContainedBy: func(f *norm.Factory, left, right memo.GroupID) memo.GroupID {
		// This is just syntatic sugar that reverses the operands.
		return f.ConstructContains(right, left)
	},
	tree.JSONExists:     (*norm.Factory).ConstructJsonExists,
	tree.JSONAllExists:  (*norm.Factory).ConstructJsonAllExists,
	tree.JSONSomeExists: (*norm.Factory).ConstructJsonSomeExists,
}

// Map from tree.BinaryOperator to Factory constructor function.
var binaryOpMap = [tree.NumBinaryOperators]binaryFactoryFunc{
	tree.Bitand:            (*norm.Factory).ConstructBitand,
	tree.Bitor:             (*norm.Factory).ConstructBitor,
	tree.Bitxor:            (*norm.Factory).ConstructBitxor,
	tree.Plus:              (*norm.Factory).ConstructPlus,
	tree.Minus:             (*norm.Factory).ConstructMinus,
	tree.Mult:              (*norm.Factory).ConstructMult,
	tree.Div:               (*norm.Factory).ConstructDiv,
	tree.FloorDiv:          (*norm.Factory).ConstructFloorDiv,
	tree.Mod:               (*norm.Factory).ConstructMod,
	tree.Pow:               (*norm.Factory).ConstructPow,
	tree.Concat:            (*norm.Factory).ConstructConcat,
	tree.LShift:            (*norm.Factory).ConstructLShift,
	tree.RShift:            (*norm.Factory).ConstructRShift,
	tree.JSONFetchText:     (*norm.Factory).ConstructFetchText,
	tree.JSONFetchVal:      (*norm.Factory).ConstructFetchVal,
	tree.JSONFetchValPath:  (*norm.Factory).ConstructFetchValPath,
	tree.JSONFetchTextPath: (*norm.Factory).ConstructFetchTextPath,
}

// Map from tree.UnaryOperator to Factory constructor function.
var unaryOpMap = [tree.NumUnaryOperators]unaryFactoryFunc{
	tree.UnaryMinus:      (*norm.Factory).ConstructUnaryMinus,
	tree.UnaryComplement: (*norm.Factory).ConstructUnaryComplement,
}

// buildScalar builds a set of memo groups that represent the given scalar
// expression.
//
// See Builder.buildStmt for a description of the remaining input and
// return values.
func (b *Builder) buildScalar(scalar tree.TypedExpr, inScope *scope) (out memo.GroupID) {
	inGroupingContext := inScope.inGroupingContext() && !inScope.groupby.inAgg
	varsUsedIn := len(inScope.groupby.varsUsed)
	switch t := scalar.(type) {
	case *scopeColumn:
		if inGroupingContext && !inScope.groupby.aggInScope.hasColumn(t.id) {
			inScope.groupby.varsUsed = append(inScope.groupby.varsUsed, t.id)
		}
		return b.factory.ConstructVariable(b.factory.InternColumnID(t.id))

	case *tree.AndExpr:
		left := b.buildScalar(t.TypedLeft(), inScope)
		right := b.buildScalar(t.TypedRight(), inScope)
		conditions := b.factory.InternList([]memo.GroupID{left, right})
		out = b.factory.ConstructAnd(conditions)

	case *tree.Array:
		els := make([]memo.GroupID, len(t.Exprs))
		arrayType := t.ResolvedType()
		elementType := arrayType.(types.TArray).Typ
		for i := range t.Exprs {
			els[i] = b.buildScalar(inScope.resolveType(t.Exprs[i], elementType), inScope)
		}
		elements := b.factory.InternList(els)
		out = b.factory.ConstructArray(elements, b.factory.InternType(arrayType))

	case *tree.BinaryExpr:
		fn := binaryOpMap[t.Operator]
		if fn != nil {
			out = fn(b.factory,
				b.buildScalar(t.TypedLeft(), inScope),
				b.buildScalar(t.TypedRight(), inScope),
			)
		} else if b.AllowUnsupportedExpr {
			out = b.factory.ConstructUnsupportedExpr(b.factory.InternTypedExpr(scalar))
		} else {
			panic(errorf("not yet implemented: operator %s", t.Operator.String()))
		}

	case *tree.CaseExpr:
		var condType types.T
		var input memo.GroupID
		if t.Expr != nil {
			condType = types.Any
			input = b.buildScalar(inScope.resolveType(t.Expr, types.Any), inScope)
		} else {
			condType = types.Bool
			input = b.factory.ConstructTrue()
		}

		whens := make([]memo.GroupID, 0, len(t.Whens)+1)
		for i := range t.Whens {
			cond := b.buildScalar(inScope.resolveType(t.Whens[i].Cond, condType), inScope)
			val := b.buildScalar(inScope.resolveType(t.Whens[i].Val, types.Any), inScope)
			whens = append(whens, b.factory.ConstructWhen(cond, val))
		}
		// Add the ELSE expression to the end of whens as a raw scalar expression.
		if t.Else != nil {
			elseExpr := b.buildScalar(inScope.resolveType(t.Else, types.Any), inScope)
			whens = append(whens, elseExpr)
		} else {
			whens = append(whens, b.buildDatum(tree.DNull))
		}
		out = b.factory.ConstructCase(input, b.factory.InternList(whens))

	case *tree.CastExpr:
		arg := b.buildScalar(inScope.resolveType(t.Expr, types.Any), inScope)
		typ := coltypes.CastTargetToDatumType(t.Type)
		out = b.factory.ConstructCast(arg, b.factory.InternType(typ))

	case *tree.CoalesceExpr:
		args := make([]memo.GroupID, len(t.Exprs))
		for i := range args {
			args[i] = b.buildScalar(t.TypedExprAt(i), inScope)
		}
		out = b.factory.ConstructCoalesce(b.factory.InternList(args))

	case *tree.ComparisonExpr:
		if sub, ok := t.Right.(*subquery); ok && sub.multiRow {
			out, _ = b.buildMultiRowSubquery(t, inScope)
		} else {
			left := b.buildScalar(t.TypedLeft(), inScope)
			right := b.buildScalar(t.TypedRight(), inScope)

			// TODO(andyk): handle t.SubOperator. Do this by mapping Any, Some,
			// and All to various formulations of the opt Exists operator. For now,
			// avoid an 'unused' linter complaint.
			_ = tree.NewTypedComparisonExprWithSubOp

			fn := comparisonOpMap[t.Operator]
			if fn != nil {
				// Most comparison ops map directly to a factory method.
				out = fn(b.factory, left, right)
			} else if b.AllowUnsupportedExpr {
				out = b.factory.ConstructUnsupportedExpr(b.factory.InternTypedExpr(scalar))
			} else {
				// TODO(rytaft): remove this check when we are confident that
				// all operators are included in comparisonOpMap.
				panic(errorf("not yet implemented: operator %s", t.Operator.String()))
			}
		}

	case *tree.DTuple:
		list := make([]memo.GroupID, len(t.D))
		for i := range t.D {
			list[i] = b.buildScalar(t.D[i], inScope)
		}
		out = b.factory.ConstructTuple(b.factory.InternList(list))

	case *tree.FuncExpr:
		out, _ = b.buildFunction(t, "", inScope)

	case *tree.IndexedVar:
		if t.Idx < 0 || t.Idx >= len(inScope.cols) {
			panic(errorf("invalid column ordinal @%d", t.Idx+1))
		}
		out = b.factory.ConstructVariable(b.factory.InternColumnID(inScope.cols[t.Idx].id))
		// TODO(rytaft): Do we need to update varsUsed here?

	case *tree.NotExpr:
		out = b.factory.ConstructNot(b.buildScalar(t.TypedInnerExpr(), inScope))

	case *tree.OrExpr:
		left := b.buildScalar(t.TypedLeft(), inScope)
		right := b.buildScalar(t.TypedRight(), inScope)
		conditions := b.factory.InternList([]memo.GroupID{left, right})
		out = b.factory.ConstructOr(conditions)

	case *tree.ParenExpr:
		out = b.buildScalar(t.TypedInnerExpr(), inScope)

	case *tree.Placeholder:
		if b.evalCtx.HasPlaceholders() {
			// Replace placeholders with their value.
			d, err := t.Eval(b.evalCtx)
			if err != nil {
				panic(builderError{err})
			}
			out = b.buildDatum(d)
		} else {
			out = b.factory.ConstructPlaceholder(b.factory.InternTypedExpr(t))
			// TODO(rytaft): Do we need to update varsUsed here?
		}

	case *tree.RangeCond:
		input := b.buildScalar(t.TypedLeft(), inScope)
		from := b.buildScalar(t.TypedFrom(), inScope)
		to := b.buildScalar(t.TypedTo(), inScope)
		out = b.buildRangeCond(t.Not, t.Symmetric, input, from, to)

	case *subquery:
		out, _ = b.buildSingleRowSubquery(t, inScope)

	case *tree.Tuple:
		list := make([]memo.GroupID, len(t.Exprs))
		for i := range t.Exprs {
			list[i] = b.buildScalar(t.Exprs[i].(tree.TypedExpr), inScope)
		}
		out = b.factory.ConstructTuple(b.factory.InternList(list))

	case *tree.UnaryExpr:
		out = b.buildScalar(t.TypedInnerExpr(), inScope)

		// Discard do-nothing unary plus operator.
		if t.Operator != tree.UnaryPlus {
			out = unaryOpMap[t.Operator](b.factory, out)
		}

	// NB: this is the exception to the sorting of the case statements. The
	// tree.Datum case needs to occur after *tree.Placeholder which implements
	// Datum.
	case tree.Datum:
		out = b.buildDatum(t)

	default:
		if b.AllowUnsupportedExpr {
			out = b.factory.ConstructUnsupportedExpr(b.factory.InternTypedExpr(scalar))
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

// buildDatum maps certain datums to separate operators, for easier matching.
func (b *Builder) buildDatum(d tree.Datum) memo.GroupID {
	if d == tree.DNull {
		return b.factory.ConstructNull(b.factory.InternType(types.Unknown))
	}
	if boolVal, ok := d.(*tree.DBool); ok {
		// Map True/False datums to True/False operator.
		if *boolVal {
			return b.factory.ConstructTrue()
		}
		return b.factory.ConstructFalse()
	}
	return b.factory.ConstructConst(b.factory.InternDatum(d))
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
) (out memo.GroupID, col *scopeColumn) {
	def, err := f.Func.Resolve(b.semaCtx.SearchPath)
	if err != nil {
		panic(builderError{err})
	}

	funcDef := memo.FuncOpDef{Name: def.Name, Type: f.ResolvedType(), Overload: f.ResolvedBuiltin()}

	if isAggregate(def) {
		col := b.buildAggregateFunction(f, funcDef, label, inScope)
		return col.group, col
	}

	argList := make([]memo.GroupID, len(f.Exprs))
	for i, pexpr := range f.Exprs {
		argList[i] = b.buildScalar(pexpr.(tree.TypedExpr), inScope)
	}

	// Construct a private FuncOpDef that refers to a resolved function overload.
	return b.factory.ConstructFunction(
		b.factory.InternList(argList), b.factory.InternFuncOpDef(&funcDef),
	), nil
}

// buildRangeCond builds a RANGE clause as a simpler expression. Examples:
// x BETWEEN a AND b                ->  x >= a AND x <= b
// x NOT BETWEEN a AND b            ->  NOT (x >= a AND x <= b)
// x BETWEEN SYMMETRIC a AND b      ->  (x >= a AND x <= b) OR (x >= b AND x <= a)
// x NOT BETWEEN SYMMETRIC a AND b  ->  NOT ((x >= a AND x <= b) OR (x >= b AND x <= a))
//
// Note that these expressions are subject to normalization rules (which can
// push down the negation).
// TODO(radu): this doesn't work when the expressions have side-effects.
func (b *Builder) buildRangeCond(
	not bool, symmetric bool, input, from, to memo.GroupID,
) memo.GroupID {
	// Build "input >= from AND input <= to".
	out := b.factory.ConstructAnd(
		b.factory.InternList([]memo.GroupID{
			b.factory.ConstructGe(input, from),
			b.factory.ConstructLe(input, to),
		}),
	)

	if symmetric {
		// Build "(input >= from AND input <= to) OR (input >= to AND input <= from)".
		lhs := out
		rhs := b.factory.ConstructAnd(
			b.factory.InternList([]memo.GroupID{
				b.factory.ConstructGe(input, to),
				b.factory.ConstructLe(input, from),
			}),
		)
		out = b.factory.ConstructOr(
			b.factory.InternList([]memo.GroupID{lhs, rhs}),
		)
	}

	if not {
		out = b.factory.ConstructNot(out)
	}
	return out
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

// NewScalar creates a new ScalarBuilder. The columns in the metadata are accessible
// from scalar expressions via IndexedVars.
func NewScalar(
	ctx context.Context, semaCtx *tree.SemaContext, evalCtx *tree.EvalContext, factory *norm.Factory,
) *ScalarBuilder {
	md := factory.Metadata()
	sb := &ScalarBuilder{
		Builder: Builder{
			factory: factory,
			colMap:  make([]scopeColumn, 1, 1+md.NumColumns()),
			ctx:     ctx,
			semaCtx: semaCtx,
			evalCtx: evalCtx,
		},
	}
	sb.scope.builder = &sb.Builder

	// Put all the columns in the current scope.
	sb.scope.cols = make([]scopeColumn, 0, md.NumColumns())
	for colID := opt.ColumnID(1); int(colID) <= md.NumColumns(); colID++ {
		name := tree.Name(md.ColumnLabel(colID))
		col := scopeColumn{
			origName: name,
			name:     name,
			typ:      md.ColumnType(colID),
			id:       colID,
		}
		sb.colMap = append(sb.colMap, col)
		sb.scope.cols = append(sb.scope.cols, col)
	}

	return sb
}

// Build a memo structure from a TypedExpr: the root group represents a scalar
// expression equivalent to expr.
func (sb *ScalarBuilder) Build(expr tree.TypedExpr) (root memo.GroupID, err error) {
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
