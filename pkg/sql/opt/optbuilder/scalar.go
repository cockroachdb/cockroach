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
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

type unaryFactoryFunc func(f *norm.Factory, input memo.GroupID) memo.GroupID
type binaryFactoryFunc func(f *norm.Factory, left, right memo.GroupID) memo.GroupID

func checkArrayElementType(t types.T) error {
	if !types.IsValidArrayElementType(t) {
		return pgerror.NewErrorf(pgerror.CodeFeatureNotSupportedError,
			"arrays of %s not allowed", t)
	}
	return nil
}

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
// See Builder.buildStmt for a description of the remaining input and return
// values.
func (b *Builder) buildScalar(scalar tree.TypedExpr, inScope *scope) (out memo.GroupID) {
	return b.buildScalarHelper(scalar, "", inScope, nil)
}

// buildScalarHelper builds a set of memo groups that represent the given scalar
// expression. If outScope is not nil, then this is a projection context, and
// the resulting memo group will be projected as an output column. Otherwise,
// the memo group is part of a larger expression that is not bound to a column.
//
// See Builder.buildStmt for a description of the remaining input and return
// values.
func (b *Builder) buildScalarHelper(
	scalar tree.TypedExpr, label string, inScope, outScope *scope,
) (out memo.GroupID) {
	// If we are in a grouping context and this expression corresponds to a
	// GROUP BY expression, return a reference to the GROUP BY column.
	// Note that GROUP BY columns cannot be reused inside an aggregate input
	// expression (when inAgg=true) because the aggregate input expressions and
	// grouping expressions are built as part of the same projection.
	inGroupingContext := inScope.inGroupingContext() && !inScope.groupby.inAgg
	if inGroupingContext {
		// TODO(rytaft): This currently regenerates a string for each subexpression.
		// Change this to generate the string once for the top-level expression and
		// check the relevant slice for this subexpression.
		if col, ok := inScope.groupby.groupStrs[symbolicExprStr(scalar)]; ok {
			// We pass aggOutScope as the input scope because it contains all of
			// the aggregates and grouping columns that are available for projection.
			// finishBuildScalarRef wraps projected columns in a variable expression
			// with a new column ID if they are not contained in the input scope, so
			// passing in aggOutScope ensures we don't create new column IDs when not
			// necessary.
			return b.finishBuildScalarRef(col, label, inScope.groupby.aggOutScope, outScope)
		}
	}

	switch t := scalar.(type) {
	case *scopeColumn:
		if inGroupingContext {
			// Non-grouping column was referenced. Note that a column that is part
			// of a larger grouping expression would have been detected by the
			// groupStrs checking code above.
			panic(builderError{pgerror.NewErrorf(pgerror.CodeGroupingError,
				"column \"%s\" must appear in the GROUP BY clause or be used in an aggregate function",
				tree.ErrString(&t.name),
			)})
		}
		return b.finishBuildScalarRef(t, label, inScope, outScope)

	case *tree.AndExpr:
		left := b.buildScalarHelper(t.TypedLeft(), "", inScope, nil)
		right := b.buildScalarHelper(t.TypedRight(), "", inScope, nil)
		conditions := b.factory.InternList([]memo.GroupID{left, right})
		out = b.factory.ConstructAnd(conditions)

	case *tree.Array:
		els := make([]memo.GroupID, len(t.Exprs))
		arrayType := t.ResolvedType()
		elementType := arrayType.(types.TArray).Typ
		if err := checkArrayElementType(elementType); err != nil {
			panic(builderError{err})
		}
		for i := range t.Exprs {
			texpr := t.Exprs[i].(tree.TypedExpr)
			els[i] = b.buildScalarHelper(texpr, "", inScope, nil)
		}
		elements := b.factory.InternList(els)
		out = b.factory.ConstructArray(elements, b.factory.InternType(arrayType))

	case *tree.ArrayFlatten:
		// We build
		//
		//  ARRAY(<subquery>)
		//
		// as
		//
		//   COALESCE(
		//     (SELECT array_agg(x) FROM (<subquery>)),
		//     ARRAY[]
		//   )
		//
		// The COALESCE is needed because ARRAY(<empty subquery>) needs to return
		// an empty array, while ARRAY_AGG with no inputs returns NULL.

		s := t.Subquery.(*subquery)
		aggInputColID := s.cols[0].id

		elemType := b.factory.Metadata().ColumnType(aggInputColID)
		if err := checkArrayElementType(elemType); err != nil {
			panic(builderError{err})
		}

		switch elemType.(type) {
		case types.TTuple, types.TArray:
			// We build this into ARRAY_AGG which doesn't support non-scalar types.
			panic(unimplementedf("can't build ARRAY(%s)", elemType))
		}

		aggColID := b.factory.Metadata().AddColumn(
			"array_agg",
			types.TArray{Typ: elemType},
		)

		typID := b.factory.InternType(types.TArray{Typ: elemType})

		var oc props.OrderingChoice
		oc.FromOrdering(s.ordering)

		out = b.factory.ConstructCoalesce(b.factory.InternList([]memo.GroupID{
			b.factory.ConstructSubquery(
				// A ScalarGroupBy always returns exactly one row, so there's no need
				// for a Max1Row here.
				b.factory.ConstructScalarGroupBy(
					s.group,
					b.factory.ConstructAggregations(
						b.factory.InternList([]memo.GroupID{
							b.factory.ConstructArrayAgg(
								b.factory.ConstructVariable(
									b.factory.InternColumnID(aggInputColID),
								),
							),
						}),
						b.factory.InternColList(opt.ColList{aggColID}),
					),
					b.factory.InternGroupByDef(&memo.GroupByDef{
						Ordering: oc,
					}),
				),
			),
			b.factory.ConstructArray(memo.EmptyList, typID),
		}))

	case *tree.BinaryExpr:
		// It's possible for an overload to be selected that expects different
		// types than the TypedExpr arguments return:
		//
		//   ARRAY[1, 2] || NULL
		//
		// This is a tricky case, because the type checker selects []int as the
		// type of the right argument, but then types it as unknown. This causes
		// issues for the execbuilder, which doesn't have enough information to
		// select the right overload. The solution is to wrap any mismatched
		// arguments with a CastExpr that preserves the static type.

		fn := binaryOpMap[t.Operator]
		left, _ := tree.ReType(t.TypedLeft(), t.ResolvedBinOp().LeftType)
		right, _ := tree.ReType(t.TypedRight(), t.ResolvedBinOp().RightType)
		out = fn(b.factory,
			b.buildScalarHelper(left, "", inScope, nil),
			b.buildScalarHelper(right, "", inScope, nil),
		)

	case *tree.CaseExpr:
		var input memo.GroupID
		if t.Expr != nil {
			texpr := t.Expr.(tree.TypedExpr)
			input = b.buildScalarHelper(texpr, "", inScope, nil)
		} else {
			input = b.factory.ConstructTrue()
		}

		whens := make([]memo.GroupID, 0, len(t.Whens)+1)
		for i := range t.Whens {
			texpr := t.Whens[i].Cond.(tree.TypedExpr)
			cond := b.buildScalarHelper(texpr, "", inScope, nil)
			texpr = t.Whens[i].Val.(tree.TypedExpr)
			val := b.buildScalarHelper(texpr, "", inScope, nil)
			whens = append(whens, b.factory.ConstructWhen(cond, val))
		}
		// Add the ELSE expression to the end of whens as a raw scalar expression.
		if t.Else != nil {
			texpr := t.Else.(tree.TypedExpr)
			elseExpr := b.buildScalarHelper(texpr, "", inScope, nil)
			whens = append(whens, elseExpr)
		} else {
			whens = append(whens, b.buildDatum(tree.DNull))
		}
		out = b.factory.ConstructCase(input, b.factory.InternList(whens))

	case *tree.IfExpr:
		cond := b.buildScalarHelper(t.Cond.(tree.TypedExpr), "", inScope, nil)
		tru := b.buildScalarHelper(t.True.(tree.TypedExpr), "", inScope, nil)
		els := b.buildScalarHelper(t.Else.(tree.TypedExpr), "", inScope, nil)
		whens := []memo.GroupID{
			b.factory.ConstructWhen(b.factory.ConstructTrue(), tru),
			els,
		}
		out = b.factory.ConstructCase(cond, b.factory.InternList(whens))

	case *tree.NullIfExpr:
		e1 := b.buildScalarHelper(t.Expr1.(tree.TypedExpr), "", inScope, nil)
		e2 := b.buildScalarHelper(t.Expr2.(tree.TypedExpr), "", inScope, nil)
		whens := []memo.GroupID{
			b.factory.ConstructWhen(e2, b.buildDatum(tree.DNull)),
			e1,
		}
		out = b.factory.ConstructCase(e1, b.factory.InternList(whens))

	case *tree.CastExpr:
		texpr := t.Expr.(tree.TypedExpr)
		arg := b.buildScalarHelper(texpr, "", inScope, nil)
		out = b.factory.ConstructCast(arg, b.factory.InternColType(t.Type.(coltypes.T)))

	case *tree.CoalesceExpr:
		args := make([]memo.GroupID, len(t.Exprs))
		for i := range args {
			args[i] = b.buildScalarHelper(t.TypedExprAt(i), "", inScope, nil)
		}
		out = b.factory.ConstructCoalesce(b.factory.InternList(args))

	case *tree.ColumnAccessExpr:
		input := b.buildScalarHelper(t.Expr.(tree.TypedExpr), "", inScope, nil)
		out = b.factory.ConstructColumnAccess(
			input, b.factory.InternTupleOrdinal(memo.TupleOrdinal(t.ColIndex)),
		)

	case *tree.ComparisonExpr:
		if sub, ok := t.Right.(*subquery); ok && sub.multiRow {
			out, _ = b.buildMultiRowSubquery(t, inScope)
		} else if b.hasSubOperator(t) {
			// Cases where the RHS is a subquery and not a scalar (of which only an
			// array or tuple is legal) were handled above.
			out = b.buildAnyScalar(t, inScope)
		} else {
			left := b.buildScalarHelper(t.TypedLeft(), "", inScope, nil)
			right := b.buildScalarHelper(t.TypedRight(), "", inScope, nil)

			fn := comparisonOpMap[t.Operator]

			if fn != nil {
				// Most comparison ops map directly to a factory method.
				out = fn(b.factory, left, right)
			} else if b.AllowUnsupportedExpr {
				out = b.factory.ConstructUnsupportedExpr(b.factory.InternTypedExpr(scalar))
			} else {
				// TODO(rytaft): remove this check when we are confident that
				// all operators are included in comparisonOpMap.
				panic(unimplementedf("unsupported comparison operator: %s", t.Operator))
			}
		}

	case *tree.DTuple:
		list := make([]memo.GroupID, len(t.D))
		for i := range t.D {
			list[i] = b.buildScalarHelper(t.D[i], "", inScope, nil)
		}
		out = b.factory.ConstructTuple(b.factory.InternList(list), b.factory.InternType(t.ResolvedType()))

	case *tree.FuncExpr:
		return b.buildFunction(t, label, inScope, outScope)

	case *tree.IndexedVar:
		if t.Idx < 0 || t.Idx >= len(inScope.cols) {
			panic(builderError{pgerror.NewErrorf(pgerror.CodeUndefinedColumnError,
				"invalid column ordinal: @%d", t.Idx+1)})
		}
		out = b.factory.ConstructVariable(b.factory.InternColumnID(inScope.cols[t.Idx].id))

	case *tree.NotExpr:
		out = b.factory.ConstructNot(b.buildScalarHelper(t.TypedInnerExpr(), "", inScope, nil))

	case *tree.OrExpr:
		left := b.buildScalarHelper(t.TypedLeft(), "", inScope, nil)
		right := b.buildScalarHelper(t.TypedRight(), "", inScope, nil)
		conditions := b.factory.InternList([]memo.GroupID{left, right})
		out = b.factory.ConstructOr(conditions)

	case *tree.ParenExpr:
		// Treat ParenExpr as if it wasn't present.
		return b.buildScalarHelper(t.TypedInnerExpr(), label, inScope, outScope)

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
		}

	case *tree.RangeCond:
		input := b.buildScalarHelper(t.TypedLeft(), "", inScope, nil)
		from := b.buildScalarHelper(t.TypedFrom(), "", inScope, nil)
		to := b.buildScalarHelper(t.TypedTo(), "", inScope, nil)
		out = b.buildRangeCond(t.Not, t.Symmetric, input, from, to)

	case *srf:
		if len(t.cols) == 1 {
			return b.finishBuildScalarRef(&t.cols[0], label, inScope, outScope)
		}
		list := make([]memo.GroupID, len(t.cols))
		for i := range t.cols {
			list[i] = b.buildScalarHelper(&t.cols[i], "", inScope, nil)
		}
		out = b.factory.ConstructTuple(b.factory.InternList(list), b.factory.InternType(t.ResolvedType()))

	case *subquery:
		out, _ = b.buildSingleRowSubquery(t, inScope)

	case *tree.Tuple:
		list := make([]memo.GroupID, len(t.Exprs))
		for i := range t.Exprs {
			list[i] = b.buildScalarHelper(t.Exprs[i].(tree.TypedExpr), "", inScope, nil)
		}
		out = b.factory.ConstructTuple(b.factory.InternList(list), b.factory.InternType(t.ResolvedType()))

	case *tree.UnaryExpr:
		out = b.buildScalarHelper(t.TypedInnerExpr(), "", inScope, nil)
		out = unaryOpMap[t.Operator](b.factory, out)

	// NB: this is the exception to the sorting of the case statements. The
	// tree.Datum case needs to occur after *tree.Placeholder which implements
	// Datum.
	case tree.Datum:
		out = b.buildDatum(t)

	default:
		if b.AllowUnsupportedExpr {
			out = b.factory.ConstructUnsupportedExpr(b.factory.InternTypedExpr(scalar))
		} else {
			panic(unimplementedf("not yet implemented: scalar expression: %T", scalar))
		}
	}

	return b.finishBuildScalar(scalar, out, label, inScope, outScope)
}

func (b *Builder) hasSubOperator(t *tree.ComparisonExpr) bool {
	return t.Operator == tree.Any || t.Operator == tree.All || t.Operator == tree.Some
}

func (b *Builder) buildAnyScalar(t *tree.ComparisonExpr, inScope *scope) memo.GroupID {
	left := b.buildScalarHelper(t.TypedLeft(), "", inScope, nil)
	right := b.buildScalarHelper(t.TypedRight(), "", inScope, nil)

	subop := opt.ComparisonOpMap[t.SubOperator]

	if t.Operator == tree.All {
		subop = opt.NegateOpMap[subop]
	}

	out := b.factory.ConstructAnyScalar(
		left,
		right,
		b.factory.InternOperator(subop),
	)

	if t.Operator == tree.All {
		out = b.factory.ConstructNot(out)
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
	f *tree.FuncExpr, label string, inScope, outScope *scope,
) (out memo.GroupID) {
	if f.WindowDef != nil {
		if inScope.groupby.inAgg {
			panic(builderError{sqlbase.NewWindowInAggError()})
		}
		panic(unimplementedf("window functions are not supported"))
	}

	def, err := f.Func.Resolve(b.semaCtx.SearchPath)
	if err != nil {
		panic(builderError{err})
	}

	funcDef := memo.FuncOpDef{
		Name:       def.Name,
		Type:       f.ResolvedType(),
		Properties: &def.FunctionProperties,
		Overload:   f.ResolvedOverload(),
	}

	if isAggregate(def) {
		return b.buildAggregateFunction(f, funcDef, label, inScope, outScope)
	}

	argList := make([]memo.GroupID, len(f.Exprs))
	for i, pexpr := range f.Exprs {
		argList[i] = b.buildScalar(pexpr.(tree.TypedExpr), inScope)
	}

	// Construct a private FuncOpDef that refers to a resolved function overload.
	out = b.factory.ConstructFunction(
		b.factory.InternList(argList), b.factory.InternFuncOpDef(&funcDef),
	)

	if isGenerator(def) {
		columns := len(def.ReturnLabels)
		return b.finishBuildGeneratorFunction(f, out, columns, label, inScope, outScope)
	}

	return b.finishBuildScalar(f, out, label, inScope, outScope)
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
		sb.scope.cols = append(sb.scope.cols, scopeColumn{
			origName: name,
			name:     name,
			typ:      md.ColumnType(colID),
			id:       colID,
		})
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
