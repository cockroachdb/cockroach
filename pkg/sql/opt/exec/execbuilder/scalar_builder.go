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

	"github.com/cockroachdb/cockroach/pkg/sql/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/pkg/errors"
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
		opt.ColumnAccessOp:    (*Builder).buildColumnAccess,
		opt.ArrayOp:           (*Builder).buildArray,
		opt.AnyOp:             (*Builder).buildAny,
		opt.AnyScalarOp:       (*Builder).buildAnyScalar,
		opt.IndirectionOp:     (*Builder).buildIndirection,
		opt.UnsupportedExprOp: (*Builder).buildUnsupportedExpr,

		// Subquery operators.
		opt.ExistsOp:   (*Builder).buildExistsSubquery,
		opt.SubqueryOp: (*Builder).buildSubquery,
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
		texpr, err := fn(b, ctx, ev)
		if err != nil {
			return nil, err
		}
		if b.evalCtx != nil && b.isConst(texpr) {
			value, err := texpr.Eval(b.evalCtx)
			if err != nil {
				// Ignore any errors here (e.g. division by zero), so they can happen
				// during execution where they are correctly handled. Note that in some
				// cases we might not even get an error (if this particular expression
				// does not get evaluated when the query runs, e.g. it's inside a CASE).
				return texpr, nil
			}
			if value == tree.DNull {
				// We don't want to return an expression that has a different type; cast
				// the NULL if necessary.
				var newExpr tree.TypedExpr
				newExpr, err = tree.ReType(tree.DNull, texpr.ResolvedType())
				if err != nil {
					return texpr, nil
				}
				return newExpr, nil
			}
			return value, nil
		}
		return texpr, nil
	}
	return nil, errors.Errorf("unsupported op %s", ev.Operator())
}

func (b *Builder) buildTypedExpr(ctx *buildScalarCtx, ev memo.ExprView) (tree.TypedExpr, error) {
	return ev.Private().(tree.TypedExpr), nil
}

func (b *Builder) buildNull(ctx *buildScalarCtx, ev memo.ExprView) (tree.TypedExpr, error) {
	return tree.ReType(tree.DNull, ev.Logical().Scalar.Type)
}

func (b *Builder) buildVariable(ctx *buildScalarCtx, ev memo.ExprView) (tree.TypedExpr, error) {
	return b.indexedVar(ctx, ev.Metadata(), ev.Private().(opt.ColumnID)), nil
}

func (b *Builder) indexedVar(
	ctx *buildScalarCtx, md *opt.Metadata, colID opt.ColumnID,
) tree.TypedExpr {
	idx, ok := ctx.ivarMap.Get(int(colID))
	if !ok {
		panic(fmt.Sprintf("cannot map variable %d to an indexed var", colID))
	}
	return ctx.ivh.IndexedVarWithType(idx, md.ColumnType(colID))
}

func (b *Builder) buildTuple(ctx *buildScalarCtx, ev memo.ExprView) (tree.TypedExpr, error) {
	if memo.MatchesTupleOfConstants(ev) {
		return memo.ExtractConstDatum(ev), nil
	}

	typedExprs := make(tree.Exprs, ev.ChildCount())
	var err error
	for i := 0; i < ev.ChildCount(); i++ {
		typedExprs[i], err = b.buildScalar(ctx, ev.Child(i))
		if err != nil {
			return nil, err
		}
	}
	typ := ev.Logical().Scalar.Type.(types.TTuple)
	return tree.NewTypedTuple(typ, typedExprs), nil
}

func (b *Builder) buildBoolean(ctx *buildScalarCtx, ev memo.ExprView) (tree.TypedExpr, error) {
	switch ev.Operator() {
	case opt.FiltersOp:
		if ev.ChildCount() == 0 {
			// This can happen if the expression is not normalized (build tests).
			return tree.DBoolTrue, nil
		}
		fallthrough

	case opt.AndOp, opt.OrOp:
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
		funcDef.Properties,
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
	return tree.NewTypedCastExpr(input, ev.Private().(coltypes.T))
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

func (b *Builder) buildColumnAccess(ctx *buildScalarCtx, ev memo.ExprView) (tree.TypedExpr, error) {
	child := ev.Child(0)
	input, err := b.buildScalar(ctx, child)
	if err != nil {
		return nil, err
	}
	childTyp := child.Logical().Scalar.Type.(types.TTuple)
	colIdx := int(ev.Private().(memo.TupleOrdinal))
	lbl := ""
	if childTyp.Labels != nil {
		lbl = childTyp.Labels[colIdx]
	}
	return tree.NewTypedColumnAccessExpr(input, lbl, colIdx), nil
}

func (b *Builder) buildArray(ctx *buildScalarCtx, ev memo.ExprView) (tree.TypedExpr, error) {
	if memo.HasOnlyConstChildren(ev) {
		return memo.ExtractConstDatum(ev), nil
	}
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

func (b *Builder) buildAnyScalar(ctx *buildScalarCtx, ev memo.ExprView) (tree.TypedExpr, error) {
	left, err := b.buildScalar(ctx, ev.Child(0))
	if err != nil {
		return nil, err
	}

	right, err := b.buildScalar(ctx, ev.Child(1))
	if err != nil {
		return nil, err
	}

	cmp := opt.ComparisonOpReverseMap[ev.Private().(opt.Operator)]
	return tree.NewTypedComparisonExprWithSubOp(tree.Any, cmp, left, right), nil
}

func (b *Builder) buildIndirection(ctx *buildScalarCtx, ev memo.ExprView) (tree.TypedExpr, error) {
	expr, err := b.buildScalar(ctx, ev.Child(0))
	if err != nil {
		return nil, err
	}

	index, err := b.buildScalar(ctx, ev.Child(1))
	if err != nil {
		return nil, err
	}

	return tree.NewTypedIndirectionExpr(expr, index), nil
}

func (b *Builder) buildUnsupportedExpr(
	ctx *buildScalarCtx, ev memo.ExprView,
) (tree.TypedExpr, error) {
	return ev.Private().(tree.TypedExpr), nil
}

func (b *Builder) buildAny(ctx *buildScalarCtx, ev memo.ExprView) (tree.TypedExpr, error) {
	input := ev.Child(0)
	// We cannot execute correlated subqueries.
	if !input.Logical().Relational.OuterCols.Empty() {
		return nil, b.decorrelationError()
	}

	// Build the execution plan for the input subquery.
	plan, err := b.buildRelational(input)
	if err != nil {
		return nil, err
	}

	// Construct tuple type of columns in the row.
	types := types.TTuple{Types: make([]types.T, plan.outputCols.Len())}
	plan.outputCols.ForEach(func(key, val int) {
		types.Types[val] = ev.Metadata().ColumnType(opt.ColumnID(key))
	})

	def := ev.Private().(*memo.SubqueryDef)
	subqueryExpr := b.addSubquery(exec.SubqueryAnyRows, types, plan.root, def.OriginalExpr)

	// Build the scalar value that is compared against each row.
	scalar, err := b.buildScalar(ctx, ev.Child(1))
	if err != nil {
		return nil, err
	}

	cmp := opt.ComparisonOpReverseMap[def.Cmp]
	return tree.NewTypedComparisonExprWithSubOp(tree.Any, cmp, scalar, subqueryExpr), nil
}

func (b *Builder) buildExistsSubquery(
	ctx *buildScalarCtx, ev memo.ExprView,
) (tree.TypedExpr, error) {
	input := ev.Child(0)
	// We cannot execute correlated subqueries.
	if !input.Logical().Relational.OuterCols.Empty() {
		return nil, b.decorrelationError()
	}

	// Build the execution plan for the subquery. Note that the subquery could
	// have subqueries of its own which are added to b.subqueries.
	root, err := b.build(input)
	if err != nil {
		return nil, err
	}

	def := ev.Private().(*memo.SubqueryDef)
	return b.addSubquery(exec.SubqueryExists, types.Bool, root, def.OriginalExpr), nil
}

func (b *Builder) buildSubquery(ctx *buildScalarCtx, ev memo.ExprView) (tree.TypedExpr, error) {
	input := ev.Child(0)
	// Typically, the input is a Max1RowOp; it might be elided if the optimizer
	// proves that more than 1 result is not possible.
	if input.Operator() == opt.Max1RowOp {
		input = input.Child(0)
	}

	// TODO(radu): for now we only support the trivial projection.
	cols := input.Logical().Relational.OutputCols
	if cols.Len() != 1 {
		return nil, errors.Errorf("subquery input with multiple columns")
	}

	// We cannot execute correlated subqueries.
	if !input.Logical().Relational.OuterCols.Empty() {
		return nil, b.decorrelationError()
	}

	// Build the execution plan for the subquery. Note that the subquery could
	// have subqueries of its own which are added to b.subqueries.
	root, err := b.build(input)
	if err != nil {
		return nil, err
	}

	def := ev.Private().(*memo.SubqueryDef)
	return b.addSubquery(exec.SubqueryOneRow, ev.Logical().Scalar.Type, root, def.OriginalExpr), nil
}

// addSubquery adds an entry to b.subqueries and creates a tree.Subquery
// expression node associated with it.
func (b *Builder) addSubquery(
	mode exec.SubqueryMode, typ types.T, root exec.Node, originalExpr *tree.Subquery,
) *tree.Subquery {
	exprNode := &tree.Subquery{
		Select: originalExpr.Select,
		Exists: mode == exec.SubqueryExists,
	}
	exprNode.SetType(typ)
	b.subqueries = append(b.subqueries, exec.Subquery{
		ExprNode: exprNode,
		Mode:     mode,
		Root:     root,
	})
	// Associate the tree.Subquery expression node with this subquery
	// by index (1-based).
	exprNode.Idx = len(b.subqueries)
	return exprNode
}

func (b *Builder) isConst(expr tree.Expr) bool {
	return b.fastIsConstVisitor.run(expr)
}

// fastIsConstVisitor determines if an expression is constant by visiting
// at most two levels of the tree (with one exception, see below).
// In essence, it determines whether an expression is constant by checking
// whether its children are const Datums.
//
// This can be used by the execbuilder since constants are evaluated
// bottom-up. If a child is *not* a const Datum, that means it was already
// determined to be non-constant, and therefore was not evaluated.
type fastIsConstVisitor struct {
	isConst bool

	// visited indicates whether we have already visited one level of the tree.
	// fastIsConstVisitor only visits at most two levels of the tree, with one
	// exception: If the second level has a Cast expression, fastIsConstVisitor
	// may visit three levels.
	visited bool
}

var _ tree.Visitor = &fastIsConstVisitor{}

func (v *fastIsConstVisitor) VisitPre(expr tree.Expr) (recurse bool, newExpr tree.Expr) {
	if v.visited {
		if _, ok := expr.(*tree.CastExpr); ok {
			// We recurse one more time for cast expressions, since the
			// execbuilder may have wrapped a NULL.
			return true, expr
		}
		if _, ok := expr.(tree.Datum); !ok || isVar(expr) {
			// If the child expression is not a const Datum, the parent expression is
			// not constant. Note that all constant literals have already been
			// normalized to Datum in TypeCheck.
			v.isConst = false
		}
		return false, expr
	}
	v.visited = true

	// If the parent expression is a variable or impure function, we know that it
	// is not constant.

	if isVar(expr) {
		v.isConst = false
		return false, expr
	}

	switch t := expr.(type) {
	case *tree.FuncExpr:
		if t.IsImpure() {
			v.isConst = false
			return false, expr
		}
	}

	return true, expr
}

func (*fastIsConstVisitor) VisitPost(expr tree.Expr) tree.Expr { return expr }

func (v *fastIsConstVisitor) run(expr tree.Expr) bool {
	v.isConst = true
	v.visited = false
	tree.WalkExprConst(v, expr)
	return v.isConst
}

// isVar returns true if the expression's value can vary during plan
// execution.
func isVar(expr tree.Expr) bool {
	switch expr.(type) {
	case tree.VariableExpr:
		return true
	case *tree.Placeholder:
		panic("placeholder should have been replaced")
	}
	return false
}
