// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package execbuilder

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

type buildScalarCtx struct {
	ivh tree.IndexedVarHelper

	// ivarMap is a map from opt.ColumnID to the index of an IndexedVar.
	// If a ColumnID is not in the map, it cannot appear in the expression.
	ivarMap opt.ColMap
}

type buildFunc func(b *Builder, ctx *buildScalarCtx, scalar opt.ScalarExpr) (tree.TypedExpr, error)

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
		opt.CollateOp:         (*Builder).buildCollate,
		opt.ArrayFlattenOp:    (*Builder).buildArrayFlatten,
		opt.IfErrOp:           (*Builder).buildIfErr,
		opt.UnsupportedExprOp: (*Builder).buildUnsupportedExpr,

		// Item operators.
		opt.ProjectionsItemOp:  (*Builder).buildItem,
		opt.AggregationsItemOp: (*Builder).buildItem,

		// Subquery operators.
		opt.ExistsOp:   (*Builder).buildExistsSubquery,
		opt.SubqueryOp: (*Builder).buildSubquery,
	}

	for _, op := range opt.BoolOperators {
		if scalarBuildFuncMap[op] == nil {
			scalarBuildFuncMap[op] = (*Builder).buildBoolean
		}
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
func (b *Builder) buildScalar(ctx *buildScalarCtx, scalar opt.ScalarExpr) (tree.TypedExpr, error) {
	if fn := scalarBuildFuncMap[scalar.Op()]; fn != nil {
		texpr, err := fn(b, ctx, scalar)
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
	return nil, errors.Errorf("unsupported op %s", scalar.Op())
}

func (b *Builder) buildTypedExpr(
	ctx *buildScalarCtx, scalar opt.ScalarExpr,
) (tree.TypedExpr, error) {
	return scalar.Private().(tree.TypedExpr), nil
}

func (b *Builder) buildNull(ctx *buildScalarCtx, scalar opt.ScalarExpr) (tree.TypedExpr, error) {
	return tree.ReType(tree.DNull, scalar.DataType())
}

func (b *Builder) buildVariable(
	ctx *buildScalarCtx, scalar opt.ScalarExpr,
) (tree.TypedExpr, error) {
	return b.indexedVar(ctx, b.mem.Metadata(), *scalar.Private().(*opt.ColumnID)), nil
}

func (b *Builder) indexedVar(
	ctx *buildScalarCtx, md *opt.Metadata, colID opt.ColumnID,
) tree.TypedExpr {
	idx, ok := ctx.ivarMap.Get(int(colID))
	if !ok {
		if b.nullifyMissingVarExprs > 0 {
			expr, err := tree.ReType(tree.DNull, md.ColumnMeta(colID).Type)
			if err != nil {
				panic(errors.NewAssertionErrorWithWrappedErrf(err, "unexpected failure during ReType"))
			}
			return expr
		}
		panic(errors.AssertionFailedf("cannot map variable %d to an indexed var", log.Safe(colID)))
	}
	return ctx.ivh.IndexedVarWithType(idx, md.ColumnMeta(colID).Type)
}

func (b *Builder) buildTuple(ctx *buildScalarCtx, scalar opt.ScalarExpr) (tree.TypedExpr, error) {
	if memo.CanExtractConstTuple(scalar) {
		return memo.ExtractConstDatum(scalar), nil
	}

	tup := scalar.(*memo.TupleExpr)
	typedExprs := make(tree.Exprs, len(tup.Elems))
	var err error
	for i, elem := range tup.Elems {
		typedExprs[i], err = b.buildScalar(ctx, elem)
		if err != nil {
			return nil, err
		}
	}
	return tree.NewTypedTuple(tup.Typ, typedExprs), nil
}

func (b *Builder) buildBoolean(ctx *buildScalarCtx, scalar opt.ScalarExpr) (tree.TypedExpr, error) {
	switch scalar.Op() {
	case opt.FiltersOp:
		if scalar.ChildCount() == 0 {
			// This can happen if the expression is not normalized (build tests).
			return tree.DBoolTrue, nil
		}
		fallthrough

	case opt.AndOp, opt.OrOp:
		expr, err := b.buildScalar(ctx, scalar.Child(0).(opt.ScalarExpr))
		if err != nil {
			return nil, err
		}
		for i, n := 1, scalar.ChildCount(); i < n; i++ {
			right, err := b.buildScalar(ctx, scalar.Child(i).(opt.ScalarExpr))
			if err != nil {
				return nil, err
			}
			if scalar.Op() == opt.OrOp {
				expr = tree.NewTypedOrExpr(expr, right)
			} else {
				expr = tree.NewTypedAndExpr(expr, right)
			}
		}
		return expr, nil

	case opt.NotOp:
		expr, err := b.buildScalar(ctx, scalar.Child(0).(opt.ScalarExpr))
		if err != nil {
			return nil, err
		}
		return tree.NewTypedNotExpr(expr), nil

	case opt.TrueOp:
		return tree.DBoolTrue, nil

	case opt.FalseOp:
		return tree.DBoolFalse, nil

	case opt.FiltersItemOp:
		return b.buildScalar(ctx, scalar.Child(0).(opt.ScalarExpr))

	case opt.RangeOp:
		return b.buildScalar(ctx, scalar.Child(0).(opt.ScalarExpr))

	default:
		panic(errors.AssertionFailedf("invalid op %s", log.Safe(scalar.Op())))
	}
}

func (b *Builder) buildComparison(
	ctx *buildScalarCtx, scalar opt.ScalarExpr,
) (tree.TypedExpr, error) {
	left, err := b.buildScalar(ctx, scalar.Child(0).(opt.ScalarExpr))
	if err != nil {
		return nil, err
	}
	right, err := b.buildScalar(ctx, scalar.Child(1).(opt.ScalarExpr))
	if err != nil {
		return nil, err
	}
	operator := opt.ComparisonOpReverseMap[scalar.Op()]
	return tree.NewTypedComparisonExpr(operator, left, right), nil
}

func (b *Builder) buildUnary(ctx *buildScalarCtx, scalar opt.ScalarExpr) (tree.TypedExpr, error) {
	input, err := b.buildScalar(ctx, scalar.Child(0).(opt.ScalarExpr))
	if err != nil {
		return nil, err
	}
	operator := opt.UnaryOpReverseMap[scalar.Op()]
	return tree.NewTypedUnaryExpr(operator, input, scalar.DataType()), nil
}

func (b *Builder) buildBinary(ctx *buildScalarCtx, scalar opt.ScalarExpr) (tree.TypedExpr, error) {
	left, err := b.buildScalar(ctx, scalar.Child(0).(opt.ScalarExpr))
	if err != nil {
		return nil, err
	}
	right, err := b.buildScalar(ctx, scalar.Child(1).(opt.ScalarExpr))
	if err != nil {
		return nil, err
	}
	operator := opt.BinaryOpReverseMap[scalar.Op()]
	return tree.NewTypedBinaryExpr(operator, left, right, scalar.DataType()), nil
}

func (b *Builder) buildFunction(
	ctx *buildScalarCtx, scalar opt.ScalarExpr,
) (tree.TypedExpr, error) {
	fn := scalar.(*memo.FunctionExpr)
	exprs := make(tree.TypedExprs, len(fn.Args))
	var err error
	for i := range exprs {
		exprs[i], err = b.buildScalar(ctx, fn.Args[i])
		if err != nil {
			return nil, err
		}
	}
	funcRef := tree.WrapFunction(fn.Name)
	return tree.NewTypedFuncExpr(
		funcRef,
		0, /* aggQualifier */
		exprs,
		nil, /* filter */
		nil, /* windowDef */
		fn.Typ,
		fn.Properties,
		fn.Overload,
	), nil
}

func (b *Builder) buildCase(ctx *buildScalarCtx, scalar opt.ScalarExpr) (tree.TypedExpr, error) {
	cas := scalar.(*memo.CaseExpr)
	input, err := b.buildScalar(ctx, cas.Input)
	if err != nil {
		return nil, err
	}

	// A searched CASE statement is represented by the optimizer with input=True.
	// The executor expects searched CASE statements to have nil inputs.
	if input == tree.DBoolTrue {
		input = nil
	}

	// Extract the list of WHEN ... THEN ... clauses.
	whens := make([]*tree.When, len(cas.Whens))
	for i, expr := range cas.Whens {
		whenExpr := expr.(*memo.WhenExpr)
		cond, err := b.buildScalar(ctx, whenExpr.Condition)
		if err != nil {
			return nil, err
		}
		val, err := b.buildScalar(ctx, whenExpr.Value)
		if err != nil {
			return nil, err
		}
		whens[i] = &tree.When{Cond: cond, Val: val}
	}

	elseExpr, err := b.buildScalar(ctx, cas.OrElse)
	if err != nil {
		return nil, err
	}
	if elseExpr == tree.DNull {
		elseExpr = nil
	}

	return tree.NewTypedCaseExpr(input, whens, elseExpr, cas.Typ)
}

func (b *Builder) buildCast(ctx *buildScalarCtx, scalar opt.ScalarExpr) (tree.TypedExpr, error) {
	cast := scalar.(*memo.CastExpr)
	input, err := b.buildScalar(ctx, cast.Input)
	if err != nil {
		return nil, err
	}
	return tree.NewTypedCastExpr(input, cast.Typ)
}

func (b *Builder) buildCoalesce(
	ctx *buildScalarCtx, scalar opt.ScalarExpr,
) (tree.TypedExpr, error) {
	coalesce := scalar.(*memo.CoalesceExpr)
	exprs := make(tree.TypedExprs, len(coalesce.Args))
	var err error
	for i := range exprs {
		exprs[i], err = b.buildScalar(ctx, coalesce.Args[i])
		if err != nil {
			return nil, err
		}
	}
	return tree.NewTypedCoalesceExpr(exprs, coalesce.Typ), nil
}

func (b *Builder) buildColumnAccess(
	ctx *buildScalarCtx, scalar opt.ScalarExpr,
) (tree.TypedExpr, error) {
	colAccess := scalar.(*memo.ColumnAccessExpr)
	input, err := b.buildScalar(ctx, colAccess.Input)
	if err != nil {
		return nil, err
	}
	childTyp := colAccess.Input.DataType()
	colIdx := int(colAccess.Idx)
	lbl := ""
	if childTyp.TupleLabels() != nil {
		lbl = childTyp.TupleLabels()[colIdx]
	}
	return tree.NewTypedColumnAccessExpr(input, lbl, colIdx), nil
}

func (b *Builder) buildArray(ctx *buildScalarCtx, scalar opt.ScalarExpr) (tree.TypedExpr, error) {
	arr := scalar.(*memo.ArrayExpr)
	if memo.CanExtractConstDatum(scalar) {
		return memo.ExtractConstDatum(scalar), nil
	}
	exprs := make(tree.TypedExprs, len(arr.Elems))
	var err error
	for i := range exprs {
		exprs[i], err = b.buildScalar(ctx, arr.Elems[i])
		if err != nil {
			return nil, err
		}
	}
	return tree.NewTypedArray(exprs, arr.Typ), nil
}

func (b *Builder) buildAnyScalar(
	ctx *buildScalarCtx, scalar opt.ScalarExpr,
) (tree.TypedExpr, error) {
	any := scalar.(*memo.AnyScalarExpr)
	left, err := b.buildScalar(ctx, any.Left)
	if err != nil {
		return nil, err
	}

	right, err := b.buildScalar(ctx, any.Right)
	if err != nil {
		return nil, err
	}

	cmp := opt.ComparisonOpReverseMap[any.Cmp]
	return tree.NewTypedComparisonExprWithSubOp(tree.Any, cmp, left, right), nil
}

func (b *Builder) buildIndirection(
	ctx *buildScalarCtx, scalar opt.ScalarExpr,
) (tree.TypedExpr, error) {
	expr, err := b.buildScalar(ctx, scalar.Child(0).(opt.ScalarExpr))
	if err != nil {
		return nil, err
	}

	index, err := b.buildScalar(ctx, scalar.Child(1).(opt.ScalarExpr))
	if err != nil {
		return nil, err
	}

	return tree.NewTypedIndirectionExpr(expr, index, scalar.DataType()), nil
}

func (b *Builder) buildCollate(ctx *buildScalarCtx, scalar opt.ScalarExpr) (tree.TypedExpr, error) {
	expr, err := b.buildScalar(ctx, scalar.Child(0).(opt.ScalarExpr))
	if err != nil {
		return nil, err
	}

	return tree.NewTypedCollateExpr(expr, scalar.(*memo.CollateExpr).Locale), nil
}

func (b *Builder) buildArrayFlatten(
	ctx *buildScalarCtx, scalar opt.ScalarExpr,
) (tree.TypedExpr, error) {
	af := scalar.(*memo.ArrayFlattenExpr)

	// The subquery here should always be uncorrelated: if it were not, we would
	// have converted it to an aggregation.
	if !af.Input.Relational().OuterCols.Empty() {
		panic(errors.AssertionFailedf("input to ArrayFlatten should be uncorrelated"))
	}

	root, err := b.buildRelational(af.Input)
	if err != nil {
		return nil, err
	}

	typ := b.mem.Metadata().ColumnMeta(af.RequestedCol).Type
	e := b.addSubquery(exec.SubqueryAllRows, typ, root.root, af.OriginalExpr)

	return tree.NewTypedArrayFlattenExpr(e), nil
}

func (b *Builder) buildIfErr(ctx *buildScalarCtx, scalar opt.ScalarExpr) (tree.TypedExpr, error) {
	ifErr := scalar.(*memo.IfErrExpr)
	cond, err := b.buildScalar(ctx, ifErr.Cond.(opt.ScalarExpr))
	if err != nil {
		return nil, err
	}

	var orElse tree.TypedExpr
	if ifErr.OrElse.ChildCount() > 0 {
		orElse, err = b.buildScalar(ctx, ifErr.OrElse.Child(0).(opt.ScalarExpr))
		if err != nil {
			return nil, err
		}
	}

	var errCode tree.TypedExpr
	if ifErr.ErrCode.ChildCount() > 0 {
		errCode, err = b.buildScalar(ctx, ifErr.ErrCode.Child(0).(opt.ScalarExpr))
		if err != nil {
			return nil, err
		}
	}

	return tree.NewTypedIfErrExpr(cond, orElse, errCode), nil
}

func (b *Builder) buildUnsupportedExpr(
	ctx *buildScalarCtx, scalar opt.ScalarExpr,
) (tree.TypedExpr, error) {
	return scalar.(*memo.UnsupportedExprExpr).Value, nil
}

func (b *Builder) buildItem(ctx *buildScalarCtx, scalar opt.ScalarExpr) (tree.TypedExpr, error) {
	return b.buildScalar(ctx, scalar.Child(0).(opt.ScalarExpr))
}

func (b *Builder) buildAny(ctx *buildScalarCtx, scalar opt.ScalarExpr) (tree.TypedExpr, error) {
	any := scalar.(*memo.AnyExpr)
	// We cannot execute correlated subqueries.
	if !any.Input.Relational().OuterCols.Empty() {
		return nil, b.decorrelationError()
	}

	// Build the execution plan for the input subquery.
	plan, err := b.buildRelational(any.Input)
	if err != nil {
		return nil, err
	}

	// Construct tuple type of columns in the row.
	contents := make([]types.T, plan.numOutputCols())
	plan.outputCols.ForEach(func(key, val int) {
		contents[val] = *b.mem.Metadata().ColumnMeta(opt.ColumnID(key)).Type
	})
	typs := types.MakeTuple(contents)
	subqueryExpr := b.addSubquery(exec.SubqueryAnyRows, typs, plan.root, any.OriginalExpr)

	// Build the scalar value that is compared against each row.
	scalarExpr, err := b.buildScalar(ctx, any.Scalar)
	if err != nil {
		return nil, err
	}

	cmp := opt.ComparisonOpReverseMap[any.Cmp]
	return tree.NewTypedComparisonExprWithSubOp(tree.Any, cmp, scalarExpr, subqueryExpr), nil
}

func (b *Builder) buildExistsSubquery(
	ctx *buildScalarCtx, scalar opt.ScalarExpr,
) (tree.TypedExpr, error) {
	exists := scalar.(*memo.ExistsExpr)
	// We cannot execute correlated subqueries.
	if !exists.Input.Relational().OuterCols.Empty() {
		return nil, b.decorrelationError()
	}

	// Build the execution plan for the subquery. Note that the subquery could
	// have subqueries of its own which are added to b.subqueries.
	plan, err := b.buildRelational(exists.Input)
	if err != nil {
		return nil, err
	}

	return b.addSubquery(exec.SubqueryExists, types.Bool, plan.root, exists.OriginalExpr), nil
}

func (b *Builder) buildSubquery(
	ctx *buildScalarCtx, scalar opt.ScalarExpr,
) (tree.TypedExpr, error) {
	subquery := scalar.(*memo.SubqueryExpr)
	input := subquery.Input

	// TODO(radu): for now we only support the trivial projection.
	cols := input.Relational().OutputCols
	if cols.Len() != 1 {
		return nil, errors.Errorf("subquery input with multiple columns")
	}

	// We cannot execute correlated subqueries.
	if !input.Relational().OuterCols.Empty() {
		return nil, b.decorrelationError()
	}

	// Build the execution plan for the subquery. Note that the subquery could
	// have subqueries of its own which are added to b.subqueries.
	plan, err := b.buildRelational(input)
	if err != nil {
		return nil, err
	}

	return b.addSubquery(exec.SubqueryOneRow, subquery.Typ, plan.root, subquery.OriginalExpr), nil
}

// addSubquery adds an entry to b.subqueries and creates a tree.Subquery
// expression node associated with it.
func (b *Builder) addSubquery(
	mode exec.SubqueryMode, typ *types.T, root exec.Node, originalExpr *tree.Subquery,
) *tree.Subquery {
	var originalSelect tree.SelectStatement
	if originalExpr != nil {
		originalSelect = originalExpr.Select
	}
	exprNode := &tree.Subquery{
		Select: originalSelect,
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
		panic(errors.AssertionFailedf("placeholder should have been replaced"))
	}
	return false
}
