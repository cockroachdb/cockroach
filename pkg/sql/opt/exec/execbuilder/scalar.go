// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package execbuilder

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/norm"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props/physical"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/xform"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins/builtinsregistry"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree/treebin"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree/treecmp"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/volatility"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

type buildScalarCtx struct {
	ivh tree.IndexedVarHelper

	// ivarMap is a map from opt.ColumnID to the index of an IndexedVar.
	// If a ColumnID is not in the map, it cannot appear in the expression.
	ivarMap colOrdMap
}

type buildFunc func(b *Builder, ctx *buildScalarCtx, scalar opt.ScalarExpr) (tree.TypedExpr, error)

var scalarBuildFuncMap [opt.NumOperators]buildFunc

func init() {
	// This code is not inline to avoid an initialization loop error (some of
	// the functions depend on scalarBuildFuncMap which in turn depends on the
	// functions).
	scalarBuildFuncMap = [opt.NumOperators]buildFunc{
		opt.VariableOp:       (*Builder).buildVariable,
		opt.ConstOp:          (*Builder).buildTypedExpr,
		opt.NullOp:           (*Builder).buildNull,
		opt.PlaceholderOp:    (*Builder).buildPlaceholder,
		opt.TupleOp:          (*Builder).buildTuple,
		opt.FunctionOp:       (*Builder).buildFunction,
		opt.CaseOp:           (*Builder).buildCase,
		opt.CastOp:           (*Builder).buildCast,
		opt.AssignmentCastOp: (*Builder).buildAssignmentCast,
		opt.CoalesceOp:       (*Builder).buildCoalesce,
		opt.ColumnAccessOp:   (*Builder).buildColumnAccess,
		opt.ArrayOp:          (*Builder).buildArray,
		opt.AnyOp:            (*Builder).buildAny,
		opt.AnyScalarOp:      (*Builder).buildAnyScalar,
		opt.IndirectionOp:    (*Builder).buildIndirection,
		opt.CollateOp:        (*Builder).buildCollate,
		opt.ArrayFlattenOp:   (*Builder).buildArrayFlatten,
		opt.IfErrOp:          (*Builder).buildIfErr,

		// Item operators.
		opt.ProjectionsItemOp:  (*Builder).buildItem,
		opt.AggregationsItemOp: (*Builder).buildItem,

		// Subquery operators.
		opt.ExistsOp:   (*Builder).buildExistsSubquery,
		opt.SubqueryOp: (*Builder).buildSubquery,

		// Routines.
		opt.UDFCallOp:    (*Builder).buildUDF,
		opt.TxnControlOp: (*Builder).buildTxnControl,
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
	fn := scalarBuildFuncMap[scalar.Op()]
	if fn == nil {
		return nil, errors.AssertionFailedf("unsupported op %s", redact.Safe(scalar.Op()))
	}
	return fn(b, ctx, scalar)
}

func (b *Builder) buildScalarWithMap(
	colMap colOrdMap, scalar opt.ScalarExpr,
) (tree.TypedExpr, error) {
	ctx := buildScalarCtx{
		ivh:     tree.MakeIndexedVarHelper(nil /* container */, colMap.MaxOrd()+1),
		ivarMap: colMap,
	}
	return b.buildScalar(&ctx, scalar)
}

func (b *Builder) buildTypedExpr(
	ctx *buildScalarCtx, scalar opt.ScalarExpr,
) (tree.TypedExpr, error) {
	return scalar.Private().(tree.TypedExpr), nil
}

func (b *Builder) buildPlaceholder(
	ctx *buildScalarCtx, scalar opt.ScalarExpr,
) (tree.TypedExpr, error) {
	if b.evalCtx != nil && b.evalCtx.Placeholders != nil {
		return eval.Expr(b.ctx, b.evalCtx, scalar.Private().(*tree.Placeholder))
	}
	return b.buildTypedExpr(ctx, scalar)
}

func (b *Builder) buildNull(ctx *buildScalarCtx, scalar opt.ScalarExpr) (tree.TypedExpr, error) {
	retypedNull, ok := eval.ReType(tree.DNull, scalar.DataType())
	if !ok {
		return nil, errors.AssertionFailedf("failed to retype NULL to %s", scalar.DataType())
	}
	return retypedNull, nil
}

func (b *Builder) buildVariable(
	ctx *buildScalarCtx, scalar opt.ScalarExpr,
) (tree.TypedExpr, error) {
	return b.indexedVar(ctx, b.mem.Metadata(), *scalar.Private().(*opt.ColumnID))
}

func (b *Builder) indexedVar(
	ctx *buildScalarCtx, md *opt.Metadata, colID opt.ColumnID,
) (tree.TypedExpr, error) {
	idx, ok := ctx.ivarMap.Get(colID)
	if !ok {
		return nil, errors.AssertionFailedf("cannot map variable %d to an indexed var", redact.Safe(colID))
	}
	return ctx.ivh.IndexedVarWithType(idx, md.ColumnMeta(colID).Type), nil
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

	case opt.IsTupleNullOp:
		expr, err := b.buildScalar(ctx, scalar.Child(0).(opt.ScalarExpr))
		if err != nil {
			return nil, err
		}
		return tree.NewTypedIsNullExpr(expr), nil

	case opt.IsTupleNotNullOp:
		expr, err := b.buildScalar(ctx, scalar.Child(0).(opt.ScalarExpr))
		if err != nil {
			return nil, err
		}
		return tree.NewTypedIsNotNullExpr(expr), nil

	default:
		return nil, errors.AssertionFailedf("invalid op %s", redact.Safe(scalar.Op()))
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

	// When the operator is an IsOp, the right is NULL, and the left is not a
	// tuple, return the unary tree.IsNullExpr.
	if scalar.Op() == opt.IsOp && right == tree.DNull && left.ResolvedType().Family() != types.TupleFamily {
		return tree.NewTypedIsNullExpr(left), nil
	}

	// When the operator is an IsNotOp, the right is NULL, and the left is not a
	// tuple, return the unary tree.IsNotNullExpr.
	if scalar.Op() == opt.IsNotOp && right == tree.DNull && left.ResolvedType().Family() != types.TupleFamily {
		return tree.NewTypedIsNotNullExpr(left), nil
	}

	operator := opt.ComparisonOpReverseMap[scalar.Op()]
	return tree.NewTypedComparisonExpr(treecmp.MakeComparisonOperator(operator), left, right), nil
}

func (b *Builder) buildUnary(ctx *buildScalarCtx, scalar opt.ScalarExpr) (tree.TypedExpr, error) {
	input, err := b.buildScalar(ctx, scalar.Child(0).(opt.ScalarExpr))
	if err != nil {
		return nil, err
	}
	operator := opt.UnaryOpReverseMap[scalar.Op()]
	return tree.NewTypedUnaryExpr(tree.MakeUnaryOperator(operator), input, scalar.DataType()), nil
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
	return tree.NewTypedBinaryExpr(treebin.MakeBinaryOperator(operator), left, right, scalar.DataType()), nil
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
	funcRef, err := b.wrapFunction(fn.Name)
	if err != nil {
		return nil, err
	}
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
	whensVals := make([]tree.When, len(cas.Whens))
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
		whensVals[i] = tree.When{Cond: cond, Val: val}
		whens[i] = &whensVals[i]
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
	return tree.NewTypedCastExpr(input, cast.Typ), nil
}

// buildAssignmentCast builds an AssignmentCastExpr with input i and type T into
// a built-in function call crdb_internal.assignment_cast(i, NULL::T).
func (b *Builder) buildAssignmentCast(
	ctx *buildScalarCtx, scalar opt.ScalarExpr,
) (tree.TypedExpr, error) {
	cast := scalar.(*memo.AssignmentCastExpr)
	input, err := b.buildScalar(ctx, cast.Input)
	if err != nil {
		return nil, err
	}
	if cast.Typ.Family() == types.TupleFamily {
		// TODO(radu): casts to Tuple are not supported (they can't be
		// serialized for distsql). This should only happen when the input is
		// always NULL so the expression should still be valid without the cast
		// (though there could be cornercases where the type does matter).
		return input, nil
	}

	const fnName = "crdb_internal.assignment_cast"
	funcRef, err := b.wrapBuiltinFunction(fnName)
	if err != nil {
		return nil, err
	}
	props, overloads := builtinsregistry.GetBuiltinProperties(fnName)
	return tree.NewTypedFuncExpr(
		funcRef,
		0, /* aggQualifier */
		tree.TypedExprs{input, tree.NewTypedCastExpr(tree.DNull, cast.Typ)},
		nil, /* filter */
		nil, /* windowDef */
		cast.Typ,
		props,
		&overloads[0],
	), nil
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
	// Find a label if there is one. It's OK if there isn't.
	lbl := ""
	if childTyp.TupleLabels() != nil {
		lbl = childTyp.TupleLabels()[colIdx]
	}
	return tree.NewTypedColumnAccessExpr(input, tree.Name(lbl), colIdx), nil
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
	return tree.NewTypedComparisonExprWithSubOp(
		treecmp.MakeComparisonOperator(treecmp.Any),
		treecmp.MakeComparisonOperator(cmp),
		left,
		right,
	), nil
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
		return nil, errors.AssertionFailedf("input to ArrayFlatten should be uncorrelated")
	}

	if b.planLazySubqueries {
		// The NormalizeArrayFlattenToAgg rule should have converted an
		// ArrayFlatten within a UDF into an aggregation.
		// We don't yet convert an ArrayFlatten within a correlated subquery
		// into an aggregation, so we return a decorrelation error.
		// TODO(mgartner): Build an ArrayFlatten within a correlated subquery as
		// a Routine, or apply NormalizeArrayFlattenToAgg to all ArrayFlattens.
		return nil, b.decorrelationError()
	}

	root, _, err := b.buildRelational(af.Input)
	if err != nil {
		return nil, err
	}

	typ := b.mem.Metadata().ColumnMeta(af.RequestedCol).Type
	e := b.addSubquery(
		exec.SubqueryAllRows, typ, root.root, af.OriginalExpr,
		int64(af.Input.Relational().Statistics().RowCountIfAvailable()),
	)

	return tree.NewTypedArrayFlattenExpr(e), nil
}

func (b *Builder) buildIfErr(ctx *buildScalarCtx, scalar opt.ScalarExpr) (tree.TypedExpr, error) {
	ifErr := scalar.(*memo.IfErrExpr)
	cond, err := b.buildScalar(ctx, ifErr.Cond)
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

func (b *Builder) buildItem(ctx *buildScalarCtx, scalar opt.ScalarExpr) (tree.TypedExpr, error) {
	return b.buildScalar(ctx, scalar.Child(0).(opt.ScalarExpr))
}

func (b *Builder) buildAny(ctx *buildScalarCtx, scalar opt.ScalarExpr) (tree.TypedExpr, error) {
	any := scalar.(*memo.AnyExpr)
	// We cannot execute correlated subqueries.
	// TODO(mgartner): Plan correlated ANY subqueries using tree.RoutineExpr.
	// See buildSubquery.
	if !any.Input.Relational().OuterCols.Empty() {
		return nil, b.decorrelationError()
	}

	if b.planLazySubqueries {
		// We cannot currently plan uncorrelated ANY subqueries as
		// lazily-evaluated routines.
		return nil, b.decorrelationError()
	}

	// Build the execution plan for the input subquery.
	plan, planCols, err := b.buildRelational(any.Input)
	if err != nil {
		return nil, err
	}

	// Construct tuple type of columns in the row.
	contents := make([]*types.T, planCols.MaxOrd()+1)
	planCols.ForEach(func(col opt.ColumnID, ord int) {
		contents[ord] = b.mem.Metadata().ColumnMeta(col).Type
	})
	typs := types.MakeTuple(contents)
	subqueryExpr := b.addSubquery(
		exec.SubqueryAnyRows, typs, plan.root, any.OriginalExpr,
		int64(any.Input.Relational().Statistics().RowCountIfAvailable()),
	)

	// Build the scalar value that is compared against each row.
	scalarExpr, err := b.buildScalar(ctx, any.Scalar)
	if err != nil {
		return nil, err
	}

	cmp := opt.ComparisonOpReverseMap[any.Cmp]
	return tree.NewTypedComparisonExprWithSubOp(
		treecmp.MakeComparisonOperator(treecmp.Any),
		treecmp.MakeComparisonOperator(cmp),
		scalarExpr,
		subqueryExpr,
	), nil
}

func (b *Builder) buildExistsSubquery(
	ctx *buildScalarCtx, scalar opt.ScalarExpr,
) (tree.TypedExpr, error) {
	exists := scalar.(*memo.ExistsExpr)
	input := exists.Input

	// Build correlated EXISTS subqueries as lazily-evaluated routines.
	//
	// Routines do not have a special mode for existential subqueries, like the
	// legacy, eager-evaluation subquery machinery does, so we must transform
	// the Exists expression. The transformation is modelled after the
	// ConvertUncorrelatedExistsToCoalesceSubquery normalization rule. The
	// transformation is effectively:
	//
	//   EXISTS (<input>)
	//   =>
	//   COALESCE((SELECT true FROM (<input>) LIMIT 1), false)
	//
	// We don't implement this as a normalization rule for correlated subqueries
	// because the transformation would prevent decorrelation rules from turning
	// the Exists expression into a join, if it is possible. Marking the rule as
	// LowPriority would not be sufficient because the rule would operate on the
	// Exists scalar expression, while the decorrelation rules operate on
	// relational expressions that contain Exists expresions. The Exists would
	// always be converted to a Coalesce before the decorrelation rules can
	// match.
	if outerCols := input.Relational().OuterCols; !outerCols.Empty() {
		// Routines do not yet support mutations.
		// TODO(mgartner): Lift this restriction once routines support
		// mutations.
		if input.Relational().CanMutate {
			return nil, b.decorrelationMutationError()
		}

		// The outer columns of the subquery become the parameters of the
		// routine.
		params := outerCols.ToList()

		// The outer columns of the subquery, as indexed columns, are the
		// arguments of the routine.
		args := make(tree.TypedExprs, len(params))
		for i := range args {
			indexedVar, err := b.indexedVar(ctx, b.mem.Metadata(), params[i])
			if err != nil {
				return nil, err
			}
			args[i] = indexedVar
		}

		// Create a single-element RelListExpr representing the subquery.
		existsCol := exists.LazyEvalProjectionCol
		aliasedCol := opt.AliasedColumn{
			Alias: b.mem.Metadata().ColumnMeta(existsCol).Alias,
			ID:    existsCol,
		}
		stmts := []memo.RelExpr{input}
		stmtProps := []*physical.Required{{Presentation: physical.Presentation{aliasedCol}}}

		// Create an wrapRootExprFn that wraps input in a Limit and a Project.
		wrapRootExpr := func(f *norm.Factory, e memo.RelExpr) opt.Expr {
			return f.ConstructProject(
				f.ConstructLimit(
					e,
					f.ConstructConst(tree.NewDInt(tree.DInt(1)), types.Int),
					props.OrderingChoice{},
				),
				memo.ProjectionsExpr{f.ConstructProjectionsItem(memo.TrueSingleton, existsCol)},
				opt.ColSet{}, /* passthrough */
			)
		}

		// Create a plan generator that can plan the single statement
		// representing the subquery, and wrap the routine in a COALESCE.
		planGen := b.buildRoutinePlanGenerator(
			params,
			stmts,
			stmtProps,
			nil,  /* stmtStr */
			true, /* allowOuterWithRefs */
			wrapRootExpr,
		)
		return tree.NewTypedCoalesceExpr(tree.TypedExprs{
			tree.NewTypedRoutineExpr(
				"exists",
				args,
				planGen,
				types.Bool,
				false, /* enableStepping */
				true,  /* calledOnNullInput */
				false, /* multiColOutput */
				false, /* generator */
				false, /* tailCall */
				false, /* procedure */
				false, /* triggerFunc */
				false, /* blockStart */
				nil,   /* blockState */
				nil,   /* cursorDeclaration */
			),
			tree.DBoolFalse,
		}, types.Bool), nil
	}

	if b.planLazySubqueries {
		// We cannot currently plan uncorrelated Exists subqueries as
		// lazily-evaluated routines. However, this path should never be
		// executed because the ConvertUncorrelatedExistsToCoalesceSubquery rule
		// converts all uncorrelated Exists into Coalesce+Subquery expressions.
		return nil, b.decorrelationError()
	}

	// Build the execution plan for the subquery. Note that the subquery could
	// have subqueries of its own which are added to b.subqueries.
	//
	// TODO(mgartner): This path should never be executed because the
	// ConvertUncorrelatedExistsToCoalesceSubquery converts all uncorrelated
	// Exists with Coalesce+Subquery expressions. Remove this and the execution
	// support for the Exists mode.
	plan, _, err := b.buildRelational(exists.Input)
	if err != nil {
		return nil, err
	}

	return b.addSubquery(
		exec.SubqueryExists, types.Bool, plan.root, exists.OriginalExpr,
		int64(exists.Input.Relational().Statistics().RowCountIfAvailable()),
	), nil
}

func (b *Builder) buildSubquery(
	ctx *buildScalarCtx, scalar opt.ScalarExpr,
) (tree.TypedExpr, error) {
	subquery := scalar.(*memo.SubqueryExpr)
	input := subquery.Input

	if b.evalCtx.SessionData().EnforceHomeRegion && b.IsANSIDML {
		inputDistributionProvidedPhysical := input.ProvidedPhysical()
		if homeRegion, ok := inputDistributionProvidedPhysical.Distribution.GetSingleRegion(); ok {
			if gatewayRegion, ok := b.evalCtx.GetLocalRegion(); ok {
				if homeRegion != gatewayRegion {
					return nil, pgerror.Newf(pgcode.QueryNotRunningInHomeRegion,
						`%s. Try running the query from region '%s'. %s`,
						execinfra.QueryNotRunningInHomeRegionMessagePrefix,
						homeRegion,
						sqlerrors.EnforceHomeRegionFurtherInfo,
					)
				}
			}
		} else {
			return nil, pgerror.Newf(pgcode.QueryHasNoHomeRegion,
				"Query has no home region. Try adding a LIMIT clause. %s",
				sqlerrors.EnforceHomeRegionFurtherInfo)
		}
	}

	// TODO(radu): for now we only support the trivial projection.
	cols := input.Relational().OutputCols
	if cols.Len() != 1 {
		return nil, errors.Errorf("subquery input with multiple columns")
	}

	// Build correlated subqueries as lazily-evaluated routines.
	if outerCols := input.Relational().OuterCols; !outerCols.Empty() {
		// Routines do not yet support mutations.
		// TODO(mgartner): Lift this restriction once routines support
		// mutations.
		if input.Relational().CanMutate {
			return nil, b.decorrelationMutationError()
		}

		// The outer columns of the subquery become the parameters of the
		// routine.
		params := outerCols.ToList()

		// The outer columns of the subquery, as indexed columns, are the
		// arguments of the routine.
		// The arguments are indexed variables representing the outer columns.
		args := make(tree.TypedExprs, len(params))
		for i := range args {
			indexedVar, err := b.indexedVar(ctx, b.mem.Metadata(), params[i])
			if err != nil {
				return nil, err
			}
			args[i] = indexedVar
		}

		// Create a single-element RelListExpr representing the subquery.
		outputCol := input.Relational().OutputCols.SingleColumn()
		aliasedCol := opt.AliasedColumn{
			Alias: b.mem.Metadata().ColumnMeta(outputCol).Alias,
			ID:    outputCol,
		}
		stmts := []memo.RelExpr{input}
		stmtProps := []*physical.Required{{Presentation: physical.Presentation{aliasedCol}}}

		// Create a tree.RoutinePlanFn that can plan the single statement
		// representing the subquery.
		planGen := b.buildRoutinePlanGenerator(
			params,
			stmts,
			stmtProps,
			nil,  /* stmtStr */
			true, /* allowOuterWithRefs */
			nil,  /* wrapRootExpr */
		)
		_, tailCall := b.tailCalls[subquery]
		return tree.NewTypedRoutineExpr(
			"subquery",
			args,
			planGen,
			subquery.Typ,
			false, /* enableStepping */
			true,  /* calledOnNullInput */
			false, /* multiColOutput */
			false, /* generator */
			tailCall,
			false, /* procedure */
			false, /* triggerFunc */
			false, /* blockStart */
			nil,   /* blockState */
			nil,   /* cursorDeclaration */
		), nil
	}

	// Build lazily-evaluated, uncorrelated subqueries as routines.
	if b.planLazySubqueries {
		// Note: We reuse the optimizer and memo from the original expression
		// because we don't need to optimize the subquery input any further.
		// It's already been fully optimized because it is uncorrelated and has
		// no outer columns.
		inputRowCount := int64(input.Relational().Statistics().RowCountIfAvailable())
		withExprs := make([]builtWithExpr, len(b.withExprs))
		copy(withExprs, b.withExprs)
		planGen := func(
			ctx context.Context, ref tree.RoutineExecFactory, args tree.Datums, fn tree.RoutinePlanGeneratedFunc,
		) error {
			// Analyze the input of the subquery to find tail calls, which will allow
			// nested routines (including lazy subqueries) to be executed in the same
			// context as this subquery.
			tailCalls := make(map[opt.ScalarExpr]struct{})
			memo.ExtractTailCalls(input, tailCalls)

			ef := ref.(exec.Factory)
			eb := New(ctx, ef, b.optimizer, b.mem, b.catalog, input, b.semaCtx, b.evalCtx, false /* allowAutoCommit */, b.IsANSIDML)
			eb.withExprs = withExprs
			eb.disableTelemetry = true
			eb.planLazySubqueries = true
			eb.tailCalls = tailCalls
			ePlan, _, err := eb.buildRelational(input)
			if err != nil {
				return err
			}
			for i := range eb.subqueries {
				if eb.subqueries[i].Mode != exec.SubqueryDiscardAllRows {
					return expectedLazyRoutineError("subquery")
				}
			}
			if len(eb.cascades) > 0 {
				return expectedLazyRoutineError("cascade")
			}
			if len(eb.triggers) > 0 {
				return expectedLazyRoutineError("trigger")
			}
			if len(eb.checks) > 0 {
				return expectedLazyRoutineError("check")
			}
			plan, err := b.factory.ConstructPlan(
				ePlan.root, eb.subqueries, eb.cascades, eb.triggers, eb.checks, inputRowCount, eb.flags,
			)
			if err != nil {
				return err
			}
			err = fn(plan, "" /* stmtForDistSQLDiagram */, true /* isFinalPlan */)
			if err != nil {
				return err
			}
			return nil
		}
		_, tailCall := b.tailCalls[subquery]
		return tree.NewTypedRoutineExpr(
			"subquery",
			nil, /* args */
			planGen,
			subquery.Typ,
			false, /* enableStepping */
			true,  /* calledOnNullInput */
			false, /* multiColOutput */
			false, /* generator */
			tailCall,
			false, /* procedure */
			false, /* triggerFunc */
			false, /* blockStart */
			nil,   /* blockState */
			nil,   /* cursorDeclaration */
		), nil
	}

	// Build the execution plan for the subquery. Note that the subquery could
	// have subqueries of its own which are added to b.subqueries.
	plan, _, err := b.buildRelational(input)
	if err != nil {
		return nil, err
	}

	// Build a subquery that is eagerly evaluated before the main query.
	return b.addSubquery(
		exec.SubqueryOneRow, subquery.Typ, plan.root, subquery.OriginalExpr,
		int64(input.Relational().Statistics().RowCountIfAvailable()),
	), nil
}

// addSubquery adds an entry to b.subqueries and creates a tree.Subquery
// expression node associated with it.
func (b *Builder) addSubquery(
	mode exec.SubqueryMode, typ *types.T, root exec.Node, originalExpr *tree.Subquery, rowCount int64,
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
		RowCount: rowCount,
	})
	// Associate the tree.Subquery expression node with this subquery
	// by index (1-based).
	exprNode.Idx = len(b.subqueries)
	return exprNode
}

// buildUDF builds a UDFCall expression into a typed expression that can be
// evaluated.
func (b *Builder) buildUDF(ctx *buildScalarCtx, scalar opt.ScalarExpr) (tree.TypedExpr, error) {
	udf := scalar.(*memo.UDFCallExpr)
	if udf.Def == nil {
		return nil, errors.AssertionFailedf("expected non-nil UDF definition")
	}

	// Build the argument expressions.
	args, err := b.buildRoutineArgs(ctx, udf.Args)
	if err != nil {
		return nil, err
	}

	for _, s := range udf.Def.Body {
		if s.Relational().CanMutate {
			b.setMutationFlags(s)
		}
	}

	blockState := udf.Def.BlockState
	if blockState != nil {
		blockState.VariableCount = len(udf.Def.Params)
		b.initRoutineExceptionHandler(blockState, udf.Def.ExceptionBlock)
	}

	// Execution expects there to be more than one body statement if a cursor is
	// opened.
	if udf.Def.CursorDeclaration != nil && len(udf.Def.Body) <= 1 {
		panic(errors.AssertionFailedf(
			"expected more than one body statement for a routine that opens a cursor",
		))
	}

	// Create a tree.RoutinePlanFn that can plan the statements in the UDF body.
	// TODO(mgartner): Add support for WITH expressions inside UDF bodies.
	planGen := b.buildRoutinePlanGenerator(
		udf.Def.Params,
		udf.Def.Body,
		udf.Def.BodyProps,
		udf.Def.BodyStmts,
		false, /* allowOuterWithRefs */
		nil,   /* wrapRootExpr */
	)

	// Enable stepping for volatile functions so that statements within the UDF
	// see mutations made by the invoking statement and by previously executed
	// statements.
	enableStepping := udf.Def.Volatility == volatility.Volatile

	// The calling routine, if any, will have already determined whether this
	// routine is in tail-call position.
	_, tailCall := b.tailCalls[udf]

	return tree.NewTypedRoutineExpr(
		udf.Def.Name,
		args,
		planGen,
		udf.Typ,
		enableStepping,
		udf.Def.CalledOnNullInput,
		udf.Def.MultiColDataSource,
		udf.Def.SetReturning,
		tailCall,
		false, /* procedure */
		udf.Def.TriggerFunc,
		udf.Def.BlockStart,
		blockState,
		udf.Def.CursorDeclaration,
	), nil
}

func (b *Builder) buildRoutineArgs(
	ctx *buildScalarCtx, routineArgs memo.ScalarListExpr,
) (args tree.TypedExprs, err error) {
	if len(routineArgs) == 0 {
		return nil, nil
	}
	args = make(tree.TypedExprs, len(routineArgs))
	for i := range routineArgs {
		args[i], err = b.buildScalar(ctx, routineArgs[i])
		if err != nil {
			return nil, err
		}
	}
	return args, nil
}

// initRoutineExceptionHandler initializes the exception handler (if any) for
// the shared BlockState of a group of sub-routines within a PLpgSQL block.
func (b *Builder) initRoutineExceptionHandler(
	blockState *tree.BlockState, exceptionBlock *memo.ExceptionBlock,
) {
	if exceptionBlock == nil {
		return
	}
	exceptionHandler := &tree.RoutineExceptionHandler{
		Codes:   exceptionBlock.Codes,
		Actions: make([]*tree.RoutineExpr, len(exceptionBlock.Actions)),
	}
	for i, action := range exceptionBlock.Actions {
		actionPlanGen := b.buildRoutinePlanGenerator(
			action.Params,
			action.Body,
			action.BodyProps,
			action.BodyStmts,
			false, /* allowOuterWithRefs */
			nil,   /* wrapRootExpr */
		)
		// Build a routine with no arguments for the exception handler. The actual
		// arguments will be supplied when (if) the handler is invoked.
		exceptionHandler.Actions[i] = tree.NewTypedRoutineExpr(
			action.Name,
			nil, /* args */
			actionPlanGen,
			action.Typ,
			true, /* enableStepping */
			action.CalledOnNullInput,
			action.MultiColDataSource,
			action.SetReturning,
			false, /* tailCall */
			false, /* procedure */
			false, /* triggerFunc */
			false, /* blockStart */
			nil,   /* blockState */
			nil,   /* cursorDeclaration */
		)
	}
	blockState.ExceptionHandler = exceptionHandler
}

type wrapRootExprFn func(f *norm.Factory, e memo.RelExpr) opt.Expr

// buildRoutinePlanGenerator returns a tree.RoutinePlanFn that can plan the
// statements in a routine that has one or more arguments.
//
// The returned tree.RoutinePlanFn copies one of the statements into a new memo
// for re-optimization each time it is called. By default, parameter references
// are replaced with constant argument values when the plan function is called.
// If allowOuterWithRefs is true, then With binding are copied to the new memo
// so that WithScans within a statement can be planned and executed.
// wrapRootExpr allows the root expression of all statements to be replaced with
// an arbitrary expression.
func (b *Builder) buildRoutinePlanGenerator(
	params opt.ColList,
	stmts []memo.RelExpr,
	stmtProps []*physical.Required,
	stmtStr []string,
	allowOuterWithRefs bool,
	wrapRootExpr wrapRootExprFn,
) tree.RoutinePlanGenerator {
	// argOrd returns the ordinal of the argument within the arguments list that
	// can be substituted for each reference to the given function parameter
	// column. If the given column does not represent a function parameter,
	// ok=false is returned.
	argOrd := func(col opt.ColumnID) (ord int, ok bool) {
		for i, param := range params {
			if col == param {
				return i, true
			}
		}
		return 0, false
	}

	// We will pre-populate the withExprs of the new execbuilder.
	var withExprs []builtWithExpr
	if allowOuterWithRefs {
		withExprs = make([]builtWithExpr, len(b.withExprs))
		copy(withExprs, b.withExprs)
	}

	// Plan the statements in a separate memo. We use an exec.Factory passed to
	// the closure rather than b.factory to support executing plans that are
	// generated with explain.Factory.
	//
	// Note: the ref argument has type tree.RoutineExecFactory rather than
	// exec.Factory to avoid import cycles.
	//
	// Note: we put o outside of the function so we allocate it only once.
	var o xform.Optimizer
	originalMemo := b.mem
	planGen := func(
		ctx context.Context, ref tree.RoutineExecFactory, args tree.Datums, fn tree.RoutinePlanGeneratedFunc,
	) (err error) {
		defer func() {
			if r := recover(); r != nil {
				// This code allows us to propagate internal errors without
				// having to add error checks everywhere throughout the code.
				// This is only possible because the code does not update shared
				// state and does not manipulate locks.
				//
				// This is the same panic-catching logic that exists in
				// o.Optimize() below. It's required here because it's possible
				// for factory functions to panic below, like
				// CopyAndReplaceDefault.
				if ok, e := errorutil.ShouldCatch(r); ok {
					err = e
					log.VEventf(ctx, 1, "%v", err)
				} else {
					// Other panic objects can't be considered "safe" and thus
					// are propagated as crashes that terminate the session.
					panic(r)
				}
			}
		}()

		for i := range stmts {
			stmt := stmts[i]
			props := stmtProps[i]
			o.Init(ctx, b.evalCtx, b.catalog)
			f := o.Factory()

			// Copy the expression into a new memo. Replace parameter references
			// with argument datums.
			var replaceFn norm.ReplaceFunc
			replaceFn = func(e opt.Expr) opt.Expr {
				switch t := e.(type) {
				case *memo.VariableExpr:
					if ord, ok := argOrd(t.Col); ok {
						return f.ConstructConstVal(args[ord], t.Typ)
					}

				case *memo.WithScanExpr:
					// Allow referring to "outer" With expressions, if
					// allowOuterWithRefs is true. The bound expressions are not
					// part of this Memo, but they are used only for their
					// relational properties, which should be valid.
					//
					// We must add all With expressions to the metadata even if they
					// aren't referred to directly because they might be referred to
					// transitively through other With expressions. For example, if
					// stmt refers to With expression &1, and &1 refers to With
					// expression &2, we must include &2 in the metadata so that its
					// relational properties are available. See #87733.
					//
					// We lazily add these With expressions to the metadata here
					// because the call to Factory.CopyAndReplace below clears With
					// expressions in the metadata.
					if allowOuterWithRefs {
						b.mem.Metadata().ForEachWithBinding(func(id opt.WithID, expr opt.Expr) {
							// Make sure to check for an existing With binding, since we may
							// have already rewritten the bound expression and added it to the
							// new memo if the associated WithExpr is part of the routine.
							if !f.Metadata().HasWithBinding(id) {
								f.Metadata().AddWithBinding(id, expr)
							}
						})
					}
					// Fall through.
				}

				return f.CopyAndReplaceDefault(e, replaceFn)
			}
			f.CopyAndReplace(originalMemo, stmt, props, replaceFn)

			if wrapRootExpr != nil {
				wrapped := wrapRootExpr(f, f.Memo().RootExpr().(memo.RelExpr)).(memo.RelExpr)
				f.Memo().SetRoot(wrapped, props)
			}

			// Optimize the memo.
			optimizedExpr, err := o.Optimize()
			if err != nil {
				return err
			}

			// Identify nested routines that are in tail-call position, and cache them
			// in the Builder. When a nested routine is evaluated, this information
			// may be used to enable tail-call optimization.
			isFinalPlan := i == len(stmts)-1
			var tailCalls map[opt.ScalarExpr]struct{}
			if isFinalPlan {
				tailCalls = make(map[opt.ScalarExpr]struct{})
				memo.ExtractTailCalls(optimizedExpr, tailCalls)
			}

			// Build the memo into a plan.
			ef := ref.(exec.Factory)
			eb := New(ctx, ef, &o, f.Memo(), b.catalog, optimizedExpr, b.semaCtx, b.evalCtx, false /* allowAutoCommit */, b.IsANSIDML)
			eb.withExprs = withExprs
			eb.disableTelemetry = true
			eb.planLazySubqueries = true
			eb.tailCalls = tailCalls
			plan, err := eb.Build()
			if err != nil {
				if errors.IsAssertionFailure(err) {
					// Enhance the error with the EXPLAIN (OPT, VERBOSE) of the
					// inner expression.
					fmtFlags := memo.ExprFmtHideQualifications | memo.ExprFmtHideScalars |
						memo.ExprFmtHideTypes | memo.ExprFmtHideFastPathChecks
					explainOpt := o.FormatExpr(optimizedExpr, fmtFlags, false /* redactableValues */)
					err = errors.WithDetailf(err, "routineExpr:\n%s", explainOpt)
				}
				return err
			}
			for j := range eb.subqueries {
				if eb.subqueries[j].Mode != exec.SubqueryDiscardAllRows {
					return expectedLazyRoutineError("subquery")
				}
			}
			var stmtForDistSQLDiagram string
			if i < len(stmtStr) {
				stmtForDistSQLDiagram = stmtStr[i]
			}
			err = fn(plan, stmtForDistSQLDiagram, isFinalPlan)
			if err != nil {
				return err
			}
		}
		return nil
	}
	return planGen
}

func expectedLazyRoutineError(typ string) error {
	return errors.AssertionFailedf("expected %s to be lazily planned as a routine", typ)
}

// buildTxnControl builds a TxnControlExpr into a typed expression that can be
// evaluated.
func (b *Builder) buildTxnControl(
	ctx *buildScalarCtx, scalar opt.ScalarExpr,
) (tree.TypedExpr, error) {
	txnExpr := scalar.(*memo.TxnControlExpr)
	// Build the argument expressions.
	args, err := b.buildRoutineArgs(ctx, txnExpr.Args)
	if err != nil {
		return nil, err
	}
	gen := func(
		ctx context.Context, evalArgs tree.Datums,
	) (con tree.StoredProcContinuation, err error) {
		defer func() {
			if r := recover(); r != nil {
				// This code allows us to propagate internal errors without
				// having to add error checks everywhere throughout the code.
				// This is only possible because the code does not update shared
				// state and does not manipulate locks.
				//
				// This is the same panic-catching logic that exists in
				// o.Optimize() below. It's required here because it's possible
				// for factory functions to panic below, like
				// CopyAndReplaceDefault.
				if ok, e := errorutil.ShouldCatch(r); ok {
					err = e
					log.VEventf(ctx, 1, "%v", err)
				} else {
					// Other panic objects can't be considered "safe" and thus
					// are propagated as crashes that terminate the session.
					panic(r)
				}
			}
		}()
		// Build the plan for the "continuation" procedure that will resume
		// execution of the parent stored procedure in a new transaction.
		var f norm.Factory
		f.Init(ctx, b.evalCtx, b.catalog)
		f.CopyMetadataFrom(b.mem)

		// Use the evaluated arguments to construct the continuation procedure.
		memoArgs := make(memo.ScalarListExpr, len(evalArgs))
		for i := range evalArgs {
			memoArgs[i] = f.ConstructConstVal(evalArgs[i], evalArgs[i].ResolvedType())
		}
		continuationProc := f.ConstructUDFCall(memoArgs, &memo.UDFCallPrivate{Def: txnExpr.Def})
		call := f.ConstructCall(continuationProc, &memo.CallPrivate{Columns: txnExpr.OutCols})
		f.Memo().SetRoot(call, txnExpr.Props)
		return f.DetachMemo(), nil
	}
	return tree.NewTxnControlExpr(
		txnExpr.TxnOp, txnExpr.TxnModes, args, gen, txnExpr.Def.Name, txnExpr.Def.Typ,
	), nil
}
