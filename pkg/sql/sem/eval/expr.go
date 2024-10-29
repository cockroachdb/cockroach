// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package eval

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree/treecmp"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// Expr evaluates a TypedExpr into a Datum.
func Expr(ctx context.Context, evalCtx *Context, n tree.TypedExpr) (tree.Datum, error) {
	return n.Eval(ctx, (*evaluator)(evalCtx))
}

type evaluator Context

func (e *evaluator) ctx() *Context { return (*Context)(e) }

func (e *evaluator) EvalAllColumnsSelector(
	ctx context.Context, selector *tree.AllColumnsSelector,
) (tree.Datum, error) {
	return nil, errors.AssertionFailedf("unhandled type %T", selector)
}

func (e *evaluator) EvalAndExpr(ctx context.Context, expr *tree.AndExpr) (tree.Datum, error) {
	left, err := expr.Left.(tree.TypedExpr).Eval(ctx, e)
	if err != nil {
		return nil, err
	}
	if left != tree.DNull {
		if v, err := tree.GetBool(left); err != nil {
			return nil, err
		} else if !v {
			return left, nil
		}
	}
	right, err := expr.Right.(tree.TypedExpr).Eval(ctx, e)
	if err != nil {
		return nil, err
	}
	if right == tree.DNull {
		return tree.DNull, nil
	}
	if v, err := tree.GetBool(right); err != nil {
		return nil, err
	} else if !v {
		return right, nil
	}
	return left, nil
}

func (e *evaluator) EvalArray(ctx context.Context, t *tree.Array) (tree.Datum, error) {
	array, err := arrayOfType(t.ResolvedType())
	if err != nil {
		return nil, err
	}

	for _, ae := range t.Exprs {
		d, err := ae.(tree.TypedExpr).Eval(ctx, e)
		if err != nil {
			return nil, err
		}
		if err := array.Append(d); err != nil {
			return nil, err
		}
	}
	return array, nil
}

func (e *evaluator) EvalArrayFlatten(
	ctx context.Context, t *tree.ArrayFlatten,
) (tree.Datum, error) {
	array, err := arrayOfType(t.ResolvedType())
	if err != nil {
		return nil, err
	}

	d, err := t.Subquery.(tree.TypedExpr).Eval(ctx, e)
	if err != nil {
		return nil, err
	}

	tuple, ok := d.(*tree.DTuple)
	if !ok {
		return nil, errors.AssertionFailedf("array subquery result (%v) is not DTuple", d)
	}
	array.Array = tuple.D
	return array, nil
}

func (e *evaluator) EvalBinaryExpr(ctx context.Context, expr *tree.BinaryExpr) (tree.Datum, error) {
	left, err := expr.Left.(tree.TypedExpr).Eval(ctx, e)
	if err != nil {
		return nil, err
	}
	if left == tree.DNull && !expr.Op.CalledOnNullInput {
		return tree.DNull, nil
	}
	right, err := expr.Right.(tree.TypedExpr).Eval(ctx, e)
	if err != nil {
		return nil, err
	}
	if right == tree.DNull && !expr.Op.CalledOnNullInput {
		return tree.DNull, nil
	}
	res, err := expr.Op.EvalOp.Eval(ctx, e, left, right)
	if err != nil {
		return nil, err
	}
	if e.TestingKnobs.AssertBinaryExprReturnTypes {
		if err := ensureExpectedType(expr.Op.ReturnType, res); err != nil {
			return nil, errors.NewAssertionErrorWithWrappedErrf(err,
				"binary op %q", expr)
		}
	}
	return res, err
}

func (e *evaluator) EvalCaseExpr(ctx context.Context, expr *tree.CaseExpr) (tree.Datum, error) {
	if expr.Expr != nil {
		// CASE <val> WHEN <expr> THEN ...
		//
		// For each "when" expression we compare for equality to <val>.
		val, err := expr.Expr.(tree.TypedExpr).Eval(ctx, e)
		if err != nil {
			return nil, err
		}

		for _, when := range expr.Whens {
			arg, err := when.Cond.(tree.TypedExpr).Eval(ctx, e)
			if err != nil {
				return nil, err
			}
			d, err := evalComparison(ctx, e.ctx(), treecmp.MakeComparisonOperator(treecmp.EQ), val, arg)
			if err != nil {
				return nil, err
			}
			if db, err := tree.GetBool(d); err != nil {
				return nil, err
			} else if db {
				return when.Val.(tree.TypedExpr).Eval(ctx, e)
			}
		}
	} else {
		// CASE WHEN <bool-expr> THEN ...
		for _, when := range expr.Whens {
			d, err := when.Cond.(tree.TypedExpr).Eval(ctx, e)
			if err != nil {
				return nil, err
			}
			if db, err := tree.GetBool(d); err != nil {
				return nil, err
			} else if db {
				return when.Val.(tree.TypedExpr).Eval(ctx, e)
			}
		}
	}

	if expr.Else != nil {
		return expr.Else.(tree.TypedExpr).Eval(ctx, e)
	}
	return tree.DNull, nil
}

func (e *evaluator) EvalCastExpr(ctx context.Context, expr *tree.CastExpr) (tree.Datum, error) {
	d, err := expr.Expr.(tree.TypedExpr).Eval(ctx, e)
	if err != nil {
		return nil, err
	}

	// NULL cast to anything is NULL.
	if d == tree.DNull {
		return d, nil
	}
	d = UnwrapDatum(ctx, e.ctx(), d)
	return PerformCast(ctx, e.ctx(), d, expr.ResolvedType())
}

func (e *evaluator) EvalCoalesceExpr(
	ctx context.Context, expr *tree.CoalesceExpr,
) (tree.Datum, error) {
	for _, ex := range expr.Exprs {
		d, err := ex.(tree.TypedExpr).Eval(ctx, e)
		if err != nil {
			return nil, err
		}
		if d != tree.DNull {
			return d, nil
		}
	}
	return tree.DNull, nil
}

func (e *evaluator) EvalCollateExpr(
	ctx context.Context, expr *tree.CollateExpr,
) (tree.Datum, error) {
	d, err := expr.Expr.(tree.TypedExpr).Eval(ctx, e)
	if err != nil {
		return nil, err
	}
	unwrapped := UnwrapDatum(ctx, e.ctx(), d)

	// buildCollated is a recursive helper function to handle evaluating COLLATE
	// on arrays.
	var buildCollated func(tree.Datum) (tree.Datum, error)
	buildCollated = func(datum tree.Datum) (tree.Datum, error) {
		if datum == tree.DNull {
			return tree.DNull, nil
		}
		switch d := datum.(type) {
		case *tree.DString:
			return tree.NewDCollatedString(string(*d), expr.Locale, &e.CollationEnv)
		case *tree.DCollatedString:
			return tree.NewDCollatedString(d.Contents, expr.Locale, &e.CollationEnv)
		case *tree.DArray:
			a := tree.NewDArray(types.MakeCollatedType(d.ParamTyp, expr.Locale))
			a.Array = make(tree.Datums, 0, len(d.Array))
			for _, elem := range d.Array {
				collatedElem, err := buildCollated(elem)
				if err != nil {
					return nil, err
				}
				if err := a.Append(collatedElem); err != nil {
					return nil, err
				}
			}
			return a, nil
		}
		return nil, pgerror.Newf(pgcode.DatatypeMismatch, "incompatible type for COLLATE: %s", datum)
	}
	return buildCollated(unwrapped)
}

func (e *evaluator) EvalColumnAccessExpr(
	ctx context.Context, expr *tree.ColumnAccessExpr,
) (tree.Datum, error) {
	d, err := expr.Expr.(tree.TypedExpr).Eval(ctx, e)
	if err != nil {
		return nil, err
	}
	if d == tree.DNull {
		return d, nil
	}
	return d.(*tree.DTuple).D[expr.ColIndex], nil
}

func (e *evaluator) EvalColumnItem(ctx context.Context, expr *tree.ColumnItem) (tree.Datum, error) {
	return nil, errors.AssertionFailedf("unhandled type %T", expr)
}

func (e *evaluator) EvalComparisonExpr(
	ctx context.Context, expr *tree.ComparisonExpr,
) (tree.Datum, error) {
	left, err := expr.Left.(tree.TypedExpr).Eval(ctx, e)
	if err != nil {
		return nil, err
	}
	right, err := expr.Right.(tree.TypedExpr).Eval(ctx, e)
	if err != nil {
		return nil, err
	}

	op := expr.Operator
	if op.Symbol.HasSubOperator() {
		return ComparisonExprWithSubOperator(ctx, e.ctx(), expr, left, right)
	}

	_, newLeft, newRight, _, not := tree.FoldComparisonExprWithDatums(op, left, right)
	if !expr.Op.CalledOnNullInput && (newLeft == tree.DNull || newRight == tree.DNull) {
		return tree.DNull, nil
	}
	d, err := expr.Op.EvalOp.Eval(ctx, e, newLeft, newRight)
	if d == tree.DNull || err != nil {
		return d, err
	}
	b, ok := d.(*tree.DBool)
	if !ok {
		return nil, errors.AssertionFailedf("%v is %T and not *DBool", d, d)
	}
	return tree.MakeDBool(*b != tree.DBool(not)), nil
}

func (e *evaluator) EvalIndexedVar(ctx context.Context, iv *tree.IndexedVar) (tree.Datum, error) {
	if e.IVarContainer == nil {
		return nil, errors.AssertionFailedf(
			"indexed var must be bound to a container before evaluation")
	}
	eivc, ok := e.IVarContainer.(IndexedVarContainer)
	if !ok {
		return nil, errors.AssertionFailedf(
			"indexed var container of type %T may not be evaluated", e.IVarContainer)
	}
	return eivc.IndexedVarEval(iv.Idx)
}

func (e *evaluator) EvalIndirectionExpr(
	ctx context.Context, expr *tree.IndirectionExpr,
) (tree.Datum, error) {
	var subscriptIdx int

	d, err := expr.Expr.(tree.TypedExpr).Eval(ctx, e)
	if err != nil {
		return nil, err
	}
	if d == tree.DNull {
		return d, nil
	}

	switch d.ResolvedType().Family() {
	case types.ArrayFamily:
		for i, t := range expr.Indirection {
			if t.Slice || i > 0 {
				return nil, errors.AssertionFailedf("unsupported feature should have been rejected during planning")
			}

			beginDatum, err := t.Begin.(tree.TypedExpr).Eval(ctx, e)
			if err != nil {
				return nil, err
			}
			if beginDatum == tree.DNull {
				return tree.DNull, nil
			}
			subscriptIdx = int(tree.MustBeDInt(beginDatum))
		}

		// Index into the DArray, using 1-indexing.
		arr := tree.MustBeDArray(d)

		// VECTOR types use 0-indexing.
		if arr.FirstIndex() == 0 {
			subscriptIdx++
		}
		if subscriptIdx < 1 || subscriptIdx > arr.Len() {
			return tree.DNull, nil
		}
		return arr.Array[subscriptIdx-1], nil
	case types.JsonFamily:
		j := tree.MustBeDJSON(d)
		curr := j.JSON
		for _, t := range expr.Indirection {
			if t.Slice {
				return nil, errors.AssertionFailedf("unsupported feature should have been rejected during planning")
			}

			field, err := t.Begin.(tree.TypedExpr).Eval(ctx, e)
			if err != nil {
				return nil, err
			}
			if field == tree.DNull {
				return tree.DNull, nil
			}
			switch field.ResolvedType().Family() {
			case types.StringFamily:
				if curr, err = curr.FetchValKeyOrIdx(string(tree.MustBeDString(field))); err != nil {
					return nil, err
				}
			case types.IntFamily:
				if curr, err = curr.FetchValIdx(int(tree.MustBeDInt(field))); err != nil {
					return nil, err
				}
			default:
				return nil, errors.AssertionFailedf("unsupported feature should have been rejected during planning")
			}
			if curr == nil {
				return tree.DNull, nil
			}
		}
		return tree.NewDJSON(curr), nil
	}
	return nil, errors.AssertionFailedf("unsupported feature should have been rejected during planning")
}

func (e *evaluator) EvalDefaultVal(ctx context.Context, expr *tree.DefaultVal) (tree.Datum, error) {
	return nil, errors.AssertionFailedf("unhandled type %T", expr)
}

func (e *evaluator) EvalIsNotNullExpr(
	ctx context.Context, expr *tree.IsNotNullExpr,
) (tree.Datum, error) {
	d, err := expr.Expr.(tree.TypedExpr).Eval(ctx, e)
	if err != nil {
		return nil, err
	}
	if d == tree.DNull {
		return tree.MakeDBool(false), nil
	}
	if t, ok := d.(*tree.DTuple); ok {
		// A tuple IS NOT NULL if all elements are not NULL.
		for _, tupleDatum := range t.D {
			if tupleDatum == tree.DNull {
				return tree.MakeDBool(false), nil
			}
		}
		return tree.MakeDBool(true), nil
	}
	return tree.MakeDBool(true), nil
}

func (e *evaluator) EvalIsNullExpr(ctx context.Context, expr *tree.IsNullExpr) (tree.Datum, error) {
	d, err := expr.Expr.(tree.TypedExpr).Eval(ctx, e)
	if err != nil {
		return nil, err
	}
	if d == tree.DNull {
		return tree.MakeDBool(true), nil
	}
	if t, ok := d.(*tree.DTuple); ok {
		// A tuple IS NULL if all elements are NULL.
		for _, tupleDatum := range t.D {
			if tupleDatum != tree.DNull {
				return tree.MakeDBool(false), nil
			}
		}
		return tree.MakeDBool(true), nil
	}
	return tree.MakeDBool(false), nil
}

func (e *evaluator) EvalIsOfTypeExpr(
	ctx context.Context, expr *tree.IsOfTypeExpr,
) (tree.Datum, error) {
	d, err := expr.Expr.(tree.TypedExpr).Eval(ctx, e)
	if err != nil {
		return nil, err
	}
	datumTyp := d.ResolvedType()

	for _, t := range expr.ResolvedTypes() {
		if datumTyp.Equivalent(t) {
			return tree.MakeDBool(tree.DBool(!expr.Not)), nil
		}
	}
	return tree.MakeDBool(tree.DBool(expr.Not)), nil
}

func (e *evaluator) EvalNotExpr(ctx context.Context, expr *tree.NotExpr) (tree.Datum, error) {
	d, err := expr.Expr.(tree.TypedExpr).Eval(ctx, e)
	if err != nil {
		return nil, err
	}
	if d == tree.DNull {
		return tree.DNull, nil
	}
	got, err := tree.GetBool(d)
	if err != nil {
		return nil, err
	}
	return tree.MakeDBool(!got), nil
}

func (e *evaluator) EvalNullIfExpr(ctx context.Context, expr *tree.NullIfExpr) (tree.Datum, error) {
	expr1, err := expr.Expr1.(tree.TypedExpr).Eval(ctx, e)
	if err != nil {
		return nil, err
	}
	expr2, err := expr.Expr2.(tree.TypedExpr).Eval(ctx, e)
	if err != nil {
		return nil, err
	}
	cond, err := evalComparison(ctx, e.ctx(), treecmp.MakeComparisonOperator(treecmp.EQ), expr1, expr2)
	if err != nil {
		return nil, err
	}
	if cond == tree.DBoolTrue {
		return tree.DNull, nil
	}
	return expr1, nil
}

func (e *evaluator) EvalFuncExpr(ctx context.Context, expr *tree.FuncExpr) (tree.Datum, error) {
	fn := expr.ResolvedOverload()
	if fn.FnWithExprs != nil {
		return fn.FnWithExprs.(FnWithExprsOverload)(ctx, e.ctx(), expr.Exprs)
	}

	nullResult, args, err := e.evalFuncArgs(ctx, expr)
	if err != nil {
		return nil, err
	}
	if nullResult {
		return tree.DNull, err
	}

	if fn.Body != "" {
		// This expression evaluator cannot run functions defined with a SQL body.
		return nil, pgerror.Newf(pgcode.FeatureNotSupported, "cannot evaluate function in this context")
	}
	if fn.Fn == nil {
		// This expression evaluator cannot run functions that are not "normal"
		// builtins; that is, not aggregate or window functions.
		return nil, pgerror.Newf(pgcode.FeatureNotSupported, "cannot evaluate function in this context")
	}

	res, err := fn.Fn.(FnOverload)(ctx, e.ctx(), args)
	if err != nil {
		return nil, expr.MaybeWrapError(err)
	}
	if e.TestingKnobs.AssertFuncExprReturnTypes {
		if err := ensureExpectedType(fn.FixedReturnType(), res); err != nil {
			return nil, errors.NewAssertionErrorWithWrappedErrf(err, "function %q", expr)
		}
	}
	return res, nil
}

func (e *evaluator) evalFuncArgs(
	ctx context.Context, expr *tree.FuncExpr,
) (propagateNulls bool, args tree.Datums, _ error) {
	args = make(tree.Datums, len(expr.Exprs))
	for i, argExpr := range expr.Exprs {
		arg, err := argExpr.(tree.TypedExpr).Eval(ctx, e)
		if err != nil {
			return false, nil, err
		}
		if arg == tree.DNull && !expr.ResolvedOverload().CalledOnNullInput {
			return true, nil, nil
		}
		args[i] = arg
	}
	return false, args, nil
}

func (e *evaluator) EvalIfErrExpr(ctx context.Context, expr *tree.IfErrExpr) (tree.Datum, error) {
	cond, evalErr := expr.Cond.(tree.TypedExpr).Eval(ctx, e)
	if evalErr == nil {
		if expr.Else == nil {
			return tree.DBoolFalse, nil
		}
		return cond, nil
	}
	if expr.ErrCode != nil {
		errpat, err := expr.ErrCode.(tree.TypedExpr).Eval(ctx, e)
		if err != nil {
			return nil, err
		}
		if errpat == tree.DNull {
			return nil, evalErr
		}
		errpatStr := string(tree.MustBeDString(errpat))
		if code := pgerror.GetPGCode(evalErr); code != pgcode.MakeCode(errpatStr) {
			return nil, evalErr
		}
	}
	if expr.Else == nil {
		return tree.DBoolTrue, nil
	}
	return expr.Else.(tree.TypedExpr).Eval(ctx, e)
}

func (e *evaluator) EvalIfExpr(ctx context.Context, expr *tree.IfExpr) (tree.Datum, error) {
	cond, err := expr.Cond.(tree.TypedExpr).Eval(ctx, e)
	if err != nil {
		return nil, err
	}
	if cond == tree.DBoolTrue {
		return expr.True.(tree.TypedExpr).Eval(ctx, e)
	}
	return expr.Else.(tree.TypedExpr).Eval(ctx, e)
}

func (e *evaluator) EvalOrExpr(ctx context.Context, expr *tree.OrExpr) (tree.Datum, error) {
	left, err := expr.Left.(tree.TypedExpr).Eval(ctx, e)
	if err != nil {
		return nil, err
	}
	if left != tree.DNull {
		if got, err := tree.GetBool(left); err != nil {
			return nil, err
		} else if got {
			return left, nil
		}
	}
	right, err := expr.Right.(tree.TypedExpr).Eval(ctx, e)
	if err != nil {
		return nil, err
	}
	if right == tree.DNull {
		return tree.DNull, nil
	}
	if got, err := tree.GetBool(right); err != nil {
		return nil, err
	} else if got {
		return right, nil
	}
	if left == tree.DNull {
		return tree.DNull, nil
	}
	return tree.DBoolFalse, nil
}

func (e *evaluator) EvalParenExpr(ctx context.Context, expr *tree.ParenExpr) (tree.Datum, error) {
	return expr.Expr.(tree.TypedExpr).Eval(ctx, e)
}

func (e *evaluator) EvalPlaceholder(ctx context.Context, t *tree.Placeholder) (tree.Datum, error) {
	if !e.ctx().HasPlaceholders() {
		// While preparing a query, there will be no available placeholders. A
		// placeholder evaluates to itself at this point.
		return t, nil
	}
	ex, ok := e.Placeholders.Value(t.Idx)
	if !ok {
		return nil, tree.NewNoValueProvidedForPlaceholderErr(t.Idx)
	}
	// Placeholder expressions cannot contain other placeholders, so we do
	// not need to recurse.
	typ := e.Placeholders.Types[t.Idx]
	if typ == nil {
		// All placeholders should be typed at this point.
		return nil, errors.AssertionFailedf("missing type for placeholder %s", t)
	}
	if !ex.ResolvedType().Equivalent(typ) {
		// This happens when we overrode the placeholder's type during type
		// checking, since the placeholder's type hint didn't match the desired
		// type for the placeholder. In this case, we cast the expression to
		// the desired type.
		// TODO(jordan,mgartner): Introduce a restriction on what casts are
		// allowed here. Most likely, only implicit casts should be allowed.
		cast := tree.NewTypedCastExpr(ex, typ)
		return cast.Eval(ctx, e)
	}
	return ex.Eval(ctx, e)
}

func (e *evaluator) EvalRangeCond(ctx context.Context, cond *tree.RangeCond) (tree.Datum, error) {
	return nil, errors.AssertionFailedf("unhandled type %T", cond)
}

func (e *evaluator) EvalSubquery(ctx context.Context, subquery *tree.Subquery) (tree.Datum, error) {
	return e.Planner.EvalSubquery(subquery)
}

func (e *evaluator) EvalRoutineExpr(
	ctx context.Context, routine *tree.RoutineExpr,
) (tree.Datum, error) {
	args, err := e.evalRoutineArgs(ctx, routine.Args)
	if err != nil {
		return nil, err
	}
	return e.Planner.EvalRoutineExpr(ctx, routine, args)
}

func (e *evaluator) evalRoutineArgs(
	ctx context.Context, routineArgs tree.TypedExprs,
) (args tree.Datums, err error) {
	if len(routineArgs) > 0 {
		// Evaluate each argument expression.
		// TODO(mgartner): Use a scratch tree.Datums to avoid allocation on
		// every invocation.
		args = make(tree.Datums, len(routineArgs))
		for i := range routineArgs {
			args[i], err = routineArgs[i].Eval(ctx, e)
			if err != nil {
				return nil, err
			}
		}
	}
	return args, nil
}

func (e *evaluator) EvalTxnControlExpr(
	ctx context.Context, expr *tree.TxnControlExpr,
) (tree.Datum, error) {
	args, err := e.evalRoutineArgs(ctx, expr.Args)
	if err != nil {
		return nil, err
	}
	return e.Planner.EvalTxnControlExpr(ctx, expr, args)
}

func (e *evaluator) EvalTuple(ctx context.Context, t *tree.Tuple) (tree.Datum, error) {
	tuple := tree.NewDTupleWithLen(t.ResolvedType(), len(t.Exprs))
	for i, expr := range t.Exprs {
		d, err := expr.(tree.TypedExpr).Eval(ctx, e)
		if err != nil {
			return nil, err
		}
		tuple.D[i] = d
	}
	return tuple, nil
}

func (e *evaluator) EvalTupleStar(ctx context.Context, star *tree.TupleStar) (tree.Datum, error) {
	return nil, errors.AssertionFailedf("unhandled type %T", star)
}

func (e *evaluator) EvalTypedDummy(context.Context, *tree.TypedDummy) (tree.Datum, error) {
	return nil, errors.AssertionFailedf("should not eval typed dummy")
}

func (e *evaluator) EvalUnaryExpr(ctx context.Context, expr *tree.UnaryExpr) (tree.Datum, error) {
	d, err := expr.Expr.(tree.TypedExpr).Eval(ctx, e)
	if err != nil {
		return nil, err
	}
	if d == tree.DNull {
		return d, nil
	}
	op := expr.GetOp()
	res, err := op.EvalOp.Eval(ctx, e, d)
	if err != nil {
		return nil, err
	}
	if e.TestingKnobs.AssertUnaryExprReturnTypes {
		if err := ensureExpectedType(op.ReturnType, res); err != nil {
			return nil, errors.NewAssertionErrorWithWrappedErrf(err, "unary op %q", expr)
		}
	}
	return res, err
}

func (e *evaluator) EvalUnresolvedName(
	ctx context.Context, name *tree.UnresolvedName,
) (tree.Datum, error) {
	return nil, errors.AssertionFailedf("unhandled type %T", name)
}

func (e *evaluator) EvalUnqualifiedStar(
	ctx context.Context, star tree.UnqualifiedStar,
) (tree.Datum, error) {
	return nil, errors.AssertionFailedf("unhandled type %T", star)
}

var _ tree.ExprEvaluator = (*evaluator)(nil)
