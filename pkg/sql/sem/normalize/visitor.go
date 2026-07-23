// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package normalize

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree/treecmp"
	"github.com/cockroachdb/errors"
)

// Expr normalizes the provided expr.
func Expr(
	ctx context.Context, evalCtx *eval.Context, typedExpr tree.TypedExpr,
) (tree.TypedExpr, error) {
	v := MakeNormalizeVisitor(ctx, evalCtx)
	expr, _ := tree.WalkExpr(&v, typedExpr)
	if v.err != nil {
		return nil, v.err
	}
	return expr.(tree.TypedExpr), nil
}

// Visitor supports the execution of Expr.
type Visitor struct {
	ctx     context.Context
	evalCtx *eval.Context
	err     error

	fastIsConstVisitor fastIsConstVisitor
}

var _ tree.Visitor = &Visitor{}

// MakeNormalizeVisitor creates a Visitor instance.
func MakeNormalizeVisitor(ctx context.Context, evalCtx *eval.Context) Visitor {
	return Visitor{ctx: ctx, evalCtx: evalCtx, fastIsConstVisitor: fastIsConstVisitor{evalCtx: evalCtx}}
}

// Err retrieves the error field in the Visitor.
func (v *Visitor) Err() error { return v.err }

// VisitPre implements the Visitor interface.
func (v *Visitor) VisitPre(expr tree.Expr) (recurse bool, newExpr tree.Expr) {
	if v.err != nil {
		return false, expr
	}

	switch expr.(type) {
	case *tree.Subquery:
		// Subqueries are pre-normalized during semantic analysis. There
		// is nothing to do here.
		return false, expr
	}

	return true, expr
}

// VisitPost implements the Visitor interface.
func (v *Visitor) VisitPost(expr tree.Expr) tree.Expr {
	if v.err != nil {
		return expr
	}
	// We don't propagate errors during this step because errors might involve a
	// branch of code that isn't traversed by normal execution (for example,
	// IF(2 = 2, 1, 1 / 0)).

	// Normalize expressions that know how to normalize themselves.
	if typed, isTyped := expr.(tree.TypedExpr); isTyped {
		expr = normalizeExpr(v, typed)
		if v.err != nil {
			return expr
		}
	}

	// Evaluate all constant expressions.
	if v.isConst(expr) {
		value, err := eval.Expr(v.ctx, v.evalCtx, expr.(tree.TypedExpr))
		if err != nil {
			// Ignore any errors here (e.g. division by zero), so they can happen
			// during execution where they are correctly handled. Note that in some
			// cases we might not even get an error (if this particular expression
			// does not get evaluated when the query runs, e.g. it's inside a CASE).
			return expr
		}
		if value == tree.DNull {
			// We don't want to return an expression that has a different type; cast
			// the NULL if necessary.
			retypedNull, ok := eval.ReType(tree.DNull, expr.(tree.TypedExpr).ResolvedType())
			if !ok {
				v.err = errors.AssertionFailedf("failed to retype NULL to %s", expr.(tree.TypedExpr).ResolvedType())
				return expr
			}
			return retypedNull
		}
		return value
	}

	return expr
}

func (v *Visitor) isConst(expr tree.Expr) bool {
	return v.fastIsConstVisitor.run(expr)
}

// isNumericZero returns true if the datum is a number and equal to
// zero.
func (v *Visitor) isNumericZero(expr tree.TypedExpr) bool {
	if d, ok := expr.(tree.Datum); ok {
		switch t := eval.UnwrapDatum(v.ctx, v.evalCtx, d).(type) {
		case *tree.DDecimal:
			return t.Decimal.Sign() == 0
		case *tree.DFloat:
			return *t == 0
		case *tree.DInt:
			return *t == 0
		}
	}
	return false
}

// isNumericOne returns true if the datum is a number and equal to
// one.
func (v *Visitor) isNumericOne(expr tree.TypedExpr) bool {
	if d, ok := expr.(tree.Datum); ok {
		switch t := eval.UnwrapDatum(v.ctx, v.evalCtx, d).(type) {
		case *tree.DDecimal:
			return t.Decimal.Cmp(&DecimalOne.Decimal) == 0
		case *tree.DFloat:
			return *t == 1.0
		case *tree.DInt:
			return *t == 1
		}
	}
	return false
}

func invertComparisonOp(op treecmp.ComparisonOperator) (treecmp.ComparisonOperator, error) {
	switch op.Symbol {
	case treecmp.EQ:
		return treecmp.MakeComparisonOperator(treecmp.EQ), nil
	case treecmp.GE:
		return treecmp.MakeComparisonOperator(treecmp.LE), nil
	case treecmp.GT:
		return treecmp.MakeComparisonOperator(treecmp.LT), nil
	case treecmp.LE:
		return treecmp.MakeComparisonOperator(treecmp.GE), nil
	case treecmp.LT:
		return treecmp.MakeComparisonOperator(treecmp.GT), nil
	default:
		return op, errors.AssertionFailedf("unable to invert: %s", op)
	}
}

// fastIsConstVisitor is similar to isConstVisitor, but it only visits
// at most two levels of the tree (with one exception, see below).
// In essence, it determines whether an expression is constant by checking
// whether its children are const tree.Datums.
//
// This can be used during normalization since constants are evaluated
// bottom-up. If a child is *not* a const tree.Datum, that means it was already
// determined to be non-constant, and therefore was not evaluated.
type fastIsConstVisitor struct {
	evalCtx *eval.Context
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
			// Visitor may have wrapped a NULL.
			return true, expr
		}
		if _, ok := expr.(tree.Datum); !ok || eval.IsVar(v.evalCtx, expr, true /*allowConstPlaceholders*/) {
			// If the child expression is not a const tree.Datum, the parent expression is
			// not constant. Note that all constant literals have already been
			// normalized to tree.Datum in TypeCheck.
			v.isConst = false
		}
		return false, expr
	}
	v.visited = true

	// If the parent expression is a variable or non-immutable operator, we know
	// that it is not constant.

	if !tree.OperatorIsImmutable(expr) ||
		eval.IsVar(v.evalCtx, expr, true /*allowConstPlaceholders*/) {
		v.isConst = false
		return false, expr
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

// DecimalOne represents the constant 1 as tree.DECIMAL.
var DecimalOne tree.DDecimal

func init() {
	DecimalOne.SetInt64(1)
}
