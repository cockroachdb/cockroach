// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package normalize

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

// ConstantEvalVisitor replaces constant TypedExprs with the result of Eval.
type ConstantEvalVisitor struct {
	ctx     context.Context
	evalCtx *eval.Context
	err     error

	fastIsConstVisitor fastIsConstVisitor
}

var _ tree.Visitor = &ConstantEvalVisitor{}

// MakeConstantEvalVisitor creates a ConstantEvalVisitor instance.
func MakeConstantEvalVisitor(ctx context.Context, evalCtx *eval.Context) ConstantEvalVisitor {
	return ConstantEvalVisitor{ctx: ctx, evalCtx: evalCtx, fastIsConstVisitor: fastIsConstVisitor{evalCtx: evalCtx}}
}

// Err retrieves the error field in the ConstantEvalVisitor.
func (v *ConstantEvalVisitor) Err() error { return v.err }

// VisitPre implements the Visitor interface.
func (v *ConstantEvalVisitor) VisitPre(expr tree.Expr) (recurse bool, newExpr tree.Expr) {
	if v.err != nil {
		return false, expr
	}
	return true, expr
}

// VisitPost implements the Visitor interface.
func (v *ConstantEvalVisitor) VisitPost(expr tree.Expr) tree.Expr {
	if v.err != nil {
		return expr
	}

	typedExpr, ok := expr.(tree.TypedExpr)
	if !ok || !v.isConst(expr) {
		return expr
	}

	value, err := eval.Expr(v.ctx, v.evalCtx, typedExpr)
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
		retypedNull, ok := eval.ReType(tree.DNull, typedExpr.ResolvedType())
		if !ok {
			v.err = errors.AssertionFailedf("failed to retype NULL to %s", typedExpr.ResolvedType())
			return expr
		}
		return retypedNull
	}
	return value
}

func (v *ConstantEvalVisitor) isConst(expr tree.Expr) bool {
	return v.fastIsConstVisitor.run(expr)
}
