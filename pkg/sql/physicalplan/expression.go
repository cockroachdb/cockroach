// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// This file contains helper code to populate execinfrapb.Expressions during
// planning.

package physicalplan

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// ExprContext is an interface containing objects necessary for creating
// execinfrapb.Expressions.
type ExprContext interface {
	// EvalContext returns the tree.EvalContext for planning.
	EvalContext() *tree.EvalContext

	// IsLocal returns true if the current plan is local.
	IsLocal() bool

	// MustEvaluateSubqueries returns true if subqueries should be evaluated
	// before creating the execinfrapb.Expression.
	MustEvaluateSubqueries() bool

	// MustReplaceSubqueriesWithNull returns true if subqueries should be replaced
	// with null before creating the execinfrapb.Expression. This is useful for
	// some explain variants.
	MustReplaceSubqueriesWithNull() bool
}

// fakeExprContext is a fake implementation of ExprContext that always behaves
// as if it were part of a non-local query.
type fakeExprContext struct{}

var _ ExprContext = fakeExprContext{}

func (fakeExprContext) EvalContext() *tree.EvalContext {
	return &tree.EvalContext{}
}

func (fakeExprContext) IsLocal() bool {
	return false
}

func (fakeExprContext) MustEvaluateSubqueries() bool {
	return true
}

func (fakeExprContext) MustReplaceSubqueriesWithNull() bool {
	return false
}

// MakeExpression creates a execinfrapb.Expression.
//
// The execinfrapb.Expression uses the placeholder syntax (@1, @2, @3..) to
// refer to columns.
//
// The expr uses IndexedVars to refer to columns. The caller can optionally
// remap these columns by passing an indexVarMap: an IndexedVar with index i
// becomes column indexVarMap[i].
//
// ctx can be nil in which case a fakeExprCtx will be used.
func MakeExpression(
	expr tree.TypedExpr, ctx ExprContext, indexVarMap []int,
) (execinfrapb.Expression, error) {
	if expr == nil {
		return execinfrapb.Expression{}, nil
	}
	if ctx == nil {
		ctx = &fakeExprContext{}
	}

	if ctx.IsLocal() {
		if indexVarMap != nil {
			// Remap our indexed vars.
			expr = sqlbase.RemapIVarsInTypedExpr(expr, indexVarMap)
		}
		return execinfrapb.Expression{LocalExpr: expr}, nil
	}

	evalCtx := ctx.EvalContext()

	outExpr := expr.(tree.Expr)
	// We format the expression using the IndexedVar and Placeholder formatting interceptors.
	fmtCtx := execinfrapb.ExprFmtCtxBase(evalCtx)
	if indexVarMap != nil {
		fmtCtx.SetIndexedVarFormat(func(ctx *tree.FmtCtx, idx int) {
			remappedIdx := indexVarMap[idx]
			if remappedIdx < 0 {
				panic(fmt.Sprintf("unmapped index %d", idx))
			}
			ctx.Printf("@%d", remappedIdx+1)
		})
	}
	if ctx.MustEvaluateSubqueries() {
		var fmtErr error
		if ctx.MustReplaceSubqueriesWithNull() {
			fmtCtx.SetSubqueriesFormat(func(ctx *tree.FmtCtx, subquery *tree.Subquery) {
				family := subquery.ResolvedType().Family()
				var expr tree.Expr = tree.DNull
				if family != types.UnknownFamily && family != types.TupleFamily {
					expr = &tree.CastExpr{
						Expr: expr,
						Type: subquery.ResolvedType(),
					}
				}
				ctx.FormatNode(expr)
			})
		} else {
			fmtCtx.SetSubqueriesFormat(func(ctx *tree.FmtCtx, subquery *tree.Subquery) {
				var val tree.Datum
				val, fmtErr = evalCtx.Planner.EvalSubquery(subquery)
				if fmtErr != nil {
					return
				}
				var newExpr tree.Expr = val
				if _, isTuple := val.(*tree.DTuple); !isTuple && subquery.ResolvedType().Family() != types.UnknownFamily {
					newExpr = &tree.CastExpr{
						Expr: val,
						Type: subquery.ResolvedType(),
					}
				}
				ctx.FormatNode(newExpr)
			})
		}
	}
	fmtCtx.FormatNode(outExpr)
	if log.V(1) {
		log.Infof(evalCtx.Ctx(), "Expr %s:\n%s", fmtCtx.String(), tree.ExprDebugString(outExpr))
	}
	return execinfrapb.Expression{Expr: fmtCtx.CloseAndGetString()}, nil
}
