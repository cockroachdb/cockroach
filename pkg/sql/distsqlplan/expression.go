// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// This file contains helper code to populate distsqlrun.Expressions during
// planning.

package distsqlplan

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/distsqlpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// ExprContext is an interface containing objects necessary for creating
// distsqlrun.Expressions.
type ExprContext interface {
	// EvalContext returns the tree.EvalContext for planning.
	EvalContext() *tree.EvalContext

	// IsLocal returns true if the current plan is local.
	IsLocal() bool

	// EvaluateSubqueries returns true if subqueries should be evaluated before
	// creating the distsqlrun.Expression.
	EvaluateSubqueries() bool
}

type ivarRemapper struct {
	indexVarMap []int
}

func (v *ivarRemapper) VisitPre(expr tree.Expr) (recurse bool, newExpr tree.Expr) {
	if ivar, ok := expr.(*tree.IndexedVar); ok {
		newIvar := *ivar
		newIvar.Idx = v.indexVarMap[ivar.Idx]
		return false, &newIvar
	}
	return true, expr
}

func (*ivarRemapper) VisitPost(expr tree.Expr) tree.Expr { return expr }

// MakeExpression creates a distsqlrun.Expression.
//
// The distsqlrun.Expression uses the placeholder syntax (@1, @2, @3..) to refer
// to columns.
//
// The expr uses IndexedVars to refer to columns. The caller can optionally
// remap these columns by passing an indexVarMap: an IndexedVar with index i
// becomes column indexVarMap[i].
func MakeExpression(
	expr tree.TypedExpr, ctx ExprContext, indexVarMap []int,
) (distsqlpb.Expression, error) {
	if expr == nil {
		return distsqlpb.Expression{}, nil
	}

	if ctx.IsLocal() {
		if indexVarMap != nil {
			// Remap our indexed vars
			v := &ivarRemapper{indexVarMap: indexVarMap}
			newExpr, _ := tree.WalkExpr(v, expr)
			expr = newExpr.(tree.TypedExpr)
		}
		return distsqlpb.Expression{LocalExpr: expr}, nil
	}

	evalCtx := ctx.EvalContext()
	subqueryVisitor := &evalAndReplaceSubqueryVisitor{
		evalCtx: evalCtx,
	}

	outExpr := expr.(tree.Expr)
	if ctx.EvaluateSubqueries() {
		outExpr, _ = tree.WalkExpr(subqueryVisitor, expr)
		if subqueryVisitor.err != nil {
			return distsqlpb.Expression{}, subqueryVisitor.err
		}
	}
	// We format the expression using the IndexedVar and Placeholder formatting interceptors.
	fmtCtx := distsqlpb.ExprFmtCtxBase(evalCtx)
	if indexVarMap != nil {
		fmtCtx.SetIndexedVarFormat(func(ctx *tree.FmtCtx, idx int) {
			remappedIdx := indexVarMap[idx]
			if remappedIdx < 0 {
				panic(fmt.Sprintf("unmapped index %d", idx))
			}
			ctx.Printf("@%d", remappedIdx+1)
		})
	}
	fmtCtx.FormatNode(outExpr)
	if log.V(1) {
		log.Infof(evalCtx.Ctx(), "Expr %s:\n%s", fmtCtx.String(), tree.ExprDebugString(outExpr))
	}
	return distsqlpb.Expression{Expr: fmtCtx.CloseAndGetString()}, nil
}

type evalAndReplaceSubqueryVisitor struct {
	evalCtx *tree.EvalContext
	err     error
}

var _ tree.Visitor = &evalAndReplaceSubqueryVisitor{}

func (e *evalAndReplaceSubqueryVisitor) VisitPre(expr tree.Expr) (bool, tree.Expr) {
	switch expr := expr.(type) {
	case *tree.Subquery:
		val, err := e.evalCtx.Planner.EvalSubquery(expr)
		if err != nil {
			e.err = err
			return false, expr
		}
		var newExpr tree.Expr = val
		if _, isTuple := val.(*tree.DTuple); !isTuple && expr.ResolvedType().Family() != types.UnknownFamily {
			newExpr = &tree.CastExpr{
				Expr: val,
				Type: expr.ResolvedType(),
			}
		}
		return false, newExpr
	default:
		return true, expr
	}
}

func (evalAndReplaceSubqueryVisitor) VisitPost(expr tree.Expr) tree.Expr { return expr }
