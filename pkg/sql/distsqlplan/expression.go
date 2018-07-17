// Copyright 2017 The Cockroach Authors.
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

// This file contains helper code to populate distsqlrun.Expressions during
// planning.

package distsqlplan

import (
	"bytes"
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/distsqlrun"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// exprFmtCtxBase produces a FmtCtx used for serializing expressions; a proper
// IndexedVar formatting function needs to be added on. It replaces placeholders
// with their values.
func exprFmtCtxBase(buf *bytes.Buffer, evalCtx *tree.EvalContext) tree.FmtCtx {
	fmtCtx := tree.MakeFmtCtx(buf, tree.FmtCheckEquivalence)
	fmtCtx.WithPlaceholderFormat(
		func(fmtCtx *tree.FmtCtx, p *tree.Placeholder) {
			d, err := p.Eval(evalCtx)
			if err != nil {
				panic(fmt.Sprintf("failed to serialize placeholder: %s", err))
			}
			d.Format(fmtCtx)
		})
	return fmtCtx
}

func walkExprAndReplaceSubqueries(t tree.Expr, subqueryReplaceFunc func(*tree.Subquery) tree.TypedExpr) tree.Expr {
	switch s := t.(type) {
	case *tree.AndExpr:
		s.Left = walkExprAndReplaceSubqueries(s.Left, subqueryReplaceFunc)
		s.Right = walkExprAndReplaceSubqueries(s.Right, subqueryReplaceFunc)
		return s
	case *tree.Array:
		for i := range s.Exprs {
			s.Exprs[i] = walkExprAndReplaceSubqueries(s.Exprs[i], subqueryReplaceFunc)
		}
		return s
	case *tree.ArrayFlatten:
		s.Subquery = walkExprAndReplaceSubqueries(s.Subquery, subqueryReplaceFunc)
		return s
	case *tree.BinaryExpr:
		s.Left = walkExprAndReplaceSubqueries(s.Left, subqueryReplaceFunc)
		s.Right = walkExprAndReplaceSubqueries(s.Right, subqueryReplaceFunc)
		return s
	case *tree.CaseExpr:
		s.Expr = walkExprAndReplaceSubqueries(s.Expr, subqueryReplaceFunc)
		s.Else = walkExprAndReplaceSubqueries(s.Else, subqueryReplaceFunc)
		for i := range s.Whens {
			s.Whens[i].Cond = walkExprAndReplaceSubqueries(s.Whens[i].Cond, subqueryReplaceFunc)
			s.Whens[i].Val = walkExprAndReplaceSubqueries(s.Whens[i].Val, subqueryReplaceFunc)
		}
		return s
	case *tree.CastExpr:
		s.Expr = walkExprAndReplaceSubqueries(s.Expr, subqueryReplaceFunc)
		return s
	case *tree.CoalesceExpr:
		for i := range s.Exprs {
			s.Exprs[i] = walkExprAndReplaceSubqueries(s.Exprs[i], subqueryReplaceFunc)
		}
		return s
	case *tree.CollateExpr:
		s.Expr = walkExprAndReplaceSubqueries(s.Expr, subqueryReplaceFunc)
		return s
	case *tree.ColumnAccessExpr:
		s.Expr = walkExprAndReplaceSubqueries(s.Expr, subqueryReplaceFunc)
		return s
	case *tree.ColumnItem:
		return s
	case *tree.ComparisonExpr:
		s.Left = walkExprAndReplaceSubqueries(s.Left, subqueryReplaceFunc)
		s.Right = walkExprAndReplaceSubqueries(s.Right, subqueryReplaceFunc)
		return s
	case *tree.FuncExpr:
		for i := range s.Exprs {
			s.Exprs[i] = walkExprAndReplaceSubqueries(s.Exprs[i], subqueryReplaceFunc)
		}
		s.Filter = walkExprAndReplaceSubqueries(s.Filter, subqueryReplaceFunc)
		return s
	case *tree.IfErrExpr:
		s.Cond = walkExprAndReplaceSubqueries(s.Cond, subqueryReplaceFunc)
		s.Else = walkExprAndReplaceSubqueries(s.Else, subqueryReplaceFunc)
		s.ErrCode = walkExprAndReplaceSubqueries(s.ErrCode, subqueryReplaceFunc)
		return s
	case *tree.IfExpr:
		s.Cond = walkExprAndReplaceSubqueries(s.Cond, subqueryReplaceFunc)
		s.True = walkExprAndReplaceSubqueries(s.True, subqueryReplaceFunc)
		s.Else = walkExprAndReplaceSubqueries(s.Else, subqueryReplaceFunc)
		return s
	case *tree.IndexedVar:
		return s
	case *tree.IndirectionExpr:
		s.Expr = walkExprAndReplaceSubqueries(s.Expr, subqueryReplaceFunc)
		return s
	case *tree.IsOfTypeExpr:
		s.Expr = walkExprAndReplaceSubqueries(s.Expr, subqueryReplaceFunc)
		return s
	case *tree.NotExpr:
		s.Expr = walkExprAndReplaceSubqueries(s.Expr, subqueryReplaceFunc)
		return s
	case *tree.NullIfExpr:
		s.Expr1 = walkExprAndReplaceSubqueries(s.Expr1, subqueryReplaceFunc)
		s.Expr2 = walkExprAndReplaceSubqueries(s.Expr2, subqueryReplaceFunc)
		return s
	case *tree.OrExpr:
		s.Left = walkExprAndReplaceSubqueries(s.Left, subqueryReplaceFunc)
		s.Right = walkExprAndReplaceSubqueries(s.Right, subqueryReplaceFunc)
		return s
	case *tree.ParenExpr:
		s.Expr = walkExprAndReplaceSubqueries(s.Expr, subqueryReplaceFunc)
		return s
	case *tree.Placeholder:
		return s
	case *tree.RangeCond:
		s.From = walkExprAndReplaceSubqueries(s.From, subqueryReplaceFunc)
		s.Left = walkExprAndReplaceSubqueries(s.Left, subqueryReplaceFunc)
		s.To = walkExprAndReplaceSubqueries(s.To, subqueryReplaceFunc)
		return s
	case *tree.Subquery:
		return subqueryReplaceFunc(s)
	case *tree.Tuple:
		for i := range s.Exprs {
			s.Exprs[i] = walkExprAndReplaceSubqueries(s.Exprs[i], subqueryReplaceFunc)
		}
		return s
	case *tree.TupleStar:
		s.Expr = walkExprAndReplaceSubqueries(s.Expr, subqueryReplaceFunc)
		return s
	case *tree.UnaryExpr:
		s.Expr = walkExprAndReplaceSubqueries(s.Expr, subqueryReplaceFunc)
		return s
	case *tree.UnqualifiedStar:
		return s
	default:
		return s
	}
}

// MakeExpression creates a distsqlrun.Expression.
//
// The distsqlrun.Expression uses the placeholder syntax (@1, @2, @3..) to refer
// to columns.
//
// The expr uses IndexedVars to refer to columns. The caller can optionally
// remap these columns by passing an indexVarMap: an IndexedVar with index i
// becomes column indexVarMap[i].
func MakeExpression(
	expr tree.TypedExpr, evalCtx *tree.EvalContext, indexVarMap []int,
) distsqlrun.Expression {
	if expr == nil {
		return distsqlrun.Expression{}
	}

	exprWithoutSubqueries := walkExprAndReplaceSubqueries(expr, func(s *tree.Subquery) tree.TypedExpr {
		val, err := evalCtx.Planner.EvalSubquery(s)
		if err != nil {
			panic("foo")
		}
		return val
	})

	// We format the expression using the IndexedVar and Placeholder formatting interceptors.
	var buf bytes.Buffer
	fmtCtx := exprFmtCtxBase(&buf, evalCtx)
	if indexVarMap != nil {
		fmtCtx.WithIndexedVarFormat(
			func(ctx *tree.FmtCtx, idx int) {
				remappedIdx := indexVarMap[idx]
				if remappedIdx < 0 {
					panic(fmt.Sprintf("unmapped index %d", idx))
				}
				ctx.Printf("@%d", remappedIdx+1)
			},
		)
	}
	fmtCtx.FormatNode(exprWithoutSubqueries)
	if log.V(1) {
		log.Infof(context.TODO(), "Expr %s:\n%s", buf.String(), tree.ExprDebugString(exprWithoutSubqueries))
	}
	return distsqlrun.Expression{Expr: buf.String()}
}
