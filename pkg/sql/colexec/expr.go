// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexec

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// ExprHelper is a utility interface that helps with expression handling in
// the vectorized engine.
type ExprHelper interface {
	// ProcessExpr processes the given expression and returns a well-typed
	// expression.
	ProcessExpr(execinfrapb.Expression, *tree.SemaContext, *tree.EvalContext, []*types.T) (tree.TypedExpr, error)
}

// ExprDeserialization describes how expression deserialization should be
// handled in the vectorized engine.
type ExprDeserialization int

const (
	// DefaultExprDeserialization is the default way of handling expression
	// deserialization in which case LocalExpr field is used if set.
	DefaultExprDeserialization ExprDeserialization = iota
	// ForcedExprDeserialization is the way of handling expression
	// deserialization in which case LocalExpr field is completely ignored and
	// the serialized representation is always deserialized.
	ForcedExprDeserialization
)

// NewExprHelper returns a new ExprHelper. forceExprDeserialization determines
// whether LocalExpr field is ignored by the helper.
func NewExprHelper(exprDeserialization ExprDeserialization) ExprHelper {
	switch exprDeserialization {
	case DefaultExprDeserialization:
		return &defaultExprHelper{}
	case ForcedExprDeserialization:
		return &forcedDeserializationExprHelper{}
	default:
		colexecerror.InternalError(fmt.Sprintf("unexpected ExprDeserialization %d", exprDeserialization))
		// This code is unreachable, but the compiler cannot infer that.
		return nil
	}
}

// defaultExprHelper is an ExprHelper that takes advantage of already present
// well-typed expression in LocalExpr when set.
type defaultExprHelper struct {
	helper execinfrapb.ExprHelper
}

var _ ExprHelper = &defaultExprHelper{}

// NewDefaultExprHelper returns an ExprHelper that takes advantage of an already
// well-typed expression in LocalExpr when set.
func NewDefaultExprHelper() ExprHelper {
	return &defaultExprHelper{}
}

func (h *defaultExprHelper) ProcessExpr(
	expr execinfrapb.Expression,
	semaCtx *tree.SemaContext,
	evalCtx *tree.EvalContext,
	typs []*types.T,
) (tree.TypedExpr, error) {
	if expr.LocalExpr != nil {
		return expr.LocalExpr, nil
	}
	h.helper.Types = typs
	tempVars := tree.MakeIndexedVarHelper(&h.helper, len(typs))
	return execinfrapb.DeserializeExpr(expr.Expr, semaCtx, evalCtx, &tempVars)
}

// forcedDeserializationExprHelper is an ExprHelper that always deserializes
// (namely, parses, type-checks, and evaluates the constants) the provided
// expression, completely ignoring LocalExpr field if set.
type forcedDeserializationExprHelper struct {
	helper execinfrapb.ExprHelper
}

var _ ExprHelper = &forcedDeserializationExprHelper{}

func (h *forcedDeserializationExprHelper) ProcessExpr(
	expr execinfrapb.Expression,
	semaCtx *tree.SemaContext,
	evalCtx *tree.EvalContext,
	typs []*types.T,
) (tree.TypedExpr, error) {
	h.helper.Types = typs
	tempVars := tree.MakeIndexedVarHelper(&h.helper, len(typs))
	return execinfrapb.DeserializeExpr(expr.Expr, semaCtx, evalCtx, &tempVars)
}

// Remove unused warning.
var _ = findIVarsInRange

// findIVarsInRange searches Expr for presence of tree.IndexedVars with indices
// in range [start, end). It returns a slice containing all such indices.
func findIVarsInRange(expr execinfrapb.Expression, start int, end int) ([]uint32, error) {
	res := make([]uint32, 0)
	if start >= end {
		return res, nil
	}
	var exprToWalk tree.Expr
	if expr.LocalExpr != nil {
		exprToWalk = expr.LocalExpr
	} else {
		e, err := parser.ParseExpr(expr.Expr)
		if err != nil {
			return nil, err
		}
		exprToWalk = e
	}
	visitor := ivarExpressionVisitor{ivarSeen: make([]bool, end)}
	_, _ = tree.WalkExpr(visitor, exprToWalk)
	for i := start; i < end; i++ {
		if visitor.ivarSeen[i] {
			res = append(res, uint32(i))
		}
	}
	return res, nil
}

type ivarExpressionVisitor struct {
	ivarSeen []bool
}

var _ tree.Visitor = &ivarExpressionVisitor{}

// VisitPre is a part of tree.Visitor interface.
func (i ivarExpressionVisitor) VisitPre(expr tree.Expr) (bool, tree.Expr) {
	switch e := expr.(type) {
	case *tree.IndexedVar:
		if e.Idx < len(i.ivarSeen) {
			i.ivarSeen[e.Idx] = true
		}
		return false, expr
	default:
		return true, expr
	}
}

// VisitPost is a part of tree.Visitor interface.
func (i ivarExpressionVisitor) VisitPost(expr tree.Expr) tree.Expr { return expr }
