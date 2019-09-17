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
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// findIVarsInRange searches Expr for presence of tree.IndexedVars with indices
// in range [start, end). It returns a slice containing all such indices.
func findIVarsInRange(expr execinfrapb.Expression, start int, end int) []uint32 {
	res := make([]uint32, 0)
	if start >= end {
		return res
	}
	if expr.LocalExpr != nil {
		visitor := ivarExpressionVisitor{ivarSeen: make([]bool, end)}
		_, _ = tree.WalkExpr(visitor, expr.LocalExpr)
		for i := start; i < end; i++ {
			if visitor.ivarSeen[i] {
				res = append(res, uint32(i))
			}
		}
	} else {
		for i := start; i < end; i++ {
			if strings.Contains(expr.Expr, fmt.Sprintf("@%d", i+1)) {
				res = append(res, uint32(i))
			}
		}
	}
	return res
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
