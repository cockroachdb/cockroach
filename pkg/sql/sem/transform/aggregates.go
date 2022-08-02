// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package transform

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
)

// isAggregateVisitor checks if walked expressions contain aggregate functions.
type isAggregateVisitor struct {
	Aggregated bool
	// searchPath is used to search for unqualified function names.
	searchPath sessiondata.SearchPath
	ctx        context.Context
}

var _ tree.Visitor = &isAggregateVisitor{}

// VisitPre satisfies the Visitor interface.
func (v *isAggregateVisitor) VisitPre(expr tree.Expr) (recurse bool, newExpr tree.Expr) {
	switch t := expr.(type) {
	case *tree.FuncExpr:
		if t.IsWindowFunctionApplication() {
			// A window function application of an aggregate builtin is not an
			// aggregate function, but it can contain aggregate functions.
			return true, expr
		}

		// TODO(mgartner): Plumb a function resolver here, or determine that the
		// function should have already been resolved.
		fd, err := t.Func.Resolve(v.ctx, &v.searchPath, nil /* resolver */)
		if err != nil {
			return false, expr
		}
		funcCls, err := fd.GetClass()
		if err != nil {
			return false, expr
		}

		if funcCls == tree.AggregateClass {
			v.Aggregated = true
			return false, expr
		}
	case *tree.Subquery:
		return false, expr
	}

	return true, expr
}

// VisitPost satisfies the Visitor interface.
func (*isAggregateVisitor) VisitPost(expr tree.Expr) tree.Expr { return expr }
