// Copyright 2023 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package cdceval

import (
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

// RewritePreviewExpression handles CDC expressions created on 22.2 clusters.
// The expression is rewritten so that it will continue functioning in
// post clusterversion.V23_1_ChangefeedExpressionProductionReady clusters.
// This code may be removed once 23.2 released.
func RewritePreviewExpression(oldExpr *tree.SelectClause) (*tree.SelectClause, error) {
	stmt, err := tree.SimpleStmtVisit(oldExpr, func(expr tree.Expr) (recurse bool, newExpr tree.Expr, err error) {
		switch t := expr.(type) {
		default:
			return true, expr, nil
		case *tree.FuncExpr:
			// Old style cdc_prev() function returned JSONb object.
			// Replace this call with a call to row_to_json((cdc_prev).*)
			if n, ok := t.Func.FunctionReference.(*tree.UnresolvedName); ok && n.Parts[0] == "cdc_prev" {
				rowToJSON := &tree.FuncExpr{
					Func: tree.ResolvableFunctionReference{
						FunctionReference: tree.FunDefs["row_to_json"],
					},
					Exprs: tree.Exprs{
						&tree.TupleStar{Expr: tree.NewUnresolvedName("cdc_prev")},
					},
				}
				return true, rowToJSON, nil
			}
		}
		return true, expr, nil
	})

	if err != nil {
		return nil, err
	}

	newExpr, ok := stmt.(*tree.SelectClause)
	if !ok {
		// We started with *tree.SelectClause, so getting anything else would be surprising.
		return nil, errors.AssertionFailedf("unexpected result type %T", stmt)
	}

	return newExpr, nil
}
