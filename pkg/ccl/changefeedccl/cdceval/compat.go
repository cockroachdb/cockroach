// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cdceval

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
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
			if n, ok := t.Func.FunctionReference.(*tree.UnresolvedName); ok {
				switch n.Parts[0] {
				case "cdc_prev":
					// Old style cdc_prev() function returned JSONb object.
					// Replace this call with a call to row_to_json((cdc_prev).*)
					rowToJSON := &tree.FuncExpr{
						Func: tree.ResolvableFunctionReference{
							FunctionReference: tree.FunDefs["row_to_json"],
						},
						Exprs: tree.Exprs{
							&tree.TupleStar{Expr: tree.NewUnresolvedName("cdc_prev")},
						},
					}
					return true, rowToJSON, nil
				case "cdc_is_delete":
					// Old style cdc_is_delete returned boolean; use event_op instead.
					newExpr, err := parser.ParseExpr("(event_op() = 'delete')")
					if err != nil {
						return false, expr, err
					}
					return true, newExpr, nil
				case "cdc_mvcc_timestamp":
					// Old cdc_mvcc_timestamp() function gets replaced with crdb_intenral_mvcc_timestamp column.
					return true, tree.NewUnresolvedName(colinfo.MVCCTimestampColumnName), nil
				case "cdc_updated_timestamp":
					// Old cdc_updated_timestamp gets replaced with event_schema_timestamp.
					newExpr := &tree.FuncExpr{
						Func: tree.ResolvableFunctionReference{
							FunctionReference: tree.NewUnresolvedName("changefeed_event_schema_timestamp"),
						},
					}
					return true, newExpr, nil
				}
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
