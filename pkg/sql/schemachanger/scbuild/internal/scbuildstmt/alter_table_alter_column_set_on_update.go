// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scbuildstmt

import (
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

func alterTableSetOnUpdate(
	b BuildCtx,
	tn *tree.TableName,
	tbl *scpb.Table,
	stmt tree.Statement,
	t *tree.AlterTableSetOnUpdate,
) {
	alterColumnPreChecks(b, tn, tbl, t.Column)
	colID := getColumnIDFromColumnName(b, tbl.TableID, t.Column, true /* required */)
	col := mustRetrieveColumnElem(b, tbl.TableID, colID)
	oldOnUpdateExpr := retrieveColumnOnUpdateExpressionElem(b, tbl.TableID, colID)
	colType := mustRetrieveColumnTypeElem(b, tbl.TableID, colID)

	// Block alters on system columns.
	panicIfSystemColumn(col, t.Column.String())

	// Block disallowed operations on computed columns.
	panicIfComputedColumn(b, tn.ObjectName, colType, t.Column.String(), t.Expr)

	// For DROP ON UPDATE.
	if t.Expr == nil {
		if oldOnUpdateExpr != nil {
			b.Drop(oldOnUpdateExpr)
		}
		return
	}

	// If our target column already has an ON UPDATE expression, we want to drop it first.
	if oldOnUpdateExpr != nil {
		b.Drop(oldOnUpdateExpr)
	}
	typedExpr := panicIfInvalidNonComputedColumnExpr(b, tbl, tn.ToUnresolvedObjectName(), col, t.Column.String(), t.Expr, tree.ColumnOnUpdateExpr)

	b.Add(&scpb.ColumnOnUpdateExpression{
		TableID:    tbl.TableID,
		ColumnID:   colID,
		Expression: *b.WrapExpression(tbl.TableID, typedExpr),
	})
}
