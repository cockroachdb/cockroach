// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scbuildstmt

import (
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

func alterTableSetDefault(
	b BuildCtx, tn *tree.TableName, tbl *scpb.Table, t *tree.AlterTableSetDefault,
) {
	alterColumnPreChecks(b, tn, tbl, t.Column)
	colID := getColumnIDFromColumnName(b, tbl.TableID, t.Column, true /* required */)
	// Block alters on system columns.
	scpb.ForEachColumn(
		b.QueryByID(tbl.TableID),
		func(_ scpb.Status, _ scpb.TargetStatus, e *scpb.Column) {
			if e.ColumnID == colID {
				// Block drops on system columns.
				panicIfSystemColumn(e, t.Column.String())
			}
		})
	// If our target column already has a default expression, we want to drop it first.
	defaultExpression := retrieveColumnDefaultExpressionElem(b, tbl.TableID, colID)
	if defaultExpression != nil {
		b.Drop(defaultExpression)
	}
	b.Add(&scpb.ColumnDefaultExpression{
		TableID:    tbl.TableID,
		ColumnID:   colID,
		Expression: *b.WrapExpression(tbl.TableID, t.Default),
	})
}
