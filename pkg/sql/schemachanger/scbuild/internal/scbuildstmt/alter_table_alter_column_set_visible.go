// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scbuildstmt

import (
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

func alterTableAlterColumnSetVisible(
	b BuildCtx,
	tn *tree.TableName,
	tbl *scpb.Table,
	stmt tree.Statement,
	t *tree.AlterTableSetVisible,
) {
	alterColumnPreChecks(b, tn, tbl, t.Column)
	columnID := getColumnIDFromColumnName(b, tbl.TableID, t.Column, true /* required */)
	// Block alters on system columns.
	panicIfSystemColumn(mustRetrieveColumnElem(b, tbl.TableID, columnID), t.Column)

	columnHidden := b.QueryByID(tbl.TableID).FilterColumnHidden().Filter(func(current scpb.Status, target scpb.TargetStatus, e *scpb.ColumnHidden) bool {
		return e.ColumnID == columnID
	}).MustGetZeroOrOneElement()

	if columnHidden != nil && t.Visible {
		b.Drop(columnHidden)
	} else if columnHidden == nil && !t.Visible {
		b.Add(&scpb.ColumnHidden{TableID: tbl.TableID, ColumnID: columnID})
	}
}
