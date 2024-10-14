// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scbuildstmt

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
)

func alterTableSetNotNull(
	b BuildCtx,
	tn *tree.TableName,
	tbl *scpb.Table,
	stmt tree.Statement,
	t *tree.AlterTableSetNotNull,
) {
	alterColumnPreChecks(b, tn, tbl, t.Column)
	columnID := getColumnIDFromColumnName(b, tbl.TableID, t.Column, true /*required */)
	if isColNotNull(b, tbl.TableID, columnID) {
		return
	}
	// Block alters on system columns.
	panicIfSystemColumn(mustRetrieveColumnElem(b, tbl.TableID, columnID), t.Column.String())
	b.Add(&scpb.ColumnNotNull{
		TableID:  tbl.TableID,
		ColumnID: columnID,
	})
}

// alterColumnPreChecks contains prerequisite checks for all ALTER COLUMN commands.
// All types of ALTER COLUMN commands should call this function first before
// any of its custom logic.
func alterColumnPreChecks(b BuildCtx, tn *tree.TableName, tbl *scpb.Table, columnName tree.Name) {
	// Cannot alter the TTL expiration column with active expiration column.
	scpb.ForEachRowLevelTTL(b.QueryByID(tbl.TableID), func(
		_ scpb.Status, _ scpb.TargetStatus, e *scpb.RowLevelTTL,
	) {
		if columnName == catpb.TTLDefaultExpirationColumnName && e.HasDurationExpr() {
			panic(sqlerrors.NewAlterDependsOnDurationExprError("alter", "column", columnName.String(), tn.Object()))
		}
	})
}
