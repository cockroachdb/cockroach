// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scbuildstmt

import (
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

func alterTableDropStored(
	b BuildCtx,
	tn *tree.TableName,
	tbl *scpb.Table,
	stmt tree.Statement,
	t *tree.AlterTableDropStored,
) {
	alterColumnPreChecks(b, tn, tbl, t.Column)
	colElems := b.ResolveColumn(tbl.TableID, t.Column, ResolveParams{
		RequiredPrivilege: privilege.CREATE,
	})
	colElem := colElems.FilterColumn().MustGetOneElement()
	columnID := colElem.ColumnID
	// Block alters on system columns.
	panicIfSystemColumn(colElem, t.Column)
	// Ensure that the column is stored (not virtual)
	panicIfVirtualColumn(b, tbl.TableID, columnID, t.Column)
	// Retrieve the compute expression element
	exprElem := retrieveColumnComputeExpressionElem(b, tbl.TableID, columnID)
	// Ensure that the column is computed
	if exprElem == nil {
		panic(pgerror.Newf(
			pgcode.InvalidColumnDefinition,
			"column %q is not a computed column",
			tree.ErrString(&t.Column)))
	}
	b.Drop(exprElem)
}

// panicIfVirtualColumn blocks operation if the column is virtual.
func panicIfVirtualColumn(
	b BuildCtx, tableID catid.DescID, columnID catid.ColumnID, columnName tree.Name,
) {
	colTypeEl := mustRetrieveColumnTypeElem(b, tableID, columnID)
	if colTypeEl.IsVirtual {
		panic(pgerror.Newf(
			pgcode.InvalidColumnDefinition,
			"column %q is not a stored computed column",
			tree.ErrString(&columnName)))
	}
}

// retrieveColumnComputeExpressionElem returns the compute expression
// element of the column. Returns nil if no expression exists and for
// older versions that store the expression as part of the ColumnType
func retrieveColumnComputeExpressionElem(
	b BuildCtx, tableID catid.DescID, columnID catid.ColumnID,
) (expr *scpb.ColumnComputeExpression) {
	return b.QueryByID(tableID).FilterColumnComputeExpression().Filter(func(
		_ scpb.Status, _ scpb.TargetStatus, e *scpb.ColumnComputeExpression) bool {
		return e.ColumnID == columnID
	}).MustGetZeroOrOneElement()
}
