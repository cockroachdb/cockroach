// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scbuildstmt

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

func alterTableDropNotNull(
	b BuildCtx,
	tn *tree.TableName,
	tbl *scpb.Table,
	stmt tree.Statement,
	t *tree.AlterTableDropNotNull,
) {
	alterColumnPreChecks(b, tn, tbl, t.Column)
	columnID := getColumnIDFromColumnName(b, tbl.TableID, t.Column, true /*required */)
	// Block alters on system columns.
	panicIfSystemColumn(mustRetrieveColumnElem(b, tbl.TableID, columnID), t.Column.String())
	// Ensure that this column is not in the primary indexes key.
	primaryIdx := getLatestPrimaryIndex(b, tbl.TableID)
	idxColumns := mustRetrieveIndexColumnElements(b, tbl.TableID, primaryIdx.IndexID)
	for _, idxCol := range idxColumns {
		if idxCol.ColumnID != columnID ||
			idxCol.Kind != scpb.IndexColumn_KEY {
			continue
		}
		colName := mustRetrieveColumnName(b, tbl.TableID, columnID)
		panic(pgerror.Newf(pgcode.InvalidTableDefinition,
			`column "%s" is in a primary index`, colName.Name))
	}
	// Ensure that we are not dropping not-null on a generated column.
	colEl := mustRetrieveColumnElem(b, tbl.TableID, columnID)
	if colEl.GeneratedAsIdentityType != catpb.GeneratedAsIdentityType_NOT_IDENTITY_COLUMN {
		colName := mustRetrieveColumnName(b, tbl.TableID, columnID)
		panic(pgerror.Newf(pgcode.Syntax,
			`column "%s" of relation "%s" is an identity column`, colName.Name, tn.ObjectName))
	}
	columNotNull := b.QueryByID(tbl.TableID).FilterColumnNotNull().Filter(func(current scpb.Status, target scpb.TargetStatus, e *scpb.ColumnNotNull) bool {
		return e.ColumnID == columnID
	}).MustGetZeroOrOneElement()
	if columNotNull == nil {
		return
	}
	b.Drop(columNotNull)
}
