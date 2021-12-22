// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scbuildstmt

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

func alterTableDropColumn(b BuildCtx, table catalog.TableDescriptor, t *tree.AlterTableDropColumn) {
	if b.SessionData().SafeUpdates {
		panic(pgerror.DangerousStatementf("ALTER TABLE DROP COLUMN will " +
			"remove all data in that column"))
	}

	// TODO(ajwerner): Deal with drop column for columns which are being added
	// currently.
	colToDrop, err := table.FindColumnWithName(t.Column)
	if err != nil {
		if t.IfExists {
			// Noop.
			return
		}
		panic(err)
	}
	// Check whether the column is being dropped.
	found := false
	scpb.ForEachColumnName(b, func(_, targetStatus scpb.Status, col *scpb.ColumnName) {
		if targetStatus == scpb.Status_ABSENT {
			if col.TableID == table.GetID() && col.Name == string(t.Column) {
				found = true
			}
		}
	})
	if found {
		// Column drops are, while the column is in the process of being dropped,
		// for whatever reason, idempotent. Return silently here.
		return
	}

	// TODO:
	// remove sequence dependencies
	// drop sequences owned by column (if not referenced by other columns)
	// drop view (if cascade specified)
	// check that no computed columns reference this column
	// check that column is not in the PK
	// drop secondary indexes
	// drop all indexes that index/store the column or use it as a partial index predicate
	// drop check constraints
	// remove comments
	// drop foreign keys

	// Clean up type backreferences if no other column
	// refers to the same type.
	if colToDrop.HasType() && colToDrop.GetType().UserDefined() {
		colType := colToDrop.GetType()
		needsDrop := true
		for _, column := range table.AllColumns() {
			if column.HasType() && column.GetID() != colToDrop.GetID() &&
				column.GetType().Oid() == colType.Oid() {
				needsDrop = false
				break
			}
		}
		if needsDrop {
			typeID, err := typedesc.UserDefinedTypeOIDToID(colType.Oid())
			onErrPanic(err)
			typ := b.MustReadType(typeID)
			b.EnqueueDrop(&scpb.ColumnTypeReference{
				TypeID:   typ.GetID(),
				TableID:  table.GetID(),
				ColumnID: colToDrop.GetID(),
			})
		}
	}

	// TODO(ajwerner): Add family information to the column.
	b.EnqueueDrop(
		columnDescToElement(table, colToDrop.ColumnDescDeepCopy(), nil, nil),
	)
	b.EnqueueDrop(&scpb.ColumnName{
		TableID:  table.GetID(),
		ColumnID: colToDrop.GetID(),
		Name:     colToDrop.GetName(),
	})
	addOrUpdatePrimaryIndexTargetsForDropColumn(b, table, colToDrop.GetID())
}

// Suppress the linter. We're not ready to fully implement this schema change
// yet.
var _ = alterTableDropColumn

// TODO (lucy): refactor this to share with the add column case.
func addOrUpdatePrimaryIndexTargetsForDropColumn(
	b BuildCtx, table catalog.TableDescriptor, colID descpb.ColumnID,
) (idxID descpb.IndexID) {
	// Check whether a target to add a PK already exists. If so, update its
	// storing columns.
	{
		var latestAdded *scpb.PrimaryIndex
		scpb.ForEachPrimaryIndex(b, func(_, targetStatus scpb.Status, idx *scpb.PrimaryIndex) {
			if targetStatus == scpb.Status_PUBLIC && idx.TableID == table.GetID() {
				latestAdded = idx
			}
		})
		if latestAdded != nil {
			for j := range latestAdded.StoringColumnIDs {
				if latestAdded.StoringColumnIDs[j] == colID {
					latestAdded.StoringColumnIDs = append(latestAdded.StoringColumnIDs[:j], latestAdded.StoringColumnIDs[j+1:]...)
					return latestAdded.IndexID
				}
			}
			panic("column not found in added primary index storing columns")
		}
	}

	// Create a new primary index identical to the existing one except for its ID.
	idxID = b.NextIndexID(table)
	newIdx := table.GetPrimaryIndex().IndexDescDeepCopy()
	newIdx.ID = idxID
	for j, id := range newIdx.KeyColumnIDs {
		if id == colID {
			newIdx.KeyColumnIDs = append(newIdx.KeyColumnIDs[:j], newIdx.KeyColumnIDs[j+1:]...)
			newIdx.KeyColumnNames = append(newIdx.KeyColumnNames[:j], newIdx.KeyColumnNames[j+1:]...)
			break
		}
	}
	for j, id := range newIdx.StoreColumnIDs {
		if id == colID {
			newIdx.StoreColumnIDs = append(newIdx.StoreColumnIDs[:j], newIdx.StoreColumnIDs[j+1:]...)
			newIdx.StoreColumnNames = append(newIdx.StoreColumnNames[:j], newIdx.StoreColumnNames[j+1:]...)
			break
		}
	}

	newPrimaryIndex, newPrimaryIndexName := primaryIndexElemFromDescriptor(&newIdx, table)
	b.EnqueueAdd(newPrimaryIndex)
	b.EnqueueAdd(newPrimaryIndexName)

	// Drop the existing primary index.
	oldPrimaryIndex, oldPrimaryIndexName := primaryIndexElemFromDescriptor(table.GetPrimaryIndex().IndexDesc(), table)
	b.EnqueueDrop(oldPrimaryIndex)
	b.EnqueueDrop(oldPrimaryIndexName)
	return idxID
}

// Suppress the linter. We're not ready to fully implement this schema change
// yet.
var _ = addOrUpdatePrimaryIndexTargetsForDropColumn
