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
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

/*
The overall idea of dropping a column is very similar to adding a column.

Recall that to add a column, the final end state should be that we modified
	a). the table descriptor such that it has new pk and new column, and
  b). the primary index (the underlying key-value paris) such that it has the new column
The approach we used, to achieve this end goal while keeping the table accessible, is to build a new primary index
(point b above) that includes the newly added column *stealthily*, and once it's built, swap it with the old primary
index "atomically".

Namely, this process is done in the following steps:
	1). Create a new primary index `idx2` and a new temporary index `idx3` (call the original primary index `idx1`).
	2). Transition `idx1` to PUBLIC, `idx2` to BACKFILL_ONLY, and `idx3` to WRITE_AND_DELETE_ONLY;
	3). Copy data from `idx1` to `idx2` (aka index backfill); During this period, which can be really long, all incoming writes/deletes
			will go to both the `idx1` and `idx3`;
	4). Transition `idx2` to WRITE_AND_DELETE_ONLY (`idx1` remains in PUBLIC, `idx3` remains in WRITE_AND_DELETE_ONLY);
	5). Merge `idx3` onto `idx2` (this is to get all incoming writes/deletes happened during backfilling `idx2` in
			step 3 above, which is conveniently recorded in `idx3`);
			You may ask: What about those writes/deletes happened during this merging process?
			Well, before the merging process, `idx2` is already in WRITE_AND_DELETE_ONLY, so all these writes/deletes will be
			recorded onto `idx2` even if `idx2` is undergoing a merging;
	6). Validate `idx2` which transition `idx2` to VALIDATED (`idx1` remains in PUBLIC, `idx3` remains in WRITE_AND_DELETE_ONLY);
	7). "Atomatically swap" `idx2` with `idx1` to be the new primary index. This means `idx1` transitions to WRITE_AND_DELETE_ONLY
			and `idx2` transitions to `PUBLIC`.
			Note:
			i). A dep rule requires us to transition `idx1` from PUBLIC to WRITE_AND_DELETE_ONLY
					before
					transition `idx2` from WRITE_AND_DELETE_ONLY to PUBLIC
			ii). At the end of this step/stage, `idx2` will be serving as the primary index for the table
	8). All that's left is cleaning up; we need to finish up transitioning `idx1` from WRITE_AND_DELETE_ONLY to DELETE_ONLY to ABSENT

Conversely, dropping a column will essentially be the same idea.
*/

func alterTableDropColumn(
	b BuildCtx, tn *tree.TableName, tbl *scpb.Table, t *tree.AlterTableDropColumn,
) {
	colElems := b.ResolveColumn(tbl.TableID, t.Column, ResolveParams{
		IsExistenceOptional: t.IfExists,
		RequiredPrivilege:   privilege.CREATE,
	})
	if colElems == nil {
		// Column does not exists but `IF EXISTS` is set
		b.MarkNameAsNonExistent(tn)
		return
	}

	// Resolve the to-be-dropped column, and mark all elements as dropped.
	colElems.ForEachElementStatus(func(current scpb.Status, target scpb.TargetStatus, e scpb.Element) {
		if target != scpb.ToAbsent {
			b.Drop(e)
		}
	})

	// Mark the old primary index element as dropped;
	// Mark the old primary index name element as dropped;
	// Maybe mark the old primary index partitioning element as dropped, if exists.
	_, _, oldPrimaryIndexElem := scpb.FindPrimaryIndex(b.QueryByID(tbl.TableID))
	if oldPrimaryIndexElem == nil {
		panic("table does not have a primary index.")
	}
	b.Drop(oldPrimaryIndexElem)

	var oldPrimaryIndexNameElem *scpb.IndexName
	var oldPrimaryIndexPartitioningElem *scpb.IndexPartitioning
	scpb.ForEachIndexName(b.QueryByID(tbl.TableID), func(_ scpb.Status, _ scpb.TargetStatus, name *scpb.IndexName) {
		if name.IndexID == oldPrimaryIndexElem.IndexID {
			oldPrimaryIndexNameElem = name
		}
	})
	scpb.ForEachIndexPartitioning(b.QueryByID(tbl.TableID), func(_ scpb.Status, _ scpb.TargetStatus, part *scpb.IndexPartitioning) {
		if part.IndexID == oldPrimaryIndexElem.IndexID {
			oldPrimaryIndexPartitioningElem = part
		}
	})
	if oldPrimaryIndexNameElem == nil {
		panic("table's primary index does not have a name")
	}
	b.Drop(oldPrimaryIndexNameElem)
	if oldPrimaryIndexPartitioningElem != nil {
		b.Drop(oldPrimaryIndexPartitioningElem)
	}

	// Mark the new primary index element as added.
	// Mark the new primary index name element as added.
	// Maybe mark the new primary index partitioning element as added.
	_, _, columnElem := scpb.FindColumnName(colElems)
	newPrimaryIndexElem := protoutil.Clone(oldPrimaryIndexElem).(*scpb.PrimaryIndex)
	newPrimaryIndexElem.IndexID = b.NextTableIndexID(tbl)
	newPrimaryIndexElem.SourceIndexID = oldPrimaryIndexElem.IndexID
	idx := indexOf(newPrimaryIndexElem.StoringColumnIDs, columnElem.ColumnID)
	newPrimaryIndexElem.StoringColumnIDs = append(newPrimaryIndexElem.StoringColumnIDs[:idx], newPrimaryIndexElem.StoringColumnIDs[idx+1:]...)
	newPrimaryIndexElem.TemporaryIndexID = newPrimaryIndexElem.IndexID + 1
	//if colinfo.CanHaveCompositeKeyEncoding(spec.colType.Type) {
	//	newPrimaryIndexElem.CompositeColumnIDs = append(newPrimaryIndexElem.CompositeColumnIDs, spec.col.ColumnID)
	//}
	b.Add(newPrimaryIndexElem)

	newPrimaryIndexNameElem := protoutil.Clone(oldPrimaryIndexNameElem).(*scpb.IndexName)
	newPrimaryIndexNameElem.IndexID = newPrimaryIndexElem.IndexID
	b.Add(newPrimaryIndexNameElem)

	if oldPrimaryIndexPartitioningElem != nil {
		updatedPartitioning := protoutil.Clone(oldPrimaryIndexPartitioningElem).(*scpb.IndexPartitioning)
		updatedPartitioning.IndexID = newPrimaryIndexElem.IndexID
		b.Add(updatedPartitioning)
	}

	// Construct the temporary index used for index backfill on the new primary index, and mark it as added.
	temp := &scpb.TemporaryIndex{
		Index:                    protoutil.Clone(newPrimaryIndexElem).(*scpb.PrimaryIndex).Index,
		IsUsingSecondaryEncoding: false,
	}
	temp.TemporaryIndexID = 0
	temp.IndexID = b.NextTableIndexID(tbl)
	b.AddTransient(temp)
	if oldPrimaryIndexPartitioningElem != nil {
		updatedPartitioning := protoutil.Clone(oldPrimaryIndexPartitioningElem).(*scpb.IndexPartitioning)
		updatedPartitioning.IndexID = temp.IndexID
		b.Add(updatedPartitioning)
	}

}

func indexOf(container []catid.ColumnID, target catid.ColumnID) int {
	for i, val := range container {
		if val == target {
			return i
		}
	}
	return -1
}
