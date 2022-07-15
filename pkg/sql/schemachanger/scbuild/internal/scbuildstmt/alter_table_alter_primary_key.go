// Copyright 2022 The Cockroach Authors.
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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

// The implementation of `ALTER PRIMARY KEY` will be broken into multiple PRs:
// 1. Implement the simplest case where we drop the old primary index and add the new primary index;
// 2. Consider the case where the new primary key is requested to be sharded.
// 3. Consider the case where the old primary index is on the implicitly created `rowid` column,
//    in which case we also need to drop that column;
// 4. Consider the case where altering primary key requires us to modify existing secondary indexes
//    (see the legacy schema change about in what cases we should rewrite)
// 5. Consider partitioning and locality (I'm not sure what they are, and why they play a role when
//    `ALTER PRIMARY KEY` but I've seen them in the old schema changer, so I assume we ought to do
//    something about them too here).
func alterTableAlterPrimaryKey(
	b BuildCtx, tn *tree.TableName, tbl *scpb.Table, t *tree.AlterTableAlterPrimaryKey,
) {
	// Panic on certain forbidden `ALTER PRIMARY KEY` cases (e.g. one of
	// the new primary key column is a virtual column). See the comments
	// for a full list of preconditions we check.
	checkForEarlyExit(b, tbl, t)

	// TODO (xiang): This section contains all fall-back cases and need to
	// 							 be removed to fully support `ALTER PRIMARY KEY`.
	fallBackIfRequestToBeSharded(t)
	fallBackIfRegionalByRow(b, tbl.TableID)

	tableElems := b.QueryByID(tbl.TableID)

	// Retrieve old primary index and its name elements.
	oldPrimaryIndexElem := mustRetrievePrimaryIndexElement(tableElems, tn.String())
	oldPrimaryIndexNameElem := mustRetrievePrimaryIndexNameElem(tableElems, oldPrimaryIndexElem.IndexID, tn.String())

	// Resolve and drop elements from the old primary index
	dropOldPrimaryIndex(b, tn, tbl, t, tableElems, oldPrimaryIndexNameElem)

	// Construct and add elements for the new primary index.
	addNewPrimaryIndex(b, tn, tbl, t, tableElems, oldPrimaryIndexElem)
}

// makeShardedDescriptor construct a sharded descriptor for the new primary key.
// Return nil if the new primary key is not hash-sharded.
func makeShardedDescriptor(b BuildCtx, t *tree.AlterTableAlterPrimaryKey) *catpb.ShardedDescriptor {
	if t.Sharded == nil {
		return nil
	}

	shardBuckets, err := tabledesc.EvalShardBucketCount(b, b.SemaCtx(), b.EvalCtx(), t.Sharded.ShardBuckets, t.StorageParams)
	if err != nil {
		panic(err)
	}
	columnNames := make([]string, len(t.Columns))
	for i, col := range t.Columns {
		columnNames[i] = col.Column.String()
	}

	return &catpb.ShardedDescriptor{
		IsSharded:    true,
		Name:         tabledesc.GetShardColumnName(columnNames, shardBuckets),
		ShardBuckets: shardBuckets,
		ColumnNames:  columnNames,
	}
}

// A utility function that converts tree.Direction to catpb.IndexColumn_Direction.
func indexColumnDirection(direction tree.Direction) catpb.IndexColumn_Direction {
	switch direction {
	case tree.DefaultDirection, tree.Ascending:
		return catpb.IndexColumn_ASC
	case tree.Descending:
		return catpb.IndexColumn_DESC
	default:
		panic(fmt.Sprintf("invalid direction %s", direction))
	}
}

// checkForEarlyExit asserts several precondition for a
// `ALTER PRIMARY KEY`, including
//   1. no expression columns allowed;
//   2. no columns that are in `DROPPED` state;
//   3. no inaccessible columns;
//   4. no nullable columns;
//   5. no virtual columns (starting from v22.1);
//   6. add more here
// Panic if any precondition is found unmet.
func checkForEarlyExit(b BuildCtx, tbl *scpb.Table, t *tree.AlterTableAlterPrimaryKey) {
	for _, col := range t.Columns {
		if col.Column == "" && col.Expr != nil {
			panic(errors.WithHint(
				pgerror.Newf(
					pgcode.InvalidColumnDefinition,
					"expressions such as %q are not allowed in primary index definition",
					col.Expr.String(),
				),
				"use columns instead",
			))
		}

		colElems := b.ResolveColumn(tbl.TableID, col.Column, ResolveParams{
			IsExistenceOptional: false,
			RequiredPrivilege:   privilege.CREATE,
		})

		colCurrentStatus, _, colElem := scpb.FindColumn(colElems)
		if colElem == nil {
			panic(fmt.Sprintf("programming error: resolving column %v does not give a "+
				"Column element.", col.Column))
		}
		if colCurrentStatus == scpb.Status_DROPPED || colCurrentStatus == scpb.Status_ABSENT {
			panic(pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
				"column %q is being dropped", col.Column))
		}
		if colElem.IsInaccessible {
			panic(pgerror.Newf(pgcode.InvalidSchemaDefinition, "cannot use inaccessible "+
				"column %q in primary key", col.Column))
		}
		_, _, colTypeElem := scpb.FindColumnType(colElems)
		if colTypeElem == nil {
			panic(fmt.Sprintf("programming error: resolving column %v does not give a "+
				"ColumnType element.", col.Column))
		}
		if colTypeElem.IsNullable {
			panic(pgerror.Newf(pgcode.InvalidSchemaDefinition, "cannot use nullable column "+
				"%q in primary key", col.Column))
		}

		if !b.EvalCtx().Settings.Version.IsActive(b, clusterversion.Start22_1) {
			if colTypeElem.IsVirtual {
				panic(pgerror.Newf(pgcode.FeatureNotSupported, "cannot use virtual column %q "+
					"in primary key", col.Column))
			}
		}
	}
}

// fallBackIfRequestToBeSharded panics with an unimplemented error
// if it is requested to be hash-sharded.
func fallBackIfRequestToBeSharded(t *tree.AlterTableAlterPrimaryKey) {
	if t.Sharded != nil {
		panic(unimplemented.NewWithIssueDetailf(83932, "ALTER PRIMARY KEY USING HASH is not yet supported",
			"cannot alter primary key using hash."))
	}
}

// fallBackIfRegionalByRow panics with an unimplemented error if
// the table is REGIONAL BY ROW.
func fallBackIfRegionalByRow(b BuildCtx, tblID catid.DescID) {
	tableElems := b.QueryByID(tblID)

	_, _, regionalByRowElem := scpb.FindTableLocalityRegionalByRow(tableElems)
	if regionalByRowElem != nil {
		_, _, tableNamespaceElem := scpb.FindNamespace(tableElems)
		if tableNamespaceElem == nil {
			panic("programming error: resolving input elements does not give a NamespaceElem element.")
		}
		panic(unimplemented.NewWithIssueDetailf(83932, "ALTER PRIMARY KEY on REGIONAL BY ROW "+
			"table is not yet supported", "cannot alter primary key on a REGIONAL BY ROW table %v",
			tableNamespaceElem.Name))
	}
}

func mustRetrievePrimaryIndexElement(
	tableElems ElementResultSet, tableName string,
) *scpb.PrimaryIndex {
	var primaryIndexElem *scpb.PrimaryIndex
	scpb.ForEachPrimaryIndex(tableElems, func(current scpb.Status, target scpb.TargetStatus, e *scpb.PrimaryIndex) {
		if current == scpb.Status_PUBLIC {
			primaryIndexElem = e
		}
	})
	if primaryIndexElem == nil {
		panic(fmt.Sprintf("programming error: resovling table %v does not give a PrimaryIndex element", tableName))
	}
	return primaryIndexElem
}

func mustRetrievePrimaryIndexNameElem(
	tableElems ElementResultSet, primaryIndexID catid.IndexID, tableName string,
) *scpb.IndexName {
	var primaryIndexNameElem *scpb.IndexName
	scpb.ForEachIndexName(tableElems, func(_ scpb.Status, _ scpb.TargetStatus, e *scpb.IndexName) {
		if e.IndexID == primaryIndexID {
			primaryIndexNameElem = e
		}
	})
	if primaryIndexNameElem == nil {
		panic(fmt.Sprintf("programming error: resolving table %v does not give a PrimaryIndex"+
			" name element", tableName))
	}
	return primaryIndexNameElem
}

func dropOldPrimaryIndex(
	b BuildCtx,
	tn *tree.TableName,
	tbl *scpb.Table,
	t *tree.AlterTableAlterPrimaryKey,
	tableElems ElementResultSet,
	oldPrimaryIndexNameElem *scpb.IndexName,
) {
	b.ResolveIndex(tbl.TableID, tree.Name(oldPrimaryIndexNameElem.Name), ResolveParams{
		IsExistenceOptional: false,
		RequiredPrivilege:   privilege.CREATE,
	}).ForEachElementStatus(func(_ scpb.Status, target scpb.TargetStatus, e scpb.Element) {
		if target != scpb.ToAbsent {
			b.Drop(e)
		}
	})
}

// addNewPrimaryIndex constructs and adds all necessary elements
// for a new primary index.
// Namely, it includes:
//     1. a temporary index element;
//		 2. three sets of index columns elements for the temporary index;
//     3. primary index element;
//     4. three sets of index columns elements;
//     5. index name;
func addNewPrimaryIndex(
	b BuildCtx,
	tn *tree.TableName,
	tbl *scpb.Table,
	t *tree.AlterTableAlterPrimaryKey,
	tableElems ElementResultSet,
	oldPrimaryIndexElem *scpb.PrimaryIndex,
) {
	newPrimaryIndex, tempIndex := addNewPrimaryIndexAndTempIndex(b, tn, tbl, t, oldPrimaryIndexElem)
	addIndexColumnsForNewPrimaryIndexAndTempIndex(b, tableElems, tn, tbl, t, newPrimaryIndex.IndexID, tempIndex.IndexID)
	addIndexNameForNewPrimaryIndex(b, tableElems, tn, tbl, t, newPrimaryIndex.IndexID)
}

// addNewPrimaryIndexAndTempIndex constructs and adds elements for
// the new primary index and its associated temporary index.
func addNewPrimaryIndexAndTempIndex(
	b BuildCtx,
	tn *tree.TableName,
	tbl *scpb.Table,
	t *tree.AlterTableAlterPrimaryKey,
	oldPrimaryIndexElem *scpb.PrimaryIndex,
) (*scpb.PrimaryIndex, *scpb.TemporaryIndex) {
	newPrimaryIndexElem := &scpb.PrimaryIndex{Index: scpb.Index{
		TableID:          tbl.TableID,
		IndexID:          nextRelationIndexID(b, tbl),
		IsUnique:         true,
		IsInverted:       oldPrimaryIndexElem.IsInverted,
		Sharding:         makeShardedDescriptor(b, t),
		ConstraintID:     oldPrimaryIndexElem.ConstraintID,
		SourceIndexID:    oldPrimaryIndexElem.IndexID,
		TemporaryIndexID: 0,
	}}
	b.Add(newPrimaryIndexElem)

	temporaryIndexElemForNewPrimaryIndex := &scpb.TemporaryIndex{
		Index:                    protoutil.Clone(newPrimaryIndexElem).(*scpb.PrimaryIndex).Index,
		IsUsingSecondaryEncoding: false,
	}
	b.AddTransient(temporaryIndexElemForNewPrimaryIndex)

	temporaryIndexElemForNewPrimaryIndex.IndexID = nextRelationIndexID(b, tbl)
	newPrimaryIndexElem.TemporaryIndexID = temporaryIndexElemForNewPrimaryIndex.IndexID

	return newPrimaryIndexElem, temporaryIndexElemForNewPrimaryIndex
}

// addIndexColumnsForNewPrimaryIndexAndTempIndex constructs and adds IndexColumn
// elements for the new primary index and its associated temporary index.
func addIndexColumnsForNewPrimaryIndexAndTempIndex(
	b BuildCtx,
	tableElems ElementResultSet,
	tn *tree.TableName,
	tbl *scpb.Table,
	t *tree.AlterTableAlterPrimaryKey,
	newPrimaryIndexID catid.IndexID,
	temporaryIndexIDForNewPrimarIndex catid.IndexID,
) {
	// Construct index columns for the new primary index as well as its temporary index
	allNonSystemColumnsName2ID := make(map[string]catid.ColumnID)
	scpb.ForEachColumnName(tableElems, func(current scpb.Status, target scpb.TargetStatus, e *scpb.ColumnName) {
		if !colinfo.IsColIDSystemColumn(e.ColumnID) {
			allNonSystemColumnsName2ID[e.Name] = e.ColumnID
		}
	})

	// all the KEY columns
	for i, col := range t.Columns {
		if colID, exist := allNonSystemColumnsName2ID[string(col.Column)]; !exist {
			panic(fmt.Sprintf("table %v does not have a column named %v", tn.String(), col.Column))
		} else {
			b.Add(&scpb.IndexColumn{
				TableID:       tbl.TableID,
				IndexID:       newPrimaryIndexID,
				ColumnID:      colID,
				OrdinalInKind: uint32(i),
				Kind:          scpb.IndexColumn_KEY,
				Direction:     indexColumnDirection(col.Direction),
			})
			b.Add(&scpb.IndexColumn{
				TableID:       tbl.TableID,
				IndexID:       temporaryIndexIDForNewPrimarIndex,
				ColumnID:      colID,
				OrdinalInKind: uint32(i),
				Kind:          scpb.IndexColumn_KEY,
				Direction:     indexColumnDirection(col.Direction),
			})

			delete(allNonSystemColumnsName2ID, col.Column.String())
		}
	}

	// no SUFFIX columns
	// what's left (all columns - KEY columns) are STORED columns.
	i := 0
	for colName, colID := range allNonSystemColumnsName2ID {
		b.Add(&scpb.IndexColumn{
			TableID:       tbl.TableID,
			IndexID:       newPrimaryIndexID,
			ColumnID:      colID,
			OrdinalInKind: uint32(i),
			Kind:          scpb.IndexColumn_STORED,
		})
		b.Add(&scpb.IndexColumn{
			TableID:       tbl.TableID,
			IndexID:       temporaryIndexIDForNewPrimarIndex,
			ColumnID:      colID,
			OrdinalInKind: uint32(i),
			Kind:          scpb.IndexColumn_STORED,
		})
		i++
		delete(allNonSystemColumnsName2ID, colName)
	}

	// A good sanity check here is that `allNonSystemColumnsName2ID` is empty
	if len(allNonSystemColumnsName2ID) != 0 {
		panic(fmt.Sprintf("internal error: there are unhandled columns from table %v: %v",
			tn.String(), allNonSystemColumnsName2ID))
	}
}

// addIndexNameForNewPrimaryIndex constructs and adds IndexName
// element for the new primary index.
func addIndexNameForNewPrimaryIndex(
	b BuildCtx,
	tableElems ElementResultSet,
	tn *tree.TableName,
	tbl *scpb.Table,
	t *tree.AlterTableAlterPrimaryKey,
	newPrimaryIndexID catid.IndexID,
) {
	newPrimaryIndexName := string(t.Name)
	if newPrimaryIndexName == "" {
		newPrimaryIndexName = tabledesc.PrimaryKeyIndexName(tn.Table())
	}
	b.Add(&scpb.IndexName{
		TableID: tbl.TableID,
		IndexID: newPrimaryIndexID,
		Name:    newPrimaryIndexName,
	})
}
