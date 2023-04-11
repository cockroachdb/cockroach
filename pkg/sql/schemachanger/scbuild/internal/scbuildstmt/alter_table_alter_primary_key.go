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
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catenumpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/paramparse"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

func alterTableAlterPrimaryKey(
	b BuildCtx, tn *tree.TableName, tbl *scpb.Table, t *tree.AlterTableAlterPrimaryKey,
) {
	alterPrimaryKey(b, tn, tbl, alterPrimaryKeySpec{
		n:             t,
		Columns:       t.Columns,
		Sharded:       t.Sharded,
		Name:          t.Name,
		StorageParams: t.StorageParams,
	})
}

type alterPrimaryKeySpec struct {
	n             tree.NodeFormatter
	Columns       tree.IndexElemList
	Sharded       *tree.ShardedIndexDef
	Name          tree.Name
	StorageParams tree.StorageParams
}

func alterPrimaryKey(b BuildCtx, tn *tree.TableName, tbl *scpb.Table, t alterPrimaryKeySpec) {

	// Panic on certain forbidden `ALTER PRIMARY KEY` cases (e.g. one of
	// the new primary key column is a virtual column). See the comments
	// for a full list of preconditions we check.
	checkForEarlyExit(b, tbl, t)

	// Nothing to do if the requested new pk is the same as the old one.
	if isNewPrimaryKeySameAsOldPrimaryKey(b, tbl, t) {
		return
	}

	// TODO (xiang): This section contains all fall-back cases and need to
	// be removed to fully support `ALTER PRIMARY KEY`.
	fallBackIfConcurrentSchemaChange(b, t, tbl.TableID)
	fallBackIfShardedIndexExists(b, t, tbl.TableID)
	fallBackIfPartitionedIndexExists(b, t, tbl.TableID)
	fallBackIfRegionalByRowTable(b, t.n, tbl.TableID)
	fallBackIfDescColInRowLevelTTLTables(b, tbl.TableID, t)
	fallBackIfSubZoneConfigExists(b, t.n, tbl.TableID)
	// Version gates functionally that is implemented after the statement is
	// publicly published.
	fallBackIfRequestedToBeShardedAndBeforeV231(b, t)

	// Retrieve old primary index and its name elements.
	oldPrimaryIndexElem, newPrimaryIndexElem := getPrimaryIndexes(b, tbl.TableID)
	if newPrimaryIndexElem != nil {
		// TODO (xiang): some other DDL stmt preceded this `ALTER PRIMARY KEY` and
		// thus a new primary index has already been created. We'd like
		// to support this use case one day
		// (e.g. `ALTER TABLE t ADD COLUMN ..., ALTER PRIMARY KEY ...;`).
		// Note that such scenarios should be caught above in
		// `fallBackIfConcurrentSchemaChange` and an unimplemented error
		// should be returned, so, here we panic with an programming error.
		panic(errors.AssertionFailedf("programming error: new primary index has already existed."))
	}

	// Handle special case where the old primary key is the hidden rowid column.
	// In this case, drop this column if it is not referenced anywhere.
	rowidToDrop := getPrimaryIndexDefaultRowIDColumn(b, tbl.TableID, oldPrimaryIndexElem.IndexID)
	if !checkIfColumnCanBeDropped(b, rowidToDrop) {
		rowidToDrop = nil
	}

	out := makeIndexSpec(b, oldPrimaryIndexElem.TableID, oldPrimaryIndexElem.IndexID)
	inColumns := make([]indexColumnSpec, 0, len(out.columns))
	{
		allColumns := getSortedAllColumnIDsInTable(b, tbl.TableID)

		// Get all KEY columns from t.Columns
		allColumnsNameToIDMapping := getAllColumnsNameToIDMapping(b, tbl.TableID)
		allKeyColumnIDs := make(map[catid.ColumnID]bool)
		for _, col := range t.Columns {
			colID, exist := allColumnsNameToIDMapping[string(col.Column)]
			if !exist {
				panic(fmt.Sprintf("table %v does not have a column named %v", tn.String(), col.Column))
			}
			if rowidToDrop != nil && colID == rowidToDrop.ColumnID {
				rowidToDrop = nil
			}
			inColumns = append(inColumns, indexColumnSpec{
				columnID:  colID,
				kind:      scpb.IndexColumn_KEY,
				direction: indexColumnDirection(col.Direction),
			})
			allKeyColumnIDs[colID] = true
		}

		// What's left are STORED columns, excluding virtual columns and system columns
		for _, colID := range allColumns {
			if _, isKeyCol := allKeyColumnIDs[colID]; isKeyCol ||
				mustRetrieveColumnTypeElem(b, tbl.TableID, colID).IsVirtual ||
				colinfo.IsColIDSystemColumn(colID) ||
				(rowidToDrop != nil && colID == rowidToDrop.ColumnID) {
				continue
			}
			inColumns = append(inColumns, indexColumnSpec{
				columnID: colID,
				kind:     scpb.IndexColumn_STORED,
			})
		}
	}
	out.apply(b.Drop)
	checkIfConstraintNameAlreadyExists(b, tbl, t)

	// Set up sharding.
	var sharding *catpb.ShardedDescriptor
	var shardColID catid.ColumnID
	var shardColCkConstraintID catid.ConstraintID
	if t.Sharded != nil {
		columnNames := make([]string, len(t.Columns))
		for i, col := range t.Columns {
			columnNames[i] = string(col.Column)
		}
		sharding, shardColID, shardColCkConstraintID = ensureShardColAndMakeShardDesc(b, tbl, columnNames,
			t.Sharded.ShardBuckets, t.StorageParams, t.n)
		inColumns = append(inColumns, indexColumnSpec{})
		copy(inColumns[1:], inColumns)
		inColumns[0] = indexColumnSpec{
			columnID: shardColID,
			kind:     scpb.IndexColumn_KEY,
		}
	}

	var sourcePrimaryIndexElem *scpb.PrimaryIndex
	if rowidToDrop == nil {
		// We're NOT dropping the rowid column => do one primary index swap.
		in, tempIn := makeSwapIndexSpec(b, out, out.primary.IndexID, inColumns)
		if sharding != nil {
			in.primary.Sharding = sharding
			tempIn.temporary.Sharding = sharding
			shardColNotNullElem := mustRetrieveColumnNotNullElem(b, tbl.TableID, shardColID)
			shardColNotNullElem.IndexIDForValidation = in.primary.IndexID
			checkConstraintElem := mustRetrieveCheckConstraintElem(b, tbl.TableID, shardColCkConstraintID)
			checkConstraintElem.IndexIDForValidation = in.primary.IndexID
		}
		if t.Name != "" {
			in.name.Name = string(t.Name)
		}
		in.apply(b.Add)
		tempIn.apply(b.AddTransient)
		newPrimaryIndexElem = in.primary
		sourcePrimaryIndexElem = in.primary
	} else {
		// We ARE dropping the rowid column => swap indexes twice and drop column.
		unionColumns := append(inColumns[:len(inColumns):len(inColumns)], indexColumnSpec{
			columnID: rowidToDrop.ColumnID,
			kind:     scpb.IndexColumn_STORED,
		})
		// Swap once to the new PK but storing rowid.
		union, tempUnion := makeSwapIndexSpec(b, out, out.primary.IndexID, unionColumns)
		union.primary.Sharding = protoutil.Clone(sharding).(*catpb.ShardedDescriptor)
		union.apply(b.AddTransient)
		tempUnion.apply(b.AddTransient)
		// Swap again to the final primary index: same PK but NOT storing rowid.
		in, tempIn := makeSwapIndexSpec(b, union, union.primary.IndexID, inColumns)
		in.primary.Sharding = sharding
		if t.Name != "" {
			in.name.Name = string(t.Name)
		}
		in.apply(b.Add)
		tempIn.apply(b.AddTransient)
		newPrimaryIndexElem = in.primary
		sourcePrimaryIndexElem = union.primary
	}
	b.LogEventForExistingTarget(newPrimaryIndexElem)

	// Recreate all secondary indexes.
	recreateAllSecondaryIndexes(b, tbl, newPrimaryIndexElem, sourcePrimaryIndexElem)

	// Drop the rowid column, if applicable.
	if rowidToDrop != nil {
		elts := b.QueryByID(rowidToDrop.TableID).Filter(hasColumnIDAttrFilter(rowidToDrop.ColumnID))
		dropColumn(b, tn, tbl, t.n, rowidToDrop, elts, tree.DropRestrict)
	}

	// Create a unique index on the old primary key columns, if applicable.
	// This is a CRDB unique feature to not regress on performance after altering PK.
	// Note that it has to precede recreating all secondary indexes because it is
	// possible we need to recreate this unique index.
	maybeAddUniqueIndexForOldPrimaryKey(b, tn, tbl, t, oldPrimaryIndexElem, newPrimaryIndexElem, rowidToDrop)
}

// fallBackIfRequestedToBeShardedAndBeforeV231 fallbacks to legacy schema changer if the
// new primary key is requested to be sharded and active cluster version is
// prior to V23_1.
func fallBackIfRequestedToBeShardedAndBeforeV231(b BuildCtx, t alterPrimaryKeySpec) {
	if t.Sharded != nil && !b.EvalCtx().Settings.Version.IsActive(b, clusterversion.V23_1) {
		panic(scerrors.NotImplementedErrorf(t.n, "ALTER PRIMARY KEY USING HASH is not "+
			"implemented before V23_1. Current cluster version is %v",
			b.EvalCtx().Settings.Version.ActiveVersion(b)))
	}
}

// checkForEarlyExit asserts several precondition for a
// `ALTER PRIMARY KEY`, including
//  1. no expression columns allowed;
//  2. no columns that are in `DROPPED` state;
//  3. no inaccessible columns;
//  4. no nullable columns;
//  5. no virtual columns (starting from v22.1);
//  6. add more here
//
// Panic if any precondition is found unmet.
func checkForEarlyExit(b BuildCtx, tbl *scpb.Table, t alterPrimaryKeySpec) {
	if err := paramparse.ValidateUniqueConstraintParams(
		t.StorageParams,
		paramparse.UniqueConstraintParamContext{
			IsPrimaryKey: true,
			IsSharded:    t.Sharded != nil,
		},
	); err != nil {
		panic(err)
	}

	usedColumns := make(map[tree.Name]bool, len(t.Columns))
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
		if usedColumns[col.Column] {
			panic(pgerror.Newf(pgcode.FeatureNotSupported,
				"new primary key contains duplicate column %q", col.Column))
		}
		usedColumns[col.Column] = true

		colElems := b.ResolveColumn(tbl.TableID, col.Column, ResolveParams{
			IsExistenceOptional: false,
			RequiredPrivilege:   privilege.CREATE,
		})

		colCurrentStatus, colTargetStatus, colElem := scpb.FindColumn(colElems)
		if colElem == nil {
			panic(errors.AssertionFailedf("programming error: resolving column %v does not give a "+
				"Column element.", col.Column))
		}
		if colCurrentStatus == scpb.Status_DROPPED || colCurrentStatus == scpb.Status_ABSENT {
			if colTargetStatus == scpb.ToPublic {
				panic(pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
					"column %q is being added", col.Column))
			}
			panic(pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
				"column %q is being dropped", col.Column))
		}
		if colElem.IsInaccessible {
			panic(pgerror.Newf(pgcode.InvalidSchemaDefinition, "cannot use inaccessible "+
				"column %q in primary key", col.Column))
		}
		if !isColNotNull(b, tbl.TableID, colElem.ColumnID) {
			panic(pgerror.Newf(pgcode.InvalidSchemaDefinition, "cannot use nullable column "+
				"%q in primary key", col.Column))
		}
	}
}

// isNewPrimaryKeySameAsOldPrimaryKey returns whether the requested new
// primary key is the same as the old primary key.
func isNewPrimaryKeySameAsOldPrimaryKey(b BuildCtx, tbl *scpb.Table, t alterPrimaryKeySpec) bool {
	oldPrimaryIndexElem := mustRetrievePrimaryIndexElement(b, tbl.TableID)
	oldPrimaryIndexKeyColumns := mustRetrieveKeyIndexColumns(b, tbl.TableID, oldPrimaryIndexElem.IndexID)

	// Check whether they have the same number of key columns.
	if len(oldPrimaryIndexKeyColumns) != len(t.Columns) {
		return false
	}

	// Check whether they are both sharded or both not sharded.
	if (oldPrimaryIndexElem.Sharding == nil) != (t.Sharded == nil) {
		return false
	}

	// Check whether all key columns (ID and directions) are the same.
	for i, col := range t.Columns {
		colElems := b.ResolveColumn(tbl.TableID, col.Column, ResolveParams{
			IsExistenceOptional: false,
			RequiredPrivilege:   privilege.CREATE,
		})
		_, _, colElem := scpb.FindColumn(colElems)
		if (oldPrimaryIndexKeyColumns[i].ColumnID != colElem.ColumnID) ||
			oldPrimaryIndexKeyColumns[i].Direction != indexColumnDirection(col.Direction) {
			return false
		}
	}

	// If both are sharded, check whether they have the same bucket count.
	if oldPrimaryIndexElem.Sharding != nil {
		shardBucketsInNewPrimaryIndex, err := tabledesc.EvalShardBucketCount(b, b.SemaCtx(), b.EvalCtx(),
			t.Sharded.ShardBuckets, t.StorageParams)
		if err != nil {
			panic(err)
		}
		if oldPrimaryIndexElem.Sharding.ShardBuckets != shardBucketsInNewPrimaryIndex {
			return false
		}
	}

	return true
}

// fallBackIfConcurrentSchemaChange panics with an unimplemented error if
// there are any other concurrent schema change on this table. This is determined
// by searching for any element that is currently not in its terminal status.
func fallBackIfConcurrentSchemaChange(b BuildCtx, t alterPrimaryKeySpec, tableID catid.DescID) {
	b.QueryByID(tableID).ForEachElementStatus(func(current scpb.Status, target scpb.TargetStatus, e scpb.Element) {
		if current != target.Status() {
			_, _, ns := scpb.FindNamespace(b.QueryByID(tableID))
			if ns == nil {
				panic(errors.AssertionFailedf("programming error: resolving table %v does not "+
					"give a Namespace element", tableID))
			}
			panic(scerrors.NotImplementedErrorf(t.n,
				"cannot perform a primary key change on %v with other schema changes on %v in the same transaction",
				ns.Name, ns.Name))
		}
	})
}

// fallBackIfPartitionedIndexExists panics with an unimplemented error
// if there exists partitioned indexes on the table.
func fallBackIfPartitionedIndexExists(b BuildCtx, t alterPrimaryKeySpec, tableID catid.DescID) {
	tableElts := b.QueryByID(tableID).Filter(notAbsentTargetFilter)
	scpb.ForEachIndexPartitioning(tableElts, func(_ scpb.Status, _ scpb.TargetStatus, _ *scpb.IndexPartitioning) {
		panic(scerrors.NotImplementedErrorf(t.n,
			"ALTER PRIMARY KEY on a table with index partitioning is not yet supported"))
	})
}

// fallBackIfShardedIndexExists panics with an unimplemented
// error if there exists sharded indexes on the table.
func fallBackIfShardedIndexExists(b BuildCtx, t alterPrimaryKeySpec, tableID catid.DescID) {
	tableElts := b.QueryByID(tableID).Filter(notAbsentTargetFilter)
	var hasSecondary bool
	scpb.ForEachSecondaryIndex(tableElts, func(_ scpb.Status, _ scpb.TargetStatus, idx *scpb.SecondaryIndex) {
		hasSecondary = true
		if idx.Sharding != nil {
			panic(scerrors.NotImplementedErrorf(t.n, "ALTER PRIMARY KEY on a table with sharded secondary "+
				"indexes is not yet supported."))
		}
	})
	// Primary index sharding only matters if there are secondary indexes: even
	// if we drop the sharding on the primary, we need to maintain it on the
	// secondaries if they exist.
	if !hasSecondary {
		return
	}
	scpb.ForEachPrimaryIndex(tableElts, func(_ scpb.Status, _ scpb.TargetStatus, idx *scpb.PrimaryIndex) {
		if idx.Sharding != nil {
			panic(scerrors.NotImplementedErrorf(t.n, "ALTER PRIMARY KEY on a table with sharded primary "+
				"indexes is not yet supported."))
		}
	})
}

// fallBackIfRegionalByRowTable panics with an unimplemented
// error if it's a REGIONAL BY ROW table because we need to
// include the implicit REGION column when constructing the
// new primary key.
func fallBackIfRegionalByRowTable(b BuildCtx, t tree.NodeFormatter, tableID catid.DescID) {
	_, _, rbrElem := scpb.FindTableLocalityRegionalByRow(b.QueryByID(tableID))
	if rbrElem != nil {
		panic(scerrors.NotImplementedErrorf(t, "ALTER PRIMARY KEY on a REGIONAL BY ROW table "+
			"is not yet supported."))
	}
}

// fallBackIfDescColInRowLevelTTLTables panics with an unimplemented
// error if the table is a (row-level-ttl table && (it has a descending
// key column || it has any inbound/outbound FK constraint)).
func fallBackIfDescColInRowLevelTTLTables(b BuildCtx, tableID catid.DescID, t alterPrimaryKeySpec) {
	if _, _, rowLevelTTLElem := scpb.FindRowLevelTTL(b.QueryByID(tableID)); rowLevelTTLElem == nil {
		return
	}

	// It's a row-level-ttl table. Ensure it has no non-descending
	// key columns, and there is no inbound/outbound foreign keys.
	for _, col := range t.Columns {
		if indexColumnDirection(col.Direction) != catenumpb.IndexColumn_ASC {
			panic(scerrors.NotImplementedErrorf(t.n, "non-ascending ordering on PRIMARY KEYs are not supported"))
		}
	}

	_, _, ns := scpb.FindNamespace(b.QueryByID(tableID))
	// Panic if there are any inbound FK constraints.
	if _, _, inboundFKElem := scpb.FindForeignKeyConstraint(b.BackReferences(tableID)); inboundFKElem != nil {
		panic(scerrors.NotImplementedErrorf(t.n,
			`foreign keys to table with TTL %q are not permitted`, ns.Name))
	}
}

func mustRetrievePrimaryIndexElement(b BuildCtx, tableID catid.DescID) (res *scpb.PrimaryIndex) {
	scpb.ForEachPrimaryIndex(b.QueryByID(tableID), func(
		current scpb.Status, target scpb.TargetStatus, e *scpb.PrimaryIndex,
	) {
		// TODO (xiang): for now, we assume there is one primary index, which
		// will not be true when there are DDL stmts (e.g. ADD/DROP COLUMN)
		// before this `ALTER PRIMARY KEY`.
		if current == scpb.Status_PUBLIC {
			res = e
		}
	})
	if res == nil {
		panic(errors.AssertionFailedf("programming error: resolving table %v does not give "+
			"a PrimaryIndex element", tableID))
	}
	return res
}

func mustRetrieveColumnElem(
	b BuildCtx, tableID catid.DescID, columnID catid.ColumnID,
) (column *scpb.Column) {
	scpb.ForEachColumn(b.QueryByID(tableID), func(current scpb.Status, target scpb.TargetStatus, e *scpb.Column) {
		if e.ColumnID == columnID {
			column = e
		}
	})
	if column == nil {
		panic(errors.AssertionFailedf("programming error: cannot find a Column element for column ID %v", columnID))
	}
	return column
}

func mustRetrieveColumnNotNullElem(
	b BuildCtx, tableID catid.DescID, columnID catid.ColumnID,
) (columnNotNullElem *scpb.ColumnNotNull) {
	scpb.ForEachColumnNotNull(b.QueryByID(tableID), func(
		current scpb.Status, target scpb.TargetStatus, e *scpb.ColumnNotNull,
	) {
		if e.ColumnID == columnID {
			columnNotNullElem = e
		}
	})
	if columnNotNullElem == nil {
		panic(errors.AssertionFailedf("programming error: cannot find a ColumnNotNull element "+
			"for column ID %v", columnID))
	}
	return columnNotNullElem
}

func mustRetrieveCheckConstraintElem(
	b BuildCtx, tableID catid.DescID, constraintID catid.ConstraintID,
) (checkConstraintElem *scpb.CheckConstraint) {
	scpb.ForEachCheckConstraint(b.QueryByID(tableID), func(
		current scpb.Status, target scpb.TargetStatus, e *scpb.CheckConstraint,
	) {
		if e.ConstraintID == constraintID {
			checkConstraintElem = e
		}
	})
	if checkConstraintElem == nil {
		panic(errors.AssertionFailedf("programming error: cannot find a CheckConstraint element"+
			" for constraint ID %v", constraintID))
	}
	return checkConstraintElem
}

func mustRetrieveColumnNameElem(
	b BuildCtx, tableID catid.DescID, columnID catid.ColumnID,
) (columnName *scpb.ColumnName) {
	scpb.ForEachColumnName(b.QueryByID(tableID), func(
		current scpb.Status, target scpb.TargetStatus, e *scpb.ColumnName,
	) {
		if e.ColumnID == columnID {
			columnName = e
		}
	})
	if columnName == nil {
		panic(errors.AssertionFailedf("programming error: cannot find a ColumnName element for column ID %v", columnID))
	}
	return columnName
}

func mustRetrieveColumnTypeElem(
	b BuildCtx, tableID catid.DescID, columnID catid.ColumnID,
) (columnType *scpb.ColumnType) {
	scpb.ForEachColumnType(b.QueryByID(tableID), func(
		current scpb.Status, target scpb.TargetStatus, e *scpb.ColumnType,
	) {
		if e.ColumnID == columnID {
			columnType = e
		}
	})
	if columnType == nil {
		panic(errors.AssertionFailedf("programming error: cannot find a ColumnType element for column ID %v", columnID))
	}
	return columnType
}

func mustRetrieveIndexElement(
	b BuildCtx, tableID catid.DescID, indexID catid.IndexID,
) (indexElem *scpb.Index) {
	scpb.ForEachSecondaryIndex(b.QueryByID(tableID), func(
		current scpb.Status, target scpb.TargetStatus, e *scpb.SecondaryIndex,
	) {
		if e.IndexID == indexID {
			indexElem = &e.Index
		}
	})
	scpb.ForEachPrimaryIndex(b.QueryByID(tableID), func(
		current scpb.Status, target scpb.TargetStatus, e *scpb.PrimaryIndex,
	) {
		if e.IndexID == indexID {
			indexElem = &e.Index
		}
	})
	if indexElem == nil {
		panic(errors.AssertionFailedf("programming error: cannot find an index with ID %v from table %v",
			indexID, tableID))
	}
	return indexElem
}

func mustRetrieveKeyIndexColumns(
	b BuildCtx, tableID catid.DescID, indexID catid.IndexID,
) (indexColumns []*scpb.IndexColumn) {
	scpb.ForEachIndexColumn(b.QueryByID(tableID), func(
		current scpb.Status, target scpb.TargetStatus, e *scpb.IndexColumn,
	) {
		if e.IndexID == indexID && e.Kind == scpb.IndexColumn_KEY {
			indexColumns = append(indexColumns, e)
		}
	})
	if indexColumns == nil {
		panic(errors.AssertionFailedf("programming error: cannot find any KEY index columns in "+
			"index %v from table %v", indexID, tableID))
	}
	return indexColumns
}

func mustRetrieveIndexNameElem(
	b BuildCtx, tableID catid.DescID, indexID catid.IndexID,
) (indexNameElem *scpb.IndexName) {
	scpb.ForEachIndexName(b.QueryByID(tableID), func(
		current scpb.Status, target scpb.TargetStatus, e *scpb.IndexName,
	) {
		if e.IndexID == indexID {
			indexNameElem = e
		}
	})
	if indexNameElem == nil {
		panic(errors.AssertionFailedf("programming error: cannot find an index name element "+
			"with ID %v from table %v", indexID, tableID))
	}
	return indexNameElem
}

func mustRetrieveConstraintWithoutIndexNameElem(
	b BuildCtx, tableID catid.DescID, constraintID catid.ConstraintID,
) (constraintWithoutIndexName *scpb.ConstraintWithoutIndexName) {
	scpb.ForEachConstraintWithoutIndexName(b.QueryByID(tableID), func(
		current scpb.Status, target scpb.TargetStatus, e *scpb.ConstraintWithoutIndexName,
	) {
		if e.ConstraintID == constraintID {
			constraintWithoutIndexName = e
		}
	})
	if constraintWithoutIndexName == nil {
		panic(errors.AssertionFailedf("programming error: cannot find a constraint name "+
			"element with ID %v from table %v", constraintID, tableID))
	}
	return constraintWithoutIndexName
}

func checkIfConstraintNameAlreadyExists(b BuildCtx, tbl *scpb.Table, t alterPrimaryKeySpec) {
	if t.Name == "" {
		return
	}
	// Check explicit constraint names.
	publicTableElts := b.QueryByID(tbl.TableID).Filter(publicTargetFilter)
	scpb.ForEachConstraintWithoutIndexName(publicTableElts, func(_ scpb.Status, _ scpb.TargetStatus, e *scpb.ConstraintWithoutIndexName) {
		if e.Name == string(t.Name) {
			panic(pgerror.Newf(pgcode.DuplicateObject, "constraint with name %q already exists", t.Name))
		}
	})
	// Check index names.
	scpb.ForEachIndexName(publicTableElts, func(_ scpb.Status, _ scpb.TargetStatus, n *scpb.IndexName) {
		if n.Name == string(t.Name) {
			panic(pgerror.Newf(pgcode.DuplicateObject, "constraint with name %q already exists", t.Name))
		}
	})
}

// recreateAllSecondaryIndexes recreates all secondary indexes. While the key
// columns remain the same in the face of a primary key change, the key suffix
// columns or the stored columns may not.
func recreateAllSecondaryIndexes(
	b BuildCtx, tbl *scpb.Table, newPrimaryIndex, sourcePrimaryIndex *scpb.PrimaryIndex,
) {
	publicTableElts := b.QueryByID(tbl.TableID).Filter(publicTargetFilter)
	// Generate all possible key suffix columns.
	var newKeySuffix []indexColumnSpec
	{
		scpb.ForEachIndexColumn(publicTableElts, func(_ scpb.Status, _ scpb.TargetStatus, ic *scpb.IndexColumn) {
			if ic.IndexID == newPrimaryIndex.IndexID && ic.Kind == scpb.IndexColumn_KEY {
				newKeySuffix = append(newKeySuffix, indexColumnSpec{
					columnID:  ic.ColumnID,
					kind:      scpb.IndexColumn_KEY_SUFFIX,
					direction: ic.Direction,
				})
			}
		})
	}
	// Recreate each secondary index.
	scpb.ForEachSecondaryIndex(publicTableElts, func(_ scpb.Status, _ scpb.TargetStatus, idx *scpb.SecondaryIndex) {
		out := makeIndexSpec(b, idx.TableID, idx.IndexID)
		var idxColIDs catalog.TableColSet
		inColumns := make([]indexColumnSpec, 0, len(out.columns))
		// Determine which columns end up in the new secondary index.
		{
			var largestKeyOrdinal uint32
			var invertedColumnID catid.ColumnID
			for _, ic := range out.columns {
				// First, add all key columns.
				// Also determine the ID of the inverted column, if applicable.
				if ic.Kind == scpb.IndexColumn_KEY {
					idxColIDs.Add(ic.ColumnID)
					inColumns = append(inColumns, indexColumnSpec{
						columnID:  ic.ColumnID,
						kind:      scpb.IndexColumn_KEY,
						direction: ic.Direction,
					})
					if idx.IsInverted && ic.OrdinalInKind >= largestKeyOrdinal {
						largestKeyOrdinal = ic.OrdinalInKind
						invertedColumnID = ic.ColumnID
					}
				}
			}
			// Next, add all the stored columns.
			for _, ic := range out.columns {
				if ic.Kind == scpb.IndexColumn_STORED && !idxColIDs.Contains(ic.ColumnID) {
					idxColIDs.Add(ic.ColumnID)
					inColumns = append(inColumns, indexColumnSpec{
						columnID: ic.ColumnID,
						kind:     scpb.IndexColumn_STORED,
					})
				}
			}
			// Finally, determine the key suffix columns: add all primary key columns
			// which have not already been added to the secondary index.
			for _, ics := range newKeySuffix {
				if !idxColIDs.Contains(ics.columnID) {
					idxColIDs.Add(ics.columnID)
					inColumns = append(inColumns, ics)
				} else if idx.IsInverted && invertedColumnID == ics.columnID {
					// In an inverted index, the inverted column's value is not equal to
					// the actual data in the row for that column. As a result, if the
					// inverted column happens to also be in the primary key, it's crucial
					// that the index key still be suffixed with that full primary key
					// value to preserve the index semantics.
					// However, this functionality is not supported by the execution
					// engine, so prevent it by returning an error.
					_, _, cn := scpb.FindColumnName(publicTableElts.Filter(hasColumnIDAttrFilter(invertedColumnID)))
					var colName string
					if cn != nil {
						colName = cn.Name
					} else {
						colName = fmt.Sprintf("#%d", invertedColumnID)
					}
					panic(unimplemented.NewWithIssuef(84405,
						"primary key column %s cannot be present in an inverted index",
						colName,
					))
				}
			}
		}
		in, temp := makeSwapIndexSpec(b, out, sourcePrimaryIndex.IndexID, inColumns)
		out.apply(b.Drop)
		in.apply(b.Add)
		temp.apply(b.AddTransient)
	})
}

// maybeAddUniqueIndexForOldPrimaryKey constructs and adds all necessary elements
// for a unique index on the old primary key columns, if certain conditions are
// met (see comments of shouldCreateUniqueIndexOnOldPrimaryKeyColumns for details).
// Namely, it includes
//  1. a SecondaryIndex element;
//  2. a set of IndexColumn elements for the secondary index;
//  3. a TemporaryIndex elements;
//  4. a set of IndexColumn elements for the temporary index;
//  5. a IndexName element;
//
// This is a CRDB unique feature that helps optimize the performance of
// queries that still filter on old primary key columns.
func maybeAddUniqueIndexForOldPrimaryKey(
	b BuildCtx,
	tn *tree.TableName,
	tbl *scpb.Table,
	t alterPrimaryKeySpec,
	oldPrimaryIndex *scpb.PrimaryIndex,
	newPrimaryIndex *scpb.PrimaryIndex,
	rowidToDrop *scpb.Column,
) {
	if !shouldCreateUniqueIndexOnOldPrimaryKeyColumns(
		b, tbl, oldPrimaryIndex.IndexID, newPrimaryIndex.IndexID, rowidToDrop,
	) {
		return
	}
	sec, temp := addNewUniqueSecondaryIndexAndTempIndex(b, tbl, oldPrimaryIndex)
	addIndexColumnsForNewUniqueSecondaryIndexAndTempIndex(b, tn, tbl, t,
		oldPrimaryIndex.IndexID, newPrimaryIndex.IndexID, sec.IndexID, temp.IndexID)
	addIndexNameForNewUniqueSecondaryIndex(b, tbl, sec.IndexID)
}

// addNewUniqueSecondaryIndexAndTempIndex constructs and adds elements for
// a new secondary index and its associated temporary index.
func addNewUniqueSecondaryIndexAndTempIndex(
	b BuildCtx, tbl *scpb.Table, oldPrimaryIndexElem *scpb.PrimaryIndex,
) (*scpb.SecondaryIndex, *scpb.TemporaryIndex) {

	sec := &scpb.SecondaryIndex{Index: scpb.Index{
		TableID:             tbl.TableID,
		IndexID:             nextRelationIndexID(b, tbl),
		IsUnique:            true,
		IsInverted:          oldPrimaryIndexElem.IsInverted,
		Sharding:            oldPrimaryIndexElem.Sharding,
		IsCreatedExplicitly: false,
		ConstraintID:        b.NextTableConstraintID(tbl.TableID),
		SourceIndexID:       oldPrimaryIndexElem.IndexID,
		TemporaryIndexID:    0,
	}}
	temp := &scpb.TemporaryIndex{
		Index:                    protoutil.Clone(sec).(*scpb.SecondaryIndex).Index,
		IsUsingSecondaryEncoding: true,
	}
	temp.ConstraintID = sec.ConstraintID + 1
	temp.IndexID = sec.IndexID + 1
	sec.TemporaryIndexID = temp.IndexID

	b.Add(sec)
	b.Add(&scpb.IndexData{TableID: sec.TableID, IndexID: sec.IndexID})
	b.AddTransient(temp)
	b.AddTransient(&scpb.IndexData{TableID: temp.TableID, IndexID: temp.IndexID})

	return sec, temp
}

// addIndexColumnsForNewUniqueSecondaryIndexAndTempIndex constructs and adds IndexColumn
// elements for the new primary index and its associated temporary index.
func addIndexColumnsForNewUniqueSecondaryIndexAndTempIndex(
	b BuildCtx,
	tn *tree.TableName,
	tbl *scpb.Table,
	t alterPrimaryKeySpec,
	oldPrimaryIndexID catid.IndexID,
	newPrimaryIndexID catid.IndexID,
	newUniqueSecondaryIndexID catid.IndexID,
	temporaryIndexIDForNewUniqueSecondaryIndex catid.IndexID,
) {
	// KEY columns = old primary key columns
	oldPrimaryIndexKeyColumns := mustRetrieveKeyIndexColumns(b, tbl.TableID, oldPrimaryIndexID)
	oldPrimaryIndexKeyColumnIDs := make([]catid.ColumnID, len(oldPrimaryIndexKeyColumns))
	for i, keyIndexCol := range oldPrimaryIndexKeyColumns {
		oldPrimaryIndexKeyColumnIDs[i] = keyIndexCol.ColumnID
	}

	for _, keyIndexColumn := range oldPrimaryIndexKeyColumns {
		b.Add(&scpb.IndexColumn{
			TableID:       tbl.TableID,
			IndexID:       newUniqueSecondaryIndexID,
			ColumnID:      keyIndexColumn.ColumnID,
			OrdinalInKind: keyIndexColumn.OrdinalInKind,
			Kind:          scpb.IndexColumn_KEY,
			Direction:     keyIndexColumn.Direction,
		})
		b.Add(&scpb.IndexColumn{
			TableID:       tbl.TableID,
			IndexID:       temporaryIndexIDForNewUniqueSecondaryIndex,
			ColumnID:      keyIndexColumn.ColumnID,
			OrdinalInKind: keyIndexColumn.OrdinalInKind,
			Kind:          scpb.IndexColumn_KEY,
			Direction:     keyIndexColumn.Direction,
		})
	}

	// SUFFIX_KEY columns = new primary index columns - old primary key columns
	// Add each column that is not in the old primary key as a SUFFIX_KEY column.
	var ord uint32 = 0
	for _, keyColInNewPrimaryIndex := range mustRetrieveKeyIndexColumns(b, tbl.TableID, newPrimaryIndexID) {
		if !descpb.ColumnIDs(oldPrimaryIndexKeyColumnIDs).Contains(keyColInNewPrimaryIndex.ColumnID) {
			b.Add(&scpb.IndexColumn{
				TableID:       tbl.TableID,
				IndexID:       newUniqueSecondaryIndexID,
				ColumnID:      keyColInNewPrimaryIndex.ColumnID,
				OrdinalInKind: ord,
				Kind:          scpb.IndexColumn_KEY_SUFFIX,
				Direction:     keyColInNewPrimaryIndex.Direction,
			})
			b.Add(&scpb.IndexColumn{
				TableID:       tbl.TableID,
				IndexID:       temporaryIndexIDForNewUniqueSecondaryIndex,
				ColumnID:      keyColInNewPrimaryIndex.ColumnID,
				OrdinalInKind: ord,
				Kind:          scpb.IndexColumn_KEY_SUFFIX,
				Direction:     keyColInNewPrimaryIndex.Direction,
			})
			ord++
		}
	}
}

// addIndexNameForNewUniqueSecondaryIndex constructs and adds an IndexName
// element for the new, unique secondary index on the old primary key.
func addIndexNameForNewUniqueSecondaryIndex(b BuildCtx, tbl *scpb.Table, indexID catid.IndexID) {
	indexName := getImplicitSecondaryIndexName(b, tbl.TableID, indexID, 0 /* numImplicitColumns */)
	b.Add(&scpb.IndexName{
		TableID: tbl.TableID,
		IndexID: indexID,
		Name:    indexName,
	})
}

// We only recreate the old primary key of the table as a unique secondary
// index if:
//   - The table has a primary key (no DROP PRIMARY KEY statements have
//     been executed).
//   - The primary key is not the default rowid primary key.
//   - The new primary key isn't the same set of columns and directions
//     other than hash sharding.
//   - There is no partitioning change.
//   - There is no existing secondary index on the old primary key columns.
func shouldCreateUniqueIndexOnOldPrimaryKeyColumns(
	b BuildCtx,
	tbl *scpb.Table,
	oldPrimaryIndexID, newPrimaryIndexID catid.IndexID,
	rowidToDrop *scpb.Column,
) bool {
	// A function that retrieves all KEY columns of this index.
	// If excludeShardedCol, sharded column is excluded, if any.
	keyColumnIDsAndDirsOfIndex := func(
		b BuildCtx, tableID catid.DescID, indexID catid.IndexID, excludeShardedCol bool,
	) (
		columnIDs descpb.ColumnIDs,
		columnDirs []catenumpb.IndexColumn_Direction,
	) {
		sharding := mustRetrieveIndexElement(b, tableID, indexID).Sharding
		allKeyIndexColumns := mustRetrieveKeyIndexColumns(b, tableID, indexID)
		for _, keyIndexCol := range allKeyIndexColumns {
			if !excludeShardedCol || sharding == nil ||
				mustRetrieveColumnNameElem(b, tableID, keyIndexCol.ColumnID).Name != sharding.Name {
				columnIDs = append(columnIDs, keyIndexCol.ColumnID)
				columnDirs = append(columnDirs, keyIndexCol.Direction)
			}
		}
		return columnIDs, columnDirs
	}

	// A function that checks whether two indexes have matching columns and directions,
	// excluding shard column if specified.
	keyColumnIDsAndDirsMatch := func(
		b BuildCtx, tableID catid.DescID, oldIndexID, newIndexID catid.IndexID, excludeShardedCol bool,
	) bool {
		oldIDs, oldDirs := keyColumnIDsAndDirsOfIndex(b, tableID, oldIndexID, excludeShardedCol)
		newIDs, newDirs := keyColumnIDsAndDirsOfIndex(b, tableID, newIndexID, excludeShardedCol)
		if !oldIDs.Equals(newIDs) {
			return false
		}
		for i := range oldDirs {
			if oldDirs[i] != newDirs[i] {
				return false
			}
		}
		return true
	}

	// If the primary key doesn't really change, don't create any unique indexes.
	if keyColumnIDsAndDirsMatch(b, tbl.TableID, oldPrimaryIndexID, newPrimaryIndexID, true /* excludeShardedCol */) {
		return false
	}

	// A function that checks whether there exists a secondary index
	// that is "identical" to the old primary index.
	// It is used to avoid creating duplicate secondary index during
	// `ALTER PRIMARY KEY`.
	alreadyHasSecondaryIndexOnPKColumns := func(
		b BuildCtx, tableID catid.DescID, oldPrimaryIndexID catid.IndexID,
	) (found bool) {
		scpb.ForEachSecondaryIndex(b.QueryByID(tableID), func(
			current scpb.Status, target scpb.TargetStatus, candidate *scpb.SecondaryIndex,
		) {
			if !mustRetrieveIndexElement(b, tableID, candidate.IndexID).IsUnique {
				return
			}
			if !keyColumnIDsAndDirsMatch(b, tableID, oldPrimaryIndexID,
				candidate.IndexID, false /* excludeShardedCol */) {
				return
			}
			// This secondary index is non-partial, unique, and has exactly the same
			// key columns (and same directions) as the old primary index!
			found = true
		})
		return found
	}

	// If there already exist suitable unique indexes, then don't create any.
	if alreadyHasSecondaryIndexOnPKColumns(b, tbl.TableID, oldPrimaryIndexID) {
		return false
	}

	// If the old PK consists of the rowid column, and if we intend to drop it,
	// then that implies that there are no references to it anywhere and we don't
	// need to guarantee its uniqueness.
	if rowidToDrop != nil {
		return false
	}

	// In all other cases, we need to create unique indexes just to be sure.
	return true
}

// getPrimaryIndexDefaultRowIDColumn checks whether the primary key is on the
// implicitly created, hidden column 'rowid' and returns it if that's the case.
func getPrimaryIndexDefaultRowIDColumn(
	b BuildCtx, tableID catid.DescID, indexID catid.IndexID,
) (column *scpb.Column) {
	// Sanity check: input `indexID` should really be the index of
	// a primary index.
	var primaryIndex *scpb.PrimaryIndex
	scpb.ForEachPrimaryIndex(b.QueryByID(tableID), func(
		current scpb.Status, target scpb.TargetStatus, e *scpb.PrimaryIndex,
	) {
		if current == scpb.Status_PUBLIC && e.IndexID == indexID {
			primaryIndex = e
		}
	})
	if primaryIndex == nil {
		panic(fmt.Sprintf("internal error: input indexID %v is not the primary index of table %v", indexID, tableID))
	}

	// This primary index should have only one column.
	indexColumns := mustRetrieveKeyIndexColumns(b, tableID, indexID)
	if len(indexColumns) != 1 {
		return nil
	}

	columnID := indexColumns[0].ColumnID

	// That one column should be hidden.
	column = mustRetrieveColumnElem(b, tableID, columnID)
	if !column.IsHidden {
		return nil
	}

	// That one column's name should be 'rowid' or prefixed by 'rowid'.
	columnName := mustRetrieveColumnNameElem(b, tableID, columnID)
	if !strings.HasPrefix(columnName.Name, "rowid") {
		return nil
	}

	// That column should be of type INT.
	columnType := mustRetrieveColumnTypeElem(b, tableID, columnID)
	if !columnType.Type.Equal(types.Int) {
		return nil
	}

	// That column should have default expression that is equal to "unique_rowid()".
	var columnDefaultExpression *scpb.ColumnDefaultExpression
	scpb.ForEachColumnDefaultExpression(b.QueryByID(tableID), func(
		current scpb.Status, target scpb.TargetStatus, e *scpb.ColumnDefaultExpression,
	) {
		if e.ColumnID == column.ColumnID {
			columnDefaultExpression = e
		}
	})
	if columnDefaultExpression == nil || columnDefaultExpression.Expr != "unique_rowid()" {
		return nil
	}

	// All checks are satisfied, return true!
	return column
}

// checkIfColumnCanBeDropped returns true iff the column is not referenced
// anywhere, and can therefore be dropped.
func checkIfColumnCanBeDropped(b BuildCtx, columnToDrop *scpb.Column) bool {
	if columnToDrop == nil {
		return false
	}
	canBeDropped := true
	walkDropColumnDependencies(b, columnToDrop, func(e scpb.Element) {
		if !canBeDropped {
			return
		}
		switch e := e.(type) {
		case *scpb.Column:
			if e.TableID != columnToDrop.TableID || e.ColumnID != columnToDrop.ColumnID {
				canBeDropped = false
			}
		case *scpb.ColumnDefaultExpression:
			if e.TableID != columnToDrop.TableID || e.ColumnID != columnToDrop.ColumnID {
				canBeDropped = false
			}
		case *scpb.ColumnOnUpdateExpression:
			if e.TableID != columnToDrop.TableID || e.ColumnID != columnToDrop.ColumnID {
				canBeDropped = false
			}
		case *scpb.UniqueWithoutIndexConstraint, *scpb.ForeignKeyConstraint:
			canBeDropped = false
		case *scpb.CheckConstraint:
			// If the check constraint is from the to-be-dropped, hash-sharded column,
			// then we conclude this (hash-sharded) column be can dropped, even if a
			// check constraint references it.
			if e.TableID == columnToDrop.TableID && e.FromHashShardedColumn && e.ColumnIDs[0] == columnToDrop.ColumnID {
				canBeDropped = true
			} else {
				canBeDropped = false
			}
		case *scpb.View, *scpb.Sequence:
			canBeDropped = false
		case *scpb.SecondaryIndex:
			isOnlyKeySuffixColumn := true
			indexElts := b.QueryByID(columnToDrop.TableID).Filter(publicTargetFilter).Filter(hasIndexIDAttrFilter(e.IndexID))
			scpb.ForEachIndexColumn(indexElts, func(_ scpb.Status, _ scpb.TargetStatus, ic *scpb.IndexColumn) {
				if columnToDrop.ColumnID == ic.ColumnID && ic.Kind != scpb.IndexColumn_KEY_SUFFIX {
					isOnlyKeySuffixColumn = false
				}
			})
			if !isOnlyKeySuffixColumn {
				canBeDropped = false
			}
		}
	})
	return canBeDropped
}

// getAllColumnsNameToIDMapping constructs a name to ID mapping
// for all non-system columns.
func getAllColumnsNameToIDMapping(
	b BuildCtx, tableID catid.DescID,
) (res map[string]catid.ColumnID) {
	res = make(map[string]catid.ColumnID)
	scpb.ForEachColumnName(b.QueryByID(tableID), func(
		current scpb.Status, target scpb.TargetStatus, e *scpb.ColumnName,
	) {
		res[e.Name] = e.ColumnID
	})
	return res
}

// getSortedAllColumnIDsInTable returns sorted IDs of all columns in table.
func getSortedAllColumnIDsInTable(b BuildCtx, tableID catid.DescID) (res []catid.ColumnID) {
	scpb.ForEachColumn(b.QueryByID(tableID), func(_ scpb.Status, _ scpb.TargetStatus, e *scpb.Column) {
		res = append(res, e.ColumnID)
	})
	sort.Slice(res, func(i, j int) bool {
		return res[i] < res[j]
	})
	return res
}

// ensureShardColAndMakeShardDesc ensures that we added the shard column (and
// its check constraint), if the shard column is not already present, and
// construct a sharded descriptor for it.
func ensureShardColAndMakeShardDesc(
	b BuildCtx,
	tbl *scpb.Table,
	columnNames []string,
	shardBuckets tree.Expr,
	storageParams tree.StorageParams,
	n tree.NodeFormatter,
) (*catpb.ShardedDescriptor, catid.ColumnID, catid.ConstraintID) {
	buckets, err := tabledesc.EvalShardBucketCount(b, b.SemaCtx(), b.EvalCtx(), shardBuckets, storageParams)
	if err != nil {
		panic(err)
	}
	shardColName, shardColID, shardColCkConstraintID := maybeCreateAndAddShardCol(b, int(buckets),
		tbl, columnNames, n)
	return &catpb.ShardedDescriptor{
		IsSharded:    true,
		Name:         shardColName,
		ShardBuckets: buckets,
		ColumnNames:  columnNames,
	}, shardColID, shardColCkConstraintID
}
