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
	"sort"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/screl"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

func alterTableDropColumn(
	b BuildCtx, tn *tree.TableName, tbl *scpb.Table, n *tree.AlterTableDropColumn,
) {
	checkSafeUpdatesForDropColumn(b)
	checkRowLevelTTLColumn(b, tn, tbl, n)
	checkRegionalByRowColumnConflict(b, tbl, n)
	b.IncrementSchemaChangeAlterCounter("table", "drop_column")
	col, elts, done := resolveColumnForDropColumn(b, tn, tbl, n)
	if done {
		return
	}
	checkColumnNotInaccessible(col, n)
	dropColumn(b, tn, tbl, n, col, elts, n.DropBehavior)
}

func checkSafeUpdatesForDropColumn(b BuildCtx) {
	if !b.SessionData().SafeUpdates {
		return
	}
	err := pgerror.DangerousStatementf("ALTER TABLE DROP COLUMN will " +
		"remove all data in that column")
	if !b.EvalCtx().TxnIsSingleStmt {
		err = errors.WithIssueLink(err, errors.IssueLink{
			IssueURL: "https://github.com/cockroachdb/cockroach/issues/46541",
			Detail: "when used in an explicit transaction combined with other " +
				"schema changes to the same table, DROP COLUMN can result in data " +
				"loss if one of the other schema change fails or is canceled",
		})
	}
	panic(err)
}

func checkRowLevelTTLColumn(
	b BuildCtx, tn *tree.TableName, tbl *scpb.Table, n *tree.AlterTableDropColumn,
) {
	var rowLevelTTL *scpb.RowLevelTTL
	publicTargets := b.QueryByID(tbl.TableID).Filter(publicTargetFilter)
	scpb.ForEachRowLevelTTL(publicTargets, func(
		_ scpb.Status, _ scpb.TargetStatus, e *scpb.RowLevelTTL,
	) {
		rowLevelTTL = e
	})
	if n.Column == colinfo.TTLDefaultExpirationColumnName && rowLevelTTL != nil {
		panic(errors.WithHintf(
			pgerror.Newf(
				pgcode.InvalidTableDefinition,
				`cannot drop column %s while row-level TTL is active`,
				n.Column,
			),
			"use ALTER TABLE %s RESET (ttl) instead",
			tn,
		))
	}
}

func checkRegionalByRowColumnConflict(b BuildCtx, tbl *scpb.Table, n *tree.AlterTableDropColumn) {
	var regionalByRow *scpb.TableLocalityRegionalByRow
	// TODO(ajwerner): Does this need to look at status or target status?
	scpb.ForEachTableLocalityRegionalByRow(b.QueryByID(tbl.TableID), func(
		_ scpb.Status, _ scpb.TargetStatus, e *scpb.TableLocalityRegionalByRow,
	) {
		regionalByRow = e
	})
	if regionalByRow == nil {
		return
	}
	rbrColName := tree.RegionalByRowRegionDefaultColName
	if regionalByRow.As != "" {
		rbrColName = tree.Name(regionalByRow.As)
	}
	if rbrColName == n.Column {
		panic(errors.WithHintf(
			pgerror.Newf(
				pgcode.InvalidColumnReference,
				"cannot drop column %s as it is used to store the region in a REGIONAL BY ROW table",
				n.Column,
			),
			"You must change the table locality before dropping this table or alter the table to use a different column to use for the region.",
		))
	}
	// TODO(ajwerner): Support dropping a column of a REGIONAL BY ROW table.
	panic(scerrors.NotImplementedErrorf(n,
		"regional by row partitioning is not supported"))
}

func resolveColumnForDropColumn(
	b BuildCtx, tn *tree.TableName, tbl *scpb.Table, n *tree.AlterTableDropColumn,
) (col *scpb.Column, elts ElementResultSet, done bool) {
	elts = b.ResolveColumn(tbl.TableID, n.Column, ResolveParams{
		IsExistenceOptional: n.IfExists,
		RequiredPrivilege:   privilege.CREATE,
	})
	var colTargetStatus scpb.TargetStatus
	_, colTargetStatus, col = scpb.FindColumn(elts)
	if col == nil || colTargetStatus == scpb.ToAbsent {
		if !n.IfExists {
			panic(errors.AssertionFailedf("failed to find column %v in %v which was already resolved",
				n.Column, tn))
		}
		return nil, nil, true
	}
	return col, elts, false
}

func checkColumnNotInaccessible(col *scpb.Column, n *tree.AlterTableDropColumn) {
	if col.IsInaccessible {
		panic(pgerror.Newf(
			pgcode.InvalidColumnReference,
			"cannot drop inaccessible column %q",
			n.Column,
		))
	}
}

func dropColumn(
	b BuildCtx,
	tn *tree.TableName,
	tbl *scpb.Table,
	n tree.NodeFormatter,
	col *scpb.Column,
	colElts ElementResultSet,
	behavior tree.DropBehavior,
) {
	_, _, cn := scpb.FindColumnName(colElts)
	_, _, ct := scpb.FindColumnType(colElts)
	checkColumnNotInPrimaryKey(b, col, cn)
	b.Drop(col)
	b.Drop(cn)
	b.Drop(ct)
	if _, _, cc := scpb.FindColumnComment(colElts); cc != nil {
		b.Drop(cc)
	}
	handleDropColumnExpressions(b, colElts, behavior)
	handleDropColumnIndexes(b, tn, col, behavior)
	handleDropColumnComputedColumns(b, tn, cn, tbl, n, col, behavior)
	handleDropColumnUniqueWithoutIndexConstraints(b, col, n)
	handleDropColumnCheckConstraints(b, col, n)
	handleDropColumnForeignKeyConstraintForwardReferences(b, col, n)
	backrefs := undroppedBackrefs(b, col.TableID)
	handleDropColumnViewBackReferences(b, backrefs, col, cn, behavior)
	handleDropColumnForeignKeyConstraintBackReferences(b, cn, backrefs, col, n, behavior)
	if !ct.IsVirtual {
		handleDropColumnPrimaryIndexes(b, tbl, n, col)
	}
	assertAllColumnElementsAreDropped(colElts)
}

func checkColumnNotInPrimaryKey(b BuildCtx, col *scpb.Column, cn *scpb.ColumnName) {
	publicTargets := b.QueryByID(col.TableID).Filter(publicTargetFilter)
	_, _, pi := scpb.FindPrimaryIndex(publicTargets)
	var ic *scpb.IndexColumn
	scpb.ForEachIndexColumn(publicTargets, func(
		_ scpb.Status, _ scpb.TargetStatus, e *scpb.IndexColumn,
	) {
		if e.ColumnID == col.ColumnID &&
			e.IndexID == pi.IndexID &&
			e.Kind == scpb.IndexColumn_KEY {
			ic = e
		}
	})
	if ic != nil {
		panic(sqlerrors.NewColumnReferencedByPrimaryKeyError(cn.Name))
	}
}

func handleDropColumnComputedColumns(
	b BuildCtx,
	tn *tree.TableName,
	cn *scpb.ColumnName,
	tbl *scpb.Table,
	n tree.NodeFormatter,
	col *scpb.Column,
	behavior tree.DropBehavior,
) {
	var toRemove catalog.TableColSet
	allElts := b.QueryByID(col.TableID)
	publicElts := allElts.Filter(publicTargetFilter)
	scpb.ForEachColumnType(publicElts, func(
		_ scpb.Status, _ scpb.TargetStatus, e *scpb.ColumnType,
	) {
		// The below check is defensive given, at the time of writing, we already
		// dropped the column type for the column in question.
		if e.ColumnID == col.ColumnID ||
			// We only care about references in the computed expression.
			e.ComputeExpr == nil {
			return
		}
		if descpb.ColumnIDs.Contains(e.ComputeExpr.ReferencedColumnIDs, col.ColumnID) {
			toRemove.Add(e.ColumnID)
		}
	})
	if !toRemove.Empty() && behavior != tree.DropCascade {
		first, _ := toRemove.Next(0)
		var computedColumnName *scpb.ColumnName
		scpb.ForEachColumnName(publicElts, func(
			_ scpb.Status, _ scpb.TargetStatus, e *scpb.ColumnName,
		) {
			if e.ColumnID == first {
				computedColumnName = e
			}
		})
		panic(sqlerrors.NewColumnReferencedByComputedColumnError(cn.Name, computedColumnName.Name))
	}
	toRemove.ForEach(func(computedColumnID descpb.ColumnID) {
		elts := allElts.Filter(func(_ scpb.Status, _ scpb.TargetStatus, e scpb.Element) bool {
			idI, _ := screl.Schema.GetAttribute(screl.ColumnID, e)
			return idI != nil && idI.(catid.ColumnID) == computedColumnID
		})
		_, _, computedCol := scpb.FindColumn(elts)
		dropColumn(b, tn, tbl, n, computedCol, elts, behavior)
	})
}

func handleDropColumnPrimaryIndexes(
	b BuildCtx, tbl *scpb.Table, n tree.NodeFormatter, col *scpb.Column,
) {
	// For now, disallow adding and dropping columns at the same time.
	// In this case, we may need an intermediate index.
	// TODO(ajwerner): Support mixing adding and dropping columns.
	if addingAnyColumns := !b.QueryByID(tbl.TableID).
		Filter(toPublicNotCurrentlyPublicFilter).
		Filter(isColumnFilter).
		IsEmpty(); addingAnyColumns {
		panic(scerrors.NotImplementedErrorf(n, "DROP COLUMN after ADD COLUMN"))
	}
	existing, freshlyAdded := getPrimaryIndexes(b, tbl.TableID)
	if freshlyAdded != nil {
		handleDropColumnFreshlyAddedPrimaryIndex(b, tbl, freshlyAdded, col)
	} else {
		handleDropColumnCreateNewPrimaryIndex(b, tbl, existing, col)
	}
}

func handleDropColumnCreateNewPrimaryIndex(
	b BuildCtx, tbl *scpb.Table, existing *scpb.PrimaryIndex, col *scpb.Column,
) *scpb.PrimaryIndex {
	return createNewPrimaryIndex(b, tbl, existing, func(
		b BuildCtx, newIndex *scpb.PrimaryIndex, existingColumns []*scpb.IndexColumn,
	) (newColumns []*scpb.IndexColumn) {
		var ic *scpb.IndexColumn
		for _, c := range existingColumns {
			if c.ColumnID == col.ColumnID {
				ic = c
				break
			}
		}
		if ic == nil {
			panic(errors.AssertionFailedf("failed to find column"))
		}
		if ic.Kind != scpb.IndexColumn_STORED {
			panic(errors.AssertionFailedf("can only drop columns which are stored in the primary index, this one is %v ",
				ic.Kind))
		}
		for _, ec := range existingColumns {
			sameKind := ec.Kind == ic.Kind
			if sameKind && ec.OrdinalInKind == ic.OrdinalInKind {
				continue
			}
			cloned := protoutil.Clone(ec).(*scpb.IndexColumn)
			if sameKind && ec.OrdinalInKind > ic.OrdinalInKind {
				cloned.OrdinalInKind--
			}
			cloned.IndexID = newIndex.IndexID
			newColumns = append(newColumns, cloned)
			b.Add(cloned)
		}
		return newColumns
	})
}

func handleDropColumnFreshlyAddedPrimaryIndex(
	b BuildCtx, tbl *scpb.Table, freshlyAdded *scpb.PrimaryIndex, col *scpb.Column,
) {
	// We want to find the freshly added index and go ahead and remove this
	// column from the stored set. That means going through the other
	// index columns for this index and adjusting their ordinal appropriately.
	var storedColumns, storedTempColumns []*scpb.IndexColumn
	var tempIndex *scpb.TemporaryIndex
	scpb.ForEachTemporaryIndex(b.QueryByID(tbl.TableID), func(
		_ scpb.Status, _ scpb.TargetStatus, e *scpb.TemporaryIndex,
	) {
		if e.IndexID == freshlyAdded.TemporaryIndexID {
			tempIndex = e
		}
	})
	if tempIndex == nil {
		panic(errors.AssertionFailedf("failed to find temp index %d", freshlyAdded.TemporaryIndexID))
	}
	scpb.ForEachIndexColumn(b.QueryByID(tbl.TableID).Filter(publicTargetFilter), func(
		_ scpb.Status, _ scpb.TargetStatus, e *scpb.IndexColumn,
	) {
		if e.Kind != scpb.IndexColumn_STORED {
			return
		}
		switch e.IndexID {
		case tempIndex.IndexID:
			storedTempColumns = append(storedTempColumns, e)
		case freshlyAdded.IndexID:
			storedColumns = append(storedColumns, e)
		}
	})
	sort.Slice(storedColumns, func(i, j int) bool {
		return storedColumns[i].OrdinalInKind < storedColumns[j].OrdinalInKind
	})
	sort.Slice(storedColumns, func(i, j int) bool {
		return storedTempColumns[i].OrdinalInKind < storedTempColumns[j].OrdinalInKind
	})
	n := -1
	for i, c := range storedColumns {
		if c.ColumnID == col.ColumnID {
			n = i
			break
		}
	}
	if n == -1 {
		panic(errors.AssertionFailedf("failed to find column %d in index %d", col.ColumnID, freshlyAdded.TemporaryIndexID))
	}
	b.Drop(storedColumns[n])
	b.Drop(storedTempColumns[n])
	for i := n + 1; i < len(storedColumns); i++ {
		storedColumns[i].OrdinalInKind--
		b.Add(storedColumns[i])
		storedTempColumns[i].OrdinalInKind--
		b.Add(storedTempColumns[i])
	}
}

func handleDropColumnUniqueWithoutIndexConstraints(
	b BuildCtx, col *scpb.Column, n tree.NodeFormatter,
) {
	publicTargets := b.QueryByID(col.TableID).Filter(publicTargetFilter)
	var constraints []*scpb.UniqueWithoutIndexConstraint
	scpb.ForEachUniqueWithoutIndexConstraint(publicTargets, func(
		_ scpb.Status, _ scpb.TargetStatus, e *scpb.UniqueWithoutIndexConstraint,
	) {
		if descpb.ColumnIDs(e.ColumnIDs).Contains(col.ColumnID) {
			constraints = append(constraints, e)
		}
	})
	if len(constraints) == 0 {
		return
	}
	// TODO(ajwerner): Support dropping UNIQUE WITHOUT INDEX constraints.
	panic(errors.Wrap(scerrors.NotImplementedError(n),
		"dropping of UNIQUE WITHOUT INDEX constraints not supported"))
}

func handleDropColumnCheckConstraints(b BuildCtx, col *scpb.Column, n tree.NodeFormatter) {
	publicTargets := b.QueryByID(col.TableID).Filter(publicTargetFilter)
	var constraints []*scpb.CheckConstraint
	scpb.ForEachCheckConstraint(publicTargets, func(
		_ scpb.Status, _ scpb.TargetStatus, e *scpb.CheckConstraint,
	) {
		if descpb.ColumnIDs.Contains(e.ReferencedColumnIDs, col.ColumnID) {
			constraints = append(constraints, e)
		}
	})
	if len(constraints) == 0 {
		return
	}
	// TODO(ajwerner): Support dropping CHECK constraints.
	panic(errors.Wrap(scerrors.NotImplementedError(n),
		"dropping of CHECK constraints not supported"))
}

func handleDropColumnForeignKeyConstraintForwardReferences(
	b BuildCtx, col *scpb.Column, n tree.NodeFormatter,
) {
	publicTargets := b.QueryByID(col.TableID).Filter(publicTargetFilter)
	var constraints []*scpb.ForeignKeyConstraint
	scpb.ForEachForeignKeyConstraint(publicTargets, func(
		_ scpb.Status, _ scpb.TargetStatus, e *scpb.ForeignKeyConstraint,
	) {
		if descpb.ColumnIDs.Contains(e.ColumnIDs, col.ColumnID) {
			constraints = append(constraints, e)
		}
	})
	if len(constraints) == 0 {
		return
	}
	// Here we will drop a constraint with or without cascade for outbound
	// constraints.
	// TODO(ajwerner): Support dropping FOREIGN KEY constraints.
	panic(errors.Wrap(scerrors.NotImplementedError(n),
		"dropping of FOREIGN KEY constraints not supported"))
}

func handleDropColumnForeignKeyConstraintBackReferences(
	b BuildCtx,
	cn *scpb.ColumnName,
	backrefs ElementResultSet,
	col *scpb.Column,
	n tree.NodeFormatter,
	behavior tree.DropBehavior,
) {
	publicTargets := b.QueryByID(col.TableID).Filter(publicTargetFilter)
	var constraints []*scpb.ForeignKeyConstraint
	scpb.ForEachForeignKeyConstraint(publicTargets, func(
		_ scpb.Status, _ scpb.TargetStatus, e *scpb.ForeignKeyConstraint,
	) {
		if descpb.ColumnIDs.Contains(e.ReferencedColumnIDs, col.ColumnID) {
			constraints = append(constraints, e)
		}
	})
	scpb.ForEachForeignKeyConstraint(backrefs, func(
		_ scpb.Status, _ scpb.TargetStatus, e *scpb.ForeignKeyConstraint,
	) {
		if descpb.ColumnIDs.Contains(e.ReferencedColumnIDs, col.ColumnID) {
			constraints = append(constraints, e)
		}
	})
	if len(constraints) == 0 {
		return
	}
	if behavior != tree.DropCascade {
		panic(pgerror.Newf(pgcode.DependentObjectsStillExist,
			"cannot drop column %s because other objects depend on it", cn.Name))
	}
	// Here we will drop a constraint with or without cascade for outbound
	// constraints.
	// TODO(ajwerner): Support dropping FOREIGN KEY constraints.
	panic(errors.Wrap(scerrors.NotImplementedError(n),
		"dropping of FOREIGN KEY constraints not supported"))
}

func handleDropColumnExpressions(b BuildCtx, colElts ElementResultSet, behavior tree.DropBehavior) {
	publicTargets := colElts.Filter(publicTargetFilter)
	if _, _, de := scpb.FindColumnDefaultExpression(publicTargets); de != nil {
		b.Drop(de)
	}
	if _, _, ue := scpb.FindColumnOnUpdateExpression(publicTargets); ue != nil {
		b.Drop(ue)
	}

	// Find all the sequences owned by this column and drop them either restrict
	// or cascade. Then, we'll need to check whether these sequences have any
	// other backreferences which have not yet been dropped. Note that we don't
	// need to wait for the other commands in this statement; postgres fails on
	// something like:
	//
	//  create table t (i serial);
	//  alter table t add column j default nextval('t_i_seq'::regclass);
	//  alter table t drop column i, drop column j;
	//  2BP01: cannot drop column i of table t because other objects depend on it
	//
	var undroppedBackrefsToCheck catalog.DescriptorIDSet
	scpb.ForEachSequenceOwner(colElts, func(
		_ scpb.Status, _ scpb.TargetStatus, so *scpb.SequenceOwner,
	) {
		if behavior == tree.DropCascade {
			dropCascadeDescriptor(b, so.SequenceID)
		} else {
			dropRestrictDescriptor(b, so.SequenceID)
			undroppedBackrefsToCheck.Add(so.SequenceID)
			b.Drop(so)
		}
	})
	// TODO(ajwerner): Track the undropped backrefs to populate a detail
	// message like postgres does. For example:
	//
	//  create table t (i serial);
	//  create table t2 (i int default nextval('t_i_seq'::regclass));
	//  drop table t restrict;
	//  ERROR:  cannot drop table t because other objects depend on it
	//  DETAIL:  default value for column i of table t2 depends on sequence t_i_seq
	//  HINT:  Use DROP ... CASCADE to drop the dependent objects too.
	//
	var hasUndroppedBackrefs bool
	undroppedBackrefsToCheck.ForEach(func(seqID descpb.ID) {
		udr := undroppedBackrefs(b, seqID)
		if !udr.IsEmpty() {
			hasUndroppedBackrefs = true
		}
	})
	_, _, cn := scpb.FindColumnName(colElts)
	if hasUndroppedBackrefs {
		panic(pgerror.Newf(pgcode.DependentObjectsStillExist,
			"cannot drop column %s because other objects depend on it", cn.Name))
	}
}

func handleDropColumnViewBackReferences(
	b BuildCtx,
	backrefs ElementResultSet,
	col *scpb.Column,
	cn *scpb.ColumnName,
	dropBehavior tree.DropBehavior,
) {
	var views []*scpb.View
	scpb.ForEachView(backrefs, func(_ scpb.Status, _ scpb.TargetStatus, e *scpb.View) {
		for _, ref := range e.ForwardReferences {
			if ref.ToID == col.TableID &&
				catalog.MakeTableColSet(ref.ColumnIDs...).Contains(col.ColumnID) {
				views = append(views, e)
			}
		}
	})
	if len(views) == 0 {
		return
	}
	if dropBehavior == tree.DropCascade {
		for _, v := range views {
			dropCascadeDescriptor(b, v.ViewID)
		}
		return
	}
	depView := views[0]
	_, _, ns := scpb.FindNamespace(b.QueryByID(col.TableID))
	_, _, nsDep := scpb.FindNamespace(b.QueryByID(depView.ViewID))
	if nsDep.DatabaseID != ns.DatabaseID || nsDep.SchemaID != ns.SchemaID {
		panic(errors.WithHintf(sqlerrors.NewDependentObjectErrorf(
			"cannot drop column %q because view %q depends on it",
			cn.Name, qualifiedName(b, depView.ViewID)),
			"you can drop %s instead.", nsDep.Name))
	}
	panic(sqlerrors.NewDependentObjectErrorf(
		"cannot drop column %q because view %q depends on it",
		cn.Name, nsDep.Name))
}

func handleDropColumnIndexes(
	b BuildCtx, tn *tree.TableName, col *scpb.Column, dropBehavior tree.DropBehavior,
) {
	tableElts := b.QueryByID(col.TableID).Filter(publicTargetFilter)
	var indexIDs catid.IndexSet
	scpb.ForEachIndexColumn(tableElts, func(
		_ scpb.Status, _ scpb.TargetStatus, e *scpb.IndexColumn,
	) {
		if e.ColumnID == col.ColumnID {
			indexIDs.Add(e.IndexID)
		}
	})
	scpb.ForEachSecondaryIndexPartial(tableElts, func(
		current scpb.Status, target scpb.TargetStatus, e *scpb.SecondaryIndexPartial,
	) {
		if descpb.ColumnIDs.Contains(e.ReferencedColumnIDs, col.ColumnID) {
			indexIDs.Add(e.IndexID)
		}
	})
	var secondaryIndexIDs catid.IndexSet
	var indexes []*scpb.SecondaryIndex
	var indexNames []*scpb.IndexName
	scpb.ForEachSecondaryIndex(tableElts, func(
		_ scpb.Status, _ scpb.TargetStatus, e *scpb.SecondaryIndex,
	) {
		if indexIDs.Contains(e.IndexID) {
			secondaryIndexIDs.Add(e.IndexID)
			indexes = append(indexes, e)
		}
	})
	scpb.ForEachIndexName(tableElts, func(
		_ scpb.Status, _ scpb.TargetStatus, e *scpb.IndexName,
	) {
		if secondaryIndexIDs.Contains(e.IndexID) {
			indexNames = append(indexNames, e)
		}
	})
	if len(indexNames) != len(indexes) {
		panic(errors.AssertionFailedf("indexes %v does not match indexNames %v",
			indexes, indexNames))
	}
	sort.Slice(indexes, func(i, j int) bool {
		return indexes[i].IndexID < indexes[j].IndexID
	})
	sort.Slice(indexNames, func(i, j int) bool {
		return indexNames[i].IndexID < indexNames[j].IndexID
	})
	for i, idx := range indexes {
		name := tree.TableIndexName{
			Table: *tn,
			Index: tree.UnrestrictedName(indexNames[i].Name),
		}
		indexElts := tableElts.Filter(func(
			current scpb.Status, target scpb.TargetStatus, e scpb.Element,
		) bool {
			idI, _ := screl.Schema.GetAttribute(screl.IndexID, e)
			return idI != nil && idI.(catid.IndexID) == idx.IndexID
		})
		dropSecondaryIndex(b, &name, dropBehavior, idx, indexElts)
	}
}

func assertAllColumnElementsAreDropped(colElts ElementResultSet) {
	if stillPublic := colElts.Filter(publicTargetFilter); !stillPublic.IsEmpty() {
		var elements []scpb.Element
		stillPublic.ForEachElementStatus(func(_ scpb.Status, _ scpb.TargetStatus, e scpb.Element) {
			elements = append(elements, e)
		})
		panic(errors.AssertionFailedf("failed to drop all of the relevant elements: %v", elements))
	}
}
