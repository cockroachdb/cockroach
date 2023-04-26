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

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/errors"
)

func alterTableDropColumn(
	b BuildCtx, tn *tree.TableName, tbl *scpb.Table, n *tree.AlterTableDropColumn,
) {
	fallBackIfSubZoneConfigExists(b, n, tbl.TableID)
	fallBackIfRegionalByRowTable(b, n, tbl.TableID)
	checkSafeUpdatesForDropColumn(b)
	checkRegionalByRowColumnConflict(b, tbl, n)
	col, elts, done := resolveColumnForDropColumn(b, tn, tbl, n)
	if done {
		return
	}
	checkRowLevelTTLColumn(b, tn, tbl, n, col)
	checkColumnNotInaccessible(col, n)
	dropColumn(b, tn, tbl, n, col, elts, n.DropBehavior)
	b.LogEventForExistingTarget(col)
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
	b BuildCtx,
	tn *tree.TableName,
	tbl *scpb.Table,
	n *tree.AlterTableDropColumn,
	colToDrop *scpb.Column,
) {
	var rowLevelTTL *scpb.RowLevelTTL
	publicTargets := b.QueryByID(tbl.TableID).Filter(publicTargetFilter)
	scpb.ForEachRowLevelTTL(publicTargets, func(
		_ scpb.Status, _ scpb.TargetStatus, e *scpb.RowLevelTTL,
	) {
		rowLevelTTL = e
	})

	if rowLevelTTL == nil {
		return
	}
	if rowLevelTTL.DurationExpr != "" && n.Column == catpb.TTLDefaultExpirationColumnName {
		panic(errors.WithHintf(
			pgerror.Newf(
				pgcode.InvalidTableDefinition,
				`cannot drop column %s while ttl_expire_after is set`,
				n.Column,
			),
			"use ALTER TABLE %s RESET (ttl) instead",
			tn,
		))
	}
	if rowLevelTTL.ExpirationExpr != "" {
		expr, err := parser.ParseExpr(string(rowLevelTTL.ExpirationExpr))
		if err != nil {
			// At this point, we should be able to parse the expiration expression.
			panic(errors.WithAssertionFailure(err))
		}
		wrappedExpr := b.WrapExpression(tbl.TableID, expr)
		if descpb.ColumnIDs(wrappedExpr.ReferencedColumnIDs).Contains(colToDrop.ColumnID) {
			panic(errors.WithHintf(
				pgerror.Newf(
					pgcode.InvalidTableDefinition,
					`cannot drop column %q referenced by row-level TTL expiration expression %q`,
					n.Column,
					rowLevelTTL.ExpirationExpr,
				),
				"use ALTER TABLE %s SET (ttl_expiration_expression = ...) to change the expression",
				tn,
			))
		}
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
	// Block drops on system columns.
	panicIfSystemColumn(col, n.Column.String())
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
	var undroppedSeqBackrefsToCheck catalog.DescriptorIDSet
	walkDropColumnDependencies(b, col, func(e scpb.Element) {
		switch e := e.(type) {
		case *scpb.Column:
			if e.TableID == col.TableID && e.ColumnID == col.ColumnID {
				b.Drop(e)
				return
			}
			elts := b.QueryByID(e.TableID).Filter(hasColumnIDAttrFilter(e.ColumnID))
			if behavior != tree.DropCascade {
				_, _, computedColName := scpb.FindColumnName(elts.Filter(publicTargetFilter))
				panic(sqlerrors.NewColumnReferencedByComputedColumnError(cn.Name, computedColName.Name))
			}
			dropColumn(b, tn, tbl, n, e, elts, behavior)
		case *scpb.PrimaryIndex:
			tableElts := b.QueryByID(e.TableID).Filter(publicTargetFilter)
			scpb.ForEachIndexColumn(tableElts, func(_ scpb.Status, _ scpb.TargetStatus, ic *scpb.IndexColumn) {
				if ic.ColumnID == col.ColumnID &&
					e.IndexID == ic.IndexID &&
					ic.Kind == scpb.IndexColumn_KEY {
					panic(sqlerrors.NewColumnReferencedByPrimaryKeyError(cn.Name))
				}
			})
		case *scpb.SecondaryIndex:
			indexElts := b.QueryByID(e.TableID).Filter(hasIndexIDAttrFilter(e.IndexID))
			_, indexTargetStatus, indexName := scpb.FindIndexName(indexElts)
			if indexTargetStatus == scpb.ToAbsent {
				return
			}
			name := tree.TableIndexName{
				Table: *tn,
				Index: tree.UnrestrictedName(indexName.Name),
			}
			dropSecondaryIndex(b, &name, behavior, e)
		case *scpb.View:
			if behavior != tree.DropCascade {
				_, _, ns := scpb.FindNamespace(b.QueryByID(col.TableID))
				_, _, nsDep := scpb.FindNamespace(b.QueryByID(e.ViewID))
				if nsDep.DatabaseID != ns.DatabaseID || nsDep.SchemaID != ns.SchemaID {
					panic(errors.WithHintf(sqlerrors.NewDependentObjectErrorf(
						"cannot drop column %q because view %q depends on it",
						cn.Name, qualifiedName(b, e.ViewID)),
						"you can drop %s instead.", nsDep.Name))
				}
				panic(sqlerrors.NewDependentObjectErrorf(
					"cannot drop column %q because view %q depends on it",
					cn.Name, nsDep.Name))
			}
			dropCascadeDescriptor(b, e.ViewID)
		case *scpb.Sequence:
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
			if behavior == tree.DropCascade {
				dropCascadeDescriptor(b, e.SequenceID)
			} else {
				dropRestrictDescriptor(b, e.SequenceID)
				undroppedSeqBackrefsToCheck.Add(e.SequenceID)
			}
		case *scpb.FunctionBody:
			if behavior != tree.DropCascade {
				_, _, fnName := scpb.FindFunctionName(b.QueryByID(e.FunctionID))
				panic(sqlerrors.NewDependentObjectErrorf(
					"cannot drop column %q because function %q depends on it",
					cn.Name, fnName.Name),
				)
			}
			dropCascadeDescriptor(b, e.FunctionID)
		case *scpb.UniqueWithoutIndexConstraint:
			// Until the appropriate version gate is hit, we still do not allow
			// dropping unique without index constraints.
			if !b.ClusterSettings().Version.IsActive(b, clusterversion.V23_1) {
				panic(scerrors.NotImplementedErrorf(nil, "dropping without"+
					"index constraints is not allowed."))
			}
			constraintElems := b.QueryByID(e.TableID).Filter(hasConstraintIDAttrFilter(e.ConstraintID))
			_, _, constraintName := scpb.FindConstraintWithoutIndexName(constraintElems.Filter(publicTargetFilter))
			alterTableDropConstraint(b, tn, tbl, &tree.AlterTableDropConstraint{
				IfExists:     false,
				Constraint:   tree.Name(constraintName.Name),
				DropBehavior: behavior,
			})
		case *scpb.UniqueWithoutIndexConstraintUnvalidated:
			// Until the appropriate version gate is hit, we still do not allow
			// dropping unique without index constraints.
			if !b.ClusterSettings().Version.IsActive(b, clusterversion.V23_1) {
				panic(scerrors.NotImplementedErrorf(nil, "dropping without"+
					"index constraints is not allowed."))
			}
			constraintElems := b.QueryByID(e.TableID).Filter(hasConstraintIDAttrFilter(e.ConstraintID))
			_, _, constraintName := scpb.FindConstraintWithoutIndexName(constraintElems.Filter(publicTargetFilter))
			alterTableDropConstraint(b, tn, tbl, &tree.AlterTableDropConstraint{
				IfExists:     false,
				Constraint:   tree.Name(constraintName.Name),
				DropBehavior: behavior,
			})
		default:
			b.Drop(e)
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
	undroppedSeqBackrefsToCheck.ForEach(func(seqID descpb.ID) {
		if udr := undroppedBackrefs(b, seqID); !udr.IsEmpty() {
			panic(pgerror.Newf(pgcode.DependentObjectsStillExist,
				"cannot drop column %s because other objects depend on it", cn.Name))
		}
	})
	if _, _, ct := scpb.FindColumnType(colElts); !ct.IsVirtual {
		handleDropColumnPrimaryIndexes(b, tbl, n, col)
	}
	assertAllColumnElementsAreDropped(colElts)
}

func walkDropColumnDependencies(b BuildCtx, col *scpb.Column, fn func(e scpb.Element)) {
	var sequencesToDrop catalog.DescriptorIDSet
	var indexesToDrop catid.IndexSet
	var columnsToDrop catalog.TableColSet
	tblElts := b.QueryByID(col.TableID).Filter(publicTargetFilter)
	// Panic if `col` is referenced in a predicate of an index or
	// unique without index constraint.
	// TODO (xiang): Remove this restriction when #96924 is fixed.
	panicIfColReferencedInPredicate(b, col, tblElts)
	tblElts.
		Filter(referencesColumnIDFilter(col.ColumnID)).
		ForEachElementStatus(func(_ scpb.Status, _ scpb.TargetStatus, e scpb.Element) {
			switch elt := e.(type) {
			case *scpb.Column, *scpb.ColumnName, *scpb.ColumnComment, *scpb.ColumnNotNull,
				*scpb.ColumnDefaultExpression, *scpb.ColumnOnUpdateExpression,
				*scpb.UniqueWithoutIndexConstraint, *scpb.CheckConstraint,
				*scpb.UniqueWithoutIndexConstraintUnvalidated, *scpb.CheckConstraintUnvalidated:
				fn(e)
			case *scpb.ColumnType:
				if elt.ColumnID == col.ColumnID {
					fn(e)
				} else {
					columnsToDrop.Add(elt.ColumnID)
				}
			case *scpb.SequenceOwner:
				fn(e)
				sequencesToDrop.Add(elt.SequenceID)
			case *scpb.SecondaryIndex:
				indexesToDrop.Add(elt.IndexID)
			case *scpb.SecondaryIndexPartial:
				indexesToDrop.Add(elt.IndexID)
			case *scpb.IndexColumn:
				indexesToDrop.Add(elt.IndexID)
			case *scpb.ForeignKeyConstraint:
				if elt.TableID == col.TableID &&
					catalog.MakeTableColSet(elt.ColumnIDs...).Contains(col.ColumnID) {
					fn(e)
				} else if elt.ReferencedTableID == col.TableID &&
					catalog.MakeTableColSet(elt.ReferencedColumnIDs...).Contains(col.ColumnID) {
					fn(e)
				}
			case *scpb.ForeignKeyConstraintUnvalidated:
				if elt.TableID == col.TableID &&
					catalog.MakeTableColSet(elt.ColumnIDs...).Contains(col.ColumnID) {
					fn(e)
				} else if elt.ReferencedTableID == col.TableID &&
					catalog.MakeTableColSet(elt.ReferencedColumnIDs...).Contains(col.ColumnID) {
					fn(e)
				}
			default:
				panic(errors.AssertionFailedf("unknown column-dependent element type %T", elt))
			}
		})
	tblElts.ForEachElementStatus(func(_ scpb.Status, _ scpb.TargetStatus, e scpb.Element) {
		switch elt := e.(type) {
		case *scpb.Column:
			if columnsToDrop.Contains(elt.ColumnID) {
				fn(e)
			}
		case *scpb.PrimaryIndex:
			if indexesToDrop.Contains(elt.IndexID) {
				fn(e)
			}
		case *scpb.SecondaryIndex:
			if indexesToDrop.Contains(elt.IndexID) {
				fn(e)
			}
		}
	})
	sequencesToDrop.ForEach(func(id descpb.ID) {
		_, target, seq := scpb.FindSequence(b.QueryByID(id))
		if target == scpb.ToPublic && seq != nil {
			fn(seq)
		}
	})
	backrefs := undroppedBackrefs(b, col.TableID)
	backrefs.ForEachElementStatus(func(_ scpb.Status, target scpb.TargetStatus, e scpb.Element) {
		switch elt := e.(type) {
		case *scpb.View:
			for _, ref := range elt.ForwardReferences {
				if ref.ToID == col.TableID &&
					catalog.MakeTableColSet(ref.ColumnIDs...).Contains(col.ColumnID) {
					fn(e)
				}
			}
		case *scpb.ForeignKeyConstraint:
			if elt.ReferencedTableID == col.TableID &&
				catalog.MakeTableColSet(elt.ReferencedColumnIDs...).Contains(col.ColumnID) {
				fn(e)
			}
		case *scpb.FunctionBody:
			for _, ref := range elt.UsesTables {
				if ref.TableID == col.TableID && catalog.MakeTableColSet(ref.ColumnIDs...).Contains(col.ColumnID) {
					fn(e)
				}
			}
		}
	})
}

// panicIfColReferencedInPredicate is a temporary fix that disallow dropping a
// column that is referenced in predicate of a partial index or unique without index.
// This restriction shall be lifted once #96924 is fixed.
func panicIfColReferencedInPredicate(b BuildCtx, col *scpb.Column, tblElts ElementResultSet) {
	contains := func(container []catid.ColumnID, target catid.ColumnID) bool {
		for _, elem := range container {
			if elem == target {
				return true
			}
		}
		return false
	}

	var violatingIndex catid.IndexID
	var violatingUWI catid.ConstraintID
	tblElts.ForEachElementStatus(func(_ scpb.Status, _ scpb.TargetStatus, e scpb.Element) {
		if violatingIndex != 0 || violatingUWI != 0 {
			return
		}
		switch elt := e.(type) {
		case *scpb.SecondaryIndex:
			if elt.EmbeddedExpr != nil && contains(elt.EmbeddedExpr.ReferencedColumnIDs, col.ColumnID) {
				violatingIndex = elt.IndexID
			}
		case *scpb.SecondaryIndexPartial:
			if contains(elt.ReferencedColumnIDs, col.ColumnID) {
				violatingIndex = elt.IndexID
			}
		case *scpb.UniqueWithoutIndexConstraint:
			if elt.Predicate != nil && contains(elt.Predicate.ReferencedColumnIDs, col.ColumnID) {
				violatingUWI = elt.ConstraintID
			}
		case *scpb.UniqueWithoutIndexConstraintUnvalidated:
			if elt.Predicate != nil && contains(elt.Predicate.ReferencedColumnIDs, col.ColumnID) {
				violatingUWI = elt.ConstraintID
			}
		}
	})
	if violatingIndex != 0 {
		colNameElem := mustRetrieveColumnNameElem(b, col.TableID, col.ColumnID)
		indexNameElem := mustRetrieveIndexNameElem(b, col.TableID, violatingIndex)
		panic(sqlerrors.NewColumnReferencedByPartialIndex(colNameElem.Name, indexNameElem.Name))
	}
	if violatingUWI != 0 {
		colNameElem := mustRetrieveColumnNameElem(b, col.TableID, col.ColumnID)
		uwiNameElem := mustRetrieveConstraintWithoutIndexNameElem(b, col.TableID, violatingUWI)
		panic(sqlerrors.NewColumnReferencedByPartialUniqueWithoutIndexConstraint(colNameElem.Name, uwiNameElem.Name))
	}
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
		handleDropColumnFreshlyAddedPrimaryIndex(b, freshlyAdded, col)
	} else {
		handleDropColumnCreateNewPrimaryIndex(b, existing, col)
	}
}

func handleDropColumnCreateNewPrimaryIndex(
	b BuildCtx, existing *scpb.PrimaryIndex, col *scpb.Column,
) *scpb.PrimaryIndex {
	out := makeIndexSpec(b, existing.TableID, existing.IndexID)
	inColumns := make([]indexColumnSpec, 0, len(out.columns)-1)
	var dropped *scpb.IndexColumn
	for _, ic := range out.columns {
		if ic.ColumnID == col.ColumnID {
			dropped = ic
		} else {
			inColumns = append(inColumns, makeIndexColumnSpec(ic))
		}
	}
	if dropped == nil {
		panic(errors.AssertionFailedf("failed to find column"))
	}
	if dropped.Kind != scpb.IndexColumn_STORED {
		panic(errors.AssertionFailedf("can only drop columns which are stored in the primary index, this one is %v ",
			dropped.Kind))
	}
	out.apply(b.Drop)
	in, temp := makeSwapIndexSpec(b, out, out.primary.IndexID, inColumns)
	in.apply(b.Add)
	temp.apply(b.AddTransient)
	return in.primary
}

func handleDropColumnFreshlyAddedPrimaryIndex(
	b BuildCtx, freshlyAdded *scpb.PrimaryIndex, col *scpb.Column,
) {
	// We want to find the freshly added index and go ahead and remove this
	// column from the stored set. That means going through the other
	// index columns for this index and adjusting their ordinal appropriately.
	var storedColumns, storedTempColumns []*scpb.IndexColumn
	var tempIndex *scpb.TemporaryIndex
	scpb.ForEachTemporaryIndex(b.QueryByID(freshlyAdded.TableID), func(
		_ scpb.Status, _ scpb.TargetStatus, e *scpb.TemporaryIndex,
	) {
		if e.IndexID == freshlyAdded.TemporaryIndexID {
			tempIndex = e
		}
	})
	if tempIndex == nil {
		panic(errors.AssertionFailedf("failed to find temp index %d", freshlyAdded.TemporaryIndexID))
	}
	scpb.ForEachIndexColumn(b.QueryByID(freshlyAdded.TableID), func(
		_ scpb.Status, targetStatus scpb.TargetStatus, e *scpb.IndexColumn,
	) {
		if targetStatus == scpb.ToAbsent {
			return
		}
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
		return
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

func assertAllColumnElementsAreDropped(colElts ElementResultSet) {
	if stillPublic := colElts.Filter(publicTargetFilter); !stillPublic.IsEmpty() {
		var elements []scpb.Element
		stillPublic.ForEachElementStatus(func(_ scpb.Status, _ scpb.TargetStatus, e scpb.Element) {
			elements = append(elements, e)
		})
		panic(errors.AssertionFailedf("failed to drop all of the relevant elements: %v", elements))
	}
}
