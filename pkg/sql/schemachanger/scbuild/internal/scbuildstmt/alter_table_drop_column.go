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
	// Version gates functionally that is implemented after the statement is
	// publicly published.
	fallbackIfAddColDropColAlterPKInOneAlterTableStmtBeforeV232(b, tbl.TableID, n)

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
	tblElts := b.QueryByID(col.TableID).Filter(orFilter(publicTargetFilter, transientTargetFilter))
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

// ensureFourNonNilPrimaryIndexes ensures we have four non nil primary indexes,
// and they have been accordingly dropped and added in the builder state.
// They are constructed from previous ADD COLUMN, DROP COLUMN,
// or ALTER PRIMARY KEY commands. If any of them is nil, it will be created
// in this function by cloning from the previous one, so that when this function
// returns, we have a valid but possibly redundant sequence of primary indexes,
// where each one is sourcing from its precedent, to achieve the schema change.
func ensureFourNonNilPrimaryIndexes(
	b BuildCtx, tableID catid.DescID,
) (oldSpec, inter1Spec, inter2Spec, finalSpec indexSpec) {
	old, inter1, inter2, final := getAllPrimaryIndexesSortedBySourcing(b, tableID)
	// Sanity check: old is never nil
	if old == nil {
		panic(errors.AssertionFailedf("programming error: old primary index is nil"))
	}

	primaryIndexes := []*scpb.PrimaryIndex{old, inter1, inter2, final}
	ret := [4]indexSpec{}
	for i, primaryIndex := range primaryIndexes {
		if i == 0 {
			// We enter this function because we need to add new primary index(es)
			// and we drop `old` here. b.Drop is idempotent so it's fine even if
			// `old` has been dropped previously.
			ret[i] = makeIndexSpec(b, tableID, primaryIndex.IndexID)
			ret[i].apply(b.Drop)
		} else {
			if primaryIndex == nil {
				// From left to right, if a primary index is nil, create one by cloning
				// and sourcing from its predecessor.
				ret[i] = addASwapInIndexByCloningFromSource(b, tableID,
					ret[i-1].primary, i == len(primaryIndexes)-1)
				updateElementsToDependOnNewFromOld(b, tableID,
					ret[i-1].primary.IndexID, ret[i].primary.IndexID,
					catid.MakeIndexIDSet(ret[i].primary.IndexID, ret[i].primary.TemporaryIndexID))
			} else {
				ret[i] = makeIndexSpec(b, tableID, primaryIndex.IndexID)
			}
		}
	}

	// Validate we end up with a valid chain of primary indexes.
	validateAddingPrimaryIndexes(b, tableID)

	return ret[0], ret[1], ret[2], ret[3]
}

// updateElementsToDependOnNewFromOld finds all elements of this table that
// "depend" on index `old`, meaning they use index `old` to either backfill
// data from (for indexes) or check validity against (for constraints), and
// update them to "depend" on index `new`.
//
// Note that this function excludes acting upon indexes whose IDs are in `excludes`.
func updateElementsToDependOnNewFromOld(
	b BuildCtx, tableID catid.DescID, old catid.IndexID, new catid.IndexID, excludes catid.IndexSet,
) {
	b.QueryByID(tableID).
		ForEachElementStatus(func(current scpb.Status, target scpb.TargetStatus, e scpb.Element) {
			switch e := e.(type) {
			case *scpb.PrimaryIndex:
				if e.SourceIndexID == old && !excludes.Contains(e.IndexID) {
					e.SourceIndexID = new
				}
			case *scpb.TemporaryIndex:
				if e.SourceIndexID == old && !excludes.Contains(e.IndexID) {
					e.SourceIndexID = new
				}
			case *scpb.SecondaryIndex:
				if e.SourceIndexID == old && !excludes.Contains(e.IndexID) {
					e.SourceIndexID = new
				}
			case *scpb.CheckConstraint:
				if e.IndexIDForValidation == old {
					e.IndexIDForValidation = new
				}
			case *scpb.ForeignKeyConstraint:
				if e.IndexIDForValidation == old {
					e.IndexIDForValidation = new
				}
			case *scpb.ColumnNotNull:
				if e.IndexIDForValidation == old {
					e.IndexIDForValidation = new
				}
			}
		})
}

func handleDropColumnPrimaryIndexes(
	b BuildCtx, tbl *scpb.Table, n tree.NodeFormatter, col *scpb.Column,
) {
	_, _, _, finalSpec := ensureFourNonNilPrimaryIndexes(b, tbl.TableID)
	dropStoredColumnFromPrimaryIndex(b, tbl.TableID, finalSpec.primary, col)
}

// dropStoredColumnFromPrimaryIndex removes `col` from a primary index `from` and
// its temporary index.
func dropStoredColumnFromPrimaryIndex(
	b BuildCtx, tableID catid.DescID, from *scpb.PrimaryIndex, col *scpb.Column,
) {
	dropIndexColumnFromInternal(b, tableID, from.IndexID, col.ColumnID, scpb.IndexColumn_STORED)
	dropIndexColumnFromInternal(b, tableID, from.TemporaryIndexID, col.ColumnID, scpb.IndexColumn_STORED)
}

// dropIndexColumnFromInternal drops column `columnID` of kind `kind` from
// index `fromID` in the table.
func dropIndexColumnFromInternal(
	b BuildCtx,
	tableID catid.DescID,
	fromID catid.IndexID,
	columnID catid.ColumnID,
	kind scpb.IndexColumn_Kind,
) {
	found := false
	for _, storedCol := range getIndexColumns(b.QueryByID(tableID), fromID, kind) {
		if found {
			// Adjust ordinalInKind for all following index columns
			storedCol.OrdinalInKind--
		}
		if storedCol.ColumnID == columnID {
			// b.Drop effectively undoes adding `storedCol`, either it was
			// previously targeting PUBLIC or TRANSIENT.
			b.Drop(storedCol)
			found = true
		}
	}
	if !found {
		panic(errors.AssertionFailedf("programming error: didn't find column %v fromID "+
			"primary index %v storing columns in table %v", columnID, fromID, tableID))
	}
}

// addASwapInIndexByCloningFromSource adds a primary index `in` that is going
// to swap out `out` yet `in`'s columns are cloned from `out`.
//
// It might sound redundant to do so such thing ("backfilling from an index into
// another of the same columns"). Yes, but it is a prep step to facilitate us
// later to modify columns of `in`, be it an ADD COLUMN, DROP COLUMN,
// or ALTER PRIMARY KEY.
//
// `isInFinal` is set if `in` is going to be the final primary indexes, in which
// case we set its target to PUBLIC. Otherwise, `in`'s target is set to TRANSIENT.
func addASwapInIndexByCloningFromSource(
	b BuildCtx, tableID catid.DescID, out *scpb.PrimaryIndex, isInFinal bool,
) (inSpec indexSpec) {
	outSpec := makeIndexSpec(b, tableID, out.IndexID)

	inColumns := make([]indexColumnSpec, 0)
	fromKeyCols := getIndexColumns(b.QueryByID(tableID), out.IndexID, scpb.IndexColumn_KEY)
	fromStoringCols := getIndexColumns(b.QueryByID(tableID), out.IndexID, scpb.IndexColumn_STORED)
	for _, fromIndexCol := range append(fromKeyCols, fromStoringCols...) {
		inColumns = append(inColumns, makeIndexColumnSpec(fromIndexCol))
	}

	inSpec, inTempSpec := makeSwapIndexSpec(b, outSpec, out.IndexID, inColumns)
	if isInFinal {
		inSpec.apply(b.Add)
	} else {
		inSpec.apply(b.AddTransient)
	}
	inTempSpec.apply(b.AddTransient)
	return inSpec
}

// haveSameIndexColsByKind returns true if two indexes have the same index
// columns of a particular kind.
func haveSameIndexColsByKind(
	b BuildCtx, tableID catid.DescID, indexID1, indexID2 catid.IndexID, kind scpb.IndexColumn_Kind,
) bool {
	tableElems := b.QueryByID(tableID)
	keyCols1 := getIndexColumns(tableElems, indexID1, kind)
	keyCols2 := getIndexColumns(tableElems, indexID2, kind)
	if len(keyCols1) != len(keyCols2) {
		return false
	}
	for i := range keyCols1 {
		if keyCols1[i].ColumnID != keyCols2[i].ColumnID ||
			keyCols1[i].Direction != keyCols2[i].Direction {
			return false
		}
	}
	return true
}

// haveSameIndexCols returns true if two indexes have the same index columns.
func haveSameIndexCols(b BuildCtx, tableID catid.DescID, indexID1, indexID2 catid.IndexID) bool {
	return haveSameIndexColsByKind(b, tableID, indexID1, indexID2, scpb.IndexColumn_KEY) &&
		haveSameIndexColsByKind(b, tableID, indexID1, indexID2, scpb.IndexColumn_KEY_SUFFIX) &&
		haveSameIndexColsByKind(b, tableID, indexID1, indexID2, scpb.IndexColumn_STORED)
}

// getAllPrimaryIndexesSortedBySourcing returns all "adding" primary indexes
// in the table and ensure they are "sorted by sourcing".
//   - "adding" means they are targeting public but currently not public;
//   - "sorted by sourcing" means if primary_index_j's source index ID points
//     to primary_index_i, then primary_index_i comes before primary_index_j.
//
// We conclude that the return has at least 1 and at most 4 primary indexes.
// To facilitate conversation, let's call them (`old`, `inter1`, `inter2`, `final`):
//   - `old` is the original, currently public primary index (it's always going
//     to exist and hence "at least 1").
//   - `inter1` is the newly added primary index that contains all the added/dropped
//     columns in this statement.
//   - `inter2` is same as `inter1` but with altered primary key.
//   - `final` is same as `inter2` but without dropped column.
//
// The following comments explain in what cases would we have 2, 3, or 4 adding
// primary indexes.
//
// Usually, if there is just one add column, or one drop column, or one
// alter primary key in one alter table statement, we will only create one new
// primary index with the correct columns. That would be `final` and we backfill
// it from `old`, that is, (`old`, nil, nil, `final`).
//
// Occasionally, we might need one intermediate primary index. It happens in the
// following two cases:
// 1). `ALTER TABLE t ALTER PRIMARY KEY where old PK is on rowid;`
// 2). `ALTER TALBE t ADD COLUMN, DROP COLUMN;`
// For 1), the intermediate primary index will be one with the altered PK but
// retaining rowid in its storing columns. The final primary index will then be
// one that drops rowid from its storing columns, so, (`old`, nil, `inter2`, `final`).
// For 2), the intermediate primary index will be one with all the added and
// dropped columns in its storing columns. Its final primary index will then be
// one that drops those to-be-dropped columns from its storing columns, so,
// (`old`, `inter1`, nil, `final`).
//
// Rarely, we would encounter something like
// `ALTER TABLE ADD COLUMN, DROP COLUMN, ALTER PRIMARY KEY;`
// To correctly build this statement, we will need two intermediate primary
// indexes where intermediate1 will be one that has all the added and dropped
// columns in its storing columns, and intermediate2 will be one that drops all
// to-be-dropped columns from its storing columns, and `final` will be one with
// altered PK, so, (`old`, `inter1`, `inter2`, `final`).
func getAllPrimaryIndexesSortedBySourcing(
	b BuildCtx, tableID catid.DescID,
) (old, inter1, inter2, final *scpb.PrimaryIndex) {
	// Collect all "adding" primary indexes (i.e. target public currently not public)
	// in this table and sort them by their `SourceIndexID`.
	primaryIndexes := make(map[*scpb.PrimaryIndex]bool)
	scpb.ForEachPrimaryIndex(b.QueryByID(tableID).
		Filter(orFilter(publicTargetFilter, transientTargetFilter)).
		Filter(notReachedTargetYetFilter),
		func(
			current scpb.Status, target scpb.TargetStatus, e *scpb.PrimaryIndex,
		) {
			primaryIndexes[e] = true
		})
	// Collect the existing, public primary index (i.e. `old`).
	_, _, old = scpb.FindPrimaryIndex(b.QueryByID(tableID).Filter(publicStatusFilter))
	primaryIndexes[old] = true

	// The following convoluted logic attempts to sort all adding primary indexes
	// by their SourceIndexID "locationally".
	sortedPrimaryIndexes := make([]*scpb.PrimaryIndex, len(primaryIndexes))
	sources := make(map[catid.IndexID]bool)
	for addingPrimaryIndex := range primaryIndexes {
		sources[addingPrimaryIndex.SourceIndexID] = true
	}
	for len(primaryIndexes) > 0 {
		for primaryIndex := range primaryIndexes {
			if _, ok := sources[primaryIndex.IndexID]; ok {
				// this primary index is currently used as someone else's source.
				continue
			}
			// Find the one that's nobody's source!
			// Put it to `sourtedPrimaryIndexes`, back to front.
			sortedPrimaryIndexes[len(primaryIndexes)-1] = primaryIndex
			delete(primaryIndexes, primaryIndex)
			delete(sources, primaryIndex.SourceIndexID)
			break
		}
	}

	// Sanity check: There should be at least 1, and at most 4 primary indexes.
	if len(sortedPrimaryIndexes) < 1 || len(sortedPrimaryIndexes) > 4 {
		panic(errors.AssertionFailedf("programming error: table %v has %v primary indexes; "+
			"should be between 1 and 4", tableID, len(sortedPrimaryIndexes)))
	}

	switch len(sortedPrimaryIndexes) {
	case 2:
		final = sortedPrimaryIndexes[1]
	case 3:
		final = sortedPrimaryIndexes[2]
		if haveSameIndexColsByKind(b, tableID,
			sortedPrimaryIndexes[0].IndexID, sortedPrimaryIndexes[1].IndexID, scpb.IndexColumn_KEY) {
			inter1 = sortedPrimaryIndexes[1]
		} else {
			inter2 = sortedPrimaryIndexes[1]
		}
	case 4:
		inter1 = sortedPrimaryIndexes[1]
		inter2 = sortedPrimaryIndexes[2]
		final = sortedPrimaryIndexes[3]
	}

	return old, inter1, inter2, final
}

// SortPrimaryIndexesBySourcingLocation sorts all adding primary indexes
// by their SourceIndexID "locationally".
func SortPrimaryIndexesBySourcingLocation(
	primaryIndexes map[*scpb.PrimaryIndex]bool,
) []*scpb.PrimaryIndex {
	// Make a local copy of `primaryIndexes` as we're going to modify `localCopy`.
	localCopy := make(map[*scpb.PrimaryIndex]bool)
	for k, v := range primaryIndexes {
		localCopy[k] = v
	}

	ret := make([]*scpb.PrimaryIndex, len(localCopy))
	sources := make(map[catid.IndexID]bool)
	for addingPrimaryIndex := range localCopy {
		sources[addingPrimaryIndex.SourceIndexID] = true
	}
	for len(localCopy) > 0 {
		for primaryIndex := range localCopy {
			if _, ok := sources[primaryIndex.IndexID]; ok {
				// this primary index is currently used as someone else's source.
				continue
			}
			// Find the one that's nobody's source!
			// Put it to `sourtedPrimaryIndexes`, back to front.
			ret[len(localCopy)-1] = primaryIndex
			delete(localCopy, primaryIndex)
			delete(sources, primaryIndex.SourceIndexID)
			break
		}
	}
	return ret
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
