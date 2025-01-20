// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scbuildstmt

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/errors"
)

// TODO (xiang): To remove fallbacks for DROP INDEX we still need to:
//   1. Model adding and dropping FK constraints correctly
//      for dropping an index with dependent FK constraint;
//   2. Once the above two are done, add a test where
//      we drop an index with a dependent FK constraint and a
//      dependent view to end-to-end test, scbuild test, scplan test.
//   3. Check if requires CCL binary for eventual zone config removal.

// DropIndex implements DROP INDEX.
// It resolves the to-be-dropped index into elements and inform `BuildCtx`
// to drop them.
// A considerable amount of effort goes to find the table name of the
// to-be-dropped index, if not specified (e.g. for `DROP INDEX idx`, we need
// to search all tables in current db and in schemas in the search path till
// we first find a table with an index of name `idx`).
func DropIndex(b BuildCtx, n *tree.DropIndex) {
	failIfSafeUpdates(b, n)

	if n.Concurrently {
		b.EvalCtx().ClientNoticeSender.BufferClientNotice(b,
			pgnotice.Newf("CONCURRENTLY is not required as all indexes are dropped concurrently"))
	}

	var anyIndexesDropped bool
	for _, index := range n.IndexList {
		if droppedIndex := maybeDropIndex(b, index, n); droppedIndex != nil {
			b.LogEventForExistingTarget(droppedIndex)
			anyIndexesDropped = true
		}
		// Increment subwork ID so we know exactly which portion in
		// a `DROP INDEX index1, index2, ...` statement is responsible
		// for the creation of the targets.
		b.IncrementSubWorkID()
		b.IncrementSchemaChangeDropCounter("index")

	}
	if anyIndexesDropped {
		b.EvalCtx().ClientNoticeSender.BufferClientNotice(
			b,
			errors.WithHint(
				pgnotice.Newf("the data for dropped indexes is reclaimed asynchronously"),
				"The reclamation delay can be customized in the zone configuration for the table.",
			),
		)
	}
}

// maybeDropIndex resolves `index` and mark its constituent elements as ToAbsent
// in the builder state enclosed by `b`.
func maybeDropIndex(
	b BuildCtx, indexName *tree.TableIndexName, n *tree.DropIndex,
) (droppedIndex *scpb.SecondaryIndex) {
	toBeDroppedIndexElms := b.ResolveIndexByName(indexName, ResolveParams{
		IsExistenceOptional: n.IfExists,
		RequiredPrivilege:   privilege.CREATE,
	})
	if toBeDroppedIndexElms == nil {
		// Attempt to resolve this index failed but `IF EXISTS` is set.
		b.MarkNameAsNonExistent(&indexName.Table)
		return nil
	}
	// Panic if dropping primary index.
	_, _, pie := scpb.FindPrimaryIndex(toBeDroppedIndexElms)
	if pie != nil {
		panic(errors.WithHint(
			pgerror.Newf(pgcode.FeatureNotSupported, "cannot drop the primary index of a table using DROP INDEX"),
			"instead, use ALTER TABLE ... ALTER PRIMARY KEY or"+
				"use DROP CONSTRAINT ... PRIMARY KEY followed by ADD CONSTRAINT ... PRIMARY KEY in a transaction",
		))
	}
	_, _, sie := scpb.FindSecondaryIndex(toBeDroppedIndexElms)
	if sie == nil {
		panic(errors.AssertionFailedf("programming error: cannot find secondary index element."))
	}
	panicIfRegionChangeUnderwayOnRBRTable(b, "DROP INDEX", sie.TableID)
	panicIfSchemaChangeIsDisallowed(b.QueryByID(sie.TableID), n)
	// Cannot drop the index if not CASCADE and a unique constraint depends on it.
	if n.DropBehavior != tree.DropCascade && sie.IsUnique && !sie.IsCreatedExplicitly {
		panic(errors.WithHint(
			pgerror.Newf(pgcode.DependentObjectsStillExist,
				"index %q is in use as unique constraint", indexName.Index.String()),
			"use CASCADE if you really want to drop it.",
		))
	}
	dropSecondaryIndex(b, indexName, n.DropBehavior, sie)
	return sie
}

// dropSecondaryIndex is a helper to drop a secondary index which may be used
// both in DROP INDEX and as a cascade from another operation.
func dropSecondaryIndex(
	b BuildCtx,
	indexName *tree.TableIndexName,
	dropBehavior tree.DropBehavior,
	sie *scpb.SecondaryIndex,
) {
	{
		next := b.WithNewSourceElementID()
		// Maybe drop dependent views.
		// If CASCADE and there are "dependent" views (i.e. views that use this
		// to-be-dropped index), then we will drop all dependent views and their
		// dependents.
		maybeDropDependentViews(next, sie, indexName.Index.String(), dropBehavior)

		// Maybe drop dependent functions.
		maybeDropDependentFunctions(next, sie, indexName.Index.String(), dropBehavior)

		// Maybe drop dependent FK constraints.
		// A PK or unique constraint is required to serve an inbound FK constraint.
		// It is possible that there is an inbound FK constraint 'fk' and it's
		// served by a unique constraint 'uc' that is provided by a unique index 'ui'.
		// In this case, if we were to drop 'ui' and no other unique constraint can be
		// found to replace 'uc' (to continue to serve 'fk'), we will require CASCADE
		//and drop 'fk' as well.
		maybeDropDependentFKConstraints(b, sie.TableID, sie.ConstraintID, string(indexName.Index), dropBehavior,
			func(fkReferencedColIDs []catid.ColumnID) bool {
				return isIndexUniqueAndCanServeFK(b, &sie.Index, fkReferencedColIDs)
			})

		// If shard index, also drop the shard column and all check constraints that
		// uses this shard column if no other index uses the shard column.
		maybeDropAdditionallyForShardedIndex(next, sie, indexName.Index.String(), dropBehavior)

		// If expression index, also drop the expression column if no other index is
		// using the expression column.
		dropAdditionallyForExpressionIndex(next, sie)
	}
	// Finally, drop all elements associated with this index.
	tblElts := b.QueryByID(sie.TableID)
	if sie.TemporaryIndexID != 0 {
		// If this secondary index has an associated temporary index, which happens
		// when it is newly added, but now needs to be dropped (e.g. CREATE INDEX
		// followed by DROP INDEX), we also need to drop the temporary index.
		tblElts = tblElts.
			Filter(orFilter(hasIndexIDAttrFilter(sie.IndexID), hasIndexIDAttrFilter(sie.TemporaryIndexID))).
			Filter(orFilter(publicTargetFilter, transientTargetFilter))
	} else {
		tblElts = tblElts.
			Filter(hasIndexIDAttrFilter(sie.IndexID)).
			Filter(publicTargetFilter)
	}
	tblElts.ForEach(func(_ scpb.Status, _ scpb.TargetStatus, e scpb.Element) {
		b.Drop(e)
	})
}

// maybeDropDependentViews attempts to drop all views that depend
// on the to be dropped index if CASCADE.
// Panic if there is a dependent view but drop behavior is not CASCADE.
func maybeDropDependentViews(
	b BuildCtx,
	toBeDroppedIndex *scpb.SecondaryIndex,
	toBeDroppedIndexName string,
	dropBehavior tree.DropBehavior,
) {
	scpb.ForEachView(b.BackReferences(toBeDroppedIndex.TableID), func(
		current scpb.Status, target scpb.TargetStatus, ve *scpb.View,
	) {
		for _, forwardRef := range ve.ForwardReferences {
			if forwardRef.IndexID != toBeDroppedIndex.IndexID {
				continue
			}

			// This view depends on the to-be-dropped index;
			if dropBehavior != tree.DropCascade {
				// Get view name for the error message
				_, _, ns := scpb.FindNamespace(b.QueryByID(ve.ViewID))
				panic(sqlerrors.NewDependentBlocksOpError("drop", "index", toBeDroppedIndexName, "view", ns.Name))
			} else {
				dropCascadeDescriptor(b, ve.ViewID)
			}
		}
	})
}

func maybeDropDependentFunctions(
	b BuildCtx,
	toBeDroppedIndex *scpb.SecondaryIndex,
	toBeDroppedIndexName string,
	dropBehavior tree.DropBehavior,
) {
	scpb.ForEachFunctionBody(b.BackReferences(toBeDroppedIndex.TableID), func(
		current scpb.Status, target scpb.TargetStatus, e *scpb.FunctionBody,
	) {
		for _, forwardRef := range e.UsesTables {
			if forwardRef.IndexID != toBeDroppedIndex.IndexID {
				continue
			}
			// This view depends on the to-be-dropped index;
			if dropBehavior != tree.DropCascade {
				// Get view name for the error message
				_, _, fnName := scpb.FindFunctionName(b.QueryByID(e.FunctionID))
				panic(sqlerrors.NewDependentBlocksOpError("drop", "index", toBeDroppedIndexName, "function", fnName.Name))
			} else {
				dropCascadeDescriptor(b, e.FunctionID)
			}
		}
	})
}

// maybeDropDependentFKConstraints attempts to drop all inbound, dependent FK constraints,
// as a result of dropping a uniqueness-providing constraint.
//
// If we find a uniqueness-providing replacement, then we don't drop those FKs.
// If we don't and `behavior` is not CASCADE, then we panic.
func maybeDropDependentFKConstraints(
	b BuildCtx,
	tableID catid.DescID,
	toBeDroppedConstraintID catid.ConstraintID,
	toBeDroppedConstraintName string,
	behavior tree.DropBehavior,
	canToBeDroppedConstraintServeFK func([]catid.ColumnID) bool,
) {
	// shouldDropFK returns true if it is a dependent FK and no uniqueness-providing
	// replacement can be found.
	shouldDropFK := func(fkReferencedColIDs []catid.ColumnID) bool {
		return canToBeDroppedConstraintServeFK(fkReferencedColIDs) &&
			!hasColsUniquenessConstraintOtherThan(b, tableID, fkReferencedColIDs, toBeDroppedConstraintID)
	}

	// ensureCascadeBehavior panics if behavior is not cascade.
	ensureCascadeBehavior := func(fkOriginTableID catid.DescID) {
		if behavior != tree.DropCascade {
			_, _, fkOriginTableName := scpb.FindNamespace(b.QueryByID(fkOriginTableID))
			panic(sqlerrors.NewUniqueConstraintReferencedByForeignKeyError(
				toBeDroppedConstraintName, fkOriginTableName.Name))
		}
	}

	// dropDependentFKConstraint is a helper function that drops a dependent
	// FK constraint with ID `fkConstraintID`.
	dropDependentFKConstraint := func(fkTableID catid.DescID, fkConstraintID catid.ConstraintID) {
		b.BackReferences(tableID).Filter(hasTableID(fkTableID)).Filter(hasConstraintIDAttrFilter(fkConstraintID)).
			ForEach(func(
				current scpb.Status, target scpb.TargetStatus, e scpb.Element,
			) {
				b.Drop(e)
			})
	}

	// Iterate over all FKs inbound to this table and decide whether any other
	// unique constraints will satisfy them if we were to drop the current unique
	// constraint.
	b.BackReferences(tableID).Filter(containsDescIDFilter(tableID)).ForEach(func(
		current scpb.Status, target scpb.TargetStatus, e scpb.Element,
	) {
		switch t := e.(type) {
		case *scpb.ForeignKeyConstraint:
			if !shouldDropFK(t.ReferencedColumnIDs) {
				return
			}
			ensureCascadeBehavior(t.TableID)
			dropDependentFKConstraint(t.TableID, t.ConstraintID)
		case *scpb.ForeignKeyConstraintUnvalidated:
			if !shouldDropFK(t.ReferencedColumnIDs) {
				return
			}
			ensureCascadeBehavior(t.TableID)
			dropDependentFKConstraint(t.TableID, t.ConstraintID)
		}
	})
}

// maybeDropAdditionallyForShardedIndex attempts to drop the additional
// shard column if the to-be-dropped index is a shard index and no other
// index uses this shard column.
// If we decide to drop the shard column, we will also drop all check
// constraints that uses this shard column.
func maybeDropAdditionallyForShardedIndex(
	b BuildCtx,
	toBeDroppedIndex *scpb.SecondaryIndex,
	toBeDroppedIndexName string,
	dropBehavior tree.DropBehavior,
) {
	if toBeDroppedIndex.Sharding == nil || !toBeDroppedIndex.Sharding.IsSharded {
		// Only proceed if this is a hash-sharded index.
		return
	}

	shardColElms := b.ResolveColumn(toBeDroppedIndex.TableID, tree.Name(toBeDroppedIndex.Sharding.Name), ResolveParams{
		IsExistenceOptional: false,
		RequiredPrivilege:   privilege.CREATE,
	})
	_, _, scte := scpb.FindColumnType(shardColElms)

	// Do not attempt to drop the shard column if the column is a physical, stored column (as is the case
	// for hash-sharded index created in v21.2 and prior).
	if !scte.IsVirtual && dropBehavior != tree.DropCascade {
		_, _, scne := scpb.FindColumnName(shardColElms)
		_, _, ns := scpb.FindNamespace(b.QueryByID(toBeDroppedIndex.TableID))
		b.EvalCtx().ClientNoticeSender.BufferClientNotice(b,
			pgnotice.Newf("The accompanying shard column %q is a physical column and dropping it can be "+
				"expensive, so, we dropped the index %q but skipped dropping %q. Issue another "+
				"'ALTER TABLE %v DROP COLUMN %v' query if you want to drop column %q.",
				scne.Name, toBeDroppedIndexName, scne.Name, ns.Name, scne.Name, scne.Name),
		)
		return
	}

	// If any other index is using this shard column, do not drop it and just return.
	if anyIndexUsesColOtherThan(b, toBeDroppedIndex.TableID, scte.ColumnID, toBeDroppedIndex.IndexID) {
		return
	}

	// This shard column is not used by any other index. Proceed to drop any check constraints
	// that use this column as well as the column itself.
	scpb.ForEachCheckConstraint(b.QueryByID(toBeDroppedIndex.TableID), func(
		current scpb.Status, target scpb.TargetStatus, e *scpb.CheckConstraint,
	) {
		if !descpb.ColumnIDs(e.ColumnIDs).Contains(scte.ColumnID) {
			return
		}

		// This check constraint uses the shard column. Resolve it and drop its elements.
		constraintElements(b, toBeDroppedIndex.TableID, e.ConstraintID).ForEach(func(
			current scpb.Status, target scpb.TargetStatus, e scpb.Element,
		) {
			if target != scpb.ToAbsent {
				b.Drop(e)
			}
		})
	})

	// Drop the shard column's resolved elements.
	shardColElms.ForEach(func(current scpb.Status, target scpb.TargetStatus, e scpb.Element) {
		if target != scpb.ToAbsent {
			b.Drop(e)
		}
	})
}

// dropAdditionallyForExpressionIndex attempts to drop the additional
// expression column if the to-be-dropped index is an expression index
// and no other index uses this expression column.
func dropAdditionallyForExpressionIndex(b BuildCtx, toBeDroppedIndex *scpb.SecondaryIndex) {
	keyColumnIDs, _, _ := getSortedColumnIDsInIndexByKind(b, toBeDroppedIndex.TableID, toBeDroppedIndex.IndexID)
	scpb.ForEachColumn(b.QueryByID(toBeDroppedIndex.TableID), func(
		current scpb.Status, target scpb.TargetStatus, ce *scpb.Column,
	) {
		if !descpb.ColumnIDs(keyColumnIDs).Contains(ce.ColumnID) {
			return
		}
		if !isExpressionIndexColumn(b, ce) {
			return
		}
		if anyIndexUsesColOtherThan(b, toBeDroppedIndex.TableID, ce.ColumnID, toBeDroppedIndex.IndexID) {
			return
		}

		// This expression column was created when we created the to-be-dropped as an "expression" index.
		// We also know no other index uses this column, so we will need to resolve this column and
		// drop its constituent elements.
		columnElements(b, toBeDroppedIndex.TableID, ce.ColumnID).ForEach(func(
			current scpb.Status, target scpb.TargetStatus, e scpb.Element,
		) {
			if target != scpb.ToAbsent {
				b.Drop(e)
			}
		})
	})
}

// anyIndexUsesColOtherThan returns true if there is another index other than `indexID` in table
// `relationID` that also uses column `colID`.
func anyIndexUsesColOtherThan(
	b BuildCtx, relationID descpb.ID, colID descpb.ColumnID, indexID descpb.IndexID,
) (used bool) {
	// A function that checks whether `ids` contains `colID`.
	hasColID := func(ids []descpb.ColumnID) bool {
		return descpb.ColumnIDs(ids).Contains(colID)
	}

	// Check whether any primary index uses this column.
	scpb.ForEachPrimaryIndex(b.QueryByID(relationID), func(
		_ scpb.Status, _ scpb.TargetStatus, e *scpb.PrimaryIndex,
	) {
		keyColumnIDs, _, _ := getSortedColumnIDsInIndexByKind(b, e.TableID, e.IndexID)
		used = used || (e.IndexID != indexID && hasColID(keyColumnIDs))
	})

	if used {
		return used
	}

	// No primary index uses this column; Check whether any secondary index uses this column.
	scpb.ForEachSecondaryIndex(b.QueryByID(relationID), func(
		_ scpb.Status, _ scpb.TargetStatus, e *scpb.SecondaryIndex,
	) {
		keyColumnIDs, keySuffixColumnIDs, storingColumnIDs := getSortedColumnIDsInIndexByKind(b, e.TableID, e.IndexID)
		used = used || (e.IndexID != indexID &&
			(hasColID(keyColumnIDs) ||
				hasColID(keySuffixColumnIDs) ||
				hasColID(storingColumnIDs)))
	})

	return used
}

// explicitColumnStartIdx returns the first index in which the column is
// explicitly part of the index.
func explicitColumnStartIdx(b BuildCtx, ie *scpb.Index) int {
	start := 0
	scpb.ForEachIndexPartitioning(b.QueryByID(ie.TableID), func(
		current scpb.Status, target scpb.TargetStatus, ipe *scpb.IndexPartitioning,
	) {
		if ipe.TableID == ie.TableID && ipe.IndexID == ie.IndexID {
			if start != 0 {
				panic(fmt.Sprintf("Index (%v) has more than one index partitioning. Should be exactly one", ie.IndexID))
			}
			start = int(ipe.NumImplicitColumns)
		}
	})
	return start
}

// explicitKeyColumnIDsWithoutShardColumn retrieve explicit column ID (excluding shard column)
// of index element `ie`.
func explicitKeyColumnIDsWithoutShardColumn(b BuildCtx, ie *scpb.Index) descpb.ColumnIDs {
	// Retrieve all key column IDs in index `ie`.
	indexKeyColumnIDs, _, _ := getSortedColumnIDsInIndexByKind(b, ie.TableID, ie.IndexID)

	// Exclude implicit key columns, if any.
	explicitColIDs := indexKeyColumnIDs[explicitColumnStartIdx(b, ie):]
	explicitColNames := make([]string, len(explicitColIDs))
	for i, colID := range explicitColIDs {
		_, _, cne := scpb.FindColumnName(columnElements(b, ie.TableID, colID))
		if cne == nil {
			panic(fmt.Sprintf("No column name is found for column ID %v", colID))
		}
		explicitColNames[i] = cne.Name
	}

	// Exclude shard column, if any.
	colIDs := make(descpb.ColumnIDs, 0, len(explicitColIDs))
	for i := range explicitColNames {
		if ie.Sharding == nil || !ie.Sharding.IsSharded || explicitColNames[i] != ie.Sharding.Name {
			colIDs = append(colIDs, explicitColIDs[i])
		}
	}
	return colIDs
}

// isIndexUniqueAndCanServeFK return true if the index `ie` is unique,
// non-partial, and can thus serve a FK that referenced `fkReferencedColIDs`.
func isIndexUniqueAndCanServeFK(
	b BuildCtx, ie *scpb.Index, fkReferencedColIDs []tree.ColumnID,
) bool {
	if !ie.IsUnique {
		return false
	}

	isPartial := false
	scpb.ForEachSecondaryIndexPartial(b.QueryByID(ie.TableID), func(
		current scpb.Status, target scpb.TargetStatus, sipe *scpb.SecondaryIndexPartial,
	) {
		if sipe.TableID == ie.TableID && sipe.IndexID == ie.IndexID {
			isPartial = true
		}
	})
	if isPartial {
		return false
	}

	keyColIDs, _, _ := getSortedColumnIDsInIndexByKind(b, ie.TableID, ie.IndexID)
	implicitKeyColIDs := keyColIDs[:explicitColumnStartIdx(b, ie)]
	explicitKeyColIDsWithoutShardCol := explicitKeyColumnIDsWithoutShardColumn(b, ie)
	allKeyColIDsWithoutShardCol := descpb.ColumnIDs(append(implicitKeyColIDs, explicitKeyColIDsWithoutShardCol...))
	return explicitKeyColIDsWithoutShardCol.PermutationOf(fkReferencedColIDs) ||
		allKeyColIDsWithoutShardCol.PermutationOf(fkReferencedColIDs)
}

// hasColsUniquenessConstraintOtherThan returns true if the table ensures
// uniqueness on `columnIDs` through a constraint other than `otherThan`.
// Uniqueness can be ensured through PK, UNIQUE, or UNIQUE WITHOUT INDEX.
//
// This function can be used to determine, e.g., whether a certain
// uniqueness-providing constraint has a replacement when we want to drop it;
// If yes, we don't need to drop any dependent inbound FKs if not cascade.
func hasColsUniquenessConstraintOtherThan(
	b BuildCtx, tableID descpb.ID, columnIDs []descpb.ColumnID, otherThan descpb.ConstraintID,
) (ret bool) {
	b.QueryByID(tableID).Filter(publicTargetFilter).Filter(publicStatusFilter).
		ForEach(func(
			current scpb.Status, target scpb.TargetStatus, e scpb.Element,
		) {
			if ret {
				return
			}
			switch t := e.(type) {
			case *scpb.PrimaryIndex:
				if t.ConstraintID != otherThan && isIndexUniqueAndCanServeFK(b, &t.Index, columnIDs) {
					ret = true
				}
			case *scpb.SecondaryIndex:
				if t.ConstraintID != otherThan && isIndexUniqueAndCanServeFK(b, &t.Index, columnIDs) {
					ret = true
				}
			case *scpb.UniqueWithoutIndexConstraint:
				if t.ConstraintID != otherThan && descpb.ColumnIDs(t.ColumnIDs).PermutationOf(columnIDs) {
					ret = true
				}
			}
		})
	return ret
}

func isExpressionIndexColumn(b BuildCtx, ce *scpb.Column) bool {
	isVirtual := false
	scpb.ForEachColumnType(b.QueryByID(ce.TableID), func(
		current scpb.Status, target scpb.TargetStatus, cte *scpb.ColumnType,
	) {
		if cte.TableID == ce.TableID && cte.ColumnID == ce.ColumnID {
			isVirtual = cte.IsVirtual
		}
	})

	return ce.IsInaccessible && isVirtual
}
