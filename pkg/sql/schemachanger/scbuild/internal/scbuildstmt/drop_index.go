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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/errors"
)

// DropIndex implements DROP INDEX.
// It resolves the to-be-dropped index into elements and inform `BuildCtx` to drop them.
// A considerable amount of effort goes to find the table name of the to-be-dropped index,
// if not specified (e.g. for `DROP INDEX idx`, we need to search all tables in current db
// and schema that has an index with name `idx`).
func DropIndex(b BuildCtx, n *tree.DropIndex) {
	if n.Concurrently {
		b.EvalCtx().ClientNoticeSender.BufferClientNotice(b,
			pgnotice.Newf("CONCURRENTLY is not required as all indexes are dropped concurrently"))
	}

	for _, index := range n.IndexList {
		toBeDroppedIndexElms := b.ResolveTableIndexBestEffort(index, ResolveParams{
			IsExistenceOptional: n.IfExists,
			RequiredPrivilege:   privilege.CREATE,
		})
		if toBeDroppedIndexElms == nil {
			// Attempt to resolve this index failed but `IF EXISTS` is set.
			b.MarkNameAsNonExistent(&index.Table)
			continue
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

		// TODO (Xiang): Check if requires CCL binary for eventual zone config removal.

		// Get the to-be-dropped secondary index element and its name element.
		_, _, sie := scpb.FindSecondaryIndex(toBeDroppedIndexElms)
		if sie == nil {
			panic("cannot find secondary index after resolve. Check resolve logic!")
		}
		_, _, sine := scpb.FindIndexName(toBeDroppedIndexElms)
		if sine == nil {
			panic("cannot find secondary index name after resolve. Check resolve logic!")
		}

		// Cannot drop the index if not CASCADE and a unique constraint depends on it.
		if n.DropBehavior != tree.DropCascade && sie.IsUnique && !sie.IsCreatedExplicitly {
			panic(errors.WithHint(
				pgerror.Newf(pgcode.DependentObjectsStillExist,
					"index %q is in use as unique constraint", sine.Name),
				"use CASCADE if you really want to drop it.",
			))
		}

		// May drop dependent views.
		mayDropDependentViews(b, toBeDroppedIndexElms, n.DropBehavior)

		// May drop dependent FK constraints.
		// A PK or unique constraint is required to serve an inbound FK constraint.
		// It is possible that there is an inbound FK constraint 'fk' and it's served by a unique constraint 'uc' that is
		// provided by a unique index 'ui'. In this case, if we were to drop 'ui' and no other unique constraint can be
		// found to replace 'uc' (to continue to serve 'fk'), we will require CASCADE and drop 'fk' as well.
		mayDropDependentFKConstraints(b, toBeDroppedIndexElms, n.DropBehavior)

		// If shard index, also drop the shard column and all check constraints that uses this shard column if no
		// other index uses the shard column.
		mayDropAdditionallyForShardedIndex(b, toBeDroppedIndexElms, n.DropBehavior)

		// If expression index, also drop the expression column if no other index is using the expression column.
		// for each key column of the index, resolve this
		dropAdditionallyForExpressionIndex(b, toBeDroppedIndexElms)

		// Finally, drop the index's resolved elements.
		toBeDroppedIndexElms.ForEachElementStatus(func(current scpb.Status, target scpb.TargetStatus, e scpb.Element) {
			if target == scpb.ToAbsent {
				// Initially, all existing elements resolved from the index will have a "ToPublic" target status.
				// So, we can skip those elements that have already been marked to be dropped.
				return
			}
			b.Drop(e)
		})

		b.EvalCtx().ClientNoticeSender.BufferClientNotice(
			b,
			errors.WithHint(
				pgnotice.Newf("the data for dropped indexes is reclaimed asynchronously"),
				"The reclamation delay can be customized in the zone configuration for the table.",
			),
		)

		// Increment subwork ID so we know exactly which portion in a `DROP INDEX index1, index2, ...` statement
		// is responsible for the creation of the targets.
		b.IncrementSubWorkID()
		b.IncrementSchemaChangeDropCounter("index")
	}
}

func explicitColumnStartIdx(b BuildCtx, ie *scpb.Index) int {
	start := 0
	b.QueryByID(ie.TableID).ForEachElementStatus(func(current scpb.Status, target scpb.TargetStatus, e scpb.Element) {
		switch t := e.(type) {
		case *scpb.IndexPartitioning:
			if t.TableID == ie.TableID && t.IndexID == ie.IndexID {
				if start != 0 {
					panic(fmt.Sprintf("Index (%v) has more than one index partitioning. Should be exactly one", ie.IndexID))
				}
				start = int(t.NumImplicitColumns)
			}
		}
	})
	// Currently, we only allow implicit partitioning on hash sharded index. When
	// that happens, the shard column always comes after implicit partition
	// columns.
	if ie.Sharding != nil && ie.Sharding.IsSharded {
		start++
	}
	return start
}

// explicitColumnIDsWithoutShardColumn retrieve explicit column ID (excluding shard column)
// of index element `e`.
func explicitColumnIDsWithoutShardColumn(b BuildCtx, ie *scpb.Index) descpb.ColumnIDs {
	explicitColIDs := ie.KeyColumnIDs[explicitColumnStartIdx(b, ie):]
	explicitColNames := make([]string, len(explicitColIDs))
	for i, colID := range explicitColIDs {
		colElms := b.ResolveColumnByID(ie.TableID, colID, ResolveParams{
			IsExistenceOptional: false,
			RequiredPrivilege:   privilege.CREATE,
		})
		_, _, cne := scpb.FindColumnName(colElms)
		if cne == nil {
			panic(fmt.Sprintf("No column name is found for column ID %v", colID))
		}
		explicitColNames[i] = cne.Name
	}

	colIDs := make(descpb.ColumnIDs, 0, len(explicitColIDs))
	for i := range explicitColNames {
		if ie.Sharding == nil || !ie.Sharding.IsSharded || explicitColNames[i] != ie.Sharding.Name {
			colIDs = append(colIDs, explicitColIDs[i])
		}
	}
	return colIDs
}

// isValidReferencedUniqueConstraint return true if `e` is a unique index and this index's
// explicit columns are a permutation of `referencedColIDs`.
func isValidReferencedUniqueConstraint(
	b BuildCtx, ie *scpb.Index, referencedColIDs []tree.ColumnID,
) bool {
	return ie.IsUnique && !ie.IsPartial &&
		explicitColumnIDsWithoutShardColumn(b, ie).PermutationOf(referencedColIDs)
}

// uniqueConstraintHasReplacementCandidate returns true if `elms` contains an index
// that can serve as a replacement candidate for the to-be-dropped secondary index `sie`, which
// references columns `referencedColumnIDs`.
func uniqueConstraintHasReplacementCandidate(
	b BuildCtx,
	elms ElementResultSet,
	sie *scpb.SecondaryIndex,
	referencedColumnIDs []descpb.ColumnID,
) bool {
	result := false

	// Check all indexes (both primary and secondary) to see if we can find a replacement candidate.
	elms.ForEachElementStatus(func(current scpb.Status, target scpb.TargetStatus, e scpb.Element) {
		if result {
			return
		}

		switch t := e.(type) {
		case *scpb.SecondaryIndex:
			if t.IndexID != sie.IndexID && isValidReferencedUniqueConstraint(b, &t.Index, referencedColumnIDs) {
				result = true
			}
		case *scpb.PrimaryIndex:
			if isValidReferencedUniqueConstraint(b, &t.Index, referencedColumnIDs) {
				result = true
			}
		}
	})

	return result
}

// Attempt to drop all views that depend on the to be dropped index if CASCADE.
// Panic if there is a dependent view but drop behavior is not CASCADE.
func mayDropDependentViews(
	b BuildCtx, toBeDroppedSecondaryIndexElms ElementResultSet, dropBehavior tree.DropBehavior,
) {
	_, _, sie := scpb.FindSecondaryIndex(toBeDroppedSecondaryIndexElms)
	_, _, sine := scpb.FindIndexName(toBeDroppedSecondaryIndexElms)
	if sie == nil || sine == nil {
		panic("invalid toBeDroppedSecondaryIndexElms; does not contain a secondary index element.")
	}

	scpb.ForEachView(b.BackReferences(sie.TableID), func(current scpb.Status, target scpb.TargetStatus, ve *scpb.View) {
		for _, forwardRef := range ve.ForwardReferences {
			if forwardRef.IndexID == sie.IndexID {
				// This view depends on the to-be-dropped index;
				if dropBehavior != tree.DropCascade {
					// Get view name for panic message
					_, _, ns := scpb.FindNamespace(b.QueryByID(ve.ViewID))
					panic(errors.WithHintf(
						sqlerrors.NewDependentObjectErrorf("cannot drop index %q because view %q depends on it",
							sine.Name, ns.Name), "you can drop %q instead.", ns.Name))
				} else {
					dropCascadeDescriptor(b, ve.ViewID)
				}
			}
		}
	})
}

// Attempt to drop all FK constraints that depend on the to be dropped index if CASCADE.
// A FK constraint can only exist if there is `PRIMARY KEY` or `UNIQUE` constraint on the referenced columns
// in the child table.
// This is relevant if we're dropping a unique index whose `UNIQUE` constraints serves some FK constraints from
// other tables. In this case, we attempt to find a replacement constraint to serve this FK constraint. If we can,
// then we can proceed to drop the index. Otherwise, we will need to drop the FK constraint as well (if CASCADE of
// course).
// Panic if there is a dependent view but drop behavior is not CASCADE.
func mayDropDependentFKConstraints(
	b BuildCtx, toBeDroppedSecondaryIndexElms ElementResultSet, dropBehavior tree.DropBehavior,
) {
	_, _, sie := scpb.FindSecondaryIndex(toBeDroppedSecondaryIndexElms)
	_, _, sine := scpb.FindIndexName(toBeDroppedSecondaryIndexElms)
	if sie == nil || sine == nil {
		panic("invalid toBeDroppedSecondaryIndexElms; does not contain a secondary index element.")
	}

	scpb.ForEachForeignKeyConstraint(b.BackReferences(sie.TableID),
		func(current scpb.Status, target scpb.TargetStatus, e *scpb.ForeignKeyConstraint) {
			if isValidReferencedUniqueConstraint(b, &sie.Index, e.ReferencedColumnIDs) &&
				!uniqueConstraintHasReplacementCandidate(b, b.QueryByID(sie.TableID), sie, e.ReferencedColumnIDs) {
				// a replacement candidate to serve this dependent FK is not found;
				if dropBehavior != tree.DropCascade {
					_, _, ns := scpb.FindNamespace(b.QueryByID(sie.TableID))
					panic(fmt.Errorf("%q is referenced by foreign key from table %q", sine.Name, ns.Name))
				}
				// Resolve that fk and drop its elements otherwise.
				b.ResolveConstraintByID(e.TableID, e.ConstraintID, ResolveParams{
					IsExistenceOptional: false,
					RequiredPrivilege:   privilege.CREATE,
				}).ForEachElementStatus(func(current scpb.Status, target scpb.TargetStatus, e scpb.Element) {
					if target == scpb.ToAbsent {
						return
					}
					b.Drop(e)
				})
			}
		})
}

// If dropping a sharded index, we will need to drop the additional shard column and check
// constraints that uses  this shard column, if no other index is using this shard column.
func mayDropAdditionallyForShardedIndex(
	b BuildCtx, toBeDroppedSecondaryIndexElms ElementResultSet, dropBehavior tree.DropBehavior,
) {
	_, _, sie := scpb.FindSecondaryIndex(toBeDroppedSecondaryIndexElms)
	_, _, sine := scpb.FindIndexName(toBeDroppedSecondaryIndexElms)
	if sie == nil || sine == nil {
		panic("invalid toBeDroppedSecondaryIndexElms; does not contain a secondary index element.")
	}

	if sie.Sharding == nil || !sie.Sharding.IsSharded {
		// Only proceed if this is a hash-sharded index.
		return
	}

	shardColElms := b.ResolveColumn(sie.TableID, tree.Name(sie.Sharding.Name), ResolveParams{
		IsExistenceOptional: false,
		RequiredPrivilege:   privilege.CREATE,
	})
	_, _, sce := scpb.FindColumn(shardColElms)

	// Do not attempt to drop the shard column if the column is a physical, stored column (as is the case
	// for hash-sharded index created in v21.2 and prior).
	if !sce.IsVirtual && dropBehavior != tree.DropCascade {
		_, _, scne := scpb.FindColumnName(shardColElms)
		_, _, ns := scpb.FindNamespace(b.QueryByID(sie.TableID))
		b.EvalCtx().ClientNoticeSender.BufferClientNotice(b,
			pgnotice.Newf("The accompanying shard column %q is a physical column and dropping it can be "+
				"expensive, so, we dropped the index %q but skipped dropping %q. Issue another "+
				"'ALTER TABLE %v DROP COLUMN %v' query if you want to drop column %q.",
				scne.Name, sine.Name, scne.Name, ns.Name, scne.Name, scne.Name),
		)
		return
	}

	// Only proceed to drop this column and checks if no other index is using this column.
	if !anyIndexUsesColOtherThan(b, sie.TableID, sce.ColumnID, sie.IndexID) {
		// This shard column is not used by any other index. Proceed to drop any check constraints
		// that use this column as well as the column itself.
		scpb.ForEachCheckConstraint(b.QueryByID(sie.TableID),
			func(current scpb.Status, target scpb.TargetStatus, e *scpb.CheckConstraint) {
				for _, colID := range e.ColumnIDs {
					if colID == sce.ColumnID {
						// This check constraint uses the shard column. Resolve it and drop its elements.
						b.ResolveConstraintByID(sie.TableID, e.ConstraintID, ResolveParams{
							IsExistenceOptional: false,
							RequiredPrivilege:   privilege.CREATE,
						}).ForEachElementStatus(func(current scpb.Status, target scpb.TargetStatus, e scpb.Element) {
							if target == scpb.ToAbsent {
								return
							}
							b.Drop(e)
						})
						return
					}
				}
			})

		// Drop the shard column's resolved elements.
		shardColElms.ForEachElementStatus(func(current scpb.Status, target scpb.TargetStatus, e scpb.Element) {
			if target == scpb.ToAbsent {
				return
			}
			b.Drop(e)
		})
	}
}

// If dropping an expression index, we will need to drop the additional expression column, if no other
// index is using this expression column.
func dropAdditionallyForExpressionIndex(
	b BuildCtx, toBeDroppedSecondaryIndexElms ElementResultSet,
) {
	_, _, sie := scpb.FindSecondaryIndex(toBeDroppedSecondaryIndexElms)
	_, _, sine := scpb.FindIndexName(toBeDroppedSecondaryIndexElms)
	if sie == nil || sine == nil {
		panic("invalid toBeDroppedSecondaryIndexElms; does not contain a secondary index element.")
	}

	scpb.ForEachColumn(b.QueryByID(sie.TableID), func(current scpb.Status, target scpb.TargetStatus, ce *scpb.Column) {
		columnInIndexKeyCols := false
		for _, colID := range sie.KeyColumnIDs {
			if colID == ce.ColumnID {
				columnInIndexKeyCols = true
				break
			}
		}

		if columnInIndexKeyCols && ce.IsInaccessible && ce.IsVirtual {
			// this column is an expression column and used in the to-be-dropped index.
			// We will need to drop this expression column as well if no other index uses it.
			used := anyIndexUsesColOtherThan(b, sie.TableID, ce.ColumnID, sie.IndexID)
			if !used {
				// No other index is using this expression column. Resolve it and drop its elements.
				b.ResolveColumnByID(sie.TableID, ce.ColumnID, ResolveParams{
					IsExistenceOptional: false,
					RequiredPrivilege:   privilege.CREATE,
				}).ForEachElementStatus(func(current scpb.Status, target scpb.TargetStatus, e scpb.Element) {
					if target == scpb.ToAbsent {
						return
					}
					b.Drop(e)
				})
			}

		}
	})
}

// anyIndexUsesColOtherThan returns true if there is another index other than `currentIndexID` in table
// `relationID` that also uses column `colID`.
func anyIndexUsesColOtherThan(
	b BuildCtx, relationID descpb.ID, colID descpb.ColumnID, indexID descpb.IndexID,
) bool {
	used := false
	// Check all primary indexes.
	scpb.ForEachPrimaryIndex(b.QueryByID(relationID),
		func(current scpb.Status, target scpb.TargetStatus, e *scpb.PrimaryIndex) {
			if used || e.IndexID == indexID {
				return
			}
			for _, pkColID := range e.KeyColumnIDs {
				if pkColID == colID {
					used = true
					return
				}
			}
		})
	// If no primary indexes use this col, check all secondary indexes.
	if !used {
		scpb.ForEachSecondaryIndex(b.QueryByID(relationID),
			func(current scpb.Status, target scpb.TargetStatus, e *scpb.SecondaryIndex) {
				if used || e.IndexID == indexID {
					return
				}
				for _, siColID := range e.KeyColumnIDs {
					if siColID == colID {
						used = true
						return
					}
				}
				for _, siColID := range e.KeySuffixColumnIDs {
					if siColID == colID {
						used = true
						return
					}
				}
				for _, siColID := range e.StoringColumnIDs {
					if siColID == colID {
						used = true
						return
					}
				}
			})
	}
	return used
}
