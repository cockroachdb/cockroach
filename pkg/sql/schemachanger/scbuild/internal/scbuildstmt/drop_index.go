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

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
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
		fmt.Printf("Xiang: Before RetrieveTableWithIndex, table name = %v\n", index.Table.ObjectName)
		fmt.Printf("Xiang: Before RetrieveTableWithIndex, table name prefix = %v\n", index.Table.ObjectNamePrefix)

		tableDesc, idx := b.RetrieveTableWithIndex(b, index, n.IfExists)
		if tableDesc == nil {
			// Early exist if resolved table descriptor is nil without error. This usually
			// happens when the resolve process fails (e.g. index not found) but
			// `IF EXISTS` is set.
			continue
		}
		fmt.Printf("Xiang: After RetrieveTableWithIndex, table name = %v\n", index.Table.ObjectName)
		fmt.Printf("Xiang: After RetrieveTableWithIndex, table name prefix = %v\n", index.Table.ObjectNamePrefix)

		elms := b.ResolveIndex(tableDesc.GetID(), tree.Name(idx.GetName()), ResolveParams{
			IsExistenceOptional: n.IfExists,
			RequiredPrivilege:   privilege.CREATE,
		})

		// Currently, a replacement primary index must be specified when dropping the primary index,
		// and this cannot be done with DROP INDEX.
		if idx.GetID() == tableDesc.GetPrimaryIndexID() {
			panic(errors.WithHint(
				pgerror.Newf(pgcode.FeatureNotSupported, "cannot drop the primary index of a table using DROP INDEX"),
				"instead, use ALTER TABLE ... ALTER PRIMARY KEY or"+
					"use DROP CONSTRAINT ... PRIMARY KEY followed by ADD CONSTRAINT ... PRIMARY KEY in a transaction",
			))
		}

		// TODO (Xiang): Check if requires CCL binary for eventual zone config removal.

		// Cannot drop the index if not CASCADE and a unique constraint depends on it.
		if n.DropBehavior != tree.DropCascade && idx.IsUnique() && !idx.IsCreatedExplicitly() {
			panic(errors.WithHint(
				pgerror.Newf(pgcode.DependentObjectsStillExist,
					"index %q is in use as unique constraint", idx.GetName()),
				"use CASCADE if you really want to drop it.",
			))
		}

		// Attempt to drop dependent views if CASCADE.
		// Panic if not CASCADE but dependent views are found.
		mayDropDependentViews(b, tableDesc, idx, n.DropBehavior)

		// Attempt to drop dependent FK constraints if CASCADE.
		// Panic if not CASCADE but dependent FK constraints are found.
		mayDropDependentFKConstraints(b, tableDesc, idx, n.DropBehavior)

		// Attempt to drop artifacts of a sharded index, including a shard column
		// and a check constraint on that column.
		// We drop those two things if no other index uses the shard column.
		mayDropArtifactsForShardedIndex(b, tableDesc, idx, n.DropBehavior)

		// Attempt to drop artifacts of an expression index, including an expression column.
		// We drop this column if no other index uses this column.
		mayDropArtifactsForExpressionIndex(b, tableDesc, idx, n.DropBehavior)

		// Finally, drop elements of the index itself.
		elms.ForEachElementStatus(func(current scpb.Status, target scpb.TargetStatus, e scpb.Element) {
			if target == scpb.ToAbsent {
				// Initially, all existing elements resolved from the index will have a "ToPublic" target status.
				// So, we can skip those elements that have already been marked to be dropped.
				return
			}
			b.Drop(e)
		})

		// Increment subwork ID so we know exactly which portion in a `DROP INDEX index1, index2, ...` statement
		// is responsible for the creation of the targets.
		b.IncrementSubWorkID()
		b.IncrementSchemaChangeDropCounter("index")
	}
}

// When a sharded index is created, an accompanying column and check constraint
// is created as artifacts. Hence, if we intend to drop a sharded index, we will
// need to drop the accompanying column and check constraint, when no other index
// uses this column.
// resolveAdditionalForShardedIndex resolves this column and the check constraint
// into elements, and append them to `erss`.
func mayDropArtifactsForShardedIndex(
	b BuildCtx, tableDesc catalog.TableDescriptor, idx catalog.Index, dropBehavior tree.DropBehavior,
) {
	if !idx.IsSharded() {
		return
	}

	shardColDesc, err := tableDesc.FindColumnWithName(tree.Name(idx.GetShardColumnName()))
	if err != nil {
		panic(err)
	}

	// Only resolve if no other index is using this column.
	if !shardColDesc.Dropped() && catalog.FindNonDropIndex(tableDesc, func(otherIdx catalog.Index) bool {
		if otherIdx.GetID() == idx.GetID() {
			return false
		}
		colIDs := otherIdx.CollectKeyColumnIDs()
		if !otherIdx.Primary() {
			colIDs.UnionWith(otherIdx.CollectSecondaryStoredColumnIDs())
			colIDs.UnionWith(otherIdx.CollectKeySuffixColumnIDs())
		}
		return colIDs.Contains(shardColDesc.GetID())
	}) == nil {
		// No other existing index uses this sharded index column.

		// Resolve any constraint that uses this column and drop their elements.
		for _, check := range tableDesc.AllActiveAndInactiveChecks() {
			if used, err := tableDesc.CheckConstraintUsesColumn(check, shardColDesc.GetID()); err != nil {
				panic(err)
			} else if used {
				if check.Validity == descpb.ConstraintValidity_Validating {
					panic(pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
						"referencing constraint %q in the middle of being added, try again later", check.Name))
				}
				b.ResolveConstraint(tableDesc.GetID(), tree.Name(check.Name), ResolveParams{
					IsExistenceOptional: false,
					RequiredPrivilege:   privilege.CREATE,
				}).ForEachElementStatus(func(current scpb.Status, target scpb.TargetStatus, e scpb.Element) {
					if target == scpb.ToAbsent {
						// Initially, all existing elements resolved from the index will have a "ToPublic" target status.
						// So, we can skip those elements that have already been marked to be dropped.
						return
					}
					b.Drop(e)
				})
			}
		}

		// Resolve this column and drop its elements.
		b.ResolveColumn(tableDesc.GetID(), tree.Name(idx.GetShardColumnName()), ResolveParams{
			IsExistenceOptional: false,
			RequiredPrivilege:   privilege.CREATE,
		}).ForEachElementStatus(func(current scpb.Status, target scpb.TargetStatus, e scpb.Element) {
			if target == scpb.ToAbsent {
				// Initially, all existing elements resolved from the index will have a "ToPublic" target status.
				// So, we can skip those elements that have already been marked to be dropped.
				return
			}
			b.Drop(e)
		})
	}
}

// When an expression index is created, an accompanying column (virtual, inaccessible)
// is created as artifacts. Hence, if we intend to drop an expression index, we will
// need to drop this accompanying column, when no other index uses this column.
// resolveAdditionalForShardedIndex resolves this column into elements, and append them to `erss`.
func mayDropArtifactsForExpressionIndex(
	b BuildCtx, tableDesc catalog.TableDescriptor, idx catalog.Index, dropBehavior tree.DropBehavior,
) {
	for i, count := 0, idx.NumKeyColumns(); i < count; i++ {
		id := idx.GetKeyColumnID(i)
		col, err := tableDesc.FindColumnWithID(id)
		if err != nil {
			panic(fmt.Sprintf("To-be-dropped index %q contains a column %q that is not found in table %v",
				idx.GetName(), idx.GetKeyColumnName(i), tableDesc.GetName()))
		}

		keyColumnOfOtherIndex := func(colID descpb.ColumnID) bool {
			for _, otherIdx := range tableDesc.AllIndexes() {
				if otherIdx.GetID() == idx.GetID() {
					continue
				}
				if otherIdx.CollectKeyColumnIDs().Contains(colID) {
					return true
				}
			}
			return false
		}

		if col.IsExpressionIndexColumn() && !keyColumnOfOtherIndex(col.GetID()) {
			// If this is the expression index column and it's not used in any other index,
			// resolve it and drop its elements.
			b.ResolveColumn(tableDesc.GetID(), col.ColName(), ResolveParams{
				IsExistenceOptional: false,
				RequiredPrivilege:   privilege.CREATE,
			}).ForEachElementStatus(func(current scpb.Status, target scpb.TargetStatus, e scpb.Element) {
				if target == scpb.ToAbsent {
					// Initially, all existing elements resolved from the index will have a "ToPublic" target status.
					// So, we can skip those elements that have already been marked to be dropped.
					return
				}
				b.Drop(e)
			})
		}
	}

}

func checkIfRequiresCCLBinary(b BuildCtx, tableDesc catalog.TableDescriptor, idx catalog.Index) {

}

func mayDropDependentViews(
	b BuildCtx, tableDesc catalog.TableDescriptor, idx catalog.Index, dropBehavior tree.DropBehavior,
) {
	for _, tableRef := range tableDesc.GetDependedOnBy() {
		if tableRef.IndexID == idx.GetID() {
			if dropBehavior != tree.DropCascade {
				viewDesc := b.ReadDescriptorByID(b, tableRef.ID)
				if viewDesc.DescriptorType() != catalog.Table || !viewDesc.(catalog.TableDescriptor).IsView() {
					panic(fmt.Sprintf("Internal Error: descriptor %v is not a view descriptor.", tableRef.ID))
				}
				panic(errors.WithHintf(
					sqlerrors.NewDependentObjectErrorf("cannot drop index %q because view %q depends on it", idx.GetName(), viewDesc.GetName()),
					"you can drop %q instead.", viewDesc.GetName()))
			} else {
				dropCascadeDescriptor(b, tableRef.ID)
			}
		}
	}
}

// Check whether we need to drop any foreign key constraint when dropping this index.
// Usually, this happens if the index is a unique index and a foreign key
// constraint depends on the unique constraint brought by the unique index. In this case,
// we will first attempt to find a unique constraint as replacement so we don't drop the
// fk constraint. If we cannot find such a replacement, we will also drop that fk constraint,
// provided CASCADE is set.
func mayDropDependentFKConstraints(
	b BuildCtx, tableDesc catalog.TableDescriptor, idx catalog.Index, dropBehavior tree.DropBehavior,
) {
	uniqueConstraintHasReplacementCandidate := func(
		referencedColumnIDs []descpb.ColumnID,
	) bool {
		for _, uc := range tableDesc.AllIndexes() {
			if uc.GetID() == idx.GetID() {
				continue
			}
			if uc.IsValidReferencedUniqueConstraint(referencedColumnIDs) {
				return true
			}
		}
		return false
	}

	err := tableDesc.ForeachInboundFK(func(fk *descpb.ForeignKeyConstraint) error {
		if idx.IsValidReferencedUniqueConstraint(fk.ReferencedColumnIDs) && !uniqueConstraintHasReplacementCandidate(fk.ReferencedColumnIDs) {
			originalTableDesc := b.ReadDescriptorByID(b, fk.OriginTableID)
			if originalTableDesc.DescriptorType() != catalog.Table || !originalTableDesc.(catalog.TableDescriptor).IsTable() {
				return fmt.Errorf("descriptor %v is not a table descriptor", fk.OriginTableID)
			}
			fmt.Printf("Xiang: original table = %v\n", originalTableDesc.GetName())
			// We didn't find a unique constraint replacement.
			// Return error if not CASCADE.
			if dropBehavior != tree.DropCascade {
				return fmt.Errorf("%q is referenced by foreign key from table %q", idx.GetName(), originalTableDesc.GetName())
			}
			fmt.Printf("Xiang: attemping to resolve constraint %v\n", fk.Name)
			// Resolve that fk and drop its elements otherwise.
			b.ResolveConstraint(originalTableDesc.GetID(), tree.Name(fk.Name), ResolveParams{
				IsExistenceOptional: false,
				RequiredPrivilege:   privilege.CREATE,
			}).ForEachElementStatus(func(current scpb.Status, target scpb.TargetStatus, e scpb.Element) {
				if target == scpb.ToAbsent {
					// Initially, all existing elements resolved from the index will have a "ToPublic" target status.
					// So, we can skip those elements that have already been marked to be dropped.
					return
				}
				b.Drop(e)
			})

		}
		return nil
	})

	if err != nil {
		panic(err)
	}
}
