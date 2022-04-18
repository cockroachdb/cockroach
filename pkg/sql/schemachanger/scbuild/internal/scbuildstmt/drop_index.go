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
		/*********************************************************************
		  Step I: Retrieve the table descriptor that has this index and the index itself.
		   *********************************************************************/
		tableDesc, idx := b.RetrieveTableWithIndex(b, index, n.IfExists)
		if tableDesc == nil {
			// Early exist if resolved table descriptor is nil without error. This usually
			// happens when the resolve process fails (e.g. index not found) but
			// `IF EXISTS` is set.
			continue
		}

		// Resolve this index into its constituent elements.
		erss := []ElementResultSet{b.ResolveIndex(tableDesc.GetID(), tree.Name(idx.GetName()), ResolveParams{
			IsExistenceOptional: n.IfExists,
			RequiredPrivilege:   privilege.CREATE,
		})}

		/*********************************************************************
		  Step II: Check eligibility. It contains all the early exists.
		  					1. Cannot drop primary index;
		  					2. If not CASCADE,
		  							2.1. cannot drop index with dependent unique constraint;
		  							2.2. cannot drop index with dependent view;
		   *********************************************************************/
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

		if n.DropBehavior != tree.DropCascade {
			// Cannot drop the index if a unique constraint depends on it.
			if idx.IsUnique() && !idx.IsCreatedExplicitly() {
				panic(errors.WithHint(
					pgerror.Newf(pgcode.DependentObjectsStillExist,
						"index %q is in use as unique constraint", idx.GetName()),
					"use CASCADE if you really want to drop it.",
				))
			}

			// Cannot drop the index if a view depends on it.
			for _, tableRef := range tableDesc.GetDependedOnBy() {
				if tableRef.IndexID == idx.GetID() {
					viewDesc := b.ReadDescriptorByID(b, tableRef.ID)
					if viewDesc.DescriptorType() != catalog.Table || !viewDesc.(catalog.TableDescriptor).IsView() {
						panic(fmt.Sprintf("Internal Error: descriptor %v is not a view descriptor.", tableRef.ID))
					}
					panic(errors.WithHintf(
						sqlerrors.NewDependentObjectErrorf("cannot drop index %q because view %q depends on it", index.Index.String(), viewDesc.GetName()),
						"you can drop %q instead.", viewDesc.GetName()))
				}
			}
		}

		/*********************************************************************
		      Step III: Drop relevant elements.
		      					1. Drop elements resolved from the index.
		  							2. In addition,
		    							2.1. if sharded index, also drop the sharded column and
		    										the check, if no other index uses that column;
		    							2.2. if expression index, also drop the expression
		    										index column if no other index uses that column;
		      					3. If CASCADE,
		      						2.1. drop all dependent views
		 **************************************************************************/
		// Resolve additional things (see comment above) if we attempt to drop a sharded index.
		resolveAdditionallyForShardedIndex(b, tableDesc, idx, &erss)

		// Resolve additional things (see comment above) if we attempt to drop an expression index.
		resolveAdditionallyForExpressionIndex(b, tableDesc, idx, &erss)

		// Inform to drop all the relevant, resolved elements.
		for _, ers := range erss {
			ers.ForEachElementStatus(func(current scpb.Status, target scpb.TargetStatus, e scpb.Element) {
				if target == scpb.ToAbsent {
					// Initially, all existing elements resolved from the index will have a "ToPublic" target status.
					// So, we can skip those elements that have already been marked to be dropped.
					return
				}
				b.Drop(e)
			})
		}

		// Additional things to drop if CASCADE is set.
		if n.DropBehavior == tree.DropCascade {
			// Inform `BuildCtx` to drop all dependent views.
			for _, tableRef := range tableDesc.GetDependedOnBy() {
				if tableRef.IndexID == idx.GetID() {
					dropCascadeDescriptor(b, tableRef.ID)
				}
			}
		}

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
func resolveAdditionallyForShardedIndex(
	b BuildCtx, tableDesc catalog.TableDescriptor, idx catalog.Index, erss *[]ElementResultSet,
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

		// Resolve any constraint that uses this column.
		for _, check := range tableDesc.AllActiveAndInactiveChecks() {
			if used, err := tableDesc.CheckConstraintUsesColumn(check, shardColDesc.GetID()); err != nil {
				panic(err)
			} else if used {
				if check.Validity == descpb.ConstraintValidity_Validating {
					panic(pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
						"referencing constraint %q in the middle of being added, try again later", check.Name))
				}
				*erss = append(*erss, b.ResolveConstraint(tableDesc.GetID(), tree.Name(check.Name), ResolveParams{
					IsExistenceOptional: false,
					RequiredPrivilege:   privilege.CREATE,
				}))
			}
		}

		// Resolve this column.
		*erss = append(*erss, b.ResolveColumn(tableDesc.GetID(), tree.Name(idx.GetShardColumnName()), ResolveParams{
			IsExistenceOptional: false,
			RequiredPrivilege:   privilege.CREATE,
		}))
	}
}

// When an expression index is created, an accompanying column (virtual, inaccessible)
// is created as artifacts. Hence, if we intend to drop an expression index, we will
// need to drop this accompanying column, when no other index uses this column.
// resolveAdditionalForShardedIndex resolves this column into elements, and append them to `erss`.
func resolveAdditionallyForExpressionIndex(
	b BuildCtx, tableDesc catalog.TableDescriptor, idx catalog.Index, erss *[]ElementResultSet,
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
			// resolve it so we can drop it later.
			*erss = append(*erss, b.ResolveColumn(tableDesc.GetID(), col.ColName(), ResolveParams{
				IsExistenceOptional: false,
				RequiredPrivilege:   privilege.CREATE,
			}))
		}
	}

}

func checkIfRequiresCCLBinary(b BuildCtx, tableDesc catalog.TableDescriptor, idx catalog.Index) {

}
