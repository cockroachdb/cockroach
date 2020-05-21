// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

func (p *planner) AlterPrimaryKey(
	ctx context.Context,
	tableDesc *sqlbase.MutableTableDescriptor,
	alterPKNode *tree.AlterTableAlterPrimaryKey,
) error {
	// Make sure that all nodes in the cluster are able to perform primary key changes before proceeding.
	if !p.ExecCfg().Settings.Version.IsActive(ctx, clusterversion.VersionPrimaryKeyChanges) {
		return pgerror.Newf(pgcode.FeatureNotSupported,
			"all nodes are not the correct version for primary key changes")
	}

	if alterPKNode.Sharded != nil {
		if !p.ExecCfg().Settings.Version.IsActive(ctx, clusterversion.VersionHashShardedIndexes) {
			return invalidClusterForShardedIndexError
		}
		if !p.EvalContext().SessionData.HashShardedIndexesEnabled {
			return hashShardedIndexesDisabledError
		}
		if alterPKNode.Interleave != nil {
			return pgerror.Newf(pgcode.FeatureNotSupported, "interleaved indexes cannot also be hash sharded")
		}
	}

	// Ensure that other schema changes on this table are not currently
	// executing, and that other schema changes have not been performed
	// in the current transaction.
	currentMutationID := tableDesc.ClusterVersion.NextMutationID
	for i := range tableDesc.Mutations {
		mut := &tableDesc.Mutations[i]
		if mut.MutationID == currentMutationID {
			return unimplemented.NewWithIssuef(
				45510, "cannot perform a primary key change on %s "+
					"with other schema changes on %s in the same transaction", tableDesc.Name, tableDesc.Name)
		}
		if mut.MutationID < currentMutationID {
			// We can handle indexes being deleted concurrently. We do this
			// in order to not be blocked on index drops created by a previous
			// primary key change. If we errored out when seeing a previous
			// index drop, then users would see a confusing message that a
			// schema change is in progress when it doesn't seem like one is.
			// TODO (rohany): This feels like such a hack until (#45150) is fixed.
			if mut.GetIndex() != nil && mut.Direction == sqlbase.DescriptorMutation_DROP {
				continue
			}
			return unimplemented.NewWithIssuef(
				45510, "table %s is currently undergoing a schema change", tableDesc.Name)
		}
	}

	for _, elem := range alterPKNode.Columns {
		col, dropped, err := tableDesc.FindColumnByName(elem.Column)
		if err != nil {
			return err
		}
		if dropped {
			return pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
				"column %q is being dropped", col.Name)
		}
		if col.Nullable {
			return pgerror.Newf(pgcode.InvalidSchemaDefinition, "cannot use nullable column %q in primary key", col.Name)
		}
	}

	// Disable primary key changes on tables that are interleaved parents.
	if len(tableDesc.PrimaryIndex.InterleavedBy) != 0 {
		var sb strings.Builder
		sb.WriteString("[")
		comma := ", "
		for i := range tableDesc.PrimaryIndex.InterleavedBy {
			interleave := &tableDesc.PrimaryIndex.InterleavedBy[i]
			if i != 0 {
				sb.WriteString(comma)
			}
			childTable, err := p.Tables().GetTableVersionByID(
				ctx,
				p.Txn(),
				interleave.Table,
				tree.ObjectLookupFlags{},
			)
			if err != nil {
				return err
			}
			sb.WriteString(childTable.Name)
		}
		sb.WriteString("]")
		return errors.Newf(
			"cannot change primary key of table %s because table(s) %s are interleaved into it",
			tableDesc.Name,
			sb.String(),
		)
	}

	nameExists := func(name string) bool {
		_, _, err := tableDesc.FindIndexByName(name)
		return err == nil
	}

	// Make a new index that is suitable to be a primary index.
	name := sqlbase.GenerateUniqueConstraintName(
		"new_primary_key",
		nameExists,
	)
	newPrimaryIndexDesc := &sqlbase.IndexDescriptor{
		Name:              name,
		Unique:            true,
		CreatedExplicitly: true,
		EncodingType:      sqlbase.PrimaryIndexEncoding,
		Type:              sqlbase.IndexDescriptor_FORWARD,
		Version:           sqlbase.SecondaryIndexFamilyFormatVersion,
	}

	// If the new index is requested to be sharded, set up the index descriptor
	// to be sharded, and add the new shard column if it is missing.
	if alterPKNode.Sharded != nil {
		shardCol, newColumn, err := setupShardedIndex(
			ctx,
			p.EvalContext(),
			&p.semaCtx,
			p.SessionData().HashShardedIndexesEnabled,
			&alterPKNode.Columns,
			alterPKNode.Sharded.ShardBuckets,
			tableDesc,
			newPrimaryIndexDesc,
			false, /* isNewTable */
		)
		if err != nil {
			return err
		}
		if newColumn {
			if err := p.setupFamilyAndConstraintForShard(
				ctx,
				tableDesc,
				shardCol,
				newPrimaryIndexDesc.Sharded.ColumnNames,
				newPrimaryIndexDesc.Sharded.ShardBuckets,
			); err != nil {
				return err
			}
		}
		telemetry.Inc(sqltelemetry.HashShardedIndexCounter)
	}

	if err := newPrimaryIndexDesc.FillColumns(alterPKNode.Columns); err != nil {
		return err
	}
	if err := tableDesc.AddIndexMutation(newPrimaryIndexDesc, sqlbase.DescriptorMutation_ADD); err != nil {
		return err
	}
	if err := tableDesc.AllocateIDs(); err != nil {
		return err
	}

	// Ensure that the new primary index stores all columns in the table. We can't
	// use AllocateID's to fill the stored columns here because it assumes
	// that the indexed columns are n.PrimaryIndex.ColumnIDs, but here we want
	// to consider the indexed columns to be newPrimaryIndexDesc.ColumnIDs.
	newPrimaryIndexDesc.StoreColumnNames, newPrimaryIndexDesc.StoreColumnIDs = nil, nil
	for _, col := range tableDesc.Columns {
		containsCol := false
		for _, colID := range newPrimaryIndexDesc.ColumnIDs {
			if colID == col.ID {
				containsCol = true
				break
			}
		}
		if !containsCol {
			newPrimaryIndexDesc.StoreColumnIDs = append(newPrimaryIndexDesc.StoreColumnIDs, col.ID)
			newPrimaryIndexDesc.StoreColumnNames = append(newPrimaryIndexDesc.StoreColumnNames, col.Name)
		}
	}

	if alterPKNode.Interleave != nil {
		if err := p.addInterleave(ctx, tableDesc, newPrimaryIndexDesc, alterPKNode.Interleave); err != nil {
			return err
		}
		if err := p.finalizeInterleave(ctx, tableDesc, newPrimaryIndexDesc); err != nil {
			return err
		}
	}

	// Since we are potentially dropping indexes here, make sure to upgrade any potentially out of
	// date foreign key representations on old tables.
	if err := p.MaybeUpgradeDependentOldForeignKeyVersionTables(ctx, tableDesc); err != nil {
		return err
	}

	// Create a new index that indexes everything the old primary index
	// does, but doesn't store anything.
	if shouldCopyPrimaryKey(tableDesc, newPrimaryIndexDesc) {
		oldPrimaryIndexCopy := protoutil.Clone(&tableDesc.PrimaryIndex).(*sqlbase.IndexDescriptor)
		// Clear the name of the index so that it gets generated by AllocateIDs.
		oldPrimaryIndexCopy.Name = ""
		oldPrimaryIndexCopy.StoreColumnIDs = nil
		oldPrimaryIndexCopy.StoreColumnNames = nil
		// Make the copy of the old primary index not-interleaved. This decision
		// can be revisited based on user experience.
		oldPrimaryIndexCopy.Interleave = sqlbase.InterleaveDescriptor{}
		if err := addIndexMutationWithSpecificPrimaryKey(tableDesc, oldPrimaryIndexCopy, newPrimaryIndexDesc); err != nil {
			return err
		}
	}

	// We have to rewrite all indexes that either:
	// * depend on uniqueness from the old primary key (inverted, non-unique, or unique with nulls).
	// * don't store or index all columns in the new primary key.
	shouldRewriteIndex := func(idx *sqlbase.IndexDescriptor) bool {
		shouldRewrite := false
		for _, colID := range newPrimaryIndexDesc.ColumnIDs {
			if !idx.ContainsColumnID(colID) {
				shouldRewrite = true
				break
			}
		}
		if idx.Unique {
			for _, colID := range idx.ColumnIDs {
				col, err := tableDesc.FindColumnByID(colID)
				if err != nil {
					panic(err)
				}
				if col.Nullable {
					shouldRewrite = true
					break
				}
			}
		}
		return shouldRewrite || !idx.Unique || idx.Type == sqlbase.IndexDescriptor_INVERTED
	}
	var indexesToRewrite []*sqlbase.IndexDescriptor
	for i := range tableDesc.Indexes {
		idx := &tableDesc.Indexes[i]
		if idx.ID != newPrimaryIndexDesc.ID && shouldRewriteIndex(idx) {
			indexesToRewrite = append(indexesToRewrite, idx)
		}
	}

	// TODO (rohany): this loop will be unused until #45510 is resolved.
	for i := range tableDesc.Mutations {
		mut := &tableDesc.Mutations[i]
		// If there is an index that is getting built right now that started in a previous txn, we
		// need to potentially rebuild that index as well.
		if idx := mut.GetIndex(); mut.MutationID < currentMutationID && idx != nil &&
			mut.Direction == sqlbase.DescriptorMutation_ADD && shouldRewriteIndex(idx) {
			indexesToRewrite = append(indexesToRewrite, idx)
		}
	}

	// Queue up a mutation for each index that needs to be rewritten.
	// This new index will have an altered ExtraColumnIDs to allow it to be rewritten
	// using the unique-ifying columns from the new table.
	var oldIndexIDs, newIndexIDs []sqlbase.IndexID
	for _, idx := range indexesToRewrite {
		// Clone the index that we want to rewrite.
		newIndex := protoutil.Clone(idx).(*sqlbase.IndexDescriptor)
		basename := newIndex.Name + "_rewrite_for_primary_key_change"
		newIndex.Name = sqlbase.GenerateUniqueConstraintName(basename, nameExists)
		if err := addIndexMutationWithSpecificPrimaryKey(tableDesc, newIndex, newPrimaryIndexDesc); err != nil {
			return err
		}
		// If the index that we are rewriting is interleaved, we need to setup the rewritten
		// index to be interleaved as well. Since we cloned the index, the interleave descriptor
		// on the new index is already set up. So, we just need to add the backreference from the
		// parent to this new index.
		if len(newIndex.Interleave.Ancestors) != 0 {
			if err := p.finalizeInterleave(ctx, tableDesc, newIndex); err != nil {
				return err
			}
		}
		oldIndexIDs = append(oldIndexIDs, idx.ID)
		newIndexIDs = append(newIndexIDs, newIndex.ID)
	}

	swapArgs := &sqlbase.PrimaryKeySwap{
		OldPrimaryIndexId: tableDesc.PrimaryIndex.ID,
		NewPrimaryIndexId: newPrimaryIndexDesc.ID,
		NewIndexes:        newIndexIDs,
		OldIndexes:        oldIndexIDs,
	}
	tableDesc.AddPrimaryKeySwapMutation(swapArgs)

	// Mark the primary key of the table as valid.
	tableDesc.PrimaryIndex.Disabled = false

	// N.B. We don't schedule index deletions here because the existing
	// indexes need to be visible to the user until the primary key swap
	// actually occurs. Deletions will get enqueued in the phase when
	// the swap happens.

	// Send a notice to users about the async cleanup jobs.
	// TODO(knz): Mention the job ID in the client notice.
	p.SendClientNotice(
		ctx,
		pgnotice.Newf(
			"primary key changes are finalized asynchronously; "+
				"further schema changes on this table may be restricted until the job completes"),
	)

	return nil
}

// We only recreate the old primary key of the table as a unique secondary
// index if:
// * The table has a primary key (no DROP PRIMARY KEY statements have
//   been executed).
// * The primary key is not the default rowid primary key.
// * The new primary key isn't the same hash sharded old primary key with a
//   different bucket count.
func shouldCopyPrimaryKey(desc *MutableTableDescriptor, newPK *sqlbase.IndexDescriptor) bool {
	oldPK := desc.PrimaryIndex
	if !desc.HasPrimaryKey() {
		return false
	}
	if desc.IsPrimaryIndexDefaultRowID() {
		return false
	}
	// The first column in the columnIDs is the shard column, which will be different.
	// Slice it out to see what the actual index columns are.
	if oldPK.IsSharded() && newPK.IsSharded() &&
		sqlbase.ColumnIDs(oldPK.ColumnIDs[1:]).Equals(newPK.ColumnIDs[1:]) {
		return false
	}

	return true
}
