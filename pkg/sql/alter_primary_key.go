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

	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

// alterPrimaryKeyLocalitySwap contains metadata on a locality swap for
// AlterPrimaryKey.
type alterPrimaryKeyLocalitySwap struct {
	localityConfigSwap descpb.PrimaryKeySwap_LocalityConfigSwap
	// mutationIdxAllowedInSameTxn is the index of the mutation which is
	// allowed to exist in the same transaction as an ALTER PRIMARY KEY.
	// It is required for the case where we're adding a column to the table
	// (the implicit crdb_internal_region column) as part of a PRIMARY KEY
	// change, when transitioning a table to REGIONAL BY ROW.
	// It is nilable to prevent the 0 index from being used in case the struct
	// is initialized with values improperly set.
	mutationIdxAllowedInSameTxn *int
	// newColumnName is set if we are creating a new column before doing the
	// ALTER PRIMARY KEY, for similar reasons as above.
	newColumnName *tree.Name
}

func (p *planner) AlterPrimaryKey(
	ctx context.Context,
	tableDesc *tabledesc.Mutable,
	alterPKNode *tree.AlterTableAlterPrimaryKey,
	alterPrimaryKeyLocalitySwap *alterPrimaryKeyLocalitySwap,
) error {
	if alterPKNode.Interleave != nil {
		if err := interleavedTableDeprecationAction(p.RunParams(ctx)); err != nil {
			return err
		}
	}

	if alterPKNode.Sharded != nil {
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
			errBase := "primary key"
			if alterPrimaryKeyLocalitySwap != nil {
				allowIdx := alterPrimaryKeyLocalitySwap.mutationIdxAllowedInSameTxn
				if allowIdx != nil && *allowIdx == i {
					continue
				}
				errBase = "locality"
			}
			return unimplemented.NewWithIssuef(
				45510,
				"cannot perform a %[1]s change on %[2]s with other schema changes on %[2]s in the same transaction",
				errBase,
				tableDesc.Name,
			)
		}
		if mut.MutationID < currentMutationID {
			// We can handle indexes being deleted concurrently. We do this
			// in order to not be blocked on index drops created by a previous
			// primary key change. If we errored out when seeing a previous
			// index drop, then users would see a confusing message that a
			// schema change is in progress when it doesn't seem like one is.
			// TODO (rohany): This feels like such a hack until (#45510) is fixed.
			if mut.GetIndex() != nil && mut.Direction == descpb.DescriptorMutation_DROP {
				continue
			}
			return unimplemented.NewWithIssuef(
				45510, "table %s is currently undergoing a schema change", tableDesc.Name)
		}
	}

	for _, elem := range alterPKNode.Columns {
		col, err := tableDesc.FindColumnWithName(elem.Column)
		if err != nil {
			return err
		}
		if col.Dropped() {
			return pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
				"column %q is being dropped", col.GetName())
		}
		if col.IsNullable() {
			return pgerror.Newf(pgcode.InvalidSchemaDefinition, "cannot use nullable column %q in primary key", col.GetName())
		}
	}

	// Disable primary key changes on tables that are interleaved parents.
	if tableDesc.GetPrimaryIndex().NumInterleavedBy() != 0 {
		var sb strings.Builder
		sb.WriteString("[")
		comma := ", "
		for i := 0; i < tableDesc.GetPrimaryIndex().NumInterleavedBy(); i++ {
			interleaveTableID := tableDesc.GetPrimaryIndex().GetInterleavedBy(i).Table
			if i != 0 {
				sb.WriteString(comma)
			}
			childTable, err := p.Descriptors().GetImmutableTableByID(
				ctx,
				p.Txn(),
				interleaveTableID,
				tree.ObjectLookupFlags{},
			)
			if err != nil {
				return err
			}
			sb.WriteString(childTable.GetName())
		}
		sb.WriteString("]")
		return errors.Newf(
			"cannot change primary key of table %s because table(s) %s are interleaved into it",
			tableDesc.Name,
			sb.String(),
		)
	}

	nameExists := func(name string) bool {
		_, err := tableDesc.FindIndexWithName(name)
		return err == nil
	}

	// Make a new index that is suitable to be a primary index.
	name := tabledesc.GenerateUniqueConstraintName(
		"new_primary_key",
		nameExists,
	)
	if alterPKNode.Name != "" &&
		// Allow reuse of existing primary key's name.
		tableDesc.PrimaryIndex.Name != string(alterPKNode.Name) &&
		nameExists(string(alterPKNode.Name)) {
		return pgerror.Newf(pgcode.DuplicateObject, "constraint with name %s already exists", alterPKNode.Name)
	}
	newPrimaryIndexDesc := &descpb.IndexDescriptor{
		Name:              name,
		Unique:            true,
		CreatedExplicitly: true,
		EncodingType:      descpb.PrimaryIndexEncoding,
		Type:              descpb.IndexDescriptor_FORWARD,
		Version:           descpb.EmptyArraysInInvertedIndexesVersion,
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
	if err := tableDesc.AddIndexMutation(newPrimaryIndexDesc, descpb.DescriptorMutation_ADD); err != nil {
		return err
	}
	if err := tableDesc.AllocateIDs(ctx); err != nil {
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

	var allowedNewColumnNames []tree.Name
	var err error
	// isNewPartitionAllBy is set if a new PARTITON ALL BY statement is introduced.
	isNewPartitionAllBy := false
	var partitionAllBy *tree.PartitionBy
	// dropPartitionAllBy is set if we should be dropping the PARTITION ALL BY component.
	dropPartitionAllBy := false

	if alterPrimaryKeyLocalitySwap != nil {
		localityConfigSwap := alterPrimaryKeyLocalitySwap.localityConfigSwap
		switch localityConfigSwap.OldLocalityConfig.Locality.(type) {
		case *descpb.TableDescriptor_LocalityConfig_Global_,
			*descpb.TableDescriptor_LocalityConfig_RegionalByTable_:
			switch to := localityConfigSwap.NewLocalityConfig.Locality.(type) {
			case *descpb.TableDescriptor_LocalityConfig_RegionalByRow_:
				isNewPartitionAllBy = true
				colName := tree.RegionalByRowRegionDefaultColName
				if as := to.RegionalByRow.As; as != nil {
					colName = tree.Name(*as)
				}
				_, dbDesc, err := p.Descriptors().GetImmutableDatabaseByID(
					ctx,
					p.txn,
					tableDesc.GetParentID(),
					tree.DatabaseLookupFlags{Required: true},
				)
				if err != nil {
					return err
				}
				partitionAllBy = partitionByForRegionalByRow(
					*dbDesc.DatabaseDesc().RegionConfig,
					colName,
				)
				if alterPrimaryKeyLocalitySwap.newColumnName != nil {
					allowedNewColumnNames = append(
						allowedNewColumnNames,
						*alterPrimaryKeyLocalitySwap.newColumnName,
					)
				}
			default:
				return errors.AssertionFailedf(
					"unknown locality config swap: %T to %T",
					localityConfigSwap.OldLocalityConfig.Locality,
					localityConfigSwap.NewLocalityConfig.Locality,
				)
			}
		case *descpb.TableDescriptor_LocalityConfig_RegionalByRow_:
			switch localityConfigSwap.NewLocalityConfig.Locality.(type) {
			case *descpb.TableDescriptor_LocalityConfig_Global_,
				*descpb.TableDescriptor_LocalityConfig_RegionalByTable_:
				// We don't want a PARTITION ALL BY anymore.
				dropPartitionAllBy = true
			}
		default:
			return errors.AssertionFailedf(
				"unknown locality config swap: %T to %T",
				localityConfigSwap.OldLocalityConfig.Locality,
				localityConfigSwap.NewLocalityConfig.Locality,
			)
		}
	} else if tableDesc.IsPartitionAllBy() {
		partitionAllBy, err = partitionByFromTableDesc(p.ExecCfg().Codec, tableDesc)
		if err != nil {
			return err
		}
	}

	if partitionAllBy != nil {
		*newPrimaryIndexDesc, err = CreatePartitioning(
			ctx,
			p.ExecCfg().Settings,
			p.EvalContext(),
			tableDesc,
			*newPrimaryIndexDesc,
			partitionAllBy,
			allowedNewColumnNames,
		)
		if err != nil {
			return err
		}
	}

	// Create a new index that indexes everything the old primary index
	// does, but doesn't store anything.
	if shouldCopyPrimaryKey(tableDesc, newPrimaryIndexDesc, alterPrimaryKeyLocalitySwap) {
		oldPrimaryIndexCopy := tableDesc.GetPrimaryIndex().IndexDescDeepCopy()
		// Clear the name of the index so that it gets generated by AllocateIDs.
		oldPrimaryIndexCopy.Name = ""
		oldPrimaryIndexCopy.StoreColumnIDs = nil
		oldPrimaryIndexCopy.StoreColumnNames = nil
		// Make the copy of the old primary index not-interleaved. This decision
		// can be revisited based on user experience.
		oldPrimaryIndexCopy.Interleave = descpb.InterleaveDescriptor{}
		if err := addIndexMutationWithSpecificPrimaryKey(ctx, tableDesc, &oldPrimaryIndexCopy, newPrimaryIndexDesc); err != nil {
			return err
		}
	}

	// We have to rewrite all indexes that either:
	// * depend on uniqueness from the old primary key (inverted, non-unique, or unique with nulls).
	// * don't store or index all columns in the new primary key.
	// * is affected by a locality config swap.
	shouldRewriteIndex := func(idx *descpb.IndexDescriptor) (bool, error) {
		if alterPrimaryKeyLocalitySwap != nil {
			return true, nil
		}
		for _, colID := range newPrimaryIndexDesc.ColumnIDs {
			if !idx.ContainsColumnID(colID) {
				return true, nil
			}
		}
		if idx.Unique {
			for _, colID := range idx.ColumnIDs {
				col, err := tableDesc.FindColumnWithID(colID)
				if err != nil {
					return false, err
				}
				if col.IsNullable() {
					return true, nil
				}
			}
		}
		return !idx.Unique || idx.Type == descpb.IndexDescriptor_INVERTED, nil
	}
	var indexesToRewrite []*descpb.IndexDescriptor
	for _, idx := range tableDesc.PublicNonPrimaryIndexes() {
		shouldRewrite, err := shouldRewriteIndex(idx.IndexDesc())
		if err != nil {
			return err
		}
		if idx.GetID() != newPrimaryIndexDesc.ID && shouldRewrite {
			indexesToRewrite = append(indexesToRewrite, idx.IndexDesc())
		}
	}

	// TODO (rohany): this loop will be unused until #45510 is resolved.
	for i := range tableDesc.Mutations {
		mut := &tableDesc.Mutations[i]
		// If there is an index that is getting built right now that started in a previous txn, we
		// need to potentially rebuild that index as well.
		if idx := mut.GetIndex(); mut.MutationID < currentMutationID && idx != nil &&
			mut.Direction == descpb.DescriptorMutation_ADD {
			shouldRewrite, err := shouldRewriteIndex(idx)
			if err != nil {
				return err
			}
			if shouldRewrite {
				indexesToRewrite = append(indexesToRewrite, idx)
			}
		}
	}

	// Queue up a mutation for each index that needs to be rewritten.
	// This new index will have an altered ExtraColumnIDs to allow it to be rewritten
	// using the unique-ifying columns from the new table.
	var oldIndexIDs, newIndexIDs []descpb.IndexID
	for _, idx := range indexesToRewrite {
		// Clone the index that we want to rewrite.
		newIndex := protoutil.Clone(idx).(*descpb.IndexDescriptor)
		basename := newIndex.Name + "_rewrite_for_primary_key_change"

		// Drop any PARTITION ALL BY clause.
		if dropPartitionAllBy {
			newIndex.ColumnNames = newIndex.ColumnNames[newIndex.Partitioning.NumImplicitColumns:]
			newIndex.ColumnIDs = newIndex.ColumnIDs[newIndex.Partitioning.NumImplicitColumns:]
			newIndex.ColumnDirections = newIndex.ColumnDirections[newIndex.Partitioning.NumImplicitColumns:]
			newIndex.Partitioning = descpb.PartitioningDescriptor{}
		}

		newIndex.Name = tabledesc.GenerateUniqueConstraintName(basename, nameExists)
		if err := addIndexMutationWithSpecificPrimaryKey(ctx, tableDesc, newIndex, newPrimaryIndexDesc); err != nil {
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
		// TODO(#59719): decide what happens if any multiregion tables have
		// PARTITION BY statements. We currently do not support this and ignore
		// these old statements.
		//
		// Detect partitioning if we are newly adding a PARTITION BY ALL statement.
		// If the table is already PARTITION ALL BY, we already have the correct implicit
		// column descriptor in front of each index, and calling CreatePartitioning again
		// will make these indexes explicit.
		if isNewPartitionAllBy {
			if *newIndex, err = CreatePartitioning(
				ctx,
				p.ExecCfg().Settings,
				p.EvalContext(),
				tableDesc,
				*newIndex,
				partitionAllBy,
				allowedNewColumnNames,
			); err != nil {
				return err
			}
		}
		oldIndexIDs = append(oldIndexIDs, idx.ID)
		newIndexIDs = append(newIndexIDs, newIndex.ID)
	}

	swapArgs := &descpb.PrimaryKeySwap{
		OldPrimaryIndexId:   tableDesc.GetPrimaryIndexID(),
		NewPrimaryIndexId:   newPrimaryIndexDesc.ID,
		NewIndexes:          newIndexIDs,
		OldIndexes:          oldIndexIDs,
		NewPrimaryIndexName: string(alterPKNode.Name),
	}
	if alterPrimaryKeyLocalitySwap != nil {
		swapArgs.LocalityConfigSwap = &alterPrimaryKeyLocalitySwap.localityConfigSwap
	}
	tableDesc.AddPrimaryKeySwapMutation(swapArgs)

	// Mark the primary key of the table as valid.
	{
		primaryIndex := *tableDesc.GetPrimaryIndex().IndexDesc()
		primaryIndex.Disabled = false
		tableDesc.SetPrimaryIndex(primaryIndex)
	}

	// N.B. We don't schedule index deletions here because the existing
	// indexes need to be visible to the user until the primary key swap
	// actually occurs. Deletions will get enqueued in the phase when
	// the swap happens.

	// Send a notice to users about how this job is asynchronous.
	// TODO(knz): Mention the job ID in the client notice.
	noticeStr := "primary key changes are finalized asynchronously"
	if alterPrimaryKeyLocalitySwap != nil {
		noticeStr = "LOCALITY changes will be finalized asynchronously"
	}
	p.BufferClientNotice(
		ctx,
		pgnotice.Newf(
			"%s; further schema changes on this table may be restricted "+
				"until the job completes",
			noticeStr,
		),
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
// * There is no partitioning change.
func shouldCopyPrimaryKey(
	desc *tabledesc.Mutable,
	newPK *descpb.IndexDescriptor,
	alterPrimaryKeyLocalitySwap *alterPrimaryKeyLocalitySwap,
) bool {
	if alterPrimaryKeyLocalitySwap != nil {
		return false
	}
	oldPK := desc.GetPrimaryIndex()
	if !desc.HasPrimaryKey() {
		return false
	}
	if desc.IsPrimaryIndexDefaultRowID() {
		return false
	}
	// The first column in the columnIDs is the shard column, which will be different.
	// Slice it out to see what the actual index columns are.
	if oldPK.IsSharded() && newPK.IsSharded() &&
		descpb.ColumnIDs(oldPK.IndexDesc().ColumnIDs[1:]).Equals(newPK.ColumnIDs[1:]) {
		return false
	}

	return true
}
