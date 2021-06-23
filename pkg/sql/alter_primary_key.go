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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/resolver"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
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
	alterPKNode tree.AlterTableAlterPrimaryKey,
	alterPrimaryKeyLocalitySwap *alterPrimaryKeyLocalitySwap,
) error {
	if alterPrimaryKeyLocalitySwap != nil {
		if err := p.checkNoRegionChangeUnderway(
			ctx,
			tableDesc.GetParentID(),
			"perform this locality change",
		); err != nil {
			return err
		}
	} else if tableDesc.IsLocalityRegionalByRow() {
		if err := p.checkNoRegionChangeUnderway(
			ctx,
			tableDesc.GetParentID(),
			"perform a primary key change on a REGIONAL BY ROW table",
		); err != nil {
			return err
		}
	}

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
		if tableDesc.IsLocalityRegionalByRow() {
			return pgerror.New(pgcode.FeatureNotSupported, "hash sharded indexes are not compatible with REGIONAL BY ROW tables")
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

	// Validate if the end result is the same as the current
	// primary index, which would mean nothing needs to be modified
	// here.
	{
		requiresIndexChange, err := p.shouldCreateIndexes(ctx, tableDesc, &alterPKNode, alterPrimaryKeyLocalitySwap)
		if err != nil {
			return err
		}
		if !requiresIndexChange {
			return nil
		}
	}

	nameExists := func(name string) bool {
		_, err := tableDesc.FindIndexWithName(name)
		return err == nil
	}

	// Make a new index that is suitable to be a primary index.
	name := tabledesc.GenerateUniqueName(
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
		Version:           descpb.StrictIndexColumnIDGuaranteesVersion,
	}

	// If the new index is requested to be sharded, set up the index descriptor
	// to be sharded, and add the new shard column if it is missing.
	if alterPKNode.Sharded != nil {
		shardCol, newColumns, newColumn, err := setupShardedIndex(
			ctx,
			p.EvalContext(),
			&p.semaCtx,
			p.SessionData().HashShardedIndexesEnabled,
			alterPKNode.Columns,
			alterPKNode.Sharded.ShardBuckets,
			tableDesc,
			newPrimaryIndexDesc,
			false, /* isNewTable */
		)
		if err != nil {
			return err
		}
		alterPKNode.Columns = newColumns
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

	{
		// Add all deletable non-virtual non-pk columns to new primary index.
		names := make(map[string]struct{}, len(newPrimaryIndexDesc.KeyColumnNames))
		for _, name := range newPrimaryIndexDesc.KeyColumnNames {
			names[name] = struct{}{}
		}
		deletable := tableDesc.DeletableColumns()
		newPrimaryIndexDesc.StoreColumnNames = make([]string, 0, len(deletable))
		for _, col := range deletable {
			if _, found := names[col.GetName()]; found || col.IsVirtual() {
				continue
			}
			newPrimaryIndexDesc.StoreColumnNames = append(newPrimaryIndexDesc.StoreColumnNames, col.GetName())
		}
		if len(newPrimaryIndexDesc.StoreColumnNames) == 0 {
			newPrimaryIndexDesc.StoreColumnNames = nil
		}
	}

	if err := tableDesc.AddIndexMutation(newPrimaryIndexDesc, descpb.DescriptorMutation_ADD); err != nil {
		return err
	}
	if err := tableDesc.AllocateIDs(ctx); err != nil {
		return err
	}

	if alterPKNode.Interleave != nil {
		if err := p.addInterleave(ctx, tableDesc, newPrimaryIndexDesc, alterPKNode.Interleave); err != nil {
			return err
		}
		if err := p.finalizeInterleave(ctx, tableDesc, newPrimaryIndexDesc); err != nil {
			return err
		}
	}

	var allowedNewColumnNames []tree.Name
	var err error
	// isNewPartitionAllBy is set if a new PARTITION ALL BY statement is introduced.
	isNewPartitionAllBy := false
	var partitionAllBy *tree.PartitionBy
	// dropPartitionAllBy is set if we should be dropping the PARTITION ALL BY component.
	dropPartitionAllBy := false

	allowImplicitPartitioning := false

	if alterPrimaryKeyLocalitySwap != nil {
		localityConfigSwap := alterPrimaryKeyLocalitySwap.localityConfigSwap
		switch to := localityConfigSwap.NewLocalityConfig.Locality.(type) {
		case *descpb.TableDescriptor_LocalityConfig_RegionalByRow_:
			// Check we are migrating from a known locality.
			switch localityConfigSwap.OldLocalityConfig.Locality.(type) {
			case *descpb.TableDescriptor_LocalityConfig_RegionalByRow_:
				// We want to drop the old PARTITION ALL BY clause in this case for all
				// the indexes if we were from a REGIONAL BY ROW.
				dropPartitionAllBy = true
			case *descpb.TableDescriptor_LocalityConfig_Global_,
				*descpb.TableDescriptor_LocalityConfig_RegionalByTable_:
			default:
				return errors.AssertionFailedf(
					"unknown locality config swap: %T to %T",
					localityConfigSwap.OldLocalityConfig.Locality,
					localityConfigSwap.NewLocalityConfig.Locality,
				)
			}

			isNewPartitionAllBy = true
			allowImplicitPartitioning = true
			colName := tree.RegionalByRowRegionDefaultColName
			if as := to.RegionalByRow.As; as != nil {
				colName = tree.Name(*as)
			}
			regionConfig, err := SynthesizeRegionConfig(
				ctx, p.txn, tableDesc.GetParentID(), p.Descriptors(),
			)
			if err != nil {
				return err
			}
			partitionAllBy = partitionByForRegionalByRow(
				regionConfig,
				colName,
			)
			if alterPrimaryKeyLocalitySwap.newColumnName != nil {
				allowedNewColumnNames = append(
					allowedNewColumnNames,
					*alterPrimaryKeyLocalitySwap.newColumnName,
				)
			}
		case *descpb.TableDescriptor_LocalityConfig_Global_,
			*descpb.TableDescriptor_LocalityConfig_RegionalByTable_:
			// We should only migrating from a REGIONAL BY ROW.
			if localityConfigSwap.OldLocalityConfig.GetRegionalByRow() == nil {
				return errors.AssertionFailedf(
					"unknown locality config swap: %T to %T",
					localityConfigSwap.OldLocalityConfig.Locality,
					localityConfigSwap.NewLocalityConfig.Locality,
				)
			}
			// We don't want a PARTITION ALL BY anymore.
			dropPartitionAllBy = true
		default:
			return errors.AssertionFailedf(
				"unknown locality config swap: %T to %T",
				localityConfigSwap.OldLocalityConfig.Locality,
				localityConfigSwap.NewLocalityConfig.Locality,
			)
		}
	} else if tableDesc.IsPartitionAllBy() {
		allowImplicitPartitioning = true
		partitionAllBy, err = partitionByFromTableDesc(p.ExecCfg().Codec, tableDesc)
		if err != nil {
			return err
		}
	}

	if partitionAllBy != nil {
		newImplicitCols, newPartitioning, err := CreatePartitioning(
			ctx,
			p.ExecCfg().Settings,
			p.EvalContext(),
			tableDesc,
			*newPrimaryIndexDesc,
			partitionAllBy,
			allowedNewColumnNames,
			allowImplicitPartitioning,
		)
		if err != nil {
			return err
		}
		tabledesc.UpdateIndexPartitioning(newPrimaryIndexDesc, true /* isIndexPrimary */, newImplicitCols, newPartitioning)
	}

	// Create a new index that indexes everything the old primary index
	// does, but doesn't store anything.
	if shouldCopyPrimaryKey(tableDesc, newPrimaryIndexDesc, alterPrimaryKeyLocalitySwap) {
		newUniqueIdx := tableDesc.GetPrimaryIndex().IndexDescDeepCopy()
		// Clear the following fields so that they get generated by AllocateIDs.
		newUniqueIdx.ID = 0
		newUniqueIdx.Name = ""
		newUniqueIdx.StoreColumnIDs = nil
		newUniqueIdx.StoreColumnNames = nil
		newUniqueIdx.KeySuffixColumnIDs = nil
		newUniqueIdx.CompositeColumnIDs = nil
		newUniqueIdx.KeyColumnIDs = nil
		// Make the copy of the old primary index not-interleaved. This decision
		// can be revisited based on user experience.
		newUniqueIdx.Interleave = descpb.InterleaveDescriptor{}
		// Set correct version and encoding type.
		newUniqueIdx.Version = descpb.StrictIndexColumnIDGuaranteesVersion
		newUniqueIdx.EncodingType = descpb.SecondaryIndexEncoding
		if err := addIndexMutationWithSpecificPrimaryKey(ctx, tableDesc, &newUniqueIdx, newPrimaryIndexDesc); err != nil {
			return err
		}
	}

	// We have to rewrite all indexes that either:
	// * depend on uniqueness from the old primary key (inverted, non-unique, or unique with nulls).
	// * don't store or index all columns in the new primary key.
	// * is affected by a locality config swap.
	shouldRewriteIndex := func(idx catalog.Index) (bool, error) {
		if alterPrimaryKeyLocalitySwap != nil {
			return true, nil
		}
		colIDs := idx.CollectKeyColumnIDs()
		if !idx.Primary() {
			colIDs.UnionWith(idx.CollectSecondaryStoredColumnIDs())
			colIDs.UnionWith(idx.CollectKeySuffixColumnIDs())
		}
		for _, colID := range newPrimaryIndexDesc.KeyColumnIDs {
			if !colIDs.Contains(colID) {
				return true, nil
			}
		}
		if idx.IsUnique() {
			for i := 0; i < idx.NumKeyColumns(); i++ {
				colID := idx.GetKeyColumnID(i)
				col, err := tableDesc.FindColumnWithID(colID)
				if err != nil {
					return false, err
				}
				if col.IsNullable() {
					return true, nil
				}
			}
		}
		return !idx.IsUnique() || idx.GetType() == descpb.IndexDescriptor_INVERTED, nil
	}
	var indexesToRewrite []catalog.Index
	for _, idx := range tableDesc.PublicNonPrimaryIndexes() {
		shouldRewrite, err := shouldRewriteIndex(idx)
		if err != nil {
			return err
		}
		if idx.GetID() != newPrimaryIndexDesc.ID && shouldRewrite {
			indexesToRewrite = append(indexesToRewrite, idx)
		}
	}

	// TODO (rohany): this loop will be unused until #45510 is resolved.
	for _, mut := range tableDesc.AllMutations() {
		// If there is an index that is getting built right now that started in a previous txn, we
		// need to potentially rebuild that index as well.
		if idx := mut.AsIndex(); mut.MutationID() < currentMutationID && idx != nil && mut.Adding() {
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
	// This new index will have an altered KeySuffixColumnIDs to allow it to be
	// rewritten using the unique-ifying columns from the new table.
	var oldIndexIDs, newIndexIDs []descpb.IndexID
	for _, idx := range indexesToRewrite {
		// Clone the index that we want to rewrite.
		newIndex := idx.IndexDescDeepCopy()
		basename := newIndex.Name + "_rewrite_for_primary_key_change"

		// Drop any PARTITION ALL BY clause.
		if dropPartitionAllBy {
			tabledesc.UpdateIndexPartitioning(&newIndex, idx.Primary(), nil /* newImplicitCols */, descpb.PartitioningDescriptor{})
		}

		// Create partitioning if we are newly adding a PARTITION BY ALL statement.
		if isNewPartitionAllBy {
			newImplicitCols, newPartitioning, err := CreatePartitioning(
				ctx,
				p.ExecCfg().Settings,
				p.EvalContext(),
				tableDesc,
				newIndex,
				partitionAllBy,
				allowedNewColumnNames,
				allowImplicitPartitioning,
			)
			if err != nil {
				return err
			}
			tabledesc.UpdateIndexPartitioning(&newIndex, idx.Primary(), newImplicitCols, newPartitioning)
		}

		newIndex.Name = tabledesc.GenerateUniqueName(basename, nameExists)
		newIndex.Version = descpb.StrictIndexColumnIDGuaranteesVersion
		newIndex.EncodingType = descpb.SecondaryIndexEncoding
		if err := addIndexMutationWithSpecificPrimaryKey(ctx, tableDesc, &newIndex, newPrimaryIndexDesc); err != nil {
			return err
		}
		// If the index that we are rewriting is interleaved, we need to setup the rewritten
		// index to be interleaved as well. Since we cloned the index, the interleave descriptor
		// on the new index is already set up. So, we just need to add the backreference from the
		// parent to this new index.
		if len(newIndex.Interleave.Ancestors) != 0 {
			if err := p.finalizeInterleave(ctx, tableDesc, &newIndex); err != nil {
				return err
			}
		}

		oldIndexIDs = append(oldIndexIDs, idx.GetID())
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

// Given the current table descriptor and the new primary keys
// index descriptor  this function determines if the two are
// equivalent and if any index creation operations are needed
// by comparing properties.
func (p *planner) shouldCreateIndexes(
	ctx context.Context,
	desc *tabledesc.Mutable,
	alterPKNode *tree.AlterTableAlterPrimaryKey,
	alterPrimaryKeyLocalitySwap *alterPrimaryKeyLocalitySwap,
) (requiresIndexChange bool, err error) {
	oldPK := desc.GetPrimaryIndex()

	// Validate if basic properties between the two match.
	if oldPK.NumKeyColumns() != len(alterPKNode.Columns) ||
		oldPK.IsSharded() != (alterPKNode.Sharded != nil) ||
		oldPK.IsInterleaved() != (alterPKNode.Interleave != nil) {
		return true, nil
	}

	// Validate if sharding properties are the same.
	if alterPKNode.Sharded != nil {
		shardBuckets, err := tabledesc.EvalShardBucketCount(ctx, &p.semaCtx, p.EvalContext(), alterPKNode.Sharded.ShardBuckets)
		if err != nil {
			return true, err
		}
		if oldPK.GetSharded().ShardBuckets != shardBuckets {
			return true, nil
		}
	}

	// Validate if interleaving properties match,
	// specifically the parent table, and the index
	// involved.
	if alterPKNode.Interleave != nil {
		_, parentTable, err := resolver.ResolveExistingTableObject(
			ctx, p, &alterPKNode.Interleave.Parent, tree.ObjectLookupFlagsWithRequiredTableKind(tree.ResolveRequireTableDesc),
		)
		if err != nil {
			return true, err
		}

		if oldPK.NumInterleaveAncestors() == 0 {
			return true, nil
		}
		if oldPK.GetInterleaveAncestor(oldPK.NumInterleaveAncestors()-1).TableID !=
			parentTable.GetID() {
			return true, nil
		}
		if oldPK.GetInterleaveAncestor(oldPK.NumInterleaveAncestors()-1).IndexID !=
			parentTable.GetPrimaryIndexID() {
			return true, nil
		}
	}

	// If the old primary key is dropped, then recreation
	// is required.
	if oldPK.IsDisabled() {
		return true, nil
	}

	// Validate the columns on the indexes
	for idx, elem := range alterPKNode.Columns {
		col, err := desc.FindColumnWithName(elem.Column)
		if err != nil {
			return true, err
		}

		if col.GetID() != oldPK.GetKeyColumnID(idx) {
			return true, nil
		}
		if (elem.Direction == tree.Ascending &&
			oldPK.GetKeyColumnDirection(idx) != descpb.IndexDescriptor_ASC) ||
			(elem.Direction == tree.Descending &&
				oldPK.GetKeyColumnDirection(idx) != descpb.IndexDescriptor_DESC) {
			return true, nil
		}
	}

	// Check partitioning changes based on primary key locality,
	// either the config changes, or the region column is changed
	// then recreate indexes.
	if alterPrimaryKeyLocalitySwap != nil {
		localitySwapConfig := alterPrimaryKeyLocalitySwap.localityConfigSwap
		if !localitySwapConfig.NewLocalityConfig.Equal(localitySwapConfig.OldLocalityConfig) {
			return true, nil
		}
		if localitySwapConfig.NewRegionalByRowColumnID != nil &&
			*localitySwapConfig.NewRegionalByRowColumnID != oldPK.GetKeyColumnID(0) {
			return true, nil
		}
	}
	return false, nil
}

// We only recreate the old primary key of the table as a unique secondary
// index if:
// * The table has a primary key (no DROP PRIMARY KEY statements have
//   been executed).
// * The primary key is not the default rowid primary key.
// * The new primary key isn't the same set of columns and directions
//   other than hash sharding.
// * There is no partitioning change.
func shouldCopyPrimaryKey(
	desc *tabledesc.Mutable,
	newPK *descpb.IndexDescriptor,
	alterPrimaryKeyLocalitySwap *alterPrimaryKeyLocalitySwap,
) bool {

	columnIDsAndDirsWithoutSharded := func(idx *descpb.IndexDescriptor) (
		columnIDs descpb.ColumnIDs,
		columnDirs []descpb.IndexDescriptor_Direction,
	) {
		for i, colName := range idx.KeyColumnNames {
			if colName != idx.Sharded.Name {
				columnIDs = append(columnIDs, idx.KeyColumnIDs[i])
				columnDirs = append(columnDirs, idx.KeyColumnDirections[i])
			}
		}
		return columnIDs, columnDirs
	}
	idsAndDirsMatch := func(old, new *descpb.IndexDescriptor) bool {
		oldIDs, oldDirs := columnIDsAndDirsWithoutSharded(old)
		newIDs, newDirs := columnIDsAndDirsWithoutSharded(new)
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
	oldPK := desc.GetPrimaryIndex().IndexDesc()
	return alterPrimaryKeyLocalitySwap == nil &&
		desc.HasPrimaryKey() &&
		!desc.IsPrimaryIndexDefaultRowID() &&
		!idsAndDirsMatch(oldPK, newPK)
}
