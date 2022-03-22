// Copyright 2017 The Cockroach Authors.
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
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/errors"
)

type dropIndexNode struct {
	n        *tree.DropIndex
	idxNames []fullIndexName
}

// DropIndex drops an index.
// Privileges: CREATE on table.
//   Notes: postgres allows only the index owner to DROP an index.
//          mysql requires the INDEX privilege on the table.
func (p *planner) DropIndex(ctx context.Context, n *tree.DropIndex) (planNode, error) {
	if err := checkSchemaChangeEnabled(
		ctx,
		p.ExecCfg(),
		"DROP INDEX",
	); err != nil {
		return nil, err
	}

	// Keep a track of the indexes that exist to check. When the IF EXISTS
	// options are provided, we will simply not include any indexes that
	// don't exist and continue execution.
	idxNames := make([]fullIndexName, 0, len(n.IndexList))
	for _, index := range n.IndexList {
		tn, tableDesc, err := expandMutableIndexName(ctx, p, index, !n.IfExists /* requireTable */)
		if err != nil {
			// Error or table did not exist.
			return nil, err
		}
		if tableDesc == nil {
			// IfExists specified and table did not exist.
			continue
		}

		if err := p.CheckPrivilege(ctx, tableDesc, privilege.CREATE); err != nil {
			return nil, err
		}

		idxNames = append(idxNames, fullIndexName{tn: tn, idxName: index.Index})
	}
	return &dropIndexNode{n: n, idxNames: idxNames}, nil
}

// ReadingOwnWrites implements the planNodeReadingOwnWrites interface.
// This is because DROP INDEX performs multiple KV operations on descriptors
// and expects to see its own writes.
func (n *dropIndexNode) ReadingOwnWrites() {}

func (n *dropIndexNode) startExec(params runParams) error {
	telemetry.Inc(sqltelemetry.SchemaChangeDropCounter("index"))

	if n.n.Concurrently {
		params.p.BufferClientNotice(
			params.ctx,
			pgnotice.Newf("CONCURRENTLY is not required as all indexes are dropped concurrently"),
		)
	}

	ctx := params.ctx
	for _, index := range n.idxNames {
		// Need to retrieve the descriptor again for each index name in
		// the list: when two or more index names refer to the same table,
		// the mutation list and new version number created by the first
		// drop need to be visible to the second drop.
		_, tableDesc, err := params.p.ResolveMutableTableDescriptor(
			ctx, index.tn, true /*required*/, tree.ResolveRequireTableOrViewDesc)
		if sqlerrors.IsUndefinedRelationError(err) {
			// Somehow the descriptor we had during planning is not there
			// any more.
			return errors.NewAssertionErrorWithWrappedErrf(err,
				"table descriptor for %q became unavailable within same txn",
				tree.ErrString(index.tn))
		}
		if err != nil {
			return err
		}

		if tableDesc.IsView() && !tableDesc.MaterializedView() {
			return pgerror.Newf(pgcode.WrongObjectType, "%q is not a table or materialized view", tableDesc.Name)
		}

		// If we couldn't find the index by name, this is either a legitimate error or
		// this statement contains an 'IF EXISTS' qualifier. Both of these cases are
		// handled by `dropIndexByName()` below so we just ignore the error here.
		idx, _ := tableDesc.FindIndexWithName(string(index.idxName))
		var shardColName string
		// If we're dropping a sharded index, record the name of its shard column to
		// potentially drop it if no other index refers to it.
		if idx != nil && idx.IsSharded() && !idx.Dropped() {
			shardColName = idx.GetShardColumnName()
		}

		// keyColumnOfOtherIndex returns true if the given column is a key
		// column of an index in the table other than idx.
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

		// Drop expression index columns if they are not key columns in any
		// other index. They cannot be referenced in constraints, computed
		// columns, or other indexes, so they are safe to drop.
		columnsDropped := false
		if idx != nil {
			for i, count := 0, idx.NumKeyColumns(); i < count; i++ {
				id := idx.GetKeyColumnID(i)
				col, err := tableDesc.FindColumnWithID(id)
				if err != nil {
					return err
				}
				if col.IsExpressionIndexColumn() && !keyColumnOfOtherIndex(col.GetID()) {
					n.queueDropColumn(tableDesc, col)
					columnsDropped = true
				}
			}
		}

		// CAUTION: After dropIndexByName returns, idx will be a pointer to a
		// different index than the one being dropped.
		if err := params.p.dropIndexByName(
			ctx, index.tn, index.idxName, tableDesc, n.n.IfExists, n.n.DropBehavior, checkIdxConstraint,
			tree.AsStringWithFQNames(n.n, params.Ann()),
		); err != nil {
			return err
		}

		if shardColName != "" {
			ok, err := n.maybeQueueDropShardColumn(tableDesc, shardColName)
			if err != nil {
				return err
			}
			columnsDropped = columnsDropped || ok
		}

		if columnsDropped {
			if err := n.finalizeDropColumn(params, tableDesc); err != nil {
				return err
			}
		}

	}
	return nil
}

// queueDropColumn queues a column to be dropped. Once all columns to drop are
// queued, call finalizeDropColumn.
func (n *dropIndexNode) queueDropColumn(tableDesc *tabledesc.Mutable, col catalog.Column) {
	tableDesc.AddColumnMutation(col.ColumnDesc(), descpb.DescriptorMutation_DROP)
	for i := range tableDesc.Columns {
		if tableDesc.Columns[i].ID == col.GetID() {
			// Note the third slice parameter which will force a copy of the backing
			// array if the column being removed is not the last column.
			tableDesc.Columns = append(tableDesc.Columns[:i:i],
				tableDesc.Columns[i+1:]...)
			break
		}
	}
}

// maybeDropShardColumn drops the given shard column, if there aren't any other
// indexes referring to it. It returns true if the column was queued to be
// dropped.
//
// Assumes that the given index is sharded.
func (n *dropIndexNode) maybeQueueDropShardColumn(
	tableDesc *tabledesc.Mutable, shardColName string,
) (bool, error) {
	shardColDesc, err := tableDesc.FindColumnWithName(tree.Name(shardColName))
	if err != nil {
		return false, err
	}
	if shardColDesc.Dropped() {
		return false, nil
	}
	if catalog.FindNonDropIndex(tableDesc, func(otherIdx catalog.Index) bool {
		colIDs := otherIdx.CollectKeyColumnIDs()
		if !otherIdx.Primary() {
			colIDs.UnionWith(otherIdx.CollectSecondaryStoredColumnIDs())
			colIDs.UnionWith(otherIdx.CollectKeySuffixColumnIDs())
		}
		return colIDs.Contains(shardColDesc.GetID())
	}) != nil {
		return false, nil
	}
	if err := n.dropShardColumnAndConstraint(tableDesc, shardColDesc); err != nil {
		return false, err
	}
	return true, nil
}

// dropShardColumnAndConstraint drops the given shard column and its associated check
// constraint.
func (n *dropIndexNode) dropShardColumnAndConstraint(
	tableDesc *tabledesc.Mutable, shardCol catalog.Column,
) error {
	validChecks := tableDesc.Checks[:0]
	for _, check := range tableDesc.AllActiveAndInactiveChecks() {
		if used, err := tableDesc.CheckConstraintUsesColumn(check, shardCol.GetID()); err != nil {
			return err
		} else if used {
			if check.Validity == descpb.ConstraintValidity_Validating {
				return pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
					"referencing constraint %q in the middle of being added, try again later", check.Name)
			}
		} else {
			validChecks = append(validChecks, check)
		}
	}

	if len(validChecks) != len(tableDesc.Checks) {
		tableDesc.Checks = validChecks
	}

	n.queueDropColumn(tableDesc, shardCol)
	return nil
}

// finalizeDropColumn finalizes the dropping of one or more columns. It should
// only be called if queueDropColumn has been called at least once.
func (n *dropIndexNode) finalizeDropColumn(params runParams, tableDesc *tabledesc.Mutable) error {
	version := params.ExecCfg().Settings.Version.ActiveVersion(params.ctx)
	if err := tableDesc.AllocateIDs(params.ctx, version); err != nil {
		return err
	}
	mutationID := tableDesc.ClusterVersion.NextMutationID
	if err := params.p.writeSchemaChange(
		params.ctx, tableDesc, mutationID, tree.AsStringWithFQNames(n.n, params.Ann()),
	); err != nil {
		return err
	}
	return nil
}

func (*dropIndexNode) Next(runParams) (bool, error) { return false, nil }
func (*dropIndexNode) Values() tree.Datums          { return tree.Datums{} }
func (*dropIndexNode) Close(context.Context)        {}

type fullIndexName struct {
	tn      *tree.TableName
	idxName tree.UnrestrictedName
}

// dropIndexConstraintBehavior is used when dropping an index to signal whether
// it is okay to do so even if it is in use as a constraint (outbound FK or
// unique). This is a subset of what is implied by DropBehavior CASCADE, which
// implies dropping *all* dependencies. This is used e.g. when the element
// constrained is being dropped anyway.
type dropIndexConstraintBehavior bool

const (
	checkIdxConstraint  dropIndexConstraintBehavior = true
	ignoreIdxConstraint dropIndexConstraintBehavior = false
)

func (p *planner) dropIndexByName(
	ctx context.Context,
	tn *tree.TableName,
	idxName tree.UnrestrictedName,
	tableDesc *tabledesc.Mutable,
	ifExists bool,
	behavior tree.DropBehavior,
	constraintBehavior dropIndexConstraintBehavior,
	jobDesc string,
) error {
	idx, err := tableDesc.FindIndexWithName(string(idxName))
	if err != nil {
		// Only index names of the form "table@idx" throw an error here if they
		// don't exist.
		if ifExists {
			// Noop.
			return nil
		}
		// Index does not exist, but we want it to: error out.
		return pgerror.WithCandidateCode(err, pgcode.UndefinedObject)
	}
	if idx.Dropped() {
		return nil
	}

	if tableDesc.IsLocalityRegionalByRow() {
		if err := p.checkNoRegionChangeUnderway(
			ctx,
			tableDesc.GetParentID(),
			"DROP INDEX on a REGIONAL BY ROW table",
		); err != nil {
			return err
		}
	}

	if idx.IsUnique() && behavior != tree.DropCascade && constraintBehavior != ignoreIdxConstraint && !idx.IsCreatedExplicitly() {
		return errors.WithHint(
			pgerror.Newf(pgcode.DependentObjectsStillExist,
				"index %q is in use as unique constraint", idx.GetName()),
			"use CASCADE if you really want to drop it.",
		)
	}

	// Check if requires CCL binary for eventual zone config removal.
	_, zone, _, err := GetZoneConfigInTxn(
		ctx, p.txn, p.ExecCfg().Codec, tableDesc.ID, nil /* index */, "", false,
	)
	if err != nil {
		return err
	}

	for _, s := range zone.Subzones {
		if s.IndexID != uint32(idx.GetID()) {
			_, err = GenerateSubzoneSpans(
				p.ExecCfg().Settings,
				p.ExecCfg().ClusterID(),
				p.ExecCfg().Codec,
				tableDesc,
				zone.Subzones,
				false, /* newSubzones */
			)
			if sqlerrors.IsCCLRequiredError(err) {
				return sqlerrors.NewCCLRequiredError(fmt.Errorf("schema change requires a CCL binary "+
					"because table %q has at least one remaining index or partition with a zone config",
					tableDesc.Name))
			}
			break
		}
	}

	// Remove all foreign key references and backreferences from the index.
	// TODO (lucy): This is incorrect for two reasons: The first is that FKs won't
	// be restored if the DROP INDEX is rolled back, and the second is that
	// validated constraints should be dropped in the schema changer in multiple
	// steps to avoid inconsistencies. We should be queuing a mutation to drop the
	// FK instead. The reason why the FK is removed here is to keep the index
	// state consistent with the removal of the reference on the other table
	// involved in the FK, in case of rollbacks (#38733).

	// TODO (rohany): switching all the checks from checking the legacy ID's to
	//  checking if the index has a prefix of the columns needed for the foreign
	//  key might result in some false positives for this index while it is in
	//  a mixed version cluster, but we have to remove all reads of the legacy
	//  explicit index fields.

	// Construct a list of all the remaining indexes, so that we can see if there
	// is another index that could replace the one we are deleting for a given
	// foreign key constraint.
	remainingIndexes := make([]catalog.Index, 1, len(tableDesc.ActiveIndexes()))
	remainingIndexes[0] = tableDesc.GetPrimaryIndex()
	for _, index := range tableDesc.PublicNonPrimaryIndexes() {
		if index.GetID() != idx.GetID() {
			remainingIndexes = append(remainingIndexes, index)
		}
	}

	// indexHasReplacementCandidate runs isValidIndex on each index in remainingIndexes and returns
	// true if at least one index satisfies isValidIndex.
	indexHasReplacementCandidate := func(isValidIndex func(index catalog.Index) bool) bool {
		foundReplacement := false
		for _, index := range remainingIndexes {
			if isValidIndex(index) {
				foundReplacement = true
				break
			}
		}
		return foundReplacement
	}

	// Check for foreign key mutations referencing this index.
	for _, m := range tableDesc.Mutations {
		if c := m.GetConstraint(); c != nil &&
			c.ConstraintType == descpb.ConstraintToUpdate_FOREIGN_KEY &&
			// If the index being deleted could be used as a index for this outbound
			// foreign key mutation, then make sure that we have another index that
			// could be used for this mutation.
			idx.IsValidOriginIndex(c.ForeignKey.OriginColumnIDs) &&
			!indexHasReplacementCandidate(func(idx catalog.Index) bool {
				return idx.IsValidOriginIndex(c.ForeignKey.OriginColumnIDs)
			}) {
			return pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
				"referencing constraint %q in the middle of being added, try again later", c.ForeignKey.Name)
		}
	}

	// If this index is used on the referencing side of any FK constraints, try
	// to remove the references or find an alternate index that will suffice.
	candidateConstraints := make([]descpb.UniqueConstraint, len(remainingIndexes))
	for i := range remainingIndexes {
		// We can't copy directly because of the interface conversion.
		candidateConstraints[i] = remainingIndexes[i]
	}
	if err := p.tryRemoveFKBackReferences(
		ctx, tableDesc, idx, behavior, candidateConstraints,
	); err != nil {
		return err
	}

	var droppedViews []string
	for _, tableRef := range tableDesc.DependedOnBy {
		if tableRef.IndexID == idx.GetID() {
			// Ensure that we have DROP privilege on all dependent views
			err := p.canRemoveDependentViewGeneric(
				ctx, "index", idx.GetName(), tableDesc.ParentID, tableRef, behavior)
			if err != nil {
				return err
			}
			viewDesc, err := p.getViewDescForCascade(
				ctx, "index", idx.GetName(), tableDesc.ParentID, tableRef.ID, behavior,
			)
			if err != nil {
				return err
			}
			viewJobDesc := fmt.Sprintf("removing view %q dependent on index %q which is being dropped",
				viewDesc.Name, idx.GetName())
			cascadedViews, err := p.removeDependentView(ctx, tableDesc, viewDesc, viewJobDesc)
			if err != nil {
				return err
			}

			qualifiedView, err := p.getQualifiedTableName(ctx, viewDesc)
			if err != nil {
				return err
			}

			droppedViews = append(droppedViews, qualifiedView.FQString())
			droppedViews = append(droppedViews, cascadedViews...)
		}
	}

	// Overwriting tableDesc.Index may mess up with the idx object we collected above. Make a copy.
	idxCopy := *idx.IndexDesc()
	idxDesc := &idxCopy

	// Currently, a replacement primary index must be specified when dropping the primary index,
	// and this cannot be done with DROP INDEX.
	if idxDesc.ID == tableDesc.GetPrimaryIndexID() {
		return errors.WithHint(
			pgerror.Newf(pgcode.FeatureNotSupported, "cannot drop the primary index of a table using DROP INDEX"),
			"instead, use ALTER TABLE ... ALTER PRIMARY KEY or"+
				"use DROP CONSTRAINT ... PRIMARY KEY followed by ADD CONSTRAINT ... PRIMARY KEY in a transaction",
		)
	}

	foundIndex := catalog.FindPublicNonPrimaryIndex(tableDesc, func(idxEntry catalog.Index) bool {
		return idxEntry.GetID() == idxDesc.ID
	})

	if foundIndex == nil {
		return pgerror.Newf(
			pgcode.ObjectNotInPrerequisiteState,
			"index %q in the middle of being added, try again later",
			idxName,
		)
	}

	idxEntry := *foundIndex.IndexDesc()
	idxOrdinal := foundIndex.Ordinal()

	// Unsplit all manually split ranges in the index so they can be
	// automatically merged by the merge queue. Gate this on being the
	// system tenant because secondary tenants aren't allowed to scan
	// the meta ranges directly.
	// TODO(Chengxiong): Remove this range unsplitting in 22.2
	st := p.EvalContext().Settings
	if p.ExecCfg().Codec.ForSystemTenant() &&
		!st.Version.IsActive(ctx, clusterversion.UnsplitRangesInAsyncGCJobs) {

		span := tableDesc.IndexSpan(p.ExecCfg().Codec, idxEntry.ID)
		txn := p.ExecCfg().DB.NewTxn(ctx, "scan-ranges-for-index-drop")
		ranges, err := kvclient.ScanMetaKVs(ctx, txn, span)
		if err != nil {
			return err
		}
		for _, r := range ranges {
			var desc roachpb.RangeDescriptor
			if err := r.ValueProto(&desc); err != nil {
				return err
			}
			// We have to explicitly check that the range descriptor's start key
			// lies within the span of the index since ScanMetaKVs returns all
			// intersecting spans.
			if !desc.GetStickyBit().IsEmpty() && span.Key.Compare(desc.StartKey.AsRawKey()) <= 0 {
				// Swallow "key is not the start of a range" errors because it would
				// mean that the sticky bit was removed and merged concurrently. DROP
				// INDEX should not fail because of this.
				if err := p.ExecCfg().DB.AdminUnsplit(ctx, desc.StartKey); err != nil && !strings.Contains(err.Error(), "is not the start of a range") {
					return err
				}
			}
		}
	}

	// the idx we picked up with FindIndexByID at the top may not
	// contain the same field any more due to other schema changes
	// intervening since the initial lookup. So we send the recent
	// copy idxEntry for drop instead.
	if err := tableDesc.AddDropIndexMutation(&idxEntry); err != nil {
		return err
	}
	tableDesc.RemovePublicNonPrimaryIndex(idxOrdinal)

	commentUpdater := p.execCfg.DescMetadaUpdaterFactory.NewMetadataUpdater(
		ctx,
		p.txn,
		p.SessionData(),
	)
	if err := commentUpdater.DeleteDescriptorComment(
		int64(tableDesc.ID), int64(idxDesc.ID), keys.IndexCommentType); err != nil {
		return err
	}

	if err := validateDescriptor(ctx, p, tableDesc); err != nil {
		return err
	}

	mutationID := tableDesc.ClusterVersion.NextMutationID
	if err := p.writeSchemaChange(ctx, tableDesc, mutationID, jobDesc); err != nil {
		return err
	}
	p.BufferClientNotice(
		ctx,
		errors.WithHint(
			pgnotice.Newf("the data for dropped indexes is reclaimed asynchronously"),
			"The reclamation delay can be customized in the zone configuration for the table.",
		),
	)
	// Record index drop in the event log. This is an auditable log event
	// and is recorded in the same transaction as the table descriptor
	// update.
	return p.logEvent(ctx,
		tableDesc.ID,
		&eventpb.DropIndex{
			TableName:           tn.FQString(),
			IndexName:           string(idxName),
			MutationID:          uint32(mutationID),
			CascadeDroppedViews: droppedViews,
		})
}
