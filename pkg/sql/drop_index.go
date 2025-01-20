// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/funcdesc"
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
	zeroInputPlanNode
	n        *tree.DropIndex
	idxNames []fullIndexName
}

// DropIndex drops an index.
// Privileges: CREATE on table.
//
//	Notes: postgres allows only the index owner to DROP an index.
//	       mysql requires the INDEX privilege on the table.
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

		// Disallow schema changes if this table's schema is locked.
		if err = checkSchemaChangeIsAllowed(tableDesc, n); err != nil {
			return nil, err
		}

		idxNames = append(idxNames, fullIndexName{tn: tn, idxName: index.Index})
	}
	return &dropIndexNode{n: n, idxNames: idxNames}, nil
}

// failDropIndexIfSafeUpdates checks if the sql_safe_updates is present, and if so, it
// will fail the operation.
func failDropIndexIfSafeUpdates(params runParams) error {
	if params.SessionData().SafeUpdates {
		err := pgerror.WithCandidateCode(
			errors.WithMessage(
				errors.New(
					"DROP INDEX"),
				"rejected (sql_safe_updates = true)",
			),
			pgcode.Warning,
		)

		return err
	}

	return nil
}

// ReadingOwnWrites implements the planNodeReadingOwnWrites interface.
// This is because DROP INDEX performs multiple KV operations on descriptors
// and expects to see its own writes.
func (n *dropIndexNode) ReadingOwnWrites() {}

func (n *dropIndexNode) startExec(params runParams) error {
	telemetry.Inc(sqltelemetry.SchemaChangeDropCounter("index"))

	if err := failDropIndexIfSafeUpdates(params); err != nil {
		return err
	}

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
		idx := catalog.FindIndexByName(tableDesc, string(index.idxName))
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
				col, err := catalog.MustFindColumnByID(tableDesc, id)
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
			// Retrieve the sharded column descriptor by name.
			shardColDesc, err := catalog.MustFindColumnByName(tableDesc, shardColName)
			if err != nil {
				return err
			}
			if !shardColDesc.Dropped() {
				// Only drop this shard column if it's not a physical column (as is the case for all hash-sharded index in 22.1
				// and after), or, CASCADE is set.
				if shardColDesc.IsVirtual() || n.n.DropBehavior == tree.DropCascade {
					ok, err := n.maybeQueueDropShardColumn(tableDesc, shardColDesc)
					if err != nil {
						return err
					}
					columnsDropped = columnsDropped || ok
				} else {
					params.p.BufferClientNotice(
						ctx,
						pgnotice.Newf("The accompanying shard column %q is a physical column and dropping it can be "+
							"expensive, so, we dropped the index %q but skipped dropping %q. Issue another "+
							"'ALTER TABLE %v DROP COLUMN %v' query if you want to drop column %q.",
							shardColName, idx.GetName(), shardColName, tableDesc.GetName(), shardColName, shardColName),
					)
				}
			}
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
	tableDesc *tabledesc.Mutable, shardColDesc catalog.Column,
) (bool, error) {
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
	for _, check := range tableDesc.CheckConstraints() {
		if check.CollectReferencedColumnIDs().Contains(shardCol.GetID()) {
			if check.GetConstraintValidity() == descpb.ConstraintValidity_Validating {
				return pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
					"referencing constraint %q in the middle of being added, try again later", check.GetName())
			}
		} else {
			validChecks = append(validChecks, check.CheckDesc())
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
	mutationID := tableDesc.ClusterVersion().NextMutationID
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
	idx, err := catalog.MustFindIndexByName(tableDesc, string(idxName))
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

	// Check for foreign key mutations referencing this index.
	activeIndexes := tableDesc.ActiveIndexes()
	for _, fk := range tableDesc.OutboundForeignKeys() {
		if !fk.IsMutation() || !idx.IsValidOriginIndex(fk) {
			continue
		}
		// If the index being deleted could be used as a index for this outbound
		// foreign key mutation, then make sure that we have another index that
		// could be used for this mutation.
		var foundReplacement bool
		for _, index := range activeIndexes {
			if index.GetID() != idx.GetID() && index.IsValidOriginIndex(fk) {
				foundReplacement = true
				break
			}
		}
		if !foundReplacement {
			return pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
				"referencing constraint %q in the middle of being added, try again later",
				fk.GetName(),
			)
		}
	}

	// If this index is used on the referencing side of any FK constraints, try
	// to remove the references or find an alternate index that will suffice.
	if uwi := idx.AsUniqueWithIndex(); uwi != nil {

		const withSearchForReplacement = true
		if err := p.tryRemoveFKBackReferences(
			ctx, tableDesc, uwi, behavior, withSearchForReplacement,
		); err != nil {
			return err
		}
	}

	var droppedViews []string
	var depsToDrop catalog.DescriptorIDSet
	for _, tableRef := range tableDesc.DependedOnBy {
		if tableRef.IndexID == idx.GetID() {
			// Ensure that we have DROP privilege on all dependent views
			err := p.canRemoveDependent(
				ctx, "index", idx.GetName(), tableDesc.ParentID, tableRef, behavior)
			if err != nil {
				return err
			}
			depsToDrop.Add(tableRef.ID)
		}
	}

	droppedViews, err = p.removeDependents(
		ctx, tableDesc, depsToDrop, "index", idx.GetName(), behavior,
	)
	if err != nil {
		return err
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

	// the idx we picked up with FindIndexByID at the top may not
	// contain the same field any more due to other schema changes
	// intervening since the initial lookup. So we send the recent
	// copy idxEntry for drop instead.
	if err := tableDesc.AddDropIndexMutation(&idxEntry); err != nil {
		return err
	}
	tableDesc.RemovePublicNonPrimaryIndex(idxOrdinal)

	if err := p.deleteComment(
		ctx, tableDesc.ID, uint32(idxDesc.ID), catalogkeys.IndexCommentType,
	); err != nil {
		return err
	}

	if err := validateDescriptor(ctx, p, tableDesc); err != nil {
		return err
	}

	mutationID := tableDesc.ClusterVersion().NextMutationID
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

func (p *planner) removeDependents(
	ctx context.Context,
	tableDesc *tabledesc.Mutable,
	depsToDrop catalog.DescriptorIDSet,
	typeName string,
	objName string,
	dropBehavior tree.DropBehavior,
) (droppedViews []string, err error) {
	for _, descId := range depsToDrop.Ordered() {
		depDesc, err := p.getDescForCascade(
			ctx, typeName, objName, tableDesc.ParentID, descId, dropBehavior,
		)
		if err != nil {
			return nil, err
		}
		if depDesc.Dropped() {
			continue
		}
		switch t := depDesc.(type) {
		case *tabledesc.Mutable:
			jobDesc := fmt.Sprintf("removing view %q dependent on %s %q which is being dropped",
				t.Name, typeName, objName)
			cascadedViews, err := p.removeDependentView(ctx, tableDesc, t, jobDesc)
			if err != nil {
				return nil, err
			}
			qualifiedView, err := p.getQualifiedTableName(ctx, t)
			if err != nil {
				return nil, err
			}

			droppedViews = append(droppedViews, cascadedViews...)
			droppedViews = append(droppedViews, qualifiedView.FQString())
		case *funcdesc.Mutable:
			if err := p.removeDependentFunction(ctx, tableDesc, t); err != nil {
				return nil, err
			}
		}
	}
	return droppedViews, nil
}
