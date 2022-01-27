// Copyright 2015 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

type dropTableNode struct {
	n *tree.DropTable
	// td is a map from table descriptor to toDelete struct, indicating which
	// tables this operation should delete.
	td map[descpb.ID]toDelete
}

type toDelete struct {
	tn   tree.ObjectName
	desc *tabledesc.Mutable
}

// DropTable drops a table.
// Privileges: DROP on table.
//   Notes: postgres allows only the table owner to DROP a table.
//          mysql requires the DROP privilege on the table.
func (p *planner) DropTable(ctx context.Context, n *tree.DropTable) (planNode, error) {
	if err := checkSchemaChangeEnabled(
		ctx,
		p.ExecCfg(),
		"DROP TABLE",
	); err != nil {
		return nil, err
	}

	td := make(map[descpb.ID]toDelete, len(n.Names))
	for i := range n.Names {
		tn := &n.Names[i]
		droppedDesc, err := p.prepareDrop(ctx, tn, !n.IfExists, tree.ResolveRequireTableDesc)
		if err != nil {
			return nil, err
		}
		if droppedDesc == nil {
			continue
		}

		td[droppedDesc.ID] = toDelete{tn, droppedDesc}
	}

	for _, toDel := range td {
		droppedDesc := toDel.desc
		for i := range droppedDesc.InboundFKs {
			ref := &droppedDesc.InboundFKs[i]
			if _, ok := td[ref.OriginTableID]; !ok {
				if err := p.canRemoveFKBackreference(ctx, droppedDesc.Name, ref, n.DropBehavior); err != nil {
					return nil, err
				}
			}
		}
		for _, ref := range droppedDesc.DependedOnBy {
			if _, ok := td[ref.ID]; !ok {
				if err := p.canRemoveDependentView(ctx, droppedDesc, ref, n.DropBehavior); err != nil {
					return nil, err
				}
			}
		}
		if err := p.canRemoveAllTableOwnedSequences(ctx, droppedDesc, n.DropBehavior); err != nil {
			return nil, err
		}

	}

	if len(td) == 0 {
		return newZeroNode(nil /* columns */), nil
	}
	return &dropTableNode{n: n, td: td}, nil
}

// ReadingOwnWrites implements the planNodeReadingOwnWrites interface.
// This is because DROP TABLE performs multiple KV operations on descriptors
// and expects to see its own writes.
func (n *dropTableNode) ReadingOwnWrites() {}

func (n *dropTableNode) startExec(params runParams) error {
	telemetry.Inc(sqltelemetry.SchemaChangeDropCounter("table"))

	ctx := params.ctx
	for _, toDel := range n.td {
		droppedDesc := toDel.desc
		if droppedDesc == nil {
			continue
		}

		droppedViews, err := params.p.dropTableImpl(
			ctx,
			droppedDesc,
			false, /* droppingDatabase */
			tree.AsStringWithFQNames(n.n, params.Ann()),
			n.n.DropBehavior,
		)
		if err != nil {
			return err
		}
		// Log a Drop Table event for this table. This is an auditable log event
		// and is recorded in the same transaction as the table descriptor
		// update.
		if err := params.p.logEvent(params.ctx,
			droppedDesc.ID,
			&eventpb.DropTable{
				TableName:           toDel.tn.FQString(),
				CascadeDroppedViews: droppedViews,
			}); err != nil {
			return err
		}
	}
	return nil
}

func (*dropTableNode) Next(runParams) (bool, error) { return false, nil }
func (*dropTableNode) Values() tree.Datums          { return tree.Datums{} }
func (*dropTableNode) Close(context.Context)        {}

// prepareDrop/dropTableImpl is used to drop a single table by
// name, which can result from a DROP TABLE, DROP VIEW, DROP SEQUENCE,
// or DROP DATABASE statement. This method returns the dropped table
// descriptor, to be used for the purpose of logging the event.  The table
// is not actually truncated or deleted synchronously. Instead, it is marked
// as deleted (meaning up_version is set and deleted is set) and the
// actual deletion happens async in a schema changer. Note that,
// courtesy of up_version, the actual truncation and dropping will
// only happen once every node ACKs the version of the descriptor with
// the deleted bit set, meaning the lease manager will not hand out
// new leases for it and existing leases are released).
// If the table does not exist, this function returns a nil descriptor.
func (p *planner) prepareDrop(
	ctx context.Context, name *tree.TableName, required bool, requiredType tree.RequiredTableKind,
) (*tabledesc.Mutable, error) {
	_, tableDesc, err := p.ResolveMutableTableDescriptor(ctx, name, required, requiredType)
	if err != nil {
		return nil, err
	}
	if tableDesc == nil {
		return nil, err
	}
	if err := p.canDropTable(ctx, tableDesc, true /* checkOwnership */); err != nil {
		return nil, err
	}
	return tableDesc, nil
}

// canDropTable returns an error if the user cannot drop the table.
func (p *planner) canDropTable(
	ctx context.Context, tableDesc *tabledesc.Mutable, checkOwnership bool,
) error {
	var err error
	hasOwnership := false
	// This checkOwnership stuff is rather unfortunate, but is required when an
	// active session has created a temporary object in a database that is being
	// dropped by a different session. The session trying to drop the database
	// can't resolve the temporary schema, and would therefore return an
	// error if we tried to check for ownership on the schema.
	if checkOwnership {
		// If the user owns the schema the table is part of, they can drop the table.
		hasOwnership, err = p.HasOwnershipOnSchema(
			ctx, tableDesc.GetParentSchemaID(), tableDesc.GetParentID())
		if err != nil {
			return err
		}
	}
	if !hasOwnership {
		return p.CheckPrivilege(ctx, tableDesc, privilege.DROP)
	}

	return nil
}

// canRemoveFKBackReference returns an error if the input backreference isn't
// allowed to be removed.
func (p *planner) canRemoveFKBackreference(
	ctx context.Context, from string, ref *descpb.ForeignKeyConstraint, behavior tree.DropBehavior,
) error {
	table, err := p.Descriptors().GetMutableTableVersionByID(ctx, ref.OriginTableID, p.txn)
	if err != nil {
		return err
	}
	if behavior != tree.DropCascade {
		return fmt.Errorf("%q is referenced by foreign key from table %q", from, table.Name)
	}
	// Check to see whether we're allowed to edit the table that has a
	// foreign key constraint on the table that we're dropping right now.
	return p.CheckPrivilege(ctx, table, privilege.CREATE)
}

// dropTableImpl does the work of dropping a table (and everything that depends
// on it if `cascade` is enabled). It returns a list of view names that were
// dropped due to `cascade` behavior. droppingParent indicates whether this
// table's parent (either database or schema) is being dropped
func (p *planner) dropTableImpl(
	ctx context.Context,
	tableDesc *tabledesc.Mutable,
	droppingParent bool,
	jobDesc string,
	behavior tree.DropBehavior,
) ([]string, error) {
	var droppedViews []string

	// Remove foreign key back references from tables that this table has foreign
	// keys to.
	// Copy out the set of outbound fks as it may be overwritten in the loop.
	outboundFKs := append([]descpb.ForeignKeyConstraint(nil), tableDesc.OutboundFKs...)
	for i := range outboundFKs {
		ref := &tableDesc.OutboundFKs[i]
		if err := p.removeFKBackReference(ctx, tableDesc, ref); err != nil {
			return droppedViews, err
		}
	}
	tableDesc.OutboundFKs = nil

	// Remove foreign key forward references from tables that have foreign keys
	// to this table.
	// Copy out the set of inbound fks as it may be overwritten in the loop.
	inboundFKs := append([]descpb.ForeignKeyConstraint(nil), tableDesc.InboundFKs...)
	for i := range inboundFKs {
		ref := &tableDesc.InboundFKs[i]
		if err := p.removeFKForBackReference(ctx, tableDesc, ref); err != nil {
			return droppedViews, err
		}
	}
	tableDesc.InboundFKs = nil

	// Remove sequence dependencies.
	for _, col := range tableDesc.PublicColumns() {
		if err := p.removeSequenceDependencies(ctx, tableDesc, col); err != nil {
			return droppedViews, err
		}
	}

	// Drop sequences that the columns of the table own.
	for _, col := range tableDesc.PublicColumns() {
		if err := p.dropSequencesOwnedByCol(ctx, col, !droppingParent, behavior); err != nil {
			return droppedViews, err
		}
	}

	// Drop all views that depend on this table, assuming that we wouldn't have
	// made it to this point if `cascade` wasn't enabled.
	// Copy out the set of dependencies as it may be overwritten in the loop.
	dependedOnBy := append([]descpb.TableDescriptor_Reference(nil), tableDesc.DependedOnBy...)
	for _, ref := range dependedOnBy {
		viewDesc, err := p.getViewDescForCascade(
			ctx, string(tableDesc.DescriptorType()), tableDesc.Name, tableDesc.ParentID, ref.ID, tree.DropCascade,
		)
		if err != nil {
			return droppedViews, err
		}
		// This view is already getting dropped. Don't do it twice.
		if viewDesc.Dropped() {
			continue
		}
		cascadedViews, err := p.dropViewImpl(ctx, viewDesc, !droppingParent, "dropping dependent view", tree.DropCascade)
		if err != nil {
			return droppedViews, err
		}

		qualifiedView, err := p.getQualifiedTableName(ctx, viewDesc)
		if err != nil {
			return droppedViews, err
		}

		droppedViews = append(droppedViews, cascadedViews...)
		droppedViews = append(droppedViews, qualifiedView.FQString())
	}

	err := p.removeTableComments(ctx, tableDesc)
	if err != nil {
		return droppedViews, err
	}

	// Remove any references to types.
	//
	// Note: In some historical context this attempted to defer these removals to
	// the asynchronous schema change in the case that the parent was being
	// dropped. This optimization was, as I understand it, to avoid creating
	// so many descriptor writes if the type was definitely being dropped. This
	// would be the case if the database were being dropped as theoretically cross
	// database types have never been permitted. Unfortunately, the droppingParent
	// flag does not indicate whether it's the schema or parent being dropped.
	//
	// TODO(ajwerner): Consider adding a flag to indicate what is actually being
	// dropped and to omit this step if it is the database rather than the schema.
	if err := p.removeBackRefsFromAllTypesInTable(ctx, tableDesc); err != nil {
		return droppedViews, err
	}

	err = p.initiateDropTable(ctx, tableDesc, !droppingParent, jobDesc)
	return droppedViews, err
}

// UnsplitRangesInSpan unsplist any manually split ranges within a span.
// TODO(Chengxiong): move this function to gc_job.go in 22.2
func UnsplitRangesInSpan(ctx context.Context, kvDB *kv.DB, span roachpb.Span) error {
	ranges, err := kvclient.ScanMetaKVs(ctx, kvDB.NewTxn(ctx, "unsplit-ranges-in-span"), span)
	if err != nil {
		return err
	}
	for _, r := range ranges {
		var desc roachpb.RangeDescriptor
		if err := r.ValueProto(&desc); err != nil {
			return err
		}

		if !span.ContainsKey(desc.StartKey.AsRawKey()) {
			continue
		}

		if !desc.GetStickyBit().IsEmpty() {
			// Swallow "key is not the start of a range" errors because it would mean
			// that the sticky bit was removed and merged concurrently. DROP TABLE
			// should not fail because of this.
			if err := kvDB.AdminUnsplit(ctx, desc.StartKey); err != nil &&
				!strings.Contains(err.Error(), "is not the start of a range") {
				return err
			}
		}
	}

	return nil
}

// unsplitRangesForTable unsplit any manually split ranges within the tablespan.
// TODO(Chengxiong): Remove this function in 22.2
func (p *planner) unsplitRangesForTable(ctx context.Context, tableDesc *tabledesc.Mutable) error {
	// Gate this on being the system tenant because secondary tenants aren't
	// allowed to scan the meta ranges directly.
	if !p.ExecCfg().Codec.ForSystemTenant() {
		return nil
	}

	span := tableDesc.TableSpan(p.ExecCfg().Codec)
	return UnsplitRangesInSpan(ctx, p.execCfg.DB, span)
}

// drainName when set implies that the name needs to go through the draining
// names process. This parameter is always passed in as true except from
// TRUNCATE which directly deletes the old name to id map and doesn't need
// drain the old map.
func (p *planner) initiateDropTable(
	ctx context.Context, tableDesc *tabledesc.Mutable, queueJob bool, jobDesc string,
) error {
	if tableDesc.Dropped() {
		return errors.Errorf("table %q is already being dropped", tableDesc.Name)
	}

	// Exit early with an error if the table is undergoing a declarative schema
	// change, before we try to get job IDs and update job statuses later. See
	// createOrUpdateSchemaChangeJob.
	if tableDesc.GetDeclarativeSchemaChangerState() != nil {
		return pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
			"cannot perform a schema change on table %q while it is undergoing a declarative schema change",
			tableDesc.GetName(),
		)
	}

	// Use the delayed GC mechanism to schedule usage of the more efficient
	// ClearRange pathway.
	if tableDesc.IsTable() {
		tableDesc.DropTime = timeutil.Now().UnixNano()
	}

	// TODO(Chengxiong): Remove this range unsplitting in 22.2
	st := p.EvalContext().Settings
	if !st.Version.IsActive(ctx, clusterversion.UnsplitRangesInAsyncGCJobs) {
		// Unsplit all manually split ranges in the table so they can be
		// automatically merged by the merge queue.
		if err := p.unsplitRangesForTable(ctx, tableDesc); err != nil {
			return err
		}
	}

	// Actually mark table descriptor as dropped.
	tableDesc.SetDropped()

	// Delete namespace entry for table.
	b := p.txn.NewBatch()
	p.dropNamespaceEntry(ctx, b, tableDesc)
	if err := p.txn.Run(ctx, b); err != nil {
		return err
	}

	// For this table descriptor, mark all previous jobs scheduled for schema changes as successful
	// and delete them from the schema change job cache.
	//
	// Since the table is being dropped, any previous schema changes to the table do not need to complete
	// and can be put in a terminal state such as Succeeded. Deleting the jobs from the cache ensures that
	// subsequent schema changes in the transaction (ie. this drop table statement) do not get a cache hit
	// and do not try to update succeeded jobs, which would raise an error. Instead, this drop table
	// statement will create a new job to drop the table.
	//
	// Note that we still wait for jobs removed from the cache to finish running
	// after the transaction, since they're not removed from the jobsCollection.
	// Also, changes made here do not affect schema change jobs created in this
	// transaction with no mutation ID; they remain in the cache, and will be
	// updated when writing the job record to drop the table.
	if err := p.markTableMutationJobsSuccessful(ctx, tableDesc); err != nil {
		return err
	}

	// Initiate an immediate schema change. When dropping a table
	// in a session, the data and the descriptor are not deleted.
	// Instead, that is taken care of asynchronously by the schema
	// change manager, which is notified via a system config gossip.
	// The schema change manager will properly schedule deletion of
	// the underlying data when the GC deadline expires.
	return p.writeDropTable(ctx, tableDesc, queueJob, jobDesc)
}

// Mark jobs as succeeded when possible, but be defensive about jobs that
// are already in a terminal state or nonexistent. This could happen for
// schema change jobs that couldn't be successfully reverted and ended up in
// a failed state. Such jobs could have already been GCed from the jobs
// table by the time this code runs.
//
// This function is called while dropping a table or truncate. Therefore, we
// have to stop the ongoing mutation jobs on the table. If the job is in the
// cache that was created during this transaction, we can simply remove the
// job from the cache because the job is not started yet. If the mutation job
// was started in another transaction, we mark the job as succeeded or failed
// to stop the job.
func (p *planner) markTableMutationJobsSuccessful(
	ctx context.Context, tableDesc *tabledesc.Mutable,
) error {
	for _, mj := range tableDesc.MutationJobs {
		jobID := mj.JobID
		// Jobs are only added in the cache during the transaction and are created
		// in a batch only when the transaction commits. So, if a job's record exists
		// in the cache, we can simply delete that record from cache because the
		// job is not created yet.
		if record, exists := p.ExtendedEvalContext().SchemaChangeJobRecords[tableDesc.ID]; exists && record.JobID == jobID {
			delete(p.ExtendedEvalContext().SchemaChangeJobRecords, tableDesc.ID)
			continue
		}
		mutationJob, err := p.execCfg.JobRegistry.LoadJobWithTxn(ctx, jobID, p.txn)
		if err != nil {
			if jobs.HasJobNotFoundError(err) {
				log.Warningf(ctx, "mutation job %d not found", jobID)
				continue
			}
			return err
		}
		if err := mutationJob.Update(
			ctx, p.txn, func(txn *kv.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
				status := md.Status
				switch status {
				case jobs.StatusSucceeded, jobs.StatusCanceled, jobs.StatusFailed, jobs.StatusRevertFailed:
					log.Warningf(ctx, "mutation job %d in unexpected state %s", jobID, status)
					return nil
				case jobs.StatusRunning, jobs.StatusPending:
					status = jobs.StatusSucceeded
				default:
					// We shouldn't mark jobs as succeeded if they're not in a state where
					// they're eligible to ever succeed, so mark them as failed.
					status = jobs.StatusFailed
				}
				log.Infof(ctx, "marking mutation job %d for dropped table as %s", jobID, status)
				ju.UpdateStatus(status)
				return nil
			}); err != nil {
			return errors.Wrap(err, "updating mutation job for dropped table")
		}
	}
	return nil
}

func (p *planner) removeFKForBackReference(
	ctx context.Context, tableDesc *tabledesc.Mutable, ref *descpb.ForeignKeyConstraint,
) error {
	var originTableDesc *tabledesc.Mutable
	// We don't want to lookup/edit a second copy of the same table.
	if tableDesc.ID == ref.OriginTableID {
		originTableDesc = tableDesc
	} else {
		lookup, err := p.Descriptors().GetMutableTableVersionByID(ctx, ref.OriginTableID, p.txn)
		if err != nil {
			return errors.Wrapf(err, "error resolving origin table ID %d", ref.OriginTableID)
		}
		originTableDesc = lookup
	}
	if originTableDesc.Dropped() {
		// The origin table is being dropped. No need to modify it further.
		return nil
	}

	if err := removeFKForBackReferenceFromTable(originTableDesc, ref, tableDesc); err != nil {
		return err
	}

	name, err := p.getQualifiedTableName(ctx, tableDesc)
	if err != nil {
		return err
	}
	jobDesc := fmt.Sprintf("updating table %q after removing constraint %q from table %q", originTableDesc.GetName(), ref.Name, name.FQString())
	return p.writeSchemaChange(ctx, originTableDesc, descpb.InvalidMutationID, jobDesc)
}

// removeFKBackReferenceFromTable edits the supplied originTableDesc to
// remove the foreign key constraint that corresponds to the supplied
// backreference, which is a member of the supplied referencedTableDesc.
func removeFKForBackReferenceFromTable(
	originTableDesc *tabledesc.Mutable,
	backref *descpb.ForeignKeyConstraint,
	referencedTableDesc catalog.TableDescriptor,
) error {
	matchIdx := -1
	for i, fk := range originTableDesc.OutboundFKs {
		if fk.ReferencedTableID == referencedTableDesc.GetID() && fk.Name == backref.Name {
			// We found a match! We want to delete it from the list now.
			matchIdx = i
			break
		}
	}
	if matchIdx == -1 {
		// There was no match: no back reference in the referenced table that
		// matched the foreign key constraint that we were trying to delete.
		// This really shouldn't happen...
		return errors.AssertionFailedf("there was no foreign key constraint "+
			"for backreference %v on table %q", backref, originTableDesc.Name)
	}
	// Delete our match.
	originTableDesc.OutboundFKs = append(
		originTableDesc.OutboundFKs[:matchIdx],
		originTableDesc.OutboundFKs[matchIdx+1:]...)
	return nil
}

// removeFKBackReference removes the FK back reference from the table that is
// referenced by the input constraint.
func (p *planner) removeFKBackReference(
	ctx context.Context, tableDesc *tabledesc.Mutable, ref *descpb.ForeignKeyConstraint,
) error {
	var referencedTableDesc *tabledesc.Mutable
	// We don't want to lookup/edit a second copy of the same table.
	if tableDesc.ID == ref.ReferencedTableID {
		referencedTableDesc = tableDesc
	} else {
		lookup, err := p.Descriptors().GetMutableTableVersionByID(ctx, ref.ReferencedTableID, p.txn)
		if err != nil {
			return errors.Wrapf(err, "error resolving referenced table ID %d", ref.ReferencedTableID)
		}
		referencedTableDesc = lookup
	}
	if referencedTableDesc.Dropped() {
		// The referenced table is being dropped. No need to modify it further.
		return nil
	}

	if err := removeFKBackReferenceFromTable(referencedTableDesc, ref.Name, tableDesc); err != nil {
		return err
	}

	name, err := p.getQualifiedTableName(ctx, tableDesc)
	if err != nil {
		return err
	}
	jobDesc := fmt.Sprintf("updating table %q after removing constraint %q from table %q", referencedTableDesc.GetName(), ref.Name, name.FQString())

	return p.writeSchemaChange(ctx, referencedTableDesc, descpb.InvalidMutationID, jobDesc)
}

// removeFKBackReferenceFromTable edits the supplied referencedTableDesc to
// remove the foreign key backreference that corresponds to the supplied fk,
// which is a member of the supplied originTableDesc.
func removeFKBackReferenceFromTable(
	referencedTableDesc *tabledesc.Mutable, fkName string, originTableDesc catalog.TableDescriptor,
) error {
	matchIdx := -1
	for i, backref := range referencedTableDesc.InboundFKs {
		if backref.OriginTableID == originTableDesc.GetID() && backref.Name == fkName {
			// We found a match! We want to delete it from the list now.
			matchIdx = i
			break
		}
	}
	if matchIdx == -1 {
		// There was no match: no back reference in the referenced table that
		// matched the foreign key constraint that we were trying to delete.
		// This really shouldn't happen...
		return errors.AssertionFailedf("there was no foreign key backreference "+
			"for constraint %q on table %q", fkName, originTableDesc.GetName())
	}
	// Delete our match.
	referencedTableDesc.InboundFKs = append(referencedTableDesc.InboundFKs[:matchIdx],
		referencedTableDesc.InboundFKs[matchIdx+1:]...)
	return nil
}

// removeMatchingReferences removes all refs from the provided slice that
// match the provided ID, returning the modified slice.
func removeMatchingReferences(
	refs []descpb.TableDescriptor_Reference, id descpb.ID,
) []descpb.TableDescriptor_Reference {
	updatedRefs := refs[:0]
	for _, ref := range refs {
		if ref.ID != id {
			updatedRefs = append(updatedRefs, ref)
		}
	}
	return updatedRefs
}

func (p *planner) removeTableComments(ctx context.Context, tableDesc *tabledesc.Mutable) error {
	_, err := p.ExtendedEvalContext().ExecCfg.InternalExecutor.ExecEx(
		ctx,
		"delete-table-comments",
		p.txn,
		sessiondata.InternalExecutorOverride{User: security.RootUserName()},
		"DELETE FROM system.comments WHERE object_id=$1",
		tableDesc.ID)
	if err != nil {
		return err
	}
	return err
}
