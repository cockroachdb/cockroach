// Copyright 2015 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package sql

import (
	"context"
	"fmt"

	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

type dropTableNode struct {
	n  *tree.DropTable
	td []toDelete
}

type toDelete struct {
	tn   *tree.TableName
	desc *sqlbase.TableDescriptor
}

// DropTable drops a table.
// Privileges: DROP on table.
//   Notes: postgres allows only the table owner to DROP a table.
//          mysql requires the DROP privilege on the table.
func (p *planner) DropTable(ctx context.Context, n *tree.DropTable) (planNode, error) {
	td := make([]toDelete, 0, len(n.Names))
	for i := range n.Names {
		name := &n.Names[i]
		tn, err := name.Normalize()
		if err != nil {
			return nil, err
		}
		droppedDesc, err := p.prepareDrop(ctx, tn, !n.IfExists, requireTableDesc)
		if err != nil {
			return nil, err
		}
		if droppedDesc == nil {
			continue
		}

		td = append(td, toDelete{tn, droppedDesc})
	}

	dropping := make(map[sqlbase.ID]bool)
	for _, d := range td {
		dropping[d.desc.ID] = true
	}

	for _, toDel := range td {
		droppedDesc := toDel.desc
		for _, idx := range droppedDesc.AllNonDropIndexes() {
			for _, ref := range idx.ReferencedBy {
				if !dropping[ref.Table] {
					if _, err := p.canRemoveFK(ctx, droppedDesc.Name, ref, n.DropBehavior); err != nil {
						return nil, err
					}
				}
			}
			for _, ref := range idx.InterleavedBy {
				if !dropping[ref.Table] {
					if err := p.canRemoveInterleave(ctx, droppedDesc.Name, ref, n.DropBehavior); err != nil {
						return nil, err
					}
				}
			}
		}
		for _, ref := range droppedDesc.DependedOnBy {
			if !dropping[ref.ID] {
				if err := p.canRemoveDependentView(ctx, droppedDesc, ref, n.DropBehavior); err != nil {
					return nil, err
				}
			}
		}
	}

	if len(td) == 0 {
		return newZeroNode(nil /* columns */), nil
	}
	return &dropTableNode{n: n, td: td}, nil
}

func (n *dropTableNode) startExec(params runParams) error {
	ctx := params.ctx
	for _, toDel := range n.td {
		droppedDesc := toDel.desc
		if droppedDesc == nil {
			continue
		}
		droppedViews, err := params.p.dropTableImpl(params, droppedDesc)
		if err != nil {
			return err
		}
		// Log a Drop Table event for this table. This is an auditable log event
		// and is recorded in the same transaction as the table descriptor
		// update.
		if err := MakeEventLogger(params.extendedEvalCtx.ExecCfg).InsertEventRecord(
			ctx,
			params.p.txn,
			EventLogDropTable,
			int32(droppedDesc.ID),
			int32(params.extendedEvalCtx.NodeID),
			struct {
				TableName           string
				Statement           string
				User                string
				CascadeDroppedViews []string
			}{toDel.tn.FQString(), n.n.String(),
				params.SessionData().User, droppedViews},
		); err != nil {
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
	ctx context.Context, name *tree.TableName, required bool, requiredType requiredType,
) (tableDesc *sqlbase.TableDescriptor, err error) {
	// DDL statements avoid the cache to avoid leases, and can view non-public descriptors.
	// TODO(vivek): check if the cache can be used.
	p.runWithOptions(resolveFlags{allowAdding: true, skipCache: true}, func() {
		tableDesc, err = ResolveExistingObject(ctx, p, name, required, requiredType)
	})
	if err != nil {
		return nil, err
	}
	if tableDesc == nil {
		return nil, err
	}

	if err := p.CheckPrivilege(ctx, tableDesc, privilege.DROP); err != nil {
		return nil, err
	}
	return tableDesc, nil
}

func (p *planner) canRemoveFK(
	ctx context.Context, from string, ref sqlbase.ForeignKeyReference, behavior tree.DropBehavior,
) (*sqlbase.TableDescriptor, error) {
	table, err := sqlbase.GetTableDescFromID(ctx, p.txn, ref.Table)
	if err != nil {
		return nil, err
	}
	if behavior != tree.DropCascade {
		return nil, fmt.Errorf("%q is referenced by foreign key from table %q", from, table.Name)
	}
	if err := p.CheckPrivilege(ctx, table, privilege.CREATE); err != nil {
		return nil, err
	}
	return table, nil
}

func (p *planner) canRemoveInterleave(
	ctx context.Context, from string, ref sqlbase.ForeignKeyReference, behavior tree.DropBehavior,
) error {
	table, err := sqlbase.GetTableDescFromID(ctx, p.txn, ref.Table)
	if err != nil {
		return err
	}
	// TODO(dan): It's possible to DROP a table that has a child interleave, but
	// some loose ends would have to be addresssed. The zone would have to be
	// kept and deleted when the last table in it is removed. Also, the dropped
	// table's descriptor would have to be kept around in some Dropped but
	// non-public state for referential integrity of the `InterleaveDescriptor`
	// pointers.
	if behavior != tree.DropCascade {
		return pgerror.UnimplementedWithIssueErrorf(
			8036, "%q is interleaved by table %q", from, table.Name)
	}
	return p.CheckPrivilege(ctx, table, privilege.CREATE)
}

func (p *planner) removeFK(
	ctx context.Context, ref sqlbase.ForeignKeyReference, table *sqlbase.TableDescriptor,
) error {
	if table == nil {
		var err error
		table, err = sqlbase.GetTableDescFromID(ctx, p.txn, ref.Table)
		if err != nil {
			return err
		}
	}
	if table.Dropped() {
		// The referenced table is being dropped. No need to modify it further.
		return nil
	}
	idx, err := table.FindIndexByID(ref.Index)
	if err != nil {
		return err
	}
	idx.ForeignKey = sqlbase.ForeignKeyReference{}
	return p.saveNonmutationAndNotify(ctx, table)
}

func (p *planner) removeInterleave(ctx context.Context, ref sqlbase.ForeignKeyReference) error {
	table, err := sqlbase.GetTableDescFromID(ctx, p.txn, ref.Table)
	if err != nil {
		return err
	}
	if table.Dropped() {
		// The referenced table is being dropped. No need to modify it further.
		return nil
	}
	idx, err := table.FindIndexByID(ref.Index)
	if err != nil {
		return err
	}
	idx.Interleave.Ancestors = nil
	return p.saveNonmutationAndNotify(ctx, table)
}

// dropTableImpl does the work of dropping a table (and everything that depends
// on it if `cascade` is enabled). It returns a list of view names that were
// dropped due to `cascade` behavior.
func (p *planner) dropTableImpl(
	params runParams, tableDesc *sqlbase.TableDescriptor,
) ([]string, error) {
	ctx := params.ctx

	var droppedViews []string

	// Remove FK and interleave relationships.
	for _, idx := range tableDesc.AllNonDropIndexes() {
		if idx.ForeignKey.IsSet() {
			if err := p.removeFKBackReference(ctx, tableDesc, idx); err != nil {
				return droppedViews, err
			}
		}
		if len(idx.Interleave.Ancestors) > 0 {
			if err := p.removeInterleaveBackReference(ctx, tableDesc, idx); err != nil {
				return droppedViews, err
			}
		}
		for _, ref := range idx.ReferencedBy {
			// Nil forces re-fetching tables, since they may have been modified.
			if err := p.removeFK(ctx, ref, nil); err != nil {
				return droppedViews, err
			}
		}
		for _, ref := range idx.InterleavedBy {
			if err := p.removeInterleave(ctx, ref); err != nil {
				return droppedViews, err
			}
		}
	}

	// Remove sequence dependencies.
	for _, columnDesc := range tableDesc.Columns {
		if err := removeSequenceDependencies(tableDesc, &columnDesc, params); err != nil {
			return droppedViews, err
		}
	}

	// Drop all views that depend on this table, assuming that we wouldn't have
	// made it to this point if `cascade` wasn't enabled.
	for _, ref := range tableDesc.DependedOnBy {
		viewDesc, err := p.getViewDescForCascade(
			ctx, tableDesc.TypeName(), tableDesc.Name, tableDesc.ParentID, ref.ID, tree.DropCascade,
		)
		if err != nil {
			return droppedViews, err
		}
		// This view is already getting dropped. Don't do it twice.
		if viewDesc.Dropped() {
			continue
		}
		cascadedViews, err := p.dropViewImpl(ctx, viewDesc, tree.DropCascade)
		if err != nil {
			return droppedViews, err
		}
		droppedViews = append(droppedViews, cascadedViews...)
		droppedViews = append(droppedViews, viewDesc.Name)
	}

	err := p.initiateDropTable(ctx, tableDesc, true /* drain name */)
	return droppedViews, err
}

// drainName when set implies that the name needs to go through the draining
// names process. This parameter is always passed in as true except from
// TRUNCATE which directly deletes the old name to id map and doesn't need
// drain the old map.
func (p *planner) initiateDropTable(
	ctx context.Context, tableDesc *sqlbase.TableDescriptor, drainName bool,
) error {
	if err := tableDesc.SetUpVersion(); err != nil {
		return err
	}

	// If the table is not interleaved and the ClearRange feature is
	// enabled in the cluster, use the delayed GC mechanism to schedule
	// usage of the more efficient ClearRange pathway. ClearRange will
	// only work if the entire hierarchy of interleaved tables are
	// dropped at once, as with ON DELETE CASCADE where the top-level
	// "root" table is dropped.
	//
	// TODO(bram): If interleaved and ON DELETE CASCADE, we will be
	// able to use this faster mechanism.
	if !tableDesc.IsInterleaved() &&
		p.ExecCfg().Settings.Version.IsActive(cluster.VersionClearRange) {
		// Get the zone config applying to this table in order to
		// ensure there is a GC TTL.
		_, _, _, err := GetZoneConfigInTxn(
			ctx, p.txn, uint32(tableDesc.ID), &sqlbase.IndexDescriptor{}, "",
		)
		if err != nil {
			return err
		}

		tableDesc.DropTime = timeutil.Now().UnixNano()
	}

	tableDesc.State = sqlbase.TableDescriptor_DROP
	if drainName {
		// Queue up name for draining.
		nameDetails := sqlbase.TableDescriptor_NameInfo{
			ParentID: tableDesc.ParentID,
			Name:     tableDesc.Name}
		tableDesc.DrainingNames = append(tableDesc.DrainingNames, nameDetails)
	}
	if err := p.writeTableDesc(ctx, tableDesc); err != nil {
		return err
	}

	// Initiate an immediate schema change. When dropping a table
	// in a session, the data and the descriptor are not deleted.
	// Instead, that is taken care of asynchronously by the schema
	// change manager, which is notified via a system config gossip.
	// The schema change manager will properly schedule deletion of
	// the underlying data when the GC deadline expires.
	p.notifySchemaChange(tableDesc, sqlbase.InvalidMutationID)

	return nil
}

func (p *planner) removeFKBackReference(
	ctx context.Context, tableDesc *sqlbase.TableDescriptor, idx sqlbase.IndexDescriptor,
) error {
	var t *sqlbase.TableDescriptor
	// We don't want to lookup/edit a second copy of the same table.
	if tableDesc.ID == idx.ForeignKey.Table {
		t = tableDesc
	} else {
		lookup, err := sqlbase.GetTableDescFromID(ctx, p.txn, idx.ForeignKey.Table)
		if err != nil {
			return errors.Errorf("error resolving referenced table ID %d: %v", idx.ForeignKey.Table, err)
		}
		t = lookup
	}
	if t.Dropped() {
		// The referenced table is being dropped. No need to modify it further.
		return nil
	}
	targetIdx, err := t.FindIndexByID(idx.ForeignKey.Index)
	if err != nil {
		return err
	}
	for k, ref := range targetIdx.ReferencedBy {
		if ref.Table == tableDesc.ID && ref.Index == idx.ID {
			targetIdx.ReferencedBy = append(targetIdx.ReferencedBy[:k], targetIdx.ReferencedBy[k+1:]...)
		}
	}
	return p.saveNonmutationAndNotify(ctx, t)
}

func (p *planner) removeInterleaveBackReference(
	ctx context.Context, tableDesc *sqlbase.TableDescriptor, idx sqlbase.IndexDescriptor,
) error {
	if len(idx.Interleave.Ancestors) == 0 {
		return nil
	}
	ancestor := idx.Interleave.Ancestors[len(idx.Interleave.Ancestors)-1]
	var t *sqlbase.TableDescriptor
	if ancestor.TableID == tableDesc.ID {
		t = tableDesc
	} else {
		lookup, err := sqlbase.GetTableDescFromID(ctx, p.txn, ancestor.TableID)
		if err != nil {
			return errors.Errorf("error resolving referenced table ID %d: %v", ancestor.TableID, err)
		}
		t = lookup
	}
	if t.Dropped() {
		// The referenced table is being dropped. No need to modify it further.
		return nil
	}
	targetIdx, err := t.FindIndexByID(ancestor.IndexID)
	if err != nil {
		return err
	}
	for k, ref := range targetIdx.InterleavedBy {
		if ref.Table == tableDesc.ID && ref.Index == idx.ID {
			targetIdx.InterleavedBy = append(targetIdx.InterleavedBy[:k], targetIdx.InterleavedBy[k+1:]...)
		}
	}
	if t != tableDesc {
		return p.saveNonmutationAndNotify(ctx, t)
	}
	return nil
}

// removeMatchingReferences removes all refs from the provided slice that
// match the provided ID, returning the modified slice.
func removeMatchingReferences(
	refs []sqlbase.TableDescriptor_Reference, id sqlbase.ID,
) []sqlbase.TableDescriptor_Reference {
	updatedRefs := refs[:0]
	for _, ref := range refs {
		if ref.ID != id {
			updatedRefs = append(updatedRefs, ref)
		}
	}
	return updatedRefs
}
