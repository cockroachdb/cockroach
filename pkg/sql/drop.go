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
	"fmt"

	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

type dropDatabaseNode struct {
	n      *parser.DropDatabase
	dbDesc *sqlbase.DatabaseDescriptor
	td     []*sqlbase.TableDescriptor
}

// DropDatabase drops a database.
// Privileges: DROP on database and DROP on all tables in the database.
//   Notes: postgres allows only the database owner to DROP a database.
//          mysql requires the DROP privileges on the database.
// TODO(XisiHuang): our DROP DATABASE is like the postgres DROP SCHEMA
// (cockroach database == postgres schema). the postgres default of not
// dropping the schema if there are dependent objects is more sensible
// (see the RESTRICT and CASCADE options).
func (p *planner) DropDatabase(ctx context.Context, n *parser.DropDatabase) (planNode, error) {
	if n.Name == "" {
		return nil, errEmptyDatabaseName
	}

	// Check that the database exists.
	dbDesc, err := getDatabaseDesc(ctx, p.txn, p.getVirtualTabler(), string(n.Name))
	if err != nil {
		return nil, err
	}
	if dbDesc == nil {
		if n.IfExists {
			// Noop.
			return &zeroNode{}, nil
		}
		return nil, sqlbase.NewUndefinedDatabaseError(string(n.Name))
	}

	if err := p.CheckPrivilege(dbDesc, privilege.DROP); err != nil {
		return nil, err
	}

	tbNames, err := getTableNames(ctx, p.txn, p.getVirtualTabler(), dbDesc, false)
	if err != nil {
		return nil, err
	}

	if len(tbNames) > 0 && n.DropBehavior != parser.DropCascade {
		return nil, pgerror.NewErrorf(pgerror.CodeDependentObjectsStillExistError,
			"database %q is not empty and CASCADE was not specified",
			parser.ErrString(parser.Name(dbDesc.Name)))
	}

	td := make([]*sqlbase.TableDescriptor, len(tbNames))
	for i := range tbNames {
		tbDesc, err := p.dropTableOrViewPrepare(ctx, &tbNames[i])
		if err != nil {
			return nil, err
		}
		if tbDesc == nil {
			// Database claims to have this table, but it does not exist.
			return nil, errors.Errorf("table %q was described by database %q, but does not exist",
				tbNames[i].String(), n.Name)
		}
		// Recursively check permissions on all dependent views, since some may
		// be in different databases.
		for _, ref := range tbDesc.DependedOnBy {
			if err := p.canRemoveDependentView(ctx, tbDesc, ref, parser.DropCascade); err != nil {
				return nil, err
			}
		}
		td[i] = tbDesc
	}

	td, err = p.filterCascadedTables(ctx, td)
	if err != nil {
		return nil, err
	}

	return &dropDatabaseNode{n: n, dbDesc: dbDesc, td: td}, nil
}

// filterCascadedTables takes a list of table descriptors and removes any
// descriptors from the list that are dependent on other descriptors in the
// list (e.g. if view v1 depends on table t1, then v1 will be filtered from
// the list).
func (p *planner) filterCascadedTables(
	ctx context.Context, tables []*sqlbase.TableDescriptor,
) ([]*sqlbase.TableDescriptor, error) {
	// Accumulate the set of all tables/views that will be deleted by cascade
	// behavior so that we can filter them out of the list.
	cascadedTables := make(map[sqlbase.ID]bool)
	for _, desc := range tables {
		if err := p.accumulateDependentTables(ctx, cascadedTables, desc); err != nil {
			return nil, err
		}
	}
	filteredTableList := make([]*sqlbase.TableDescriptor, 0, len(tables))
	for _, desc := range tables {
		if !cascadedTables[desc.ID] {
			filteredTableList = append(filteredTableList, desc)
		}
	}
	return filteredTableList, nil
}

func (p *planner) accumulateDependentTables(
	ctx context.Context, dependentTables map[sqlbase.ID]bool, desc *sqlbase.TableDescriptor,
) error {
	for _, ref := range desc.DependedOnBy {
		dependentTables[ref.ID] = true
		dependentDesc, err := sqlbase.GetTableDescFromID(ctx, p.txn, ref.ID)
		if err != nil {
			return err
		}
		if err := p.accumulateDependentTables(ctx, dependentTables, dependentDesc); err != nil {
			return err
		}
	}
	return nil
}

func (n *dropDatabaseNode) Start(params runParams) error {
	ctx := params.ctx
	p := params.p
	tbNameStrings := make([]string, 0, len(n.td))
	for _, tbDesc := range n.td {
		if tbDesc.IsView() {
			cascadedViews, err := p.dropViewImpl(ctx, tbDesc, parser.DropCascade)
			if err != nil {
				return err
			}
			tbNameStrings = append(tbNameStrings, cascadedViews...)
		} else {
			cascadedViews, err := p.dropTableImpl(ctx, tbDesc)
			if err != nil {
				return err
			}
			tbNameStrings = append(tbNameStrings, cascadedViews...)
		}
		tbNameStrings = append(tbNameStrings, tbDesc.Name)
	}

	zoneKey, nameKey, descKey := getKeysForDatabaseDescriptor(n.dbDesc)
	zoneKeyPrefix := sqlbase.MakeZoneKeyPrefix(n.dbDesc.ID)

	b := &client.Batch{}
	if p.session.Tracing.KVTracingEnabled() {
		log.VEventf(ctx, 2, "Del %s", descKey)
		log.VEventf(ctx, 2, "Del %s", nameKey)
		log.VEventf(ctx, 2, "DelRange %s", zoneKeyPrefix)
	}
	b.Del(descKey)
	b.Del(nameKey)
	// Delete the zone config entry for this database.
	b.DelRange(zoneKeyPrefix, zoneKeyPrefix.PrefixEnd(), false /* returnKeys */)

	p.session.setTestingVerifyMetadata(func(systemConfig config.SystemConfig) error {
		for _, key := range [...]roachpb.Key{descKey, nameKey, zoneKey} {
			if err := expectDeleted(systemConfig, key); err != nil {
				return err
			}
		}
		return nil
	})

	p.session.tables.addUncommittedDatabase(n.dbDesc.Name, n.dbDesc.ID, true /*dropped*/)

	if err := p.txn.Run(ctx, b); err != nil {
		return err
	}

	// Log Drop Database event. This is an auditable log event and is recorded
	// in the same transaction as the table descriptor update.
	if err := MakeEventLogger(p.LeaseMgr()).InsertEventRecord(
		ctx,
		p.txn,
		EventLogDropDatabase,
		int32(n.dbDesc.ID),
		int32(p.evalCtx.NodeID),
		struct {
			DatabaseName          string
			Statement             string
			User                  string
			DroppedTablesAndViews []string
		}{n.n.Name.String(), n.n.String(), p.session.User, tbNameStrings},
	); err != nil {
		return err
	}
	return nil
}

func (*dropDatabaseNode) Next(runParams) (bool, error) { return false, nil }
func (*dropDatabaseNode) Close(context.Context)        {}
func (*dropDatabaseNode) Values() parser.Datums        { return parser.Datums{} }

type dropIndexNode struct {
	n        *parser.DropIndex
	idxNames []fullIndexName
}

type fullIndexName struct {
	tn      *parser.TableName
	idxName parser.Name
}

// DropIndex drops an index.
// Privileges: CREATE on table.
//   Notes: postgres allows only the index owner to DROP an index.
//          mysql requires the INDEX privilege on the table.
func (p *planner) DropIndex(ctx context.Context, n *parser.DropIndex) (planNode, error) {
	idxNames := make([]fullIndexName, len(n.IndexList))
	for i, index := range n.IndexList {
		tn, err := p.expandIndexName(ctx, index)
		if err != nil {
			return nil, err
		}

		tableDesc, err := MustGetTableDesc(ctx, p.txn, p.getVirtualTabler(), tn, true /*allowAdding*/)
		if err != nil {
			return nil, err
		}

		if err := p.CheckPrivilege(tableDesc, privilege.CREATE); err != nil {
			return nil, err
		}

		idxNames[i].tn = tn
		idxNames[i].idxName = index.Index
	}
	return &dropIndexNode{n: n, idxNames: idxNames}, nil
}

func (n *dropIndexNode) Start(params runParams) error {
	ctx := params.ctx
	for _, index := range n.idxNames {
		// Need to retrieve the descriptor again for each index name in
		// the list: when two or more index names refer to the same table,
		// the mutation list and new version number created by the first
		// drop need to be visible to the second drop.
		tableDesc, err := getTableDesc(ctx, params.p.txn, params.p.getVirtualTabler(), index.tn)
		if err != nil || tableDesc == nil {
			// newPlan() and Start() ultimately run within the same
			// transaction. If we got a descriptor during newPlan(), we
			// must have it here too.
			panic(fmt.Sprintf("table descriptor for %s became unavailable within same txn", index.tn))
		}

		if err := params.p.dropIndexByName(
			ctx, index.idxName, tableDesc, n.n.IfExists, n.n.DropBehavior,
			parser.AsStringWithFlags(n.n, parser.FmtSimpleQualified),
		); err != nil {
			return err
		}
	}
	return nil
}

func (p *planner) dropIndexByName(
	ctx context.Context,
	idxName parser.Name,
	tableDesc *sqlbase.TableDescriptor,
	ifExists bool,
	behavior parser.DropBehavior,
	jobDesc string,
) error {
	idx, dropped, err := tableDesc.FindIndexByName(string(idxName))
	if err != nil {
		if ifExists {
			// Noop.
			return nil
		}
		// Index does not exist, but we want it to: error out.
		return err
	}
	if dropped {
		return nil
	}
	// Queue the mutation.
	var droppedViews []string
	if idx.ForeignKey.IsSet() {
		if behavior != parser.DropCascade {
			return fmt.Errorf("index %q is in use as a foreign key constraint", idx.Name)
		}
		if err := p.removeFKBackReference(ctx, tableDesc, idx); err != nil {
			return err
		}
	}

	if len(idx.Interleave.Ancestors) > 0 {
		if err := p.removeInterleaveBackReference(ctx, tableDesc, idx); err != nil {
			return err
		}
	}

	for _, ref := range idx.ReferencedBy {
		fetched, err := p.canRemoveFK(ctx, idx.Name, ref, behavior)
		if err != nil {
			return err
		}
		if err := p.removeFK(ctx, ref, fetched); err != nil {
			return err
		}
	}
	for _, ref := range idx.InterleavedBy {
		if err := p.removeInterleave(ctx, ref); err != nil {
			return err
		}
	}

	for _, tableRef := range tableDesc.DependedOnBy {
		if tableRef.IndexID == idx.ID {
			// Ensure that we have DROP privilege on all dependent views
			err := p.canRemoveDependentViewGeneric(
				ctx, "index", idx.Name, tableDesc.ParentID, tableRef, behavior)
			if err != nil {
				return err
			}
			viewDesc, err := p.getViewDescForCascade(
				ctx, "index", idx.Name, tableDesc.ParentID, tableRef.ID, behavior,
			)
			if err != nil {
				return err
			}
			cascadedViews, err := p.removeDependentView(ctx, tableDesc, viewDesc)
			if err != nil {
				return err
			}
			droppedViews = append(droppedViews, viewDesc.Name)
			droppedViews = append(droppedViews, cascadedViews...)
		}
	}
	found := false
	for i := range tableDesc.Indexes {
		if tableDesc.Indexes[i].ID == idx.ID {
			if err := tableDesc.AddIndexMutation(tableDesc.Indexes[i], sqlbase.DescriptorMutation_DROP); err != nil {
				return err
			}
			tableDesc.Indexes = append(tableDesc.Indexes[:i], tableDesc.Indexes[i+1:]...)
			found = true
			break
		}
	}
	if !found {
		return fmt.Errorf("index %q in the middle of being added, try again later", idxName)
	}

	if err := tableDesc.Validate(ctx, p.txn); err != nil {
		return err
	}
	mutationID, err := p.createSchemaChangeJob(ctx, tableDesc, jobDesc)
	if err != nil {
		return err
	}
	if err := p.writeTableDesc(ctx, tableDesc); err != nil {
		return err
	}
	// Record index drop in the event log. This is an auditable log event
	// and is recorded in the same transaction as the table descriptor
	// update.
	if err := MakeEventLogger(p.LeaseMgr()).InsertEventRecord(
		ctx,
		p.txn,
		EventLogDropIndex,
		int32(tableDesc.ID),
		int32(p.evalCtx.NodeID),
		struct {
			TableName           string
			IndexName           string
			Statement           string
			User                string
			MutationID          uint32
			CascadeDroppedViews []string
		}{tableDesc.Name, string(idxName), jobDesc, p.session.User, uint32(mutationID),
			droppedViews},
	); err != nil {
		return err
	}
	p.notifySchemaChange(tableDesc, mutationID)

	return nil
}

func (*dropIndexNode) Next(runParams) (bool, error) { return false, nil }
func (*dropIndexNode) Close(context.Context)        {}
func (*dropIndexNode) Values() parser.Datums        { return parser.Datums{} }

type dropViewNode struct {
	n  *parser.DropView
	td []*sqlbase.TableDescriptor
}

// DropView drops a view.
// Privileges: DROP on view.
//   Notes: postgres allows only the view owner to DROP a view.
//          mysql requires the DROP privilege on the view.
func (p *planner) DropView(ctx context.Context, n *parser.DropView) (planNode, error) {
	td := make([]*sqlbase.TableDescriptor, 0, len(n.Names))
	for _, name := range n.Names {
		tn, err := name.NormalizeTableName()
		if err != nil {
			return nil, err
		}
		if err := tn.QualifyWithDatabase(p.session.Database); err != nil {
			return nil, err
		}

		droppedDesc, err := p.dropTableOrViewPrepare(ctx, tn)
		if err != nil {
			return nil, err
		}
		if droppedDesc == nil {
			if n.IfExists {
				continue
			}
			// View does not exist, but we want it to: error out.
			return nil, sqlbase.NewUndefinedRelationError(tn)
		}
		if !droppedDesc.IsView() {
			return nil, sqlbase.NewWrongObjectTypeError(tn, "view")
		}

		td = append(td, droppedDesc)
	}

	// Ensure this view isn't depended on by any other views, or that if it is
	// then `cascade` was specified or it was also explicitly specified in the
	// DROP VIEW command.
	for _, droppedDesc := range td {
		for _, ref := range droppedDesc.DependedOnBy {
			// Don't verify that we can remove a dependent view if that dependent
			// view was explicitly specified in the DROP VIEW command.
			if descInSlice(ref.ID, td) {
				continue
			}
			if err := p.canRemoveDependentView(ctx, droppedDesc, ref, n.DropBehavior); err != nil {
				return nil, err
			}
		}
	}

	if len(td) == 0 {
		return &zeroNode{}, nil
	}
	return &dropViewNode{n: n, td: td}, nil
}

func descInSlice(descID sqlbase.ID, td []*sqlbase.TableDescriptor) bool {
	for _, desc := range td {
		if descID == desc.ID {
			return true
		}
	}
	return false
}

func (n *dropViewNode) Start(params runParams) error {
	ctx := params.ctx
	for _, droppedDesc := range n.td {
		if droppedDesc == nil {
			continue
		}
		cascadeDroppedViews, err := params.p.dropViewImpl(ctx, droppedDesc, n.n.DropBehavior)
		if err != nil {
			return err
		}
		// Log a Drop View event for this table. This is an auditable log event
		// and is recorded in the same transaction as the table descriptor
		// update.
		if err := MakeEventLogger(params.p.LeaseMgr()).InsertEventRecord(
			ctx,
			params.p.txn,
			EventLogDropView,
			int32(droppedDesc.ID),
			int32(params.p.evalCtx.NodeID),
			struct {
				ViewName            string
				Statement           string
				User                string
				CascadeDroppedViews []string
			}{droppedDesc.Name, n.n.String(), params.p.session.User, cascadeDroppedViews},
		); err != nil {
			return err
		}
	}
	return nil
}

func (*dropViewNode) Next(runParams) (bool, error) { return false, nil }
func (*dropViewNode) Close(context.Context)        {}
func (*dropViewNode) Values() parser.Datums        { return parser.Datums{} }

type dropTableNode struct {
	n  *parser.DropTable
	td []*sqlbase.TableDescriptor
}

// DropTable drops a table.
// Privileges: DROP on table.
//   Notes: postgres allows only the table owner to DROP a table.
//          mysql requires the DROP privilege on the table.
func (p *planner) DropTable(ctx context.Context, n *parser.DropTable) (planNode, error) {
	td := make([]*sqlbase.TableDescriptor, 0, len(n.Names))
	for _, name := range n.Names {
		tn, err := name.NormalizeTableName()
		if err != nil {
			return nil, err
		}
		if err := tn.QualifyWithDatabase(p.session.Database); err != nil {
			return nil, err
		}

		droppedDesc, err := p.dropTableOrViewPrepare(ctx, tn)
		if err != nil {
			return nil, err
		}
		if droppedDesc == nil {
			if n.IfExists {
				continue
			}
			// Table does not exist, but we want it to: error out.
			return nil, sqlbase.NewUndefinedRelationError(tn)
		}
		if !droppedDesc.IsTable() {
			return nil, sqlbase.NewWrongObjectTypeError(tn, "table")
		}
		td = append(td, droppedDesc)
	}

	dropping := make(map[sqlbase.ID]bool)
	for _, d := range td {
		dropping[d.ID] = true
	}

	for _, droppedDesc := range td {
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
		return &zeroNode{}, nil
	}
	return &dropTableNode{n: n, td: td}, nil
}

func (p *planner) canRemoveFK(
	ctx context.Context, from string, ref sqlbase.ForeignKeyReference, behavior parser.DropBehavior,
) (*sqlbase.TableDescriptor, error) {
	table, err := sqlbase.GetTableDescFromID(ctx, p.txn, ref.Table)
	if err != nil {
		return nil, err
	}
	if behavior != parser.DropCascade {
		return nil, fmt.Errorf("%q is referenced by foreign key from table %q", from, table.Name)
	}
	if err := p.CheckPrivilege(table, privilege.CREATE); err != nil {
		return nil, err
	}
	return table, nil
}

func (p *planner) canRemoveInterleave(
	ctx context.Context, from string, ref sqlbase.ForeignKeyReference, behavior parser.DropBehavior,
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
	if behavior != parser.DropCascade {
		return pgerror.UnimplementedWithIssueErrorf(
			8036, "%q is interleaved by table %q", from, table.Name)
	}
	if err := p.CheckPrivilege(table, privilege.CREATE); err != nil {
		return err
	}
	return nil
}

func (p *planner) canRemoveDependentView(
	ctx context.Context,
	from *sqlbase.TableDescriptor,
	ref sqlbase.TableDescriptor_Reference,
	behavior parser.DropBehavior,
) error {
	return p.canRemoveDependentViewGeneric(ctx, from.TypeName(), from.Name, from.ParentID, ref, behavior)
}

func (p *planner) canRemoveDependentViewGeneric(
	ctx context.Context,
	typeName string,
	objName string,
	parentID sqlbase.ID,
	ref sqlbase.TableDescriptor_Reference,
	behavior parser.DropBehavior,
) error {
	viewDesc, err := p.getViewDescForCascade(ctx, typeName, objName, parentID, ref.ID, behavior)
	if err != nil {
		return err
	}
	if err := p.CheckPrivilege(viewDesc, privilege.DROP); err != nil {
		return err
	}
	// If this view is depended on by other views, we have to check them as well.
	for _, ref := range viewDesc.DependedOnBy {
		if err := p.canRemoveDependentView(ctx, viewDesc, ref, behavior); err != nil {
			return err
		}
	}
	return nil
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

// Drops the view and any additional views that depend on it.
// Returns the names of any additional views that were also dropped
// due to `cascade` behavior.
func (p *planner) removeDependentView(
	ctx context.Context, tableDesc, viewDesc *sqlbase.TableDescriptor,
) ([]string, error) {
	// In the table whose index is being removed, filter out all back-references
	// that refer to the view that's being removed.
	tableDesc.DependedOnBy = removeMatchingReferences(tableDesc.DependedOnBy, viewDesc.ID)
	// Then proceed to actually drop the view and log an event for it.
	return p.dropViewImpl(ctx, viewDesc, parser.DropCascade)
}

func (n *dropTableNode) Start(params runParams) error {
	ctx := params.ctx
	for _, droppedDesc := range n.td {
		if droppedDesc == nil {
			continue
		}
		droppedViews, err := params.p.dropTableImpl(ctx, droppedDesc)
		if err != nil {
			return err
		}
		// Log a Drop Table event for this table. This is an auditable log event
		// and is recorded in the same transaction as the table descriptor
		// update.
		if err := MakeEventLogger(params.p.LeaseMgr()).InsertEventRecord(
			ctx,
			params.p.txn,
			EventLogDropTable,
			int32(droppedDesc.ID),
			int32(params.p.evalCtx.NodeID),
			struct {
				TableName           string
				Statement           string
				User                string
				CascadeDroppedViews []string
			}{droppedDesc.Name, n.n.String(), params.p.session.User, droppedViews},
		); err != nil {
			return err
		}
	}
	return nil
}

func (*dropTableNode) Next(runParams) (bool, error) { return false, nil }
func (*dropTableNode) Close(context.Context)        {}
func (*dropTableNode) Values() parser.Datums        { return parser.Datums{} }

// dropTableOrViewPrepare/dropTableImpl is used to drop a single table by
// name, which can result from either a DROP TABLE or DROP DATABASE
// statement. This method returns the dropped table descriptor, to be
// used for the purpose of logging the event.  The table is not
// actually truncated or deleted synchronously. Instead, it is marked
// as deleted (meaning up_version is set and deleted is set) and the
// actual deletion happens async in a schema changer. Note that,
// courtesy of up_version, the actual truncation and dropping will
// only happen once every node ACKs the version of the descriptor with
// the deleted bit set, meaning the lease manager will not hand out
// new leases for it and existing leases are released).
// If the table does not exist, this function returns a nil descriptor.
func (p *planner) dropTableOrViewPrepare(
	ctx context.Context, name *parser.TableName,
) (*sqlbase.TableDescriptor, error) {
	tableDesc, err := getTableOrViewDesc(ctx, p.txn, p.getVirtualTabler(), name)
	if err != nil {
		return nil, err
	}
	if tableDesc == nil {
		return nil, err
	}

	if err := p.CheckPrivilege(tableDesc, privilege.DROP); err != nil {
		return nil, err
	}
	return tableDesc, nil
}

// dropTableImpl does the work of dropping a table (and everything that depends
// on it if `cascade` is enabled). It returns a list of view names that were
// dropped due to `cascade` behavior.
func (p *planner) dropTableImpl(
	ctx context.Context, tableDesc *sqlbase.TableDescriptor,
) ([]string, error) {
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

	// Drop all views that depend on this table, assuming that we wouldn't have
	// made it to this point if `cascade` wasn't enabled.
	for _, ref := range tableDesc.DependedOnBy {
		viewDesc, err := p.getViewDescForCascade(
			ctx, tableDesc.TypeName(), tableDesc.Name, tableDesc.ParentID, ref.ID, parser.DropCascade,
		)
		if err != nil {
			return droppedViews, err
		}
		// This view is already getting dropped. Don't do it twice.
		if viewDesc.Dropped() {
			continue
		}
		cascadedViews, err := p.dropViewImpl(ctx, viewDesc, parser.DropCascade)
		if err != nil {
			return droppedViews, err
		}
		droppedViews = append(droppedViews, cascadedViews...)
		droppedViews = append(droppedViews, viewDesc.Name)
	}

	if err := p.initiateDropTable(ctx, tableDesc); err != nil {
		return droppedViews, err
	}

	p.session.setTestingVerifyMetadata(func(systemConfig config.SystemConfig) error {
		return verifyDropTableMetadata(systemConfig, tableDesc.ID, "table")
	})
	return droppedViews, nil
}

func (p *planner) initiateDropTable(ctx context.Context, tableDesc *sqlbase.TableDescriptor) error {
	if err := tableDesc.SetUpVersion(); err != nil {
		return err
	}
	tableDesc.State = sqlbase.TableDescriptor_DROP
	if err := p.writeTableDesc(ctx, tableDesc); err != nil {
		return err
	}
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

func verifyDropTableMetadata(
	systemConfig config.SystemConfig, tableID sqlbase.ID, objType string,
) error {
	desc, err := GetTableDesc(systemConfig, tableID)
	if err != nil {
		return err
	}
	if desc == nil {
		return errors.Errorf("%s %d missing", objType, tableID)
	}
	if desc.Dropped() {
		return nil
	}
	return errors.Errorf("expected %s %d to be marked as deleted", objType, tableID)
}

// dropViewImpl does the work of dropping a view (and views that depend on it
// if `cascade is specified`). Returns the names of any additional views that
// were also dropped due to `cascade` behavior.
func (p *planner) dropViewImpl(
	ctx context.Context, viewDesc *sqlbase.TableDescriptor, behavior parser.DropBehavior,
) ([]string, error) {
	var cascadeDroppedViews []string

	// Remove back-references from the tables/views this view depends on.
	for _, depID := range viewDesc.DependsOn {
		dependencyDesc, err := sqlbase.GetTableDescFromID(ctx, p.txn, depID)
		if err != nil {
			return cascadeDroppedViews,
				errors.Errorf("error resolving dependency relation ID %d: %v", depID, err)
		}
		// The dependency is also being deleted, so we don't have to remove the
		// references.
		if dependencyDesc.Dropped() {
			continue
		}
		dependencyDesc.DependedOnBy = removeMatchingReferences(dependencyDesc.DependedOnBy, viewDesc.ID)
		if err := p.saveNonmutationAndNotify(ctx, dependencyDesc); err != nil {
			return cascadeDroppedViews, err
		}
	}
	viewDesc.DependsOn = nil

	if behavior == parser.DropCascade {
		for _, ref := range viewDesc.DependedOnBy {
			dependentDesc, err := p.getViewDescForCascade(
				ctx, viewDesc.TypeName(), viewDesc.Name, viewDesc.ParentID, ref.ID, behavior,
			)
			if err != nil {
				return cascadeDroppedViews, err
			}
			cascadedViews, err := p.dropViewImpl(ctx, dependentDesc, behavior)
			if err != nil {
				return cascadeDroppedViews, err
			}
			cascadeDroppedViews = append(cascadeDroppedViews, cascadedViews...)
			cascadeDroppedViews = append(cascadeDroppedViews, dependentDesc.Name)
		}
	}

	if err := p.initiateDropTable(ctx, viewDesc); err != nil {
		return cascadeDroppedViews, err
	}

	p.session.setTestingVerifyMetadata(func(systemConfig config.SystemConfig) error {
		return verifyDropTableMetadata(systemConfig, viewDesc.ID, "view")
	})
	return cascadeDroppedViews, nil
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

func (p *planner) getViewDescForCascade(
	ctx context.Context,
	typeName string,
	objName string,
	parentID, viewID sqlbase.ID,
	behavior parser.DropBehavior,
) (*sqlbase.TableDescriptor, error) {
	viewDesc, err := sqlbase.GetTableDescFromID(ctx, p.txn, viewID)
	if err != nil {
		log.Warningf(ctx, "unable to retrieve descriptor for view %d: %v", viewID, err)
		return nil, errors.Wrapf(err, "error resolving dependent view ID %d", viewID)
	}
	if behavior != parser.DropCascade {
		viewName := viewDesc.Name
		if viewDesc.ParentID != parentID {
			var err error
			viewName, err = p.getQualifiedTableName(ctx, viewDesc)
			if err != nil {
				log.Warningf(ctx, "unable to retrieve qualified name of view %d: %v", viewID, err)
				msg := fmt.Sprintf("cannot drop %s %q because a view depends on it", typeName, objName)
				return nil, sqlbase.NewDependentObjectError(msg)
			}
		}
		msg := fmt.Sprintf("cannot drop %s %q because view %q depends on it",
			typeName, objName, viewName)
		hint := fmt.Sprintf("you can drop %s instead.", viewName)
		return nil, sqlbase.NewDependentObjectErrorWithHint(msg, hint)
	}
	return viewDesc, nil
}

type dropUserNode struct {
	n *parser.DropUser
	// The number of users deleted.
	numDeleted int
}

func (n *dropUserNode) Start(params runParams) error {
	numDeleted := 0
	for _, name := range n.n.Names {
		normalizedUsername, err := NormalizeAndValidateUsername(string(name))
		if err != nil {
			return err
		}

		// Note: protected users like security.RootUser are not included in system.users,
		// so there is no need to filter them out.

		// TODO: Remove the privileges granted to the user.
		// Note: The current remove user from CLI just deletes the entry from system.users,
		// keeping the functionality same for now.
		internalExecutor := InternalExecutor{LeaseManager: params.p.LeaseMgr()}
		rowsAffected, err := internalExecutor.ExecuteStatementInTransaction(
			params.ctx,
			"drop-user",
			params.p.txn,
			"DELETE FROM system.users WHERE username=$1",
			normalizedUsername,
		)
		if err != nil {
			return err
		}

		if rowsAffected == 0 && !n.n.IfExists {
			return errors.Errorf("user %s does not exist", normalizedUsername)
		}

		numDeleted += rowsAffected
	}

	n.numDeleted = numDeleted

	return nil
}

func (*dropUserNode) Next(runParams) (bool, error)   { return false, nil }
func (*dropUserNode) Close(context.Context)          {}
func (*dropUserNode) Values() parser.Datums          { return parser.Datums{} }
func (n *dropUserNode) FastPathResults() (int, bool) { return n.numDeleted, true }

// DropUser drops a list of users.
// Privileges: DELETE on system.users.
func (p *planner) DropUser(ctx context.Context, n *parser.DropUser) (planNode, error) {
	tDesc, err := getTableDesc(ctx, p.txn, p.getVirtualTabler(), &parser.TableName{DatabaseName: "system", TableName: "users"})
	if err != nil {
		return nil, err
	}

	if err := p.CheckPrivilege(tDesc, privilege.DELETE); err != nil {
		return nil, err
	}

	return &dropUserNode{n: n}, nil
}
