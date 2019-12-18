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

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

type dropDatabaseNode struct {
	n      *tree.DropDatabase
	dbDesc *sqlbase.DatabaseDescriptor
	td     []toDelete
}

// DropDatabase drops a database.
// Privileges: DROP on database and DROP on all tables in the database.
//   Notes: postgres allows only the database owner to DROP a database.
//          mysql requires the DROP privileges on the database.
// TODO(XisiHuang): our DROP DATABASE is like the postgres DROP SCHEMA
// (cockroach database == postgres schema). the postgres default of not
// dropping the schema if there are dependent objects is more sensible
// (see the RESTRICT and CASCADE options).
func (p *planner) DropDatabase(ctx context.Context, n *tree.DropDatabase) (planNode, error) {
	if n.Name == "" {
		return nil, errEmptyDatabaseName
	}

	if string(n.Name) == p.SessionData().Database && p.SessionData().SafeUpdates {
		return nil, pgerror.DangerousStatementf("DROP DATABASE on current database")
	}

	// Check that the database exists.
	dbDesc, err := p.ResolveUncachedDatabaseByName(ctx, string(n.Name), !n.IfExists)
	if err != nil {
		return nil, err
	}
	if dbDesc == nil {
		// IfExists was specified and database was not found.
		return newZeroNode(nil /* columns */), nil
	}

	if err := p.CheckPrivilege(ctx, dbDesc, privilege.DROP); err != nil {
		return nil, err
	}

	tbNames, err := GetObjectNames(
		ctx, p.txn, p, dbDesc, tree.PublicSchema, true, /*explicitPrefix*/
	)
	if err != nil {
		return nil, err
	}

	if len(tbNames) > 0 {
		switch n.DropBehavior {
		case tree.DropRestrict:
			return nil, pgerror.Newf(pgcode.DependentObjectsStillExist,
				"database %q is not empty and RESTRICT was specified",
				tree.ErrNameStringP(&dbDesc.Name))
		case tree.DropDefault:
			// The default is CASCADE, however be cautious if CASCADE was
			// not specified explicitly.
			if p.SessionData().SafeUpdates {
				return nil, pgerror.DangerousStatementf(
					"DROP DATABASE on non-empty database without explicit CASCADE")
			}
		}
	}

	td := make([]toDelete, 0, len(tbNames))
	for i := range tbNames {
		tbDesc, err := p.prepareDrop(ctx, &tbNames[i], false /*required*/, ResolveAnyDescType)
		if err != nil {
			return nil, err
		}
		if tbDesc == nil {
			continue
		}
		// Recursively check permissions on all dependent views, since some may
		// be in different databases.
		for _, ref := range tbDesc.DependedOnBy {
			if err := p.canRemoveDependentView(ctx, tbDesc, ref, tree.DropCascade); err != nil {
				return nil, err
			}
		}
		td = append(td, toDelete{&tbNames[i], tbDesc})
	}

	td, err = p.filterCascadedTables(ctx, td)
	if err != nil {
		return nil, err
	}
	return &dropDatabaseNode{n: n, dbDesc: dbDesc, td: td}, nil
}

func (n *dropDatabaseNode) startExec(params runParams) error {
	ctx := params.ctx
	p := params.p
	tbNameStrings := make([]string, 0, len(n.td))
	droppedTableDetails := make([]jobspb.DroppedTableDetails, 0, len(n.td))
	tableDescs := make([]*sqlbase.MutableTableDescriptor, 0, len(n.td))

	for _, toDel := range n.td {
		if toDel.desc.IsView() {
			continue
		}
		droppedTableDetails = append(droppedTableDetails, jobspb.DroppedTableDetails{
			Name: toDel.tn.FQString(),
			ID:   toDel.desc.ID,
		})
		tableDescs = append(tableDescs, toDel.desc)
	}

	jobID, err := p.createDropTablesJob(
		ctx,
		tableDescs,
		droppedTableDetails,
		tree.AsStringWithFQNames(n.n, params.Ann()),
		true, /* drainNames */
		n.dbDesc.ID)
	if err != nil {
		return err
	}

	for _, toDel := range n.td {
		cascadedObjects, err := p.dropObject(ctx, toDel.desc, tree.DropCascade)
		if err != nil {
			return err
		}
		tbNameStrings = append(tbNameStrings, cascadedObjects...)
		tbNameStrings = append(tbNameStrings, toDel.tn.FQString())
	}

	descKey := sqlbase.MakeDescMetadataKey(n.dbDesc.ID)

	b := &client.Batch{}
	if p.ExtendedEvalContext().Tracing.KVTracingEnabled() {
		log.VEventf(ctx, 2, "Del %s", descKey)
	}
	b.Del(descKey)

	err = sqlbase.RemoveDatabaseNamespaceEntry(
		ctx, p.txn, n.dbDesc.Name, p.ExtendedEvalContext().Tracing.KVTracingEnabled(),
	)
	if err != nil {
		return err
	}

	// No job was created because no tables were dropped, so zone config can be
	// immediately removed.
	if jobID == 0 {
		zoneKeyPrefix := config.MakeZoneKeyPrefix(uint32(n.dbDesc.ID))
		if p.ExtendedEvalContext().Tracing.KVTracingEnabled() {
			log.VEventf(ctx, 2, "DelRange %s", zoneKeyPrefix)
		}
		// Delete the zone config entry for this database.
		b.DelRange(zoneKeyPrefix, zoneKeyPrefix.PrefixEnd(), false /* returnKeys */)
	}

	p.Tables().addUncommittedDatabase(n.dbDesc.Name, n.dbDesc.ID, dbDropped)

	if err := p.txn.Run(ctx, b); err != nil {
		return err
	}

	if err := p.removeDbComment(ctx, n.dbDesc.ID); err != nil {
		return err
	}

	// Log Drop Database event. This is an auditable log event and is recorded
	// in the same transaction as the table descriptor update.
	return MakeEventLogger(params.extendedEvalCtx.ExecCfg).InsertEventRecord(
		ctx,
		p.txn,
		EventLogDropDatabase,
		int32(n.dbDesc.ID),
		int32(params.extendedEvalCtx.NodeID),
		struct {
			DatabaseName         string
			Statement            string
			User                 string
			DroppedSchemaObjects []string
		}{n.n.Name.String(), n.n.String(), p.SessionData().User, tbNameStrings},
	)
}

func (*dropDatabaseNode) Next(runParams) (bool, error) { return false, nil }
func (*dropDatabaseNode) Close(context.Context)        {}
func (*dropDatabaseNode) Values() tree.Datums          { return tree.Datums{} }

// filterCascadedTables takes a list of table descriptors and removes any
// descriptors from the list that are dependent on other descriptors in the
// list (e.g. if view v1 depends on table t1, then v1 will be filtered from
// the list).
func (p *planner) filterCascadedTables(ctx context.Context, tables []toDelete) ([]toDelete, error) {
	// Accumulate the set of all tables/views that will be deleted by cascade
	// behavior so that we can filter them out of the list.
	cascadedTables := make(map[sqlbase.ID]bool)
	for _, toDel := range tables {
		desc := toDel.desc
		if err := p.accumulateDependentTables(ctx, cascadedTables, desc); err != nil {
			return nil, err
		}
	}
	filteredTableList := make([]toDelete, 0, len(tables))
	for _, toDel := range tables {
		if !cascadedTables[toDel.desc.ID] {
			filteredTableList = append(filteredTableList, toDel)
		}
	}
	return filteredTableList, nil
}

func (p *planner) accumulateDependentTables(
	ctx context.Context, dependentTables map[sqlbase.ID]bool, desc *sqlbase.MutableTableDescriptor,
) error {
	for _, ref := range desc.DependedOnBy {
		dependentTables[ref.ID] = true
		dependentDesc, err := p.Tables().getMutableTableVersionByID(ctx, ref.ID, p.txn)
		if err != nil {
			return err
		}
		if err := p.accumulateDependentTables(ctx, dependentTables, dependentDesc); err != nil {
			return err
		}
	}
	return nil
}

func (p *planner) removeDbComment(ctx context.Context, dbID sqlbase.ID) error {
	_, err := p.ExtendedEvalContext().ExecCfg.InternalExecutor.Exec(
		ctx,
		"delete-db-comment",
		p.txn,
		"DELETE FROM system.comments WHERE type=$1 AND object_id=$2 AND sub_id=0",
		keys.DatabaseCommentType,
		dbID)

	return err
}
