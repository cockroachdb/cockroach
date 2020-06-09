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
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/resolver"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

type dropDatabaseNode struct {
	n               *tree.DropDatabase
	dbDesc          *sqlbase.ImmutableDatabaseDescriptor
	td              []toDelete
	schemasToDelete []string
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

	schemas, err := p.Tables().GetSchemasForDatabase(ctx, p.txn, dbDesc.GetID())
	if err != nil {
		return nil, err
	}

	var tbNames TableNames
	schemasToDelete := make([]string, 0, len(schemas))
	for _, schema := range schemas {
		schemasToDelete = append(schemasToDelete, schema)
		toAppend, err := resolver.GetObjectNames(
			ctx, p.txn, p, p.ExecCfg().Codec, dbDesc, schema, true, /*explicitPrefix*/
		)
		if err != nil {
			return nil, err
		}
		tbNames = append(tbNames, toAppend...)
	}

	if len(tbNames) > 0 {
		switch n.DropBehavior {
		case tree.DropRestrict:
			return nil, pgerror.Newf(pgcode.DependentObjectsStillExist,
				"database %q is not empty and RESTRICT was specified",
				tree.ErrNameString(dbDesc.GetName()))
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
	for i, tbName := range tbNames {
		found, desc, err := p.LookupObject(
			ctx,
			tree.ObjectLookupFlags{
				CommonLookupFlags: tree.CommonLookupFlags{Required: true},
				RequireMutable:    true,
				IncludeOffline:    true,
			},
			tbName.Catalog(),
			tbName.Schema(),
			tbName.Table(),
		)
		if err != nil {
			return nil, err
		}
		if !found {
			continue
		}
		tbDesc, ok := desc.(*sqlbase.MutableTableDescriptor)
		if !ok {
			return nil, errors.AssertionFailedf(
				"descriptor for %q is not MutableTableDescriptor",
				tbName.String(),
			)
		}
		if tbDesc.State == sqlbase.TableDescriptor_OFFLINE {
			dbName := dbDesc.GetName()
			return nil, pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
				"cannot drop a database with OFFLINE tables, ensure %s is"+
					" dropped or made public before dropping database %s",
				tbName.String(), tree.AsString((*tree.Name)(&dbName)))
		}
		if err := p.prepareDropWithTableDesc(ctx, tbDesc); err != nil {
			return nil, err
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

	return &dropDatabaseNode{n: n, dbDesc: dbDesc, td: td, schemasToDelete: schemasToDelete}, nil
}

func (n *dropDatabaseNode) startExec(params runParams) error {
	telemetry.Inc(sqltelemetry.SchemaChangeDropCounter("database"))

	ctx := params.ctx
	p := params.p
	tbNameStrings := make([]string, 0, len(n.td))
	droppedTableDetails := make([]jobspb.DroppedTableDetails, 0, len(n.td))
	tableDescs := make([]*sqlbase.MutableTableDescriptor, 0, len(n.td))

	for _, toDel := range n.td {
		droppedTableDetails = append(droppedTableDetails, jobspb.DroppedTableDetails{
			Name: toDel.tn.FQString(),
			ID:   toDel.desc.ID,
		})
		tableDescs = append(tableDescs, toDel.desc)
	}
	if err := p.createDropDatabaseJob(
		ctx, n.dbDesc.GetID(), droppedTableDetails, tree.AsStringWithFQNames(n.n, params.Ann()),
	); err != nil {
		return err
	}

	// When views, sequences, and tables are dropped, don't queue a separate job
	// for each of them, since the single DROP DATABASE job will cover them all.
	for _, toDel := range n.td {
		desc := toDel.desc
		var cascadedObjects []string
		var err error
		if desc.IsView() {
			// TODO(knz): dependent dropped views should be qualified here.
			cascadedObjects, err = p.dropViewImpl(ctx, desc, false /* queueJob */, "", tree.DropCascade)
		} else if desc.IsSequence() {
			err = p.dropSequenceImpl(ctx, desc, false /* queueJob */, "", tree.DropCascade)
		} else {
			// TODO(knz): dependent dropped table names should be qualified here.
			cascadedObjects, err = p.dropTableImpl(ctx, desc, false /* queueJob */, "")
		}
		if err != nil {
			return err
		}
		tbNameStrings = append(tbNameStrings, cascadedObjects...)
		tbNameStrings = append(tbNameStrings, toDel.tn.FQString())
	}

	descKey := sqlbase.MakeDescMetadataKey(p.ExecCfg().Codec, n.dbDesc.GetID())

	b := &kv.Batch{}
	if p.ExtendedEvalContext().Tracing.KVTracingEnabled() {
		log.VEventf(ctx, 2, "Del %s", descKey)
	}
	b.Del(descKey)

	for _, schemaToDelete := range n.schemasToDelete {
		if err := sqlbase.RemoveSchemaNamespaceEntry(
			ctx,
			p.txn,
			p.ExecCfg().Codec,
			n.dbDesc.GetID(),
			schemaToDelete,
		); err != nil {
			return err
		}
	}

	err := sqlbase.RemoveDatabaseNamespaceEntry(
		ctx, p.txn, p.ExecCfg().Codec, n.dbDesc.GetName(), p.ExtendedEvalContext().Tracing.KVTracingEnabled(),
	)
	if err != nil {
		return err
	}

	// No job was created because no tables were dropped, so zone config can be
	// immediately removed, if applicable.
	if len(tableDescs) == 0 && params.ExecCfg().Codec.ForSystemTenant() {
		zoneKeyPrefix := config.MakeZoneKeyPrefix(config.SystemTenantObjectID(n.dbDesc.GetID()))
		if p.ExtendedEvalContext().Tracing.KVTracingEnabled() {
			log.VEventf(ctx, 2, "DelRange %s", zoneKeyPrefix)
		}
		// Delete the zone config entry for this database.
		b.DelRange(zoneKeyPrefix, zoneKeyPrefix.PrefixEnd(), false /* returnKeys */)
	}

	p.Tables().AddUncommittedDatabase(n.dbDesc.GetName(), n.dbDesc.GetID(), descs.DBDropped)

	if err := p.txn.Run(ctx, b); err != nil {
		return err
	}

	if err := p.removeDbComment(ctx, n.dbDesc.GetID()); err != nil {
		return err
	}

	// Log Drop Database event. This is an auditable log event and is recorded
	// in the same transaction as the table descriptor update.
	return MakeEventLogger(params.extendedEvalCtx.ExecCfg).InsertEventRecord(
		ctx,
		p.txn,
		EventLogDropDatabase,
		int32(n.dbDesc.GetID()),
		int32(params.extendedEvalCtx.NodeID.SQLInstanceID()),
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
		dependentDesc, err := p.Tables().GetMutableTableVersionByID(ctx, ref.ID, p.txn)
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
	_, err := p.ExtendedEvalContext().ExecCfg.InternalExecutor.ExecEx(
		ctx,
		"delete-db-comment",
		p.txn,
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		"DELETE FROM system.comments WHERE type=$1 AND object_id=$2 AND sub_id=0",
		keys.DatabaseCommentType,
		dbID)

	return err
}
