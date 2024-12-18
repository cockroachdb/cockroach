// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/descmetadata"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/errors"
)

type dropDatabaseNode struct {
	zeroInputPlanNode
	n      *tree.DropDatabase
	dbDesc *dbdesc.Mutable
	d      *dropCascadeState
}

// DropDatabase drops a database.
// Privileges: DROP on database and DROP on all tables in the database.
//
//	Notes: postgres allows only the database owner to DROP a database.
//	       mysql requires the DROP privileges on the database.
func (p *planner) DropDatabase(ctx context.Context, n *tree.DropDatabase) (planNode, error) {
	if err := checkSchemaChangeEnabled(
		ctx,
		p.ExecCfg(),
		"DROP DATABASE",
	); err != nil {
		return nil, err
	}

	if n.Name == "" {
		return nil, sqlerrors.ErrEmptyDatabaseName
	}

	if string(n.Name) == p.SessionData().Database && p.SessionData().SafeUpdates {
		return nil, pgerror.DangerousStatementf("DROP DATABASE on current database")
	}

	// Check that the database exists.
	if db, err := p.Descriptors().ByName(p.txn).MaybeGet().Database(ctx, string(n.Name)); err != nil {
		return nil, err
	} else if db == nil && n.IfExists {
		return newZeroNode(nil /* columns */), nil
	}
	dbDesc, err := p.Descriptors().MutableByName(p.txn).Database(ctx, string(n.Name))
	if err != nil {
		return nil, err
	}

	if err := p.CheckPrivilege(ctx, dbDesc, privilege.DROP); err != nil {
		return nil, err
	}

	schemas, err := p.Descriptors().GetSchemasForDatabase(ctx, p.txn, dbDesc)
	if err != nil {
		return nil, err
	}

	d := newDropCascadeState()

	for _, schema := range schemas {
		res, err := p.Descriptors().ByName(p.txn).Get().Schema(ctx, dbDesc, schema)
		if err != nil {
			return nil, err
		}
		if err := d.collectObjectsInSchema(ctx, p, dbDesc, res); err != nil {
			return nil, err
		}
	}

	if len(d.objectNamesToDelete) > 0 || len(d.functionsToDelete) > 0 {
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

	if err := d.resolveCollectedObjects(ctx, true /*dropDatabase*/, p); err != nil {
		return nil, err
	}

	return &dropDatabaseNode{
		n:      n,
		dbDesc: dbDesc,
		d:      d,
	}, nil
}

func (n *dropDatabaseNode) startExec(params runParams) error {
	telemetry.Inc(sqltelemetry.SchemaChangeDropCounter("database"))

	ctx := params.ctx
	p := params.p

	// Exit early with an error if the schema is undergoing a declarative schema
	// change.
	if catalog.HasConcurrentDeclarativeSchemaChange(n.dbDesc) {
		return scerrors.ConcurrentSchemaChangeError(n.dbDesc)
	}

	// Drop all of the collected objects.
	if err := n.d.dropAllCollectedObjects(ctx, p); err != nil {
		return err
	}

	var schemasIDsToDelete []descpb.ID
	for _, schemaWithDbDesc := range n.d.schemasToDelete {
		schemaToDelete := schemaWithDbDesc.schema
		switch schemaToDelete.SchemaKind() {
		case catalog.SchemaPublic:
			b := p.Txn().NewBatch()
			if err := p.Descriptors().DeleteDescriptorlessPublicSchemaToBatch(
				ctx, p.ExtendedEvalContext().Tracing.KVTracingEnabled(), n.dbDesc, b,
			); err != nil {
				return err
			}
			if err := p.txn.Run(ctx, b); err != nil {
				return err
			}
		case catalog.SchemaTemporary:
			b := p.Txn().NewBatch()
			if err := p.Descriptors().DeleteTempSchemaToBatch(
				ctx, p.ExtendedEvalContext().Tracing.KVTracingEnabled(), n.dbDesc, schemaToDelete.GetName(), b,
			); err != nil {
				return err
			}
			if err := p.txn.Run(ctx, b); err != nil {
				return err
			}
		case catalog.SchemaUserDefined:
			// For user defined schemas, we have to do a bit more work.
			mutDesc, err := p.Descriptors().MutableByID(p.txn).Schema(ctx, schemaToDelete.GetID())
			if err != nil {
				return err
			}
			if err := params.p.dropSchemaImpl(ctx, n.dbDesc, mutDesc); err != nil {
				return err
			}
			schemasIDsToDelete = append(schemasIDsToDelete, schemaToDelete.GetID())
		}
	}

	if err := p.createDropDatabaseJob(
		ctx,
		n.dbDesc.GetID(),
		schemasIDsToDelete,
		n.d.getDroppedTableDetails(),
		n.d.typesToDelete,
		n.d.functionsToDelete,
		tree.AsStringWithFQNames(n.n, params.Ann()),
	); err != nil {
		return err
	}

	n.dbDesc.SetDropped()
	b := p.txn.NewBatch()
	if err := p.dropNamespaceEntry(ctx, b, n.dbDesc); err != nil {
		return err
	}

	// Note that a job was already queued above.
	if err := p.writeDatabaseChangeToBatch(ctx, n.dbDesc, b); err != nil {
		return err
	}
	if err := p.txn.Run(ctx, b); err != nil {
		return err
	}

	metadataUpdater := descmetadata.NewMetadataUpdater(
		ctx,
		p.InternalSQLTxn(),
		p.Descriptors(),
		&p.ExecCfg().Settings.SV,
		p.SessionData(),
	)

	err := metadataUpdater.DeleteDatabaseRoleSettings(ctx, n.dbDesc.GetID())
	if err != nil {
		return err
	}

	if err := p.deleteComment(
		ctx, n.dbDesc.GetID(), 0 /* subID */, catalogkeys.DatabaseCommentType,
	); err != nil {
		return err
	}

	// Log Drop Database event. This is an auditable log event and is recorded
	// in the same transaction as the table descriptor update.
	return p.logEvent(ctx,
		n.dbDesc.GetID(),
		&eventpb.DropDatabase{
			DatabaseName:         n.n.Name.String(),
			DroppedSchemaObjects: n.d.droppedNames,
		})
}

func (*dropDatabaseNode) Next(runParams) (bool, error) { return false, nil }
func (*dropDatabaseNode) Close(context.Context)        {}
func (*dropDatabaseNode) Values() tree.Datums          { return tree.Datums{} }

// filterImplicitlyDeletedObjects takes a list of table descriptors and removes
// any descriptor that will be implicitly deleted.
func filterImplicitlyDeletedObjects(
	tables []toDelete, implicitDeleteObjects map[descpb.ID]*tabledesc.Mutable,
) []toDelete {
	filteredDeleteList := make([]toDelete, 0, len(tables))
	for _, toDel := range tables {
		if _, found := implicitDeleteObjects[toDel.desc.ID]; !found {
			filteredDeleteList = append(filteredDeleteList, toDel)
		}
	}
	return filteredDeleteList
}

// accumulateAllObjectsToDelete constructs a list of all the descriptors that
// will be deleted as a side effect of deleting the given objects. Additional
// objects may be deleted because of cascading views or sequence ownership. We
// also return a map of objects that will be "implicitly" deleted so we can
// filter on it later.
func (p *planner) accumulateAllObjectsToDelete(
	ctx context.Context, objects []toDelete,
) ([]*tabledesc.Mutable, map[descpb.ID]*tabledesc.Mutable, error) {
	implicitDeleteObjects := make(map[descpb.ID]*tabledesc.Mutable)
	visited := make(map[descpb.ID]struct{})
	for _, toDel := range objects {
		err := p.accumulateCascadingViews(ctx, implicitDeleteObjects, visited, toDel.desc)
		if err != nil {
			return nil, nil, err
		}
		// Sequences owned by the table will also be implicitly deleted.
		if toDel.desc.IsTable() {
			err := p.accumulateOwnedSequences(ctx, implicitDeleteObjects, toDel.desc)
			if err != nil {
				return nil, nil, err
			}
		}
	}
	allObjectsToDelete := make([]*tabledesc.Mutable, 0,
		len(objects)+len(implicitDeleteObjects))
	for _, desc := range implicitDeleteObjects {
		allObjectsToDelete = append(allObjectsToDelete, desc)
	}
	for _, toDel := range objects {
		if _, found := implicitDeleteObjects[toDel.desc.ID]; !found {
			allObjectsToDelete = append(allObjectsToDelete, toDel.desc)
		}
	}
	return allObjectsToDelete, implicitDeleteObjects, nil
}

// accumulateOwnedSequences finds all sequences that will be dropped as a result
// of the table referenced by desc being dropped, and adds them to the
// dependentObjects map.
func (p *planner) accumulateOwnedSequences(
	ctx context.Context, dependentObjects map[descpb.ID]*tabledesc.Mutable, desc *tabledesc.Mutable,
) error {
	for colID := range desc.GetColumns() {
		for _, seqID := range desc.GetColumns()[colID].OwnsSequenceIds {
			ownedSeqDesc, err := p.Descriptors().MutableByID(p.txn).Table(ctx, seqID)
			if err != nil {
				// Special case error swallowing for #50711 and #50781, which can
				// cause columns to own sequences that have been dropped/do not
				// exist.
				if errors.Is(err, catalog.ErrDescriptorDropped) ||
					pgerror.GetPGCode(err) == pgcode.UndefinedTable {
					log.Infof(ctx,
						"swallowing error for owned sequence that was not found %s", err.Error())
					continue
				}
				return err
			}
			dependentObjects[seqID] = ownedSeqDesc
		}
	}
	return nil
}

// accumulateCascadingViews finds all views that are to be deleted as part
// of a drop database cascade. This is important as CRDB allows cross-database
// references, which means this list can't be constructed by simply scanning
// the namespace table.
func (p *planner) accumulateCascadingViews(
	ctx context.Context,
	dependentObjects map[descpb.ID]*tabledesc.Mutable,
	visited map[descpb.ID]struct{},
	desc *tabledesc.Mutable,
) error {
	visited[desc.ID] = struct{}{}
	for _, ref := range desc.DependedOnBy {
		desc, err := p.Descriptors().MutableByID(p.txn).Desc(ctx, ref.ID)
		if err != nil {
			return err
		}

		dependentDesc, ok := desc.(*tabledesc.Mutable)
		if !ok {
			continue
		}

		if !dependentDesc.IsView() {
			continue
		}

		_, seen := visited[ref.ID]
		if dependentObjects[ref.ID] == dependentDesc || seen {
			// This view's dependencies are already added.
			continue
		}
		dependentObjects[ref.ID] = dependentDesc

		if err := p.accumulateCascadingViews(ctx, dependentObjects, visited, dependentDesc); err != nil {
			return err
		}
	}
	return nil
}
