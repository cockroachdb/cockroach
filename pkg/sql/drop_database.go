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

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/errors"
)

type dropDatabaseNode struct {
	n      *tree.DropDatabase
	dbDesc *dbdesc.Mutable
	d      *dropCascadeState
}

// DropDatabase drops a database.
// Privileges: DROP on database and DROP on all tables in the database.
//   Notes: postgres allows only the database owner to DROP a database.
//          mysql requires the DROP privileges on the database.
func (p *planner) DropDatabase(ctx context.Context, n *tree.DropDatabase) (planNode, error) {
	if err := checkSchemaChangeEnabled(
		ctx,
		p.ExecCfg(),
		"DROP DATABASE",
	); err != nil {
		return nil, err
	}

	if n.Name == "" {
		return nil, errEmptyDatabaseName
	}

	if string(n.Name) == p.SessionData().Database && p.SessionData().SafeUpdates {
		return nil, pgerror.DangerousStatementf("DROP DATABASE on current database")
	}

	// Check that the database exists.
	dbDesc, err := p.Descriptors().GetMutableDatabaseByName(ctx, p.txn, string(n.Name),
		tree.DatabaseLookupFlags{Required: !n.IfExists})
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

	schemas, err := p.Descriptors().GetSchemasForDatabase(ctx, p.txn, dbDesc.GetID())
	if err != nil {
		return nil, err
	}

	d := newDropCascadeState()

	for _, schema := range schemas {
		res, err := p.Descriptors().GetSchemaByName(
			ctx, p.txn, dbDesc, schema, tree.SchemaLookupFlags{
				Required:       true,
				RequireMutable: true,
			},
		)
		if err != nil {
			return nil, err
		}
		if err := d.collectObjectsInSchema(ctx, p, dbDesc, res); err != nil {
			return nil, err
		}
	}

	if len(d.objectNamesToDelete) > 0 {
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

	if err := d.resolveCollectedObjects(ctx, p, dbDesc); err != nil {
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

	var schemasIDsToDelete []descpb.ID
	for _, schemaWithDbDesc := range n.d.schemasToDelete {
		schemaToDelete := schemaWithDbDesc.schema
		switch schemaToDelete.SchemaKind() {
		case catalog.SchemaTemporary, catalog.SchemaPublic:
			// The public schema and temporary schemas are cleaned up by just removing
			// the existing namespace entries.
			key := catalogkeys.MakeSchemaNameKey(p.ExecCfg().Codec, n.dbDesc.GetID(), schemaToDelete.GetName())
			if err := p.txn.Del(ctx, key); err != nil {
				return err
			}
		case catalog.SchemaUserDefined:
			// For user defined schemas, we have to do a bit more work.
			mutDesc, ok := schemaToDelete.(*schemadesc.Mutable)
			if !ok {
				return errors.AssertionFailedf("expected Mutable, found %T", schemaToDelete)
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
		tree.AsStringWithFQNames(n.n, params.Ann()),
	); err != nil {
		return err
	}

	// Drop all of the collected objects.
	if err := n.d.dropAllCollectedObjects(ctx, p); err != nil {
		return err
	}

	n.dbDesc.AddDrainingName(descpb.NameInfo{
		ParentID:       keys.RootNamespaceID,
		ParentSchemaID: keys.RootNamespaceID,
		Name:           n.dbDesc.Name,
	})
	n.dbDesc.State = descpb.DescriptorState_DROP

	b := &kv.Batch{}
	// Note that a job was already queued above.
	if err := p.writeDatabaseChangeToBatch(ctx, n.dbDesc, b); err != nil {
		return err
	}
	if err := p.txn.Run(ctx, b); err != nil {
		return err
	}

	if err := p.removeDbComment(ctx, n.dbDesc.GetID()); err != nil {
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
	for _, toDel := range objects {
		err := p.accumulateCascadingViews(ctx, implicitDeleteObjects, toDel.desc)
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
			ownedSeqDesc, err := p.Descriptors().GetMutableTableVersionByID(ctx, seqID, p.txn)
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
	ctx context.Context, dependentObjects map[descpb.ID]*tabledesc.Mutable, desc *tabledesc.Mutable,
) error {
	for _, ref := range desc.DependedOnBy {
		dependentDesc, err := p.Descriptors().GetMutableTableVersionByID(ctx, ref.ID, p.txn)
		if err != nil {
			return err
		}
		if !dependentDesc.IsView() {
			continue
		}
		dependentObjects[ref.ID] = dependentDesc
		if err := p.accumulateCascadingViews(ctx, dependentObjects, dependentDesc); err != nil {
			return err
		}
	}
	return nil
}

func (p *planner) removeDbComment(ctx context.Context, dbID descpb.ID) error {
	_, err := p.ExtendedEvalContext().ExecCfg.InternalExecutor.ExecEx(
		ctx,
		"delete-db-comment",
		p.txn,
		sessiondata.InternalExecutorOverride{User: security.RootUserName()},
		"DELETE FROM system.comments WHERE type=$1 AND object_id=$2 AND sub_id=0",
		keys.DatabaseCommentType,
		dbID)

	return err
}
