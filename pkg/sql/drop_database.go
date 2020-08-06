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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
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
	n                       *tree.DropDatabase
	dbDesc                  *sqlbase.ImmutableDatabaseDescriptor
	td                      []toDelete
	schemasToDelete         []string
	allTableObjectsToDelete []*sqlbase.MutableTableDescriptor
	typesToDelete           []*sqlbase.MutableTypeDescriptor
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

	schemas, err := p.Descriptors().GetSchemasForDatabase(ctx, p.txn, dbDesc.GetID())
	if err != nil {
		return nil, err
	}

	var objNames []tree.ObjectName
	schemasToDelete := make([]string, 0, len(schemas))
	for _, schema := range schemas {
		schemasToDelete = append(schemasToDelete, schema)
		toAppend, err := resolver.GetObjectNames(
			ctx, p.txn, p, p.ExecCfg().Codec, dbDesc, schema, true, /*explicitPrefix*/
		)
		if err != nil {
			return nil, err
		}
		for i := range toAppend {
			objNames = append(objNames, &toAppend[i])
		}
	}

	if len(objNames) > 0 {
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

	var typesToDelete []*sqlbase.MutableTypeDescriptor
	td := make([]toDelete, 0, len(objNames))
	for i, objName := range objNames {
		// First try looking up objName as a table.
		found, desc, err := p.LookupObject(
			ctx,
			tree.ObjectLookupFlags{
				// Note we set required to be false here in order to not error out
				// if we don't find the object,
				CommonLookupFlags: tree.CommonLookupFlags{Required: false},
				RequireMutable:    true,
				IncludeOffline:    true,
				DesiredObjectKind: tree.TableObject,
			},
			objName.Catalog(),
			objName.Schema(),
			objName.Object(),
		)
		if err != nil {
			return nil, err
		}
		if found {
			tbDesc, ok := desc.(*sqlbase.MutableTableDescriptor)
			if !ok {
				return nil, errors.AssertionFailedf(
					"descriptor for %q is not MutableTableDescriptor",
					objName.Object(),
				)
			}
			if tbDesc.State == descpb.TableDescriptor_OFFLINE {
				dbName := dbDesc.GetName()
				return nil, pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
					"cannot drop a database with OFFLINE tables, ensure %s is"+
						" dropped or made public before dropping database %s",
					objName.FQString(), tree.AsString((*tree.Name)(&dbName)))
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
			td = append(td, toDelete{objNames[i], tbDesc})
		} else {
			// If we couldn't resolve objName as a table, try a type.
			found, desc, err := p.LookupObject(
				ctx,
				tree.ObjectLookupFlags{
					CommonLookupFlags: tree.CommonLookupFlags{Required: true},
					RequireMutable:    true,
					IncludeOffline:    true,
					DesiredObjectKind: tree.TypeObject,
				},
				objName.Catalog(),
				objName.Schema(),
				objName.Object(),
			)
			if err != nil {
				return nil, err
			}
			// If we couldn't find the object at all, then continue.
			if !found {
				continue
			}
			typDesc, ok := desc.(*sqlbase.MutableTypeDescriptor)
			if !ok {
				return nil, errors.AssertionFailedf(
					"descriptor for %q is not MutableTypeDescriptor",
					objName.Object(),
				)
			}
			// Types can only depend on objects within this database, so we don't
			// need to do any more verification about whether or not we can drop
			// this type.
			typesToDelete = append(typesToDelete, typDesc)
		}
	}

	allObjectsToDelete, implicitDeleteMap, err := p.accumulateAllObjectsToDelete(ctx, td)
	if err != nil {
		return nil, err
	}

	return &dropDatabaseNode{
		n:                       n,
		dbDesc:                  dbDesc,
		td:                      filterImplicitlyDeletedObjects(td, implicitDeleteMap),
		schemasToDelete:         schemasToDelete,
		allTableObjectsToDelete: allObjectsToDelete,
		typesToDelete:           typesToDelete,
	}, nil
}

func (n *dropDatabaseNode) startExec(params runParams) error {
	telemetry.Inc(sqltelemetry.SchemaChangeDropCounter("database"))

	ctx := params.ctx
	p := params.p
	tbNameStrings := make([]string, 0, len(n.td))
	droppedTableDetails := make([]jobspb.DroppedTableDetails, 0, len(n.td))

	for _, delDesc := range n.allTableObjectsToDelete {
		droppedTableDetails = append(droppedTableDetails, jobspb.DroppedTableDetails{
			Name: delDesc.Name,
			ID:   delDesc.ID,
		})
	}
	if err := p.createDropDatabaseJob(
		ctx, n.dbDesc.GetID(), droppedTableDetails, n.typesToDelete, tree.AsStringWithFQNames(n.n, params.Ann()),
	); err != nil {
		return err
	}

	for _, typ := range n.typesToDelete {
		// Drop the types. Note that we set queueJob to be false because the types
		// will be dropped in bulk as part of the DROP DATABASE job.
		if err := p.dropTypeImpl(params.ctx, typ, tree.AsStringWithFQNames(n.n, params.Ann()), false /* queueJob */); err != nil {
			return err
		}
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
			cascadedObjects, err = p.dropTableImpl(ctx, desc, true /* droppingDatabase */, "")
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
		if err := catalogkv.RemoveSchemaNamespaceEntry(
			ctx,
			p.txn,
			p.ExecCfg().Codec,
			n.dbDesc.GetID(),
			schemaToDelete,
		); err != nil {
			return err
		}
	}

	err := catalogkv.RemoveDatabaseNamespaceEntry(
		ctx, p.txn, p.ExecCfg().Codec, n.dbDesc.GetName(), p.ExtendedEvalContext().Tracing.KVTracingEnabled(),
	)
	if err != nil {
		return err
	}

	// No job was created because no tables were dropped, so zone config can be
	// immediately removed, if applicable.
	if len(n.allTableObjectsToDelete) == 0 && params.ExecCfg().Codec.ForSystemTenant() {
		zoneKeyPrefix := config.MakeZoneKeyPrefix(config.SystemTenantObjectID(n.dbDesc.GetID()))
		if p.ExtendedEvalContext().Tracing.KVTracingEnabled() {
			log.VEventf(ctx, 2, "DelRange %s", zoneKeyPrefix)
		}
		// Delete the zone config entry for this database.
		b.DelRange(zoneKeyPrefix, zoneKeyPrefix.PrefixEnd(), false /* returnKeys */)
	}

	p.Descriptors().AddUncommittedDatabase(n.dbDesc.GetName(), n.dbDesc.GetID(), descs.DBDropped)

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

// filterImplicitlyDeletedObjects takes a list of table descriptors and removes
// any descriptor that will be implicitly deleted.
func filterImplicitlyDeletedObjects(
	tables []toDelete, implicitDeleteObjects map[descpb.ID]*MutableTableDescriptor,
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
) ([]*MutableTableDescriptor, map[descpb.ID]*MutableTableDescriptor, error) {
	implicitDeleteObjects := make(map[descpb.ID]*MutableTableDescriptor)
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
	allObjectsToDelete := make([]*MutableTableDescriptor, 0,
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
	ctx context.Context,
	dependentObjects map[descpb.ID]*MutableTableDescriptor,
	desc *sqlbase.MutableTableDescriptor,
) error {
	for colID := range desc.GetColumns() {
		for _, seqID := range desc.GetColumns()[colID].OwnsSequenceIds {
			ownedSeqDesc, err := p.Descriptors().GetMutableTableVersionByID(ctx, seqID, p.txn)
			if err != nil {
				// Special case error swallowing for #50711 and #50781, which can
				// cause columns to own sequences that have been dropped/do not
				// exist.
				if errors.Is(err, sqlbase.ErrDescriptorNotFound) {
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
	dependentObjects map[descpb.ID]*MutableTableDescriptor,
	desc *sqlbase.MutableTableDescriptor,
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
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		"DELETE FROM system.comments WHERE type=$1 AND object_id=$2 AND sub_id=0",
		keys.DatabaseCommentType,
		dbID)

	return err
}
