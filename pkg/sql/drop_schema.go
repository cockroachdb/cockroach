// Copyright 2020 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/errors"
)

type dropSchemaNode struct {
	n  *tree.DropSchema
	db *sqlbase.MutableDatabaseDescriptor
	d  *dropCascadeState
}

// Use to satisfy the linter.
var _ planNode = &dropSchemaNode{n: nil}

func (p *planner) DropSchema(ctx context.Context, n *tree.DropSchema) (planNode, error) {
	db, err := p.ResolveMutableDatabaseDescriptor(ctx, p.CurrentDatabase(), true /* required */)
	if err != nil {
		return nil, err
	}

	d := newDropCascadeState()

	// Collect all schemas to be deleted.
	for _, scName := range n.Names {
		found, sc, err := p.LogicalSchemaAccessor().GetSchema(ctx, p.txn, p.ExecCfg().Codec, db.ID, scName)
		if err != nil {
			return nil, err
		}
		if !found {
			if n.IfExists {
				continue
			}
			return nil, pgerror.Newf(pgcode.InvalidSchemaName, "unknown schema %q", scName)
		}
		switch sc.Kind {
		case catalog.SchemaPublic, catalog.SchemaVirtual, catalog.SchemaTemporary:
			return nil, pgerror.Newf(pgcode.InvalidSchemaName, "cannot drop schema %q", scName)
		case catalog.SchemaUserDefined:
			namesBefore := len(d.objectNamesToDelete)
			// TODO (rohany): Do a permissions check on the schema.
			if err := d.collectObjectsInSchema(ctx, p, db, &sc); err != nil {
				return nil, err
			}
			// We added some new objects to delete. Ensure that we have the correct
			// drop behavior to be doing this.
			if namesBefore != len(d.objectNamesToDelete) && n.DropBehavior != tree.DropCascade {
				return nil, pgerror.Newf(pgcode.DependentObjectsStillExist,
					"schema %q is not empty and CASCADE was not specified", scName)
			}
		default:
			return nil, errors.AssertionFailedf("unknown schema kind %d", sc.Kind)
		}
	}

	if err := d.resolveCollectedObjects(ctx, p, db); err != nil {
		return nil, err
	}

	return &dropSchemaNode{n: n, d: d, db: db}, nil
}

func (n *dropSchemaNode) startExec(params runParams) error {
	telemetry.Inc(sqltelemetry.SchemaChangeDropCounter("schema"))

	ctx := params.ctx
	p := params.p

	// Drop all collected objects.
	if err := n.d.dropAllCollectedObjects(ctx, p); err != nil {
		return err
	}

	// Queue the job to actually drop the schema.
	schemaIDs := make([]descpb.ID, len(n.d.schemasToDelete))
	for i := range n.d.schemasToDelete {
		sc := n.d.schemasToDelete[i]
		schemaIDs[i] = sc.ID
		mutDesc, err := params.p.Descriptors().GetMutableSchemaDescriptorByID(ctx, sc.ID, params.p.txn)
		if err != nil {
			return err
		}
		mutDesc.DrainingNames = append(mutDesc.DrainingNames, descpb.NameInfo{
			ParentID:       n.db.ID,
			ParentSchemaID: keys.RootNamespaceID,
			Name:           mutDesc.Name,
		})
		n.db.Schemas[mutDesc.Name] = descpb.DatabaseDescriptor_SchemaInfo{
			ID:      mutDesc.ID,
			Dropped: true,
		}
		// Mark the descriptor as dropped.
		mutDesc.State = descpb.SchemaDescriptor_DROP
		if err := p.writeSchemaDesc(ctx, mutDesc); err != nil {
			return err
		}
	}

	// Write out the change to the database.
	if err := p.writeNonDropDatabaseChange(
		ctx, n.db,
		fmt.Sprintf("updating parent database %s for %s", n.db.GetName(), tree.AsStringWithFQNames(n.n, params.Ann())),
	); err != nil {
		return err
	}

	// Create the job to drop the schema.
	if err := p.createDropSchemaJob(
		schemaIDs,
		n.d.getDroppedTableDetails(),
		n.d.typesToDelete,
		tree.AsStringWithFQNames(n.n, params.Ann()),
	); err != nil {
		return err
	}

	// Log Drop Schema event. This is an auditable log event and is recorded
	// in the same transaction as table descriptor update.
	for _, sc := range n.d.schemasToDelete {
		if err := MakeEventLogger(params.extendedEvalCtx.ExecCfg).InsertEventRecord(
			ctx,
			p.txn,
			EventLogDropSchema,
			int32(sc.ID),
			int32(params.extendedEvalCtx.NodeID.SQLInstanceID()),
			struct {
				SchemaName string
				Statement  string
				User       string
			}{sc.Name, n.n.String(), p.SessionData().User},
		); err != nil {
			return err
		}
	}
	return nil
}

func (p *planner) createDropSchemaJob(
	schemas []descpb.ID,
	tableDropDetails []jobspb.DroppedTableDetails,
	typesToDrop []*sqlbase.MutableTypeDescriptor,
	jobDesc string,
) error {
	typeIDs := make([]descpb.ID, 0, len(typesToDrop))
	for _, t := range typesToDrop {
		typeIDs = append(typeIDs, t.ID)
	}

	_, err := p.extendedEvalCtx.QueueJob(jobs.Record{
		Description:   jobDesc,
		Username:      p.User(),
		DescriptorIDs: schemas,
		Details: jobspb.SchemaChangeDetails{
			DroppedSchemas:    schemas,
			DroppedTables:     tableDropDetails,
			DroppedTypes:      typeIDs,
			DroppedDatabaseID: descpb.InvalidID,
			// The version distinction for database jobs doesn't matter for jobs that
			// drop schemas.
			FormatVersion: jobspb.DatabaseJobFormatVersion,
		},
		Progress: jobspb.SchemaChangeProgress{},
	})
	return err
}

func (n *dropSchemaNode) Next(params runParams) (bool, error) { return false, nil }
func (n *dropSchemaNode) Values() tree.Datums                 { return tree.Datums{} }
func (n *dropSchemaNode) Close(ctx context.Context)           {}
func (n *dropSchemaNode) ReadingOwnWrites()                   {}
