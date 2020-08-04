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

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/errors"
)

type dropSchemaNode struct {
	n  *tree.DropSchema
	db *sqlbase.ImmutableDatabaseDescriptor
	d  *dropCascadeState
}

// Use to satisfy the linter.
var _ planNode = &dropSchemaNode{n: nil}

func (p *planner) DropSchema(ctx context.Context, n *tree.DropSchema) (planNode, error) {
	// Resolve the current schema.
	db, err := p.ResolveUncachedDatabaseByName(ctx, p.CurrentDatabase(), true /* required */)
	if err != nil {
		return nil, err
	}

	d := &dropCascadeState{}

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
		case sqlbase.SchemaPublic, sqlbase.SchemaVirtual, sqlbase.SchemaTemporary:
			return nil, pgerror.Newf(pgcode.InvalidSchemaName, "cannot drop schema %q", scName)
		case sqlbase.SchemaUserDefined:
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
	telemetry.Inc(sqltelemetry.SchemaChangeDropCounter("database"))

	ctx := params.ctx
	p := params.p

	// Drop all collected objects.
	if err := n.d.dropAllCollectedObjects(ctx, p); err != nil {
		return err
	}

	mutDB := sqlbase.NewMutableExistingDatabaseDescriptor(*n.db.DatabaseDesc())
	mutDB.MaybeIncrementVersion()

	// Queue the job to actually drop the schema.
	schemaIDs := make([]sqlbase.ID, len(n.d.schemasToDelete))
	b := p.txn.NewBatch()
	for i := range n.d.schemasToDelete {
		sc := n.d.schemasToDelete[i]
		schemaIDs[i] = sc.ID
		mutDesc := sqlbase.NewMutableExistingSchemaDescriptor(*sc.Desc.SchemaDesc())
		mutDesc.DrainingNames = append(mutDesc.DrainingNames, sqlbase.NameInfo{
			ParentID:       n.db.ID,
			ParentSchemaID: keys.RootNamespaceID,
			Name:           mutDesc.Name,
		})
		mutDB.Schemas[mutDesc.Name] = sqlbase.DatabaseDescriptor_SchemaInfo{
			ID:      mutDesc.ID,
			Dropped: true,
		}
		// TODO (rohany): Write that it's dropped here.
		mutDesc.MaybeIncrementVersion()
		if err := catalogkv.WriteDescToBatch(
			ctx,
			p.extendedEvalCtx.Tracing.KVTracingEnabled(),
			p.ExecCfg().Settings,
			b, p.ExecCfg().Codec,
			sc.ID,
			mutDesc,
		); err != nil {
			return err
		}
	}
	if err := catalogkv.WriteDescToBatch(
		ctx,
		p.extendedEvalCtx.Tracing.KVTracingEnabled(),
		p.ExecCfg().Settings,
		b, p.ExecCfg().Codec,
		mutDB.ID,
		mutDB,
	); err != nil {
		return err
	}
	if err := p.txn.Run(ctx, b); err != nil {
		return err
	}

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
	schemas []sqlbase.ID,
	tableDropDetails []jobspb.DroppedTableDetails,
	typesToDrop []*sqlbase.MutableTypeDescriptor,
	jobDesc string,
) error {
	tableIDs := make([]sqlbase.ID, 0, len(tableDropDetails))
	for _, d := range tableDropDetails {
		tableIDs = append(tableIDs, d.ID)
	}
	typeIDs := make([]sqlbase.ID, 0, len(typesToDrop))
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
			DroppedDatabaseID: sqlbase.InvalidID,
			FormatVersion:     jobspb.JobResumerFormatVersion,
		},
		Progress: jobspb.SchemaChangeProgress{},
	})
	return err
}

// TODO (rohany): Have a separate method dropSchemaImpl that actually does the drop schema work,
//  but not the object cascading.
// I'm not sure what actually needs to get done here... The drop job will handle it all.

func (n *dropSchemaNode) Next(params runParams) (bool, error) { return false, nil }
func (n *dropSchemaNode) Values() tree.Datums                 { return tree.Datums{} }
func (n *dropSchemaNode) Close(ctx context.Context)           {}
func (n *dropSchemaNode) ReadingOwnWrites()                   {}
