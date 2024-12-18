// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/funcdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/errors"
)

type dropSchemaNode struct {
	zeroInputPlanNode
	n *tree.DropSchema
	d *dropCascadeState
}

// Use to satisfy the linter.
var _ planNode = &dropSchemaNode{n: nil}

func (p *planner) DropSchema(ctx context.Context, n *tree.DropSchema) (planNode, error) {
	if err := checkSchemaChangeEnabled(
		ctx,
		p.ExecCfg(),
		"DROP SCHEMA",
	); err != nil {
		return nil, err
	}

	d := newDropCascadeState()

	// Collect all schemas to be deleted.
	for _, schema := range n.Names {
		dbName := p.CurrentDatabase()
		if schema.ExplicitCatalog {
			dbName = schema.Catalog()
		}
		scName := schema.Schema()

		db, err := p.Descriptors().MutableByName(p.txn).Database(ctx, dbName)
		if err != nil {
			return nil, err
		}
		sc, err := p.Descriptors().ByName(p.txn).MaybeGet().Schema(ctx, db, scName)
		if err != nil {
			return nil, err
		}
		if sc == nil {
			if n.IfExists {
				continue
			}
			return nil, pgerror.Newf(pgcode.InvalidSchemaName, "unknown schema %q", scName)
		}
		hasOwnership, err := p.HasOwnership(ctx, sc)
		if err != nil {
			return nil, err
		}
		if !hasOwnership {
			return nil, pgerror.Newf(pgcode.InsufficientPrivilege,
				"must be owner of schema %s", tree.Name(sc.GetName()))
		}

		if scName == catconstants.PublicSchemaName {
			return nil, pgerror.Newf(pgcode.InvalidSchemaName, "cannot drop schema %q", scName)
		}

		switch sc.SchemaKind() {
		case catalog.SchemaPublic, catalog.SchemaVirtual, catalog.SchemaTemporary:
			return nil, pgerror.Newf(pgcode.InvalidSchemaName, "cannot drop schema %q", scName)
		case catalog.SchemaUserDefined:
			namesBefore := len(d.objectNamesToDelete)
			fnsBefore := len(d.functionsToDelete)
			if err := d.collectObjectsInSchema(ctx, p, db, sc); err != nil {
				return nil, err
			}
			// We added some new objects to delete. Ensure that we have the correct
			// drop behavior to be doing this.
			if (namesBefore != len(d.objectNamesToDelete) || fnsBefore != len(d.functionsToDelete)) &&
				n.DropBehavior != tree.DropCascade {

				return nil, pgerror.Newf(pgcode.DependentObjectsStillExist,
					"schema %q is not empty and CASCADE was not specified", scName)
			}
			sqltelemetry.IncrementUserDefinedSchemaCounter(sqltelemetry.UserDefinedSchemaDrop)
		default:
			return nil, errors.AssertionFailedf("unknown schema kind %d", sc.SchemaKind())
		}

	}

	if err := d.resolveCollectedObjects(ctx, false /*dropDatabase*/, p); err != nil {
		return nil, err
	}

	return &dropSchemaNode{n: n, d: d}, nil
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
		sc := n.d.schemasToDelete[i].schema
		schemaIDs[i] = sc.GetID()
		db := n.d.schemasToDelete[i].dbDesc
		mutDesc, err := p.Descriptors().MutableByID(p.txn).Schema(ctx, sc.GetID())
		if err != nil {
			return err
		}
		if err := p.dropSchemaImpl(ctx, db, mutDesc); err != nil {
			return err
		}
	}

	// Write out the change to the database.
	for i := range n.d.schemasToDelete {
		sc := n.d.schemasToDelete[i].schema
		db := n.d.schemasToDelete[i].dbDesc
		if err := p.writeNonDropDatabaseChange(
			ctx, db,
			fmt.Sprintf("updating parent database %s for %s", db.GetName(), sc.GetName()),
		); err != nil {
			return err
		}
	}

	// Create the job to drop the schema.
	if err := p.createDropSchemaJob(
		schemaIDs,
		n.d.getDroppedTableDetails(),
		n.d.typesToDelete,
		n.d.functionsToDelete,
		tree.AsStringWithFQNames(n.n, params.Ann()),
	); err != nil {
		return err
	}

	// Log Drop Schema event. This is an auditable log event and is recorded
	// in the same transaction as table descriptor update.
	for _, schemaToDelete := range n.d.schemasToDelete {
		sc := schemaToDelete.schema
		qualifiedSchemaName, err := p.getQualifiedSchemaName(params.ctx, sc)
		if err != nil {
			return err
		}

		if err := params.p.logEvent(params.ctx,
			sc.GetID(),
			&eventpb.DropSchema{
				SchemaName: qualifiedSchemaName.String(),
			}); err != nil {
			return err
		}
	}
	return nil
}

// dropSchemaImpl performs the logic of dropping a user defined schema. It does
// not create a job to perform the final cleanup of the schema.
func (p *planner) dropSchemaImpl(
	ctx context.Context, parentDB *dbdesc.Mutable, sc *schemadesc.Mutable,
) error {

	// Update parent database schemas mapping.
	delete(parentDB.Schemas, sc.GetName())

	// Exit early with an error if the schema is undergoing a declarative schema
	// change.
	if catalog.HasConcurrentDeclarativeSchemaChange(sc) {
		return scerrors.ConcurrentSchemaChangeError(sc)
	}

	// Update the schema descriptor as dropped.
	sc.SetDropped()

	// Populate namespace update batch.
	b := p.txn.NewBatch()
	if err := p.dropNamespaceEntry(ctx, b, sc); err != nil {
		return err
	}

	// Remove any associated comments.
	if err := p.deleteComment(ctx, sc.GetID(), 0, catalogkeys.SchemaCommentType); err != nil {
		return err
	}

	// Write the updated descriptor.
	if err := p.writeSchemaDesc(ctx, sc); err != nil {
		return err
	}

	// Run the namespace update batch.
	return p.txn.Run(ctx, b)
}

func (p *planner) createDropSchemaJob(
	schemas []descpb.ID,
	tableDropDetails []jobspb.DroppedTableDetails,
	typesToDrop []*typedesc.Mutable,
	functionsToDrop []*funcdesc.Mutable,
	jobDesc string,
) error {
	if err := p.checkDDLAtomicity(); err != nil {
		return err
	}

	typeIDs := make([]descpb.ID, 0, len(typesToDrop))
	for _, t := range typesToDrop {
		typeIDs = append(typeIDs, t.ID)
	}
	fnIDs := make([]descpb.ID, 0, len(functionsToDrop))
	for _, f := range functionsToDrop {
		fnIDs = append(fnIDs, f.ID)
	}

	p.extendedEvalCtx.QueueJob(&jobs.Record{
		Description:   jobDesc,
		Username:      p.User(),
		DescriptorIDs: schemas,
		Details: jobspb.SchemaChangeDetails{
			DroppedSchemas:    schemas,
			DroppedTables:     tableDropDetails,
			DroppedTypes:      typeIDs,
			DroppedFunctions:  fnIDs,
			DroppedDatabaseID: descpb.InvalidID,
			// The version distinction for database jobs doesn't matter for jobs that
			// drop schemas.
			FormatVersion: jobspb.DatabaseJobFormatVersion,
			SessionData:   &p.SessionData().SessionData,
		},
		Progress:      jobspb.SchemaChangeProgress{},
		NonCancelable: true,
	})
	return nil
}

func (n *dropSchemaNode) Next(params runParams) (bool, error) { return false, nil }
func (n *dropSchemaNode) Values() tree.Datums                 { return tree.Datums{} }
func (n *dropSchemaNode) Close(ctx context.Context)           {}
func (n *dropSchemaNode) ReadingOwnWrites()                   {}
