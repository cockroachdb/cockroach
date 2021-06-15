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

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/errors"
)

type alterSchemaNode struct {
	n    *tree.AlterSchema
	db   *dbdesc.Mutable
	desc *schemadesc.Mutable
}

// Use to satisfy the linter.
var _ planNode = &alterSchemaNode{n: nil}

func (p *planner) AlterSchema(ctx context.Context, n *tree.AlterSchema) (planNode, error) {
	if err := checkSchemaChangeEnabled(
		ctx,
		p.ExecCfg(),
		"ALTER SCHEMA",
	); err != nil {
		return nil, err
	}

	dbName := p.CurrentDatabase()
	if n.Schema.ExplicitCatalog {
		dbName = n.Schema.Catalog()
	}
	db, err := p.Descriptors().GetMutableDatabaseByName(ctx, p.txn, dbName,
		tree.DatabaseLookupFlags{Required: true})
	if err != nil {
		return nil, err
	}
	schema, err := p.Descriptors().GetSchemaByName(ctx, p.txn, db,
		string(n.Schema.SchemaName), tree.SchemaLookupFlags{
			Required:       true,
			RequireMutable: true,
		})
	if err != nil {
		return nil, err
	}
	switch schema.SchemaKind() {
	case catalog.SchemaPublic, catalog.SchemaVirtual, catalog.SchemaTemporary:
		return nil, pgerror.Newf(pgcode.InvalidSchemaName, "cannot modify schema %q", n.Schema.String())
	case catalog.SchemaUserDefined:
		desc := schema.(*schemadesc.Mutable)
		// The user must be a superuser or the owner of the schema to modify it.
		hasAdmin, err := p.HasAdminRole(ctx)
		if err != nil {
			return nil, err
		}
		if !hasAdmin {
			hasOwnership, err := p.HasOwnership(ctx, desc)
			if err != nil {
				return nil, err
			}
			if !hasOwnership {
				return nil, pgerror.Newf(pgcode.InsufficientPrivilege, "must be owner of schema %q", desc.Name)
			}
		}
		sqltelemetry.IncrementUserDefinedSchemaCounter(sqltelemetry.UserDefinedSchemaAlter)
		return &alterSchemaNode{n: n, db: db, desc: desc}, nil
	default:
		return nil, errors.AssertionFailedf("unknown schema kind")
	}
}

func (n *alterSchemaNode) startExec(params runParams) error {
	switch t := n.n.Cmd.(type) {
	case *tree.AlterSchemaRename:
		newName := string(t.NewName)

		oldQualifiedSchemaName, err := params.p.getQualifiedSchemaName(params.ctx, n.desc)
		if err != nil {
			return err
		}

		if err := params.p.renameSchema(
			params.ctx, n.db, n.desc, newName, tree.AsStringWithFQNames(n.n, params.Ann()),
		); err != nil {
			return err
		}

		newQualifiedSchemaName, err := params.p.getQualifiedSchemaName(params.ctx, n.desc)
		if err != nil {
			return err
		}

		return params.p.logEvent(params.ctx, n.desc.ID, &eventpb.RenameSchema{
			SchemaName:    oldQualifiedSchemaName.String(),
			NewSchemaName: newQualifiedSchemaName.String(),
		})
	case *tree.AlterSchemaOwner:
		newOwner := t.Owner
		return params.p.alterSchemaOwner(
			params.ctx, n.desc, newOwner, tree.AsStringWithFQNames(n.n, params.Ann()),
		)
	default:
		return errors.AssertionFailedf("unknown schema cmd %T", t)
	}
}

func (p *planner) alterSchemaOwner(
	ctx context.Context,
	scDesc *schemadesc.Mutable,
	newOwner security.SQLUsername,
	jobDescription string,
) error {
	oldOwner := scDesc.GetPrivileges().Owner()

	if err := p.checkCanAlterSchemaAndSetNewOwner(ctx, scDesc, newOwner); err != nil {
		return err
	}

	// If the owner we want to set to is the current owner, do a no-op.
	if newOwner == oldOwner {
		return nil
	}

	return p.writeSchemaDescChange(ctx, scDesc, jobDescription)
}

// checkCanAlterSchemaAndSetNewOwner handles privilege checking and setting new owner.
// Called in ALTER SCHEMA and REASSIGN OWNED BY.
func (p *planner) checkCanAlterSchemaAndSetNewOwner(
	ctx context.Context, scDesc *schemadesc.Mutable, newOwner security.SQLUsername,
) error {
	if err := p.checkCanAlterToNewOwner(ctx, scDesc, newOwner); err != nil {
		return err
	}

	// The user must also have CREATE privilege on the schema's database.
	parentDBDesc, err := p.Descriptors().GetMutableDescriptorByID(ctx, scDesc.GetParentID(), p.txn)
	if err != nil {
		return err
	}
	if err := p.CheckPrivilege(ctx, parentDBDesc, privilege.CREATE); err != nil {
		return err
	}

	// Update the owner of the schema.
	privs := scDesc.GetPrivileges()
	privs.SetOwner(newOwner)

	qualifiedSchemaName, err := p.getQualifiedSchemaName(ctx, scDesc)
	if err != nil {
		return err
	}

	return p.logEvent(ctx,
		scDesc.GetID(),
		&eventpb.AlterSchemaOwner{
			SchemaName: qualifiedSchemaName.String(),
			Owner:      newOwner.Normalized(),
		})
}

func (p *planner) renameSchema(
	ctx context.Context, db *dbdesc.Mutable, desc *schemadesc.Mutable, newName string, jobDesc string,
) error {
	// Check that there isn't a name collision with the new name.
	found, _, err := schemaExists(ctx, p.txn, p.ExecCfg().Codec, db.ID, newName)
	if err != nil {
		return err
	}
	if found {
		return sqlerrors.NewSchemaAlreadyExistsError(newName)
	}

	// Ensure that the new name is a valid schema name.
	if err := schemadesc.IsSchemaNameValid(newName); err != nil {
		return err
	}

	// Set the new name for the descriptor.
	oldName := desc.Name
	desc.SetName(newName)

	// Write a new namespace entry for the new name.
	nameKey := catalogkeys.MakeSchemaNameKey(p.execCfg.Codec, desc.ParentID, newName)
	b := p.txn.NewBatch()
	if p.ExtendedEvalContext().Tracing.KVTracingEnabled() {
		log.VEventf(ctx, 2, "CPut %s -> %d", nameKey, desc.ID)
	}
	b.CPut(nameKey, desc.ID, nil)
	if err := p.txn.Run(ctx, b); err != nil {
		return err
	}

	// Update the schema mapping in the parent database.

	// First, ensure that the new name isn't present, and that we have an entry
	// for the old name.
	_, oldPresent := db.Schemas[oldName]
	_, newPresent := db.Schemas[newName]
	if !oldPresent {
		return errors.AssertionFailedf(
			"old name %q not present in database schema mapping",
			oldName,
		)
	}
	if newPresent {
		return errors.AssertionFailedf(
			"new name %q already present in database schema mapping",
			newName,
		)
	}

	// Mark the old schema name as dropped.
	db.Schemas[oldName] = descpb.DatabaseDescriptor_SchemaInfo{
		ID:      desc.ID,
		Dropped: true,
	}
	// Create an entry for the new schema name.
	db.Schemas[newName] = descpb.DatabaseDescriptor_SchemaInfo{
		ID:      desc.ID,
		Dropped: false,
	}
	if err := p.writeNonDropDatabaseChange(
		ctx, db,
		fmt.Sprintf("updating parent database %s for %s", db.GetName(), jobDesc),
	); err != nil {
		return err
	}

	// Write the change to the schema itself.
	return p.writeSchemaDescChange(ctx, desc, jobDesc)
}

func (n *alterSchemaNode) Next(params runParams) (bool, error) { return false, nil }
func (n *alterSchemaNode) Values() tree.Datums                 { return tree.Datums{} }
func (n *alterSchemaNode) Close(ctx context.Context)           {}
func (n *alterSchemaNode) ReadingOwnWrites()                   {}
