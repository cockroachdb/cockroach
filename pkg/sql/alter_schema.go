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
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/util/log"
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
	dbName := p.CurrentDatabase()
	if n.Schema.ExplicitCatalog {
		dbName = n.Schema.Catalog()
	}
	db, err := p.ResolveMutableDatabaseDescriptor(ctx, dbName, true /* required */)
	if err != nil {
		return nil, err
	}
	found, schema, err := p.ResolveMutableSchemaDescriptor(ctx, db.ID, string(n.Schema.SchemaName), true /* required */)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, pgerror.Newf(pgcode.InvalidSchemaName, "schema %q does not exist", n.Schema.String())
	}
	switch schema.Kind {
	case catalog.SchemaPublic, catalog.SchemaVirtual, catalog.SchemaTemporary:
		return nil, pgerror.Newf(pgcode.InvalidSchemaName, "cannot modify schema %q", n.Schema.String())
	case catalog.SchemaUserDefined:
		desc := schema.Desc.(*schemadesc.Mutable)
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
		oldName := n.desc.Name
		newName := string(t.NewName)
		if err := params.p.renameSchema(
			params.ctx, n.db, n.desc, newName, tree.AsStringWithFQNames(n.n, params.Ann()),
		); err != nil {
			return err
		}
		return MakeEventLogger(params.extendedEvalCtx.ExecCfg).InsertEventRecord(
			params.ctx,
			params.p.txn,
			EventLogRenameSchema,
			int32(n.desc.ID),
			int32(params.extendedEvalCtx.NodeID.SQLInstanceID()),
			struct {
				SchemaName    string
				NewSchemaName string
				User          string
			}{oldName, newName, params.p.User().Normalized()},
		)
	case *tree.AlterSchemaOwner:
		newOwner := t.Owner
		if err := params.p.alterSchemaOwner(
			params.ctx, n.desc, newOwner, tree.AsStringWithFQNames(n.n, params.Ann()),
		); err != nil {
			return err
		}
		return MakeEventLogger(params.extendedEvalCtx.ExecCfg).InsertEventRecord(
			params.ctx,
			params.p.txn,
			EventLogAlterSchemaOwner,
			int32(n.desc.ID),
			int32(params.extendedEvalCtx.NodeID.SQLInstanceID()),
			struct {
				SchemaName string
				Owner      string
				User       string
			}{n.desc.Name, newOwner.Normalized(), params.p.User().Normalized()},
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
	privs := scDesc.GetPrivileges()

	// If the owner we want to set to is the current owner, do a no-op.
	if newOwner == privs.Owner() {
		return nil
	}

	if err := p.checkCanAlterSchemaAndSetNewOwner(ctx, scDesc, newOwner); err != nil {
		return err
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

	return nil
}

func (p *planner) renameSchema(
	ctx context.Context, db *dbdesc.Mutable, desc *schemadesc.Mutable, newName string, jobDesc string,
) error {
	// Check that there isn't a name collision with the new name.
	found, err := p.schemaExists(ctx, db.ID, newName)
	if err != nil {
		return err
	}
	if found {
		return pgerror.Newf(pgcode.DuplicateSchema, "schema %q already exists", newName)
	}

	// Ensure that the new name is a valid schema name.
	if err := schemadesc.IsSchemaNameValid(newName); err != nil {
		return err
	}

	// Set the new name for the descriptor.
	oldName := desc.Name
	desc.SetName(newName)

	// Write a new namespace entry for the new name.
	nameKey := catalogkeys.NewSchemaKey(desc.ParentID, newName).Key(p.execCfg.Codec)
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
