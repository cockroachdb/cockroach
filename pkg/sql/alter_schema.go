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

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
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
	db, err := p.ResolveMutableDatabaseDescriptor(ctx, p.CurrentDatabase(), true /* required */)
	if err != nil {
		return nil, err
	}
	found, schema, err := p.ResolveMutableSchemaDescriptor(ctx, db.ID, n.Schema, true /* required */)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, pgerror.Newf(pgcode.InvalidSchemaName, "schema %q does not exist", n.Schema)
	}
	switch schema.Kind {
	case catalog.SchemaPublic, catalog.SchemaVirtual, catalog.SchemaTemporary:
		return nil, pgerror.Newf(pgcode.InvalidSchemaName, "cannot modify schema %q", n.Schema)
	case catalog.SchemaUserDefined:
		desc := schema.Desc.(*schemadesc.Mutable)
		// The user must be the owner of the schema to modify it.
		hasOwnership, err := p.HasOwnership(ctx, desc)
		if err != nil {
			return nil, err
		}
		if !hasOwnership {
			return nil, pgerror.Newf(pgcode.InsufficientPrivilege, "must be owner of schema %q", desc.Name)
		}
		return &alterSchemaNode{n: n, db: db, desc: desc}, nil
	default:
		return nil, errors.AssertionFailedf("unknown schema kind")
	}
}

func (n *alterSchemaNode) startExec(params runParams) error {
	switch t := n.n.Cmd.(type) {
	case *tree.AlterSchemaRename:
		return params.p.renameSchema(params.ctx, n.db, n.desc, t.NewName, tree.AsStringWithFQNames(n.n, params.Ann()))
	case *tree.AlterSchemaOwner:
		return params.p.alterSchemaOwner(params.ctx, n.db, n.desc, t.Owner, tree.AsStringWithFQNames(n.n, params.Ann()))
	default:
		return errors.AssertionFailedf("unknown schema cmd %T", t)
	}
}

func (p *planner) alterSchemaOwner(
	ctx context.Context,
	db *dbdesc.Mutable,
	scDesc *schemadesc.Mutable,
	newOwner string,
	jobDesc string,
) error {
	privs := scDesc.GetPrivileges()

	hasOwnership, err := p.HasOwnership(ctx, scDesc)
	if err != nil {
		return err
	}

	if err := p.checkCanAlterToNewOwner(ctx, scDesc, privs, newOwner, hasOwnership); err != nil {
		return err
	}

	// The new owner must also have CREATE privilege on the schema's database.
	if err := p.CheckPrivilegeForUser(ctx, db, privilege.CREATE, newOwner); err != nil {
		return err
	}

	// If the owner we want to set to is the current owner, do a no-op.
	if newOwner == privs.Owner {
		return nil
	}

	// Update the owner of the schema.
	privs.SetOwner(newOwner)

	return p.writeSchemaDescChange(ctx, scDesc, jobDesc)
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
