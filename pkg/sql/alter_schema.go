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

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

type alterSchemaNode struct {
	n    *tree.AlterSchema
	db   *sqlbase.MutableDatabaseDescriptor
	desc *sqlbase.MutableSchemaDescriptor
}

// Use to satisfy the linter.
var _ planNode = &alterSchemaNode{n: nil}

func (p *planner) AlterSchema(ctx context.Context, n *tree.AlterSchema) (planNode, error) {
	// TODO (rohany, lucy): There should be an API to get a MutableSchemaDescriptor
	//  by name from the descs.Collection.
	db, err := p.ResolveUncachedDatabaseByName(ctx, p.CurrentDatabase(), true /* required */)
	if err != nil {
		return nil, err
	}
	mutDB := sqlbase.NewMutableExistingDatabaseDescriptor(*db.DatabaseDesc())
	found, schema, err := p.LogicalSchemaAccessor().GetSchema(ctx, p.txn, p.ExecCfg().Codec, db.ID, n.Schema)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, pgerror.Newf(pgcode.InvalidSchemaName, "schema %q does not exist", n.Schema)
	}
	switch schema.Kind {
	case sqlbase.SchemaPublic, sqlbase.SchemaVirtual, sqlbase.SchemaTemporary:
		return nil, pgerror.Newf(pgcode.InvalidSchemaName, "cannot modify schema %q", n.Schema)
	case sqlbase.SchemaUserDefined:
		// TODO (rohany): Check permissions here.
		desc, err := p.Descriptors().GetMutableSchemaDescriptorByID(ctx, schema.ID, p.txn)
		if err != nil {
			return nil, err
		}
		return &alterSchemaNode{n: n, db: mutDB, desc: desc}, nil
	default:
		return nil, errors.AssertionFailedf("unknown schema kind")
	}
}

func (n *alterSchemaNode) startExec(params runParams) error {
	switch t := n.n.Cmd.(type) {
	case *tree.AlterSchemaRename:
		return params.p.renameSchema(params.ctx, n.db, n.desc, t.NewName, tree.AsStringWithFQNames(n.n, params.Ann()))
	default:
		return errors.AssertionFailedf("unknown schema cmd %T", t)
	}
}

func (p *planner) renameSchema(
	ctx context.Context,
	db *sqlbase.MutableDatabaseDescriptor,
	desc *sqlbase.MutableSchemaDescriptor,
	newName string,
	jobDesc string,
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
	if err := sqlbase.IsSchemaNameValid(newName); err != nil {
		return err
	}

	// Set the new name for the descriptor.
	oldName := desc.Name
	desc.SetName(newName)

	// Write a new namespace entry for the new name.
	nameKey := sqlbase.NewSchemaKey(desc.ParentID, newName).Key(p.execCfg.Codec)
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
	if err := p.writeDatabaseChange(ctx, db); err != nil {
		return err
	}

	// Write the change to the schema itself.
	return p.writeSchemaDescChange(ctx, desc, jobDesc)
}

func (n *alterSchemaNode) Next(params runParams) (bool, error) { return false, nil }
func (n *alterSchemaNode) Values() tree.Datums                 { return tree.Datums{} }
func (n *alterSchemaNode) Close(ctx context.Context)           {}
func (n *alterSchemaNode) ReadingOwnWrites()                   {}
