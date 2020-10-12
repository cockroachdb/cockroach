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

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

type alterTableSetSchemaNode struct {
	newSchema string
	tableDesc *tabledesc.Mutable
	n         *tree.AlterTableSetSchema
}

// AlterTableSetSchema sets the schema for a table, view or sequence.
// Privileges: DROP on source table/view/sequence, CREATE on destination schema.
func (p *planner) AlterTableSetSchema(
	ctx context.Context, n *tree.AlterTableSetSchema,
) (planNode, error) {
	tn := n.Name.ToTableName()
	requiredTableKind := tree.ResolveAnyTableKind
	if n.IsView {
		requiredTableKind = tree.ResolveRequireViewDesc
	} else if n.IsSequence {
		requiredTableKind = tree.ResolveRequireSequenceDesc
	}
	tableDesc, err := p.ResolveMutableTableDescriptor(
		ctx, &tn, !n.IfExists, requiredTableKind)
	if err != nil {
		return nil, err
	}
	if tableDesc == nil {
		// Noop.
		return newZeroNode(nil /* columns */), nil
	}

	if err := checkViewMatchesMaterialized(tableDesc, n.IsView, n.IsMaterialized); err != nil {
		return nil, err
	}

	if tableDesc.Temporary {
		return nil, pgerror.Newf(pgcode.FeatureNotSupported,
			"cannot move objects into or out of temporary schemas")
	}

	// The user needs DROP privilege on the table to set the schema.
	err = p.CheckPrivilege(ctx, tableDesc, privilege.DROP)
	if err != nil {
		return nil, err
	}

	// Check if any views depend on this table/view. Because our views
	// are currently just stored as strings, they explicitly specify the name
	// of everything they depend on. Rather than trying to rewrite the view's
	// query with the new name, we simply disallow such renames for now.
	if len(tableDesc.DependedOnBy) > 0 {
		return nil, p.dependentViewError(
			ctx, tableDesc.TypeName(), tableDesc.Name, tableDesc.ParentID, tableDesc.DependedOnBy[0].ID,
			"set schema on",
		)
	}

	return &alterTableSetSchemaNode{
		newSchema: string(n.Schema),
		tableDesc: tableDesc,
		n:         n,
	}, nil
}

func (n *alterTableSetSchemaNode) startExec(params runParams) error {
	ctx := params.ctx
	p := params.p
	tableDesc := n.tableDesc
	schemaID := tableDesc.GetParentSchemaID()
	databaseID := tableDesc.GetParentID()

	desiredSchemaID, err := p.prepareSetSchema(ctx, tableDesc, n.newSchema)
	if err != nil {
		return err
	}

	// If the schema being changed to is the same as the current schema for the
	// table, do a no-op.
	if desiredSchemaID == schemaID {
		return nil
	}

	exists, _, err := catalogkv.LookupObjectID(
		ctx, p.txn, p.ExecCfg().Codec, databaseID, desiredSchemaID, tableDesc.Name,
	)
	if err == nil && exists {
		return pgerror.Newf(pgcode.DuplicateRelation,
			"relation %s already exists in schema %s", tableDesc.Name, n.newSchema)
	} else if err != nil {
		return err
	}

	renameDetails := descpb.NameInfo{
		ParentID:       databaseID,
		ParentSchemaID: schemaID,
		Name:           tableDesc.Name,
	}
	tableDesc.AddDrainingName(renameDetails)

	// Set the tableDesc's new schema id to the desired schema's id.
	tableDesc.SetParentSchemaID(desiredSchemaID)

	if err := p.writeSchemaChange(
		ctx, tableDesc, descpb.InvalidMutationID, tree.AsStringWithFQNames(n.n, params.Ann()),
	); err != nil {
		return err
	}

	newTbKey := catalogkv.MakeObjectNameKey(ctx, p.ExecCfg().Settings,
		databaseID, desiredSchemaID, tableDesc.Name)

	return p.writeNameKey(ctx, newTbKey, tableDesc.ID)
}

// ReadingOwnWrites implements the planNodeReadingOwnWrites interface.
// This is because SET SCHEMA performs multiple KV operations on descriptors
// and expects to see its own writes.
func (n *alterTableSetSchemaNode) ReadingOwnWrites() {}

func (n *alterTableSetSchemaNode) Next(runParams) (bool, error) { return false, nil }
func (n *alterTableSetSchemaNode) Values() tree.Datums          { return tree.Datums{} }
func (n *alterTableSetSchemaNode) Close(context.Context)        {}
