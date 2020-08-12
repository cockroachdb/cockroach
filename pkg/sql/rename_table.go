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

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

type renameTableNode struct {
	n            *tree.RenameTable
	oldTn, newTn *tree.TableName
	tableDesc    *sqlbase.MutableTableDescriptor
}

// RenameTable renames the table, view or sequence.
// Privileges: DROP on source table/view/sequence, CREATE on destination database.
//   Notes: postgres requires the table owner.
//          mysql requires ALTER, DROP on the original table, and CREATE, INSERT
//          on the new table (and does not copy privileges over).
func (p *planner) RenameTable(ctx context.Context, n *tree.RenameTable) (planNode, error) {
	oldTn := n.Name.ToTableName()
	newTn := n.NewName.ToTableName()
	toRequire := tree.ResolveRequireTableOrViewDesc
	if n.IsView {
		toRequire = tree.ResolveRequireViewDesc
	} else if n.IsSequence {
		toRequire = tree.ResolveRequireSequenceDesc
	}

	tableDesc, err := p.ResolveMutableTableDescriptor(ctx, &oldTn, !n.IfExists, toRequire)
	if err != nil {
		return nil, err
	}
	if tableDesc == nil {
		// Noop.
		return newZeroNode(nil /* columns */), nil
	}

	if tableDesc.State != descpb.TableDescriptor_PUBLIC {
		return nil, sqlbase.NewUndefinedRelationError(&oldTn)
	}

	if err := p.CheckPrivilege(ctx, tableDesc, privilege.DROP); err != nil {
		return nil, err
	}

	// Check if any views depend on this table/view. Because our views
	// are currently just stored as strings, they explicitly specify the name
	// of everything they depend on. Rather than trying to rewrite the view's
	// query with the new name, we simply disallow such renames for now.
	if len(tableDesc.DependedOnBy) > 0 {
		return nil, p.dependentViewError(
			ctx, tableDesc.TypeName(), oldTn.String(),
			tableDesc.ParentID, tableDesc.DependedOnBy[0].ID, "rename",
		)
	}

	return &renameTableNode{n: n, oldTn: &oldTn, newTn: &newTn, tableDesc: tableDesc}, nil
}

// ReadingOwnWrites implements the planNodeReadingOwnWrites interface.
// This is because RENAME DATABASE performs multiple KV operations on descriptors
// and expects to see its own writes.
func (n *renameTableNode) ReadingOwnWrites() {}

func (n *renameTableNode) startExec(params runParams) error {
	p := params.p
	ctx := params.ctx
	tableDesc := n.tableDesc
	oldTn := n.oldTn
	prevDBID := tableDesc.ParentID

	var targetDbDesc *UncachedDatabaseDescriptor
	// If the target new name has no qualifications, then assume that the table
	// is intended to be renamed into the same database and schema.
	newTn := n.newTn
	if !newTn.ExplicitSchema && !newTn.ExplicitCatalog {
		newTn.ObjectNamePrefix = oldTn.ObjectNamePrefix
		var err error
		targetDbDesc, err = p.ResolveUncachedDatabaseByName(ctx, string(oldTn.CatalogName), true /* required */)
		if err != nil {
			return err
		}
	} else {
		// Otherwise, resolve the new qualified name of the table. We are in the
		// process of deprecating qualified rename targets, so issue a notice.
		// TODO (rohany): Convert this to take in an unqualified name after 20.2
		//  is released (#51445).
		params.p.noticeSender.AppendNotice(
			errors.WithHintf(
				pgnotice.Newf("renaming tables with a qualification is deprecated"),
				"use ALTER TABLE %s RENAME TO %s instead",
				n.n.Name.String(),
				newTn.Table(),
			),
		)

		newUn := newTn.ToUnresolvedObjectName()
		var prefix tree.ObjectNamePrefix
		var err error
		targetDbDesc, prefix, err = p.ResolveUncachedDatabase(ctx, newUn)
		if err != nil {
			return err
		}
		newTn.ObjectNamePrefix = prefix
	}

	if err := p.CheckPrivilege(ctx, targetDbDesc, privilege.CREATE); err != nil {
		return err
	}

	// Disable renaming objects between schemas of the same database.
	if newTn.Catalog() == oldTn.Catalog() && newTn.Schema() != oldTn.Schema() {
		return errors.WithHint(
			pgerror.Newf(pgcode.InvalidName, "cannot change schema of table with RENAME"),
			"use ALTER TABLE ... SET SCHEMA instead",
		)
	}

	// oldTn and newTn are already normalized, so we can compare directly here.
	if oldTn.Catalog() == newTn.Catalog() &&
		oldTn.Schema() == newTn.Schema() &&
		oldTn.Table() == newTn.Table() {
		// Noop.
		return nil
	}

	exists, id, err := catalogkv.LookupPublicTableID(
		params.ctx, params.p.txn, p.ExecCfg().Codec, targetDbDesc.GetID(), newTn.Table(),
	)
	if err == nil && exists {
		// Try and see what kind of object we collided with.
		desc, err := catalogkv.GetAnyDescriptorByID(
			params.ctx, params.p.txn, p.ExecCfg().Codec, id, catalogkv.Immutable)
		if err != nil {
			return sqlbase.WrapErrorWhileConstructingObjectAlreadyExistsErr(err)
		}
		return sqlbase.MakeObjectAlreadyExistsError(desc.DescriptorProto(), newTn.Table())
	} else if err != nil {
		return err
	}

	tableDesc.SetName(newTn.Table())
	tableDesc.ParentID = targetDbDesc.GetID()

	if err := tableDesc.Validate(ctx, p.txn, p.ExecCfg().Codec); err != nil {
		return err
	}

	descID := tableDesc.GetID()
	parentSchemaID := tableDesc.GetParentSchemaID()

	renameDetails := descpb.NameInfo{
		ParentID:       prevDBID,
		ParentSchemaID: parentSchemaID,
		Name:           oldTn.Table()}
	tableDesc.AddDrainingName(renameDetails)

	if err := p.writeSchemaChange(
		ctx, tableDesc, descpb.InvalidMutationID, tree.AsStringWithFQNames(n.n, params.Ann()),
	); err != nil {
		return err
	}

	if err == nil && exists {
		// Try and see what kind of object we collided with.
		desc, err := catalogkv.GetAnyDescriptorByID(params.ctx, params.p.txn, p.ExecCfg().Codec, id, catalogkv.Immutable)
		if err != nil {
			return sqlbase.WrapErrorWhileConstructingObjectAlreadyExistsErr(err)
		}
		return sqlbase.MakeObjectAlreadyExistsError(desc.DescriptorProto(), newTn.Table())
	} else if err != nil {
		return err
	}

	newTbKey := catalogkv.MakeObjectNameKey(ctx, params.ExecCfg().Settings,
		targetDbDesc.GetID(), tableDesc.GetParentSchemaID(), newTn.Table())

	return p.writeNameKey(ctx, newTbKey, descID)
}

func (n *renameTableNode) Next(runParams) (bool, error) { return false, nil }
func (n *renameTableNode) Values() tree.Datums          { return tree.Datums{} }
func (n *renameTableNode) Close(context.Context)        {}

// TODO(a-robinson): Support renaming objects depended on by views once we have
// a better encoding for view queries (#10083).
func (p *planner) dependentViewError(
	ctx context.Context, typeName, objName string, parentID, viewID descpb.ID, op string,
) error {
	viewDesc, err := catalogkv.MustGetTableDescByID(ctx, p.txn, p.ExecCfg().Codec, viewID)
	if err != nil {
		return err
	}
	viewName := viewDesc.Name
	if viewDesc.ParentID != parentID {
		viewFQName, err := p.getQualifiedTableName(ctx, viewDesc)
		if err != nil {
			log.Warningf(ctx, "unable to retrieve name of view %d: %v", viewID, err)
			return sqlbase.NewDependentObjectErrorf(
				"cannot %s %s %q because a view depends on it",
				op, typeName, objName)
		}
		viewName = viewFQName.FQString()
	}
	return errors.WithHintf(
		sqlbase.NewDependentObjectErrorf("cannot %s %s %q because view %q depends on it",
			op, typeName, objName, viewName),
		"you can drop %s instead.", viewName)
}

// writeNameKey writes a name key to a batch and runs the batch.
func (p *planner) writeNameKey(ctx context.Context, key sqlbase.DescriptorKey, ID descpb.ID) error {
	marshalledKey := key.Key(p.ExecCfg().Codec)
	b := &kv.Batch{}
	if p.extendedEvalCtx.Tracing.KVTracingEnabled() {
		log.VEventf(ctx, 2, "CPut %s -> %d", marshalledKey, ID)
	}
	b.CPut(marshalledKey, ID, nil)

	return p.txn.Run(ctx, b)
}
