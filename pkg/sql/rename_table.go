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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/resolver"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
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
	toRequire := resolver.ResolveRequireTableOrViewDesc
	if n.IsView {
		toRequire = resolver.ResolveRequireViewDesc
	} else if n.IsSequence {
		toRequire = resolver.ResolveRequireSequenceDesc
	}

	tableDesc, err := p.ResolveMutableTableDescriptor(ctx, &oldTn, !n.IfExists, toRequire)
	if err != nil {
		return nil, err
	}
	if tableDesc == nil {
		// Noop.
		return newZeroNode(nil /* columns */), nil
	}

	if tableDesc.State != sqlbase.TableDescriptor_PUBLIC {
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
		return nil, p.dependentViewRenameError(
			ctx, tableDesc.TypeName(), oldTn.String(), tableDesc.ParentID, tableDesc.DependedOnBy[0].ID)
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
	oldTn := n.oldTn
	newTn := n.newTn
	tableDesc := n.tableDesc

	oldUn := oldTn.ToUnresolvedObjectName()
	prevDbDesc, prefix, err := p.ResolveUncachedDatabase(ctx, oldUn)
	if err != nil {
		return err
	}
	oldTn.ObjectNamePrefix = prefix

	// Check if target database exists.
	// We also look at uncached descriptors here.
	newUn := newTn.ToUnresolvedObjectName()
	targetDbDesc, prefix, err := p.ResolveUncachedDatabase(ctx, newUn)
	if err != nil {
		return err
	}
	newTn.ObjectNamePrefix = prefix

	if err := p.CheckPrivilege(ctx, targetDbDesc, privilege.CREATE); err != nil {
		return err
	}

	isNewSchemaTemp, _, err := temporarySchemaSessionID(newTn.Schema())
	if err != nil {
		return err
	}

	if newTn.ExplicitSchema && !isNewSchemaTemp && tableDesc.Temporary {
		return pgerror.New(
			pgcode.FeatureNotSupported,
			"cannot convert a temporary table to a persistent table during renames",
		)
	}

	// oldTn and newTn are already normalized, so we can compare directly here.
	if oldTn.Catalog() == newTn.Catalog() &&
		oldTn.Schema() == newTn.Schema() &&
		oldTn.Table() == newTn.Table() {
		// Noop.
		return nil
	}

	tableDesc.SetName(newTn.Table())
	tableDesc.ParentID = targetDbDesc.GetID()

	newTbKey := sqlbase.MakeObjectNameKey(ctx, params.ExecCfg().Settings,
		targetDbDesc.GetID(), tableDesc.GetParentSchemaID(), newTn.Table()).Key(p.ExecCfg().Codec)

	if err := tableDesc.Validate(ctx, p.txn, p.ExecCfg().Codec); err != nil {
		return err
	}

	descID := tableDesc.GetID()
	parentSchemaID := tableDesc.GetParentSchemaID()

	renameDetails := sqlbase.NameInfo{
		ParentID:       prevDbDesc.GetID(),
		ParentSchemaID: parentSchemaID,
		Name:           oldTn.Table()}
	tableDesc.DrainingNames = append(tableDesc.DrainingNames, renameDetails)
	if err := p.writeSchemaChange(
		ctx, tableDesc, sqlbase.InvalidMutationID, tree.AsStringWithFQNames(n.n, params.Ann()),
	); err != nil {
		return err
	}

	// We update the descriptor to the new name, but also leave the mapping of the
	// old name to the id, so that the name is not reused until the schema changer
	// has made sure it's not in use any more.
	b := &kv.Batch{}
	if p.extendedEvalCtx.Tracing.KVTracingEnabled() {
		log.VEventf(ctx, 2, "CPut %s -> %d", newTbKey, descID)
	}
	err = catalogkv.WriteDescToBatch(ctx, p.extendedEvalCtx.Tracing.KVTracingEnabled(),
		p.EvalContext().Settings, b, p.ExecCfg().Codec, descID, tableDesc)
	if err != nil {
		return err
	}

	exists, id, err := sqlbase.LookupPublicTableID(
		params.ctx, params.p.txn, p.ExecCfg().Codec, targetDbDesc.GetID(), newTn.Table(),
	)
	if err == nil && exists {
		// Try and see what kind of object we collided with.
		desc, err := catalogkv.GetDescriptorByID(params.ctx, params.p.txn, p.ExecCfg().Codec, id)
		if err != nil {
			return err
		}
		return sqlbase.MakeObjectAlreadyExistsError(desc.DescriptorProto(), newTn.Table())
	} else if err != nil {
		return err
	}

	b.CPut(newTbKey, descID, nil)
	return p.txn.Run(ctx, b)
}

func (n *renameTableNode) Next(runParams) (bool, error) { return false, nil }
func (n *renameTableNode) Values() tree.Datums          { return tree.Datums{} }
func (n *renameTableNode) Close(context.Context)        {}

// TODO(a-robinson): Support renaming objects depended on by views once we have
// a better encoding for view queries (#10083).
func (p *planner) dependentViewRenameError(
	ctx context.Context, typeName, objName string, parentID, viewID sqlbase.ID,
) error {
	viewDesc, err := sqlbase.GetTableDescFromID(ctx, p.txn, p.ExecCfg().Codec, viewID)
	if err != nil {
		return err
	}
	viewName := viewDesc.Name
	if viewDesc.ParentID != parentID {
		var err error
		viewName, err = p.getQualifiedTableName(ctx, viewDesc)
		if err != nil {
			log.Warningf(ctx, "unable to retrieve name of view %d: %v", viewID, err)
			return sqlbase.NewDependentObjectErrorf(
				"cannot rename %s %q because a view depends on it",
				typeName, objName)
		}
	}
	return errors.WithHintf(
		sqlbase.NewDependentObjectErrorf("cannot rename %s %q because view %q depends on it",
			typeName, objName, viewName),
		"you can drop %s instead.", viewName)
}
