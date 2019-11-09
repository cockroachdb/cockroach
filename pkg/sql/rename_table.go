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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
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
	toRequire := ResolveRequireTableOrViewDesc
	if n.IsView {
		toRequire = ResolveRequireViewDesc
	} else if n.IsSequence {
		toRequire = ResolveRequireSequenceDesc
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

func (n *renameTableNode) startExec(params runParams) error {
	p := params.p
	ctx := params.ctx
	oldTn := n.oldTn
	newTn := n.newTn
	tableDesc := n.tableDesc

	prevDbDesc, err := p.ResolveUncachedDatabase(ctx, oldTn)
	if err != nil {
		return err
	}

	// Check if target database exists.
	// We also look at uncached descriptors here.
	targetDbDesc, err := p.ResolveUncachedDatabase(ctx, newTn)
	if err != nil {
		return err
	}

	if err := p.CheckPrivilege(ctx, targetDbDesc, privilege.CREATE); err != nil {
		return err
	}

	// oldTn and newTn are already normalized, so we can compare directly here.
	if oldTn.Catalog() == newTn.Catalog() &&
		oldTn.Schema() == newTn.Schema() &&
		oldTn.Table() == newTn.Table() {
		// Noop.
		return nil
	}

	tableDesc.SetName(newTn.Table())
	tableDesc.ParentID = targetDbDesc.ID

	newTbKey := sqlbase.MakePublicTableNameKey(ctx, params.ExecCfg().Settings,
		targetDbDesc.ID, newTn.Table()).Key()

	if err := tableDesc.Validate(ctx, p.txn); err != nil {
		return err
	}

	descID := tableDesc.GetID()

	renameDetails := sqlbase.TableDescriptor_NameInfo{
		ParentID: prevDbDesc.ID,
		Name:     oldTn.Table()}
	tableDesc.DrainingNames = append(tableDesc.DrainingNames, renameDetails)
	if err := p.writeSchemaChange(ctx, tableDesc, sqlbase.InvalidMutationID); err != nil {
		return err
	}

	// We update the descriptor to the new name, but also leave the mapping of the
	// old name to the id, so that the name is not reused until the schema changer
	// has made sure it's not in use any more.
	b := &client.Batch{}
	if p.extendedEvalCtx.Tracing.KVTracingEnabled() {
		log.VEventf(ctx, 2, "CPut %s -> %d", newTbKey, descID)
	}
	if err := writeDescToBatch(ctx, p.extendedEvalCtx.Tracing.KVTracingEnabled(), p.EvalContext().Settings, b, descID, tableDesc.TableDesc()); err != nil {
		return err
	}
	b.CPut(newTbKey, descID, nil)

	if err := p.txn.Run(ctx, b); err != nil {
		if _, ok := err.(*roachpb.ConditionFailedError); ok {
			return sqlbase.NewRelationAlreadyExistsError(newTn.Table())
		}
		return err
	}

	return nil
}

func (n *renameTableNode) Next(runParams) (bool, error) { return false, nil }
func (n *renameTableNode) Values() tree.Datums          { return tree.Datums{} }
func (n *renameTableNode) Close(context.Context)        {}

// TODO(a-robinson): Support renaming objects depended on by views once we have
// a better encoding for view queries (#10083).
func (p *planner) dependentViewRenameError(
	ctx context.Context, typeName, objName string, parentID, viewID sqlbase.ID,
) error {
	viewDesc, err := sqlbase.GetTableDescFromID(ctx, p.txn, viewID)
	if err != nil {
		return err
	}
	viewName := viewDesc.Name
	if viewDesc.ParentID != parentID {
		var err error
		viewName, err = p.getQualifiedTableName(ctx, viewDesc)
		if err != nil {
			log.Warningf(ctx, "unable to retrieve name of view %d: %v", viewID, err)
			msg := fmt.Sprintf("cannot rename %s %q because a view depends on it",
				typeName, objName)
			return sqlbase.NewDependentObjectError(msg)
		}
	}
	msg := fmt.Sprintf("cannot rename %s %q because view %q depends on it",
		typeName, objName, viewName)
	hint := fmt.Sprintf("you can drop %s instead.", viewName)
	return sqlbase.NewDependentObjectErrorWithHint(msg, hint)
}
