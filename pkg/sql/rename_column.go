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

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

var errEmptyColumnName = pgerror.New(pgcode.Syntax, "empty column name")

type renameColumnNode struct {
	n         *tree.RenameColumn
	tableDesc *sqlbase.MutableTableDescriptor
}

// RenameColumn renames the column.
// Privileges: CREATE on table.
//   notes: postgres requires CREATE on the table.
//          mysql requires ALTER, CREATE, INSERT on the table.
func (p *planner) RenameColumn(ctx context.Context, n *tree.RenameColumn) (planNode, error) {
	// Check if table exists.
	tableDesc, err := p.ResolveMutableTableDescriptor(ctx, &n.Table, !n.IfExists, ResolveRequireTableDesc)
	if err != nil {
		return nil, err
	}
	if tableDesc == nil {
		return newZeroNode(nil /* columns */), nil
	}

	if err := p.CheckPrivilege(ctx, tableDesc, privilege.CREATE); err != nil {
		return nil, err
	}

	return &renameColumnNode{n: n, tableDesc: tableDesc}, nil
}

// ReadingOwnWrites implements the planNodeReadingOwnWrites interface.
// This is because RENAME COLUMN performs multiple KV operations on descriptors
// and expects to see its own writes.
func (n *renameColumnNode) ReadingOwnWrites() {}

func (n *renameColumnNode) startExec(params runParams) error {
	p := params.p
	ctx := params.ctx
	tableDesc := n.tableDesc

	descChanged, err := params.p.renameColumn(params.ctx, tableDesc, &n.n.Name, &n.n.NewName)
	if err != nil {
		return err
	}

	if !descChanged {
		return nil
	}

	if err := tableDesc.Validate(ctx, p.txn); err != nil {
		return err
	}

	return p.writeSchemaChange(
		ctx, tableDesc, sqlbase.InvalidMutationID, tree.AsStringWithFQNames(n.n, params.Ann()))
}

func (p *planner) renameColumn(
	ctx context.Context, tableDesc *sqlbase.MutableTableDescriptor, oldName, newName *tree.Name,
) (bool, error) {
	if *newName == "" {
		return false, errEmptyColumnName
	}

	col, _, err := tableDesc.FindColumnByName(*oldName)
	if err != nil {
		return false, err
	}

	for _, tableRef := range tableDesc.DependedOnBy {
		found := false
		for _, colID := range tableRef.ColumnIDs {
			if colID == col.ID {
				found = true
			}
		}
		if found {
			return false, p.dependentViewRenameError(
				ctx, "column", oldName.String(), tableDesc.ParentID, tableRef.ID)
		}
	}

	if *oldName == *newName {
		// Noop.
		return false, nil
	}

	if _, _, err := tableDesc.FindColumnByName(*newName); err == nil {
		return false, fmt.Errorf("column name %q already exists", tree.ErrString(newName))
	}

	if err := tableDesc.RenameColumn(col, oldName.String(), newName.String()); err != nil {
		return false, err
	}

	return true, nil
}

func (n *renameColumnNode) Next(runParams) (bool, error) { return false, nil }
func (n *renameColumnNode) Values() tree.Datums          { return tree.Datums{} }
func (n *renameColumnNode) Close(context.Context)        {}
