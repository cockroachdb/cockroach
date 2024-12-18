// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

var errEmptyColumnName = pgerror.New(pgcode.Syntax, "empty column name")

type renameColumnNode struct {
	zeroInputPlanNode
	n         *tree.RenameColumn
	tableDesc *tabledesc.Mutable
}

// RenameColumn renames the column.
// Privileges: CREATE on table.
//
//	notes: postgres requires CREATE on the table.
//	       mysql requires ALTER, CREATE, INSERT on the table.
func (p *planner) RenameColumn(ctx context.Context, n *tree.RenameColumn) (planNode, error) {
	if err := checkSchemaChangeEnabled(
		ctx,
		p.ExecCfg(),
		"RENAME COLUMN",
	); err != nil {
		return nil, err
	}

	// Check if table exists.
	_, tableDesc, err := p.ResolveMutableTableDescriptor(ctx, &n.Table, !n.IfExists, tree.ResolveRequireTableDesc)
	if err != nil {
		return nil, err
	}
	if tableDesc == nil {
		return newZeroNode(nil /* columns */), nil
	}

	if err := p.CheckPrivilege(ctx, tableDesc, privilege.CREATE); err != nil {
		return nil, err
	}

	// Disallow schema changes if this table's schema is locked.
	if err := checkSchemaChangeIsAllowed(tableDesc, n); err != nil {
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

	descChanged, err := params.p.renameColumn(params.ctx, tableDesc, n.n.Name, n.n.NewName)
	if err != nil {
		return err
	}

	if !descChanged {
		return nil
	}

	if err := validateDescriptor(ctx, p, tableDesc); err != nil {
		return err
	}

	return p.writeSchemaChange(
		ctx, tableDesc, descpb.InvalidMutationID, tree.AsStringWithFQNames(n.n, params.Ann()))
}

// findColumnToRename will return the column in tableDesc which is to be renamed
// from oldName to newName, provided that the rename is valid. If not, it will
// return an error.
func (p *planner) findColumnToRename(
	ctx context.Context, tableDesc *tabledesc.Mutable, oldName, newName tree.Name,
) (catalog.Column, error) {
	if newName == "" {
		return nil, errEmptyColumnName
	}

	col, err := catalog.MustFindColumnByTreeName(tableDesc, oldName)
	if err != nil {
		return nil, err
	}
	// Block renaming of system columns.
	if col.IsSystemColumn() {
		return nil, pgerror.Newf(
			pgcode.FeatureNotSupported,
			"cannot rename system column %q", col.ColName(),
		)
	}
	for _, tableRef := range tableDesc.DependedOnBy {
		found := false
		for _, colID := range tableRef.ColumnIDs {
			if colID == col.GetID() {
				found = true
			}
		}
		if found {
			return nil, p.dependentError(
				ctx, "column", oldName.String(), tableDesc.ParentID, tableRef.ID, "rename",
			)
		}
	}
	if oldName == newName {
		// Noop.
		return nil, nil
	}

	if col.IsInaccessible() {
		return nil, pgerror.Newf(
			pgcode.UndefinedColumn,
			"column %q is inaccessible and cannot be renamed",
			col.GetName(),
		)
	}
	// Understand if the active column already exists before checking for column
	// mutations to detect assertion failure of empty mutation and no column.
	// Otherwise we would have to make the above call twice.
	_, err = checkColumnDoesNotExist(tableDesc, newName)
	if err != nil {
		return nil, err
	}
	return col, nil
}

// renameColumn will rename the column in tableDesc from oldName to newName.
// If allowRenameOfShardColumn is false, this method will return an error if
// the column being renamed is a generated column for a hash sharded index.
func (p *planner) renameColumn(
	ctx context.Context, tableDesc *tabledesc.Mutable, oldName, newName tree.Name,
) (changed bool, err error) {
	col, err := p.findColumnToRename(ctx, tableDesc, oldName, newName)
	if err != nil || col == nil {
		return false, err
	}
	if tableDesc.IsShardColumn(col) {
		return false, pgerror.Newf(pgcode.ReservedName, "cannot rename shard column")
	}
	if err := tabledesc.RenameColumnInTable(tableDesc, col, newName, func(shardCol catalog.Column, newShardColName tree.Name) (bool, error) {
		if c, err := p.findColumnToRename(ctx, tableDesc, shardCol.ColName(), newShardColName); err != nil || c == nil {
			return false, err
		}
		return true, nil
	}); err != nil {
		return false, err
	}
	return true, nil
}

func (n *renameColumnNode) Next(runParams) (bool, error) { return false, nil }
func (n *renameColumnNode) Values() tree.Datums          { return tree.Datums{} }
func (n *renameColumnNode) Close(context.Context)        {}
