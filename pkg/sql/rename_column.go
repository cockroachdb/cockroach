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

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/resolver"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemaexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/errors"
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
	tableDesc, err := p.ResolveMutableTableDescriptor(ctx, &n.Table, !n.IfExists, resolver.ResolveRequireTableDesc)
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

	const allowRenameOfShardColumn = false
	descChanged, err := params.p.renameColumn(params.ctx, tableDesc, &n.n.Name,
		&n.n.NewName, allowRenameOfShardColumn)
	if err != nil {
		return err
	}

	if !descChanged {
		return nil
	}

	if err := tableDesc.Validate(ctx, p.txn, p.ExecCfg().Codec); err != nil {
		return err
	}

	return p.writeSchemaChange(
		ctx, tableDesc, sqlbase.InvalidMutationID, tree.AsStringWithFQNames(n.n, params.Ann()))
}

// renameColumn will rename the column in tableDesc from oldName to newName.
// If allowRenameOfShardColumn is false, this method will return an error if
// the column being renamed is a generated column for a hash sharded index.
func (p *planner) renameColumn(
	ctx context.Context,
	tableDesc *sqlbase.MutableTableDescriptor,
	oldName, newName *tree.Name,
	allowRenameOfShardColumn bool,
) (changed bool, err error) {
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
	isShardColumn := tableDesc.IsShardColumn(col)
	if isShardColumn && !allowRenameOfShardColumn {
		return false, pgerror.Newf(pgcode.ReservedName, "cannot rename shard column")
	}
	// Understand if the active column already exists before checking for column
	// mutations to detect assertion failure of empty mutation and no column.
	// Otherwise we would have to make the above call twice.
	_, columnNotFoundErr := tableDesc.FindActiveColumnByName(string(*newName))
	if m := tableDesc.FindColumnMutationByName(*newName); m != nil {
		switch m.Direction {
		case sqlbase.DescriptorMutation_ADD:
			return false, pgerror.Newf(pgcode.DuplicateColumn,
				"duplicate: column %q in the middle of being added, not yet public",
				col.Name)
		case sqlbase.DescriptorMutation_DROP:
			return false, pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
				"column %q being dropped, try again later", col.Name)
		default:
			if columnNotFoundErr != nil {
				return false, errors.AssertionFailedf(
					"mutation in state %s, direction %s, and no column descriptor",
					errors.Safe(m.State), errors.Safe(m.Direction))
			}
		}
	}
	if columnNotFoundErr == nil {
		return false, sqlbase.NewColumnAlreadyExistsError(tree.ErrString(newName), tableDesc.Name)
	}

	// Rename the column in CHECK constraints.
	// Renaming columns that are being referenced by checks that are being added is not allowed.
	for i := range tableDesc.Checks {
		var err error
		tableDesc.Checks[i].Expr, err = schemaexpr.RenameColumn(tableDesc.Checks[i].Expr, *oldName, *newName)
		if err != nil {
			return false, err
		}
	}

	// Rename the column in computed columns.
	for i := range tableDesc.Columns {
		if otherCol := &tableDesc.Columns[i]; otherCol.IsComputed() {
			newExpr, err := schemaexpr.RenameColumn(*otherCol.ComputeExpr, *oldName, *newName)
			if err != nil {
				return false, err
			}
			otherCol.ComputeExpr = &newExpr
		}
	}

	// Rename the column in partial index predicates.
	for i := range tableDesc.Indexes {
		if index := &tableDesc.Indexes[i]; index.IsPartial() {
			newExpr, err := schemaexpr.RenameColumn(index.Predicate, *oldName, *newName)
			if err != nil {
				return false, err
			}
			index.Predicate = newExpr
		}
	}

	// Rename the column in hash-sharded index descriptors. Potentially rename the
	// shard column too if we haven't already done it.
	shardColumnsToRename := make(map[tree.Name]tree.Name) // map[oldShardColName]newShardColName
	maybeUpdateShardedDesc := func(shardedDesc *sqlbase.ShardedDescriptor) {
		if !shardedDesc.IsSharded {
			return
		}
		oldShardColName := tree.Name(sqlbase.GetShardColumnName(
			shardedDesc.ColumnNames, shardedDesc.ShardBuckets))
		var changed bool
		for i, c := range shardedDesc.ColumnNames {
			if c == string(*oldName) {
				changed = true
				shardedDesc.ColumnNames[i] = string(*newName)
			}
		}
		if !changed {
			return
		}
		newName, alreadyRenamed := shardColumnsToRename[oldShardColName]
		if !alreadyRenamed {
			newName = tree.Name(sqlbase.GetShardColumnName(
				shardedDesc.ColumnNames, shardedDesc.ShardBuckets))
			shardColumnsToRename[oldShardColName] = newName
		}
		// Keep the shardedDesc name in sync with the column name.
		shardedDesc.Name = string(newName)
	}
	for _, idx := range tableDesc.AllNonDropIndexes() {
		maybeUpdateShardedDesc(&idx.Sharded)
	}

	// Rename the column in the indexes.
	tableDesc.RenameColumnDescriptor(col, string(*newName))

	// Rename any shard columns which need to be renamed because their name was
	// based on this column.
	for oldShardColName, newShardColName := range shardColumnsToRename {
		// Recursively call p.renameColumn. We don't need to worry about deeper than
		// one recursive call because shard columns cannot refer to each other.
		const allowRenameOfShardColumn = true
		_, err = p.renameColumn(ctx, tableDesc, &oldShardColName, &newShardColName,
			allowRenameOfShardColumn)
		if err != nil {
			return false, err
		}
	}

	return true, nil
}

func (n *renameColumnNode) Next(runParams) (bool, error) { return false, nil }
func (n *renameColumnNode) Values() tree.Datums          { return tree.Datums{} }
func (n *renameColumnNode) Close(context.Context)        {}
