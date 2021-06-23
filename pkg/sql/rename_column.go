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

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemaexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

var errEmptyColumnName = pgerror.New(pgcode.Syntax, "empty column name")

type renameColumnNode struct {
	n         *tree.RenameColumn
	tableDesc *tabledesc.Mutable
}

// RenameColumn renames the column.
// Privileges: CREATE on table.
//   notes: postgres requires CREATE on the table.
//          mysql requires ALTER, CREATE, INSERT on the table.
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

	if err := validateDescriptor(ctx, p, tableDesc); err != nil {
		return err
	}

	return p.writeSchemaChange(
		ctx, tableDesc, descpb.InvalidMutationID, tree.AsStringWithFQNames(n.n, params.Ann()))
}

// renameColumn will rename the column in tableDesc from oldName to newName.
// If allowRenameOfShardColumn is false, this method will return an error if
// the column being renamed is a generated column for a hash sharded index.
func (p *planner) renameColumn(
	ctx context.Context,
	tableDesc *tabledesc.Mutable,
	oldName, newName *tree.Name,
	allowRenameOfShardColumn bool,
) (changed bool, err error) {
	if *newName == "" {
		return false, errEmptyColumnName
	}

	col, err := tableDesc.FindColumnWithName(*oldName)
	if err != nil {
		return false, err
	}

	for _, tableRef := range tableDesc.DependedOnBy {
		found := false
		for _, colID := range tableRef.ColumnIDs {
			if colID == col.GetID() {
				found = true
			}
		}
		if found {
			return false, p.dependentViewError(
				ctx, "column", oldName.String(), tableDesc.ParentID, tableRef.ID, "rename",
			)
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
	_, err = checkColumnDoesNotExist(tableDesc, *newName)
	if err != nil {
		return false, err
	}

	// Rename the column in CHECK constraints.
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
	for _, index := range tableDesc.PublicNonPrimaryIndexes() {
		if index.IsPartial() {
			newExpr, err := schemaexpr.RenameColumn(index.GetPredicate(), *oldName, *newName)
			if err != nil {
				return false, err
			}
			index.IndexDesc().Predicate = newExpr
		}
	}

	// Do all of the above renames inside check constraints, computed expressions,
	// and index predicates that are in mutations.
	for i := range tableDesc.Mutations {
		m := &tableDesc.Mutations[i]
		if constraint := m.GetConstraint(); constraint != nil {
			if constraint.ConstraintType == descpb.ConstraintToUpdate_CHECK ||
				constraint.ConstraintType == descpb.ConstraintToUpdate_NOT_NULL {
				var err error
				constraint.Check.Expr, err = schemaexpr.RenameColumn(constraint.Check.Expr, *oldName, *newName)
				if err != nil {
					return false, err
				}
			}
		} else if otherCol := m.GetColumn(); otherCol != nil {
			if otherCol.IsComputed() {
				newExpr, err := schemaexpr.RenameColumn(*otherCol.ComputeExpr, *oldName, *newName)
				if err != nil {
					return false, err
				}
				otherCol.ComputeExpr = &newExpr
			}
		} else if index := m.GetIndex(); index != nil {
			if index.IsPartial() {
				var err error
				index.Predicate, err = schemaexpr.RenameColumn(index.Predicate, *oldName, *newName)
				if err != nil {
					return false, err
				}
			}
		}
	}

	// Rename the column in hash-sharded index descriptors. Potentially rename the
	// shard column too if we haven't already done it.
	shardColumnsToRename := make(map[tree.Name]tree.Name) // map[oldShardColName]newShardColName
	maybeUpdateShardedDesc := func(shardedDesc *descpb.ShardedDescriptor) {
		if !shardedDesc.IsSharded {
			return
		}
		oldShardColName := tree.Name(tabledesc.GetShardColumnName(
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
			newName = tree.Name(tabledesc.GetShardColumnName(
				shardedDesc.ColumnNames, shardedDesc.ShardBuckets))
			shardColumnsToRename[oldShardColName] = newName
		}
		// Keep the shardedDesc name in sync with the column name.
		shardedDesc.Name = string(newName)
	}
	for _, idx := range tableDesc.NonDropIndexes() {
		maybeUpdateShardedDesc(&idx.IndexDesc().Sharded)
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

	// Rename the REGIONAL BY ROW column reference.
	if tableDesc.IsLocalityRegionalByRow() {
		rbrColName, err := tableDesc.GetRegionalByRowTableRegionColumnName()
		if err != nil {
			return false, err
		}
		if rbrColName == *oldName {
			tableDesc.SetTableLocalityRegionalByRow(*newName)
		}
	}
	return true, nil
}

func (n *renameColumnNode) Next(runParams) (bool, error) { return false, nil }
func (n *renameColumnNode) Values() tree.Datums          { return tree.Datums{} }
func (n *renameColumnNode) Close(context.Context)        {}
