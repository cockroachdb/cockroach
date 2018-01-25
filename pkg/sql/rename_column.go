// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package sql

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/pkg/errors"
)

var errEmptyColumnName = errors.New("empty column name")

// RenameColumn renames the column.
// Privileges: CREATE on table.
//   notes: postgres requires CREATE on the table.
//          mysql requires ALTER, CREATE, INSERT on the table.
func (p *planner) RenameColumn(ctx context.Context, n *tree.RenameColumn) (planNode, error) {
	// Check if table exists.
	tn, err := n.Table.NormalizeWithDatabaseName(p.SessionData().Database)
	if err != nil {
		return nil, err
	}
	tableDesc, err := getTableDesc(ctx, p.txn, p.getVirtualTabler(), tn)
	if err != nil {
		return nil, err
	}
	if tableDesc == nil {
		if n.IfExists {
			// Noop.
			return &zeroNode{}, nil
		}
		// Key does not exist, but we want it to: error out.
		return nil, fmt.Errorf("table %q does not exist", tn.Table())
	}

	if err := p.CheckPrivilege(ctx, tableDesc, privilege.CREATE); err != nil {
		return nil, err
	}

	if n.NewName == "" {
		return nil, errEmptyColumnName
	}

	col, _, err := tableDesc.FindColumnByName(n.Name)
	// n.IfExists only applies to table, no need to check here.
	if err != nil {
		return nil, err
	}

	for _, tableRef := range tableDesc.DependedOnBy {
		found := false
		for _, colID := range tableRef.ColumnIDs {
			if colID == col.ID {
				found = true
			}
		}
		if found {
			return nil, p.dependentViewRenameError(
				ctx, "column", n.Name.String(), tableDesc.ParentID, tableRef.ID)
		}
	}

	if n.Name == n.NewName {
		// Noop.
		return &zeroNode{}, nil
	}

	if _, _, err := tableDesc.FindColumnByName(n.NewName); err == nil {
		return nil, fmt.Errorf("column name %q already exists", string(n.NewName))
	}

	preFn := func(expr tree.Expr) (err error, recurse bool, newExpr tree.Expr) {
		if vBase, ok := expr.(tree.VarName); ok {
			v, err := vBase.NormalizeVarName()
			if err != nil {
				return err, false, nil
			}
			if c, ok := v.(*tree.ColumnItem); ok {
				if string(c.ColumnName) == string(n.Name) {
					c.ColumnName = n.NewName
				}
			}
			return nil, false, v
		}
		return nil, true, expr
	}

	renameIn := func(expression string) (string, error) {
		parsed, err := parser.ParseExpr(expression)
		if err != nil {
			return "", err
		}

		renamed, err := tree.SimpleVisit(parsed, preFn)
		if err != nil {
			return "", err
		}

		return renamed.String(), nil
	}

	// Rename the column in CHECK constraints.
	for i := range tableDesc.Checks {
		var err error
		tableDesc.Checks[i].Expr, err = renameIn(tableDesc.Checks[i].Expr)
		if err != nil {
			return nil, err
		}
	}

	// Rename the column in computed columns.
	for i := range tableDesc.Columns {
		if tableDesc.Columns[i].ComputeExpr != nil {
			newExpr, err := renameIn(*tableDesc.Columns[i].ComputeExpr)
			if err != nil {
				return nil, err
			}
			tableDesc.Columns[i].ComputeExpr = &newExpr
		}
	}

	// Rename the column in the indexes.
	tableDesc.RenameColumnDescriptor(col, string(n.NewName))

	if err := tableDesc.SetUpVersion(); err != nil {
		return nil, err
	}

	descKey := sqlbase.MakeDescMetadataKey(tableDesc.GetID())
	if err := tableDesc.Validate(ctx, p.txn); err != nil {
		return nil, err
	}
	if err := p.txn.Put(ctx, descKey, sqlbase.WrapDescriptor(tableDesc)); err != nil {
		return nil, err
	}
	p.notifySchemaChange(tableDesc, sqlbase.InvalidMutationID)
	return &zeroNode{}, nil
}
