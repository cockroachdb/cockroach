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
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

var errEmptyColumnName = pgerror.NewError(pgerror.CodeSyntaxError, "empty column name")

type renameColumnNode struct {
	n         *tree.RenameColumn
	tableDesc *sqlbase.TableDescriptor
}

// RenameColumn renames the column.
// Privileges: CREATE on table.
//   notes: postgres requires CREATE on the table.
//          mysql requires ALTER, CREATE, INSERT on the table.
func (p *planner) RenameColumn(ctx context.Context, n *tree.RenameColumn) (planNode, error) {
	// Check if table exists.
	tableDesc, err := p.ResolveMutableTableDescriptor(ctx, &n.Table, !n.IfExists, requireTableDesc)
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

func (n *renameColumnNode) startExec(params runParams) error {
	p := params.p
	ctx := params.ctx
	tableDesc := n.tableDesc

	if n.n.NewName == "" {
		return errEmptyColumnName
	}

	col, _, err := tableDesc.FindColumnByName(n.n.Name)
	// n.n.IfExists only applies to table, no need to check here.
	if err != nil {
		return err
	}

	for _, tableRef := range tableDesc.DependedOnBy {
		found := false
		for _, colID := range tableRef.ColumnIDs {
			if colID == col.ID {
				found = true
			}
		}
		if found {
			return p.dependentViewRenameError(
				ctx, "column", n.n.Name.String(), tableDesc.ParentID, tableRef.ID)
		}
	}

	if n.n.Name == n.n.NewName {
		// Noop.
		return nil
	}

	if _, _, err := tableDesc.FindColumnByName(n.n.NewName); err == nil {
		return fmt.Errorf("column name %q already exists", string(n.n.NewName))
	}

	preFn := func(expr tree.Expr) (err error, recurse bool, newExpr tree.Expr) {
		if vBase, ok := expr.(tree.VarName); ok {
			v, err := vBase.NormalizeVarName()
			if err != nil {
				return err, false, nil
			}
			if c, ok := v.(*tree.ColumnItem); ok {
				if string(c.ColumnName) == string(n.n.Name) {
					c.ColumnName = n.n.NewName
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
			return err
		}
	}

	// Rename the column in computed columns.
	for i := range tableDesc.Columns {
		if tableDesc.Columns[i].IsComputed() {
			newExpr, err := renameIn(*tableDesc.Columns[i].ComputeExpr)
			if err != nil {
				return err
			}
			tableDesc.Columns[i].ComputeExpr = &newExpr
		}
	}

	// Rename the column in the indexes.
	tableDesc.RenameColumnDescriptor(col, string(n.n.NewName))

	if err := tableDesc.Validate(ctx, p.txn, p.EvalContext().Settings); err != nil {
		return err
	}

	return p.writeSchemaChange(ctx, tableDesc, sqlbase.InvalidMutationID)
}

func (n *renameColumnNode) Next(runParams) (bool, error) { return false, nil }
func (n *renameColumnNode) Values() tree.Datums          { return tree.Datums{} }
func (n *renameColumnNode) Close(context.Context)        {}
