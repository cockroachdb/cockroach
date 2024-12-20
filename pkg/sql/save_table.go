// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

// saveTableNode is used for internal testing. It is a node that passes through
// input data but saves it in a table. The table can be used subsequently, e.g.
// to look at statistics.
//
// The node creates the table on startup. If the table exists, it errors out.
type saveTableNode struct {
	singleInputPlanNode

	target tree.TableName

	// Column names from the saved table. These could be different than the names
	// of the columns in the source plan. Note that saveTableNode passes through
	// the source plan's column names.
	colNames []string

	run struct {
		// vals accumulates a ValuesClause with the rows.
		vals tree.ValuesClause
	}
}

// saveTableInsertBatch is the number of rows per issued INSERT statement.
const saveTableInsertBatch = 100

func (p *planner) makeSaveTable(
	input planNode, target *tree.TableName, colNames []string,
) planNode {
	return &saveTableNode{
		singleInputPlanNode: singleInputPlanNode{input},
		target:              *target,
		colNames:            colNames,
	}
}

func (n *saveTableNode) startExec(params runParams) error {
	create := &tree.CreateTable{
		Table: n.target,
	}

	cols := planColumns(n.input)
	if len(n.colNames) != len(cols) {
		return errors.AssertionFailedf(
			"number of column names (%d) does not match number of columns (%d)",
			len(n.colNames), len(cols),
		)
	}
	for i := 0; i < len(cols); i++ {
		def := &tree.ColumnTableDef{
			Name: tree.Name(n.colNames[i]),
			Type: cols[i].Typ,
		}
		def.Nullable.Nullability = tree.SilentNull
		create.Defs = append(create.Defs, def)
	}

	_, err := params.p.ExtendedEvalContext().ExecCfg.InternalDB.
		Executor().Exec(
		params.ctx,
		"create save table",
		nil, /* txn */
		create.String(),
	)
	return err
}

// issue inserts rows into the target table of the saveTableNode.
func (n *saveTableNode) issue(params runParams) error {
	if v := &n.run.vals; len(v.Rows) > 0 {
		stmt := fmt.Sprintf("INSERT INTO %s %s", n.target.String(), v.String())
		if _, err := params.p.ExtendedEvalContext().ExecCfg.InternalDB.
			Executor().Exec(
			params.ctx,
			"insert into save table",
			nil, /* txn */
			stmt,
		); err != nil {
			return errors.Wrapf(err, "while running %s", stmt)
		}
		v.Rows = nil
	}
	return nil
}

// Next is part of the planNode interface.
func (n *saveTableNode) Next(params runParams) (bool, error) {
	res, err := n.input.Next(params)
	if err != nil {
		return res, err
	}
	if !res {
		// We are done. Insert any accumulated rows.
		err := n.issue(params)
		return false, err
	}
	row := n.input.Values()
	exprs := make(tree.Exprs, len(row))
	for i := range row {
		exprs[i] = row[i]
	}
	n.run.vals.Rows = append(n.run.vals.Rows, exprs)
	if len(n.run.vals.Rows) >= saveTableInsertBatch {
		if err := n.issue(params); err != nil {
			return false, err
		}
	}
	return true, nil
}

// Values is part of the planNode interface.
func (n *saveTableNode) Values() tree.Datums {
	return n.input.Values()
}

// Close is part of the planNode interface.
func (n *saveTableNode) Close(ctx context.Context) {
	n.input.Close(ctx)
}
