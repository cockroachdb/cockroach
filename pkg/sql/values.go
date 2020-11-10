// Copyright 2015 The Cockroach Authors.
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
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/rowcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

type valuesNode struct {
	// Note: the columns can be renamed in place (see planMutableColumns).
	columns colinfo.ResultColumns
	tuples  [][]tree.TypedExpr

	// specifiedInQuery is set if the valuesNode represents a literal
	// relational expression that was present in the original SQL text,
	// as opposed to e.g. a valuesNode resulting from the expansion of
	// a vtable value generator. This changes distsql physical planning.
	specifiedInQuery bool

	valuesRun
}

// Values implements the VALUES clause.
func (p *planner) Values(
	ctx context.Context, origN tree.Statement, desiredTypes []*types.T,
) (planNode, error) {
	v := &valuesNode{
		specifiedInQuery: true,
	}

	// If we have names, extract them.
	var n *tree.ValuesClause
	switch t := origN.(type) {
	case *tree.ValuesClauseWithNames:
		n = &t.ValuesClause
	case *tree.ValuesClause:
		n = t
	default:
		return nil, errors.AssertionFailedf("unhandled case in values: %T %v", origN, origN)
	}

	if len(n.Rows) == 0 {
		return v, nil
	}

	numCols := len(n.Rows[0])

	v.tuples = make([][]tree.TypedExpr, 0, len(n.Rows))
	tupleBuf := make([]tree.TypedExpr, len(n.Rows)*numCols)

	v.columns = make(colinfo.ResultColumns, 0, numCols)

	// We need to save and restore the previous value of the field in
	// semaCtx in case we are recursively called within a subquery
	// context.
	defer p.semaCtx.Properties.Restore(p.semaCtx.Properties)

	// Ensure there are no special functions in the clause.
	p.semaCtx.Properties.Require("VALUES", tree.RejectSpecial)

	for num, tuple := range n.Rows {
		if a, e := len(tuple), numCols; a != e {
			return nil, newValuesListLenErr(e, a)
		}

		// Chop off prefix of tupleBuf and limit its capacity.
		tupleRow := tupleBuf[:numCols:numCols]
		tupleBuf = tupleBuf[numCols:]

		for i, expr := range tuple {
			desired := types.Any
			if len(desiredTypes) > i {
				desired = desiredTypes[i]
			}

			// Clear the properties so we can check them below.
			typedExpr, err := p.analyzeExpr(ctx, expr, nil, tree.IndexedVarHelper{}, desired, false, "")
			if err != nil {
				return nil, err
			}

			typ := typedExpr.ResolvedType()
			if num == 0 {
				v.columns = append(v.columns, colinfo.ResultColumn{Name: "column" + strconv.Itoa(i+1), Typ: typ})
			} else if v.columns[i].Typ.Family() == types.UnknownFamily {
				v.columns[i].Typ = typ
			} else if typ.Family() != types.UnknownFamily && !typ.Equivalent(v.columns[i].Typ) {
				return nil, pgerror.Newf(pgcode.DatatypeMismatch,
					"VALUES types %s and %s cannot be matched", typ, v.columns[i].Typ)
			}

			tupleRow[i] = typedExpr
		}
		v.tuples = append(v.tuples, tupleRow)
	}
	return v, nil
}

func (p *planner) newContainerValuesNode(columns colinfo.ResultColumns, capacity int) *valuesNode {
	return &valuesNode{
		columns: columns,
		valuesRun: valuesRun{
			rows: rowcontainer.NewRowContainerWithCapacity(
				p.EvalContext().Mon.MakeBoundAccount(),
				colinfo.ColTypeInfoFromResCols(columns),
				capacity,
			),
		},
	}
}

// valuesRun is the run-time state of a valuesNode during local execution.
type valuesRun struct {
	rows    *rowcontainer.RowContainer
	nextRow int // The index of the next row.
}

func (n *valuesNode) startExec(params runParams) error {
	if n.rows != nil {
		// n.rows was already created in newContainerValuesNode.
		// Nothing to do here.
		return nil
	}

	// This node is coming from a SQL query (as opposed to sortNode and
	// others that create a valuesNode internally for storing results
	// from other planNodes), so its expressions need evaluating.
	// This may run subqueries.
	n.rows = rowcontainer.NewRowContainerWithCapacity(
		params.extendedEvalCtx.Mon.MakeBoundAccount(),
		colinfo.ColTypeInfoFromResCols(n.columns),
		len(n.tuples),
	)

	row := make([]tree.Datum, len(n.columns))
	for _, tupleRow := range n.tuples {
		for i, typedExpr := range tupleRow {
			var err error
			row[i], err = typedExpr.Eval(params.EvalContext())
			if err != nil {
				return err
			}
		}
		if _, err := n.rows.AddRow(params.ctx, row); err != nil {
			return err
		}
	}

	return nil
}

func (n *valuesNode) Next(runParams) (bool, error) {
	if n.nextRow >= n.rows.Len() {
		return false, nil
	}
	n.nextRow++
	return true, nil
}

func (n *valuesNode) Values() tree.Datums {
	return n.rows.At(n.nextRow - 1)
}

func (n *valuesNode) Close(ctx context.Context) {
	if n.rows != nil {
		n.rows.Close(ctx)
		n.rows = nil
	}
}

func newValuesListLenErr(exp, got int) error {
	return pgerror.Newf(
		pgcode.Syntax,
		"VALUES lists must all be the same length, expected %d columns, found %d",
		exp, got)
}
