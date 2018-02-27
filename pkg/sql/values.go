// Copyright 2015 The Cockroach Authors.
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
	"strconv"

	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

type valuesNode struct {
	columns sqlbase.ResultColumns
	tuples  [][]tree.TypedExpr
	// isConst is set if the valuesNode only contains constant expressions (no
	// subqueries). In this case, rows will be evaluated during the first call
	// to planNode.Start and memoized for future consumption. A valuesNode with
	// isConst = true can serve its values multiple times. See valuesNode.Reset.
	isConst bool

	// specifiedInQuery is set if the valuesNode represents a literal
	// relational expression that was present in the original SQL text,
	// as opposed to e.g. a valuesNode resulting from the expansion of
	// a vtable value generator. This changes distsql physical planning.
	specifiedInQuery bool

	valuesRun
}

// Values implements the VALUES clause.
func (p *planner) Values(
	ctx context.Context, n *tree.ValuesClause, desiredTypes []types.T,
) (planNode, error) {
	v := &valuesNode{
		specifiedInQuery: true,
		isConst:          true,
	}
	if len(n.Tuples) == 0 {
		return v, nil
	}

	numCols := len(n.Tuples[0].Exprs)

	v.tuples = make([][]tree.TypedExpr, 0, len(n.Tuples))
	tupleBuf := make([]tree.TypedExpr, len(n.Tuples)*numCols)

	v.columns = make(sqlbase.ResultColumns, 0, numCols)

	lastKnownSubqueryIndex := len(p.curPlan.subqueryPlans)

	for num, tuple := range n.Tuples {
		if a, e := len(tuple.Exprs), numCols; a != e {
			return nil, newValuesListLenErr(e, a)
		}

		// Chop off prefix of tupleBuf and limit its capacity.
		tupleRow := tupleBuf[:numCols:numCols]
		tupleBuf = tupleBuf[numCols:]

		for i, expr := range tuple.Exprs {
			if err := p.txCtx.AssertNoAggregationOrWindowing(
				expr, "VALUES", p.SessionData().SearchPath,
			); err != nil {
				return nil, err
			}

			desired := types.Any
			if len(desiredTypes) > i {
				desired = desiredTypes[i]
			}
			typedExpr, err := p.analyzeExpr(ctx, expr, nil, tree.IndexedVarHelper{}, desired, false, "")
			if err != nil {
				return nil, err
			}

			typ := typedExpr.ResolvedType()
			if num == 0 {
				v.columns = append(v.columns, sqlbase.ResultColumn{Name: "column" + strconv.Itoa(i+1), Typ: typ})
			} else if v.columns[i].Typ == types.Unknown {
				v.columns[i].Typ = typ
			} else if typ != types.Unknown && !typ.Equivalent(v.columns[i].Typ) {
				return nil, fmt.Errorf("VALUES list type mismatch, %s for %s", typ, v.columns[i].Typ)
			}

			tupleRow[i] = typedExpr
		}
		v.tuples = append(v.tuples, tupleRow)
	}

	// TODO(nvanbenschoten): if v.isConst, we should be able to evaluate n.rows
	// ahead of time. This requires changing the contract for planNode.Close such
	// that it must always be called unless an error is returned from a planNode
	// constructor. This would simplify the Close contract, but would make some
	// code (like in planner.SelectClause) more messy.
	v.isConst = (len(p.curPlan.subqueryPlans) == lastKnownSubqueryIndex)
	return v, nil
}

func (p *planner) newContainerValuesNode(columns sqlbase.ResultColumns, capacity int) *valuesNode {
	return &valuesNode{
		columns: columns,
		isConst: true,
		valuesRun: valuesRun{
			rows: sqlbase.NewRowContainer(
				p.EvalContext().Mon.MakeBoundAccount(), sqlbase.ColTypeInfoFromResCols(columns), capacity,
			),
		},
	}
}

// valuesRun is the run-time state of a valuesNode during local execution.
type valuesRun struct {
	rows    *sqlbase.RowContainer
	nextRow int // The index of the next row.
}

func (n *valuesNode) startExec(params runParams) error {
	if n.rows != nil {
		if !n.isConst {
			log.Fatalf(params.ctx, "valuesNode evaluated twice")
		}
		return nil
	}

	// This node is coming from a SQL query (as opposed to sortNode and
	// others that create a valuesNode internally for storing results
	// from other planNodes), so its expressions need evaluting.
	// This may run subqueries.
	n.rows = sqlbase.NewRowContainer(
		params.extendedEvalCtx.Mon.MakeBoundAccount(),
		sqlbase.ColTypeInfoFromResCols(n.columns),
		len(n.tuples),
	)

	row := make([]tree.Datum, len(n.columns))
	for _, tupleRow := range n.tuples {
		for i, typedExpr := range tupleRow {
			if n.columns[i].Omitted {
				row[i] = tree.DNull
			} else {
				var err error
				row[i], err = typedExpr.Eval(params.EvalContext())
				if err != nil {
					return err
				}
			}
		}
		if _, err := n.rows.AddRow(params.ctx, row); err != nil {
			return err
		}
	}

	return nil
}

// Reset resets the valuesNode processing state without requiring recomputation
// of the values tuples if the valuesNode is processed again. Reset can only
// be called if valuesNode.isConst.
func (n *valuesNode) Reset(ctx context.Context) {
	if !n.isConst {
		log.Fatalf(ctx, "valuesNode.Reset can only be called on constant valuesNodes")
	}
	n.nextRow = 0
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
	return errors.Errorf("VALUES lists must all be the same length, expected %d columns, found %d",
		exp, got)
}
