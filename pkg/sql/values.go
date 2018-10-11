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
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
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
	ctx context.Context, origN tree.Statement, desiredTypes []types.T,
) (planNode, error) {
	v := &valuesNode{
		specifiedInQuery: true,
		isConst:          true,
	}

	// If we have names, extract them.
	var n *tree.ValuesClause
	var names tree.NameList
	switch t := origN.(type) {
	case *tree.ValuesClauseWithNames:
		n = &t.ValuesClause
		names = t.Names
	case *tree.ValuesClause:
		n = t
	default:
		return nil, pgerror.NewAssertionErrorf("unhandled case in values: %T %v", origN, origN)
	}

	if len(n.Rows) == 0 {
		return v, nil
	}

	numCols := len(n.Rows[0])

	v.tuples = make([][]tree.TypedExpr, 0, len(n.Rows))
	tupleBuf := make([]tree.TypedExpr, len(n.Rows)*numCols)

	v.columns = make(sqlbase.ResultColumns, 0, numCols)

	lastKnownSubqueryIndex := len(p.curPlan.subqueryPlans)

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
			if names != nil && (!(typ.Equivalent(desired) || typ == types.Unknown)) {
				var colName tree.Name
				if len(names) > i {
					colName = names[i]
				} else {
					colName = "unknown"
				}
				desiredColTyp, err := sqlbase.DatumTypeToColumnType(desired)
				if err != nil {
					return nil, err
				}
				err = sqlbase.CheckColumnValueType(typ, desiredColTyp, string(colName))
				if err != nil {
					return nil, err
				}
				// For some reason we didn't detect a new error. Return a fresh one.
				return nil, sqlbase.NewMismatchedTypeError(typ, desiredColTyp.SemanticType, string(colName))
			}

			if num == 0 {
				v.columns = append(v.columns, sqlbase.ResultColumn{Name: "column" + strconv.Itoa(i+1), Typ: typ})
			} else if v.columns[i].Typ == types.Unknown {
				v.columns[i].Typ = typ
			} else if typ != types.Unknown && !typ.Equivalent(v.columns[i].Typ) {
				return nil, pgerror.NewErrorf(pgerror.CodeDatatypeMismatchError,
					"VALUES types %s and %s cannot be matched", typ, v.columns[i].Typ)
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
	// from other planNodes), so its expressions need evaluating.
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
	return pgerror.NewErrorf(
		pgerror.CodeSyntaxError,
		"VALUES lists must all be the same length, expected %d columns, found %d",
		exp, got)
}
