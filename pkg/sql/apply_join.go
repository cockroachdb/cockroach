// Copyright 2019 The Cockroach Authors.
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
	"errors"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec/execbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/norm"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/xform"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

// applyJoinNode implements apply join: the execution component of correlated
// subqueries. The node reads rows from the left planDataSource, and for each
// row, re-plans the right side of the join after replacing its outer columns
// with the corresponding values from the current row on the left. The new right
// plan is then executed and joined with the left row according to normal join
// semantics. This node doesn't support right or full outer joins, or set
// operations.
type applyJoinNode struct {
	joinType sqlbase.JoinType

	optimizer xform.Optimizer

	// The memo for the right side of the join - the one with the outer columns.
	rightMemo *memo.Memo

	// The data source with no outer columns.
	input planDataSource

	// leftBoundColMap maps an outer opt column id, bound by this apply join, to
	// the position in the left plan's row that contains the binding.
	leftBoundColMap opt.ColMap

	// pred represents the join predicate.
	pred *joinPredicate

	// columns contains the metadata for the results of this node.
	columns sqlbase.ResultColumns

	right memo.RelExpr

	run struct {
		emptyRight         tree.Datums
		leftRow            tree.Datums
		leftRowFoundAMatch bool
		rightPlan          planNode
		out                tree.Datums
		done               bool
	}
}

func newApplyJoinNode(
	joinType sqlbase.JoinType,
	left planDataSource,
	leftBoundColMap opt.ColMap,
	right memo.RelExpr,
	pred *joinPredicate,
	memo *memo.Memo,
) (planNode, error) {
	switch joinType {
	case sqlbase.JoinType_RIGHT_OUTER, sqlbase.JoinType_FULL_OUTER:
		return nil, errors.New("unsupported right outer apply join")
	case sqlbase.JoinType_EXCEPT_ALL, sqlbase.JoinType_INTERSECT_ALL:
		return nil, errors.New("unsupported apply set op")
	}
	return &applyJoinNode{
		joinType:        joinType,
		input:           left,
		leftBoundColMap: leftBoundColMap,
		pred:            pred,
		rightMemo:       memo,
		right:           right,
		columns:         pred.info.SourceColumns,
	}, nil
}

func (a *applyJoinNode) startExec(params runParams) error {
	// If needed, pre-allocate a left row of NULL tuples for when the
	// join predicate fails to match.
	if a.joinType == sqlbase.LeftOuterJoin {
		a.run.emptyRight = make(tree.Datums, a.right.Relational().OutputCols.Len())
		for i := range a.run.emptyRight {
			a.run.emptyRight[i] = tree.DNull
		}
	}
	return nil
}

func (applyJoinNode) replaceVars(
	f *norm.Factory, applyInput memo.RelExpr, vars map[opt.ColumnID]tree.Datum,
) {
	var replace func(e opt.Expr) opt.Expr
	replace = func(e opt.Expr) opt.Expr {
		switch t := e.(type) {
		case *memo.VariableExpr:
			if d, ok := vars[t.Col]; ok {
				return f.ConstructConstVal(d)
			}
		}
		return f.CopyAndReplaceDefault(e, replace)
	}
	f.CopyAndReplace(applyInput, applyInput.RequiredPhysical(), replace)
}

func (a *applyJoinNode) Next(params runParams) (bool, error) {
	if a.run.done {
		return false, nil
	}

	for {
		if a.run.rightPlan != nil {
			// We've got a right plan set up. Get its next row.
			ok, err := a.run.rightPlan.Next(params)
			if err != nil {
				return false, err
			}
			if ok {
				rrow := a.run.rightPlan.Values()
				// Compute join.
				predMatched, err := a.pred.eval(params.EvalContext(), a.run.leftRow, rrow)
				if err != nil {
					return false, err
				}
				if predMatched {
					switch a.joinType {
					case sqlbase.JoinType_LEFT_ANTI:
						// We found a match, but we're doing an anti-join, so we're done
						// with this left row.
						a.run.rightPlan.Close(params.ctx)
						a.run.rightPlan = nil
						continue
					case sqlbase.JoinType_LEFT_SEMI:
						// We found a match, but we're doing an semi-join, so we're done
						// with this left row after we output it.
						a.run.rightPlan.Close(params.ctx)
						a.run.rightPlan = nil
						a.pred.prepareRow(a.run.out, a.run.leftRow, nil)
						return true, nil
					}
					a.run.leftRowFoundAMatch = true
					a.pred.prepareRow(a.run.out, a.run.leftRow, rrow)
					return true, nil
				}
			} else {
				// No more rows on the right. Output LEFT OUTER or ANTI match if
				// necessary.
				a.run.rightPlan.Close(params.ctx)
				a.run.rightPlan = nil
				if !a.run.leftRowFoundAMatch {
					switch a.joinType {
					case sqlbase.JoinType_LEFT_OUTER:
						a.pred.prepareRow(a.run.out, a.run.leftRow, a.run.emptyRight)
						return true, nil
					case sqlbase.JoinType_LEFT_ANTI:
						a.pred.prepareRow(a.run.out, a.run.leftRow, nil)
						return true, nil
					}
				}
			}
		}

		a.run.leftRowFoundAMatch = false
		// We need a new row on the left.
		ok, err := a.input.plan.Next(params)
		if err != nil {
			return false, err
		}
		if !ok {
			// No more rows on the left. Goodbye!
			a.run.done = true
			return false, nil
		}

		// Extract the values of the outer columns of the other side of the apply
		// from the latest input row.
		row := a.input.plan.Values()
		a.run.leftRow = row

		bindings := make(map[opt.ColumnID]tree.Datum, a.leftBoundColMap.Len())
		a.leftBoundColMap.ForEach(func(k, v int) {
			bindings[opt.ColumnID(k)] = row[v]
		})

		// The missing piece is how to propagate outer columns from apply joins
		// *above* this one. It seems like we need to keep a map inside of the
		// exec params that contains the mapping I mentioned above. Then, the
		// function in question will take as input that mapping from the exec
		// params, and before invoking it, we'll add the current left-side row to
		// the mapping.

		a.optimizer.Init(params.p.EvalContext())
		f := a.optimizer.Factory()

		// Replace the outer VariableExprs that this applyJoin node is responsible
		// for with the constant values in the latest left row.
		a.replaceVars(f, a.right, bindings)

		a.optimizer.Optimize()

		execFactory := makeExecFactory(params.p)
		p, err := execbuilder.New(&execFactory, f.Memo(), a.right, params.EvalContext()).Build()
		if err != nil {
			return false, err
		}
		plan := p.(*planTop)

		if err := startExec(params, plan.plan); err != nil {
			return false, err
		}

		// We've got a fresh right plan. Continue along in the loop, which will deal
		// with joining the right plan's output with our left row.
		a.run.rightPlan = plan.plan
		continue
	}
}

func (a *applyJoinNode) Values() tree.Datums {
	return a.run.out
}

func (a *applyJoinNode) Close(ctx context.Context) {
	if a.run.rightPlan != nil {
		a.run.rightPlan.Close(ctx)
	}
	a.input.plan.Close(ctx)
}
