// Copyright 2018 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

type lookupJoinNode struct {
	input planNode
	table *scanNode

	// joinType is either INNER or LEFT_OUTER.
	joinType sqlbase.JoinType

	// keyCols identifies the columns from the input which are used for the
	// lookup. These correspond to a prefix of the index columns (of the index we
	// are looking up into).
	keyCols []int

	// columns are the produced columns, namely the input clumns and and the
	// columns in the table scanNode.
	columns sqlbase.ResultColumns

	// onCond is any ON condition to be used in conjunction with the implicit
	// equality condition on keyCols.
	onCond tree.TypedExpr

	props physicalProps

	run lookupJoinRun
}

// lookupJoinRun is the state for the local execution path for lookup join.
//
// We have no local execution path; we fall back on doing a full table scan and
// using the joinNode to do the join. Note that this can be significantly worse
// than not having lookup joins at all, because no filters are being pushed into
// the scan as constraints.
//
// This path is temporary and only exists to avoid failures (especially in logic
// tests) when DistSQL is not being used.
type lookupJoinRun struct {
	n *joinNode
}

// startExec is part of the execStartable interface.
func (lj *lookupJoinNode) startExec(params runParams) error {
	// Make sure the table node has a span (full scan).
	var err error
	lj.table.spans, err = spansFromConstraint(lj.table.desc, lj.table.index, nil /* constraint */)
	if err != nil {
		return err
	}

	// Create a joinNode that joins the input and the table. Note that startExec
	// will be called on lj.input and lj.table.

	leftSrc := planDataSource{
		info: &sqlbase.DataSourceInfo{SourceColumns: planColumns(lj.input)},
		plan: lj.input,
	}
	rightSrc := planDataSource{
		info: &sqlbase.DataSourceInfo{SourceColumns: planColumns(lj.table)},
		plan: lj.table,
	}

	pred, _, err := params.p.makeJoinPredicate(
		context.TODO(), leftSrc.info, rightSrc.info, lj.joinType, nil, /* cond */
	)
	if err != nil {
		return err
	}

	// TODO(radu): we are relying on the fact that the equality constraints
	// (implied by keyCols) are still in the ON condition. We'd normally have to
	// explicitly program the equalities.
	onAndExprs := splitAndExpr(params.EvalContext(), lj.onCond, nil /* exprs */)
	for _, e := range onAndExprs {
		if e != tree.DBoolTrue && !pred.tryAddEqualityFilter(e, leftSrc.info, rightSrc.info) {
			pred.onCond = mergeConj(pred.onCond, e)
		}
	}
	lj.run.n = params.p.makeJoinNode(leftSrc, rightSrc, pred)
	return lj.run.n.startExec(params)
}

func (lj *lookupJoinNode) Next(params runParams) (bool, error) {
	return lj.run.n.Next(params)
}

func (lj *lookupJoinNode) Values() tree.Datums {
	return lj.run.n.Values()
}

func (lj *lookupJoinNode) Close(ctx context.Context) {
	if lj.run.n != nil {
		lj.run.n.Close(ctx)
	}
}
