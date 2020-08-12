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

	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
)

// A groupNode implements the planNode interface and handles the grouping logic.
// It "wraps" a planNode which is used to retrieve the ungrouped results.
type groupNode struct {
	// The schema for this groupNode.
	columns sqlbase.ResultColumns

	// The source node (which returns values that feed into the aggregation).
	plan planNode

	// Indices of the group by columns in the source plan.
	groupCols []int

	// Set when we have an input ordering on (a subset of) grouping columns. Only
	// column indices in groupCols can appear in this ordering.
	groupColOrdering sqlbase.ColumnOrdering

	// isScalar is set for "scalar groupby", where we want a result
	// even if there are no input rows, e.g. SELECT MIN(x) FROM t.
	isScalar bool

	// funcs are the aggregation functions that the renders use.
	funcs []*aggregateFuncHolder

	reqOrdering ReqOrdering
}

func (n *groupNode) startExec(params runParams) error {
	panic("groupNode cannot be run in local mode")
}

func (n *groupNode) Next(params runParams) (bool, error) {
	panic("groupNode cannot be run in local mode")
}

func (n *groupNode) Values() tree.Datums {
	panic("groupNode cannot be run in local mode")
}

func (n *groupNode) Close(ctx context.Context) {
	n.plan.Close(ctx)
	for _, f := range n.funcs {
		f.close(ctx)
	}
}

// aggIsGroupingColumn returns true if the given output aggregation is an
// any_not_null aggregation for a grouping column. The grouping column
// index is also returned.
func (n *groupNode) aggIsGroupingColumn(aggIdx int) (colIdx int, ok bool) {
	if holder := n.funcs[aggIdx]; holder.funcName == builtins.AnyNotNull {
		for _, c := range n.groupCols {
			for _, renderIdx := range holder.argRenderIdxs {
				if c == renderIdx {
					return c, true
				}
			}
		}
	}
	return -1, false
}

type aggregateFuncHolder struct {
	// Name of the aggregate function. Empty if this column reproduces a bucket
	// key unchanged.
	funcName string

	// The argument of the function is a single value produced by the renderNode
	// underneath. If the function has no argument (COUNT_ROWS), it is empty.
	argRenderIdxs []int
	// If there is a filter, the result is a single value produced by the
	// renderNode underneath. If there is no filter, it is set to
	// tree.NoColumnIdx.
	filterRenderIdx int

	// arguments are constant expressions that can be optionally passed into an
	// aggregator.
	arguments tree.Datums

	run aggregateFuncRun
}

// aggregateFuncRun contains the run-time state for one aggregation function
// during local execution.
type aggregateFuncRun struct {
	buckets       map[string]tree.AggregateFunc
	bucketsMemAcc mon.BoundAccount
	seen          map[string]struct{}
}

// newAggregateFuncHolder creates an aggregateFuncHolder.
//
// If function is nil, this is an "ident" aggregation (meaning that the input is
// a group-by column and the "aggregation" returns its value)
//
// If the aggregation function takes no arguments (e.g. COUNT_ROWS),
// argRenderIdx is noRenderIdx.
func (n *groupNode) newAggregateFuncHolder(
	funcName string, argRenderIdxs []int, arguments tree.Datums, acc mon.BoundAccount,
) *aggregateFuncHolder {
	res := &aggregateFuncHolder{
		funcName:        funcName,
		argRenderIdxs:   argRenderIdxs,
		filterRenderIdx: tree.NoColumnIdx,
		arguments:       arguments,
		run: aggregateFuncRun{
			buckets:       make(map[string]tree.AggregateFunc),
			bucketsMemAcc: acc,
		},
	}
	return res
}

func (a *aggregateFuncHolder) hasFilter() bool {
	return a.filterRenderIdx != tree.NoColumnIdx
}

// setDistinct causes a to ignore duplicate values of the argument.
func (a *aggregateFuncHolder) setDistinct() {
	a.run.seen = make(map[string]struct{})
}

// isDistinct returns true if only distinct values are aggregated,
// e.g. SUM(DISTINCT x).
func (a *aggregateFuncHolder) isDistinct() bool {
	return a.run.seen != nil
}

func (a *aggregateFuncHolder) close(ctx context.Context) {
	for _, aggFunc := range a.run.buckets {
		aggFunc.Close(ctx)
	}

	a.run.buckets = nil
	a.run.seen = nil

	a.run.bucketsMemAcc.Close(ctx)
}
