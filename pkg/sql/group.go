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

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// A groupNode implements the planNode interface and handles the grouping logic.
// It "wraps" a planNode which is used to retrieve the ungrouped results.
type groupNode struct {
	// The schema for this groupNode.
	columns colinfo.ResultColumns

	// The source node (which returns values that feed into the aggregation).
	plan planNode

	// Indices of the group by columns in the source plan.
	groupCols []int

	// Set when we have an input ordering on (a subset of) grouping columns. Only
	// column indices in groupCols can appear in this ordering.
	groupColOrdering colinfo.ColumnOrdering

	// isScalar is set for "scalar groupby", where we want a result
	// even if there are no input rows, e.g. SELECT MIN(x) FROM t.
	isScalar bool

	// funcs contains the information about all aggregate functions.
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
}

type aggregateFuncHolder struct {
	// Name of the aggregate function.
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
	// isDistinct indicates whether only distinct values are aggregated.
	isDistinct bool
}

// newAggregateFuncHolder creates an aggregateFuncHolder.
//
// If function is nil, this is an "ident" aggregation (meaning that the input is
// a group-by column and the "aggregation" returns its value)
//
// If the aggregation function takes no arguments (e.g. COUNT_ROWS),
// argRenderIdx is noRenderIdx.
func newAggregateFuncHolder(
	funcName string, argRenderIdxs []int, arguments tree.Datums, isDistinct bool,
) *aggregateFuncHolder {
	res := &aggregateFuncHolder{
		funcName:        funcName,
		argRenderIdxs:   argRenderIdxs,
		filterRenderIdx: tree.NoColumnIdx,
		arguments:       arguments,
		isDistinct:      isDistinct,
	}
	return res
}

func (a *aggregateFuncHolder) hasFilter() bool {
	return a.filterRenderIdx != tree.NoColumnIdx
}
