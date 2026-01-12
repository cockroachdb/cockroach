// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package plannode

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// A groupNode implements the planNode interface and handles the grouping logic.
// It "wraps" a planNode which is used to retrieve the ungrouped results.
type GroupNode struct {
	singleInputPlanNode

	// The schema for this groupNode.
	Columns colinfo.ResultColumns

	// Indices of the group by columns in the source plan.
	GroupCols []exec.NodeColumnOrdinal

	// Set when we have an input ordering on (a subset of) grouping columns. Only
	// column indices in GroupCols can appear in this ordering.
	GroupColOrdering colinfo.ColumnOrdering

	// IsScalar is set for "scalar groupby", where we want a result
	// even if there are no input rows, e.g. SELECT MIN(x) FROM t.
	IsScalar bool

	// Funcs contains the information about all aggregate functions.
	Funcs []*aggregateFuncHolder

	ReqOrdering ReqOrdering

	// EstimatedRowCount, when set, is the estimated number of rows that this
	// groupNode will output.
	EstimatedRowCount uint64

	// estimatedInputRowCount, when set, is the estimated number of rows that
	// this groupNode will read from its input.
	EstimatedInputRowCount uint64
}

func (n *groupNode) StartExec(params runParams) error {
	panic("groupNode cannot be run in local mode")
}

func (n *groupNode) Next(params runParams) (bool, error) {
	panic("groupNode cannot be run in local mode")
}

func (n *groupNode) Values() tree.Datums {
	panic("groupNode cannot be run in local mode")
}

func (n *groupNode) Close(ctx context.Context) {
	n.Source.Close(ctx)
}

type aggregateFuncHolder struct {
	// Name of the aggregate function.
	FuncName string
	// The argument of the function is a single value produced by the renderNode
	// underneath. If the function has no argument (COUNT_ROWS), it is empty.
	ArgRenderIdxs []exec.NodeColumnOrdinal
	// If there is a filter, the result is a single value produced by the
	// renderNode underneath. If there is no filter, it is set to
	// tree.NoColumnIdx.
	FilterRenderIdx int
	// Arguments are constant expressions that can be optionally passed into an
	// aggregator.
	Arguments tree.Datums
	// IsDistinct indicates whether only distinct values are aggregated.
	IsDistinct bool
	// DistsqlBlocklist is set when this function cannot be evaluated in
	// distributed fashion.
	DistsqlBlocklist bool
}

// newAggregateFuncHolder creates an aggregateFuncHolder.
//
// If function is nil, this is an "ident" aggregation (meaning that the input is
// a group-by column and the "aggregation" returns its value)
//
// If the aggregation function takes no arguments (e.g. COUNT_ROWS),
// argRenderIdx is noRenderIdx.
func newAggregateFuncHolder(
	funcName string,
	argRenderIdxs []exec.NodeColumnOrdinal,
	arguments tree.Datums,
	isDistinct bool,
	DistsqlBlocklist bool,
) *aggregateFuncHolder {
	res := &aggregateFuncHolder{
		FuncName:         funcName,
		ArgRenderIdxs:    argRenderIdxs,
		FilterRenderIdx:  tree.NoColumnIdx,
		Arguments:        arguments,
		IsDistinct:       isDistinct,
		DistsqlBlocklist: DistsqlBlocklist,
	}
	return res
}

func (a *aggregateFuncHolder) HasFilter() bool {
	return a.FilterRenderIdx != tree.NoColumnIdx
}



// Exported aliases for use outside the package
type AggregateFuncHolder = aggregateFuncHolder

func NewAggregateFuncHolder(
	funcName string,
	argRenderIdxs []exec.NodeColumnOrdinal,
	arguments tree.Datums,
	isDistinct bool,
	DistsqlBlocklist bool,
) *aggregateFuncHolder {
	return newAggregateFuncHolder(funcName, argRenderIdxs, arguments, isDistinct, DistsqlBlocklist)
}

// Lowercase alias
type groupNode = GroupNode
