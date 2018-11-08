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

package ordering

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/util"
)

// CanProvide returns true if the given operator returns rows that can
// satisfy the given required ordering.
func CanProvide(expr memo.RelExpr, required *props.OrderingChoice) bool {
	if required.Any() {
		return true
	}
	if util.RaceEnabled {
		checkRequired(expr, required)
	}
	return funcMap[expr.Op()].canProvideOrdering(expr, required)
}

// BuildChildRequired returns the ordering that must be required of its
// given child in order to satisfy a required ordering. Can only be called if
// CanProvide is true for the required ordering.
func BuildChildRequired(
	parent memo.RelExpr, required *props.OrderingChoice, childIdx int,
) props.OrderingChoice {
	result := funcMap[parent.Op()].buildChildReqOrdering(parent, required, childIdx)
	if util.RaceEnabled && !result.Any() {
		checkRequired(parent.Child(childIdx).(memo.RelExpr), &result)
	}
	return result
}

type funcs struct {
	canProvideOrdering func(expr memo.RelExpr, required *props.OrderingChoice) bool

	buildChildReqOrdering func(
		parent memo.RelExpr, required *props.OrderingChoice, childIdx int,
	) props.OrderingChoice
}

var funcMap [opt.NumOperators]funcs

func init() {
	for _, op := range opt.RelationalOperators {
		funcMap[op] = funcs{
			canProvideOrdering:    canNeverProvideOrdering,
			buildChildReqOrdering: noChildReqOrdering,
		}
	}
	funcMap[opt.ScanOp] = funcs{
		canProvideOrdering:    scanCanProvideOrdering,
		buildChildReqOrdering: noChildReqOrdering,
	}
	funcMap[opt.SelectOp] = funcs{
		canProvideOrdering:    selectCanProvideOrdering,
		buildChildReqOrdering: selectBuildChildReqOrdering,
	}
	funcMap[opt.ProjectOp] = funcs{
		canProvideOrdering:    projectCanProvideOrdering,
		buildChildReqOrdering: projectBuildChildReqOrdering,
	}
	funcMap[opt.IndexJoinOp] = funcs{
		canProvideOrdering:    lookupOrIndexJoinCanProvideOrdering,
		buildChildReqOrdering: lookupOrIndexJoinBuildChildReqOrdering,
	}
	funcMap[opt.LookupJoinOp] = funcs{
		canProvideOrdering:    lookupOrIndexJoinCanProvideOrdering,
		buildChildReqOrdering: lookupOrIndexJoinBuildChildReqOrdering,
	}
	funcMap[opt.RowNumberOp] = funcs{
		canProvideOrdering:    rowNumberCanProvideOrdering,
		buildChildReqOrdering: rowNumberBuildChildReqOrdering,
	}
	funcMap[opt.MergeJoinOp] = funcs{
		canProvideOrdering:    mergeJoinCanProvideOrdering,
		buildChildReqOrdering: mergeJoinBuildChildReqOrdering,
	}
	funcMap[opt.LimitOp] = funcs{
		canProvideOrdering:    limitOrOffsetCanProvideOrdering,
		buildChildReqOrdering: limitOrOffsetBuildChildReqOrdering,
	}
	funcMap[opt.OffsetOp] = funcs{
		canProvideOrdering:    limitOrOffsetCanProvideOrdering,
		buildChildReqOrdering: limitOrOffsetBuildChildReqOrdering,
	}
	funcMap[opt.ExplainOp] = funcs{
		canProvideOrdering:    canNeverProvideOrdering,
		buildChildReqOrdering: explainBuildChildReqOrdering,
	}
	funcMap[opt.ScalarGroupByOp] = funcs{
		// ScalarGroupBy always has exactly one result; any required ordering should
		// have been simplified to Any (unless normalization rules are disabled).
		canProvideOrdering:    canNeverProvideOrdering,
		buildChildReqOrdering: scalarGroupByBuildChildReqOrdering,
	}
	funcMap[opt.GroupByOp] = funcs{
		canProvideOrdering:    groupByCanProvideOrdering,
		buildChildReqOrdering: groupByBuildChildReqOrdering,
	}
	funcMap[opt.DistinctOnOp] = funcs{
		canProvideOrdering:    distinctOnCanProvideOrdering,
		buildChildReqOrdering: distinctOnBuildChildReqOrdering,
	}
	funcMap[opt.SortOp] = funcs{
		canProvideOrdering:    nil, // should never get called
		buildChildReqOrdering: noChildReqOrdering,
	}
}

func canNeverProvideOrdering(expr memo.RelExpr, required *props.OrderingChoice) bool {
	return false
}

func noChildReqOrdering(
	parent memo.RelExpr, required *props.OrderingChoice, childIdx int,
) props.OrderingChoice {
	return props.OrderingChoice{}
}

// checkRequired runs sanity checks on the ordering required of an operator.
func checkRequired(expr memo.RelExpr, required *props.OrderingChoice) {
	rel := expr.Relational()

	// Verify that the ordering only refers to output columns.
	if !required.SubsetOfCols(rel.OutputCols) {
		panic(fmt.Sprintf("required ordering refers to non-output columns (op %s)", expr.Op()))
	}

	// Verify that columns in a column group are equivalent.
	for i := range required.Columns {
		c := &required.Columns[i]
		if !c.Group.SubsetOf(rel.FuncDeps.ComputeEquivGroup(c.AnyID())) {
			panic(fmt.Sprintf(
				"ordering column group %s contains non-equivalent columns (op %s)",
				c.Group, expr.Op(),
			))
		}
	}
}
