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

package xform

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
)

// coster encapsulates the cost model for the optimizer. The coster assigns an
// estimated cost to each expression in the memo so that the optimizer can
// choose the lowest cost expression tree. The estimated cost is a best-effort
// approximation of the actual cost of execution, based on table and index
// statistics that are propagated throughout the logical expression tree.
type coster struct {
	mem *memo.Memo
}

func (c *coster) init(mem *memo.Memo) {
	c.mem = mem
}

// computeCost calculates the estimated cost of the candidate best expression,
// based on its logical properties as well as the cost of its children. Each
// expression's cost must always be >= the total costs of its children, so that
// branch-and-bound pruning will work properly.
//
// TODO: This is just a skeleton, and needs to compute real costs.
func (c *coster) computeCost(candidate *memo.BestExpr, props *memo.LogicalProps) {
	var cost memo.Cost
	switch candidate.Operator() {
	case opt.SortOp:
		cost = c.computeSortCost(candidate, props)

	case opt.ScanOp:
		cost = c.computeScanCost(candidate, props)

	case opt.SelectOp:
		cost = c.computeSelectCost(candidate, props)

	case opt.ValuesOp:
		cost = c.computeValuesCost(candidate, props)

	default:
		// By default, cost of parent is sum of child costs.
		cost = c.computeChildrenCost(candidate)
	}

	candidate.SetCost(cost)
}

func (c *coster) computeSortCost(candidate *memo.BestExpr, props *memo.LogicalProps) memo.Cost {
	cost := memo.Cost(props.Relational.Stats.RowCount) * 0.25
	return cost + c.computeChildrenCost(candidate)
}

func (c *coster) computeScanCost(candidate *memo.BestExpr, props *memo.LogicalProps) memo.Cost {
	return memo.Cost(props.Relational.Stats.RowCount)
}

func (c *coster) computeSelectCost(candidate *memo.BestExpr, props *memo.LogicalProps) memo.Cost {
	// The filter has to be evaluated on each input row.
	inputRowCount := c.mem.BestExprLogical(candidate.Child(0)).Relational.Stats.RowCount
	cost := memo.Cost(inputRowCount) * 0.1
	return cost + c.computeChildrenCost(candidate)
}

func (c *coster) computeValuesCost(candidate *memo.BestExpr, props *memo.LogicalProps) memo.Cost {
	return memo.Cost(props.Relational.Stats.RowCount)
}

func (c *coster) computeChildrenCost(candidate *memo.BestExpr) memo.Cost {
	var cost memo.Cost
	for i := 0; i < candidate.ChildCount(); i++ {
		cost += c.mem.BestExprCost(candidate.Child(i))
	}
	return cost
}
