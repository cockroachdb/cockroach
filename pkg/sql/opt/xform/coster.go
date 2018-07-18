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
	"fmt"
	"math"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
)

// Coster is used by the optimizer to assign a cost to a candidate expression
// that can provide a set of required physical properties. If a candidate
// expression has a lower cost than any other expression in the memo group, then
// it becomes the new best expression for the group.
//
// Coster is an interface so that different costing algorithms can be used by
// the optimizer. For example, the OptSteps command uses a custom coster that
// assigns infinite costs to some expressions in order to prevent them from
// being part of the lowest cost tree (for debugging purposes).
type Coster interface {
	// ComputeCost returns the estimated cost of executing the candidate
	// expression. The optimizer does not expect the cost to correspond to any
	// real-world metric, but does expect costs to be comparable to one another,
	// as well as summable.
	ComputeCost(candidate *memo.BestExpr, props *props.Logical) memo.Cost
}

// coster encapsulates the default cost model for the optimizer. The coster
// assigns an estimated cost to each expression in the memo so that the
// optimizer can choose the lowest cost expression tree. The estimated cost is
// a best-effort approximation of the actual cost of execution, based on table
// and index statistics that are propagated throughout the logical expression
// tree.
type coster struct {
	mem *memo.Memo
}

func newCoster(mem *memo.Memo) *coster {
	return &coster{mem: mem}
}

const (
	// These costs have been copied from the Postgres optimizer:
	// https://github.com/postgres/postgres/blob/master/src/include/optimizer/cost.h
	// TODO(rytaft): "How Good are Query Optimizers, Really?" says that the
	// PostgreSQL ratio between CPU and I/O is probably unrealistic in modern
	// systems since much of the data can be cached in memory. Consider
	// increasing the cpuCostFactor to account for this.
	cpuCostFactor    = 0.01
	seqIOCostFactor  = 1
	randIOCostFactor = 4
)

// computeCost calculates the estimated cost of the candidate best expression,
// based on its logical properties as well as the cost of its children. Each
// expression's cost must always be >= the total costs of its children, so that
// branch-and-bound pruning will work properly.
func (c *coster) ComputeCost(candidate *memo.BestExpr, logical *props.Logical) memo.Cost {
	var cost memo.Cost
	switch candidate.Operator() {
	case opt.SortOp:
		cost = c.computeSortCost(candidate, logical)

	case opt.ScanOp:
		cost = c.computeScanCost(candidate, logical)

	case opt.VirtualScanOp:
		cost = c.computeVirtualScanCost(candidate, logical)

	case opt.SelectOp:
		cost = c.computeSelectCost(candidate, logical)

	case opt.ValuesOp:
		cost = c.computeValuesCost(candidate, logical)

	case opt.InnerJoinOp, opt.LeftJoinOp, opt.RightJoinOp, opt.FullJoinOp,
		opt.SemiJoinOp, opt.AntiJoinOp, opt.InnerJoinApplyOp, opt.LeftJoinApplyOp,
		opt.RightJoinApplyOp, opt.FullJoinApplyOp, opt.SemiJoinApplyOp, opt.AntiJoinApplyOp:
		// All join ops use hash join by default.
		cost = c.computeHashJoinCost(candidate, logical)

	case opt.MergeJoinOp:
		cost = c.computeMergeJoinCost(candidate, logical)

	case opt.IndexJoinOp:
		cost = c.computeIndexJoinCost(candidate, logical)

	case opt.LookupJoinOp:
		cost = c.computeLookupJoinCost(candidate, logical)

	// TODO(rytaft): Add linear cost functions for GROUP BY, set ops, etc.

	case opt.ExplainOp:
		// Technically, the cost of an Explain operation is independent of the cost
		// of the underlying plan. However, we want to explain the plan we would get
		// without EXPLAIN, i.e. the lowest cost plan. So we let the default code
		// below pass through the input plan cost.
		fallthrough

	default:
		// By default, cost of parent is sum of child costs.
		cost = c.computeChildrenCost(candidate)
	}
	if !cost.Less(memo.MaxCost) {
		// Optsteps uses MaxCost to suppress expressions in the memo. When an
		// expression with MaxCost is added to the memo, it can lead to an obscure
		// crash with an unknown operation. We'd rather detect this early.
		panic(fmt.Sprintf("operator %s with MaxCost added to the memo", candidate.Operator()))
	}
	return cost
}

func (c *coster) computeSortCost(candidate *memo.BestExpr, logical *props.Logical) memo.Cost {
	rowCount := logical.Relational.Stats.RowCount
	cost := memo.Cost(rowCount) * cpuCostFactor
	if rowCount > 1 {
		// TODO(rytaft): This is the cost of a local, in-memory sort. When a
		// certain amount of memory is used, distsql switches to a disk-based sort
		// with a temp RocksDB store.
		cost = memo.Cost(rowCount*math.Log2(rowCount)) * cpuCostFactor
	}

	return cost + c.computeChildrenCost(candidate)
}

func (c *coster) computeScanCost(candidate *memo.BestExpr, logical *props.Logical) memo.Cost {
	// Scanning an index with a few columns is faster than scanning an index with
	// many columns. Ideally, we would want to use statistics about the size of
	// each column. In lieu of that, use the number of columns.
	def := candidate.Private(c.mem).(*memo.ScanOpDef)
	rowCount := memo.Cost(logical.Relational.Stats.RowCount)
	perRowCost := c.rowScanCost(def.Table, def.Index, def.Cols.Len())
	if def.Reverse {
		perRowCost *= 2
	}
	return rowCount * (seqIOCostFactor + perRowCost)
}

func (c *coster) computeVirtualScanCost(
	candidate *memo.BestExpr, logical *props.Logical,
) memo.Cost {
	// Virtual tables are generated on-the-fly according to system metadata that
	// is assumed to be in memory.
	rowCount := memo.Cost(logical.Relational.Stats.RowCount)
	return rowCount * cpuCostFactor
}

// rowScanCost is the CPU cost to scan one row, which depends on the number of
// columns in the index and (to a lesser extent) on the number of columns we are
// scanning.
func (c *coster) rowScanCost(table opt.TableID, index int, numScannedCols int) memo.Cost {
	md := c.mem.Metadata()
	numCols := md.Table(table).Index(index).ColumnCount()
	// The number of the columns in the index matter because more columns means
	// more data to scan. The number of columns we actually return also matters
	// because that is the amount of data that we could potentially transfer over
	// the network.
	return memo.Cost(numCols+numScannedCols) * cpuCostFactor
}

func (c *coster) computeSelectCost(candidate *memo.BestExpr, logical *props.Logical) memo.Cost {
	// The filter has to be evaluated on each input row.
	inputRowCount := c.mem.BestExprLogical(candidate.Child(0)).Relational.Stats.RowCount
	cost := memo.Cost(inputRowCount) * cpuCostFactor
	return cost + c.computeChildrenCost(candidate)
}

func (c *coster) computeValuesCost(candidate *memo.BestExpr, logical *props.Logical) memo.Cost {
	return memo.Cost(logical.Relational.Stats.RowCount) * cpuCostFactor
}

func (c *coster) computeHashJoinCost(candidate *memo.BestExpr, logical *props.Logical) memo.Cost {
	leftRowCount := c.mem.BestExprLogical(candidate.Child(0)).Relational.Stats.RowCount
	rightRowCount := c.mem.BestExprLogical(candidate.Child(1)).Relational.Stats.RowCount

	// A hash join must process every row from both tables once.
	//
	// We add some factors to account for the hashtable build and lookups. The
	// right side is the one stored in the hashtable, so we use a larger factor
	// for that side. This ensures that a join with the smaller right side is
	// preferred to the symmetric join.
	//
	// TODO(rytaft): This is the cost of an in-memory hash join. When a certain
	// amount of memory is used, distsql switches to a disk-based hash join with
	// a temp RocksDB store.
	cost := memo.Cost(1.25*leftRowCount+1.75*rightRowCount) * cpuCostFactor

	// Add the CPU cost of emitting the rows.
	// TODO(radu): ideally we would have an estimate of how many rows we actually
	// have to run the ON condition on.
	cost += memo.Cost(logical.Relational.Stats.RowCount) * cpuCostFactor
	return cost + c.computeChildrenCost(candidate)
}

func (c *coster) computeMergeJoinCost(candidate *memo.BestExpr, logical *props.Logical) memo.Cost {
	leftRowCount := c.mem.BestExprLogical(candidate.Child(0)).Relational.Stats.RowCount
	rightRowCount := c.mem.BestExprLogical(candidate.Child(1)).Relational.Stats.RowCount

	cost := memo.Cost(leftRowCount+rightRowCount) * cpuCostFactor

	// Add the CPU cost of emitting the rows.
	// TODO(radu): ideally we would have an estimate of how many rows we actually
	// have to run the ON condition on.
	cost += memo.Cost(logical.Relational.Stats.RowCount) * cpuCostFactor
	return cost + c.computeChildrenCost(candidate)
}

func (c *coster) computeIndexJoinCost(candidate *memo.BestExpr, logical *props.Logical) memo.Cost {
	leftRowCount := c.mem.BestExprLogical(candidate.Child(0)).Relational.Stats.RowCount
	def := candidate.Private(c.mem).(*memo.IndexJoinDef)

	cost := c.mem.BestExprCost(candidate.Child(0))

	// The rows in the (left) input are used to probe into the (right) table.
	// Since the matching rows in the table may not all be in the same range, this
	// counts as random I/O.
	perRowCost := cpuCostFactor + randIOCostFactor +
		c.rowScanCost(def.Table, opt.PrimaryIndex, def.Cols.Len())
	cost += memo.Cost(leftRowCount) * perRowCost
	return cost
}

func (c *coster) computeLookupJoinCost(candidate *memo.BestExpr, logical *props.Logical) memo.Cost {
	leftRowCount := c.mem.BestExprLogical(candidate.Child(0)).Relational.Stats.RowCount
	def := candidate.Private(c.mem).(*memo.LookupJoinDef)

	cost := c.mem.BestExprCost(candidate.Child(0))

	// The rows in the (left) input are used to probe into the (right) table.
	// Since the matching rows in the table may not all be in the same range, this
	// counts as random I/O.
	perLookupCost := memo.Cost(randIOCostFactor)
	cost += memo.Cost(leftRowCount) * perLookupCost

	// Each lookup might retrieve many rows; add the IO cost of retrieving the
	// rows (relevant when we expect many resulting rows per lookup) and the CPU
	// cost of emitting the rows.
	perRowCost := seqIOCostFactor + c.rowScanCost(def.Table, def.Index, def.LookupCols.Len())
	cost += memo.Cost(logical.Relational.Stats.RowCount) * perRowCost
	return cost
}

func (c *coster) computeChildrenCost(candidate *memo.BestExpr) memo.Cost {
	var cost memo.Cost
	for i := 0; i < candidate.ChildCount(); i++ {
		cost += c.mem.BestExprCost(candidate.Child(i))
	}
	return cost
}
