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

	// hugeCost is used with expressions we want to avoid; for example: scanning
	// an index that doesn't match a "force index" flag.
	hugeCost = 1e100
)

// Init initializes a new coster structure with the given memo.
func (c *coster) Init(mem *memo.Memo) {
	c.mem = mem
}

// computeCost calculates the estimated cost of the candidate best expression,
// based on its logical properties as well as the cost of its children. Each
// expression's cost must always be >= the total costs of its children, so that
// branch-and-bound pruning will work properly.
//
// Note: each custom function to compute the cost of an operator calculates
// the cost based on Big-O estimated complexity. Most constant factors are
// ignored for now.
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

	case opt.ProjectOp:
		cost = c.computeProjectCost(candidate, logical)

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

	case opt.UnionOp, opt.IntersectOp, opt.ExceptOp,
		opt.UnionAllOp, opt.IntersectAllOp, opt.ExceptAllOp:
		cost = c.computeSetOpCost(candidate, logical)

	case opt.GroupByOp, opt.ScalarGroupByOp, opt.DistinctOnOp:
		cost = c.computeGroupByCost(candidate, logical)

	case opt.LimitOp:
		cost = c.computeLimitCost(candidate, logical)

	case opt.OffsetOp:
		cost = c.computeOffsetCost(candidate, logical)

	case opt.RowNumberOp:
		cost = c.computeRowNumberCost(candidate, logical)

	case opt.ZipOp:
		cost = c.computeZipCost(candidate, logical)

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
	// Add the CPU cost of emitting the rows.
	rowCount := logical.Relational.Stats.RowCount()
	cost := memo.Cost(rowCount) * cpuCostFactor

	if rowCount > 1 {
		// TODO(rytaft): This is the cost of a local, in-memory sort. When a
		// certain amount of memory is used, distsql switches to a disk-based sort
		// with a temp RocksDB store.
		physical := c.mem.LookupPhysicalProps(candidate.Required())
		perRowCost := c.rowSortCost(len(physical.Ordering.Columns))
		cost += memo.Cost(rowCount*math.Log2(rowCount)) * perRowCost
	}

	return cost + c.computeChildrenCost(candidate)
}

func (c *coster) computeScanCost(candidate *memo.BestExpr, logical *props.Logical) memo.Cost {
	// Scanning an index with a few columns is faster than scanning an index with
	// many columns. Ideally, we would want to use statistics about the size of
	// each column. In lieu of that, use the number of columns.
	def := candidate.Private(c.mem).(*memo.ScanOpDef)
	if def.Flags.ForceIndex && def.Flags.Index != def.Index {
		// If we are forcing an index, any other index has a very high cost. In
		// practice, this will only happen when this is a primary index scan.
		return hugeCost
	}
	rowCount := logical.Relational.Stats.RowCount()
	perRowCost := c.rowScanCost(def.Table, def.Index, def.Cols.Len())

	props := c.mem.LookupPhysicalProps(candidate.Required())
	if _, reverse := def.CanProvideOrdering(c.mem.Metadata(), &props.Ordering); reverse {
		if rowCount > 1 {
			// Need to do binary search to seek to the previous row.
			perRowCost += memo.Cost(math.Log2(rowCount)) * cpuCostFactor
		}
	}
	return memo.Cost(rowCount) * (seqIOCostFactor + perRowCost)
}

func (c *coster) computeVirtualScanCost(
	candidate *memo.BestExpr, logical *props.Logical,
) memo.Cost {
	// Virtual tables are generated on-the-fly according to system metadata that
	// is assumed to be in memory.
	rowCount := memo.Cost(logical.Relational.Stats.RowCount())
	return rowCount * cpuCostFactor
}

// rowSortCost is the CPU cost to sort one row, which depends on the number of
// columns in the sort key.
func (c *coster) rowSortCost(numKeyCols int) memo.Cost {
	// There is a fixed "non-comparison" cost and a comparison cost proportional
	// to the key columns. Note that the cost has to be high enough so that a
	// sort is almost always more expensive than a reverse scan or an index scan.
	return (1 + memo.Cost(numKeyCols)) * cpuCostFactor
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
	inputRowCount := c.mem.BestExprLogical(candidate.Child(0)).Relational.Stats.RowCount()
	cost := memo.Cost(inputRowCount) * cpuCostFactor
	return cost + c.computeChildrenCost(candidate)
}

func (c *coster) computeProjectCost(candidate *memo.BestExpr, logical *props.Logical) memo.Cost {
	// Each synthesized column causes an expression to be evaluated on each row.
	rowCount := logical.Relational.Stats.RowCount()
	synthesizedColCount := memo.MakeExprView(c.mem, candidate.Child(1)).ChildCount()
	cost := memo.Cost(rowCount) * memo.Cost(synthesizedColCount) * cpuCostFactor

	// Add the CPU cost of emitting the rows.
	cost += memo.Cost(rowCount) * cpuCostFactor
	return cost + c.computeChildrenCost(candidate)
}

func (c *coster) computeValuesCost(candidate *memo.BestExpr, logical *props.Logical) memo.Cost {
	return memo.Cost(logical.Relational.Stats.RowCount()) * cpuCostFactor
}

func (c *coster) computeHashJoinCost(candidate *memo.BestExpr, logical *props.Logical) memo.Cost {
	leftRowCount := c.mem.BestExprLogical(candidate.Child(0)).Relational.Stats.RowCount()
	rightRowCount := c.mem.BestExprLogical(candidate.Child(1)).Relational.Stats.RowCount()

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
	cost += memo.Cost(logical.Relational.Stats.RowCount()) * cpuCostFactor
	return cost + c.computeChildrenCost(candidate)
}

func (c *coster) computeMergeJoinCost(candidate *memo.BestExpr, logical *props.Logical) memo.Cost {
	leftRowCount := c.mem.BestExprLogical(candidate.Child(0)).Relational.Stats.RowCount()
	rightRowCount := c.mem.BestExprLogical(candidate.Child(1)).Relational.Stats.RowCount()

	cost := memo.Cost(leftRowCount+rightRowCount) * cpuCostFactor

	// Add the CPU cost of emitting the rows.
	// TODO(radu): ideally we would have an estimate of how many rows we actually
	// have to run the ON condition on.
	cost += memo.Cost(logical.Relational.Stats.RowCount()) * cpuCostFactor
	return cost + c.computeChildrenCost(candidate)
}

func (c *coster) computeIndexJoinCost(candidate *memo.BestExpr, logical *props.Logical) memo.Cost {
	leftRowCount := c.mem.BestExprLogical(candidate.Child(0)).Relational.Stats.RowCount()
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
	leftRowCount := c.mem.BestExprLogical(candidate.Child(0)).Relational.Stats.RowCount()
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
	cost += memo.Cost(logical.Relational.Stats.RowCount()) * perRowCost
	return cost
}

func (c *coster) computeSetOpCost(candidate *memo.BestExpr, logical *props.Logical) memo.Cost {
	// Add the CPU cost of emitting the rows.
	cost := memo.Cost(logical.Relational.Stats.RowCount()) * cpuCostFactor

	// A set operation must process every row from both tables once.
	// UnionAll can avoid any extra computation, but all other set operations
	// must perform a hash table lookup or update for each input row.
	if candidate.Operator() != opt.UnionAllOp {
		leftRowCount := c.mem.BestExprLogical(candidate.Child(0)).Relational.Stats.RowCount()
		rightRowCount := c.mem.BestExprLogical(candidate.Child(1)).Relational.Stats.RowCount()
		cost += memo.Cost(leftRowCount+rightRowCount) * cpuCostFactor
	}

	return cost + c.computeChildrenCost(candidate)
}

func (c *coster) computeGroupByCost(candidate *memo.BestExpr, logical *props.Logical) memo.Cost {
	// Add the CPU cost of emitting the rows.
	cost := memo.Cost(logical.Relational.Stats.RowCount()) * cpuCostFactor

	// GroupBy must process each input row once. Cost per row depends on the
	// number of grouping columns and the number of aggregates.
	inputRowCount := c.mem.BestExprLogical(candidate.Child(0)).Relational.Stats.RowCount()
	aggsCount := memo.MakeExprView(c.mem, candidate.Child(1)).ChildCount()
	def := candidate.Private(c.mem).(*memo.GroupByDef)
	groupingColCount := def.GroupingCols.Len()
	cost += memo.Cost(inputRowCount) * memo.Cost(aggsCount+groupingColCount) * cpuCostFactor

	// TODO(radu): take into account how many grouping columns we have an ordering
	// on for DistinctOn.

	return cost + c.computeChildrenCost(candidate)
}

func (c *coster) computeLimitCost(candidate *memo.BestExpr, logical *props.Logical) memo.Cost {
	// Add the CPU cost of emitting the rows.
	cost := memo.Cost(logical.Relational.Stats.RowCount()) * cpuCostFactor
	return cost + c.computeChildrenCost(candidate)
}

func (c *coster) computeOffsetCost(candidate *memo.BestExpr, logical *props.Logical) memo.Cost {
	// Add the CPU cost of emitting the rows.
	cost := memo.Cost(logical.Relational.Stats.RowCount()) * cpuCostFactor
	return cost + c.computeChildrenCost(candidate)
}

func (c *coster) computeRowNumberCost(candidate *memo.BestExpr, logical *props.Logical) memo.Cost {
	// Add the CPU cost of emitting the rows.
	cost := memo.Cost(logical.Relational.Stats.RowCount()) * cpuCostFactor
	return cost + c.computeChildrenCost(candidate)
}

func (c *coster) computeZipCost(candidate *memo.BestExpr, logical *props.Logical) memo.Cost {
	// Add the CPU cost of emitting the rows.
	cost := memo.Cost(logical.Relational.Stats.RowCount()) * cpuCostFactor
	return cost + c.computeChildrenCost(candidate)
}

func (c *coster) computeChildrenCost(candidate *memo.BestExpr) memo.Cost {
	var cost memo.Cost
	for i := 0; i < candidate.ChildCount(); i++ {
		cost += c.mem.BestExprCost(candidate.Child(i))
	}
	return cost
}
