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
	"math/rand"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/ordering"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props/physical"
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
	ComputeCost(candidate memo.RelExpr, required *physical.Required) memo.Cost
}

// coster encapsulates the default cost model for the optimizer. The coster
// assigns an estimated cost to each expression in the memo so that the
// optimizer can choose the lowest cost expression tree. The estimated cost is
// a best-effort approximation of the actual cost of execution, based on table
// and index statistics that are propagated throughout the logical expression
// tree.
type coster struct {
	mem *memo.Memo

	// perturbation indicates how much to randomly perturb the cost. It is used
	// to generate alternative plans for testing. For example, if perturbation is
	// 0.5, and the estimated cost of an expression is c, the cost returned by
	// ComputeCost will be in the range [c - 0.5 * c, c + 0.5 * c).
	perturbation float64
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
func (c *coster) Init(mem *memo.Memo, perturbation float64) {
	c.mem = mem
	c.perturbation = perturbation
}

// computeCost calculates the estimated cost of the candidate best expression,
// based on its logical properties as well as the cost of its children. Each
// expression's cost must always be >= the total costs of its children, so that
// branch-and-bound pruning will work properly.
//
// Note: each custom function to compute the cost of an operator calculates
// the cost based on Big-O estimated complexity. Most constant factors are
// ignored for now.
func (c *coster) ComputeCost(candidate memo.RelExpr, required *physical.Required) memo.Cost {
	var cost memo.Cost
	switch candidate.Op() {
	case opt.SortOp:
		cost = c.computeSortCost(candidate.(*memo.SortExpr), required)

	case opt.ScanOp:
		cost = c.computeScanCost(candidate.(*memo.ScanExpr), required)

	case opt.VirtualScanOp:
		cost = c.computeVirtualScanCost(candidate.(*memo.VirtualScanExpr))

	case opt.SelectOp:
		cost = c.computeSelectCost(candidate.(*memo.SelectExpr))

	case opt.ProjectOp:
		cost = c.computeProjectCost(candidate.(*memo.ProjectExpr))

	case opt.ValuesOp:
		cost = c.computeValuesCost(candidate.(*memo.ValuesExpr))

	case opt.InnerJoinOp, opt.LeftJoinOp, opt.RightJoinOp, opt.FullJoinOp,
		opt.SemiJoinOp, opt.AntiJoinOp, opt.InnerJoinApplyOp, opt.LeftJoinApplyOp,
		opt.RightJoinApplyOp, opt.FullJoinApplyOp, opt.SemiJoinApplyOp, opt.AntiJoinApplyOp:
		// All join ops use hash join by default.
		cost = c.computeHashJoinCost(candidate)

	case opt.MergeJoinOp:
		cost = c.computeMergeJoinCost(candidate.(*memo.MergeJoinExpr))

	case opt.IndexJoinOp:
		cost = c.computeIndexJoinCost(candidate.(*memo.IndexJoinExpr))

	case opt.LookupJoinOp:
		cost = c.computeLookupJoinCost(candidate.(*memo.LookupJoinExpr))

	case opt.UnionOp, opt.IntersectOp, opt.ExceptOp,
		opt.UnionAllOp, opt.IntersectAllOp, opt.ExceptAllOp:
		cost = c.computeSetCost(candidate)

	case opt.GroupByOp, opt.ScalarGroupByOp, opt.DistinctOnOp:
		cost = c.computeGroupingCost(candidate, required)

	case opt.LimitOp:
		cost = c.computeLimitCost(candidate.(*memo.LimitExpr))

	case opt.OffsetOp:
		cost = c.computeOffsetCost(candidate.(*memo.OffsetExpr))

	case opt.RowNumberOp:
		cost = c.computeRowNumberCost(candidate.(*memo.RowNumberExpr))

	case opt.ProjectSetOp:
		cost = c.computeProjectSetCost(candidate.(*memo.ProjectSetExpr))

	case opt.ExplainOp:
		// Technically, the cost of an Explain operation is independent of the cost
		// of the underlying plan. However, we want to explain the plan we would get
		// without EXPLAIN, i.e. the lowest cost plan. So do nothing special to get
		// default behavior.
	}
	if !cost.Less(memo.MaxCost) {
		// Optsteps uses MaxCost to suppress nodes in the memo. When a node with
		// MaxCost is added to the memo, it can lead to an obscure crash with an
		// unknown node. We'd rather detect this early.
		panic(fmt.Sprintf("node %s with MaxCost added to the memo", candidate.Op()))
	}

	if c.perturbation != 0 {
		// Don't perturb the cost if we are forcing an index.
		if cost != hugeCost {
			// Get a random value in the range [-1.0, 1.0)
			multiplier := 2*rand.Float64() - 1

			// If perturbation is p, and the estimated cost of an expression is c,
			// the new cost is in the range [max(0, c - pc), c + pc). For example,
			// if p=1.5, the new cost is in the range [0, c + 1.5 * c).
			cost += cost * memo.Cost(c.perturbation*multiplier)
			// The cost must always be >= 0.
			if cost < 0 {
				cost = 0
			}
		}
	}

	return cost
}

func (c *coster) computeSortCost(sort *memo.SortExpr, required *physical.Required) memo.Cost {
	// We calculate a per-row cost and multiply by (1 + log2(rowCount)).
	// The constant term is necessary for cases where the estimated row count is
	// very small.
	// TODO(rytaft): This is the cost of a local, in-memory sort. When a
	// certain amount of memory is used, distsql switches to a disk-based sort
	// with a temp RocksDB store.
	rowCount := sort.Relational().Stats.RowCount
	perRowCost := c.rowSortCost(len(required.Ordering.Columns))
	cost := memo.Cost(rowCount) * perRowCost
	if rowCount > 1 {
		cost *= (1 + memo.Cost(math.Log2(rowCount)))
	}

	return cost
}

func (c *coster) computeScanCost(scan *memo.ScanExpr, required *physical.Required) memo.Cost {
	// Scanning an index with a few columns is faster than scanning an index with
	// many columns. Ideally, we would want to use statistics about the size of
	// each column. In lieu of that, use the number of columns.
	if scan.Flags.ForceIndex && scan.Flags.Index != scan.Index {
		// If we are forcing an index, any other index has a very high cost. In
		// practice, this will only happen when this is a primary index scan.
		return hugeCost
	}
	rowCount := scan.Relational().Stats.RowCount
	perRowCost := c.rowScanCost(scan.Table, scan.Index, scan.Cols.Len())

	if ordering.ScanIsReverse(scan, &required.Ordering) {
		if rowCount > 1 {
			// Need to do binary search to seek to the previous row.
			perRowCost += memo.Cost(math.Log2(rowCount)) * cpuCostFactor
		}
	}
	return memo.Cost(rowCount) * (seqIOCostFactor + perRowCost)
}

func (c *coster) computeVirtualScanCost(scan *memo.VirtualScanExpr) memo.Cost {
	// Virtual tables are generated on-the-fly according to system metadata that
	// is assumed to be in memory.
	rowCount := memo.Cost(scan.Relational().Stats.RowCount)
	return rowCount * cpuCostFactor
}

func (c *coster) computeSelectCost(sel *memo.SelectExpr) memo.Cost {
	// The filter has to be evaluated on each input row.
	inputRowCount := sel.Input.Relational().Stats.RowCount
	cost := memo.Cost(inputRowCount) * cpuCostFactor
	return cost
}

func (c *coster) computeProjectCost(prj *memo.ProjectExpr) memo.Cost {
	// Each synthesized column causes an expression to be evaluated on each row.
	rowCount := prj.Relational().Stats.RowCount
	synthesizedColCount := len(prj.Projections)
	cost := memo.Cost(rowCount) * memo.Cost(synthesizedColCount) * cpuCostFactor

	// Add the CPU cost of emitting the rows.
	cost += memo.Cost(rowCount) * cpuCostFactor
	return cost
}

func (c *coster) computeValuesCost(values *memo.ValuesExpr) memo.Cost {
	return memo.Cost(values.Relational().Stats.RowCount) * cpuCostFactor
}

func (c *coster) computeHashJoinCost(join memo.RelExpr) memo.Cost {
	leftRowCount := join.Child(0).(memo.RelExpr).Relational().Stats.RowCount
	rightRowCount := join.Child(1).(memo.RelExpr).Relational().Stats.RowCount

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
	cost += memo.Cost(join.Relational().Stats.RowCount) * cpuCostFactor
	return cost
}

func (c *coster) computeMergeJoinCost(join *memo.MergeJoinExpr) memo.Cost {
	leftRowCount := join.Left.Relational().Stats.RowCount
	rightRowCount := join.Right.Relational().Stats.RowCount

	cost := memo.Cost(leftRowCount+rightRowCount) * cpuCostFactor

	// Add the CPU cost of emitting the rows.
	// TODO(radu): ideally we would have an estimate of how many rows we actually
	// have to run the ON condition on.
	cost += memo.Cost(join.Relational().Stats.RowCount) * cpuCostFactor
	return cost
}

func (c *coster) computeIndexJoinCost(join *memo.IndexJoinExpr) memo.Cost {
	leftRowCount := join.Input.Relational().Stats.RowCount

	// The rows in the (left) input are used to probe into the (right) table.
	// Since the matching rows in the table may not all be in the same range, this
	// counts as random I/O.
	perRowCost := cpuCostFactor + randIOCostFactor +
		c.rowScanCost(join.Table, opt.PrimaryIndex, join.Cols.Len())
	return memo.Cost(leftRowCount) * perRowCost
}

func (c *coster) computeLookupJoinCost(join *memo.LookupJoinExpr) memo.Cost {
	leftRowCount := join.Input.Relational().Stats.RowCount

	// The rows in the (left) input are used to probe into the (right) table.
	// Since the matching rows in the table may not all be in the same range, this
	// counts as random I/O.
	perLookupCost := memo.Cost(randIOCostFactor)
	cost := memo.Cost(leftRowCount) * perLookupCost

	// Each lookup might retrieve many rows; add the IO cost of retrieving the
	// rows (relevant when we expect many resulting rows per lookup) and the CPU
	// cost of emitting the rows.
	numLookupCols := join.Cols.Difference(join.Input.Relational().OutputCols).Len()
	perRowCost := seqIOCostFactor + c.rowScanCost(join.Table, join.Index, numLookupCols)
	cost += memo.Cost(join.Relational().Stats.RowCount) * perRowCost
	return cost
}

func (c *coster) computeSetCost(set memo.RelExpr) memo.Cost {
	// Add the CPU cost of emitting the rows.
	cost := memo.Cost(set.Relational().Stats.RowCount) * cpuCostFactor

	// A set operation must process every row from both tables once.
	// UnionAll can avoid any extra computation, but all other set operations
	// must perform a hash table lookup or update for each input row.
	if set.Op() != opt.UnionAllOp {
		leftRowCount := set.Child(0).(memo.RelExpr).Relational().Stats.RowCount
		rightRowCount := set.Child(1).(memo.RelExpr).Relational().Stats.RowCount
		cost += memo.Cost(leftRowCount+rightRowCount) * cpuCostFactor
	}

	return cost
}

func (c *coster) computeGroupingCost(grouping memo.RelExpr, required *physical.Required) memo.Cost {
	// Add the CPU cost of emitting the rows.
	cost := memo.Cost(grouping.Relational().Stats.RowCount) * cpuCostFactor

	// GroupBy must process each input row once. Cost per row depends on the
	// number of grouping columns and the number of aggregates.
	inputRowCount := grouping.Child(0).(memo.RelExpr).Relational().Stats.RowCount
	aggsCount := grouping.Child(1).ChildCount()
	private := grouping.Private().(*memo.GroupingPrivate)
	groupingColCount := private.GroupingCols.Len()
	cost += memo.Cost(inputRowCount) * memo.Cost(aggsCount+groupingColCount) * cpuCostFactor

	if groupingColCount > 0 {
		// Add a cost that reflects the use of a hash table - unless we are doing a
		// streaming aggregation where all the grouping columns are ordered; we
		// interpolate linearly if only part of the grouping columns are ordered.
		//
		// The cost is chosen so that it's always less than the cost to sort the
		// input.
		hashCost := memo.Cost(inputRowCount) * cpuCostFactor
		n := ordering.StreamingGroupingCols(private, &required.Ordering).Len()
		// n = 0:                factor = 1
		// n = groupingColCount: factor = 0
		hashCost *= 1 - memo.Cost(n)/memo.Cost(groupingColCount)
		cost += hashCost
	}

	return cost
}

func (c *coster) computeLimitCost(limit *memo.LimitExpr) memo.Cost {
	// Add the CPU cost of emitting the rows.
	cost := memo.Cost(limit.Relational().Stats.RowCount) * cpuCostFactor
	return cost
}

func (c *coster) computeOffsetCost(offset *memo.OffsetExpr) memo.Cost {
	// Add the CPU cost of emitting the rows.
	cost := memo.Cost(offset.Relational().Stats.RowCount) * cpuCostFactor
	return cost
}

func (c *coster) computeRowNumberCost(rowNum *memo.RowNumberExpr) memo.Cost {
	// Add the CPU cost of emitting the rows.
	cost := memo.Cost(rowNum.Relational().Stats.RowCount) * cpuCostFactor
	return cost
}

func (c *coster) computeProjectSetCost(projectSet *memo.ProjectSetExpr) memo.Cost {
	// Add the CPU cost of emitting the rows.
	cost := memo.Cost(projectSet.Relational().Stats.RowCount) * cpuCostFactor
	return cost
}

// rowSortCost is the CPU cost to sort one row, which depends on the number of
// columns in the sort key.
func (c *coster) rowSortCost(numKeyCols int) memo.Cost {
	// Sorting involves comparisons on the key columns, but the cost isn't
	// directly proportional: we only compare the second column if the rows are
	// equal on the first column; and so on. We also account for a fixed
	// "non-comparison" cost related to processing the
	// row. The formula is:
	//
	//   cpuCostFactor * [ 1 + Sum eqProb^(i-1) with i=1 to numKeyCols ]
	//
	const eqProb = 0.1
	cost := cpuCostFactor
	for i, c := 0, cpuCostFactor; i < numKeyCols; i, c = i+1, c*eqProb {
		// c is cpuCostFactor * eqProb^i.
		cost += c
	}

	// There is a fixed "non-comparison" cost and a comparison cost proportional
	// to the key columns. Note that the cost has to be high enough so that a
	// sort is almost always more expensive than a reverse scan or an index scan.
	return memo.Cost(cost)
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
