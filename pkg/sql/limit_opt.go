// Copyright 2017 The Cockroach Authors.
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
//
// Author: Raphael 'kena' Poss (knz@cockroachlabs.com)

package sql

import (
	"fmt"
	"math"
)

// applyLimit tells this node to optimize things under the assumption that
// we will only need the first `numRows` rows.
//
// The special value math.MaxInt64 indicates "no limit".
//
// If soft is true, this is a "soft" limit and is only a hint; the node must
// still be able to produce all results if requested.
//
// If soft is false, this is a "hard" limit and is a promise that Next will
// never be called more than numRows times.
//
// The action of calling this method triggers limit-based query plan
// optimizations, e.g. in expandSelectNode(). The primary user is
// limitNode.Start(ctx) after it has fully evaluated the limit and
// offset expressions. EXPLAIN also does this, see expandPlan() for
// explainPlanNode.
//
// TODO(radu) Arguably, this interface has room for improvement.  A
// limitNode may have a hard limit locally which is larger than the
// soft limit propagated up by nodes downstream. We may want to
// improve this API to pass both the soft and hard limit.
func applyLimit(plan planNode, numRows int64, soft bool) {
	switch n := plan.(type) {
	case *scanNode:
		// Either a limitNode or EXPLAIN is pushing a limit down onto this
		// node. The special value math.MaxInt64 means "no limit".
		if !n.disableBatchLimits && numRows != math.MaxInt64 {
			if soft {
				n.hardLimit = 0
				n.softLimit = numRows
			} else {
				n.hardLimit = numRows
				n.softLimit = 0
			}
		}

	case *limitNode:
		// A higher-level limitNode or EXPLAIN is pushing a limit down onto
		// this node. Accept it unless the local limit is definitely
		// smaller, in which case we propagate that as a hard limit instead.
		// TODO(radu): we may get a smaller "soft" limit from the upper node
		// and we may have a larger "hard" limit locally. In general, it's
		// not clear which of those results in less work.
		if !n.evaluated {
			n.estimateLimit()
		}
		hintCount := numRows
		if hintCount > n.count {
			hintCount = n.count
			soft = false
		}
		applyLimit(n.plan, getLimit(hintCount, n.offset), soft)

	case *sortNode:
		if n.needSort && numRows != math.MaxInt64 {
			v := n.p.newContainerValuesNode(n.plan.Columns(), int(numRows))
			v.ordering = n.ordering
			if soft {
				n.sortStrategy = newIterativeSortStrategy(v)
			} else {
				n.sortStrategy = newSortTopKStrategy(v, numRows)
			}
		}
		if n.needSort {
			// We can't propagate the limit, because the sort
			// potentially needs all rows.
			numRows = math.MaxInt64
			soft = true
		}
		applyLimit(n.plan, numRows, soft)

	case *groupNode:
		if n.needOnlyOneRow {
			// We have a single MIN/MAX function and the underlying plan's
			// ordering matches the function. We only need to retrieve one row.
			applyLimit(n.plan, 1, false /* !soft */)
		} else {
			setUnlimited(n.plan)
		}

	case *indexJoinNode:
		applyLimit(n.index, numRows, soft)
		setUnlimited(n.table)

	case *unionNode:
		applyLimit(n.right, numRows, true)
		applyLimit(n.left, numRows, true)

	case *distinctNode:
		applyLimit(n.plan, numRows, true)

	case *filterNode:
		applyLimit(n.source.plan, numRows, soft || !isFilterTrue(n.filter))

	case *renderNode:
		applyLimit(n.source.plan, numRows, soft)

	case *windowNode:
		setUnlimited(n.plan)

	case *joinNode:
		setUnlimited(n.left.plan)
		setUnlimited(n.right.plan)

	case *ordinalityNode:
		applyLimit(n.source, numRows, soft)

	case *deleteNode:
		setUnlimited(n.run.rows)
	case *updateNode:
		setUnlimited(n.run.rows)
	case *insertNode:
		setUnlimited(n.run.rows)
	case *createTableNode:
		if n.sourcePlan != nil {
			applyLimit(n.sourcePlan, numRows, soft)
		}
	case *createViewNode:
		setUnlimited(n.sourcePlan)
	case *explainDebugNode:
		setUnlimited(n.plan)
	case *explainDistSQLNode:
		setUnlimited(n.plan)
	case *explainTraceNode:
		setUnlimited(n.plan)
	case *explainPlanNode:
		if n.expanded {
			setUnlimited(n.plan)
		}

	case *delayedNode:
		if n.plan != nil {
			setUnlimited(n.plan)
		}

	case *splitNode:
		setUnlimited(n.rows)

	case *relocateNode:
		setUnlimited(n.rows)

	case *valuesNode:
	case *alterTableNode:
	case *copyNode:
	case *createDatabaseNode:
	case *createIndexNode:
	case *createUserNode:
	case *dropDatabaseNode:
	case *dropIndexNode:
	case *dropTableNode:
	case *dropViewNode:
	case *emptyNode:
	case *hookFnNode:
	case *valueGenerator:
	case *showRangesNode:
	case *scatterNode:

	default:
		panic(fmt.Sprintf("unhandled node type: %T", plan))
	}
}

func setUnlimited(plan planNode) {
	applyLimit(plan, math.MaxInt64, true)
}

// getLimit computes the actual number of rows to request from the
// data source to honour both the required count and offset together.
// This also ensures that the resulting number of rows does not
// overflow.
func getLimit(count, offset int64) int64 {
	if offset > math.MaxInt64-count {
		count = math.MaxInt64 - offset
	}
	return count + offset
}
