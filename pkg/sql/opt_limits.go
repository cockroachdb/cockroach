// Copyright 2017 The Cockroach Authors.
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
	"fmt"
	"math"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
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
// limitNode.Start(params) after it has fully evaluated the limit and
// offset expressions. EXPLAIN also does this, see expandPlan() for
// explainPlanNode.
//
// TODO(radu): Arguably, this interface has room for improvement.  A
// limitNode may have a hard limit locally which is larger than the
// soft limit propagated up by nodes downstream. We may want to
// improve this API to pass both the soft and hard limit.
func (p *planner) applyLimit(plan planNode, numRows int64, soft bool) {
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
		// this node. Prefer the local "hard" limit, unless the limit pushed
		// down is "hard" and smaller than the local limit.
		//
		// TODO(radu): we may get a smaller "soft" limit from the upper node
		// and we may have a larger "hard" limit locally. In general, it's
		// not clear which of those results in less work.
		if !n.evaluated {
			n.estimateLimit()
		}
		count := n.count
		if !soft && numRows < count {
			count = numRows
		}
		p.applyLimit(n.plan, getLimit(count, n.offset), false /* soft */)

	case *sortNode:
		// We can't propagate the limit, because the sort potentially needs all
		// rows.
		p.setUnlimited(n.plan)

	case *groupNode:
		if n.needOnlyOneRow {
			// We have a single MIN/MAX function and the underlying plan's
			// ordering matches the function. We only need to retrieve one row.
			p.applyLimit(n.plan, 1, false /* soft */)
		} else {
			p.setUnlimited(n.plan)
		}

	case *indexJoinNode:
		// If we have a limit in the table node (i.e. post-index-join), the
		// limit in the index is soft.
		p.applyLimit(n.input, numRows, soft || !isFilterTrue(n.table.filter))
		p.setUnlimited(n.table)

	case *unionNode:
		if n.right != nil {
			p.applyLimit(n.right, numRows, true)
		}
		if n.left != nil {
			p.applyLimit(n.left, numRows, true)
		}

	case *distinctNode:
		p.applyLimit(n.plan, numRows, true)

	case *filterNode:
		p.applyLimit(n.source.plan, numRows, soft || !isFilterTrue(n.filter))

	case *renderNode:
		p.applyLimit(n.source.plan, numRows, soft)

	case *windowNode:
		p.setUnlimited(n.plan)

	case *max1RowNode:
		p.setUnlimited(n.plan)

	case *joinNode:
		p.setUnlimited(n.left.plan)
		p.setUnlimited(n.right.plan)

	case *ordinalityNode:
		p.applyLimit(n.source, numRows, soft)

	case *spoolNode:
		if !soft {
			n.hardLimit = numRows
		}
		p.applyLimit(n.source, numRows, soft)

	case *delayedNode:
		if n.plan != nil {
			p.applyLimit(n.plan, numRows, soft)
		}

	case *projectSetNode:
		p.applyLimit(n.source, numRows, true)

	case *rowCountNode:
		p.setUnlimited(n.source)
	case *serializeNode:
		p.setUnlimited(n.source)
	case *deleteNode:
		// A limit does not propagate into a mutation. When there is a
		// surrounding query, the mutation must run to completion even if
		// the surrounding query only uses parts of its results.
		p.setUnlimited(n.source)
	case *updateNode:
		// A limit does not propagate into a mutation. When there is a
		// surrounding query, the mutation must run to completion even if
		// the surrounding query only uses parts of its results.
		p.setUnlimited(n.source)
	case *insertNode:
		// A limit does not propagate into a mutation. When there is a
		// surrounding query, the mutation must run to completion even if
		// the surrounding query only uses parts of its results.
		p.setUnlimited(n.source)
	case *upsertNode:
		// A limit does not propagate into a mutation. When there is a
		// surrounding query, the mutation must run to completion even if
		// the surrounding query only uses parts of its results.
		p.setUnlimited(n.source)
	case *createTableNode:
		if n.sourcePlan != nil {
			p.applyLimit(n.sourcePlan, numRows, soft)
		}
	case *explainDistSQLNode:
		// EXPLAIN ANALYZE is special: it handles its own limit propagation, since
		// it fully executes during startExec.
		if !n.analyze {
			p.setUnlimited(n.plan)
		}
	case *showTraceReplicaNode:
		p.setUnlimited(n.plan)
	case *explainPlanNode:
		p.setUnlimited(n.plan)

	case *splitNode:
		p.setUnlimited(n.rows)

	case *unsplitNode:
		p.setUnlimited(n.rows)

	case *relocateNode:
		p.setUnlimited(n.rows)

	case *cancelQueriesNode:
		p.setUnlimited(n.rows)

	case *cancelSessionsNode:
		p.setUnlimited(n.rows)

	case *controlJobsNode:
		p.setUnlimited(n.rows)

	case *errorIfRowsNode:
		p.setUnlimited(n.plan)

	case *bufferNode:
		p.setUnlimited(n.plan)

	case *exportNode:
		p.setUnlimited(n.source)

	case *valuesNode:
	case *virtualTableNode:
	case *alterIndexNode:
	case *alterTableNode:
	case *alterSequenceNode:
	case *alterUserSetPasswordNode:
	case *deleteRangeNode:
	case *renameColumnNode:
	case *renameDatabaseNode:
	case *renameIndexNode:
	case *renameTableNode:
	case *scrubNode:
	case *truncateNode:
	case *changePrivilegesNode:
	case *commentOnColumnNode:
	case *commentOnDatabaseNode:
	case *commentOnIndexNode:
	case *commentOnTableNode:
	case *createDatabaseNode:
	case *createIndexNode:
	case *CreateUserNode:
	case *createViewNode:
	case *createSequenceNode:
	case *createStatsNode:
	case *dropDatabaseNode:
	case *dropIndexNode:
	case *dropTableNode:
	case *dropViewNode:
	case *dropSequenceNode:
	case *DropUserNode:
	case *explainVecNode:
	case *zeroNode:
	case *unaryNode:
	case *hookFnNode:
	case *sequenceSelectNode:
	case *setVarNode:
	case *setClusterSettingNode:
	case *setZoneConfigNode:
	case *showFingerprintsNode:
	case *showTraceNode:
	case *scatterNode:
	case *scanBufferNode:
	case *recursiveCTENode:
	case *unsplitAllNode:

	case *applyJoinNode, *lookupJoinNode, *zigzagJoinNode, *saveTableNode:
		// These nodes are only planned by the optimizer.

	default:
		panic(fmt.Sprintf("unhandled node type: %T", plan))
	}
}

func (p *planner) setUnlimited(plan planNode) {
	p.applyLimit(plan, math.MaxInt64, true)
}

// getLimit computes the actual number of rows to request from the
// data source to honor both the required count and offset together.
// This also ensures that the resulting number of rows does not
// overflow.
func getLimit(count, offset int64) int64 {
	if offset > math.MaxInt64-count {
		count = math.MaxInt64 - offset
	}
	return count + offset
}

func isFilterTrue(expr tree.TypedExpr) bool {
	return expr == nil || expr == tree.DBoolTrue
}
