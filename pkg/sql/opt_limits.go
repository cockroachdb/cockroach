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
)

// applySoftLimit tells this node to optimize things under the assumption that
// only the first `numRows` rows of its output will be needed. This is a "soft"
// limit and is only a hint; the node must still be able to produce all results
// if required. applySoftLimit will propagate the soft limit information down
// through the tree as far as possible. If it can be propagated all the way down
// to a scanNode, it is used to set the node's `softLimit` (replacing any
// existing `hardLimit` or `softLimit`).
//
// applySoftLimit should still be called on a node even if all rows of its output
// will be needed, since a soft limit might be possible further down the tree.
// The special value math.MaxInt64 indicates "no limit", and can be used to
// call applySoftLimit in this case (see propagateSoftLimits).
func (p *planner) applySoftLimit(plan planNode, numRows int64) {
	switch n := plan.(type) {
	case *scanNode:
		// The special value math.MaxInt64 means "no limit".
		if !n.disableBatchLimits && numRows != math.MaxInt64 {
			n.hardLimit = 0
			n.softLimit = numRows
		}

	case *limitNode:
		// A higher-level limitNode is pushing a soft limit down onto this
		// node. Prefer the local "hard" limit.
		//
		// TODO(radu): we may get a smaller "soft" limit from the upper node
		// and we may have a larger "hard" limit locally. In general, it's
		// not clear which of those results in less work.
		if !n.evaluated {
			n.estimateLimit()
		}
		p.applySoftLimit(n.plan, getLimit(n.count, n.offset))

	case *sortNode:
		// We can't propagate the limit, because the sort potentially needs all
		// rows.
		p.propagateSoftLimits(n.plan)

	case *groupNode:
		p.propagateSoftLimits(n.plan)

	case *indexJoinNode:
		p.applySoftLimit(n.input, numRows)
		p.propagateSoftLimits(n.table)

	case *unionNode:
		if n.right != nil {
			p.applySoftLimit(n.right, numRows)
		}
		if n.left != nil {
			p.applySoftLimit(n.left, numRows)
		}

	case *distinctNode:
		p.applySoftLimit(n.plan, numRows)

	case *filterNode:
		p.applySoftLimit(n.source.plan, numRows)

	case *renderNode:
		p.applySoftLimit(n.source.plan, numRows)

	case *windowNode:
		p.propagateSoftLimits(n.plan)

	case *max1RowNode:
		p.propagateSoftLimits(n.plan)

	case *joinNode:
		p.propagateSoftLimits(n.left.plan)
		p.propagateSoftLimits(n.right.plan)

	case *ordinalityNode:
		p.applySoftLimit(n.source, numRows)

	case *spoolNode:
		p.propagateSoftLimits(n.source)

	case *delayedNode:
		if n.plan != nil {
			p.propagateSoftLimits(n.plan)
		}

	case *projectSetNode:
		p.applySoftLimit(n.source, numRows)

	case *rowCountNode:
		p.propagateSoftLimits(n.source)
	case *serializeNode:
		p.propagateSoftLimits(n.source)
	case *deleteNode:
		// A limit does not propagate into a mutation. When there is a
		// surrounding query, the mutation must run to completion even if
		// the surrounding query only uses parts of its results.
		p.propagateSoftLimits(n.source)
	case *updateNode:
		// A limit does not propagate into a mutation. When there is a
		// surrounding query, the mutation must run to completion even if
		// the surrounding query only uses parts of its results.
		p.propagateSoftLimits(n.source)
	case *insertNode:
		// A limit does not propagate into a mutation. When there is a
		// surrounding query, the mutation must run to completion even if
		// the surrounding query only uses parts of its results.
		p.propagateSoftLimits(n.source)
	case *upsertNode:
		// A limit does not propagate into a mutation. When there is a
		// surrounding query, the mutation must run to completion even if
		// the surrounding query only uses parts of its results.
		p.propagateSoftLimits(n.source)
	case *createTableNode:
		if n.sourcePlan != nil {
			p.propagateSoftLimits(n.sourcePlan)
		}
	case *explainDistSQLNode:
		// EXPLAIN ANALYZE is special: it handles its own limit propagation, since
		// it fully executes during startExec.
		if !n.analyze {
			p.propagateSoftLimits(n.plan)
		}
	case *showTraceReplicaNode:
		p.propagateSoftLimits(n.plan)
	case *explainPlanNode:
		p.propagateSoftLimits(n.plan)

	case *splitNode:
		p.propagateSoftLimits(n.rows)

	case *unsplitNode:
		p.propagateSoftLimits(n.rows)

	case *relocateNode:
		p.propagateSoftLimits(n.rows)

	case *cancelQueriesNode:
		p.propagateSoftLimits(n.rows)

	case *cancelSessionsNode:
		p.propagateSoftLimits(n.rows)

	case *controlJobsNode:
		p.propagateSoftLimits(n.rows)

	case *errorIfRowsNode:
		p.propagateSoftLimits(n.plan)

	case *bufferNode:
		p.propagateSoftLimits(n.plan)

	case *exportNode:
		p.propagateSoftLimits(n.source)

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

// propagateSoftLimits triggers a recursion through a planNode's
// children, introducing and passing down soft limit hinting where applicable.
// See applySoftLimit for more detail.
func (p *planner) propagateSoftLimits(plan planNode) {
	p.applySoftLimit(plan, math.MaxInt64)
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
