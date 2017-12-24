// Copyright 2016 The Cockroach Authors.
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

package sql

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util"
)

// setNeededColumns informs the node about which columns are
// effectively needed by the consumer of its result rows.
func setNeededColumns(plan planNode, needed []bool) {
	switch n := plan.(type) {
	case *createTableNode:
		if n.n.As() {
			setNeededColumns(n.sourcePlan, allColumns(n.sourcePlan))
		}

	case *explainDistSQLNode:
		setNeededColumns(n.plan, allColumns(n.plan))

	case *showTraceNode:
		setNeededColumns(n.plan, allColumns(n.plan))

	case *explainPlanNode:
		if n.optimized {
			setNeededColumns(n.plan, allColumns(n.plan))
		}

	case *limitNode:
		setNeededColumns(n.plan, needed)

	case *indexJoinNode:
		// Currently all the needed result columns are provided by the
		// table sub-source; from the index sub-source we only need the PK
		// columns sufficient to configure the table sub-source.
		// TODO(radu/knz): see the comments at the start of index_join.go,
		// perhaps this can be optimized to utilize the column values
		// already provided by the index instead of re-retrieving them
		// using the table scanNode.
		setNeededColumns(n.table, needed)
		setNeededColumns(n.index, n.primaryKeyColumns)

	case *unionNode:
		if !n.emitAll {
			// For UNION (as opposed to UNION ALL) we have to check for
			// uniqueness so we need all the columns.
			needed = allColumns(n)
		}
		setNeededColumns(n.left, needed)
		setNeededColumns(n.right, needed)

	case *joinNode:
		// Note: getNeededColumns takes into account both the columns
		// tested for equality and the join predicate expression.
		leftNeeded, rightNeeded := n.pred.getNeededColumns(needed)
		setNeededColumns(n.left.plan, leftNeeded)
		setNeededColumns(n.right.plan, rightNeeded)
		markOmitted(n.columns, needed)

	case *ordinalityNode:
		setNeededColumns(n.source, needed[:len(needed)-1])
		markOmitted(n.columns[:len(needed)-1], needed[:len(needed)-1])

	case *valuesNode:
		markOmitted(n.columns, needed)

	case *delayedNode:
		if n.plan != nil {
			setNeededColumns(n.plan, needed)
		}
		markOmitted(n.columns, needed)

	case *scanNode:
		// Reset the needed columns set.
		n.valNeededForCol = util.FastIntSet{}
		for i, colNeeded := range needed {
			// All the values involved in the filter expression are needed too.
			if colNeeded || n.filterVars.IndexedVarUsed(i) {
				n.valNeededForCol.Add(i)
				n.resultColumns[i].Omitted = false
			} else {
				n.resultColumns[i].Omitted = true
			}
		}

	case *distinctNode:
		// Distinct needs values for every input column.
		setNeededColumns(n.plan, allColumns(n.plan))

	case *filterNode:
		// Detect which columns from the source are needed in addition to
		// those needed by the context.
		sourceNeeded := make([]bool, len(n.source.info.sourceColumns))
		copy(sourceNeeded, needed)
		for i := range sourceNeeded {
			sourceNeeded[i] = sourceNeeded[i] || n.ivarHelper.IndexedVarUsed(i)
		}
		setNeededColumns(n.source.plan, sourceNeeded)

	case *renderNode:
		// Optimization: remove all the render expressions that are not
		// needed. While doing so, some indexed vars may disappear
		// entirely, which may enable omission of more columns from the
		// source. To detect this, we need to reset the IndexedVarHelper
		// and re-bind all the expressions.
		n.ivarHelper.Reset()
		for i, val := range needed {
			if !val {
				// This render is not used, so reduce its expression to NULL.
				n.render[i] = tree.DNull
				continue
			}
			n.render[i] = n.ivarHelper.Rebind(n.render[i], false, true)
		}

		// Now detect which columns from the source are still needed.
		sourceNeeded := make([]bool, len(n.source.info.sourceColumns))
		for i := range sourceNeeded {
			sourceNeeded[i] = n.ivarHelper.IndexedVarUsed(i)
		}
		setNeededColumns(n.source.plan, sourceNeeded)
		markOmitted(n.columns, needed)

	case *sortNode:
		sourceNeeded := make([]bool, len(planColumns(n.plan)))
		copy(sourceNeeded[:len(needed)], needed)

		// All the ordering columns are also needed.
		for _, o := range n.ordering {
			sourceNeeded[o.ColIdx] = true
		}

		setNeededColumns(n.plan, sourceNeeded)
		markOmitted(n.columns, sourceNeeded[:len(n.columns)])

	case *groupNode:
		// TODO(knz): This can be optimized by removing the aggregation
		// results that are not needed, then removing additional renders
		// from the source that would otherwise only be needed for the
		// omitted aggregation results.
		setNeededColumns(n.plan, allColumns(n.plan))

	case *windowNode:
		// TODO(knz): This can be optimized by removing the window function
		// definitions that are not needed, then removing additional
		// renders from the source that would otherwise only be needed for
		// the omitted window definitions.
		setNeededColumns(n.plan, allColumns(n.plan))

	case *deleteNode:
		// TODO(knz): This can be optimized by omitting the columns that
		// are not part of the primary key, do not participate in
		// foreign key relations and that are not needed for RETURNING.
		setNeededColumns(n.run.rows, allColumns(n.run.rows))

	case *updateNode:
		// TODO(knz): This can be optimized by omitting the columns that
		// are not part of the primary key, do not participate in
		// foreign key relations and that are not needed for RETURNING.
		setNeededColumns(n.run.rows, allColumns(n.run.rows))

	case *insertNode:
		// TODO(knz): This can be optimized by omitting the columns that
		// are not part of the primary key, do not participate in
		// foreign key relations and that are not needed for RETURNING.
		setNeededColumns(n.run.rows, allColumns(n.run.rows))

	case *splitNode:
		setNeededColumns(n.rows, allColumns(n.rows))

	case *testingRelocateNode:
		setNeededColumns(n.rows, allColumns(n.rows))

	case *alterTableNode:
	case *alterSequenceNode:
	case *alterUserSetPasswordNode:
	case *cancelQueryNode:
	case *controlJobNode:
	case *scrubNode:
	case *copyNode:
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
	case *zeroNode:
	case *unaryNode:
	case *hookFnNode:
	case *valueGenerator:
	case *setVarNode:
	case *setClusterSettingNode:
	case *setZoneConfigNode:
	case *showZoneConfigNode:
	case *showRangesNode:
	case *showFingerprintsNode:
	case *scatterNode:

	default:
		panic(fmt.Sprintf("unhandled node type: %T", plan))
	}
}

// allColumns returns true for every column produced by the plan.
func allColumns(plan planNode) []bool {
	needed := make([]bool, len(planColumns(plan)))
	for i := range needed {
		needed[i] = true
	}
	return needed
}

// markOmitted propagates the information from the needed array back
// to the sqlbase.ResultColumns array.
func markOmitted(cols sqlbase.ResultColumns, needed []bool) {
	for i, val := range needed {
		cols[i].Omitted = !val
	}
}
