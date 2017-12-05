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

package sql

import "github.com/cockroachdb/cockroach/pkg/sql/sqlbase"

var noColumns = make(sqlbase.ResultColumns, 0)

// planColumns returns the signature of rows logically computed
// by the given planNode.
// The signature consists of the list of columns with
// their name and type.
//
// The length of the returned slice is guaranteed to be equal to the
// length of the tuple returned by the planNode's Values() method
// during local execution.
//
// The returned slice is *not* mutable. To modify the result column
// set, implement a separate recursion (e.g. needed_columns.go) or use
// planMutableColumns defined below.
//
// Available after newPlan().
func planColumns(plan planNode) sqlbase.ResultColumns {
	return getPlanColumns(plan, false)
}

// planMutableColumns is similar to planColumns() but returns a
// ResultColumns slice that can be modified by the caller.
func planMutableColumns(plan planNode) sqlbase.ResultColumns {
	return getPlanColumns(plan, true)
}

// getPlanColumns implements the logic for the
// planColumns/planMutableColumns functions. The mut argument
// indicates whether the slice should be mutable (mut=true) or not.
func getPlanColumns(plan planNode, mut bool) sqlbase.ResultColumns {
	switch n := plan.(type) {

	// Nodes that define their own schema.
	case *copyNode:
		return n.resultColumns
	case *delayedNode:
		return n.columns
	case *distinctNode:
		return n.columns
	case *groupNode:
		return n.columns
	case *hookFnNode:
		return n.header
	case *joinNode:
		return n.columns
	case *ordinalityNode:
		return n.columns
	case *renderNode:
		return n.columns
	case *scanNode:
		return n.resultColumns
	case *sortNode:
		return n.columns
	case *valueGenerator:
		return n.columns
	case *valuesNode:
		return n.columns
	case *explainPlanNode:
		return n.run.results.columns
	case *windowNode:
		return n.run.values.columns
	case *showTraceNode:
		return n.columns

		// Nodes with a fixed schema.
	case *scrubNode:
		return n.getColumns(mut, scrubColumns)
	case *explainDistSQLNode:
		return n.getColumns(mut, explainDistSQLColumns)
	case *testingRelocateNode:
		return n.getColumns(mut, relocateNodeColumns)
	case *scatterNode:
		return n.getColumns(mut, scatterNodeColumns)
	case *showZoneConfigNode:
		return n.getColumns(mut, showZoneConfigNodeColumns)
	case *showRangesNode:
		return n.getColumns(mut, showRangesColumns)
	case *showFingerprintsNode:
		return n.getColumns(mut, showFingerprintsColumns)
	case *splitNode:
		return n.getColumns(mut, splitNodeColumns)

		// Nodes using the RETURNING helper.
	case *deleteNode:
		return n.rh.columns
	case *insertNode:
		return n.rh.columns
	case *updateNode:
		return n.rh.columns

		// Nodes that have the same schema as their source or their
		// valueNode helper.
	case *filterNode:
		return getPlanColumns(n.source.plan, mut)
	case *indexJoinNode:
		return getPlanColumns(n.table, mut)
	case *limitNode:
		return getPlanColumns(n.plan, mut)
	case *unionNode:
		if n.inverted {
			return getPlanColumns(n.right, mut)
		}
		return getPlanColumns(n.left, mut)
	}

	// Every other node has no columns in their results.
	return noColumns
}

// optColumnsSlot is a helper struct for nodes with a static signature
// (e.g. explainDistSQLNode). It allows instances to reuse a common
// (shared) ResultColumns slice as long as no read/write access is
// requested to the slice via planMutableColumns.
type optColumnsSlot struct {
	columns sqlbase.ResultColumns
}

func (c *optColumnsSlot) getColumns(mut bool, cols sqlbase.ResultColumns) sqlbase.ResultColumns {
	if c.columns != nil {
		return c.columns
	}
	if !mut {
		return cols
	}
	c.columns = make(sqlbase.ResultColumns, len(cols))
	copy(c.columns, cols)
	return c.columns
}
