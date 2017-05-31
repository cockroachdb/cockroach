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

package sql

import "github.com/cockroachdb/cockroach/pkg/sql/sqlbase"

var noColumns = make(sqlbase.ResultColumns, 0)

// planColumns returns the signature of rows logically computed
// by the given planNode.
// The signature consist's of the list of columns with
// their name and type.
//
// The length of the returned slice is guaranteed to be equal to the
// length of the tuple returned by the planNode's Values() method
// during local exeecution.
//
// Available after newPlan().
func planColumns(plan planNode) sqlbase.ResultColumns {
	switch n := plan.(type) {

	// Nodes that define their own schema.
	case *copyNode:
		return n.resultColumns
	case *delayedNode:
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
		return n.results.columns
	case *windowNode:
		return n.values.columns

		// Nodes with a fixed schema.
	case *emptyNode:
		return noColumns
	case *explainDistSQLNode:
		return explainDistSQLColumns
	case *relocateNode:
		return relocateNodeColumns
	case *scatterNode:
		return scatterNodeColumns
	case *showRangesNode:
		return showRangesColumns
	case *showFingerprintsNode:
		return showFingerprintsColumns
	case *splitNode:
		return splitNodeColumns

		// Nodes using the RETURNING helper.
	case *deleteNode:
		return n.rh.columns
	case *insertNode:
		return n.rh.columns
	case *updateNode:
		return n.rh.columns

		// Nodes that have the same schema as their source or their
		// valueNode helper.
	case *distinctNode:
		return planColumns(n.plan)
	case *filterNode:
		return planColumns(n.source.plan)
	case *indexJoinNode:
		return planColumns(n.table)
	case *limitNode:
		return planColumns(n.plan)
	case *unionNode:
		return planColumns(n.left)

	}

	// Every other node has no columns in their results.
	return noColumns
}
