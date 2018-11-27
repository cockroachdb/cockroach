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

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

// zigzagJoinNode represents a zigzag join. A zigzag join uses multiple indexes
// at the same time that are all prefixed with fixed columns (columns
// constrained to constant values) followed by values that must remain
// equal across all sides (such as primary keys). The zigzag joiner takes
// advantage of the sorted order of those equality columns to efficiently
// find rows that satisfy the constant constraints as well as the equality
// constraints.
//
// For a more detailed description of zigzag joins, as well as when they can
// be planned, see the comment in distsqlrun/zigzagjoiner.go.
type zigzagJoinNode struct {
	// sides contains information about each individual "side" of a
	// zigzag join. Must contain 2 or more zigzagJoinSides.
	sides []zigzagJoinSide

	// joinType is either INNER or LEFT_OUTER.
	joinType sqlbase.JoinType

	// columns are the produced columns, namely the columns in all
	// indexes in 'sides' - in the same order as sides.
	columns sqlbase.ResultColumns

	// onCond is any ON condition to be used in conjunction with the implicit
	// equality condition on keyCols.
	onCond tree.TypedExpr

	props physicalProps
}

// zigzagJoinSide contains information about one "side" of the zigzag
// join. Note that the length of all eqCols in one zigzagJoinNode should
// be the same.
type zigzagJoinSide struct {
	// scan references a scan node containing index/table descriptor references
	// for this side of the join.
	scan *scanNode

	// eqCols is an int slice containing the equated columns for this side
	// of the zigzag join.
	eqCols []int

	// fixedVals contains fixed values for a prefix of this side's index columns.
	// Represented as a values node with one row/tuple, and just the columns
	// that are fixed.
	fixedVals *valuesNode
}

// startExec is part of the execStartable interface.
func (zj *zigzagJoinNode) startExec(params runParams) error {
	panic("zigzag joins cannot be executed outside of distsql")
}

// Next is part of the planNode interface.
func (zj *zigzagJoinNode) Next(params runParams) (bool, error) {
	panic("zigzag joins cannot be executed outside of distsql")
}

// Values is part of the planNode interface.
func (zj *zigzagJoinNode) Values() tree.Datums {
	panic("zigzag joins cannot be executed outside of distsql")
}

// Close is part of the planNode interface.
func (zj *zigzagJoinNode) Close(ctx context.Context) {
}
