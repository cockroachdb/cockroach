// Copyright 2018 The Cockroach Authors.
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
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
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
// be planned, see the comment in rowexec/zigzagjoiner.go.
type zigzagJoinNode struct {
	// sides contains information about each individual "side" of a
	// zigzag join. Must contain 2 or more zigzagJoinSides.
	sides []zigzagJoinSide

	// columns are the produced columns, namely the columns in all
	// indexes in 'sides' - in the same order as sides.
	columns colinfo.ResultColumns

	// onCond is any ON condition to be used in conjunction with the implicit
	// equality condition on keyCols.
	onCond tree.TypedExpr

	reqOrdering ReqOrdering
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
