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

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

type lookupJoinNode struct {
	input planNode
	table *scanNode

	// joinType is either INNER or LEFT_OUTER.
	joinType sqlbase.JoinType

	// eqCols identifies the columns from the input which are used for the
	// lookup. These correspond to a prefix of the index columns (of the index we
	// are looking up into).
	eqCols []int

	// eqColsAreKey is true when each lookup can return at most one row.
	eqColsAreKey bool

	// columns are the produced columns, namely the input columns and (unless the
	// join type is semi or anti join) the columns in the table scanNode.
	columns sqlbase.ResultColumns

	// onCond is any ON condition to be used in conjunction with the implicit
	// equality condition on eqCols.
	onCond tree.TypedExpr

	reqOrdering ReqOrdering
}

// CanParallelize indicates whether the fetchers can parallelize the
// batches of lookups that can be performed. As of now, this is true if
// the equality columns that the lookup joiner uses form keys that
// can return at most 1 row.
func (lj *lookupJoinNode) CanParallelize() bool {
	return lj.eqColsAreKey
}

func (lj *lookupJoinNode) startExec(params runParams) error {
	panic("lookupJoinNode cannot be run in local mode")
}

func (lj *lookupJoinNode) Next(params runParams) (bool, error) {
	panic("lookupJoinNode cannot be run in local mode")
}

func (lj *lookupJoinNode) Values() tree.Datums {
	panic("lookupJoinNode cannot be run in local mode")
}

func (lj *lookupJoinNode) Close(ctx context.Context) {
	lj.input.Close(ctx)
	lj.table.Close(ctx)
}
