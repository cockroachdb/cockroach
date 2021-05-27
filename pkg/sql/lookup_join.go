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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

type lookupJoinNode struct {
	input planNode
	table *scanNode

	// joinType is either INNER, LEFT_OUTER, LEFT_SEMI, or LEFT_ANTI.
	joinType descpb.JoinType

	// eqCols represents the part of the join condition used to perform
	// the lookup into the index. It should only be set when lookupExpr is empty.
	// eqCols identifies the columns from the input which are used for the
	// lookup. These correspond to a prefix of the index columns (of the index we
	// are looking up into).
	eqCols []int

	// eqColsAreKey is true when each lookup can return at most one row.
	eqColsAreKey bool

	// lookupExpr represents the part of the join condition used to perform
	// the lookup into the index. It should only be set when eqCols is empty.
	// lookupExpr is used instead of eqCols when the lookup condition is
	// more complicated than a simple equality between input columns and index
	// columns. In this case, lookupExpr specifies the expression that will be
	// used to construct the spans for each lookup.
	lookupExpr tree.TypedExpr

	// If remoteLookupExpr is set, this is a locality optimized lookup join. In
	// this case, lookupExpr contains the lookup join conditions targeting ranges
	// located on local nodes (relative to the gateway region), and
	// remoteLookupExpr contains the lookup join conditions targeting remote
	// nodes. The optimizer will only plan a locality optimized lookup join if it
	// is known that each lookup returns at most one row. This fact allows the
	// execution engine to use the local conditions in lookupExpr first, and if a
	// match is found locally for each input row, there is no need to search
	// remote nodes. If a local match is not found for all input rows, the
	// execution engine uses remoteLookupExpr to search remote nodes.
	remoteLookupExpr tree.TypedExpr

	// columns are the produced columns, namely the input columns and (unless the
	// join type is semi or anti join) the columns in the table scanNode.
	columns colinfo.ResultColumns

	// onCond is any ON condition to be used in conjunction with the implicit
	// equality condition on eqCols or the conditions in lookupExpr.
	onCond tree.TypedExpr

	isSecondJoinInPairedJoiner bool

	reqOrdering ReqOrdering
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
