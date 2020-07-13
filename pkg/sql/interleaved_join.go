// Copyright 2020 The Cockroach Authors.
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

type interleavedJoinNode struct {
	left       *scanNode
	leftFilter tree.TypedExpr

	right       *scanNode
	rightFilter tree.TypedExpr

	leftIsAncestor bool

	joinType sqlbase.JoinType

	// columns are the produced columns, namely the columns in the left scanNode
	// and (unless the join type is semi or anti join) the columns in the right
	// scanNode.
	columns sqlbase.ResultColumns

	// onCond is any ON condition to be used in conjunction with the implicit
	// equality condition on eqCols.
	onCond tree.TypedExpr

	reqOrdering ReqOrdering
}

func (ij *interleavedJoinNode) ancestor() *scanNode {
	if ij.leftIsAncestor {
		return ij.left
	}
	return ij.right
}

func (ij *interleavedJoinNode) descendant() *scanNode {
	if ij.leftIsAncestor {
		return ij.right
	}
	return ij.left
}

func (ij *interleavedJoinNode) startExec(params runParams) error {
	panic("interleavedJoinNode cannot be run in local mode")
}

func (ij *interleavedJoinNode) Next(params runParams) (bool, error) {
	panic("interleavedJoinNode cannot be run in local mode")
}

func (ij *interleavedJoinNode) Values() tree.Datums {
	panic("interleavedJoinNode cannot be run in local mode")
}

func (ij *interleavedJoinNode) Close(ctx context.Context) {
	ij.left.Close(ctx)
	ij.right.Close(ctx)
}
