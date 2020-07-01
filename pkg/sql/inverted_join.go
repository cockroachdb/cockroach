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

type invertedJoinNode struct {
	input planNode
	table *scanNode

	// joinType is one of INNER, LEFT_OUTER, LEFT_SEMI, LEFT_ANTI.
	joinType sqlbase.JoinType

	// The inverted expression to evaluate.
	invertedExpr tree.TypedExpr

	// inputCol identifies the column from the input to be used for the
	// inverted join.
	inputCol int32

	// columns are the produced columns, namely the input columns and (unless the
	// join type is semi or anti join) the columns in the table scanNode.
	columns sqlbase.ResultColumns

	// onExpr is any ON condition to be used in conjunction with the inverted
	// expression.
	onExpr tree.TypedExpr
}

// CanParallelize indicates whether the fetchers can parallelize the
// batches of lookups that can be performed. This should be kept in
// sync with the behavior of invertedJoiner scan behavior.
func (ij *invertedJoinNode) CanParallelize() bool {
	return true
}

func (ij *invertedJoinNode) startExec(params runParams) error {
	panic("invertedJoinNode cannot be run in local mode")
}

func (ij *invertedJoinNode) Next(params runParams) (bool, error) {
	panic("invertedJoinNode cannot be run in local mode")
}

func (ij *invertedJoinNode) Values() tree.Datums {
	panic("invertedJoinNode cannot be run in local mode")
}

func (ij *invertedJoinNode) Close(ctx context.Context) {
	ij.input.Close(ctx)
	ij.table.Close(ctx)
}
