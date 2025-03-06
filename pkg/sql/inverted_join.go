// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

type invertedJoinNode struct {
	singleInputPlanNode
	invertedJoinPlanningInfo

	// columns are the produced columns, namely the input columns and (unless the
	// join type is semi or anti join) the columns fetched from the table.
	// It includes an additional continuation column when IsFirstJoinInPairedJoin
	// is true.
	columns colinfo.ResultColumns
}

type invertedJoinPlanningInfo struct {
	fetch fetchPlanningInfo

	// joinType is one of INNER, LEFT_OUTER, LEFT_SEMI, LEFT_ANTI.
	joinType descpb.JoinType

	// prefixEqCols identifies the columns from the input which are used for the
	// lookup if the index is a multi-column inverted index. These correspond to
	// the non-inverted prefix columns of the index we are looking up. This is
	// empty if the index is not a multi-column inverted index.
	prefixEqCols []exec.NodeColumnOrdinal

	// The inverted expression to evaluate.
	invertedExpr tree.TypedExpr

	// onExpr is any ON condition to be used in conjunction with the inverted
	// expression.
	onExpr tree.TypedExpr

	isFirstJoinInPairedJoiner bool

	reqOrdering ReqOrdering

	// finalizeLastStageCb will be nil in the spec factory.
	finalizeLastStageCb func(*physicalplan.PhysicalPlan)
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
}
