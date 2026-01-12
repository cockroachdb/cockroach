// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package plannode

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/inverted"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

type InvertedFilterNode struct {
	singleInputPlanNode
	InvertedFilterPlanningInfo
	Columns colinfo.ResultColumns
}

type InvertedFilterPlanningInfo struct {
	Expression          *inverted.SpanExpression
	PreFiltererExpr     tree.TypedExpr
	PreFiltererType     *types.T
	InvColumn           int
	FinalizeLastStageCb func(*physicalplan.PhysicalPlan) // will be nil in the spec factory
}

func (n *invertedFilterNode) StartExec(params runParams) error {
	panic("invertedFiltererNode can't be run in local mode")
}
func (n *invertedFilterNode) Close(ctx context.Context) {
	n.Source.Close(ctx)
}
func (n *invertedFilterNode) Next(params runParams) (bool, error) {
	panic("invertedFiltererNode can't be run in local mode")
}
func (n *invertedFilterNode) Values() tree.Datums {
	panic("invertedFiltererNode can't be run in local mode")
}


// Lowercase alias
type invertedFilterNode = InvertedFilterNode

// Lowercase alias
type invertedFilterPlanningInfo = InvertedFilterPlanningInfo
