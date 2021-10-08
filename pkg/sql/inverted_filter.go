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

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/inverted"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

type invertedFilterNode struct {
	input           planNode
	expression      *inverted.SpanExpression
	preFiltererExpr tree.TypedExpr
	preFiltererType *types.T
	invColumn       int
	resultColumns   colinfo.ResultColumns
}

func (n *invertedFilterNode) startExec(params runParams) error {
	panic("invertedFiltererNode can't be run in local mode")
}
func (n *invertedFilterNode) Close(ctx context.Context) {
	n.input.Close(ctx)
}
func (n *invertedFilterNode) Next(params runParams) (bool, error) {
	panic("invertedFiltererNode can't be run in local mode")
}
func (n *invertedFilterNode) Values() tree.Datums {
	panic("invertedFiltererNode can't be run in local mode")
}
