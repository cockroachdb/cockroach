// Copyright 2016 The Cockroach Authors.
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

// joinNode is a planNode whose rows are the result of a join operation.
type joinNode struct {
	// The data sources.
	left  planDataSource
	right planDataSource

	// pred represents the join predicate.
	pred *joinPredicate

	// mergeJoinOrdering is set if the left and right sides have similar ordering
	// on the equality columns (or a subset of them). The column indices refer to
	// equality columns: a ColIdx of i refers to left column
	// pred.leftEqualityIndices[i] and right column pred.rightEqualityIndices[i].
	mergeJoinOrdering colinfo.ColumnOrdering

	reqOrdering ReqOrdering

	// columns contains the metadata for the results of this node.
	columns colinfo.ResultColumns
}

func (p *planner) makeJoinNode(
	left planDataSource, right planDataSource, pred *joinPredicate,
) *joinNode {
	n := &joinNode{
		left:    left,
		right:   right,
		pred:    pred,
		columns: pred.cols,
	}
	return n
}

func (n *joinNode) startExec(params runParams) error {
	panic("joinNode cannot be run in local mode")
}

// Next implements the planNode interface.
func (n *joinNode) Next(params runParams) (res bool, err error) {
	panic("joinNode cannot be run in local mode")
}

// Values implements the planNode interface.
func (n *joinNode) Values() tree.Datums {
	panic("joinNode cannot be run in local mode")
}

// Close implements the planNode interface.
func (n *joinNode) Close(ctx context.Context) {
	n.right.plan.Close(ctx)
	n.left.plan.Close(ctx)
}
