// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package plannode

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// sortNode represents a node that sorts the rows returned by its
// sub-node.
type SortNode struct {
	singleInputPlanNode
	Ordering colinfo.ColumnOrdering
	// When alreadyOrderedPrefix is non-zero, the input is already ordered on
	// the prefix Ordering[:alreadyOrderedPrefix].
	AlreadyOrderedPrefix int
	// EstimatedInputRowCount, when set, is the estimated number of rows that
	// this sortNode will read from its input.
	EstimatedInputRowCount uint64
}

func (n *sortNode) StartExec(runParams) error {
	panic("sortNode cannot be run in local mode")
}

func (n *sortNode) Next(params runParams) (bool, error) {
	panic("sortNode cannot be run in local mode")
}

func (n *sortNode) Values() tree.Datums {
	panic("sortNode cannot be run in local mode")
}

func (n *sortNode) Close(ctx context.Context) {
	n.Source.Close(ctx)
}



// Lowercase alias
type sortNode = SortNode
