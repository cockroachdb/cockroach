// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package plannode

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// topKNode represents a node that returns only the top K rows according to the
// ordering, in the order specified.
type TopKNode struct {
	singleInputPlanNode
	K        int64
	Ordering colinfo.ColumnOrdering
	// When alreadyOrderedPrefix is non-zero, the input is already ordered on
	// the prefix Ordering[:alreadyOrderedPrefix].
	AlreadyOrderedPrefix int
	// EstimatedInputRowCount, when set, is the estimated number of rows that
	// this topKNode will read from its input.
	EstimatedInputRowCount uint64
}

func (n *topKNode) StartExec(params runParams) error {
	panic("topKNode cannot be run in local mode")
}

func (n *topKNode) Next(params runParams) (bool, error) {
	panic("topKNode cannot be run in local mode")
}

func (n *topKNode) Values() tree.Datums {
	panic("topKNode cannot be run in local mode")
}

func (n *topKNode) Close(ctx context.Context) {
	n.Source.Close(ctx)
}



// Lowercase alias
type topKNode = TopKNode
