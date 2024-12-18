// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// topKNode represents a node that returns only the top K rows according to the
// ordering, in the order specified.
type topKNode struct {
	singleInputPlanNode
	k        int64
	ordering colinfo.ColumnOrdering
	// When alreadyOrderedPrefix is non-zero, the input is already ordered on
	// the prefix ordering[:alreadyOrderedPrefix].
	alreadyOrderedPrefix int
	// estimatedInputRowCount, when set, is the estimated number of rows that
	// this topKNode will read from its input.
	estimatedInputRowCount uint64
}

func (n *topKNode) startExec(params runParams) error {
	panic("topKNode cannot be run in local mode")
}

func (n *topKNode) Next(params runParams) (bool, error) {
	panic("topKNode cannot be run in local mode")
}

func (n *topKNode) Values() tree.Datums {
	panic("topKNode cannot be run in local mode")
}

func (n *topKNode) Close(ctx context.Context) {
	n.input.Close(ctx)
}
