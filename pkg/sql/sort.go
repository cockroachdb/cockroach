// Copyright 2015 The Cockroach Authors.
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

// sortNode represents a node that sorts the rows returned by its
// sub-node.
type sortNode struct {
	plan     planNode
	ordering colinfo.ColumnOrdering
	// When alreadyOrderedPrefix is non-zero, the input is already ordered on
	// the prefix ordering[:alreadyOrderedPrefix].
	alreadyOrderedPrefix int
}

func (n *sortNode) startExec(runParams) error {
	panic("sortNode cannot be run in local mode")
}

func (n *sortNode) Next(params runParams) (bool, error) {
	panic("sortNode cannot be run in local mode")
}

func (n *sortNode) Values() tree.Datums {
	panic("sortNode cannot be run in local mode")
}

func (n *sortNode) Close(ctx context.Context) {
	n.plan.Close(ctx)
}
