// Copyright 2021 The Cockroach Authors.
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

// topKNode represents a node that returns only the top K rows according to the
// ordering, in the order specified.
type topKNode struct {
	plan     planNode
	k        int64
	ordering colinfo.ColumnOrdering
	// When alreadyOrderedPrefix is non-zero, the input is already ordered on
	// the prefix ordering[:alreadyOrderedPrefix].
	alreadyOrderedPrefix int
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
	n.plan.Close(ctx)
}
