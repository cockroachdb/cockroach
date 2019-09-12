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

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util"
)

// distinctNode de-duplicates rows returned by a wrapped planNode.
type distinctNode struct {
	plan planNode

	// distinctOnColIdxs are the column indices of the child planNode and
	// is what defines the distinct key.
	// For a normal DISTINCT (without the ON clause), distinctOnColIdxs
	// contains all the column indices of the child planNode.
	// Otherwise, distinctOnColIdxs is a strict subset of the child
	// planNode's column indices indicating which columns are specified in
	// the DISTINCT ON (<exprs>) clause.
	distinctOnColIdxs util.FastIntSet

	// Subset of distinctOnColIdxs on which the input guarantees an ordering.
	// All rows that are equal on these columns appear contiguously in the input.
	columnsInOrder util.FastIntSet

	reqOrdering ReqOrdering
}

func (n *distinctNode) startExec(params runParams) error {
	panic("distinctNode can't be called in local mode")
}

func (n *distinctNode) Next(params runParams) (bool, error) {
	panic("distinctNode can't be called in local mode")
}

func (n *distinctNode) Values() tree.Datums {
	panic("distinctNode can't be called in local mode")
}

func (n *distinctNode) Close(ctx context.Context) {
	n.plan.Close(ctx)
}
