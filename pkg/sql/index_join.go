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

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// indexJoinNode implements joining of results from an index with the rows
// of a table. The input to an indexJoinNode is the result of scanning a
// non-covering index (potentially processed through other operations like
// filtering, sorting, limiting).
type indexJoinNode struct {
	input planNode

	// Indices of the PK columns in the input plan.
	keyCols []int

	table *scanNode

	// The columns returned by this node.
	cols []catalog.Column
	// There is a 1-1 correspondence between cols and resultColumns.
	resultColumns colinfo.ResultColumns

	reqOrdering ReqOrdering
}

func (n *indexJoinNode) startExec(params runParams) error {
	panic("indexJoinNode cannot be run in local mode")
}

func (n *indexJoinNode) Next(params runParams) (bool, error) {
	panic("indexJoinNode cannot be run in local mode")
}

func (n *indexJoinNode) Values() tree.Datums {
	panic("indexJoinNode cannot be run in local mode")
}

func (n *indexJoinNode) Close(ctx context.Context) {
	n.input.Close(ctx)
	n.table.Close(ctx)
}
