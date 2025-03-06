// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// indexJoinNode implements joining of results from an index with the rows
// of a table. The input to an indexJoinNode is the result of scanning a
// non-covering index (potentially processed through other operations like
// filtering, sorting, limiting).
type indexJoinNode struct {
	singleInputPlanNode
	indexJoinPlanningInfo

	// columns are the produced columns, namely the columns fetched from the table
	// by primary key.
	columns colinfo.ResultColumns
}

type indexJoinPlanningInfo struct {
	fetch fetchPlanningInfo

	// Indices of the PK columns in the input plan.
	keyCols []exec.NodeColumnOrdinal

	reqOrdering ReqOrdering

	limitHint int64

	// finalizeLastStageCb will be nil in the spec factory.
	finalizeLastStageCb func(*physicalplan.PhysicalPlan)
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
}
