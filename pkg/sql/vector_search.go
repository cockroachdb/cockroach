// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// TODO(vector-index-team): add required fields.
type vectorSearchNode struct {
	zeroInputPlanNode
	table               catalog.TableDescriptor
	index               catalog.Index
	prefixSpans         roachpb.Spans
	queryVector         tree.TypedExpr
	targetNeighborCount uint64

	// columns are the produced columns, namely the primary key columns of the
	// table.
	columns colinfo.ResultColumns
}

func (vs *vectorSearchNode) startExec(params runParams) error {
	panic("vectorSearchNode cannot be run in local mode")
}

func (vs *vectorSearchNode) Next(params runParams) (bool, error) {
	panic("vectorSearchNode cannot be run in local mode")
}

func (vs *vectorSearchNode) Values() tree.Datums {
	panic("vectorSearchNode cannot be run in local mode")
}

func (vs *vectorSearchNode) Close(ctx context.Context) {}

// TODO(vector-index-team): add required fields.
type vectorMutationSearchNode struct {
	singleInputPlanNode

	table          catalog.TableDescriptor
	index          catalog.Index
	prefixKeyCols  []exec.NodeColumnOrdinal
	queryVectorCol exec.NodeColumnOrdinal
	primaryKeyCols []exec.NodeColumnOrdinal
	isIndexPut     bool

	// columns are the produced columns, namely the input columns, the partition
	// column, and (optionally) the quantized vector column.
	columns colinfo.ResultColumns
}

func (vs *vectorMutationSearchNode) startExec(params runParams) error {
	panic("vectorMutationSearchNode cannot be run in local mode")
}

func (vs *vectorMutationSearchNode) Next(params runParams) (bool, error) {
	panic("vectorMutationSearchNode cannot be run in local mode")
}

func (vs *vectorMutationSearchNode) Values() tree.Datums {
	panic("vectorMutationSearchNode cannot be run in local mode")
}

func (vs *vectorMutationSearchNode) Close(ctx context.Context) {
	vs.input.Close(ctx)
}
