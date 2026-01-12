// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package plannode

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

type VectorSearchNode struct {
	zeroInputPlanNode
	VectorSearchPlanningInfo
}

type VectorSearchPlanningInfo struct {
	Table               catalog.TableDescriptor
	Index               catalog.Index
	PrefixKeys          []roachpb.Key
	QueryVector         tree.TypedExpr
	TargetNeighborCount uint64

	// Cols is the list of non-vector index columns that will be produced by the
	// vector-search operator.
	Cols                []catalog.Column
	Columns             colinfo.ResultColumns
	FinalizeLastStageCb func(*physicalplan.PhysicalPlan) // will be nil in the spec factory
}

func (vs *vectorSearchNode) StartExec(params runParams) error {
	panic("vectorSearchNode cannot be run in local mode")
}

func (vs *vectorSearchNode) Next(params runParams) (bool, error) {
	panic("vectorSearchNode cannot be run in local mode")
}

func (vs *vectorSearchNode) Values() tree.Datums {
	panic("vectorSearchNode cannot be run in local mode")
}

func (vs *vectorSearchNode) Close(ctx context.Context) {}

type VectorMutationSearchNode struct {
	singleInputPlanNode
	VectorMutationSearchPlanningInfo
	// Columns are the produced columns, namely the input columns, the partition
	// column, and (optionally) the quantized vector column.
	Columns colinfo.ResultColumns
}

type VectorMutationSearchPlanningInfo struct {
	Table               catalog.TableDescriptor
	Index               catalog.Index
	PrefixKeyCols       []exec.NodeColumnOrdinal
	QueryVectorCol      exec.NodeColumnOrdinal
	SuffixKeyCols       []exec.NodeColumnOrdinal
	IsIndexPut          bool
	FinalizeLastStageCb func(*physicalplan.PhysicalPlan) // will be nil in the spec factory
}

func (vs *vectorMutationSearchNode) StartExec(params runParams) error {
	panic("vectorMutationSearchNode cannot be run in local mode")
}

func (vs *vectorMutationSearchNode) Next(params runParams) (bool, error) {
	panic("vectorMutationSearchNode cannot be run in local mode")
}

func (vs *vectorMutationSearchNode) Values() tree.Datums {
	panic("vectorMutationSearchNode cannot be run in local mode")
}

func (vs *vectorMutationSearchNode) Close(ctx context.Context) {
	vs.Source.Close(ctx)
}

// Lowercase aliases
type vectorSearchNode = VectorSearchNode
type vectorMutationSearchNode = VectorMutationSearchNode

// Lowercase aliases for planning info
type vectorSearchPlanningInfo = VectorSearchPlanningInfo
type vectorMutationSearchPlanningInfo = VectorMutationSearchPlanningInfo
