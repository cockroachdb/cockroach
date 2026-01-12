// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package plannode

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// filterNode implements a filtering stage. It is intended to be used
// during plan optimizations in order to avoid instantiating a fully
// blown selectTopNode/renderNode pair.
type FilterNode struct {
	singleInputPlanNode
	Columns     colinfo.ResultColumns
	Filter      tree.TypedExpr
	ReqOrdering ReqOrdering
}

// filterNode implements eval.IndexedVarContainer
var _ eval.IndexedVarContainer = &filterNode{}

// IndexedVarEval implements the eval.IndexedVarContainer interface.
func (f *filterNode) IndexedVarEval(idx int) (tree.Datum, error) {
	return f.Source.Values()[idx], nil
}

// IndexedVarResolvedType implements the tree.IndexedVarContainer interface.
func (f *filterNode) IndexedVarResolvedType(idx int) *types.T {
	return f.Columns[idx].Typ
}

func (f *filterNode) StartExec(runParams) error {
	return nil
}

// Next implements the planNode interface.
func (f *filterNode) Next(params runParams) (bool, error) {
	panic("filterNode cannot be run in local mode")
}

func (f *filterNode) Values() tree.Datums {
	panic("filterNode cannot be run in local mode")
}

func (f *filterNode) Close(ctx context.Context) { f.Source.Close(ctx) }



// Lowercase alias
type filterNode = FilterNode
