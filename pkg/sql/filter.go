// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

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
type filterNode struct {
	singleInputPlanNode
	columns     colinfo.ResultColumns
	filter      tree.TypedExpr
	reqOrdering ReqOrdering
}

// filterNode implements eval.IndexedVarContainer
var _ eval.IndexedVarContainer = &filterNode{}

// IndexedVarEval implements the eval.IndexedVarContainer interface.
func (f *filterNode) IndexedVarEval(idx int) (tree.Datum, error) {
	return f.input.Values()[idx], nil
}

// IndexedVarResolvedType implements the tree.IndexedVarContainer interface.
func (f *filterNode) IndexedVarResolvedType(idx int) *types.T {
	return f.columns[idx].Typ
}

func (f *filterNode) startExec(runParams) error {
	return nil
}

// Next implements the planNode interface.
func (f *filterNode) Next(params runParams) (bool, error) {
	panic("filterNode cannot be run in local mode")
}

func (f *filterNode) Values() tree.Datums {
	panic("filterNode cannot be run in local mode")
}

func (f *filterNode) Close(ctx context.Context) { f.input.Close(ctx) }
