// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package sql

import (
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

// filterNode implements a filtering stage. It is intended to be used
// during plan optimizations in order to avoid instantiating a fully
// blown selectTopNode/renderNode pair.
type filterNode struct {
	source     planDataSource
	filter     tree.TypedExpr
	ivarHelper tree.IndexedVarHelper
	props      physicalProps
}

func (f *filterNode) computePhysicalProps(evalCtx *tree.EvalContext) {
	f.props = planPhysicalProps(f.source.plan)
	f.props.applyExpr(evalCtx, f.filter)
}

// IndexedVarEval implements the tree.IndexedVarContainer interface.
func (f *filterNode) IndexedVarEval(idx int, ctx *tree.EvalContext) (tree.Datum, error) {
	return f.source.plan.Values()[idx].Eval(ctx)
}

// IndexedVarResolvedType implements the tree.IndexedVarContainer interface.
func (f *filterNode) IndexedVarResolvedType(idx int) types.T {
	return f.source.info.sourceColumns[idx].Typ
}

// IndexedVarNodeFormatter implements the tree.IndexedVarContainer interface.
func (f *filterNode) IndexedVarNodeFormatter(idx int) tree.NodeFormatter {
	return f.source.info.NodeFormatter(idx)
}

// Start implements the planNode interface.
func (f *filterNode) Start(params runParams) error {
	return f.source.plan.Start(params)
}

// Next implements the planNode interface.
func (f *filterNode) Next(params runParams) (bool, error) {
	for {
		if next, err := f.source.plan.Next(params); !next {
			return false, err
		}

		params.p.evalCtx.IVarHelper = &f.ivarHelper
		passesFilter, err := sqlbase.RunFilter(f.filter, &params.p.evalCtx)
		params.p.evalCtx.IVarHelper = nil
		if err != nil {
			return false, err
		}

		if passesFilter {
			return true, nil
		}
		// Row was filtered out; grab the next row.
	}
}

func (f *filterNode) Close(ctx context.Context) {
	f.source.plan.Close(ctx)
}
func (f *filterNode) Values() tree.Datums { return f.source.plan.Values() }
