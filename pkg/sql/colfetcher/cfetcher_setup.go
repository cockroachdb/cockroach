// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colfetcher

import (
	"context"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecargs"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecutils"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
)

// cFetcherTableArgs describes the information about the index we're fetching
// from. Note that only columns that need to be fetched (i.e. requested by the
// caller) are included in the internal state.
type cFetcherTableArgs struct {
	spec descpb.IndexFetchSpec
	// ColIdxMap is a mapping from ColumnID to the ordinal of the corresponding
	// column within spec.FetchedColumns.
	ColIdxMap catalog.TableColMap
	// typs are the types from spec.FetchedColumns.
	typs []*types.T
}

var cFetcherTableArgsPool = sync.Pool{
	New: func() interface{} {
		return &cFetcherTableArgs{}
	},
}

func (a *cFetcherTableArgs) Release() {
	*a = cFetcherTableArgs{
		// The types are small objects, so we don't bother deeply resetting this
		// slice.
		typs: a.typs[:0],
	}
	cFetcherTableArgsPool.Put(a)
}

func (a *cFetcherTableArgs) populateTypes(cols []descpb.IndexFetchSpec_Column) {
	if cap(a.typs) < len(cols) {
		a.typs = make([]*types.T, len(cols))
	} else {
		a.typs = a.typs[:len(cols)]
	}
	for i := range cols {
		a.typs[i] = cols[i].Type
	}
}

// populateTableArgs fills in cFetcherTableArgs.
func populateTableArgs(
	ctx context.Context, flowCtx *execinfra.FlowCtx, fetchSpec *descpb.IndexFetchSpec,
) (_ *cFetcherTableArgs, _ error) {
	args := cFetcherTableArgsPool.Get().(*cFetcherTableArgs)

	*args = cFetcherTableArgs{
		spec: *fetchSpec,
		typs: args.typs,
	}
	// Before we can safely use types from the fetch spec, we need to make sure
	// they are hydrated. In row execution engine it is done during the processor
	// initialization, but neither ColBatchScan nor cFetcher are processors, so we
	// need to do the hydration ourselves.
	resolver := flowCtx.NewTypeResolver(flowCtx.Txn)
	for i := range args.spec.FetchedColumns {
		if err := typedesc.EnsureTypeIsHydrated(ctx, args.spec.FetchedColumns[i].Type, &resolver); err != nil {
			return nil, err
		}
	}
	for i := range args.spec.KeyAndSuffixColumns {
		if err := typedesc.EnsureTypeIsHydrated(ctx, args.spec.KeyAndSuffixColumns[i].Type, &resolver); err != nil {
			return nil, err
		}
	}
	args.populateTypes(args.spec.FetchedColumns)
	for i := range args.spec.FetchedColumns {
		args.ColIdxMap.Set(args.spec.FetchedColumns[i].ColumnID, i)
	}

	return args, nil
}

// populateTableArgsLegacy is a legacy version of populateTableArgs which
// examines the given post-processing spec to find the set of the needed
// columns. These columns are added to the table args and the column references
// in post are adjusted accordingly.
// - neededColumns is a set containing the ordinals of all columns that need to
// be fetched.
func populateTableArgsLegacy(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	table catalog.TableDescriptor,
	index catalog.Index,
	invertedCol catalog.Column,
	hasSystemColumns bool,
	post *execinfrapb.PostProcessSpec,
	helper *colexecargs.ExprHelper,
) (_ *cFetcherTableArgs, neededColumns util.FastIntSet, _ error) {
	args := cFetcherTableArgsPool.Get().(*cFetcherTableArgs)

	readableColumns := table.ReadableColumns()
	numCols := len(readableColumns)
	var systemColumns []catalog.Column
	if hasSystemColumns {
		systemColumns = table.SystemColumns()
		numCols += len(systemColumns)
	}

	var err error
	// Make sure that render expressions are deserialized right away so that we
	// don't have to re-parse them multiple times.
	if post.RenderExprs != nil {
		// Populate all column types.
		typs := args.typs[:0]
		if cap(typs) >= numCols {
			typs = typs[:numCols]
		} else {
			typs = make([]*types.T, numCols)
		}
		for i := range readableColumns {
			typs[i] = readableColumns[i].GetType()
			if invertedCol != nil && readableColumns[i].GetID() == invertedCol.GetID() {
				typs[i] = invertedCol.GetType()
			}
		}
		for i := range systemColumns {
			typs[len(readableColumns)+i] = systemColumns[i].GetType()
		}
		for i := range post.RenderExprs {
			// It is ok to use the evalCtx of the flowCtx since it won't be
			// mutated (we are not evaluating the expressions). It's also ok to
			// update post in-place even if flowCtx.PreserveFlowSpecs is true
			// since we're not really mutating the render expressions.
			post.RenderExprs[i].LocalExpr, err = helper.ProcessExpr(post.RenderExprs[i], flowCtx.EvalCtx, typs)
			if err != nil {
				return args, neededColumns, err
			}
		}
	}

	// Now find the set of columns that are actually needed based on the
	// post-processing spec.
	neededColumns = getNeededColumns(post, numCols)

	colIDs := make([]descpb.ColumnID, 0, neededColumns.Len())
	for idx, ok := neededColumns.Next(0); ok; idx, ok = neededColumns.Next(idx + 1) {
		if idx < len(readableColumns) {
			colIDs = append(colIDs, readableColumns[idx].GetID())
		} else {
			colIDs = append(colIDs, systemColumns[idx-len(readableColumns)].GetID())
		}
	}

	// Remap the post processing spec.
	if len(colIDs) != numCols {
		idxMap := make([]int, numCols)
		newIdx := 0
		for idx, ok := neededColumns.Next(0); ok; idx, ok = neededColumns.Next(idx + 1) {
			idxMap[idx] = newIdx
			newIdx++
		}
		remapPostProcessSpec(post, idxMap, flowCtx.PreserveFlowSpecs)
	}

	*args = cFetcherTableArgs{
		typs: args.typs,
	}
	if err := rowenc.InitIndexFetchSpec(
		&args.spec, flowCtx.Codec(), table, index, colIDs,
	); err != nil {
		return nil, util.FastIntSet{}, err
	}
	args.populateTypes(args.spec.FetchedColumns)
	for i := range args.spec.FetchedColumns {
		args.ColIdxMap.Set(args.spec.FetchedColumns[i].ColumnID, i)
	}

	// Before we can safely use types from the table descriptor, we need to
	// make sure they are hydrated. In row execution engine it is done during
	// the processor initialization, but neither ColBatchScan nor cFetcher are
	// processors, so we need to do the hydration ourselves.
	resolver := flowCtx.NewTypeResolver(flowCtx.Txn)
	return args, neededColumns, resolver.HydrateTypeSlice(ctx, args.typs)
}

// getNeededColumns returns the set of needed columns that a processor core must
// output because these columns are used by the post-processing stage. It is
// assumed that the render expressions, if any, have already been deserialized.
func getNeededColumns(post *execinfrapb.PostProcessSpec, numColumns int) util.FastIntSet {
	var neededColumns util.FastIntSet
	if !post.Projection && len(post.RenderExprs) == 0 {
		// All columns are needed.
		neededColumns.AddRange(0, numColumns-1)
	} else if post.Projection {
		for _, neededColOrd := range post.OutputColumns {
			neededColumns.Add(int(neededColOrd))
		}
	} else {
		var visitor ivarExpressionVisitor
		for _, expr := range post.RenderExprs {
			visitor.ivarSeen = colexecutils.MaybeAllocateBoolArray(visitor.ivarSeen, numColumns)
			_, _ = tree.WalkExpr(visitor, expr.LocalExpr)
			for i, seen := range visitor.ivarSeen {
				if seen {
					neededColumns.Add(i)
				}
			}
			if neededColumns.Len() == numColumns {
				// All columns are needed, so we can stop processing the
				// subsequent render expressions.
				break
			}
		}
	}
	return neededColumns
}

// remapPostProcessSpec updates post so that all IndexedVars refer to the new
// ordinals according to idxMap. It is assumed that the render expressions, if
// any, have already been deserialized.
//
// For example, say we have idxMap = [0, 0, 1, 2, 0, 0] and a render expression
// like '(@1 + @4) / @3`, then it'll be updated into '(@1 + @3) / @2'. Such an
// idxMap indicates that the table has 6 columns and only 3 of them (0th, 2nd,
// 3rd) are needed.
//
// If preserveFlowSpecs is true, then this method updates post to store the
// original output columns or render expressions. Notably, in order to not
// corrupt the flow specs that have been scheduled to run on the remote nodes,
// this method will allocate fresh slices instead of updating the old slices in
// place (the flow specs for the remote nodes have shallow copies of this
// PostProcessSpec).
// NB: it is ok that we're modifying the specs - we are in the flow setup path
// which occurs **after** we have sent out SetupFlowRequest RPCs. In other
// words, every node must have gotten the unmodified version of the spec and is
// now free to modify it as it pleases.
func remapPostProcessSpec(post *execinfrapb.PostProcessSpec, idxMap []int, preserveFlowSpecs bool) {
	if post.Projection {
		outputColumns := post.OutputColumns
		if preserveFlowSpecs && post.OriginalOutputColumns == nil {
			// This is the first time we're modifying this PostProcessSpec, but
			// we've been asked to preserve the specs, so we have to set the
			// original output columns. We are also careful to allocate a new
			// slice to populate the updated projection.
			post.OriginalOutputColumns = outputColumns
			post.OutputColumns = make([]uint32, len(outputColumns))
		}
		for i, colIdx := range outputColumns {
			post.OutputColumns[i] = uint32(idxMap[colIdx])
		}
	} else if post.RenderExprs != nil {
		renderExprs := post.RenderExprs
		if preserveFlowSpecs && post.OriginalRenderExprs == nil {
			// This is the first time we're modifying this PostProcessSpec, but
			// we've been asked to preserve the specs, so we have to set the
			// original render expressions. We are also careful to allocate a
			// new slice to populate the updated render expressions.
			post.OriginalRenderExprs = renderExprs
			post.RenderExprs = make([]execinfrapb.Expression, len(renderExprs))
		}
		for i := range renderExprs {
			post.RenderExprs[i].LocalExpr = physicalplan.RemapIVarsInTypedExpr(renderExprs[i].LocalExpr, idxMap)
		}
	}
}

type ivarExpressionVisitor struct {
	ivarSeen []bool
}

var _ tree.Visitor = &ivarExpressionVisitor{}

// VisitPre is a part of tree.Visitor interface.
func (i ivarExpressionVisitor) VisitPre(expr tree.Expr) (bool, tree.Expr) {
	switch e := expr.(type) {
	case *tree.IndexedVar:
		if e.Idx < len(i.ivarSeen) {
			i.ivarSeen[e.Idx] = true
		}
		return false, expr
	default:
		return true, expr
	}
}

// VisitPost is a part of tree.Visitor interface.
func (i ivarExpressionVisitor) VisitPost(expr tree.Expr) tree.Expr { return expr }
