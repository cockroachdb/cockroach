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
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecargs"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
)

// cFetcherTableArgs describes the information about the index we're fetching
// from. Note that only columns that need to be fetched (i.e. requested by the
// caller) are included in the internal state.
type cFetcherTableArgs struct {
	desc  catalog.TableDescriptor
	index catalog.Index
	// ColIdxMap is a mapping from ColumnID of each column to its ordinal. Only
	// needed columns are present.
	ColIdxMap        catalog.TableColMap
	isSecondaryIndex bool
	// cols are all needed columns of the table that are present in the index.
	// The system columns, if requested, are at the end of cols.
	cols []catalog.Column
	// typs are the types of only needed columns from the table.
	typs []*types.T
}

var cFetcherTableArgsPool = sync.Pool{
	New: func() interface{} {
		return &cFetcherTableArgs{}
	},
}

func (a *cFetcherTableArgs) Release() {
	// Deeply reset the column descriptors.
	for i := range a.cols {
		a.cols[i] = nil
	}
	*a = cFetcherTableArgs{
		cols: a.cols[:0],
		// The types are small objects, so we don't bother deeply resetting this
		// slice.
		typs: a.typs[:0],
	}
	cFetcherTableArgsPool.Put(a)
}

func (a *cFetcherTableArgs) populateTypes(cols []catalog.Column) {
	if cap(a.typs) < len(cols) {
		a.typs = make([]*types.T, len(cols))
	} else {
		a.typs = a.typs[:len(cols)]
	}
	for i := range cols {
		a.typs[i] = cols[i].GetType()
	}
}

// populateTableArgs fills all fields of the cFetcherTableArgs except for
// ColIdxMap. Note that all columns accessible from the index (i.e. present in
// the key or value part) will be included in the result. In order to prune
// the unnecessary columns away, use keepOnlyNeededColumns.
//
// If index is a secondary index, then all inaccessible columns are pruned away.
// In such a scenario a non-nil idxMap is returned that allows to remap ordinals
// referring to columns from the whole table to the correct positions among only
// accessible columns. post will be adjusted automatically. Columns that are
// not accessible from the secondary index have an undefined value corresponding
// to them if idxMap is non-nil.
//
// For example, say the table has 4 columns (@1, @2, @3, @4), but only 2 columns
// are present in the index we're reading from (@3, @1). In this case, the
// returned table args only contains columns (@1, @3) and we get an index map as
//   idxMap = [0, x, 1, x] (where 'x' indicates an undefined value).
// Note that although @3 appears earlier than @1 in the index, because we
// iterate over all columns of the table according to their column ordinals, we
// will see @1 first, so it gets the 0th slot, and @3 second, so it gets the 1st
// slot.
func populateTableArgs(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	table catalog.TableDescriptor,
	index catalog.Index,
	invertedCol catalog.Column,
	visibility execinfrapb.ScanVisibility,
	hasSystemColumns bool,
	post *execinfrapb.PostProcessSpec,
	helper *colexecargs.ExprHelper,
) (_ *cFetcherTableArgs, idxMap []int, _ error) {
	args := cFetcherTableArgsPool.Get().(*cFetcherTableArgs)
	// First, find all columns present in the table and possibly include the
	// system columns (when requested).
	cols := args.cols[:0]
	if visibility == execinfra.ScanVisibilityPublicAndNotPublic {
		cols = append(cols, table.ReadableColumns()...)
	} else {
		cols = append(cols, table.PublicColumns()...)
	}
	if invertedCol != nil {
		for i, col := range cols {
			if col.GetID() == invertedCol.GetID() {
				cols[i] = invertedCol
				break
			}
		}
	}
	numSystemCols := 0
	if hasSystemColumns {
		systemCols := table.SystemColumns()
		numSystemCols = len(systemCols)
		cols = append(cols, systemCols...)
	}

	if index.Primary() {
		// Prune virtual computed columns because they do not exist in primary
		// indexes.
		idxMap = make([]int, len(cols))
		colIdx := 0
		for i := range cols {
			if !cols[i].IsVirtual() {
				idxMap[i] = colIdx
				cols[colIdx] = cols[i]
				colIdx++
			}
		}
		cols = cols[:colIdx]
		if err := remapPostProcessSpec(
			flowCtx, post, idxMap, helper, args.typs,
		); err != nil {
			return nil, nil, err
		}
	} else {
		// If we have a secondary index, not all columns might be available from
		// the index, so we'll prune the unavailable columns away.
		colIDs := index.CollectKeyColumnIDs()
		colIDs.UnionWith(index.CollectSecondaryStoredColumnIDs())
		colIDs.UnionWith(index.CollectKeySuffixColumnIDs())
		if colIDs.Len() < len(cols)-numSystemCols {
			needTypesBeforeRemapping := post.RenderExprs != nil
			if needTypesBeforeRemapping {
				args.populateTypes(cols)
			}
			idxMap = make([]int, len(cols))
			colIdx := 0
			for i := range cols {
				//gcassert:bce
				id := cols[i].GetID()
				if colIDs.Contains(id) || (hasSystemColumns && i >= len(cols)-numSystemCols) {
					idxMap[i] = colIdx
					cols[colIdx] = cols[i]
					colIdx++
				}
			}
			cols = cols[:colIdx]
			if err := remapPostProcessSpec(
				flowCtx, post, idxMap, helper, args.typs,
			); err != nil {
				return nil, nil, err
			}
		}
	}

	*args = cFetcherTableArgs{
		desc:             table,
		index:            index,
		isSecondaryIndex: !index.Primary(),
		cols:             cols,
		typs:             args.typs,
	}
	args.populateTypes(cols)

	// Before we can safely use types from the table descriptor, we need to
	// make sure they are hydrated. In row execution engine it is done during
	// the processor initialization, but neither ColBatchScan nor cFetcher are
	// processors, so we need to do the hydration ourselves.
	resolver := flowCtx.TypeResolverFactory.NewTypeResolver(flowCtx.Txn)
	return args, idxMap, resolver.HydrateTypeSlice(ctx, args.typs)
}

// keepOnlyNeededColumns updates the tableArgs to prune all unnecessary columns
// away based on neededColumns slice. If we're reading of the secondary index
// that is not covering all columns, idxMap must be non-nil describing the
// remapping that needs to be used for column ordinals from neededColumns.
// post is updated accordingly to refer to new ordinals of columns. The method
// also populates tableArgs.ColIdxMap.
//
// If traceKV is true, then all columns are considered as needed, and
// neededColumns is ignored.
func keepOnlyNeededColumns(
	flowCtx *execinfra.FlowCtx,
	tableArgs *cFetcherTableArgs,
	idxMap []int,
	neededColumns []uint32,
	post *execinfrapb.PostProcessSpec,
	helper *colexecargs.ExprHelper,
) error {
	if !flowCtx.TraceKV && len(neededColumns) < len(tableArgs.cols) {
		// If the tracing is not enabled and we don't need all of the available
		// columns, we will prune all of the not needed columns away.

		// First, populate a set of needed columns.
		var neededColumnsSet util.FastIntSet
		for _, neededColumn := range neededColumns {
			neededColIdx := int(neededColumn)
			if idxMap != nil {
				neededColIdx = idxMap[neededColIdx]
			}
			neededColumnsSet.Add(neededColIdx)
		}

		// When idxMap is non-nil, we can reuse that. Note that in this case
		// the length of idxMap is equal to the number of columns in the
		// whole table, and we are reading from the secondary index, so the
		// slice will have the sufficient size. We also don't need to reset
		// it since we'll update the needed positions below.
		if idxMap == nil {
			idxMap = make([]int, len(tableArgs.typs))
		}

		// Iterate over all needed columns, populate the idxMap, and adjust
		// the post-processing spec to refer only to the needed columns
		// directly.
		//
		// If non-nil idxMap was passed into this method, we have to update it
		// by essentially applying a projection on top of the already present
		// projection. Consider the following example:
		//   idxMap = [0, x, 1, x] (where 'x' indicates an undefined value)
		// and
		//   neededColumns = [2].
		// Such a setup means that only columns with ordinals @1 and @3 are
		// present in the secondary index while only @3 is actually needed.
		// Above, we have already remapped neededColIdx = 2 to be 1, so now
		// neededColumnsSet only contains 1. The post-processing already refers
		// to this column as having index 1.
		// However, since we are pruning the column with index 0 away, the
		// post-processing stage will see a single column. Thus, we have to
		// update the index map to be
		//   idxMap = [x, 0, x, x]
		// and then remap the post-processing spec below so that it refers to
		// the single needed column with the correct ordinal.
		neededColIdx := 0
		for idx, ok := neededColumnsSet.Next(0); ok; idx, ok = neededColumnsSet.Next(idx + 1) {
			idxMap[idx] = neededColIdx
			neededColIdx++
		}
		if err := remapPostProcessSpec(
			flowCtx, post, idxMap, helper, tableArgs.typs,
		); err != nil {
			return err
		}

		// Now we have to actually prune out the unnecessary columns.
		neededColIdx = 0
		for idx, ok := neededColumnsSet.Next(0); ok; idx, ok = neededColumnsSet.Next(idx + 1) {
			tableArgs.cols[neededColIdx] = tableArgs.cols[idx]
			tableArgs.typs[neededColIdx] = tableArgs.typs[idx]
			neededColIdx++
		}
		tableArgs.cols = tableArgs.cols[:neededColIdx]
		tableArgs.typs = tableArgs.typs[:neededColIdx]
	}

	// Populate the ColIdxMap.
	for i := range tableArgs.cols {
		tableArgs.ColIdxMap.Set(tableArgs.cols[i].GetID(), i)
	}
	return nil
}

// remapPostProcessSpec updates post so that all IndexedVars refer to the new
// ordinals according to idxMap.
//
// For example, say we have idxMap = [0, 0, 1, 2, 0, 0] and a render expression
// like '(@1 + @4) / @3`, then it'll be updated into '(@1 + @3) / @2'. Such an
// idxMap indicates that the table has 6 columns and only 3 of them (0th, 2nd,
// 3rd) are needed.
//
// typsBeforeRemapping need to contain all the types of columns before the
// mapping of idxMap was applied. These will only be used if post.RenderExprs is
// not nil.
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
func remapPostProcessSpec(
	flowCtx *execinfra.FlowCtx,
	post *execinfrapb.PostProcessSpec,
	idxMap []int,
	helper *colexecargs.ExprHelper,
	typsBeforeRemapping []*types.T,
) error {
	if post.Projection {
		outputColumns := post.OutputColumns
		if flowCtx.PreserveFlowSpecs && post.OriginalOutputColumns == nil {
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
		if flowCtx.PreserveFlowSpecs && post.OriginalRenderExprs == nil {
			// This is the first time we're modifying this PostProcessSpec, but
			// we've been asked to preserve the specs, so we have to set the
			// original render expressions. We are also careful to allocate a
			// new slice to populate the updated render expressions.
			post.OriginalRenderExprs = renderExprs
			post.RenderExprs = make([]execinfrapb.Expression, len(renderExprs))
		}
		var err error
		for i := range renderExprs {
			// Make sure that the render expression is deserialized if we
			// are on the remote node.
			//
			// It is ok to use the evalCtx of the flowCtx since it won't be
			// mutated (we are not evaluating the expressions).
			post.RenderExprs[i].LocalExpr, err = helper.ProcessExpr(renderExprs[i], flowCtx.EvalCtx, typsBeforeRemapping)
			if err != nil {
				return err
			}
			post.RenderExprs[i].LocalExpr = physicalplan.RemapIVarsInTypedExpr(renderExprs[i].LocalExpr, idxMap)
		}
	}
	return nil
}
