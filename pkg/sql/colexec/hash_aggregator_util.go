// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexec

import (
	"context"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/stringarena"
)

// hashAggregatorHelper is a helper for the hash aggregator that facilitates
// the selection of tuples on which to perform the aggregation.
type hashAggregatorHelper interface {
	// makeSeenMaps returns a dense slice of maps used to handle distinct
	// aggregation of a single aggregation group (it is of the same length as
	// the number of functions with DISTINCT clause). It will be nil whenever
	// no aggregate function has a DISTINCT clause.
	makeSeenMaps() []map[string]struct{}
	// performAggregation performs aggregation of all functions in fns on all
	// tuples in batch that are relevant for each function (meaning that only
	// tuples that pass the criteria - like DISTINCT and/or FILTER will be
	// aggregated). seen is a dense slice of maps used for storing encoded
	// tuples that have already been seen by the corresponding group (it can be
	// nil when no aggregate function performs distinct aggregation).
	performAggregation(ctx context.Context, batch coldata.Batch, fns []aggregateFunc, seen []map[string]struct{})
}

// newHashAggregatorHelper creates a new hashAggregatorHelper based on provided
// aggDistinct and aggFilter vectors. If there are no functions that perform
// either DISTINCT or FILTER aggregation, then the defaultHashAggregatorHelper
// is returned which has negligible performance overhead.
// The arguments are:
// - aggDistinct is the boolean vector that for each aggregate function
// indicates whether the function is performing DISTINCT aggregation. It can be
// nil which means that all functions perform regular (non-distinct)
// aggregation.
// - aggFilter is the integer vector that for each aggregate function indicates
// the column index on which FILTER clause must be evaluated, that column must
// be of boolean type. It can be left nil which means that no function has a
// FILTER clause.
func newHashAggregatorHelper(
	allocator *colmem.Allocator,
	inputTypes []*types.T,
	groupCols []uint32,
	aggCols [][]uint32,
	aggDistinct []bool,
	aggFilter []int,
	datumAlloc *sqlbase.DatumAlloc,
) hashAggregatorHelper {
	if aggFilter == nil {
		aggFilter = make([]int, len(aggCols))
		for i := range aggFilter {
			aggFilter[i] = tree.NoColumnIdx
		}
	}
	hasFilterAgg := false
	// distinctAggIdxs is a dense list of function indices that perform
	// distinct aggregation.
	var distinctAggIdxs []int
	for i := range aggCols {
		if aggDistinct != nil && aggDistinct[i] {
			distinctAggIdxs = append(distinctAggIdxs, i)
		}
		if aggFilter[i] != tree.NoColumnIdx {
			hasFilterAgg = true
		}
	}
	if len(distinctAggIdxs) == 0 && !hasFilterAgg {
		return newDefaultHashAggregatorHelper(aggCols)
	}
	if len(distinctAggIdxs) > 0 && !hasFilterAgg {
		return newDistinctHashAggregatorHelper(allocator, inputTypes, groupCols, aggCols, distinctAggIdxs, datumAlloc)
	}
	filters := make([]*filteringHashAggHelper, len(aggCols))
	for i, filterIdx := range aggFilter {
		filters[i] = newFilteringHashAggHelper(filterIdx)
	}
	if len(distinctAggIdxs) == 0 {
		return newFilteringHashAggregatorHelper(aggCols, filters)
	}
	return newFilteringDistinctHashAggregatorHelper(allocator, inputTypes, groupCols, aggCols, distinctAggIdxs, filters, datumAlloc)
}

// defaultHashAggregatorHelper is the default hashAggregatorHelper for the case
// when no aggregate function is performing DISTINCT or FILTERing aggregation.
type defaultHashAggregatorHelper struct {
	aggCols [][]uint32
}

var _ hashAggregatorHelper = &defaultHashAggregatorHelper{}

func newDefaultHashAggregatorHelper(aggCols [][]uint32) hashAggregatorHelper {
	return &defaultHashAggregatorHelper{aggCols: aggCols}
}

func (h *defaultHashAggregatorHelper) makeSeenMaps() []map[string]struct{} {
	return nil
}

func (h *defaultHashAggregatorHelper) performAggregation(
	_ context.Context, batch coldata.Batch, fns []aggregateFunc, _ []map[string]struct{},
) {
	for fnIdx, fn := range fns {
		fn.Compute(batch, h.aggCols[fnIdx])
	}
}

// hashAggregatorHelperBase is a utility struct that provides non-default
// hashAggregatorHelpers with the logic necessary for saving/restoring the
// batch.
type hashAggregatorHelperBase struct {
	aggCols [][]uint32

	usesSel bool
	origSel []int
	origLen int
}

func newAggregatorHelperBase(aggCols [][]uint32) *hashAggregatorHelperBase {
	b := &hashAggregatorHelperBase{aggCols: aggCols}
	b.origSel = make([]int, coldata.BatchSize())
	return b
}

func (h *hashAggregatorHelperBase) saveBatch(batch coldata.Batch) {
	h.origLen = batch.Length()
	sel := batch.Selection()
	h.usesSel = sel != nil
	if h.usesSel {
		copy(h.origSel[:h.origLen], sel[:h.origLen])
	}
}

func (h *hashAggregatorHelperBase) restoreBatch(batch coldata.Batch) {
	batch.SetSelection(h.usesSel)
	if h.usesSel {
		copy(batch.Selection()[:h.origLen], h.origSel[:h.origLen])
	}
	batch.SetLength(h.origLen)
}

// filteringHashAggHelper is a utility struct that helps with handling of a
// FILTER clause of a single aggregate function.
type filteringHashAggHelper struct {
	filter      colexecbase.Operator
	filterInput *singleBatchOperator
}

var noFilterHashAggHelper = &filteringHashAggHelper{}

// newFilteringHashAggHelper returns a new filteringHashAggHelper.
// tree.NoColumnIdx index can be used to indicate that there is no FILTER
// clause for the aggregate function.
func newFilteringHashAggHelper(filterIdx int) *filteringHashAggHelper {
	if filterIdx == tree.NoColumnIdx {
		return noFilterHashAggHelper
	}
	filterInput := &singleBatchOperator{}
	h := &filteringHashAggHelper{
		filter:      newBoolVecToSelOp(filterInput, filterIdx),
		filterInput: filterInput,
	}
	return h
}

// applyFilter updates the selection vector of batch to include only those
// tuples for which filtering column has 'true' value set. It also returns
// whether batch might have been modified.
func (h *filteringHashAggHelper) applyFilter(
	ctx context.Context, batch coldata.Batch,
) (_ coldata.Batch, maybeModified bool) {
	if h.filter == nil {
		return batch, false
	}
	h.filterInput.reset(batch)
	newBatch := h.filter.Next(ctx)
	return newBatch, true
}

// filteringHashAggregatorHelper is a hashAggregatorHelper that handles the
// aggregate functions which have at least one FILTER clause but no DISTINCT
// clauses.
type filteringHashAggregatorHelper struct {
	*hashAggregatorHelperBase

	filters []*filteringHashAggHelper
}

var _ hashAggregatorHelper = &filteringHashAggregatorHelper{}

func newFilteringHashAggregatorHelper(
	aggCols [][]uint32, filters []*filteringHashAggHelper,
) hashAggregatorHelper {
	h := &filteringHashAggregatorHelper{
		hashAggregatorHelperBase: newAggregatorHelperBase(aggCols),
		filters:                  filters,
	}
	return h
}

func (h *filteringHashAggregatorHelper) makeSeenMaps() []map[string]struct{} {
	return nil
}

func (h *filteringHashAggregatorHelper) performAggregation(
	ctx context.Context, batch coldata.Batch, fns []aggregateFunc, _ []map[string]struct{},
) {
	h.saveBatch(batch)
	for fnIdx, fn := range fns {
		batchToComputeOn, maybeModified := h.filters[fnIdx].applyFilter(ctx, batch)
		if batchToComputeOn.Length() > 0 {
			// It is possible that all tuples to aggregate have been filtered
			// out, so we need to check the length.
			fn.Compute(batchToComputeOn, h.aggCols[fnIdx])
		}
		if maybeModified {
			// Restore the batch so that the next iteration (or the caller of this
			// function) sees the batch with the original selection vector and
			// length.
			h.restoreBatch(batch)
		}
	}
}

// distinctHashAggregatorHelperBase is a utility struct shared by
// hashAggregatorHelpers that perform DISTINCT aggregation.
// Note that the "distinctness" of tuples is handled by encoding grouping
// (which is done once, one on the first batch of the group) and aggregation
// columns of a tuple (one tuple at a time) and storing it in a map.
// Another approach has been prototyped but showed worse performance:
// - using the vectorized hash table - the benefit of such approach is that we
// don't reduce ourselves to one tuple at a time (because we would be hashing
// the full columns at once), but the big disadvantage is that the full tuples
// are stored in the hash table (instead of an encoded representation)
type distinctHashAggregatorHelperBase struct {
	*hashAggregatorHelperBase

	inputTypes         []*types.T
	groupCols          []int
	nonDistinctAggIdxs []int
	distinctAggIdxs    []int
	seenAlloc          *seenMapsAlloc
	arena              stringarena.Arena
	datumAlloc         *sqlbase.DatumAlloc
	scratch            struct {
		ed      sqlbase.EncDatum
		encoded []byte
		// converted is a scratch space for converting a single element.
		converted []tree.Datum
	}
}

func newDistinctHashAggregatorHelperBase(
	allocator *colmem.Allocator,
	inputTypes []*types.T,
	groupCols []uint32,
	aggCols [][]uint32,
	distinctAggIdxs []int,
	datumAlloc *sqlbase.DatumAlloc,
) *distinctHashAggregatorHelperBase {
	h := &distinctHashAggregatorHelperBase{
		hashAggregatorHelperBase: newAggregatorHelperBase(aggCols),
		inputTypes:               inputTypes,
		nonDistinctAggIdxs:       make([]int, 0, len(aggCols)-len(distinctAggIdxs)),
		distinctAggIdxs:          distinctAggIdxs,
		seenAlloc:                newSeenMapsAlloc(allocator, len(distinctAggIdxs)),
		arena:                    stringarena.Make(allocator.GetAccount()),
		datumAlloc:               datumAlloc,
	}
	for aggIdx := range aggCols {
		isDistinct := false
		for _, distinctAggIdx := range distinctAggIdxs {
			if aggIdx == distinctAggIdx {
				isDistinct = true
				break
			}
		}
		if !isDistinct {
			h.nonDistinctAggIdxs = append(h.nonDistinctAggIdxs, aggIdx)
		}
	}
	h.scratch.converted = []tree.Datum{nil}
	h.groupCols = make([]int, len(groupCols))
	for i := range groupCols {
		h.groupCols[i] = int(groupCols[i])
	}
	return h
}

// seenMapsAlloc is a utility struct that batches allocations of seen map
// slices.
type seenMapsAlloc struct {
	allocator         *colmem.Allocator
	numDistinctAggFns int
	newAllocCount     int
	newAllocMemSize   int64
	buf               []map[string]struct{}
}

func newSeenMapsAlloc(allocator *colmem.Allocator, numDistinctAggFns int) *seenMapsAlloc {
	return &seenMapsAlloc{
		allocator:         allocator,
		numDistinctAggFns: numDistinctAggFns,
		newAllocCount:     numDistinctAggFns * hashAggregatorAllocSize,
		newAllocMemSize:   int64(numDistinctAggFns * hashAggregatorAllocSize * int(sizeOfSeenMap)),
	}
}

const sizeOfSeenMap = unsafe.Sizeof(map[string]struct{}{})

func (a *seenMapsAlloc) newSeenMapsSlice() []map[string]struct{} {
	if len(a.buf) == 0 {
		a.allocator.AdjustMemoryUsage(a.newAllocMemSize)
		a.buf = make([]map[string]struct{}, a.newAllocCount)
	}
	ret := a.buf[0:a.numDistinctAggFns]
	a.buf = a.buf[a.numDistinctAggFns:]
	return ret
}

func (h *distinctHashAggregatorHelperBase) makeSeenMaps() []map[string]struct{} {
	seen := h.seenAlloc.newSeenMapsSlice()
	for i := range h.distinctAggIdxs {
		seen[i] = make(map[string]struct{})
	}
	return seen
}

// selectDistinctTuples updates the batch in-place to select only those tuples
// that haven't been seen by the aggregate function yet when the function
// performs DISTINCT aggregation. aggColsConverter must have already done the
// conversion of the relevant aggregate columns. This function assumes that
// seen map is non-nil and is the same that is used for all batches from the
// same aggregation group.
func (h *distinctHashAggregatorHelperBase) selectDistinctTuples(
	ctx context.Context,
	batch coldata.Batch,
	aggFnIdx int,
	aggColsConverter *vecToDatumConverter,
	seen map[string]struct{},
) {
	oldLen := batch.Length()
	oldSel := batch.Selection()
	usesSel := oldSel != nil
	batch.SetSelection(true)
	newSel := batch.Selection()
	var (
		tupleIdx int
		err      error
		newLen   int
		s        string
	)
	for idx := 0; idx < oldLen; idx++ {
		h.scratch.encoded = h.scratch.encoded[:0]
		for _, colIdx := range h.aggCols[aggFnIdx] {
			h.scratch.ed.Datum = aggColsConverter.getDatumColumn(int(colIdx))[idx]
			h.scratch.encoded, err = h.scratch.ed.Fingerprint(
				h.inputTypes[colIdx], h.datumAlloc, h.scratch.encoded,
			)
			if err != nil {
				colexecerror.InternalError(err)
			}
		}
		if _, seenPreviously := seen[string(h.scratch.encoded)]; !seenPreviously {
			s, err = h.arena.AllocBytes(ctx, h.scratch.encoded)
			if err != nil {
				colexecerror.InternalError(err)
			}
			seen[s] = struct{}{}
			tupleIdx = idx
			if usesSel {
				tupleIdx = oldSel[idx]
			}
			newSel[newLen] = tupleIdx
			newLen++
		}
	}
	batch.SetLength(newLen)
}

// distinctHashAggregatorHelper is a hashAggregatorHelper that handles the
// aggregate functions with any number of DISTINCT but with *no* FILTER
// clauses. The helper should be shared among all groups for aggregation. The
// fact that it doesn't handle any filtering aggregation allows us to use the
// same vecToDatumConverter for all aggregate functions.
type distinctHashAggregatorHelper struct {
	*distinctHashAggregatorHelperBase
	// aggColsConverter is responsible for converting all columns used all
	// aggregate functions once a new batch is received.
	aggColsConverter *vecToDatumConverter
}

var _ hashAggregatorHelper = &distinctHashAggregatorHelper{}

func newDistinctHashAggregatorHelper(
	allocator *colmem.Allocator,
	inputTypes []*types.T,
	groupCols []uint32,
	aggCols [][]uint32,
	distinctAggIdxs []int,
	datumAlloc *sqlbase.DatumAlloc,
) hashAggregatorHelper {
	h := &distinctHashAggregatorHelper{
		distinctHashAggregatorHelperBase: newDistinctHashAggregatorHelperBase(
			allocator, inputTypes, groupCols, aggCols, distinctAggIdxs, datumAlloc,
		),
	}
	// aggColsToConvert will contain indices of columns that are inputs to at
	// least one aggregate function.
	var aggColsToConvert util.FastIntSet
	for _, aggIdx := range distinctAggIdxs {
		for _, aggCol := range aggCols[aggIdx] {
			aggColsToConvert.Add(int(aggCol))
		}
	}
	aggVecIdxsToConvert := make([]int, 0, aggColsToConvert.Len())
	for i, ok := aggColsToConvert.Next(0); ok; i, ok = aggColsToConvert.Next(i + 1) {
		aggVecIdxsToConvert = append(aggVecIdxsToConvert, i)
	}
	h.aggColsConverter = newVecToDatumConverter(len(inputTypes), aggVecIdxsToConvert)
	return h
}

// performAggregation executes Compute on all fns paying attention to distinct
// tuples if the corresponding function performs DISTINCT aggregation. For such
// functions the approach is as follows:
// 1. store the batch state because we will be modifying some of it
// 2. convert all necessary aggregate columns to datums
// 3. for every function:
//    1) update the batch's selection vector to include only tuples we haven't
//       yet seen making sure to remember that new tuples we have just seen
//    2) execute Compute on the updated batch
//    3) restore the batch to the original state (if it might have been
//       modified).
func (h *distinctHashAggregatorHelper) performAggregation(
	ctx context.Context, batch coldata.Batch, fns []aggregateFunc, seen []map[string]struct{},
) {
	h.saveBatch(batch)
	h.aggColsConverter.convertBatch(batch)
	// First compute all non-distinct aggregations.
	for _, fnIdx := range h.nonDistinctAggIdxs {
		fns[fnIdx].Compute(batch, h.aggCols[fnIdx])
	}
	// Now compute all distinct aggregations restoring the batch after each one.
	for distinctAggSlot, fnIdx := range h.distinctAggIdxs {
		h.selectDistinctTuples(ctx, batch, fnIdx, h.aggColsConverter, seen[distinctAggSlot])
		if batch.Length() > 0 {
			fns[fnIdx].Compute(batch, h.aggCols[fnIdx])
		}
		h.restoreBatch(batch)
	}
}

// filteringDistinctHashAggregatorHelper is a hashAggregatorHelper that handles
// the aggregate functions with any number of DISTINCT and/or FILTER clauses.
// The helper should be shared among all groups for aggregation. The filtering
// is delegated to filteringHashAggHelpers, and this struct handles the
// "distinctness" of aggregation.
type filteringDistinctHashAggregatorHelper struct {
	*distinctHashAggregatorHelperBase

	filters []*filteringHashAggHelper
	// aggColsConverters is a dense list of separate converters for every
	// function that performs DISTINCT aggregation. Due to the presence of
	// filters, we cannot use a single converter like in
	// distinctHashAggregatorHelper because a filter is applied before the
	// distinctness check, so the tuples seen by the aggregation functions can
	// be different.
	// TODO(yuzefovich): we could have a shared converter among all aggregate
	// functions that don't have a filter clause, but it doesn't seem to be
	// worth introducing such logic at the moment.
	aggColsConverters []*vecToDatumConverter
}

var _ hashAggregatorHelper = &filteringDistinctHashAggregatorHelper{}

func newFilteringDistinctHashAggregatorHelper(
	allocator *colmem.Allocator,
	inputTypes []*types.T,
	groupCols []uint32,
	aggCols [][]uint32,
	distinctAggIdxs []int,
	filters []*filteringHashAggHelper,
	datumAlloc *sqlbase.DatumAlloc,
) hashAggregatorHelper {
	h := &filteringDistinctHashAggregatorHelper{
		distinctHashAggregatorHelperBase: newDistinctHashAggregatorHelperBase(
			allocator, inputTypes, groupCols, aggCols, distinctAggIdxs, datumAlloc,
		),
		filters:           filters,
		aggColsConverters: make([]*vecToDatumConverter, len(distinctAggIdxs)),
	}
	for i, aggIdx := range distinctAggIdxs {
		aggColsToConvert := make([]int, len(aggCols[aggIdx]))
		for i, aggCol := range aggCols[aggIdx] {
			aggColsToConvert[i] = int(aggCol)
		}
		h.aggColsConverters[i] = newVecToDatumConverter(len(inputTypes), aggColsToConvert)
	}
	return h
}

// performAggregation executes Compute on all fns paying attention to distinct
// tuples if the corresponding function performs DISTINCT aggregation. For such
// functions the approach is as follows:
// 1. store the batch state because we will be modifying some of it
// 2. for every function:
//    1) apply the filter to the selection vector of the batch
//    2) convert all aggregate columns for this function to datums if the
//       function performs DISTINCT aggregation
//    3) update the batch's (possibly updated) selection vector to include only
//       tuples we haven't yet seen making sure to remember that new tuples we
//       have just seen
//    4) execute Compute on the updated batch
//    5) restore the batch to the original state (if it might have been
//       modified).
func (h *filteringDistinctHashAggregatorHelper) performAggregation(
	ctx context.Context, batch coldata.Batch, fns []aggregateFunc, seen []map[string]struct{},
) {
	h.saveBatch(batch)
	h.saveBatch(batch)
	// First compute all non-distinct aggregations.
	for _, fnIdx := range h.nonDistinctAggIdxs {
		batchToComputeOn, maybeModified := h.filters[fnIdx].applyFilter(ctx, batch)
		if batchToComputeOn.Length() > 0 {
			fns[fnIdx].Compute(batch, h.aggCols[fnIdx])
		}
		if maybeModified {
			h.restoreBatch(batch)
		}
	}
	// Now compute all distinct aggregations restoring the batch after each one.
	for distinctAggSlot, fnIdx := range h.distinctAggIdxs {
		batchToComputeOn, _ := h.filters[fnIdx].applyFilter(ctx, batch)
		if batchToComputeOn.Length() > 0 {
			h.aggColsConverters[distinctAggSlot].convertBatch(batchToComputeOn)
			h.selectDistinctTuples(ctx, batchToComputeOn, fnIdx, h.aggColsConverters[distinctAggSlot], seen[distinctAggSlot])
			if batchToComputeOn.Length() > 0 {
				fns[fnIdx].Compute(batchToComputeOn, h.aggCols[fnIdx])
			}
		}
		h.restoreBatch(batch)
	}
}

// singleBatchOperator is a helper colexecbase.Operator that returns the
// provided batch on the first call to Next() and zero batch on all consequent
// calls (until it is reset). It must be reset before it can be used for the
// first time.
type singleBatchOperator struct {
	colexecbase.ZeroInputNode
	NonExplainable

	nexted bool
	batch  coldata.Batch
}

var _ colexecbase.Operator = &singleBatchOperator{}

func (o *singleBatchOperator) Init() {}

func (o *singleBatchOperator) Next(context.Context) coldata.Batch {
	if o.nexted {
		return coldata.ZeroBatch
	}
	o.nexted = true
	return o.batch
}

func (o *singleBatchOperator) reset(batch coldata.Batch) {
	o.nexted = false
	o.batch = batch
}
