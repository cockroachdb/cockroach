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

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/stringarena"
)

// hashAggregatorHelper is a helper for the hash aggregator that facilitates
// the selection of tuples on which to perform the aggregation.
type hashAggregatorHelper interface {
	// performAggregation performs aggregation of all functions in fns on all
	// tuples in batch that are relevant for each function (meaning that only
	// tuples that pass the criteria - like DISTINCT and/or FILTER will be
	// aggregated).
	performAggregation(ctx context.Context, batch coldata.Batch, fns []aggregateFunc)
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
	hasDistinctAgg, hasFilterAgg := false, false
	for i := range aggCols {
		if aggDistinct != nil && aggDistinct[i] {
			hasDistinctAgg = true
		}
		if aggFilter[i] != tree.NoColumnIdx {
			hasFilterAgg = true
		}
	}
	if !hasDistinctAgg && !hasFilterAgg {
		return newDefaultHashAggregatorHelper(aggCols)
	}
	filters := make([]*filteringHashAggHelper, len(aggCols))
	for i, filterIdx := range aggFilter {
		filters[i] = newFilteringHashAggHelper(filterIdx)
	}
	if !hasDistinctAgg {
		return newFilteringHashAggregatorHelper(aggCols, filters)
	}
	return newCustomHashAggregatorHelper(allocator, inputTypes, groupCols, aggCols, aggDistinct, filters, datumAlloc)
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

func (h *defaultHashAggregatorHelper) performAggregation(
	_ context.Context, batch coldata.Batch, fns []aggregateFunc,
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
// tuples for which filtering column has 'true' value set.
func (h *filteringHashAggHelper) applyFilter(
	ctx context.Context, batch coldata.Batch,
) coldata.Batch {
	if h.filter == nil {
		return batch
	}
	h.filterInput.reset(batch)
	newBatch := h.filter.Next(ctx)
	return newBatch
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

func (h *filteringHashAggregatorHelper) performAggregation(
	ctx context.Context, batch coldata.Batch, fns []aggregateFunc,
) {
	h.saveBatch(batch)
	for fnIdx, fn := range fns {
		batchToComputeOn := h.filters[fnIdx].applyFilter(ctx, batch)
		if batchToComputeOn.Length() > 0 {
			// It is possible that all tuples to aggregate have been filtered
			// out, so we need to check the length.
			fn.Compute(batchToComputeOn, h.aggCols[fnIdx])
		}
		// Restore the batch so that the next iteration (or the caller of this
		// function) sees the batch with the original selection vector and
		// length.
		h.restoreBatch(batch)
	}
}

// customHashAggregatorHelper is a hashAggregatorHelper that is able to handle
// the aggregate function with any number of DISTINCT and FILTER clauses. The
// helper should be shared among all groups for aggregation. The filtering is
// delegated to filteringHashAggHelpers, and this struct handles the
// "distinctness" of aggregation.
//
// Note that the "distinctness" of tuples is handled by encoding grouping and
// aggregation columns of a tuple (one tuple at a time) and storing it in a
// map. A few other approaches have been prototyped but showed worse
// performance:
// 1. using the vectorized hash table - the benefit of such approach is that we
// don't reduce ourselves to one tuple at a time (because we would be hashing
// the full columns at once), but the big disadvantage is that the full tuples
// are stored in the hash table (instead of an encoded representation)
// 2. plumbing the knowledge of "groups" into this helper - the benefit is that
// we would need to encode only aggregation columns and would be resetting the
// maps when a new group starts. This would have worked well for the ordered
// aggregator but is terrible for the hash aggregator because we would need to
// have a separate helper struct for every bucket.
type customHashAggregatorHelper struct {
	*hashAggregatorHelperBase

	filters []*filteringHashAggHelper

	inputTypes []*types.T
	seen       []map[string]struct{}
	// colsToEncode contains the slices of column indices that, when used for
	// encoding, uniquely identify a tuple for the purposes of "distinctness".
	colsToEncode [][]uint32
	arena        stringarena.Arena
	datumAlloc   *sqlbase.DatumAlloc
	scratch      struct {
		ed      sqlbase.EncDatum
		encoded []byte
		// converted is a scratch space for converting a single element.
		converted []tree.Datum
	}
}

var _ hashAggregatorHelper = &customHashAggregatorHelper{}

func newCustomHashAggregatorHelper(
	allocator *colmem.Allocator,
	inputTypes []*types.T,
	groupCols []uint32,
	aggCols [][]uint32,
	aggDistinct []bool,
	filters []*filteringHashAggHelper,
	datumAlloc *sqlbase.DatumAlloc,
) *customHashAggregatorHelper {
	h := &customHashAggregatorHelper{
		hashAggregatorHelperBase: newAggregatorHelperBase(aggCols),
		filters:                  filters,
		inputTypes:               inputTypes,
		seen:                     make([]map[string]struct{}, len(aggCols)),
		colsToEncode:             make([][]uint32, len(aggCols)),
		arena:                    stringarena.Make(allocator.GetAccount()),
		datumAlloc:               datumAlloc,
	}
	for aggIdx, isAggDistinct := range aggDistinct {
		if isAggDistinct {
			h.seen[aggIdx] = make(map[string]struct{})
			hashCols := make([]uint32, len(groupCols)+len(aggCols[aggIdx]))
			copy(hashCols, groupCols)
			copy(hashCols[len(groupCols):], aggCols[aggIdx])
			h.colsToEncode[aggIdx] = hashCols
		}
	}
	h.scratch.converted = []tree.Datum{nil}
	return h
}

// performAggregation executes Compute on all fns paying attention to distinct
// tuples if the corresponding function performs DISTINCT aggregation. For such
// functions the approach is as follows:
// 1. store the batch state because we will be modifying some of it
// 2. for every function:
//    1) apply the filter to the selection vector of the batch
//    2) update the batch's (possibly updated) selection vector to include only
//       tuples we haven't yet seen making sure to remember that new tuples we
//       have just seen
//    3) execute Compute on the updated batch
//    4) restore the batch to the original state.
func (h *customHashAggregatorHelper) performAggregation(
	ctx context.Context, batch coldata.Batch, fns []aggregateFunc,
) {
	h.saveBatch(batch)
	for fnIdx, fn := range fns {
		batchToComputeOn := h.filters[fnIdx].applyFilter(ctx, batch)
		if batchToComputeOn.Length() > 0 {
			h.selectDistinctTuples(ctx, batchToComputeOn, fnIdx)
			if batchToComputeOn.Length() > 0 {
				fn.Compute(batchToComputeOn, h.aggCols[fnIdx])
			}
		}
		h.restoreBatch(batch)
	}
}

// selectDistinctTuples updates the batch in-place to select only those tuples
// that haven't been seen by the aggregate function yet when the function
// performs DISTINCT aggregation (if not, the batch is not changed).
func (h *customHashAggregatorHelper) selectDistinctTuples(
	ctx context.Context, batch coldata.Batch, aggFnIdx int,
) {
	seen := h.seen[aggFnIdx]
	if seen == nil {
		// This aggregate function performs regular (non-distinct) aggregation,
		// so we don't need to do anything.
		return
	}
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
	colVecs := batch.ColVecs()
	fakeSel := []int{0}
	for idx := 0; idx < oldLen; idx++ {
		tupleIdx = idx
		if usesSel {
			tupleIdx = oldSel[idx]
		}
		fakeSel[0] = tupleIdx
		h.scratch.encoded = h.scratch.encoded[:0]
		for _, colIdx := range h.colsToEncode[aggFnIdx] {
			PhysicalTypeColVecToDatum(
				h.scratch.converted, colVecs[colIdx], 1 /* length */, fakeSel, h.datumAlloc,
			)
			h.scratch.ed.Datum = h.scratch.converted[0]
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
			newSel[newLen] = tupleIdx
			newLen++
		}
	}
	batch.SetLength(newLen)
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
