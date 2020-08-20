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
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/stringarena"
)

// hashAggregatorHelper is a helper for the hash aggregator that facilitates
// the selection of tuples on which to perform the aggregation.
type hashAggregatorHelper interface {
	// makeSeenMaps returns a slice of maps used to handle distinct aggregation
	// of a single aggregation bucket. A corresponding entry in the slice is
	// nil if the function doesn't have a DISTINCT clause. The slice itself
	// will be nil whenever no aggregate function has a DISTINCT clause.
	makeSeenMaps() []map[string]struct{}
	// performAggregation performs aggregation of all functions in bucket on
	// tuples in vecs that are relevant for each function (meaning that only
	// tuples that pass the criteria - like DISTINCT and/or FILTER will be
	// aggregated).
	performAggregation(ctx context.Context, vecs []coldata.Vec, inputLen int, sel []int, bucket *hashAggBucket)
}

// newHashAggregatorHelper creates a new hashAggregatorHelper based on the
// provided aggregator specification. If there are no functions that perform
// either DISTINCT or FILTER aggregation, then the defaultHashAggregatorHelper
// is returned which has negligible performance overhead.
func newHashAggregatorHelper(
	allocator *colmem.Allocator,
	memAccount *mon.BoundAccount,
	inputTypes []*types.T,
	spec *execinfrapb.AggregatorSpec,
	datumAlloc *sqlbase.DatumAlloc,
) hashAggregatorHelper {
	hasDistinct, hasFilterAgg := false, false
	aggFilter := make([]int, len(spec.Aggregations))
	for i, aggFn := range spec.Aggregations {
		if aggFn.Distinct {
			hasDistinct = true
		}
		if aggFn.FilterColIdx != nil {
			aggFilter[i] = int(*aggFn.FilterColIdx)
			hasFilterAgg = true
		} else {
			aggFilter[i] = tree.NoColumnIdx
		}
	}

	if !hasDistinct && !hasFilterAgg {
		return newDefaultHashAggregatorHelper(spec)
	}
	filters := make([]*filteringSingleFunctionHelper, len(spec.Aggregations))
	for i, filterIdx := range aggFilter {
		filters[i] = newFilteringHashAggHelper(allocator, inputTypes, filterIdx)
	}
	if !hasDistinct {
		return newFilteringHashAggregatorHelper(spec, filters)
	}
	return newFilteringDistinctHashAggregatorHelper(memAccount, inputTypes, spec, filters, datumAlloc)
}

// defaultHashAggregatorHelper is the default hashAggregatorHelper for the case
// when no aggregate function is performing DISTINCT or FILTERing aggregation.
type defaultHashAggregatorHelper struct {
	spec *execinfrapb.AggregatorSpec
}

var _ hashAggregatorHelper = &defaultHashAggregatorHelper{}

func newDefaultHashAggregatorHelper(spec *execinfrapb.AggregatorSpec) hashAggregatorHelper {
	return &defaultHashAggregatorHelper{spec: spec}
}

func (h *defaultHashAggregatorHelper) makeSeenMaps() []map[string]struct{} {
	return nil
}

func (h *defaultHashAggregatorHelper) performAggregation(
	_ context.Context, vecs []coldata.Vec, inputLen int, sel []int, bucket *hashAggBucket,
) {
	for fnIdx, fn := range bucket.fns {
		fn.Compute(vecs, h.spec.Aggregations[fnIdx].ColIdx, inputLen, sel)
	}
}

// hashAggregatorHelperBase is a utility struct that provides non-default
// hashAggregatorHelpers with the logic necessary for saving/restoring the
// input state.
type hashAggregatorHelperBase struct {
	spec *execinfrapb.AggregatorSpec

	vecs    []coldata.Vec
	usesSel bool
	origSel []int
	origLen int
}

func newAggregatorHelperBase(spec *execinfrapb.AggregatorSpec) *hashAggregatorHelperBase {
	b := &hashAggregatorHelperBase{spec: spec}
	b.origSel = make([]int, coldata.BatchSize())
	return b
}

func (h *hashAggregatorHelperBase) saveState(vecs []coldata.Vec, origLen int, origSel []int) {
	h.vecs = vecs
	h.origLen = origLen
	h.usesSel = origSel != nil
	if h.usesSel {
		copy(h.origSel[:h.origLen], origSel[:h.origLen])
	}
}

func (h *hashAggregatorHelperBase) restoreState() ([]coldata.Vec, int, []int) {
	sel := h.origSel
	if !h.usesSel {
		sel = nil
	}
	return h.vecs, h.origLen, sel
}

// filteringSingleFunctionHelper is a utility struct that helps with handling
// of a FILTER clause of a single aggregate function.
type filteringSingleFunctionHelper struct {
	filter      colexecbase.Operator
	filterInput *singleBatchOperator
}

var noFilterHashAggHelper = &filteringSingleFunctionHelper{}

// newFilteringHashAggHelper returns a new filteringSingleFunctionHelper.
// tree.NoColumnIdx index can be used to indicate that there is no FILTER
// clause for the aggregate function.
func newFilteringHashAggHelper(
	allocator *colmem.Allocator, typs []*types.T, filterIdx int,
) *filteringSingleFunctionHelper {
	if filterIdx == tree.NoColumnIdx {
		return noFilterHashAggHelper
	}
	filterInput := newSingleBatchOperator(allocator, typs)
	h := &filteringSingleFunctionHelper{
		filter:      newBoolVecToSelOp(filterInput, filterIdx),
		filterInput: filterInput,
	}
	return h
}

// applyFilter returns the updated selection vector that includes only tuples
// for which filtering column has 'true' value set. It also returns whether
// state might have been modified.
func (h *filteringSingleFunctionHelper) applyFilter(
	ctx context.Context, vecs []coldata.Vec, inputLen int, sel []int,
) (_ []coldata.Vec, _ int, _ []int, maybeModified bool) {
	if h.filter == nil {
		return vecs, inputLen, sel, false
	}
	h.filterInput.reset(vecs, inputLen, sel)
	newBatch := h.filter.Next(ctx)
	return newBatch.ColVecs(), newBatch.Length(), newBatch.Selection(), true
}

// filteringHashAggregatorHelper is a hashAggregatorHelper that handles the
// aggregate functions which have at least one FILTER clause but no DISTINCT
// clauses.
type filteringHashAggregatorHelper struct {
	*hashAggregatorHelperBase

	filters []*filteringSingleFunctionHelper
}

var _ hashAggregatorHelper = &filteringHashAggregatorHelper{}

func newFilteringHashAggregatorHelper(
	spec *execinfrapb.AggregatorSpec, filters []*filteringSingleFunctionHelper,
) hashAggregatorHelper {
	h := &filteringHashAggregatorHelper{
		hashAggregatorHelperBase: newAggregatorHelperBase(spec),
		filters:                  filters,
	}
	return h
}

func (h *filteringHashAggregatorHelper) makeSeenMaps() []map[string]struct{} {
	return nil
}

func (h *filteringHashAggregatorHelper) performAggregation(
	ctx context.Context, vecs []coldata.Vec, inputLen int, sel []int, bucket *hashAggBucket,
) {
	h.saveState(vecs, inputLen, sel)
	for fnIdx, fn := range bucket.fns {
		var maybeModified bool
		vecs, inputLen, sel, maybeModified = h.filters[fnIdx].applyFilter(ctx, vecs, inputLen, sel)
		if inputLen > 0 {
			// It is possible that all tuples to aggregate have been filtered
			// out, so we need to check the length.
			fn.Compute(vecs, h.spec.Aggregations[fnIdx].ColIdx, inputLen, sel)
		}
		if maybeModified {
			// Restore the state so that the next iteration sees the input with
			// the original selection vector and length.
			vecs, inputLen, sel = h.restoreState()
		}
	}
}

// filteringDistinctHashAggregatorHelper is a hashAggregatorHelper that handles
// the aggregate functions with any number of DISTINCT and/or FILTER clauses.
// The helper should be shared among all groups for aggregation. The filtering
// is delegated to filteringHashAggHelpers, and this struct handles the
// "distinctness" of aggregation.
// Note that the "distinctness" of tuples is handled by encoding aggregation
// columns of a tuple (one tuple at a time) and storing it in a seen map that
// is separate for each aggregation group and for each aggregate function with
// DISTINCT clause.
// Other approaches have been prototyped but showed worse performance:
// - using the vectorized hash table - the benefit of such approach is that we
// don't reduce ourselves to one tuple at a time (because we would be hashing
// the full columns at once), but the big disadvantage is that the full tuples
// are stored in the hash table (instead of an encoded representation).
// TODO(yuzefovich): reevaluate the vectorized hash table once it is
// dynamically resizable and can store only a subset of columns from the input.
// - using a single global map for a particular aggregate function that is
// shared among all aggregation groups - the benefit of such approach is that
// we only have a handful of map, but it turned out that such global map grows
// a lot bigger and has worse performance.
type filteringDistinctHashAggregatorHelper struct {
	*hashAggregatorHelperBase

	inputTypes       []*types.T
	filters          []*filteringSingleFunctionHelper
	aggColsConverter *vecToDatumConverter
	arena            stringarena.Arena
	datumAlloc       *sqlbase.DatumAlloc
	scratch          struct {
		ed      sqlbase.EncDatum
		encoded []byte
		// converted is a scratch space for converting a single element.
		converted []tree.Datum
		sel       []int
	}
}

var _ hashAggregatorHelper = &filteringDistinctHashAggregatorHelper{}

func newFilteringDistinctHashAggregatorHelper(
	memAccount *mon.BoundAccount,
	inputTypes []*types.T,
	spec *execinfrapb.AggregatorSpec,
	filters []*filteringSingleFunctionHelper,
	datumAlloc *sqlbase.DatumAlloc,
) hashAggregatorHelper {
	h := &filteringDistinctHashAggregatorHelper{
		hashAggregatorHelperBase: newAggregatorHelperBase(spec),
		inputTypes:               inputTypes,
		arena:                    stringarena.Make(memAccount),
		datumAlloc:               datumAlloc,
		filters:                  filters,
	}
	var vecIdxsToConvert []int
	for _, aggFn := range spec.Aggregations {
		if aggFn.Distinct {
			for _, aggCol := range aggFn.ColIdx {
				found := false
				for _, vecIdx := range vecIdxsToConvert {
					if vecIdx == int(aggCol) {
						found = true
						break
					}
				}
				if !found {
					vecIdxsToConvert = append(vecIdxsToConvert, int(aggCol))
				}
			}
		}
	}
	h.aggColsConverter = newVecToDatumConverter(len(inputTypes), vecIdxsToConvert)
	h.scratch.converted = []tree.Datum{nil}
	h.scratch.sel = make([]int, coldata.BatchSize())
	return h
}

func (h *filteringDistinctHashAggregatorHelper) makeSeenMaps() []map[string]struct{} {
	// Note that we consciously don't account for the memory used under seen
	// maps because that memory is likely noticeably smaller than the memory
	// used (and accounted for) in other parts of the hash aggregation (the
	// vectorized hash table and the aggregate functions).
	seen := make([]map[string]struct{}, len(h.spec.Aggregations))
	for i, aggFn := range h.spec.Aggregations {
		if aggFn.Distinct {
			seen[i] = make(map[string]struct{})
		}
	}
	return seen
}

// selectDistinctTuples returns new selection vector that contains only tuples
// that haven't been seen by the aggregate function yet when the function
// performs DISTINCT aggregation. aggColsConverter must have already done the
// conversion of the relevant aggregate columns *without* deselection. This
// function assumes that seen map is non-nil and is the same that is used for
// all batches from the same aggregation group.
func (h *filteringDistinctHashAggregatorHelper) selectDistinctTuples(
	ctx context.Context, inputLen int, sel []int, inputIdxs []uint32, seen map[string]struct{},
) (newLen int, newSel []int) {
	newSel = h.scratch.sel
	var (
		tupleIdx int
		err      error
		s        string
	)
	for idx := 0; idx < inputLen; idx++ {
		h.scratch.encoded = h.scratch.encoded[:0]
		tupleIdx = idx
		if sel != nil {
			tupleIdx = sel[idx]
		}
		for _, colIdx := range inputIdxs {
			h.scratch.ed.Datum = h.aggColsConverter.getDatumColumn(int(colIdx))[tupleIdx]
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
	return
}

// performAggregation executes Compute on all fns paying attention to distinct
// tuples if the corresponding function performs DISTINCT aggregation (as well
// as to any present FILTER clauses). For such functions the approach is as
// follows:
// 1. Store the input state because we will be modifying some of it.
// 2. Convert all aggregate columns of functions that perform DISTINCT
//    aggregation.
// 3. For every function:
//    1) Apply the filter to the selection vector of the input.
//    2) Update the (possibly updated) selection vector to include only tuples
//       we haven't yet seen making sure to remember that new tuples we have
//       just seen.
//    3) Execute Compute on the updated state.
//    4) Restore the state to the original state (if it might have been
//       modified).
func (h *filteringDistinctHashAggregatorHelper) performAggregation(
	ctx context.Context, vecs []coldata.Vec, inputLen int, sel []int, bucket *hashAggBucket,
) {
	h.saveState(vecs, inputLen, sel)
	h.aggColsConverter.convertVecs(vecs, inputLen, sel)
	var maybeModified bool
	for aggFnIdx, aggFn := range h.spec.Aggregations {
		vecs, inputLen, sel, maybeModified = h.filters[aggFnIdx].applyFilter(ctx, vecs, inputLen, sel)
		if inputLen > 0 && aggFn.Distinct {
			inputLen, sel = h.selectDistinctTuples(ctx, inputLen, sel, aggFn.ColIdx, bucket.seen[aggFnIdx])
			maybeModified = true
		}
		if inputLen > 0 {
			bucket.fns[aggFnIdx].Compute(vecs, aggFn.ColIdx, inputLen, sel)
		}
		if maybeModified {
			vecs, inputLen, sel = h.restoreState()
		}
	}
}

// singleBatchOperator is a helper colexecbase.Operator that returns the
// provided vectors as a batch on the first call to Next() and zero batch on
// all consequent calls (until it is reset). It must be reset before it can be
// used for the first time.
type singleBatchOperator struct {
	colexecbase.ZeroInputNode
	NonExplainable

	nexted bool
	batch  coldata.Batch
}

var _ colexecbase.Operator = &singleBatchOperator{}

func newSingleBatchOperator(allocator *colmem.Allocator, typs []*types.T) *singleBatchOperator {
	return &singleBatchOperator{
		batch: allocator.NewMemBatchNoCols(typs, coldata.BatchSize()),
	}
}

func (o *singleBatchOperator) Init() {}

func (o *singleBatchOperator) Next(context.Context) coldata.Batch {
	if o.nexted {
		return coldata.ZeroBatch
	}
	o.nexted = true
	return o.batch
}

func (o *singleBatchOperator) reset(vecs []coldata.Vec, inputLen int, sel []int) {
	o.nexted = false
	for i, vec := range vecs {
		o.batch.ReplaceCol(vec, i)
	}
	o.batch.SetLength(inputLen)
	o.batch.SetSelection(sel != nil)
	if sel != nil {
		copy(o.batch.Selection(), sel[:inputLen])
	}
}
