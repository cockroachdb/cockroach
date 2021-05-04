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
	"github.com/cockroachdb/cockroach/pkg/sql/colconv"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecagg"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/stringarena"
	"github.com/cockroachdb/errors"
)

// aggregatorHelper is a helper for the aggregators that facilitates the
// selection of tuples on which to perform the aggregation.
type aggregatorHelper interface {
	// makeSeenMaps returns a slice of maps used to handle distinct aggregation
	// of a single aggregation bucket. A corresponding entry in the slice is
	// nil if the function doesn't have a DISTINCT clause. The slice itself
	// will be nil whenever no aggregate function has a DISTINCT clause.
	makeSeenMaps() []map[string]struct{}
	// performAggregation performs aggregation of all functions in bucket on
	// tuples in vecs that are relevant for each function (meaning that only
	// tuples that pass the criteria - like DISTINCT and/or FILTER will be
	// aggregated). groups is a boolean slice in which 'true' represents a
	// start of the new aggregation group (when groups is nil, then all tuples
	// belong to the same group).
	// Note: inputLen is assumed to be greater than zero.
	performAggregation(ctx context.Context, vecs []coldata.Vec, inputLen int, sel []int, bucket *aggBucket, groups []bool)
}

// newAggregatorHelper creates a new aggregatorHelper based on the provided
// aggregator specification. If there are no functions that perform either
// DISTINCT or FILTER aggregation, then the defaultAggregatorHelper
// is returned which has negligible performance overhead.
func newAggregatorHelper(
	args *colexecagg.NewAggregatorArgs,
	datumAlloc *rowenc.DatumAlloc,
	isHashAgg bool,
	maxBatchSize int,
) aggregatorHelper {
	hasDistinct, hasFilterAgg := false, false
	aggFilter := make([]int, len(args.Spec.Aggregations))
	for i, aggFn := range args.Spec.Aggregations {
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
		return newDefaultAggregatorHelper(args.Spec)
	}
	if !isHashAgg {
		if hasFilterAgg {
			colexecerror.InternalError(errors.AssertionFailedf(
				"filtering ordered aggregation is not supported",
			))
		}
		return newDistinctOrderedAggregatorHelper(args, datumAlloc, maxBatchSize)
	}
	filters := make([]*filteringSingleFunctionHashHelper, len(args.Spec.Aggregations))
	for i, filterIdx := range aggFilter {
		filters[i] = newFilteringHashAggHelper(args, filterIdx, maxBatchSize)
	}
	if !hasDistinct {
		return newFilteringHashAggregatorHelper(args.Spec, filters, maxBatchSize)
	}
	return newFilteringDistinctHashAggregatorHelper(args, filters, datumAlloc, maxBatchSize)
}

// defaultAggregatorHelper is the default aggregatorHelper for the case
// when no aggregate function is performing DISTINCT or FILTERing aggregation.
type defaultAggregatorHelper struct {
	spec *execinfrapb.AggregatorSpec
}

var _ aggregatorHelper = &defaultAggregatorHelper{}

func newDefaultAggregatorHelper(spec *execinfrapb.AggregatorSpec) aggregatorHelper {
	return &defaultAggregatorHelper{spec: spec}
}

func (h *defaultAggregatorHelper) makeSeenMaps() []map[string]struct{} {
	return nil
}

func (h *defaultAggregatorHelper) performAggregation(
	_ context.Context, vecs []coldata.Vec, inputLen int, sel []int, bucket *aggBucket, _ []bool,
) {
	for fnIdx, fn := range bucket.fns {
		fn.Compute(vecs, h.spec.Aggregations[fnIdx].ColIdx, inputLen, sel)
	}
}

// aggregatorHelperBase is a utility struct that provides non-default
// aggregatorHelpers with the logic necessary for saving/restoring the
// input state.
type aggregatorHelperBase struct {
	spec *execinfrapb.AggregatorSpec

	vecs    []coldata.Vec
	usesSel bool
	origSel []int
	origLen int
}

func newAggregatorHelperBase(
	spec *execinfrapb.AggregatorSpec, maxBatchSize int,
) *aggregatorHelperBase {
	b := &aggregatorHelperBase{spec: spec}
	b.origSel = make([]int, maxBatchSize)
	return b
}

func (b *aggregatorHelperBase) saveState(vecs []coldata.Vec, origLen int, origSel []int) {
	b.vecs = vecs
	b.origLen = origLen
	b.usesSel = origSel != nil
	if b.usesSel {
		copy(b.origSel[:b.origLen], origSel[:b.origLen])
	}
}

func (b *aggregatorHelperBase) restoreState() ([]coldata.Vec, int, []int) {
	sel := b.origSel
	if !b.usesSel {
		sel = nil
	}
	return b.vecs, b.origLen, sel
}

// filteringSingleFunctionHashHelper is a utility struct that helps with
// handling of a FILTER clause of a single aggregate function for the hash
// aggregation.
type filteringSingleFunctionHashHelper struct {
	filter      colexecop.Operator
	filterInput *singleBatchOperator
}

var noFilterHashAggHelper = &filteringSingleFunctionHashHelper{}

// newFilteringHashAggHelper returns a new filteringSingleFunctionHashHelper.
// tree.NoColumnIdx index can be used to indicate that there is no FILTER
// clause for the aggregate function.
func newFilteringHashAggHelper(
	args *colexecagg.NewAggregatorArgs, filterIdx int, maxBatchSize int,
) *filteringSingleFunctionHashHelper {
	if filterIdx == tree.NoColumnIdx {
		return noFilterHashAggHelper
	}
	filterInput := newSingleBatchOperator(args.Allocator, args.InputTypes, maxBatchSize)
	h := &filteringSingleFunctionHashHelper{
		filter:      colexecutils.NewBoolVecToSelOp(filterInput, filterIdx),
		filterInput: filterInput,
	}
	return h
}

// applyFilter returns the updated selection vector that includes only tuples
// for which filtering column has 'true' value set. It also returns whether
// state might have been modified.
func (h *filteringSingleFunctionHashHelper) applyFilter(
	ctx context.Context, vecs []coldata.Vec, inputLen int, sel []int,
) (_ []coldata.Vec, _ int, _ []int, maybeModified bool) {
	if h.filter == nil {
		return vecs, inputLen, sel, false
	}
	h.filterInput.reset(vecs, inputLen, sel)
	// Note that it is ok that we call Init on every iteration - it is a noop
	// every time except for the first one.
	h.filter.Init(ctx)
	newBatch := h.filter.Next()
	return newBatch.ColVecs(), newBatch.Length(), newBatch.Selection(), true
}

// filteringHashAggregatorHelper is an aggregatorHelper that handles the
// aggregate functions which have at least one FILTER clause but no DISTINCT
// clauses for the hash aggregation.
type filteringHashAggregatorHelper struct {
	*aggregatorHelperBase

	filters []*filteringSingleFunctionHashHelper
}

var _ aggregatorHelper = &filteringHashAggregatorHelper{}

func newFilteringHashAggregatorHelper(
	spec *execinfrapb.AggregatorSpec, filters []*filteringSingleFunctionHashHelper, maxBatchSize int,
) aggregatorHelper {
	h := &filteringHashAggregatorHelper{
		aggregatorHelperBase: newAggregatorHelperBase(spec, maxBatchSize),
		filters:              filters,
	}
	return h
}

func (h *filteringHashAggregatorHelper) makeSeenMaps() []map[string]struct{} {
	return nil
}

func (h *filteringHashAggregatorHelper) performAggregation(
	ctx context.Context, vecs []coldata.Vec, inputLen int, sel []int, bucket *aggBucket, _ []bool,
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

// distinctAggregatorHelperBase is a utility struct that facilitates handling
// of DISTINCT clause for aggregatorHelpers. Note that the "distinctness" of
// tuples is handled by encoding aggregation columns of a tuple (one tuple at a
// time) and storing it in a seen map that is separate for each aggregation
// group and for each aggregate function with DISTINCT clause.
// Other approaches have been prototyped but showed worse performance:
// - using the vectorized hash table - the benefit of such approach is that we
// don't reduce ourselves to one tuple at a time (because we would be hashing
// the full columns at once), but the big disadvantage is that the full tuples
// are stored in the hash table (instead of an encoded representation).
// - using a single global map for a particular aggregate function that is
// shared among all aggregation groups - the benefit of such approach is that
// we only have a handful of map, but it turned out that such global map grows
// a lot bigger and has worse performance.
type distinctAggregatorHelperBase struct {
	*aggregatorHelperBase

	inputTypes       []*types.T
	aggColsConverter *colconv.VecToDatumConverter
	arena            stringarena.Arena
	datumAlloc       *rowenc.DatumAlloc
	scratch          struct {
		ed      rowenc.EncDatum
		encoded []byte
		// converted is a scratch space for converting a single element.
		converted []tree.Datum
		sel       []int
	}
}

func newDistinctAggregatorHelperBase(
	args *colexecagg.NewAggregatorArgs, datumAlloc *rowenc.DatumAlloc, maxBatchSize int,
) *distinctAggregatorHelperBase {
	b := &distinctAggregatorHelperBase{
		aggregatorHelperBase: newAggregatorHelperBase(args.Spec, maxBatchSize),
		inputTypes:           args.InputTypes,
		arena:                stringarena.Make(args.MemAccount),
		datumAlloc:           datumAlloc,
	}
	var vecIdxsToConvert []int
	for _, aggFn := range args.Spec.Aggregations {
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
	b.aggColsConverter = colconv.NewVecToDatumConverter(len(args.InputTypes), vecIdxsToConvert, false /* willRelease */)
	b.scratch.converted = []tree.Datum{nil}
	b.scratch.sel = make([]int, maxBatchSize)
	return b
}

func (b *distinctAggregatorHelperBase) makeSeenMaps() []map[string]struct{} {
	// Note that we consciously don't account for the memory used under seen
	// maps because that memory is likely noticeably smaller than the memory
	// used (and accounted for) in other parts of the hash aggregation (the
	// vectorized hash table and the aggregate functions).
	seen := make([]map[string]struct{}, len(b.spec.Aggregations))
	for i, aggFn := range b.spec.Aggregations {
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
func (b *distinctAggregatorHelperBase) selectDistinctTuples(
	ctx context.Context,
	inputLen int,
	sel []int,
	inputIdxs []uint32,
	seen map[string]struct{},
	groups []bool,
) (newLen int, newSel []int) {
	newSel = b.scratch.sel
	var (
		tupleIdx int
		err      error
		s        string
	)
	for idx := 0; idx < inputLen; idx++ {
		b.scratch.encoded = b.scratch.encoded[:0]
		tupleIdx = idx
		if sel != nil {
			tupleIdx = sel[idx]
		}
		if groups != nil && groups[tupleIdx] {
			// We have encountered a new group, so we need to clear the seen
			// map. It turns out that it is faster to delete entries from the
			// old map rather than allocating a new one.
			for s := range seen {
				delete(seen, s)
			}
		}
		for _, colIdx := range inputIdxs {
			// Note that we don't need to explicitly unset ed because encoded
			// field is never set during fingerprinting - we'll compute the
			// encoding and return it without updating the EncDatum; therefore,
			// simply setting Datum field to the argument is sufficient.
			b.scratch.ed.Datum = b.aggColsConverter.GetDatumColumn(int(colIdx))[tupleIdx]
			// We know that we have tree.Datum, so there will definitely be no
			// need to decode b.scratch.ed for fingerprinting, so we pass in
			// nil memory account.
			b.scratch.encoded, err = b.scratch.ed.Fingerprint(
				ctx, b.inputTypes[colIdx], b.datumAlloc, b.scratch.encoded, nil, /* acc */
			)
			if err != nil {
				colexecerror.InternalError(err)
			}
		}
		if _, seenPreviously := seen[string(b.scratch.encoded)]; !seenPreviously {
			s, err = b.arena.AllocBytes(ctx, b.scratch.encoded)
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

// filteringDistinctHashAggregatorHelper is an aggregatorHelper that handles
// the aggregate functions with any number of DISTINCT and/or FILTER clauses
// for the hash aggregation. The helper should be shared among all groups for
// aggregation. The filtering is delegated to filteringSingleFunctionHashHelper
// whereas the "distinctness" is delegated to distinctAggregatorHelperBase.
type filteringDistinctHashAggregatorHelper struct {
	*distinctAggregatorHelperBase

	filters []*filteringSingleFunctionHashHelper
}

var _ aggregatorHelper = &filteringDistinctHashAggregatorHelper{}

func newFilteringDistinctHashAggregatorHelper(
	args *colexecagg.NewAggregatorArgs,
	filters []*filteringSingleFunctionHashHelper,
	datumAlloc *rowenc.DatumAlloc,
	maxBatchSize int,
) aggregatorHelper {
	return &filteringDistinctHashAggregatorHelper{
		distinctAggregatorHelperBase: newDistinctAggregatorHelperBase(
			args,
			datumAlloc,
			maxBatchSize,
		),
		filters: filters,
	}
}

// performAggregation executes Compute on all aggregate functions in bucket
// paying attention to distinct tuples if the corresponding function performs
// DISTINCT aggregation (as well as to any present FILTER clauses).
// For such functions the approach is as follows:
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
	ctx context.Context, vecs []coldata.Vec, inputLen int, sel []int, bucket *aggBucket, _ []bool,
) {
	h.saveState(vecs, inputLen, sel)
	h.aggColsConverter.ConvertVecs(vecs, inputLen, sel)
	for aggFnIdx, aggFn := range h.spec.Aggregations {
		var maybeModified bool
		vecs, inputLen, sel, maybeModified = h.filters[aggFnIdx].applyFilter(ctx, vecs, inputLen, sel)
		if inputLen > 0 && aggFn.Distinct {
			inputLen, sel = h.selectDistinctTuples(
				ctx, inputLen, sel, aggFn.ColIdx, bucket.seen[aggFnIdx], nil, /* groups */
			)
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

type distinctOrderedAggregatorHelper struct {
	*distinctAggregatorHelperBase
}

var _ aggregatorHelper = &distinctOrderedAggregatorHelper{}

func newDistinctOrderedAggregatorHelper(
	args *colexecagg.NewAggregatorArgs, datumAlloc *rowenc.DatumAlloc, maxBatchSize int,
) aggregatorHelper {
	return &distinctOrderedAggregatorHelper{
		distinctAggregatorHelperBase: newDistinctAggregatorHelperBase(
			args,
			datumAlloc,
			maxBatchSize,
		),
	}
}

// performAggregation executes Compute on all aggregate function in bucket
// paying attention to distinct tuples if the corresponding function performs
// DISTINCT aggregation
func (h *distinctOrderedAggregatorHelper) performAggregation(
	ctx context.Context,
	vecs []coldata.Vec,
	inputLen int,
	sel []int,
	bucket *aggBucket,
	groups []bool,
) {
	h.saveState(vecs, inputLen, sel)
	h.aggColsConverter.ConvertVecs(vecs, inputLen, sel)
	for aggFnIdx, aggFn := range h.spec.Aggregations {
		var maybeModified bool
		if aggFn.Distinct {
			inputLen, sel = h.selectDistinctTuples(
				ctx, inputLen, sel, aggFn.ColIdx, bucket.seen[aggFnIdx], groups,
			)
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

// singleBatchOperator is a helper colexecop.Operator that returns the
// provided vectors as a batch on the first call to Next() and zero batch on
// all consequent calls (until it is reset). It must be reset before it can be
// used for the first time.
type singleBatchOperator struct {
	colexecop.ZeroInputNode
	colexecop.NonExplainable

	nexted bool
	batch  coldata.Batch
}

var _ colexecop.Operator = &singleBatchOperator{}

func newSingleBatchOperator(
	allocator *colmem.Allocator, typs []*types.T, maxBatchSize int,
) *singleBatchOperator {
	return &singleBatchOperator{
		batch: allocator.NewMemBatchNoCols(typs, maxBatchSize),
	}
}

func (o *singleBatchOperator) Init(context.Context) {}

func (o *singleBatchOperator) Next() coldata.Batch {
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

// aggBucket stores the aggregation functions for the corresponding aggregation
// group as well as other utility information.
type aggBucket struct {
	fns []colexecagg.AggregateFunc
	// seen is a slice of maps used to handle distinct aggregation. A
	// corresponding entry in the slice is nil if the function doesn't have a
	// DISTINCT clause. The slice itself will be nil whenever no aggregate
	// function has a DISTINCT clause.
	seen []map[string]struct{}
}

func (b *aggBucket) init(
	fns []colexecagg.AggregateFunc, seen []map[string]struct{}, groups []bool,
) {
	b.fns = fns
	for _, fn := range b.fns {
		fn.Init(groups)
	}
	b.seen = seen
}

func (b *aggBucket) reset() {
	for _, fn := range b.fns {
		fn.Reset()
	}
	for _, seen := range b.seen {
		for k := range seen {
			delete(seen, k)
		}
	}
}

const sizeOfAggBucket = int64(unsafe.Sizeof(aggBucket{}))
const aggBucketSliceOverhead = int64(unsafe.Sizeof([]aggBucket{}))

// aggBucketAlloc is a utility struct that batches allocations of aggBuckets.
type aggBucketAlloc struct {
	allocator *colmem.Allocator
	buf       []aggBucket
}

func (a *aggBucketAlloc) newAggBucket() *aggBucket {
	if len(a.buf) == 0 {
		a.allocator.AdjustMemoryUsage(aggBucketSliceOverhead + hashAggregatorAllocSize*sizeOfAggBucket)
		a.buf = make([]aggBucket, hashAggregatorAllocSize)
	}
	ret := &a.buf[0]
	a.buf = a.buf[1:]
	return ret
}
