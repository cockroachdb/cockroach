// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package colexecjoin

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexechash"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/memsize"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/errors"
)

// hashJoinerState represents the state of the hash join columnar operator.
type hashJoinerState int

const (
	// hjBuilding represents the state the hashJoiner is in when it is in the
	// build phase. Output columns from the build table are stored and a hash
	// map is constructed from its equality columns.
	hjBuilding = iota

	// hjProbing represents the state the hashJoiner is in when it is in the
	// probe phase. Probing is done in batches against the stored hash map.
	hjProbing

	// hjEmittingRight represents the state the hashJoiner is in when it is
	// emitting only either unmatched or matched rows from its build table
	// after having consumed the probe table. Unmatched rows are emitted for
	// right/full outer and right anti joins whereas matched rows are emitted
	// for right semi joins.
	hjEmittingRight

	// hjDone represents the state the hashJoiner is in when it has finished
	// emitting all output rows. Note that the build side will have been fully
	// consumed in this state, but the probe side *might* have not been fully
	// consumed.
	hjDone
)

// HashJoinerSpec is the specification for a hash join operator. The hash
// joiner performs a join on the left and right's equal columns and returns
// combined left and right output columns.
type HashJoinerSpec struct {
	JoinType descpb.JoinType
	// Left and Right are the specifications of the two input table sources to
	// the hash joiner.
	Left  hashJoinerSourceSpec
	Right hashJoinerSourceSpec

	// trackBuildMatches indicates whether or not we need to track if a row
	// from the build table had a match (this is needed with RIGHT/FULL OUTER,
	// RIGHT SEMI, and RIGHT ANTI joins).
	trackBuildMatches bool

	// rightDistinct indicates whether or not the build table equality column
	// tuples are distinct. If they are distinct, performance can be optimized.
	rightDistinct bool
}

type hashJoinerSourceSpec struct {
	// EqCols specify the indices of the source tables equality column during the
	// hash join.
	EqCols []uint32

	// SourceTypes specify the types of the input columns of the source table for
	// the hash joiner.
	SourceTypes []*types.T
}

// hashJoiner performs a hash join on the input tables equality columns.
// It requires that the output for every input batch in the probe phase fits
// within coldata.BatchSize(), otherwise the behavior is undefined. A join is
// performed and there is no guarantee on the ordering of the output columns.
// The hash table will be built on the right side source, and the left side
// source will be used for probing.
//
// Before the build phase, all equality and output columns from the build table
// are collected and stored.
//
// In the vectorized implementation of the build phase, the following tasks are
// performed:
//  1. The bucket number (hash value) of each key tuple is computed and stored
//     into a buckets array.
//  2. The values in the buckets array is normalized to fit within the hash table
//     numBuckets.
//  3. The bucket-chaining hash table organization is prepared with the computed
//     buckets.
//
// Depending on the value of the spec.rightDistinct flag, there are two
// variations of the probe phase. The planner will set rightDistinct to true if
// and only if the right equality columns make a distinct key.
//
// In the columnarized implementation of the distinct build table probe phase,
// the following tasks are performed by the fastProbe function:
//
//  1. Compute the bucket number for each probe row's key tuple and store the
//     results into the buckets array.
//  2. In order to find the position of these key tuples in the hash table:
//     - First find the first element in the bucket's linked list for each key tuple
//     and store it in the ToCheckID array. Initialize the ToCheck array with the
//     full sequence of input indices (0...batchSize - 1).
//     - While ToCheck is not empty, each element in ToCheck represents a position
//     of the key tuples for which the key has not yet been found in the hash
//     table. Perform a multi-column equality check to see if the key columns
//     match that of the build table's key columns at ToCheckID.
//     - Update the differs array to store whether or not the probe's key tuple
//     matched the corresponding build's key tuple.
//     - Select the indices that differed and store them into ToCheck since they
//     need to be further processed.
//     - For the differing tuples, find the next ID in that bucket of the hash table
//     and put it into the ToCheckID array.
//  3. Now, ToCheckID for every probe's key tuple contains the index of the
//     matching build's key tuple in the hash table. Use it to project output
//     columns from the has table to build the resulting batch.
//
// In the columnarized implementation of the non-distinct build table probe
// phase, the following tasks are performed by the probe function:
//
//  1. Compute the bucket number for each probe row's key tuple and store the
//     results into the buckets array.
//  2. In order to find the position of these key tuples in the hash table:
//     - First find the first element in the bucket's linked list for each key tuple
//     and store it in the ToCheckID array. Initialize the ToCheck array with the
//     full sequence of input indices (0...batchSize - 1).
//     - While ToCheck is not empty, each element in ToCheck represents a position
//     of the key tuples for which the key has not yet been visited by any prior
//     probe. Perform a multi-column equality check to see if the key columns
//     match that of the build table's key columns at ToCheckID.
//     - Update the differs array to store whether or not the probe's key tuple
//     matched the corresponding build's key tuple.
//     - For the indices that did not differ, we can lazily update the HashTable's
//     Same linked list to store a list of all identical keys starting at head.
//     Once a key has been added to ht.Same, ht.Visited is set to true. For the
//     indices that have never been visited, we want to continue checking this
//     bucket for identical values by adding this key to ToCheck.
//     - Select the indices that differed and store them into ToCheck since they
//     need to be further processed.
//     - For the differing tuples, find the next ID in that bucket of the hash table
//     and put it into the ToCheckID array.
//  3. Now, head stores the keyID of the first match in the build table for every
//     probe table key. ht.Same is used to select all build key matches for each
//     probe key, which are added to the resulting batch. Output batching is done
//     to ensure that each batch is at most coldata.BatchSize().
//
// In the case that an outer join on the probe table side is performed, every
// single probe row is kept even if its ToCheckID is 0. If a ToCheckID of 0 is
// found, this means that the matching build table row should be all NULL. This
// is done by setting probeRowUnmatched at that row to true.
//
// In the case that an outer join on the build table side is performed, an
// emitRight is performed after the probing ends. This is done by gathering
// all build table rows that have never been matched and stitching it together
// with NULL values on the probe side.
type hashJoiner struct {
	colexecop.TwoInputInitHelper

	// hashTableAllocator should be used when building the hash table from the
	// right input.
	hashTableAllocator *colmem.Allocator
	// outputUnlimitedAllocator should *only* be used when populating the
	// output when we have already fully built the hash table from the right
	// input and are only populating output one batch at a time. If we were to
	// use a limited allocator, we could hit the memory limit during output
	// population, and it would have been very hard to fall back to disk backed
	// hash joiner because we might have already emitted partial output.
	outputUnlimitedAllocator *colmem.Allocator
	// spec holds the specification for the current hash join process.
	spec HashJoinerSpec
	// state stores the current state of the hash joiner.
	state                      hashJoinerState
	hashTableInitialNumBuckets uint32
	// ht holds the HashTable that is populated during the build phase and used
	// during the probe phase.
	ht *colexechash.HashTable
	// output stores the resulting output batch that is constructed and returned
	// for every input batch during the probe phase.
	output      coldata.Batch
	outputTypes []*types.T

	// probeState is used in hjProbing state.
	probeState struct {
		// buildIdx and probeIdx represents the matching row indices that are
		// used to stitch together the join results.
		buildIdx []int
		probeIdx []int

		// probeRowUnmatched is used in the case of left/full outer joins. We
		// use probeRowUnmatched to represent that the resulting columns should
		// be NULL on the build table. This indicates that the probe table row
		// did not match any build table rows.
		probeRowUnmatched []bool
		// buildRowMatched is used in the case that spec.trackBuildMatches is
		// true. This means that an outer join is performed on the build side
		// and buildRowMatched marks all the build table rows that have been
		// matched already. The rows that were unmatched are emitted during the
		// hjEmittingRight phase.
		//
		// Note that this is the only slice in probeState of non-constant size
		// (i.e. not limited by coldata.BatchSize() in capacity), so it's the
		// only one we perform the memory accounting for.
		buildRowMatched []bool

		// buckets is used to store the computed hash value of each key in a
		// single probe batch.
		buckets []uint32
		// prevBatch, if not nil, indicates that the previous probe input batch
		// has not been fully processed.
		prevBatch coldata.Batch
		// prevBatchResumeIdx indicates the index of the probe row to resume the
		// collection from. It is used only in case of non-distinct build source
		// (every probe row can have multiple matching build rows).
		prevBatchResumeIdx int
	}

	// emittingRightState is used in hjEmittingRight state.
	emittingRightState struct {
		rowIdx int
	}

	// accountedFor tracks how much memory the hash joiner has accounted for so
	// far related to some of the internal slices in the hash table.
	accountedFor struct {
		// hashtableSame tracks the current memory usage of hj.ht.Same.
		hashtableSame int64
		// hashtableVisited tracks the current memory usage of hj.ht.Visited.
		hashtableVisited int64
		// buildRowMatched tracks the current memory usage of
		// hj.probeState.buildRowMatched.
		buildRowMatched int64
	}

	exportBufferedState struct {
		rightExported      int
		rightWindowedBatch coldata.Batch
	}
}

var _ colexecop.BufferingInMemoryOperator = &hashJoiner{}
var _ colexecop.Resetter = &hashJoiner{}

// HashJoinerInitialNumBuckets is the number of the hash buckets initially
// allocated by the hash table that is used by the in-memory hash joiner.
// This number was chosen after running the micro-benchmarks and relevant
// TPCH queries using tpchvec/bench.
const HashJoinerInitialNumBuckets = 256

func (hj *hashJoiner) Init(ctx context.Context) {
	if !hj.TwoInputInitHelper.Init(ctx) {
		return
	}

	allowNullEquality, probeMode := false, colexechash.HashTableDefaultProbeMode
	if hj.spec.JoinType.IsSetOpJoin() {
		allowNullEquality = true
		probeMode = colexechash.HashTableDeletingProbeMode
	}
	// This number was chosen after running the micro-benchmarks and relevant
	// TPCH queries using tpchvec/bench.
	const hashTableLoadFactor = 1.0
	hj.ht = colexechash.NewHashTable(
		ctx,
		hj.hashTableAllocator,
		coldata.BatchSize(),
		hashTableLoadFactor,
		hj.hashTableInitialNumBuckets,
		hj.spec.Right.SourceTypes,
		hj.spec.Right.EqCols,
		allowNullEquality,
		colexechash.HashTableFullBuildMode,
		probeMode,
	)

	hj.exportBufferedState.rightWindowedBatch = hj.outputUnlimitedAllocator.NewMemBatchWithFixedCapacity(
		hj.spec.Right.SourceTypes, 0, /* size */
	)
	hj.state = hjBuilding
}

func (hj *hashJoiner) Next() coldata.Batch {
	for {
		switch hj.state {
		case hjBuilding:
			hj.build()
			if hj.ht.Vals.Length() == 0 {
				// The build side is empty, so we might be able to
				// short-circuit probing phase altogether.
				if hj.spec.JoinType.IsEmptyOutputWhenRightIsEmpty() {
					hj.state = hjDone
				}
			}
			continue
		case hjProbing:
			output := hj.exec()
			if output.Length() == 0 {
				if hj.spec.trackBuildMatches {
					hj.state = hjEmittingRight
				} else {
					hj.state = hjDone
				}
				continue
			}
			return output
		case hjEmittingRight:
			if hj.emittingRightState.rowIdx == hj.ht.Vals.Length() {
				hj.state = hjDone
				continue
			}
			hj.emitRight(hj.spec.JoinType == descpb.RightSemiJoin /* matched */)
			return hj.output
		case hjDone:
			return coldata.ZeroBatch
		default:
			colexecerror.InternalError(errors.AssertionFailedf("hash joiner in unhandled state"))
			// This code is unreachable, but the compiler cannot infer that.
			return nil
		}
	}
}

func (hj *hashJoiner) build() {
	hj.ht.FullBuild(hj.InputTwo)

	// At this point, we have fully built the hash table on the right side
	// (meaning we have fully consumed the right input), so it'd be a shame to
	// fall back to disk, thus, we use the unlimited allocator.
	allocator := hj.outputUnlimitedAllocator

	// If we might have duplicates in the hash table (meaning that rightDistinct
	// is false), we need to set up Same and Visited slices for the prober
	// (depending on the join type).
	var needSame bool
	// Visited slice is always used for set-operation joins, regardless of
	// the fact whether the right side is distinct.
	needVisited := hj.spec.JoinType.IsSetOpJoin()

	if !hj.spec.rightDistinct {
		switch hj.spec.JoinType {
		case descpb.LeftAntiJoin, descpb.ExceptAllJoin, descpb.IntersectAllJoin:
		default:
			// We don't need Same with LEFT ANTI, EXCEPT ALL, and INTERSECT ALL
			// joins because they have a separate collectSingleMatch method.
			needSame = true
			// Visited isn't needed for LEFT ANTI joins (it's used by EXCEPT ALL
			// and INTERSECT ALL, but those cases are handled above already).
			needVisited = true
		}
	}
	if needSame {
		hj.ht.Same = colexecutils.MaybeAllocateUint32Array(hj.ht.Same, hj.ht.Vals.Length()+1)
		newAccountedFor := memsize.Uint32 * int64(cap(hj.ht.Same))
		// hj.ht.Same will never shrink, so the delta is non-negative.
		allocator.AdjustMemoryUsageAfterAllocation(newAccountedFor - hj.accountedFor.hashtableSame)
		hj.accountedFor.hashtableSame = newAccountedFor
	}

	if needVisited {
		hj.ht.Visited = colexecutils.MaybeAllocateBoolArray(hj.ht.Visited, hj.ht.Vals.Length()+1)
		newAccountedFor := memsize.Bool * int64(cap(hj.ht.Visited))
		// hj.ht.Visited will never shrink, so the delta is non-negative.
		allocator.AdjustMemoryUsageAfterAllocation(newAccountedFor - hj.accountedFor.hashtableVisited)
		hj.accountedFor.hashtableVisited = newAccountedFor
		// Since keyID = 0 is reserved for end of list, it can be marked as
		// visited at the beginning.
		hj.ht.Visited[0] = true
	}

	if hj.spec.trackBuildMatches {
		hj.probeState.buildRowMatched = colexecutils.MaybeAllocateBoolArray(hj.probeState.buildRowMatched, hj.ht.Vals.Length())
		newAccountedFor := memsize.Bool * int64(cap(hj.probeState.buildRowMatched))
		// hj.probeState.buildRowMatched will never shrink, so the delta is
		// non-negative.
		allocator.AdjustMemoryUsageAfterAllocation(newAccountedFor - hj.accountedFor.buildRowMatched)
		hj.accountedFor.buildRowMatched = newAccountedFor
	}

	hj.state = hjProbing
}

// emitRight populates the output batch to emit tuples from the right side that
// didn't get a match when matched==false (right/full outer and right anti
// joins) or did get a match when matched==true (right semi joins).
func (hj *hashJoiner) emitRight(matched bool) {
	// Make sure that hj.probeState.buildIdx is of sufficient size (it is used
	// as a selection vector to select only the necessary tuples).
	buildIdxSize := hj.ht.Vals.Length() - hj.emittingRightState.rowIdx
	if buildIdxSize > coldata.BatchSize() {
		buildIdxSize = coldata.BatchSize()
	}
	if cap(hj.probeState.buildIdx) < buildIdxSize {
		hj.probeState.buildIdx = make([]int, buildIdxSize)
	} else {
		hj.probeState.buildIdx = hj.probeState.buildIdx[:buildIdxSize]
	}

	// Find the next batch of tuples that have the requested 'matched' value.
	nResults := 0
	for nResults < coldata.BatchSize() && hj.emittingRightState.rowIdx < hj.ht.Vals.Length() {
		if hj.probeState.buildRowMatched[hj.emittingRightState.rowIdx] == matched {
			hj.probeState.buildIdx[nResults] = hj.emittingRightState.rowIdx
			nResults++
		}
		hj.emittingRightState.rowIdx++
	}
	hj.resetOutput(nResults)

	// We have already fully built the hash table from the right input and now
	// are only populating output one batch at a time. If we were to use a
	// limited allocator, we could hit the limit here, and it would have been
	// very hard to fall back to disk backed hash joiner because we might have
	// already emitted partial output.
	hj.outputUnlimitedAllocator.PerformOperation(hj.output.ColVecs(), func() {
		var rightOutColOffset int
		if hj.spec.JoinType.ShouldIncludeLeftColsInOutput() {
			// Set all elements in the probe columns of the output batch to null.
			for i := range hj.spec.Left.SourceTypes {
				outCol := hj.output.ColVec(i)
				outCol.Nulls().SetNullRange(0 /* startIdx */, nResults)
			}
			rightOutColOffset = len(hj.spec.Left.SourceTypes)
		}

		outCols := hj.output.ColVecs()[rightOutColOffset : rightOutColOffset+len(hj.spec.Right.SourceTypes)]
		for i := range hj.spec.Right.SourceTypes {
			outCol := outCols[i]
			valCol := hj.ht.Vals.ColVec(i)
			outCol.Copy(
				coldata.SliceArgs{
					Src:       valCol,
					SrcEndIdx: nResults,
					Sel:       hj.probeState.buildIdx,
				},
			)
		}

		hj.output.SetLength(nResults)
	})
}

// prepareForCollecting sets up the hash joiner for collecting by making sure
// that various slices in hj.probeState are of sufficient length depending on
// the join type. Note that batchSize might cap the number of tuples collected
// in a single output batch (this is the case with non-distinct collectProbe*
// methods).
func (hj *hashJoiner) prepareForCollecting(batchSize int) {
	if !hj.spec.JoinType.ShouldIncludeLeftColsInOutput() {
		// Right semi/anti joins have a separate collecting method that simply
		// records the fact whether build rows had a match and don't need these
		// probing slices.
		return
	}
	// Note that we don't need to zero out the slices if they have enough
	// capacity because the correct values will always be set in the collecting
	// methods.
	if cap(hj.probeState.probeIdx) < batchSize {
		hj.probeState.probeIdx = make([]int, batchSize)
	} else {
		hj.probeState.probeIdx = hj.probeState.probeIdx[:batchSize]
	}
	if hj.spec.JoinType.IsLeftAntiOrExceptAll() || hj.spec.JoinType == descpb.IntersectAllJoin {
		// Left anti, except all, and intersect all joins have special
		// collectSingleMatch method that only uses the probeIdx slice.
		return
	}
	if hj.spec.JoinType.IsLeftOuterOrFullOuter() {
		hj.probeState.probeRowUnmatched = colexecutils.MaybeAllocateLimitedBoolArray(
			hj.probeState.probeRowUnmatched, batchSize,
		)
	}
	if cap(hj.probeState.buildIdx) < batchSize {
		hj.probeState.buildIdx = make([]int, batchSize)
	} else {
		hj.probeState.buildIdx = hj.probeState.buildIdx[:batchSize]
	}
}

// exec is a general prober that works with non-distinct build table equality
// columns. It returns a Batch with N + M columns where N is the number of
// left source columns and M is the number of right source columns. The first N
// columns correspond to the respective left source columns, followed by the
// right source columns as the last M elements.
func (hj *hashJoiner) exec() coldata.Batch {
	if batch := hj.probeState.prevBatch; batch != nil {
		// We didn't finish probing the last read batch on the previous call to
		// exec, so we continue where we left off.
		hj.probeState.prevBatch = nil
		batchSize := batch.Length()
		sel := batch.Selection()

		// Since we're probing the same batch for the second time, it is likely
		// that every probe tuple has multiple matches, so we want to maximize
		// the number of tuples we collect in a single output batch, and,
		// therefore, we use coldata.BatchSize() here.
		hj.prepareForCollecting(coldata.BatchSize())
		nResults := hj.collect(batch, batchSize, sel)
		if nResults > 0 {
			hj.congregate(nResults, batch)
			return hj.output
		}
		// There were no matches in that batch, so we move on to the next one.
	}
	for {
		batch := hj.InputOne.Next()
		batchSize := batch.Length()

		if batchSize == 0 {
			return coldata.ZeroBatch
		}

		for i, colIdx := range hj.spec.Left.EqCols {
			hj.ht.Keys[i] = batch.ColVec(int(colIdx))
		}

		sel := batch.Selection()

		// First, we compute the hash values for all tuples in the batch.
		if cap(hj.probeState.buckets) < batchSize {
			hj.probeState.buckets = make([]uint32, batchSize)
		} else {
			// Note that we don't need to clear old values from buckets
			// because the correct values will be populated in
			// ComputeBuckets.
			hj.probeState.buckets = hj.probeState.buckets[:batchSize]
		}
		hj.ht.ComputeBuckets(hj.probeState.buckets, hj.ht.Keys, batchSize, sel)

		// Then, we initialize ToCheckID with the initial hash buckets and
		// ToCheck with all applicable indices. Notably, only probing tuples
		// that have hash matches are included into ToCheck whereas ToCheckID is
		// correctly set for all tuples.
		hj.ht.ProbeScratch.SetupLimitedSlices(batchSize)
		// Early bounds checks.
		toCheckIDs := hj.ht.ProbeScratch.ToCheckID
		_ = toCheckIDs[batchSize-1]
		var nToCheck uint32
		for i, bucket := range hj.probeState.buckets[:batchSize] {
			f := hj.ht.BuildScratch.First[bucket]
			//gcassert:bce
			toCheckIDs[i] = f
			if f != 0 {
				// Non-zero "first" key indicates that there is a match of
				// hashes, and we need to include the current tuple to check
				// whether it is an actual match.
				hj.ht.ProbeScratch.ToCheck[nToCheck] = uint32(i)
				nToCheck++
			}
		}

		hj.prepareForCollecting(batchSize)
		checker, collector := hj.ht.Check, hj.collect
		if hj.spec.rightDistinct {
			checker, collector = hj.ht.DistinctCheck, hj.distinctCollect
		}

		// Now we find equality matches for all probing tuples.
		for nToCheck > 0 {
			// Continue searching for the build table matching keys while the
			// ToCheck array is non-empty.
			nToCheck = checker(nToCheck, sel)
			toCheckSlice := hj.ht.ProbeScratch.ToCheck[:nToCheck]
			nToCheck = 0
			for _, toCheck := range toCheckSlice {
				nextID := hj.ht.BuildScratch.Next[hj.ht.ProbeScratch.ToCheckID[toCheck]]
				toCheckIDs[toCheck] = nextID
				if nextID != 0 {
					hj.ht.ProbeScratch.ToCheck[nToCheck] = toCheck
					nToCheck++
				}
			}
		}

		// We're processing a new batch, so we'll reset the index to start
		// collecting from.
		hj.probeState.prevBatchResumeIdx = 0

		// Finally, we collect all matches that we can emit in the probing phase
		// in a single batch.
		nResults := collector(batch, batchSize, sel)
		if nResults > 0 {
			hj.congregate(nResults, batch)
			break
		}
	}
	return hj.output
}

// congregate uses the probeIdx and buildIdx pairs to stitch together the
// resulting join rows and add them to the output batch with the left table
// columns preceding the right table columns.
//
// This method should not be called for RIGHT SEMI and RIGHT ANTI joins because
// they populate the output after probing is done, when in the hjEmittingRight
// state.
func (hj *hashJoiner) congregate(nResults int, batch coldata.Batch) {
	if buildutil.CrdbTestBuild {
		if !hj.spec.JoinType.ShouldIncludeLeftColsInOutput() {
			panic(errors.AssertionFailedf(
				"unexpectedly hashJoiner.congregate is called for RIGHT SEMI or RIGHT ANTI join",
			))
		}
	}
	hj.resetOutput(nResults)
	// We have already fully built the hash table from the right input and now
	// are only populating output one batch at a time. If we were to use a
	// limited allocator, we could hit the limit here, and it would have been
	// very hard to fall back to disk backed hash joiner because we might have
	// already emitted partial output.
	hj.outputUnlimitedAllocator.PerformOperation(hj.output.ColVecs(), func() {
		// Populate the left output columns which are needed by all join types
		// other than RIGHT SEMI and RIGHT ANTI joins, and those two types don't
		// use this code path.
		outCols := hj.output.ColVecs()[:len(hj.spec.Left.SourceTypes)]
		for i := range hj.spec.Left.SourceTypes {
			outCol := outCols[i]
			valCol := batch.ColVec(i)
			outCol.Copy(
				coldata.SliceArgs{
					Src:       valCol,
					Sel:       hj.probeState.probeIdx,
					SrcEndIdx: nResults,
				},
			)
		}

		if hj.spec.JoinType.ShouldIncludeRightColsInOutput() {
			rightColOffset := len(hj.spec.Left.SourceTypes)
			// If the hash table is empty, then there is nothing to copy. The nulls
			// will be set below.
			if hj.ht.Vals.Length() > 0 {
				outCols = hj.output.ColVecs()[rightColOffset : rightColOffset+len(hj.spec.Right.SourceTypes)]
				for i := range hj.spec.Right.SourceTypes {
					outCol := outCols[i]
					valCol := hj.ht.Vals.ColVec(i)
					// Note that if for some index i, probeRowUnmatched[i] is true, then
					// hj.buildIdx[i] == 0 which will copy the garbage zeroth row of the
					// hash table, but we will set the NULL value below.
					outCol.Copy(
						coldata.SliceArgs{
							Src:       valCol,
							SrcEndIdx: nResults,
							Sel:       hj.probeState.buildIdx,
						},
					)
				}
			}
			if hj.spec.JoinType.IsLeftOuterOrFullOuter() {
				// Add in the nulls we needed to set for the outer join.
				for i := range hj.spec.Right.SourceTypes {
					outCol := hj.output.ColVec(i + rightColOffset)
					nulls := outCol.Nulls()
					for i, isNull := range hj.probeState.probeRowUnmatched[:nResults] {
						if isNull {
							nulls.SetNull(i)
						}
					}
				}
			}
		}

		if hj.spec.trackBuildMatches {
			// Early bounds checks.
			buildIdx := hj.probeState.buildIdx
			_ = buildIdx[nResults-1]
			if hj.spec.JoinType.IsLeftOuterOrFullOuter() {
				// Early bounds checks.
				probeRowUnmatched := hj.probeState.probeRowUnmatched
				_ = probeRowUnmatched[nResults-1]
				for i := 0; i < nResults; i++ {
					//gcassert:bce
					if !probeRowUnmatched[i] {
						//gcassert:bce
						bIdx := buildIdx[i]
						hj.probeState.buildRowMatched[bIdx] = true
					}
				}
			} else {
				for i := 0; i < nResults; i++ {
					//gcassert:bce
					bIdx := buildIdx[i]
					hj.probeState.buildRowMatched[bIdx] = true
				}
			}
		}

		hj.output.SetLength(nResults)
	})
}

func (hj *hashJoiner) ExportBuffered(input colexecop.Operator) coldata.Batch {
	if hj.InputOne == input {
		// We do not buffer anything from the left source. Furthermore, the memory
		// limit can only hit during the building of the hash table step at which
		// point we haven't requested a single batch from the left.
		return coldata.ZeroBatch
	} else if hj.InputTwo == input {
		if hj.ht == nil || hj.exportBufferedState.rightExported == hj.ht.Vals.Length() {
			// The right input has been fully exported.
			return coldata.ZeroBatch
		}
		newRightExported := hj.exportBufferedState.rightExported + coldata.BatchSize()
		if newRightExported > hj.ht.Vals.Length() {
			newRightExported = hj.ht.Vals.Length()
		}
		startIdx, endIdx := hj.exportBufferedState.rightExported, newRightExported
		b := hj.exportBufferedState.rightWindowedBatch
		// We don't need to worry about selection vectors on hj.ht.Vals because the
		// tuples have been already selected during building of the hash table.
		for i := range hj.spec.Right.SourceTypes {
			window := hj.ht.Vals.ColVec(i).Window(startIdx, endIdx)
			b.ReplaceCol(window, i)
		}
		b.SetLength(endIdx - startIdx)
		hj.exportBufferedState.rightExported = newRightExported
		return b
	} else {
		colexecerror.InternalError(errors.New(
			"unexpectedly ExportBuffered is called with neither left nor right inputs to hash join",
		))
		// This code is unreachable, but the compiler cannot infer that.
		return nil
	}
}

// ReleaseBeforeExport implements the colexecop.BufferingInMemoryOperator
// interface.
func (hj *hashJoiner) ReleaseBeforeExport() {}

// ReleaseAfterExport implements the colexecop.BufferingInMemoryOperator
// interface.
func (hj *hashJoiner) ReleaseAfterExport(input colexecop.Operator) {
	if hj.InputOne == input {
		// We don't have anything to release for the left input.
		return
	}
	if hj.ht == nil {
		// Resources have already been released.
		return
	}
	// We can only spill to disk while building the hash table (i.e. before the
	// probing phase), so we only need to release the hash table and the
	// windowed batch we used for the export (since we haven't allocated any
	// other resources).
	hj.ht.Release()
	hj.ht = nil
	hj.exportBufferedState.rightWindowedBatch = nil
}

func (hj *hashJoiner) resetOutput(nResults int) {
	// We're resetting the output meaning that we have already fully built the
	// hash table from the right input and now are only populating output one
	// batch at a time. If we were to use a limited allocator, we could hit the
	// limit here, and it would have been very hard to fall back to disk backed
	// hash joiner because we might have already emitted partial output.
	//
	// We are also consciously not limiting the size of the output batch based
	// on the memory footprint based on the following reasoning:
	// 1. the main code-path (congregate()) has already modified the internal
	// state under the assumption that nResults rows will be emitted with the
	// next batch
	// 2. nResults is usually limited by size of the batch coming from the left
	// input, so we assume that it is of reasonable memory footprint
	// 3. in emitRight() method, the output batch will never use more memory
	// than the hash table itself, so if we haven't spilled yet, then the output
	// batch will be of reasonable size too
	// 4. when the hashJoiner is used by the external hash joiner as the main
	// strategy, the hash-based partitioner is responsible for making sure that
	// partitions fit within memory limit.
	hj.output, _ = hj.outputUnlimitedAllocator.ResetMaybeReallocateNoMemLimit(
		hj.outputTypes, hj.output, nResults,
	)
}

func (hj *hashJoiner) Reset(ctx context.Context) {
	hj.TwoInputInitHelper.Reset(ctx)
	hj.state = hjBuilding
	// Note that hj.ht.Reset() doesn't reset hj.ht.Same and hj.ht.Visited
	// slices, but we'll reset them manually in hj.build(). We also keep
	// references to those slices, so we don't release any of the memory we've
	// accounted for.
	hj.ht.Reset(ctx)
	// Note that we don't zero out hj.probeState.buildIdx,
	// hj.probeState.probeIdx, and hj.probeState.probeRowUnmatched because the
	// values in these slices are always set in collecting methods.
	// hj.probeState.buildRowMatched is reset after building the hash table is
	// complete in build() method.
	hj.emittingRightState.rowIdx = 0
	if buildutil.CrdbTestBuild {
		if hj.ht == nil {
			colexecerror.InternalError(errors.AssertionFailedf(
				"the hash joiner is being reset after having spilled to disk",
			))
		}
	}
	hj.exportBufferedState.rightExported = 0
}

// MakeHashJoinerSpec creates a specification for columnar hash join operator.
// leftEqCols and rightEqCols specify the equality columns while leftOutCols
// and rightOutCols specifies the output columns. leftTypes and rightTypes
// specify the input column types of the two sources. rightDistinct indicates
// whether the equality columns of the right source form a key.
func MakeHashJoinerSpec(
	joinType descpb.JoinType,
	leftEqCols []uint32,
	rightEqCols []uint32,
	leftTypes []*types.T,
	rightTypes []*types.T,
	rightDistinct bool,
) HashJoinerSpec {
	switch joinType {
	case descpb.LeftSemiJoin:
		// In a left semi join, we don't need to store anything but a single row per
		// build row, since all we care about is whether a row on the left matches
		// any row on the right.
		// Note that this is *not* the case if we have an ON condition, since we'll
		// also need to make sure that a row on the left passes the ON condition
		// with the row on the right to emit it. However, we don't support ON
		// conditions just yet. When we do, we'll have a separate case for that.
		rightDistinct = true
	case descpb.LeftAntiJoin,
		descpb.RightAntiJoin,
		descpb.RightSemiJoin,
		descpb.IntersectAllJoin,
		descpb.ExceptAllJoin:
		// LEFT/RIGHT ANTI, RIGHT SEMI, INTERSECT ALL, and EXCEPT ALL joins
		// currently rely on the fact that ht.ProbeScratch.HeadID is populated
		// in order to perform the matching. However, HeadID is only populated
		// when the right side is considered to be non-distinct, so we override
		// that information here. Note that it forces these joins to be slower
		// than they could have been if they utilized the actual
		// distinctness information.
		// TODO(yuzefovich): refactor these joins to take advantage of the
		// actual distinctness information.
		rightDistinct = false
	}
	var trackBuildMatches bool
	switch joinType {
	case descpb.RightOuterJoin, descpb.FullOuterJoin,
		descpb.RightSemiJoin, descpb.RightAntiJoin:
		trackBuildMatches = true
	}

	left := hashJoinerSourceSpec{
		EqCols:      leftEqCols,
		SourceTypes: leftTypes,
	}
	right := hashJoinerSourceSpec{
		EqCols:      rightEqCols,
		SourceTypes: rightTypes,
	}
	return HashJoinerSpec{
		JoinType:          joinType,
		Left:              left,
		Right:             right,
		trackBuildMatches: trackBuildMatches,
		rightDistinct:     rightDistinct,
	}
}

// NewHashJoinerArgs encompasses all arguments to NewHashJoiner call.
type NewHashJoinerArgs struct {
	BuildSideAllocator       *colmem.Allocator
	OutputUnlimitedAllocator *colmem.Allocator
	Spec                     HashJoinerSpec
	LeftSource               colexecop.Operator
	RightSource              colexecop.Operator
	InitialNumBuckets        uint32
}

// NewHashJoiner creates a new equality hash join operator on the left and
// right input tables.
// hashTableAllocator should use a limited memory account and will be used for
// the build side whereas outputUnlimitedAllocator should use an unlimited
// memory account and will only be used when populating the output.
// memoryLimit will limit the size of the batches produced by the hash joiner.
func NewHashJoiner(args NewHashJoinerArgs) colexecop.ResettableOperator {
	return &hashJoiner{
		TwoInputInitHelper:         colexecop.MakeTwoInputInitHelper(args.LeftSource, args.RightSource),
		hashTableAllocator:         args.BuildSideAllocator,
		outputUnlimitedAllocator:   args.OutputUnlimitedAllocator,
		spec:                       args.Spec,
		outputTypes:                args.Spec.JoinType.MakeOutputTypes(args.Spec.Left.SourceTypes, args.Spec.Right.SourceTypes),
		hashTableInitialNumBuckets: args.InitialNumBuckets,
	}
}
