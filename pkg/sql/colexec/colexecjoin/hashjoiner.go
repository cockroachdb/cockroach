// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
	"github.com/cockroachdb/cockroach/pkg/sql/types"
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
// 1. The bucket number (hash value) of each key tuple is computed and stored
//    into a buckets array.
// 2. The values in the buckets array is normalized to fit within the hash table
//    numBuckets.
// 3. The bucket-chaining hash table organization is prepared with the computed
//    buckets.
//
// Depending on the value of the spec.rightDistinct flag, there are two
// variations of the probe phase. The planner will set rightDistinct to true if
// and only if the right equality columns make a distinct key.
//
// In the columnarized implementation of the distinct build table probe phase,
// the following tasks are performed by the fastProbe function:
//
// 1. Compute the bucket number for each probe row's key tuple and store the
//    results into the buckets array.
// 2. In order to find the position of these key tuples in the hash table:
// - First find the first element in the bucket's linked list for each key tuple
//   and store it in the GroupID array. Initialize the ToCheck array with the
//   full sequence of input indices (0...batchSize - 1).
// - While ToCheck is not empty, each element in ToCheck represents a position
//   of the key tuples for which the key has not yet been found in the hash
//   table. Perform a multi-column equality check to see if the key columns
//   match that of the build table's key columns at GroupID.
// - Update the differs array to store whether or not the probe's key tuple
//   matched the corresponding build's key tuple.
// - Select the indices that differed and store them into ToCheck since they
//   need to be further processed.
// - For the differing tuples, find the next ID in that bucket of the hash table
//   and put it into the GroupID array.
// 3. Now, GroupID for every probe's key tuple contains the index of the
//    matching build's key tuple in the hash table. Use it to project output
//    columns from the has table to build the resulting batch.
//
// In the columnarized implementation of the non-distinct build table probe
// phase, the following tasks are performed by the probe function:
//
// 1. Compute the bucket number for each probe row's key tuple and store the
//    results into the buckets array.
// 2. In order to find the position of these key tuples in the hash table:
// - First find the first element in the bucket's linked list for each key tuple
//   and store it in the GroupID array. Initialize the ToCheck array with the
//   full sequence of input indices (0...batchSize - 1).
// - While ToCheck is not empty, each element in ToCheck represents a position
//   of the key tuples for which the key has not yet been visited by any prior
//   probe. Perform a multi-column equality check to see if the key columns
//   match that of the build table's key columns at GroupID.
// - Update the differs array to store whether or not the probe's key tuple
//   matched the corresponding build's key tuple.
// - For the indices that did not differ, we can lazily update the HashTable's
//   same linked list to store a list of all identical keys starting at head.
//   Once a key has been added to ht.Same, ht.Visited is set to true. For the
//   indices that have never been visited, we want to continue checking this
//   bucket for identical values by adding this key to ToCheck.
// - Select the indices that differed and store them into ToCheck since they
//   need to be further processed.
// - For the differing tuples, find the next ID in that bucket of the hash table
//   and put it into the GroupID array.
// 3. Now, head stores the keyID of the first match in the build table for every
//    probe table key. ht.Same is used to select all build key matches for each
//    probe key, which are added to the resulting batch. Output batching is done
//    to ensure that each batch is at most coldata.BatchSize().
//
// In the case that an outer join on the probe table side is performed, every
// single probe row is kept even if its GroupID is 0. If a GroupID of 0 is
// found, this means that the matching build table row should be all NULL. This
// is done by setting probeRowUnmatched at that row to true.
//
// In the case that an outer join on the build table side is performed, an
// emitRight is performed after the probing ends. This is done by gathering
// all build table rows that have never been matched and stitching it together
// with NULL values on the probe side.
type hashJoiner struct {
	*joinHelper

	// buildSideAllocator should be used when building the hash table from the
	// right input.
	buildSideAllocator *colmem.Allocator
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
	hashTableInitialNumBuckets uint64
	// ht holds the HashTable that is populated during the build phase and used
	// during the probe phase.
	ht *colexechash.HashTable
	// memoryLimit is the total amount of RAM available for the hash joiner.
	// This limits the output batches (and is also the same limit for the size
	// of the hash table).
	memoryLimit int64
	// output stores the resulting output batch that is constructed and returned
	// for every input batch during the probe phase.
	output      coldata.Batch
	outputTypes []*types.T

	// probeState is used in hjProbing state.
	probeState struct {
		// buildIdx and probeIdx represents the matching row indices that are used to
		// stitch together the join results.
		buildIdx []int
		probeIdx []int

		// probeRowUnmatched is used in the case of left/full outer joins. We
		// use probeRowUnmatched to represent that the resulting columns should
		// be NULL on the build table. This indicates that the probe table row
		// did not match any build table rows.
		probeRowUnmatched []bool
		// buildRowMatched is used in the case that spec.trackBuildMatches is true. This
		// means that an outer join is performed on the build side and buildRowMatched
		// marks all the build table rows that have been matched already. The rows
		// that were unmatched are emitted during the hjEmittingRight phase.
		buildRowMatched []bool

		// buckets is used to store the computed hash value of each key in a single
		// probe batch.
		buckets []uint64
		// prevBatch, if not nil, indicates that the previous probe input batch has
		// not been fully processed.
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
	if !hj.init(ctx) {
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
		hj.buildSideAllocator,
		hashTableLoadFactor,
		hj.hashTableInitialNumBuckets,
		hj.spec.Right.SourceTypes,
		hj.spec.Right.EqCols,
		allowNullEquality,
		colexechash.HashTableFullBuildMode,
		probeMode,
	)

	hj.exportBufferedState.rightWindowedBatch = hj.buildSideAllocator.NewMemBatchWithFixedCapacity(
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
	hj.ht.FullBuild(hj.inputTwo)

	// We might have duplicates in the hash table, so we need to set up
	// same and visited slices for the prober.
	if !hj.spec.rightDistinct && !hj.spec.JoinType.IsLeftAntiOrExceptAll() {
		// We don't need same with LEFT ANTI and EXCEPT ALL joins because
		// they have separate collectLeftAnti method.
		hj.ht.Same = colexecutils.MaybeAllocateUint64Array(hj.ht.Same, hj.ht.Vals.Length()+1)
	}
	if !hj.spec.rightDistinct || hj.spec.JoinType.IsSetOpJoin() {
		// visited slice is also used for set-operation joins, regardless of
		// the fact whether the right side is distinct.
		hj.ht.Visited = colexecutils.MaybeAllocateBoolArray(hj.ht.Visited, hj.ht.Vals.Length()+1)
		// Since keyID = 0 is reserved for end of list, it can be marked as visited
		// at the beginning.
		hj.ht.Visited[0] = true
	}

	if hj.spec.trackBuildMatches {
		if cap(hj.probeState.buildRowMatched) < hj.ht.Vals.Length() {
			hj.probeState.buildRowMatched = make([]bool, hj.ht.Vals.Length())
		} else {
			hj.probeState.buildRowMatched = hj.probeState.buildRowMatched[:hj.ht.Vals.Length()]
			for n := 0; n < hj.ht.Vals.Length(); n += copy(hj.probeState.buildRowMatched[n:], colexecutils.ZeroBoolColumn) {
			}
		}
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
				coldata.CopySliceArgs{
					SliceArgs: coldata.SliceArgs{
						Src:       valCol,
						SrcEndIdx: nResults,
						Sel:       hj.probeState.buildIdx,
					},
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
	if hj.spec.JoinType.IsRightSemiOrRightAnti() {
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
	if hj.spec.JoinType.IsLeftAntiOrExceptAll() {
		// Left anti and except all joins have special collectLeftAnti method
		// that only uses probeIdx slice.
		return
	}
	if hj.spec.JoinType.IsLeftOuterOrFullOuter() {
		if cap(hj.probeState.probeRowUnmatched) < batchSize {
			hj.probeState.probeRowUnmatched = make([]bool, batchSize)
		} else {
			hj.probeState.probeRowUnmatched = hj.probeState.probeRowUnmatched[:batchSize]
		}
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
		batch := hj.inputOne.Next()
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
			hj.probeState.buckets = make([]uint64, batchSize)
		} else {
			// Note that we don't need to clear old values from buckets
			// because the correct values will be populated in
			// ComputeBuckets.
			hj.probeState.buckets = hj.probeState.buckets[:batchSize]
		}
		hj.ht.ComputeBuckets(hj.probeState.buckets, hj.ht.Keys, batchSize, sel)

		// Then, we initialize GroupID with the initial hash buckets and
		// ToCheck with all applicable indices.
		hj.ht.ProbeScratch.SetupLimitedSlices(batchSize, hj.ht.BuildMode)
		// Early bounds checks.
		groupIDs := hj.ht.ProbeScratch.GroupID
		_ = groupIDs[batchSize-1]
		var nToCheck uint64
		switch hj.spec.JoinType {
		case descpb.LeftAntiJoin, descpb.RightAntiJoin, descpb.ExceptAllJoin:
			// The setup of probing for LEFT/RIGHT ANTI and EXCEPT ALL joins
			// needs a special treatment in order to reuse the same "check"
			// functions below.
			for i, bucket := range hj.probeState.buckets[:batchSize] {
				f := hj.ht.BuildScratch.First[bucket]
				//gcassert:bce
				groupIDs[i] = f
				if hj.ht.BuildScratch.First[bucket] != 0 {
					// Non-zero "first" key indicates that there is a match of hashes
					// and we need to include the current tuple to check whether it is
					// an actual match.
					hj.ht.ProbeScratch.ToCheck[nToCheck] = uint64(i)
					nToCheck++
				}
			}
		default:
			for i, bucket := range hj.probeState.buckets[:batchSize] {
				f := hj.ht.BuildScratch.First[bucket]
				//gcassert:bce
				groupIDs[i] = f
			}
			copy(hj.ht.ProbeScratch.ToCheck, colexechash.HashTableInitialToCheck[:batchSize])
			nToCheck = uint64(batchSize)
		}

		// Now we collect all matches that we can emit in the probing phase
		// in a single batch.
		hj.prepareForCollecting(batchSize)
		var nResults int
		if hj.spec.rightDistinct {
			for nToCheck > 0 {
				// Continue searching along the hash table next chains for the corresponding
				// buckets. If the key is found or end of next chain is reached, the key is
				// removed from the ToCheck array.
				nToCheck = hj.ht.DistinctCheck(nToCheck, sel)
				hj.ht.FindNext(hj.ht.BuildScratch.Next, nToCheck)
			}

			nResults = hj.distinctCollect(batch, batchSize, sel)
		} else {
			for nToCheck > 0 {
				// Continue searching for the build table matching keys while the ToCheck
				// array is non-empty.
				nToCheck = hj.ht.Check(hj.ht.Keys, nToCheck, sel)
				hj.ht.FindNext(hj.ht.BuildScratch.Next, nToCheck)
			}

			// We're processing a new batch, so we'll reset the index to start
			// collecting from.
			hj.probeState.prevBatchResumeIdx = 0
			nResults = hj.collect(batch, batchSize, sel)
		}

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
func (hj *hashJoiner) congregate(nResults int, batch coldata.Batch) {
	hj.resetOutput(nResults)
	// We have already fully built the hash table from the right input and now
	// are only populating output one batch at a time. If we were to use a
	// limited allocator, we could hit the limit here, and it would have been
	// very hard to fall back to disk backed hash joiner because we might have
	// already emitted partial output.
	hj.outputUnlimitedAllocator.PerformOperation(hj.output.ColVecs(), func() {
		if hj.spec.JoinType.ShouldIncludeLeftColsInOutput() {
			outCols := hj.output.ColVecs()[:len(hj.spec.Left.SourceTypes)]
			for i := range hj.spec.Left.SourceTypes {
				outCol := outCols[i]
				valCol := batch.ColVec(i)
				outCol.Copy(
					coldata.CopySliceArgs{
						SliceArgs: coldata.SliceArgs{
							Src:       valCol,
							Sel:       hj.probeState.probeIdx,
							SrcEndIdx: nResults,
						},
					},
				)
			}
		}

		if hj.spec.JoinType.ShouldIncludeRightColsInOutput() {
			rightColOffset := len(hj.spec.Left.SourceTypes)
			// If the hash table is empty, then there is nothing to copy. The nulls
			// will be set below.
			if hj.ht.Vals.Length() > 0 {
				outCols := hj.output.ColVecs()[rightColOffset : rightColOffset+len(hj.spec.Right.SourceTypes)]
				for i := range hj.spec.Right.SourceTypes {
					outCol := outCols[i]
					valCol := hj.ht.Vals.ColVec(i)
					// Note that if for some index i, probeRowUnmatched[i] is true, then
					// hj.buildIdx[i] == 0 which will copy the garbage zeroth row of the
					// hash table, but we will set the NULL value below.
					outCol.Copy(
						coldata.CopySliceArgs{
							SliceArgs: coldata.SliceArgs{
								Src:       valCol,
								SrcEndIdx: nResults,
								Sel:       hj.probeState.buildIdx,
							},
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
	if hj.inputOne == input {
		// We do not buffer anything from the left source. Furthermore, the memory
		// limit can only hit during the building of the hash table step at which
		// point we haven't requested a single batch from the left.
		return coldata.ZeroBatch
	} else if hj.inputTwo == input {
		if hj.exportBufferedState.rightExported == hj.ht.Vals.Length() {
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

func (hj *hashJoiner) resetOutput(nResults int) {
	minCapacity := nResults
	if minCapacity < 1 {
		minCapacity = 1
	}
	// We're resetting the output meaning that we have already fully built the
	// hash table from the right input and now are only populating output one
	// batch at a time. If we were to use a limited allocator, we could hit the
	// limit here, and it would have been very hard to fall back to disk backed
	// hash joiner because we might have already emitted partial output.
	hj.output, _ = hj.outputUnlimitedAllocator.ResetMaybeReallocate(
		hj.outputTypes, hj.output, minCapacity, hj.memoryLimit,
	)
}

func (hj *hashJoiner) Reset(ctx context.Context) {
	for _, input := range []colexecop.Operator{hj.inputOne, hj.inputTwo} {
		if r, ok := input.(colexecop.Resetter); ok {
			r.Reset(ctx)
		}
	}
	hj.state = hjBuilding
	hj.ht.Reset(ctx)
	// Note that we don't zero out hj.probeState.buildIdx,
	// hj.probeState.probeIdx, and hj.probeState.probeRowUnmatched because the
	// values in these slices are always set in collecting methods.
	// hj.probeState.buildRowMatched is reset after building the hash table is
	// complete in build() method.
	hj.emittingRightState.rowIdx = 0
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

// NewHashJoiner creates a new equality hash join operator on the left and
// right input tables.
// buildSideAllocator should use a limited memory account and will be used for
// the build side whereas outputUnlimitedAllocator should use an unlimited
// memory account and will only be used when populating the output.
// memoryLimit will limit the size of the batches produced by the hash joiner.
func NewHashJoiner(
	buildSideAllocator, outputUnlimitedAllocator *colmem.Allocator,
	spec HashJoinerSpec,
	leftSource, rightSource colexecop.Operator,
	initialNumBuckets uint64,
	memoryLimit int64,
) colexecop.ResettableOperator {
	return &hashJoiner{
		joinHelper:                 newJoinHelper(leftSource, rightSource),
		buildSideAllocator:         buildSideAllocator,
		outputUnlimitedAllocator:   outputUnlimitedAllocator,
		spec:                       spec,
		memoryLimit:                memoryLimit,
		outputTypes:                spec.JoinType.MakeOutputTypes(spec.Left.SourceTypes, spec.Right.SourceTypes),
		hashTableInitialNumBuckets: initialNumBuckets,
	}
}
