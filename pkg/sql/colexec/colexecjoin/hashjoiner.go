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
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecargs"
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
// Depending on the value of the spec.RightDistinct flag, there are two
// variations of the probe phase. The planner will set RightDistinct to true if
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
// is done by setting ProbeRowUnmatched at that row to true.
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
	spec colexecargs.HashJoinerSpec
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
		colexechash.JoinProbeState
		// buildRowMatched is used in the case that spec.TrackBuildMatches is
		// true. This means that an outer join is performed on the build side
		// and buildRowMatched marks all the build table rows that have been
		// matched already. The rows that were unmatched are emitted during the
		// hjEmittingRight phase.
		//
		// Note that this is the only slice in probeState of non-constant size
		// (i.e. not limited by coldata.BatchSize() in capacity), so it's the
		// only one we perform the memory accounting for.
		buildRowMatched []bool
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
		hashTableReleased  bool
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
				if hj.spec.TrackBuildMatches {
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
	hj.ht.FullBuild(hj.InputTwo, false /* storeHashCodes */)

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

	if !hj.spec.RightDistinct {
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

	if hj.spec.TrackBuildMatches {
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
	// Make sure that hj.probeState.BuildIdx is of sufficient size (it is used
	// as a selection vector to select only the necessary tuples).
	buildIdxSize := hj.ht.Vals.Length() - hj.emittingRightState.rowIdx
	if buildIdxSize > coldata.BatchSize() {
		buildIdxSize = coldata.BatchSize()
	}
	if cap(hj.probeState.BuildIdx) < buildIdxSize {
		hj.probeState.BuildIdx = make([]int, buildIdxSize)
	} else {
		hj.probeState.BuildIdx = hj.probeState.BuildIdx[:buildIdxSize]
	}

	// Find the next batch of tuples that have the requested 'matched' value.
	nResults := 0
	for nResults < coldata.BatchSize() && hj.emittingRightState.rowIdx < hj.ht.Vals.Length() {
		if hj.probeState.buildRowMatched[hj.emittingRightState.rowIdx] == matched {
			hj.probeState.BuildIdx[nResults] = hj.emittingRightState.rowIdx
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
					Sel:       hj.probeState.BuildIdx,
				},
			)
		}

		hj.output.SetLength(nResults)
	})
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
		hj.probeState.PrepareForCollecting(coldata.BatchSize())
		nResults := hj.collect(&hj.probeState.JoinProbeState, batch, batchSize, sel)
		if nResults > 0 {
			hj.congregate(nResults, batch)
			return hj.output
		}
		// There were no matches in that batch, so we move on to the next one.
	}
	for {
		batch := hj.InputOne.Next()
		batchSize := batch.Length()
		sel := batch.Selection()

		if batchSize == 0 {
			return coldata.ZeroBatch
		}

		// First, we compute the hash values for all tuples in the batch.
		hj.probeState.PrepareForNewBatch(hj.ht, batch)

		// Then, we initialize ToCheckID with the initial hash buckets and
		// ToCheck with all applicable indices. Notably, only probing tuples
		// that have hash matches are included into ToCheck whereas ToCheckID is
		// correctly set for all tuples.
		//
		// Early bounds checks.
		toCheckIDs := hj.ht.ProbeScratch.ToCheckID
		_ = toCheckIDs[batchSize-1]
		var nToCheck uint32
		for i, bucket := range hj.probeState.Buckets[:batchSize] {
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

		hj.probeState.PrepareForCollecting(batchSize)
		checker, collector := hj.ht.Check, hj.collect
		if hj.spec.RightDistinct {
			checker, collector = hj.ht.DistinctCheck, hj.ht.DistinctCollect
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
		nResults := collector(&hj.probeState.JoinProbeState, batch, batchSize, sel)
		if nResults > 0 {
			hj.congregate(nResults, batch)
			break
		}
	}
	return hj.output
}

// congregate uses the ProbeIdx and BuildIdx pairs to stitch together the
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
					Sel:       hj.probeState.ProbeIdx,
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
					// Note that if for some index i, ProbeRowUnmatched[i] is true, then
					// hj.BuildIdx[i] == 0 which will copy the garbage zeroth row of the
					// hash table, but we will set the NULL value below.
					outCol.Copy(
						coldata.SliceArgs{
							Src:       valCol,
							SrcEndIdx: nResults,
							Sel:       hj.probeState.BuildIdx,
						},
					)
				}
			}
			if hj.spec.JoinType.IsLeftOuterOrFullOuter() {
				// Add in the nulls we needed to set for the outer join.
				for i := range hj.spec.Right.SourceTypes {
					outCol := hj.output.ColVec(i + rightColOffset)
					nulls := outCol.Nulls()
					for i, isNull := range hj.probeState.ProbeRowUnmatched[:nResults] {
						if isNull {
							nulls.SetNull(i)
						}
					}
				}
			}
		}

		if hj.spec.TrackBuildMatches {
			// Early bounds checks.
			buildIdx := hj.probeState.BuildIdx
			_ = buildIdx[nResults-1]
			if hj.spec.JoinType.IsLeftOuterOrFullOuter() {
				// Early bounds checks.
				probeRowUnmatched := hj.probeState.ProbeRowUnmatched
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
		if hj.exportBufferedState.hashTableReleased {
			return coldata.ZeroBatch
		}
		if hj.exportBufferedState.rightExported == hj.ht.Vals.Length() {
			// We no longer need the hash table, so we can release it.
			hj.ht.Release()
			hj.exportBufferedState.hashTableReleased = true
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
	// Note that we don't zero out hj.probeState.BuildIdx,
	// hj.probeState.ProbeIdx, and hj.probeState.ProbeRowUnmatched because the
	// values in these slices are always set in collecting methods.
	// hj.probeState.buildRowMatched is reset after building the hash table is
	// complete in build() method.
	hj.emittingRightState.rowIdx = 0
	if buildutil.CrdbTestBuild {
		if hj.exportBufferedState.hashTableReleased {
			colexecerror.InternalError(errors.AssertionFailedf(
				"the hash joiner is being reset after having spilled to disk",
			))
		}
	}
	hj.exportBufferedState.hashTableReleased = false
	hj.exportBufferedState.rightExported = 0
}

// NewHashJoinerArgs encompasses all arguments to NewHashJoiner call.
type NewHashJoinerArgs struct {
	BuildSideAllocator       *colmem.Allocator
	OutputUnlimitedAllocator *colmem.Allocator
	Spec                     colexecargs.HashJoinerSpec
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
	hj := &hashJoiner{
		TwoInputInitHelper:         colexecop.MakeTwoInputInitHelper(args.LeftSource, args.RightSource),
		hashTableAllocator:         args.BuildSideAllocator,
		outputUnlimitedAllocator:   args.OutputUnlimitedAllocator,
		spec:                       args.Spec,
		outputTypes:                args.Spec.JoinType.MakeOutputTypes(args.Spec.Left.SourceTypes, args.Spec.Right.SourceTypes),
		hashTableInitialNumBuckets: args.InitialNumBuckets,
	}
	hj.probeState.JoinProbeState = colexechash.JoinProbeState{
		Type:   args.Spec.JoinType,
		EqCols: args.Spec.Left.EqCols,
	}
	return hj
}
