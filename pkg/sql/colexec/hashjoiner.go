// Copyright 2018 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/pkg/errors"
)

// hashJoinerState represents the state of the hash join columnar operator.
type hashJoinerState int

const (
	// hjBuilding represents the state the hashJoiner when it is in the build
	// phase. Output columns from the build table are stored and a hash map is
	// constructed from its equality columns.
	hjBuilding = iota

	// hjProbing represents the state the hashJoiner is in when it is in the probe
	// phase. Probing is done in batches against the stored hash map.
	hjProbing

	// hjEmittingUnmatched represents the state the hashJoiner is in when it is
	// emitting unmatched rows from its build table after having consumed the
	// probe table. This happens in the case of an outer join on the build side.
	hjEmittingUnmatched
)

// hashJoinerSpec is the specification for a hash join operator. The hash
// joiner performs a join on the left and right's equal columns and returns
// combined left and right output columns.
type hashJoinerSpec struct {
	joinType sqlbase.JoinType
	// left and right are the specifications of the two input table sources to
	// the hash joiner.
	left  hashJoinerSourceSpec
	right hashJoinerSourceSpec

	// rightDistinct indicates whether or not the build table equality column
	// tuples are distinct. If they are distinct, performance can be optimized.
	rightDistinct bool
}

type hashJoinerSourceSpec struct {
	// eqCols specify the indices of the source tables equality column during the
	// hash join.
	eqCols []uint32

	// outCols specify the indices of the columns that should be outputted by the
	// hash joiner.
	outCols []uint32

	// sourceTypes specify the types of the input columns of the source table for
	// the hash joiner.
	sourceTypes []coltypes.T

	// outer specifies whether an outer join is required over the input.
	outer bool
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
//   and store it in the groupID array. Initialize the toCheck array with the
//   full sequence of input indices (0...batchSize - 1).
// - While toCheck is not empty, each element in toCheck represents a position
//   of the key tuples for which the key has not yet been found in the hash
//   table. Perform a multi-column equality check to see if the key columns
//   match that of the build table's key columns at groupID.
// - Update the differs array to store whether or not the probe's key tuple
//   matched the corresponding build's key tuple.
// - Select the indices that differed and store them into toCheck since they
//   need to be further processed.
// - For the differing tuples, find the next ID in that bucket of the hash table
//   and put it into the groupID array.
// 3. Now, groupID for every probe's key tuple contains the index of the
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
//   and store it in the groupID array. Initialize the toCheck array with the
//   full sequence of input indices (0...batchSize - 1).
// - While toCheck is not empty, each element in toCheck represents a position
//   of the key tuples for which the key has not yet been visited by any prior
//   probe. Perform a multi-column equality check to see if the key columns
//   match that of the build table's key columns at groupID.
// - Update the differs array to store whether or not the probe's key tuple
//   matched the corresponding build's key tuple.
// - For the indices that did not differ, we can lazily update the hashTable's
//   same linked list to store a list of all identical keys starting at head.
//   Once a key has been added to ht.same, ht.visited is set to true. For the
//   indices that have never been visited, we want to continue checking this
//   bucket for identical values by adding this key to toCheck.
// - Select the indices that differed and store them into toCheck since they
//   need to be further processed.
// - For the differing tuples, find the next ID in that bucket of the hash table
//   and put it into the groupID array.
// 3. Now, head stores the keyID of the first match in the build table for every
//    probe table key. ht.same is used to select all build key matches for each
//    probe key, which are added to the resulting batch. Output batching is done
//    to ensure that each batch is at most coldata.BatchSize().
//
// In the case that an outer join on the probe table side is performed, every
// single probe row is kept even if its groupID is 0. If a groupID of 0 is
// found, this means that the matching build table row should be all NULL. This
// is done by setting probeRowUnmatched at that row to true.
//
// In the case that an outer join on the build table side is performed, an
// emitUnmatched is performed after the probing ends. This is done by gathering
// all build table rows that have never been matched and stitching it together
// with NULL values on the probe side.
type hashJoiner struct {
	twoInputNode

	allocator *Allocator
	// spec holds the specification for the current hash join process.
	spec hashJoinerSpec
	// state stores the current state of the hash joiner.
	state hashJoinerState
	// ht holds the hashTable that is populated during the build phase and used
	// during the probe phase.
	ht *hashTable
	// output stores the resulting output batch that is constructed and returned
	// for every input batch during the probe phase.
	output coldata.Batch
	// outputBatchSize specifies the desired length of the output batch which by
	// default is coldata.BatchSize() but can be varied in tests.
	outputBatchSize uint16

	// probeState is used in hjProbing state.
	probeState struct {
		// buildIdx and probeIdx represents the matching row indices that are used to
		// stitch together the join results. Since probing is done on a per-batch
		// basis, the indices will always fit within uint16. However, the matching
		// build table row index should be an uint64 since it refers to the entirety
		// of the build table.
		buildIdx []uint64
		probeIdx []uint16

		// probeRowUnmatched is used in the case that the prober.spec.outer is true.
		// This means that an outer join is performed on the probe side and we use
		// probeRowUnmatched to represent that the resulting columns should be NULL on
		// the build table. This indicates that the probe table row did not match any
		// build table rows.
		probeRowUnmatched []bool
		// buildRowMatched is used in the case that prober.buildOuter is true. This
		// means that an outer join is performed on the build side and buildRowMatched
		// marks all the build table rows that have been matched already. The rows
		// that were unmatched are emitted during the emitUnmatched phase.
		buildRowMatched []bool

		// prevBatch, if not nil, indicates that the previous probe input batch has
		// not been fully processed.
		prevBatch coldata.Batch
		// prevBatchResumeIdx indicates the index of the probe row to resume the
		// collection from. It is used only in case of non-distinct build source
		// (every probe row can have multiple matching build rows).
		prevBatchResumeIdx uint16
	}

	// emittingUnmatchedState is used in hjEmittingUnmatched state.
	emittingUnmatchedState struct {
		rowIdx uint64
	}

	exportBufferedState struct {
		rightExported      uint64
		rightWindowedBatch coldata.Batch
	}
}

var _ bufferingInMemoryOperator = &hashJoiner{}
var _ resetter = &hashJoiner{}

func (hj *hashJoiner) Init() {
	hj.inputOne.Init()
	hj.inputTwo.Init()

	hj.ht = newHashTable(
		hj.allocator,
		hashTableNumBuckets,
		hj.spec.right.sourceTypes,
		hj.spec.right.eqCols,
		hj.spec.right.outCols,
		false, /* allowNullEquality */
	)

	hj.exportBufferedState.rightWindowedBatch = hj.allocator.NewMemBatchWithSize(hj.spec.right.sourceTypes, 0 /* size */)
	hj.state = hjBuilding
}

func (hj *hashJoiner) Next(ctx context.Context) coldata.Batch {
	hj.resetOutput()
	for {
		switch hj.state {
		case hjBuilding:
			hj.build(ctx)
			continue
		case hjProbing:
			hj.exec(ctx)

			if hj.output.Length() == 0 && hj.spec.right.outer {
				hj.state = hjEmittingUnmatched
				continue
			}
			return hj.output
		case hjEmittingUnmatched:
			hj.emitUnmatched()
			return hj.output
		default:
			execerror.VectorizedInternalPanic("hash joiner in unhandled state")
			// This code is unreachable, but the compiler cannot infer that.
			return nil
		}
	}
}

func (hj *hashJoiner) build(ctx context.Context) {
	hj.ht.build(ctx, hj.inputTwo)

	if !hj.spec.rightDistinct {
		hj.ht.maybeAllocateSameAndVisited()
	}

	if hj.spec.right.outer {
		if uint64(cap(hj.probeState.buildRowMatched)) < hj.ht.vals.length {
			hj.probeState.buildRowMatched = make([]bool, hj.ht.vals.length)
		} else {
			hj.probeState.buildRowMatched = hj.probeState.buildRowMatched[:hj.ht.vals.length]
			for n := uint64(0); n < hj.ht.vals.length; n += uint64(copy(hj.probeState.buildRowMatched[n:], zeroBoolColumn)) {
			}
		}
	}

	hj.state = hjProbing
}

func (hj *hashJoiner) emitUnmatched() {
	// Set all elements in the probe columns of the output batch to null.
	for i := range hj.spec.left.outCols {
		outCol := hj.output.ColVec(i)
		outCol.Nulls().SetNulls()
	}

	nResults := uint16(0)

	for nResults < hj.outputBatchSize && hj.emittingUnmatchedState.rowIdx < hj.ht.vals.length {
		if !hj.probeState.buildRowMatched[hj.emittingUnmatchedState.rowIdx] {
			hj.probeState.buildIdx[nResults] = hj.emittingUnmatchedState.rowIdx
			nResults++
		}
		hj.emittingUnmatchedState.rowIdx++
	}

	outCols := hj.output.ColVecs()[len(hj.spec.left.outCols) : len(hj.spec.left.outCols)+len(hj.ht.outCols)]
	for outColIdx, inColIdx := range hj.ht.outCols {
		outCol := outCols[outColIdx]
		valCol := hj.ht.vals.colVecs[inColIdx]
		colType := hj.ht.valTypes[inColIdx]
		// NOTE: this Copy is not accounted for because we don't want for memory
		// limit error to occur at this point - we have already built the hash
		// table and now are only consuming the left source one batch at a time,
		// so such behavior should be a minor deviation from the limit. If we were
		// to hit the limit here, it would have been very hard to fall back to disk
		// backed hash joiner because we might have already emitted partial output.
		// This behavior is acceptable - we allocated hj.output batch already, so
		// the concern here is only for the variable-sized types that exceed our
		// estimations.
		outCol.Copy(
			coldata.CopySliceArgs{
				SliceArgs: coldata.SliceArgs{
					ColType:   colType,
					Src:       valCol,
					SrcEndIdx: uint64(nResults),
				},
				Sel64: hj.probeState.buildIdx,
			},
		)
	}

	hj.output.SetLength(nResults)
}

// exec is a general prober that works with non-distinct build table equality
// columns. It returns a Batch with N + M columns where N is the number of
// left source columns and M is the number of right source columns. The first N
// columns correspond to the respective left source columns, followed by the
// right source columns as the last M elements. Even though all the columns are
// present in the result, only the specified output columns store relevant
// information. The remaining columns are there as dummy columns and their
// states are undefined.
//
// rightDistinct is true if the build table equality columns are distinct. It
// performs the same operation as the exec() function normally would while
// taking a shortcut to improve speed.
func (hj *hashJoiner) exec(ctx context.Context) {
	hj.output.SetLength(0)

	if batch := hj.probeState.prevBatch; batch != nil {
		// The previous result was bigger than the maximum batch size, so we didn't
		// finish outputting it in the last call to probe. Continue outputting the
		// result from the previous batch.
		hj.probeState.prevBatch = nil
		batchSize := batch.Length()
		sel := batch.Selection()

		nResults := hj.collect(batch, batchSize, sel)
		hj.congregate(nResults, batch, batchSize)
	} else {
		for {
			batch := hj.inputOne.Next(ctx)
			batchSize := batch.Length()

			if batchSize == 0 {
				break
			}

			for i, colIdx := range hj.spec.left.eqCols {
				hj.ht.keys[i] = batch.ColVec(int(colIdx))
			}

			sel := batch.Selection()

			var nToCheck uint16
			switch hj.spec.joinType {
			case sqlbase.JoinType_LEFT_ANTI:
				// The setup of probing for LEFT ANTI join needs a special treatment in
				// order to reuse the same "check" functions below.
				//
				// First, we compute the hash values for all tuples in the batch.
				hj.ht.computeBuckets(ctx, hj.ht.buckets, hj.ht.keys, uint64(batchSize), sel)
				// Then, we iterate over all tuples to see whether there is at least
				// one tuple in the hash table that has the same hash value.
				for i := uint16(0); i < batchSize; i++ {
					if hj.ht.first[hj.ht.buckets[i]] != 0 {
						// Non-zero "first" key indicates that there is a match of hashes
						// and we need to include the current tuple to check whether it is
						// an actual match.
						hj.ht.groupID[i] = hj.ht.first[hj.ht.buckets[i]]
						hj.ht.toCheck[nToCheck] = i
						nToCheck++
					}
				}
				// We need to reset headID for all tuples in the batch to remove any
				// leftover garbage from the previous iteration. For tuples that need
				// to be checked, headID will be updated accordingly; for tuples that
				// definitely don't have a match, the zero value will remain until the
				// "collecting" and "congregation" step in which such tuple will be
				// included into the output.
				copy(hj.ht.headID[:batchSize], zeroUint64Column)
			default:
				// Initialize groupID with the initial hash buckets and toCheck with all
				// applicable indices.
				hj.ht.lookupInitial(ctx, batchSize, sel)
				nToCheck = batchSize
			}

			var nResults uint16

			if hj.spec.rightDistinct {
				for nToCheck > 0 {
					// Continue searching along the hash table next chains for the corresponding
					// buckets. If the key is found or end of next chain is reached, the key is
					// removed from the toCheck array.
					nToCheck = hj.ht.distinctCheck(nToCheck, sel)
					hj.ht.findNext(nToCheck)
				}

				nResults = hj.distinctCollect(batch, batchSize, sel)
			} else {
				for nToCheck > 0 {
					// Continue searching for the build table matching keys while the toCheck
					// array is non-empty.
					nToCheck = hj.ht.check(nToCheck, sel)
					hj.ht.findNext(nToCheck)
				}

				// We're processing a new batch, so we'll reset the index to start
				// collecting from.
				hj.probeState.prevBatchResumeIdx = 0
				nResults = hj.collect(batch, batchSize, sel)
			}

			hj.congregate(nResults, batch, batchSize)

			if hj.output.Length() > 0 {
				break
			}
		}
	}
}

// congregate uses the probeIdx and buildIdx pairs to stitch together the
// resulting join rows and add them to the output batch with the left table
// columns preceding the right table columns.
func (hj *hashJoiner) congregate(nResults uint16, batch coldata.Batch, batchSize uint16) {
	// NOTE: Copy() calls are not accounted for because we don't want for memory
	// limit error to occur at this point - we have already built the hash
	// table and now are only consuming the left source one batch at a time,
	// so such behavior should be a minor deviation from the limit. If we were
	// to hit the limit here, it would have been very hard to fall back to disk
	// backed hash joiner because we might have already emitted partial output.
	// This behavior is acceptable - we allocated hj.output batch already, so the
	// concern here is only for the variable-sized types that exceed our
	// estimations.

	rightColOffset := len(hj.spec.left.outCols)
	// If the hash table is empty, then there is nothing to copy. The nulls
	// will be set below.
	if hj.ht.vals.length > 0 {
		outCols := hj.output.ColVecs()[rightColOffset : rightColOffset+len(hj.ht.outCols)]
		for outColIdx, inColIdx := range hj.ht.outCols {
			outCol := outCols[outColIdx]
			valCol := hj.ht.vals.colVecs[inColIdx]
			colType := hj.ht.valTypes[inColIdx]
			// Note that if for some index i, probeRowUnmatched[i] is true, then
			// hj.buildIdx[i] == 0 which will copy the garbage zeroth row of the
			// hash table, but we will set the NULL value below.
			outCol.Copy(
				coldata.CopySliceArgs{
					SliceArgs: coldata.SliceArgs{
						ColType:   colType,
						Src:       valCol,
						SrcEndIdx: uint64(nResults),
					},
					Sel64: hj.probeState.buildIdx,
				},
			)
		}
	}
	if hj.spec.left.outer {
		// Add in the nulls we needed to set for the outer join.
		for outColIdx := range hj.ht.outCols {
			outCol := hj.output.ColVec(outColIdx + rightColOffset)
			nulls := outCol.Nulls()
			for i, isNull := range hj.probeState.probeRowUnmatched {
				if isNull {
					nulls.SetNull(uint16(i))
				}
			}
		}
	}

	outCols := hj.output.ColVecs()[:len(hj.spec.left.outCols)]
	for outColIdx, inColIdx := range hj.spec.left.outCols {
		outCol := outCols[outColIdx]
		valCol := batch.ColVec(int(inColIdx))
		colType := hj.spec.left.sourceTypes[inColIdx]

		outCol.Copy(
			coldata.CopySliceArgs{
				SliceArgs: coldata.SliceArgs{
					ColType:   colType,
					Src:       valCol,
					Sel:       hj.probeState.probeIdx,
					SrcEndIdx: uint64(nResults),
				},
			},
		)
	}

	if hj.spec.right.outer {
		// In order to determine which rows to emit for the outer join on the build
		// table in the end, we need to mark the matched build table rows.
		if hj.spec.left.outer {
			for i := uint16(0); i < nResults; i++ {
				if !hj.probeState.probeRowUnmatched[i] {
					hj.probeState.buildRowMatched[hj.probeState.buildIdx[i]] = true
				}
			}
		} else {
			for i := uint16(0); i < nResults; i++ {
				hj.probeState.buildRowMatched[hj.probeState.buildIdx[i]] = true
			}
		}
	}

	hj.output.SetLength(nResults)
}

func (hj *hashJoiner) ExportBuffered(input Operator) coldata.Batch {
	if hj.inputOne == input {
		// We do not buffer anything from the left source. Furthermore, the memory
		// limit can only hit during the building of the hash table step at which
		// point we haven't requested a single batch from the left.
		return coldata.ZeroBatch
	} else if hj.inputTwo == input {
		if hj.exportBufferedState.rightExported == hj.ht.vals.length {
			return coldata.ZeroBatch
		}
		newRightExported := hj.exportBufferedState.rightExported + uint64(coldata.BatchSize())
		if newRightExported > hj.ht.vals.length {
			newRightExported = hj.ht.vals.length
		}
		startIdx, endIdx := hj.exportBufferedState.rightExported, newRightExported
		b := hj.exportBufferedState.rightWindowedBatch
		for i, t := range hj.spec.right.sourceTypes {
			window := hj.ht.vals.colVecs[i].Window(t, startIdx, endIdx)
			b.ReplaceCol(window, i)
		}
		b.SetLength(uint16(endIdx - startIdx))
		hj.exportBufferedState.rightExported = newRightExported
		return b
	} else {
		execerror.VectorizedInternalPanic(errors.New(
			"unexpectedly ExportBuffered is called with neither left nor right inputs to hash join",
		))
		// This code is unreachable, but the compiler cannot infer that.
		return nil
	}
}

func (hj *hashJoiner) resetOutput() {
	if hj.output == nil {
		var outColTypes []coltypes.T
		for _, probeOutCol := range hj.spec.left.outCols {
			outColTypes = append(outColTypes, hj.spec.left.sourceTypes[probeOutCol])
		}
		for _, buildOutCol := range hj.spec.right.outCols {
			outColTypes = append(outColTypes, hj.spec.right.sourceTypes[buildOutCol])
		}
		hj.output = hj.allocator.NewMemBatch(outColTypes)
	} else {
		hj.output.ResetInternalBatch()
	}
}

func (hj *hashJoiner) reset() {
	for _, input := range []Operator{hj.inputOne, hj.inputTwo} {
		if r, ok := input.(resetter); ok {
			r.reset()
		}
	}
	hj.state = hjBuilding
	hj.ht.reset()
	copy(hj.probeState.buildIdx[:coldata.BatchSize()], zeroUint64Column)
	copy(hj.probeState.probeIdx[:coldata.BatchSize()], zeroUint16Column)
	if hj.spec.left.outer {
		copy(hj.probeState.probeRowUnmatched[:coldata.BatchSize()], zeroBoolColumn)
	}
	// hj.probeState.buildRowMatched is reset after building the hash table is
	// complete in build() method.
	hj.emittingUnmatchedState.rowIdx = 0
	hj.exportBufferedState.rightExported = 0
}

// makeHashJoinerSpec creates a specification for columnar hash join operator.
// leftEqCols and rightEqCols specify the equality columns while leftOutCols
// and rightOutCols specifies the output columns. leftTypes and rightTypes
// specify the input column types of the two sources. rightDistinct indicates
// whether the equality columns of the right source form a key.
func makeHashJoinerSpec(
	joinType sqlbase.JoinType,
	leftEqCols []uint32,
	rightEqCols []uint32,
	leftTypes []coltypes.T,
	rightTypes []coltypes.T,
	rightDistinct bool,
) (hashJoinerSpec, error) {
	var (
		spec                  hashJoinerSpec
		leftOuter, rightOuter bool
	)
	// TODO(yuzefovich): get rid of "outCols" entirely and plumb the assumption
	// of outputting all columns into the hash joiner itself.
	leftOutCols := make([]uint32, len(leftTypes))
	for i := range leftOutCols {
		leftOutCols[i] = uint32(i)
	}
	rightOutCols := make([]uint32, len(rightTypes))
	for i := range rightOutCols {
		rightOutCols[i] = uint32(i)
	}
	switch joinType {
	case sqlbase.JoinType_INNER:
	case sqlbase.JoinType_RIGHT_OUTER:
		rightOuter = true
	case sqlbase.JoinType_LEFT_OUTER:
		leftOuter = true
	case sqlbase.JoinType_FULL_OUTER:
		rightOuter = true
		leftOuter = true
	case sqlbase.JoinType_LEFT_SEMI:
		// In a semi-join, we don't need to store anything but a single row per
		// build row, since all we care about is whether a row on the left matches
		// any row on the right.
		// Note that this is *not* the case if we have an ON condition, since we'll
		// also need to make sure that a row on the left passes the ON condition
		// with the row on the right to emit it. However, we don't support ON
		// conditions just yet. When we do, we'll have a separate case for that.
		rightDistinct = true
		rightOutCols = rightOutCols[:0]
	case sqlbase.JoinType_LEFT_ANTI:
		rightOutCols = rightOutCols[:0]
	default:
		return spec, errors.Errorf("hash join of type %s not supported", joinType)
	}

	left := hashJoinerSourceSpec{
		eqCols:      leftEqCols,
		outCols:     leftOutCols,
		sourceTypes: leftTypes,
		outer:       leftOuter,
	}
	right := hashJoinerSourceSpec{
		eqCols:      rightEqCols,
		outCols:     rightOutCols,
		sourceTypes: rightTypes,
		outer:       rightOuter,
	}
	spec = hashJoinerSpec{
		joinType:      joinType,
		left:          left,
		right:         right,
		rightDistinct: rightDistinct,
	}
	return spec, nil
}

// newHashJoiner creates a new equality hash join operator on the left and
// right input tables.
func newHashJoiner(
	allocator *Allocator, spec hashJoinerSpec, leftSource, rightSource Operator,
) Operator {
	hj := &hashJoiner{
		twoInputNode:    newTwoInputNode(leftSource, rightSource),
		allocator:       allocator,
		spec:            spec,
		outputBatchSize: coldata.BatchSize(),
	}
	hj.probeState.buildIdx = make([]uint64, coldata.BatchSize())
	hj.probeState.probeIdx = make([]uint16, coldata.BatchSize())
	if spec.left.outer {
		hj.probeState.probeRowUnmatched = make([]bool, coldata.BatchSize())
	}
	return hj
}
