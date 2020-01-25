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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execerror"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
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

// hashJoinerSpec is the specification for a hash joiner processor. The hash
// joiner performs a join on the left and right's equal columns and returns
// combined left and right output columns.
type hashJoinerSpec struct {
	// left and right are the specifications of the two input table sources to
	// the hash joiner.
	left  hashJoinerSourceSpec
	right hashJoinerSourceSpec

	// buildRightSide indicates whether or not the build table is the right side.
	// By default, this flag is false and the build table is the left side.
	buildRightSide bool

	// buildDistinct indicates whether or not the build table equality column
	// tuples are distinct. If they are distinct, performance can be optimized.
	buildDistinct bool
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

	// source specifies the input operator to the hash join.
	source Operator

	// outer specifies whether an outer join is required over the input.
	outer bool
}

// hashJoinEqOp performs a hash join on the input tables equality columns.
// It requires that the output for every input batch in the probe phase fits
// within coldata.BatchSize(), otherwise the behavior is undefined. A join is
// performed and there is no guarantee on the ordering of the output columns.
//
// Before the build phase, all equality and output columns from the build table
// are collected and stored.
//
// In the vectorized implementation of the build phase, the following tasks are
// performed:
// 1. The bucket number (hash value) of each key tuple is computed and stored
//    into a buckets array.
// 2. The values in the buckets array is normalized to fit within the hash table
//    bucketSize.
// 3. The bucket-chaining hash table organization is prepared with the computed
//    buckets.
//
// Depending on the value of the spec.buildDistinct flag, there are two
// variations of the probe phase. The planner will set buildDistinct to true if
// and only if either the left equality columns or the right equality columns
// make a distinct key. This corresponding table would then be used as the build
// table.
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
type hashJoinEqOp struct {
	allocator *Allocator
	// spec, if not nil, holds the specification for the current hash joiner
	// process.
	spec hashJoinerSpec

	// ht holds the hashTable that is populated during the build
	// phase and used during the probe phase.
	ht *hashTable

	// builder is used by the hashJoiner to execute the build phase.
	builder *hashJoinBuilder

	// prober, if not nil, stores the batch prober used by the hashJoiner in the
	// probe phase.
	prober *hashJoinProber

	// runningState stores the current state hashJoiner.
	runningState hashJoinerState

	// outputBatchSize specifies the desired length of the output batch which by
	// default is coldata.BatchSize() but can be varied in tests.
	outputBatchSize uint16

	// emittingUnmatchedState is used when hjEmittingUnmatched.
	emittingUnmatchedState struct {
		rowIdx uint64
	}
}

func (hj *hashJoinEqOp) ChildCount(verbose bool) int {
	return 2
}

func (hj *hashJoinEqOp) Child(nth int, verbose bool) execinfra.OpNode {
	switch nth {
	case 0:
		return hj.spec.left.source
	case 1:
		return hj.spec.right.source
	}
	execerror.VectorizedInternalPanic(fmt.Sprintf("invalid idx %d", nth))
	// This code is unreachable, but the compiler cannot infer that.
	return nil
}

var _ Operator = &hashJoinEqOp{}

func (hj *hashJoinEqOp) Init() {
	hj.spec.left.source.Init()
	hj.spec.right.source.Init()

	// Prepare the hashTable using the specified side as the build table. Prepare
	// the prober using the other side as the probe table.
	var build, probe hashJoinerSourceSpec
	if hj.spec.buildRightSide {
		build = hj.spec.right
		probe = hj.spec.left
	} else {
		build = hj.spec.left
		probe = hj.spec.right
	}

	hj.ht = newHashTable(
		hj.allocator,
		hashTableBucketSize,
		build.sourceTypes,
		build.eqCols,
		build.outCols,
		false, /* allowNullEquality */
	)

	hj.builder = newHashJoinBuilder(
		hj.ht,
		build,
	)

	hj.prober = newHashJoinProber(
		hj.allocator,
		hj.ht, probe, build,
		hj.spec.buildRightSide,
		hj.spec.buildDistinct,
		hj.outputBatchSize,
	)

	hj.runningState = hjBuilding
}

func (hj *hashJoinEqOp) Next(ctx context.Context) coldata.Batch {
	hj.prober.batch.ResetInternalBatch()
	for {
		switch hj.runningState {
		case hjBuilding:
			hj.build(ctx)
			continue
		case hjProbing:
			hj.prober.exec(ctx)

			if hj.prober.batch.Length() == 0 && hj.builder.spec.outer {
				hj.runningState = hjEmittingUnmatched
				continue
			}
			return hj.prober.batch
		case hjEmittingUnmatched:
			hj.emitUnmatched()
			return hj.prober.batch
		default:
			execerror.VectorizedInternalPanic("hash joiner in unhandled state")
			// This code is unreachable, but the compiler cannot infer that.
			return nil
		}
	}
}

func (hj *hashJoinEqOp) build(ctx context.Context) {
	hj.builder.distinctExec(ctx)

	if !hj.spec.buildDistinct {
		hj.ht.same = make([]uint64, hj.ht.vals.length+1)
		hj.ht.allocateVisited()
	}

	if hj.builder.spec.outer {
		hj.prober.buildRowMatched = make([]bool, hj.ht.vals.length)
	}

	hj.runningState = hjProbing
}

func (hj *hashJoinEqOp) emitUnmatched() {
	// Set all elements in the probe columns of the output batch to null.
	for i := range hj.prober.spec.outCols {
		outCol := hj.prober.batch.ColVec(i + hj.prober.probeColOffset)
		outCol.Nulls().SetNulls()
	}

	nResults := uint16(0)

	for nResults < hj.outputBatchSize && hj.emittingUnmatchedState.rowIdx < hj.ht.vals.length {
		if !hj.prober.buildRowMatched[hj.emittingUnmatchedState.rowIdx] {
			hj.prober.buildIdx[nResults] = hj.emittingUnmatchedState.rowIdx
			nResults++
		}
		hj.emittingUnmatchedState.rowIdx++
	}

	outCols := hj.prober.batch.ColVecs()[hj.prober.buildColOffset : hj.prober.buildColOffset+len(hj.ht.outCols)]
	hj.allocator.PerformOperation(outCols, func() {
		for outColIdx, inColIdx := range hj.ht.outCols {
			outCol := outCols[outColIdx]
			valCol := hj.ht.vals.colVecs[inColIdx]
			colType := hj.ht.valTypes[inColIdx]

			outCol.Copy(
				coldata.CopySliceArgs{
					SliceArgs: coldata.SliceArgs{
						ColType:   colType,
						Src:       valCol,
						SrcEndIdx: uint64(nResults),
					},
					Sel64: hj.prober.buildIdx,
				},
			)
		}
	})

	hj.prober.batch.SetLength(nResults)
}

// hashJoinBuilder is used by the hashJoiner during the build phase. It
// pre-loads all batches from the build relation before building the hash table.
type hashJoinBuilder struct {
	ht *hashTable

	// spec holds the specifications for the source operator used in the build
	// phase.
	spec hashJoinerSourceSpec
}

func newHashJoinBuilder(ht *hashTable, spec hashJoinerSourceSpec) *hashJoinBuilder {
	return &hashJoinBuilder{
		ht:   ht,
		spec: spec,
	}
}

// exec executes distinctExec, and then eagerly populates the hashTable's same
// array by probing the hashTable with every single input key. This is intended
// for use by the hash aggregator.
func (builder *hashJoinBuilder) exec(ctx context.Context) {
	builder.distinctExec(ctx)

	builder.ht.same = make([]uint64, builder.ht.vals.length+1)
	builder.ht.visited = make([]bool, builder.ht.vals.length+1)
	builder.ht.head = make([]bool, builder.ht.vals.length+1)

	// Since keyID = 0 is reserved for end of list, it can be marked as visited
	// at the beginning.
	builder.ht.visited[0] = true

	nKeyCols := len(builder.spec.eqCols)
	batchStart := uint64(0)
	for batchStart < builder.ht.vals.length {
		batchEnd := batchStart + uint64(coldata.BatchSize())
		if batchEnd > builder.ht.vals.length {
			batchEnd = builder.ht.vals.length
		}

		batchSize := uint16(batchEnd - batchStart)

		for i := 0; i < nKeyCols; i++ {
			builder.ht.keys[i] = builder.ht.vals.colVecs[builder.ht.keyCols[i]].Window(builder.ht.valTypes[builder.ht.keyCols[i]], batchStart, batchEnd)
		}

		builder.ht.lookupInitial(ctx, batchSize, nil)
		nToCheck := batchSize

		for nToCheck > 0 {
			// Continue searching for the build table matching keys while the toCheck
			// array is non-empty.
			nToCheck = builder.ht.check(nToCheck, nil)
			builder.ht.findNext(nToCheck)
		}

		// Reset each element of headID to 0 to indicate that the probe key has not
		// been found in the build table. Also mark the corresponding indices as
		// head of the linked list.
		for i := uint16(0); i < batchSize; i++ {
			builder.ht.head[builder.ht.headID[i]] = true
			builder.ht.headID[i] = 0
		}

		batchStart = batchEnd
	}
}

// distinctExec executes the entirety of the hash table build phase using the
// source as the build relation. The source operator is entirely consumed in the
// process.
func (builder *hashJoinBuilder) distinctExec(ctx context.Context) {
	for {
		batch := builder.spec.source.Next(ctx)

		if batch.Length() == 0 {
			break
		}

		builder.ht.loadBatch(batch)
	}

	nKeyCols := len(builder.spec.eqCols)
	keyCols := make([]coldata.Vec, nKeyCols)
	for i := 0; i < nKeyCols; i++ {
		keyCols[i] = builder.ht.vals.colVecs[builder.ht.keyCols[i]]
	}

	// builder.ht.next is used to store the computed hash value of each key.
	builder.ht.next = make([]uint64, builder.ht.vals.length+1)
	builder.ht.computeBuckets(ctx, builder.ht.next[1:], keyCols, builder.ht.vals.length, nil)
	builder.ht.buildNextChains(ctx)
}

// hashJoinProber is used by the hashJoinEqOp during the probe phase. It
// operates on a single batch of obtained from the probe relation and probes the
// hashTable to construct the resulting output batch.
type hashJoinProber struct {
	ht *hashTable

	// batch stores the resulting output batch that is constructed and returned
	// for every input batch during the probe phase.
	batch coldata.Batch
	// outputBatchSize specifies the desired length of the output batch which by
	// default is coldata.BatchSize() but can be varied in tests.
	outputBatchSize uint16

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

	// buildColOffset and probeColOffset represent the index in the initial batch
	// to copy the build/probe table columns. This offset depends on whether the
	// build table is the left table.
	buildColOffset int
	probeColOffset int

	// spec holds the specifications for the source operator used in the probe
	// phase.
	spec hashJoinerSourceSpec
	// build holds the source specification for the build table.
	build hashJoinerSourceSpec

	// buildRightSide indicates whether the prober is probing on the left source
	// or the right source.
	buildRightSide bool
	// buildDistinct indicates whether or not the build table equality column
	// tuples are distinct. If they are distinct, performance can be optimized.
	buildDistinct bool

	// prevBatch, if not nil, indicates that the previous probe input batch has
	// not been fully processed.
	prevBatch coldata.Batch
}

func newHashJoinProber(
	allocator *Allocator,
	ht *hashTable,
	probe hashJoinerSourceSpec,
	build hashJoinerSourceSpec,
	buildRightSide bool,
	buildDistinct bool,
	outputBatchSize uint16,
) *hashJoinProber {
	var outColTypes []coltypes.T
	var buildColOffset, probeColOffset int
	if buildRightSide {
		for _, probeOutCol := range probe.outCols {
			outColTypes = append(outColTypes, probe.sourceTypes[probeOutCol])
		}
		for _, buildOutCol := range build.outCols {
			outColTypes = append(outColTypes, build.sourceTypes[buildOutCol])
		}
		buildColOffset = len(probe.outCols)
		probeColOffset = 0
	} else {
		for _, buildOutCol := range build.outCols {
			outColTypes = append(outColTypes, build.sourceTypes[buildOutCol])
		}
		for _, probeOutCol := range probe.outCols {
			outColTypes = append(outColTypes, probe.sourceTypes[probeOutCol])
		}
		buildColOffset = 0
		probeColOffset = len(build.outCols)
	}

	var probeRowUnmatched []bool
	if probe.outer {
		probeRowUnmatched = make([]bool, coldata.BatchSize())
	}

	return &hashJoinProber{
		ht: ht,

		batch:           allocator.NewMemBatch(outColTypes),
		outputBatchSize: outputBatchSize,

		buildIdx: make([]uint64, coldata.BatchSize()),
		probeIdx: make([]uint16, coldata.BatchSize()),

		spec:  probe,
		build: build,

		probeRowUnmatched: probeRowUnmatched,

		buildColOffset: buildColOffset,
		probeColOffset: probeColOffset,

		buildRightSide: buildRightSide,
		buildDistinct:  buildDistinct,
	}
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
// buildDistinct is true if the build table equality columns are distinct. It
// performs the same operation as the exec() function normally would while
// taking a shortcut to improve speed.
func (prober *hashJoinProber) exec(ctx context.Context) {
	prober.batch.SetLength(0)

	if batch := prober.prevBatch; batch != nil {
		// The previous result was bigger than the maximum batch size, so we didn't
		// finish outputting it in the last call to probe. Continue outputting the
		// result from the previous batch.
		prober.prevBatch = nil
		batchSize := batch.Length()
		sel := batch.Selection()

		nResults := prober.collect(batch, batchSize, sel)
		prober.congregate(nResults, batch, batchSize)
	} else {
		for {
			batch := prober.spec.source.Next(ctx)
			batchSize := batch.Length()

			if batchSize == 0 {
				break
			}

			for i, colIdx := range prober.spec.eqCols {
				prober.ht.keys[i] = batch.ColVec(int(colIdx))
			}

			sel := batch.Selection()

			// Initialize groupID with the initial hash buckets and toCheck with all
			// applicable indices.
			prober.ht.lookupInitial(ctx, batchSize, sel)
			nToCheck := batchSize

			var nResults uint16

			if prober.buildDistinct {
				for nToCheck > 0 {
					// Continue searching along the hash table next chains for the corresponding
					// buckets. If the key is found or end of next chain is reached, the key is
					// removed from the toCheck array.
					nToCheck = prober.ht.distinctCheck(nToCheck, sel)
					prober.ht.findNext(nToCheck)
				}

				nResults = prober.distinctCollect(batch, batchSize, sel)
			} else {
				for nToCheck > 0 {
					// Continue searching for the build table matching keys while the toCheck
					// array is non-empty.
					nToCheck = prober.ht.check(nToCheck, sel)
					prober.ht.findNext(nToCheck)
				}

				nResults = prober.collect(batch, batchSize, sel)
			}

			prober.congregate(nResults, batch, batchSize)

			if prober.batch.Length() > 0 {
				break
			}
		}
	}
}

// congregate uses the probeIdx and buildIdx pairs to stitch together the
// resulting join rows and add them to the output batch with the left table
// columns preceding the right table columns.
func (prober *hashJoinProber) congregate(nResults uint16, batch coldata.Batch, batchSize uint16) {
	// If the hash table is empty, then there is nothing to copy. The nulls
	// will be set below.
	if prober.ht.vals.length > 0 {
		outCols := prober.batch.ColVecs()[prober.buildColOffset : prober.buildColOffset+len(prober.ht.outCols)]
		prober.ht.allocator.PerformOperation(outCols, func() {
			for outColIdx, inColIdx := range prober.ht.outCols {
				outCol := outCols[outColIdx]
				valCol := prober.ht.vals.colVecs[inColIdx]
				colType := prober.ht.valTypes[inColIdx]
				// Note that if for some index i, probeRowUnmatched[i] is true, then
				// prober.buildIdx[i] == 0 which will copy the garbage zeroth row of the
				// hash table, but we will set the NULL value below.
				outCol.Copy(
					coldata.CopySliceArgs{
						SliceArgs: coldata.SliceArgs{
							ColType:   colType,
							Src:       valCol,
							SrcEndIdx: uint64(nResults),
						},
						Sel64: prober.buildIdx,
					},
				)
			}
		})
	}
	if prober.spec.outer {
		// Add in the nulls we needed to set for the outer join.
		for outColIdx := range prober.ht.outCols {
			outCol := prober.batch.ColVec(outColIdx + prober.buildColOffset)
			nulls := outCol.Nulls()
			for i, isNull := range prober.probeRowUnmatched {
				if isNull {
					nulls.SetNull(uint16(i))
				}
			}
		}
	}

	outCols := prober.batch.ColVecs()[prober.probeColOffset : prober.probeColOffset+len(prober.spec.outCols)]
	prober.ht.allocator.PerformOperation(outCols, func() {
		for outColIdx, inColIdx := range prober.spec.outCols {
			outCol := outCols[outColIdx]
			valCol := batch.ColVec(int(inColIdx))
			colType := prober.spec.sourceTypes[inColIdx]

			outCol.Copy(
				coldata.CopySliceArgs{
					SliceArgs: coldata.SliceArgs{
						ColType:   colType,
						Src:       valCol,
						Sel:       prober.probeIdx,
						SrcEndIdx: uint64(nResults),
					},
				},
			)
		}
	})

	if prober.build.outer {
		// In order to determine which rows to emit for the outer join on the build
		// table in the end, we need to mark the matched build table rows.
		if prober.spec.outer {
			for i := uint16(0); i < nResults; i++ {
				if !prober.probeRowUnmatched[i] {
					prober.buildRowMatched[prober.buildIdx[i]] = true
				}
			}
		} else {
			for i := uint16(0); i < nResults; i++ {
				prober.buildRowMatched[prober.buildIdx[i]] = true
			}
		}
	}

	prober.batch.SetLength(nResults)
}

// NewEqHashJoinerOp creates a new equality hash join operator on the left and
// right input tables. leftEqCols and rightEqCols specify the equality columns
// while leftOutCols and rightOutCols specifies the output columns. leftTypes
// and rightTypes specify the input column types of the two sources.
func NewEqHashJoinerOp(
	allocator *Allocator,
	leftSource Operator,
	rightSource Operator,
	leftEqCols []uint32,
	rightEqCols []uint32,
	leftOutCols []uint32,
	rightOutCols []uint32,
	leftTypes []coltypes.T,
	rightTypes []coltypes.T,
	buildRightSide bool,
	buildDistinct bool,
	joinType sqlbase.JoinType,
) (Operator, error) {
	var leftOuter, rightOuter bool
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
		buildRightSide = true
		buildDistinct = true
		if len(rightOutCols) != 0 {
			return nil, errors.Errorf("semi-join can't have right-side output columns")
		}
	default:
		return nil, errors.Errorf("hash join of type %s not supported", joinType)
	}

	spec := hashJoinerSpec{
		left: hashJoinerSourceSpec{
			eqCols:      leftEqCols,
			outCols:     leftOutCols,
			sourceTypes: leftTypes,
			source:      leftSource,
			outer:       leftOuter,
		},

		right: hashJoinerSourceSpec{
			eqCols:      rightEqCols,
			outCols:     rightOutCols,
			sourceTypes: rightTypes,
			source:      rightSource,
			outer:       rightOuter,
		},

		buildRightSide: buildRightSide,
		buildDistinct:  buildDistinct,
	}

	return &hashJoinEqOp{
		allocator:       allocator,
		spec:            spec,
		outputBatchSize: coldata.BatchSize(),
	}, nil
}
