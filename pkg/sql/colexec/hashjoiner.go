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

// todo(changangela): support rehashing instead of large fixed bucket size
const hashTableBucketSize = 1 << 16

// hashJoinerState represents the state of the processor.
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
//    to ensure that each batch is at most col.BatchSize().
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

func (hj *hashJoinEqOp) ChildCount() int {
	return 2
}

func (hj *hashJoinEqOp) Child(nth int) execinfra.OpNode {
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

	hj.ht = makeHashTable(
		hj.allocator,
		hashTableBucketSize,
		build.sourceTypes,
		build.eqCols,
		build.outCols,
		false, /* allowNullEquality */
	)

	hj.builder = makeHashJoinBuilder(
		hj.ht,
		build,
	)

	hj.prober = makeHashJoinProber(
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
		hj.ht.same = make([]uint64, hj.ht.size+1)
		hj.ht.allocateVisited()
	}

	if hj.builder.spec.outer {
		hj.prober.buildRowMatched = make([]bool, hj.ht.size)
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

	for nResults < hj.outputBatchSize && hj.emittingUnmatchedState.rowIdx < hj.ht.size {
		if !hj.prober.buildRowMatched[hj.emittingUnmatchedState.rowIdx] {
			hj.prober.buildIdx[nResults] = hj.emittingUnmatchedState.rowIdx
			nResults++
		}
		hj.emittingUnmatchedState.rowIdx++
	}

	for outColIdx, inColIdx := range hj.ht.outCols {
		outCol := hj.prober.batch.ColVec(outColIdx + hj.prober.buildColOffset)
		valCol := hj.ht.vals[inColIdx]
		colType := hj.ht.valTypes[inColIdx]

		hj.allocator.Copy(
			outCol,
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

	hj.prober.batch.SetLength(nResults)
}

// hashTable is a structure used by the hash joiner to store the build table
// batches. Keys are stored according to the encoding of the equality column,
// which point to the corresponding output keyID. The keyID is calculated
// using the below equation:
//
// keyID = keys.indexOf(key) + 1
//
// and inversely:
//
// keys[keyID - 1] = key
//
// The table can then be probed in column batches to find at most one matching
// row per column batch row.
type hashTable struct {
	allocator *Allocator
	// first stores the keyID of the first key that resides in each bucket. This
	// keyID is used to determine the corresponding equality column key as well
	// as output column values.
	first []uint64

	// next is a densely-packed list that stores the keyID of the next key in the
	// hash table bucket chain, where an id of 0 is reserved to represent end of
	// chain.
	next []uint64

	// same and visited are only used when the hashTable contains non-distinct
	// keys.
	//
	// same is a densely-packed list that stores the keyID of the next key in the
	// hash table that has the same value as the current key. The headID of the key
	// is the first key of that value found in the next linked list. This field
	// will be lazily populated by the prober.
	same []uint64
	// visited represents whether each of the corresponding keys have been touched
	// by the prober.
	visited []bool

	// head indicates whether each of the corresponding keys is the head of the
	// linked list at same. This would allow us to eliminate keys that are not the
	// head and thereby should not be traversed.
	head []bool

	// vals stores the union of the equality and output columns of the build
	// table. A key tuple is defined as the elements in each row of vals that
	// makes up the equality columns. The ID of a key at any index of vals is
	// index + 1.
	vals []coldata.Vec
	// valTypes stores the corresponding types of the val columns.
	valTypes []coltypes.T
	// valCols stores the union of the keyCols and outCols.
	valCols []uint32
	// keyCols stores the indices of vals which are key columns.
	keyCols []uint32

	// outCols stores the indices of vals which are output columns.
	outCols []uint32
	// outTypes stores the types of the output columns.
	outTypes []coltypes.T

	// size returns the total number of tuples the hashTable currently stores.
	size uint64
	// bucketSize returns the number of buckets the hashTable employs. This is
	// equivalent to the size of first.
	bucketSize uint64

	// keys stores the equality columns on the probe table for a single batch.
	keys []coldata.Vec
	// buckets is used to store the computed hash value of each key in a single
	// batch.
	buckets []uint64

	// groupID stores the keyID that maps to the joining rows of the build table.
	// The ith element of groupID stores the keyID of the build table that
	// corresponds to the ith key in the probe table.
	groupID []uint64
	// toCheck stores the indices of the eqCol rows that have yet to be found or
	// rejected.
	toCheck []uint16

	// headID stores the first build table keyID that matched with the probe batch
	// key at any given index.
	headID []uint64

	// differs stores whether the key at any index differs with the build table
	// key.
	differs []bool

	// allowNullEquality determines if NULL keys should be treated as equal to
	// each other.
	allowNullEquality bool

	cancelChecker CancelChecker
}

func makeHashTable(
	allocator *Allocator,
	bucketSize uint64,
	sourceTypes []coltypes.T,
	eqCols []uint32,
	outCols []uint32,
	allowNullEquality bool,
) *hashTable {
	// Compute the union of eqCols and outCols and compress vals to only keep the
	// important columns.
	nCols := len(sourceTypes)
	keepCol := make([]bool, nCols)
	compressed := make([]uint32, nCols)

	for _, colIdx := range eqCols {
		keepCol[colIdx] = true
	}

	for _, colIdx := range outCols {
		keepCol[colIdx] = true
	}

	// Extract the important columns and discard the rest.
	cols := make([]coldata.Vec, 0, nCols)
	nKeep := uint32(0)

	keepTypes := make([]coltypes.T, 0, nCols)
	keepCols := make([]uint32, 0, nCols)

	for i := 0; i < nCols; i++ {
		if keepCol[i] {
			cols = append(cols, allocator.NewMemColumn(sourceTypes[i], 0))
			keepTypes = append(keepTypes, sourceTypes[i])
			keepCols = append(keepCols, uint32(i))

			compressed[i] = nKeep
			nKeep++
		}
	}

	// Extract and types and indices of the	 eqCols and outCols.
	nKeys := len(eqCols)
	keyTypes := make([]coltypes.T, nKeys)
	keys := make([]uint32, nKeys)
	for i, colIdx := range eqCols {
		keyTypes[i] = sourceTypes[colIdx]
		keys[i] = compressed[colIdx]
	}

	nOutCols := len(outCols)
	outTypes := make([]coltypes.T, nOutCols)
	outs := make([]uint32, nOutCols)
	for i, colIdx := range outCols {
		outTypes[i] = sourceTypes[colIdx]
		outs[i] = compressed[colIdx]
	}

	return &hashTable{
		allocator: allocator,
		first:     make([]uint64, bucketSize),

		vals:     cols,
		valTypes: keepTypes,
		valCols:  keepCols,
		keyCols:  keys,
		outCols:  outs,
		outTypes: outTypes,

		bucketSize: bucketSize,

		groupID: make([]uint64, coldata.BatchSize()),
		toCheck: make([]uint16, coldata.BatchSize()),
		differs: make([]bool, coldata.BatchSize()),

		headID: make([]uint64, coldata.BatchSize()),

		keys:    make([]coldata.Vec, len(eqCols)),
		buckets: make([]uint64, coldata.BatchSize()),

		allowNullEquality: allowNullEquality,
	}
}

// loadBatch appends a new batch of keys and outputs to the existing keys and
// output columns.
func (ht *hashTable) loadBatch(batch coldata.Batch) {
	batchSize := batch.Length()
	for i, colIdx := range ht.valCols {
		ht.allocator.Append(
			ht.vals[i],
			coldata.SliceArgs{
				ColType:   ht.valTypes[i],
				Src:       batch.ColVec(int(colIdx)),
				Sel:       batch.Selection(),
				DestIdx:   ht.size,
				SrcEndIdx: uint64(batchSize),
			},
		)
	}

	ht.size += uint64(batchSize)
}

// initHash, rehash, and finalizeHash work together to compute the hash value
// for an individual key tuple which represents a row's equality columns. Since this
// key is a tuple of various types, rehash is used to apply a transformation on
// the resulting hash value based on an element of the key of a specified type.
//
// We currently use the same hash functions used by go's maps.
// TODO(asubiotto): Once https://go-review.googlesource.com/c/go/+/155118/ is
// in, we should use the public API.

// rehash takes an element of a key (tuple representing a row of equality
// column values) at a given column and computes a new hash by applying a
// transformation to the existing hash. This function is generated by execgen,
// so it doesn't appear in this file. Look at hashjoiner_tmpl.go for the source
// code.
//
// initHash initializes the hash value of each key to its initial state for
// rehashing purposes.
func (ht *hashTable) initHash(buckets []uint64, nKeys uint64) {
	for i := uint64(0); i < nKeys; i++ {
		buckets[i] = 1
	}
}

// finalizeHash takes each key's hash value and applies a final transformation
// onto it so that it fits within the hashTable's bucket size.
func (ht *hashTable) finalizeHash(buckets []uint64, nKeys uint64) {
	for i := uint64(0); i < nKeys; i++ {
		// Since bucketSize is a power of 2, modulo bucketSize could be optimized
		// into a bitwise operation which improves benchmark performance by 20%.
		// In effect, the following code is equivalent to (but faster than):
		// buckets[i] = buckets[i] % ht.bucketSize
		buckets[i] = buckets[i] & (ht.bucketSize - 1)
	}
}

// computeBuckets computes the hash value of each key and stores the result in
// buckets.
func (ht *hashTable) computeBuckets(
	ctx context.Context, buckets []uint64, keys []coldata.Vec, nKeys uint64, sel []uint16,
) {
	ht.initHash(buckets, nKeys)

	if nKeys == 0 {
		// No work to do - avoid doing the loops below.
		return
	}

	for i, k := range ht.keyCols {
		ht.rehash(ctx, buckets, i, ht.valTypes[k], keys[i], nKeys, sel)
	}

	ht.finalizeHash(buckets, nKeys)
}

// buildNextChains builds the hash map from the computed hash values.
func (ht *hashTable) buildNextChains(ctx context.Context) {
	for id := uint64(1); id <= ht.size; id++ {
		ht.cancelChecker.check(ctx)
		// keyID is stored into corresponding hash bucket at the front of the next
		// chain.
		hash := ht.next[id]
		ht.next[id] = ht.first[hash]
		ht.first[hash] = id
	}
}

// allocateVisited allocates the visited array in the hashTable.
func (ht *hashTable) allocateVisited() {
	ht.visited = make([]bool, ht.size+1)

	// Since keyID = 0 is reserved for end of list, it can be marked as visited
	// at the beginning.
	ht.visited[0] = true
}

// hashJoinBuilder is used by the hashJoiner during the build phase. It
// pre-loads all batches from the build relation before building the hash table.
type hashJoinBuilder struct {
	ht *hashTable

	// spec holds the specifications for the source operator used in the build
	// phase.
	spec hashJoinerSourceSpec
}

func makeHashJoinBuilder(ht *hashTable, spec hashJoinerSourceSpec) *hashJoinBuilder {
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

	builder.ht.same = make([]uint64, builder.ht.size+1)
	builder.ht.visited = make([]bool, builder.ht.size+1)
	builder.ht.head = make([]bool, builder.ht.size+1)

	// Since keyID = 0 is reserved for end of list, it can be marked as visited
	// at the beginning.
	builder.ht.visited[0] = true

	nKeyCols := len(builder.spec.eqCols)
	batchStart := uint64(0)
	for batchStart < builder.ht.size {
		batchEnd := batchStart + uint64(coldata.BatchSize())
		if batchEnd > builder.ht.size {
			batchEnd = builder.ht.size
		}

		batchSize := uint16(batchEnd - batchStart)

		for i := 0; i < nKeyCols; i++ {
			builder.ht.keys[i] = builder.ht.vals[builder.ht.keyCols[i]].Slice(builder.ht.valTypes[builder.ht.keyCols[i]], batchStart, batchEnd)
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
		keyCols[i] = builder.ht.vals[builder.ht.keyCols[i]]
	}

	// builder.ht.next is used to store the computed hash value of each key.
	builder.ht.next = make([]uint64, builder.ht.size+1)
	builder.ht.computeBuckets(ctx, builder.ht.next[1:], keyCols, builder.ht.size, nil)
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

func makeHashJoinProber(
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

// lookupInitial finds the corresponding hash table buckets for the equality
// column of the batch and stores the results in groupID. It also initializes
// toCheck with all indices in the range [0, batchSize).
func (ht *hashTable) lookupInitial(ctx context.Context, batchSize uint16, sel []uint16) {
	ht.computeBuckets(ctx, ht.buckets, ht.keys, uint64(batchSize), sel)
	for i := uint16(0); i < batchSize; i++ {
		ht.groupID[i] = ht.first[ht.buckets[i]]
		ht.toCheck[i] = i
	}
}

// findNext determines the id of the next key inside the groupID buckets for
// each equality column key in toCheck.
func (ht *hashTable) findNext(nToCheck uint16) {
	for i := uint16(0); i < nToCheck; i++ {
		ht.groupID[ht.toCheck[i]] = ht.next[ht.groupID[ht.toCheck[i]]]
	}
}

// checkCols performs a column by column checkCol on the key columns.
func (ht *hashTable) checkCols(nToCheck uint16, sel []uint16) {
	for i, k := range ht.keyCols {
		ht.checkCol(ht.valTypes[k], i, nToCheck, sel)
	}
}

// check performs an equality check between the current key in the groupID bucket
// and the probe key at that index. If there is a match, the hashTable's same
// array is updated to lazily populate the linked list of identical build
// table keys. The visited flag for corresponding build table key is also set. A
// key is removed from toCheck if it has already been visited in a previous
// probe, or the bucket has reached the end (key not found in build table). The
// new length of toCheck is returned by this function.
func (ht *hashTable) check(nToCheck uint16, sel []uint16) uint16 {
	ht.checkCols(nToCheck, sel)
	nDiffers := uint16(0)
	for i := uint16(0); i < nToCheck; i++ {
		if !ht.differs[ht.toCheck[i]] {
			// If the current key matches with the probe key, we want to update headID
			// with the current key if it has not been set yet.
			keyID := ht.groupID[ht.toCheck[i]]
			if ht.headID[ht.toCheck[i]] == 0 {
				ht.headID[ht.toCheck[i]] = keyID
			}
			firstID := ht.headID[ht.toCheck[i]]

			if !ht.visited[keyID] {
				// We can then add this keyID into the same array at the end of the
				// corresponding linked list and mark this ID as visited. Since there
				// can be multiple keys that match this probe key, we want to mark
				// differs at this position to be true. This way, the prober will
				// continue probing for this key until it reaches the end of the next
				// chain.
				ht.differs[ht.toCheck[i]] = true
				ht.visited[keyID] = true

				if firstID != keyID {
					ht.same[keyID] = ht.same[firstID]
					ht.same[firstID] = keyID
				}
			}
		}

		if ht.differs[ht.toCheck[i]] {
			// Continue probing in this next chain for the probe key.
			ht.differs[ht.toCheck[i]] = false
			ht.toCheck[nDiffers] = ht.toCheck[i]
			nDiffers++
		}
	}

	return nDiffers
}

// congregate uses the probeIdx and buildIdx pairs to stitch together the
// resulting join rows and add them to the output batch with the left table
// columns preceding the right table columns.
func (prober *hashJoinProber) congregate(nResults uint16, batch coldata.Batch, batchSize uint16) {
	for outColIdx, inColIdx := range prober.ht.outCols {
		outCol := prober.batch.ColVec(outColIdx + prober.buildColOffset)
		valCol := prober.ht.vals[inColIdx]
		colType := prober.ht.valTypes[inColIdx]

		// If the hash table is empty, then there is nothing to copy. The nulls
		// will be set below.
		if prober.ht.size > 0 {
			// Note that if for some index i, probeRowUnmatched[i] is true, then
			// prober.buildIdx[i] == 0 which will copy the garbage zeroth row of the
			// hash table, but we will set the NULL value below.
			prober.ht.allocator.Copy(
				outCol,
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
		if prober.spec.outer {
			// Add in the nulls we needed to set for the outer join.
			nulls := outCol.Nulls()
			for i, isNull := range prober.probeRowUnmatched {
				if isNull {
					nulls.SetNull(uint16(i))
				}
			}
		}
	}

	for outColIdx, inColIdx := range prober.spec.outCols {
		outCol := prober.batch.ColVec(outColIdx + prober.probeColOffset)
		valCol := batch.ColVec(int(inColIdx))
		colType := prober.spec.sourceTypes[inColIdx]

		prober.ht.allocator.Copy(
			outCol,
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

// distinctCheck determines if the current key in the groupID buckets matches the
// equality column key. If there is a match, then the key is removed from
// toCheck. If the bucket has reached the end, the key is rejected. The toCheck
// list is reconstructed to only hold the indices of the eqCol keys that have
// not been found. The new length of toCheck is returned by this function.
func (ht *hashTable) distinctCheck(nToCheck uint16, sel []uint16) uint16 {
	ht.checkCols(nToCheck, sel)

	// Select the indices that differ and put them into toCheck.
	nDiffers := uint16(0)
	for i := uint16(0); i < nToCheck; i++ {
		if ht.differs[ht.toCheck[i]] {
			ht.differs[ht.toCheck[i]] = false
			ht.toCheck[nDiffers] = ht.toCheck[i]
			nDiffers++
		}
	}

	return nDiffers
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
