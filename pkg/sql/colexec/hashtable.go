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
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execgen"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// hashTableBuildMode represents different modes in which the hashTable can be
// built.
type hashTableBuildMode int

const (
	// hashTableFullBuildMode is the mode where hashTable buffers all input
	// tuples and populates first and next arrays for each hash bucket.
	hashTableFullBuildMode hashTableBuildMode = iota

	// hashTableDistinctBuildMode is the mode where hashTable only buffers
	// distinct tuples and discards the duplicates. In this mode the hash table
	// actually stores only the equality columns, so the columns with positions
	// not present in eqCols will remain zero-capacity vectors in vals.
	hashTableDistinctBuildMode
)

// hashTableProbeMode represents different modes of probing the hashTable.
type hashTableProbeMode int

const (
	// hashTableDefaultProbeMode is the default probing mode of the hashTable.
	hashTableDefaultProbeMode hashTableProbeMode = iota

	// hashTableDeletingProbeMode is the mode of probing the hashTable in which
	// it "deletes" the tuples from itself once they are matched against
	// probing tuples.
	// For example, if we have a hashTable consisting of tuples {1, 1}, {1, 2},
	// {2, 3}, and the probing tuples are {1, 4}, {1, 5}, {1, 6}, then we get
	// the following when probing on the first column only:
	//   {1, 4} -> {1, 1}   | hashTable = {1, 2}, {2, 3}
	//   {1, 5} -> {1, 2}   | hashTable = {2, 3}
	//   {1, 6} -> no match | hashTable = {2, 3}
	// Note that the output of such probing is not fully deterministic when
	// tuples contain non-equality columns.
	hashTableDeletingProbeMode
)

// hashTableBuildBuffer stores the information related to the build table.
type hashTableBuildBuffer struct {
	// first stores the first keyID of the key that resides in each bucket.
	// This keyID is used to determine the corresponding equality column key as
	// well as output column values.
	first []uint64

	// next is a densely-packed list that stores the keyID of the next key in the
	// hash table bucket chain, where an id of 0 is reserved to represent end of
	// chain.
	next []uint64
}

// hashTableProbeBuffer stores the information related to the probe table.
type hashTableProbeBuffer struct {
	// first stores the first keyID of the key that resides in each bucket.
	// This keyID is used to determine the corresponding equality column key as
	// well as output column values.
	first []uint64

	// next is a densely-packed list that stores the keyID of the next key in the
	// hash table bucket chain, where an id of 0 is reserved to represent end of
	// chain.
	next []uint64

	// limitedSlicesAreAccountedFor indicates whether we have already
	// accounted for the memory used by the slices below.
	limitedSlicesAreAccountedFor bool

	///////////////////////////////////////////////////////////////
	// Slices below are allocated dynamically but are limited by //
	// coldata.BatchSize() in size.                              //
	///////////////////////////////////////////////////////////////

	// headID stores the first build table keyID that matched with the probe batch
	// key at any given index.
	headID []uint64

	// differs stores whether the key at any index differs with the build table
	// key.
	differs []bool

	// distinct stores whether the key in the probe batch is distinct in the build
	// table.
	distinct []bool

	// groupID stores the keyID that maps to the joining rows of the build table.
	// The ith element of groupID stores the keyID of the build table that
	// corresponds to the ith key in the probe table.
	groupID []uint64

	// toCheck stores the indices of the eqCol rows that have yet to be found or
	// rejected.
	toCheck []uint64

	// hashBuffer stores the hash values of each tuple in the probe table. It will
	// be dynamically updated when the hashTable is build in distinct mode.
	hashBuffer []uint64
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
	allocator *colmem.Allocator

	// unlimitedSlicesNumUint64AccountedFor stores the number of uint64 from
	// the unlimited slices that we have already accounted for.
	unlimitedSlicesNumUint64AccountedFor int64

	// buildScratch contains the scratch buffers required for the build table.
	buildScratch hashTableBuildBuffer

	// probeScratch contains the scratch buffers required for the probe table.
	probeScratch hashTableProbeBuffer

	// keys is a scratch space that stores the equality columns (either of
	// probe or build table, depending on the phase of algorithm).
	keys []coldata.Vec

	// same and visited are only used when the hashTable contains non-distinct
	// keys (in hashTableFullBuildMode mode).
	//
	// same is a densely-packed list that stores the keyID of the next key in the
	// hash table that has the same value as the current key. The headID of the key
	// is the first key of that value found in the next linked list. This field
	// will be lazily populated by the prober.
	same []uint64
	// visited represents whether each of the corresponding keys have been touched
	// by the prober.
	visited []bool

	// vals stores columns of the build source that are specified in
	// colsToStore in the constructor. The ID of a tuple at any index of vals
	// is index + 1.
	vals *appendOnlyBufferedBatch
	// keyCols stores the indices of vals which are key columns.
	keyCols []uint32

	// numBuckets returns the number of buckets the hashTable employs. This is
	// equivalent to the size of first.
	numBuckets uint64
	// loadFactor determines the average number of tuples per bucket exceeding
	// of which will trigger resizing the hash table.
	loadFactor float64

	// allowNullEquality determines if NULL keys should be treated as equal to
	// each other.
	allowNullEquality bool

	overloadHelper execgen.OverloadHelper
	datumAlloc     rowenc.DatumAlloc
	cancelChecker  CancelChecker

	buildMode hashTableBuildMode
	probeMode hashTableProbeMode
}

var _ resetter = &hashTable{}

// newHashTable returns a new hashTable.
//
// - loadFactor determines the average number of tuples per bucket which, if
// exceeded, will trigger resizing the hash table. This number can have a
// noticeable effect on the performance, so every user of the hash table should
// choose the number that works well for the corresponding use case. 1.0 could
// be used as the initial default value, and most likely the best value will be
// in [0.1, 10.0] range.
//
// - initialNumHashBuckets determines the number of buckets allocated initially.
// When the current load factor of the hash table exceeds the loadFactor, the
// hash table is resized by doubling the number of buckets. The user of the hash
// table should choose this number based on the amount of its other allocations,
// but it is likely should be in [8, coldata.BatchSize()] range.
// The thinking process for choosing coldata.BatchSize() could be roughly as
// follows:
// - on one hand, if we make several other allocations that have to be at
// least coldata.BatchSize() in size, then we don't win much in the case of
// the input with small number of tuples;
// - on the other hand, if we start out with a larger number, we won't be
// using the vast of majority of the buckets on the input with small number
// of tuples (a downside) while not gaining much in the case of the input
// with large number of tuples.
func newHashTable(
	allocator *colmem.Allocator,
	loadFactor float64,
	initialNumHashBuckets uint64,
	sourceTypes []*types.T,
	eqCols []uint32,
	allowNullEquality bool,
	buildMode hashTableBuildMode,
	probeMode hashTableProbeMode,
) *hashTable {
	if !allowNullEquality && probeMode == hashTableDeletingProbeMode {
		// At the moment, we don't have a use case for such behavior, so let's
		// assert that it is not requested.
		colexecerror.InternalError(errors.AssertionFailedf("hashTableDeletingProbeMode is supported only when null equality is allowed"))
	}
	// Note that we don't perform memory accounting of the internal memory here
	// and delay it till buildFromBufferedTuples in order to appease *-disk
	// logic test configs (our disk-spilling infrastructure doesn't know how to
	// fallback to disk when a memory limit is hit in the constructor methods
	// of the operators or in Init() implementations).

	// colsToStore indicates the positions of columns to actually store in the
	// hash table depending on the build mode:
	// - all columns are stored in the full build mode
	// - only columns with indices in eqCols are stored in the distinct build
	// mode (columns with other indices will remain zero-capacity vectors in
	// vals).
	var colsToStore []int
	switch buildMode {
	case hashTableFullBuildMode:
		colsToStore = make([]int, len(sourceTypes))
		for i := range colsToStore {
			colsToStore[i] = i
		}
	case hashTableDistinctBuildMode:
		colsToStore = make([]int, len(eqCols))
		for i := range colsToStore {
			colsToStore[i] = int(eqCols[i])
		}
	default:
		colexecerror.InternalError(errors.AssertionFailedf("unknown hashTableBuildMode %d", buildMode))
	}
	ht := &hashTable{
		allocator: allocator,
		buildScratch: hashTableBuildBuffer{
			first: make([]uint64, initialNumHashBuckets),
		},
		keys:              make([]coldata.Vec, len(eqCols)),
		vals:              newAppendOnlyBufferedBatch(allocator, sourceTypes, colsToStore),
		keyCols:           eqCols,
		numBuckets:        initialNumHashBuckets,
		loadFactor:        loadFactor,
		allowNullEquality: allowNullEquality,
		buildMode:         buildMode,
		probeMode:         probeMode,
	}

	if buildMode == hashTableDistinctBuildMode {
		ht.probeScratch.first = make([]uint64, initialNumHashBuckets)
		// ht.buildScratch.next will be populated dynamically by appending to
		// it, but we need to make sure that the special keyID=0 (which
		// indicates the end of the hash chain) is always present.
		ht.buildScratch.next = []uint64{0}
	}

	return ht
}

// hashTableInitialToCheck is a slice that contains all consequent integers in
// [0, coldata.MaxBatchSize) range that can be used to initialize toCheck buffer
// for most of the join types.
var hashTableInitialToCheck []uint64

func init() {
	hashTableInitialToCheck = make([]uint64, coldata.MaxBatchSize)
	for i := range hashTableInitialToCheck {
		hashTableInitialToCheck[i] = uint64(i)
	}
}

// shouldResize returns whether the hash table storing numTuples should be
// resized in order to not exceed the load factor given the current number of
// buckets.
func (ht *hashTable) shouldResize(numTuples int) bool {
	return float64(numTuples)/float64(ht.numBuckets) > ht.loadFactor
}

const sizeOfUint64 = int64(unsafe.Sizeof(uint64(0)))

// accountForLimitedSlices checks whether we have already accounted for the
// memory used by the slices that are limited by coldata.BatchSize() in size
// and adjusts the allocator accordingly if we haven't.
func (p *hashTableProbeBuffer) accountForLimitedSlices(allocator *colmem.Allocator) {
	if p.limitedSlicesAreAccountedFor {
		return
	}
	const sizeOfBool = int64(unsafe.Sizeof(true))
	internalMemMaxUsed := sizeOfUint64*int64(5*coldata.BatchSize()) + sizeOfBool*int64(2*coldata.BatchSize())
	allocator.AdjustMemoryUsage(internalMemMaxUsed)
	p.limitedSlicesAreAccountedFor = true
}

// buildFromBufferedTuples builds the hash table from already buffered tuples
// in ht.vals. It'll determine the appropriate number of buckets that satisfy
// the target load factor.
func (ht *hashTable) buildFromBufferedTuples(ctx context.Context) {
	for ht.shouldResize(ht.vals.Length()) {
		ht.numBuckets *= 2
	}
	ht.buildScratch.first = maybeAllocateUint64Array(ht.buildScratch.first, int(ht.numBuckets))
	if ht.probeScratch.first != nil {
		ht.probeScratch.first = maybeAllocateUint64Array(ht.probeScratch.first, int(ht.numBuckets))
	}
	for i, keyCol := range ht.keyCols {
		ht.keys[i] = ht.vals.ColVec(int(keyCol))
	}
	// ht.buildScratch.next is used to store the computed hash value of each key.
	ht.buildScratch.next = maybeAllocateUint64Array(ht.buildScratch.next, ht.vals.Length()+1)
	ht.computeBuckets(ctx, ht.buildScratch.next[1:], ht.keys, ht.vals.Length(), nil /* sel */)
	ht.buildNextChains(ctx, ht.buildScratch.first, ht.buildScratch.next, 1, uint64(ht.vals.Length()))
	// Account for memory used by the internal auxiliary slices that are
	// limited in size.
	ht.probeScratch.accountForLimitedSlices(ht.allocator)
	// Note that if ht.probeScratch.first is nil, it'll have zero capacity.
	newUint64Count := int64(cap(ht.buildScratch.first) + cap(ht.probeScratch.first) + cap(ht.buildScratch.next))
	ht.allocator.AdjustMemoryUsage(sizeOfUint64 * (newUint64Count - ht.unlimitedSlicesNumUint64AccountedFor))
	ht.unlimitedSlicesNumUint64AccountedFor = newUint64Count
}

// fullBuild executes the entirety of the hash table build phase using the input
// as the build source. The input is entirely consumed in the process. Note that
// the hash table is assumed to operate in hashTableFullBuildMode.
func (ht *hashTable) fullBuild(ctx context.Context, input colexecbase.Operator) {
	if ht.buildMode != hashTableFullBuildMode {
		colexecerror.InternalError(errors.AssertionFailedf(
			"hashTable.fullBuild is called in unexpected build mode %d", ht.buildMode,
		))
	}
	// We're using the hash table with the full build mode in which we will
	// fully buffer all tuples from the input first and only then we'll build
	// the hash table. Such approach allows us to compute the desired number of
	// hash buckets for the target load factor (this is done in
	// buildFromBufferedTuples()).
	for {
		batch := input.Next(ctx)
		if batch.Length() == 0 {
			break
		}
		ht.allocator.PerformOperation(ht.vals.ColVecs(), func() {
			ht.vals.append(batch, 0 /* startIdx */, batch.Length())
		})
	}
	ht.buildFromBufferedTuples(ctx)
}

// distinctBuild appends all distinct tuples from batch to the hash table. Note
// that the hash table is assumed to operate in hashTableDistinctBuildMode.
// batch is updated to include only the distinct tuples.
func (ht *hashTable) distinctBuild(ctx context.Context, batch coldata.Batch) {
	if ht.buildMode != hashTableDistinctBuildMode {
		colexecerror.InternalError(errors.AssertionFailedf(
			"hashTable.distinctBuild is called in unexpected build mode %d", ht.buildMode,
		))
	}
	ht.computeHashAndBuildChains(ctx, batch)
	ht.removeDuplicates(batch, ht.keys, ht.probeScratch.first, ht.probeScratch.next, ht.checkProbeForDistinct)
	// We only check duplicates when there is at least one buffered tuple.
	if ht.vals.Length() > 0 {
		ht.removeDuplicates(batch, ht.keys, ht.buildScratch.first, ht.buildScratch.next, ht.checkBuildForDistinct)
	}
	if batch.Length() > 0 {
		ht.appendAllDistinct(ctx, batch)
	}
}

// computeHashAndBuildChains computes the hash codes of the tuples in batch and
// then builds 'next' chains between those tuples. The goal is to separate all
// tuples in batch into singly linked lists containing only tuples with the
// same hash code. Those 'next' chains are stored in ht.probeScratch.next.
func (ht *hashTable) computeHashAndBuildChains(ctx context.Context, batch coldata.Batch) {
	srcVecs := batch.ColVecs()
	for i, keyCol := range ht.keyCols {
		ht.keys[i] = srcVecs[keyCol]
	}

	batchLength := batch.Length()
	if cap(ht.probeScratch.next) < batchLength+1 {
		ht.probeScratch.next = make([]uint64, batchLength+1)
	}
	ht.computeBuckets(ctx, ht.probeScratch.next[1:batchLength+1], ht.keys, batchLength, batch.Selection())
	ht.probeScratch.hashBuffer = append(ht.probeScratch.hashBuffer[:0], ht.probeScratch.next[1:batchLength+1]...)

	// We need to zero out 'first' buffer for all hash codes present in
	// hashBuffer, and there are two possible approaches that we choose from
	// based on a heuristic - we can either iterate over all hash codes and
	// zero out only the relevant elements (beneficial when 'first' buffer is
	// at least batchLength in size) or zero out the whole 'first' buffer
	// (beneficial otherwise).
	if batchLength < len(ht.probeScratch.first) {
		for _, hash := range ht.probeScratch.hashBuffer[:batchLength] {
			ht.probeScratch.first[hash] = 0
		}
	} else {
		for n := 0; n < len(ht.probeScratch.first); n += copy(ht.probeScratch.first[n:], zeroUint64Column) {
		}
	}

	ht.buildNextChains(ctx, ht.probeScratch.first, ht.probeScratch.next, 1 /* offset */, uint64(batchLength))
}

// findBuckets finds the buckets for all tuples in batch when probing against a
// hash table that is specified by 'first' and 'next' vectors as well as
// 'duplicatesChecker'. `duplicatesChecker` takes a slice of key columns of the
// batch, number of tuples to check, and the selection vector of the batch, and
// it returns number of tuples that needs to be checked for next iteration.
// The "buckets" are specified by equal values in ht.probeScratch.headID.
// NOTE: *first* and *next* vectors should be properly populated.
// NOTE: batch is assumed to be non-zero length.
func (ht *hashTable) findBuckets(
	batch coldata.Batch,
	keyCols []coldata.Vec,
	first, next []uint64,
	duplicatesChecker func([]coldata.Vec, uint64, []int) uint64,
) {
	batchLength := batch.Length()
	sel := batch.Selection()

	ht.probeScratch.setupLimitedSlices(batchLength, ht.buildMode)
	// Early bounds checks.
	groupIDs := ht.probeScratch.groupID
	_ = groupIDs[batchLength-1]
	for i, hash := range ht.probeScratch.hashBuffer[:batchLength] {
		f := first[hash]
		//gcassert:bce
		groupIDs[i] = f
	}
	copy(ht.probeScratch.toCheck, hashTableInitialToCheck[:batchLength])

	for nToCheck := uint64(batchLength); nToCheck > 0; {
		// Continue searching for the build table matching keys while the toCheck
		// array is non-empty.
		nToCheck = duplicatesChecker(keyCols, nToCheck, sel)
		ht.findNext(next, nToCheck)
	}
}

// removeDuplicates updates the selection vector of the batch to only include
// distinct tuples when probing against a hash table specified by 'first' and
// 'next' vectors as well as 'duplicatesChecker'.
// NOTE: *first* and *next* vectors should be properly populated.
func (ht *hashTable) removeDuplicates(
	batch coldata.Batch,
	keyCols []coldata.Vec,
	first, next []uint64,
	duplicatesChecker func([]coldata.Vec, uint64, []int) uint64,
) {
	ht.findBuckets(batch, keyCols, first, next, duplicatesChecker)
	ht.updateSel(batch)
}

// appendAllDistinct appends all tuples from batch to the hash table. It
// assumes that all tuples are distinct and that ht.probeScratch.hashBuffer
// contains the hash codes for all of them.
// NOTE: batch must be of non-zero length.
func (ht *hashTable) appendAllDistinct(ctx context.Context, batch coldata.Batch) {
	numBuffered := uint64(ht.vals.Length())
	ht.allocator.PerformOperation(ht.vals.ColVecs(), func() {
		ht.vals.append(batch, 0 /* startIdx */, batch.Length())
	})
	ht.buildScratch.next = append(ht.buildScratch.next, ht.probeScratch.hashBuffer[:batch.Length()]...)
	ht.buildNextChains(ctx, ht.buildScratch.first, ht.buildScratch.next, numBuffered+1, uint64(batch.Length()))
	if ht.shouldResize(ht.vals.Length()) {
		ht.buildFromBufferedTuples(ctx)
	}
}

// maybeRepairAfterDistinctBuild checks whether the hash table built via
// distinctBuild is in an inconsistent state and repairs it if so.
func (ht *hashTable) maybeRepairAfterDistinctBuild(ctx context.Context) {
	// buildScratch.next has an extra 0th element not used by the tuples
	// reserved for the end of the chain.
	if len(ht.buildScratch.next) < ht.vals.Length()+1 {
		// The hash table in such a state that some distinct tuples were
		// appended to ht.vals, but 'next' and 'first' slices were not updated
		// accordingly.
		numConsistentTuples := len(ht.buildScratch.next) - 1
		lastBatchNumDistinctTuples := ht.vals.Length() - numConsistentTuples
		ht.buildScratch.next = append(ht.buildScratch.next, ht.probeScratch.hashBuffer[:lastBatchNumDistinctTuples]...)
		ht.buildNextChains(ctx, ht.buildScratch.first, ht.buildScratch.next, uint64(numConsistentTuples)+1, uint64(lastBatchNumDistinctTuples))
	}
}

// checkCols performs a column by column checkCol on the key columns.
func (ht *hashTable) checkCols(probeVecs []coldata.Vec, nToCheck uint64, probeSel []int) {
	switch ht.probeMode {
	case hashTableDefaultProbeMode:
		for i, keyCol := range ht.keyCols {
			ht.checkCol(probeVecs[i], ht.vals.ColVec(int(keyCol)), i, nToCheck, probeSel)
		}
	case hashTableDeletingProbeMode:
		for i, keyCol := range ht.keyCols {
			ht.checkColDeleting(probeVecs[i], ht.vals.ColVec(int(keyCol)), i, nToCheck, probeSel)
		}
	default:
		colexecerror.InternalError(errors.AssertionFailedf("unsupported hash table probe mode: %d", ht.probeMode))
	}
}

// checkColsForDistinctTuples performs a column by column check to find distinct
// tuples in the probe table that are not present in the build table.
func (ht *hashTable) checkColsForDistinctTuples(
	probeVecs []coldata.Vec, nToCheck uint64, probeSel []int,
) {
	buildVecs := ht.vals.ColVecs()
	for i := range ht.keyCols {
		probeVec := probeVecs[i]
		buildVec := buildVecs[ht.keyCols[i]]

		ht.checkColForDistinctTuples(probeVec, buildVec, nToCheck, probeSel)
	}
}

// computeBuckets computes the hash value of each key and stores the result in
// buckets.
func (ht *hashTable) computeBuckets(
	ctx context.Context, buckets []uint64, keys []coldata.Vec, nKeys int, sel []int,
) {
	if nKeys == 0 {
		// No work to do - avoid doing the loops below.
		return
	}

	initHash(buckets, nKeys, defaultInitHashValue)

	// Check if we received more tuples than the current allocation size and
	// increase it if so (limiting it by coldata.BatchSize()).
	if nKeys > ht.datumAlloc.AllocSize && ht.datumAlloc.AllocSize < coldata.BatchSize() {
		ht.datumAlloc.AllocSize = nKeys
		if ht.datumAlloc.AllocSize > coldata.BatchSize() {
			ht.datumAlloc.AllocSize = coldata.BatchSize()
		}
	}

	for i := range ht.keyCols {
		rehash(ctx, buckets, keys[i], nKeys, sel, ht.cancelChecker, ht.overloadHelper, &ht.datumAlloc)
	}

	finalizeHash(buckets, nKeys, ht.numBuckets)
}

// buildNextChains builds the hash map from the computed hash values.
func (ht *hashTable) buildNextChains(
	ctx context.Context, first, next []uint64, offset, batchSize uint64,
) {
	// The loop direction here is reversed to ensure that when we are building the
	// next chain for the probe table, the keyID in each equality chain inside
	// `next` is strictly in ascending order. This is crucial to ensure that when
	// built in distinct mode, hash table will emit the first distinct tuple it
	// encounters. When the next chain is built for build side, this invariant no
	// longer holds for the equality chains inside `next`. This is ok however for
	// the build side since all tuple that buffered on build side are already
	// distinct, therefore we can be sure that when we emit a tuple, there cannot
	// potentially be other tuples with the same key.
	for id := offset + batchSize - 1; id >= offset; id-- {
		// keyID is stored into corresponding hash bucket at the front of the next
		// chain.
		hash := next[id]
		firstKeyID := first[hash]
		// This is to ensure that `first` always points to the tuple with smallest
		// keyID in each equality chain. firstKeyID==0 means it is the first tuple
		// that we have encountered with the given hash value.
		if firstKeyID == 0 || id < firstKeyID {
			next[id] = first[hash]
			first[hash] = id
		} else {
			next[id] = next[firstKeyID]
			next[firstKeyID] = id
		}
	}
	ht.cancelChecker.checkEveryCall(ctx)
}

// setupLimitedSlices ensures that headID, differs, distinct, groupID, and
// toCheck are of the desired length and are setup for probing.
// Note that if the old groupID or toCheck slices have enough capacity, they
// are *not* zeroed out.
func (p *hashTableProbeBuffer) setupLimitedSlices(length int, buildMode hashTableBuildMode) {
	p.headID = maybeAllocateLimitedUint64Array(p.headID, length)
	p.differs = maybeAllocateLimitedBoolArray(p.differs, length)
	if buildMode == hashTableDistinctBuildMode {
		p.distinct = maybeAllocateLimitedBoolArray(p.distinct, length)
	}
	// Note that we don't use maybeAllocate* methods below because groupID and
	// toCheck don't need to be zeroed out when reused.
	if cap(p.groupID) < length {
		p.groupID = make([]uint64, length)
	} else {
		p.groupID = p.groupID[:length]
	}
	if cap(p.toCheck) < length {
		p.toCheck = make([]uint64, length)
	} else {
		p.toCheck = p.toCheck[:length]
	}
}

// findNext determines the id of the next key inside the groupID buckets for
// each equality column key in toCheck.
func (ht *hashTable) findNext(next []uint64, nToCheck uint64) {
	for _, toCheck := range ht.probeScratch.toCheck[:nToCheck] {
		ht.probeScratch.groupID[toCheck] = next[ht.probeScratch.groupID[toCheck]]
	}
}

// checkBuildForDistinct finds all tuples in probeVecs that are *not* present in
// buffered tuples stored in ht.vals. It stores the probeVecs's distinct tuples'
// keyIDs in headID buffer.
// NOTE: It assumes that probeVecs does not contain any duplicates itself.
// NOTE: It assumes that probeSel has already been populated and it is not nil.
// NOTE: It assumes that nToCheck is positive.
func (ht *hashTable) checkBuildForDistinct(
	probeVecs []coldata.Vec, nToCheck uint64, probeSel []int,
) uint64 {
	if probeSel == nil {
		colexecerror.InternalError(errors.AssertionFailedf("invalid selection vector"))
	}
	ht.checkColsForDistinctTuples(probeVecs, nToCheck, probeSel)
	nDiffers := uint64(0)
	toCheckSlice := ht.probeScratch.toCheck
	_ = toCheckSlice[nToCheck-1]
	for toCheckPos := uint64(0); toCheckPos < nToCheck && nDiffers < nToCheck; toCheckPos++ {
		//gcassert:bce
		toCheck := toCheckSlice[toCheckPos]
		if ht.probeScratch.distinct[toCheck] {
			ht.probeScratch.distinct[toCheck] = false
			// Calculated using the convention: keyID = keys.indexOf(key) + 1.
			ht.probeScratch.headID[toCheck] = toCheck + 1
		} else if ht.probeScratch.differs[toCheck] {
			// Continue probing in this next chain for the probe key.
			ht.probeScratch.differs[toCheck] = false
			//gcassert:bce
			toCheckSlice[nDiffers] = toCheck
			nDiffers++
		}
	}
	return nDiffers
}

// checkBuildForAggregation finds all tuples in probeVecs that *are* present in
// buffered tuples stored in ht.vals. For each present tuple at position i it
// stores keyID of its duplicate in headID[i], for all non-present tuples the
// corresponding headID value is left unchanged.
// NOTE: It assumes that probeVecs does not contain any duplicates itself.
// NOTE: It assumes that probeSel has already been populated and it is not nil.
// NOTE: It assumes that nToCheck is positive.
func (ht *hashTable) checkBuildForAggregation(
	probeVecs []coldata.Vec, nToCheck uint64, probeSel []int,
) uint64 {
	if probeSel == nil {
		colexecerror.InternalError(errors.AssertionFailedf("invalid selection vector"))
	}
	ht.checkColsForDistinctTuples(probeVecs, nToCheck, probeSel)
	nDiffers := uint64(0)
	toCheckSlice := ht.probeScratch.toCheck
	_ = toCheckSlice[nToCheck-1]
	for toCheckPos := uint64(0); toCheckPos < nToCheck && nDiffers < nToCheck; toCheckPos++ {
		//gcassert:bce
		toCheck := toCheckSlice[toCheckPos]
		if !ht.probeScratch.distinct[toCheck] {
			// If the tuple is distinct, it doesn't have a duplicate in the
			// hash table already, so we skip it.
			if ht.probeScratch.differs[toCheck] {
				// We have a hash collision, so we need to continue probing
				// against the next tuples in the hash chain.
				ht.probeScratch.differs[toCheck] = false
				//gcassert:bce
				toCheckSlice[nDiffers] = toCheck
				nDiffers++
			} else {
				// This tuple has a duplicate in the hash table, so we remember
				// keyID of that duplicate.
				ht.probeScratch.headID[toCheck] = ht.probeScratch.groupID[toCheck]
			}
		}
	}
	return nDiffers
}

// reset resets the hashTable for reuse.
// NOTE: memory that already has been allocated for ht.vals is *not* released.
// However, resetting the length of ht.vals to zero doesn't confuse the
// allocator - it is smart enough to look at the capacities of the allocated
// vectors, and the capacities would stay the same until an actual new
// allocation is needed, and at that time the allocator will update the memory
// account accordingly.
func (ht *hashTable) reset(_ context.Context) {
	for n := 0; n < len(ht.buildScratch.first); n += copy(ht.buildScratch.first[n:], zeroUint64Column) {
	}
	ht.vals.ResetInternalBatch()
	ht.vals.SetLength(0)
	// ht.probeScratch.next, ht.same and ht.visited are reset separately before
	// they are used (these slices are not used in all of the code paths).
	// ht.probeScratch.headID, ht.probeScratch.differs, and
	// ht.probeScratch.distinct are reset before they are used (these slices
	// are limited in size by dynamically allocated).
	// ht.probeScratch.groupID and ht.probeScratch.toCheck don't need to be
	// reset because they are populated manually every time before checking the
	// columns.
	if ht.buildMode == hashTableDistinctBuildMode {
		// In "distinct" build mode, ht.buildScratch.next is populated
		// iteratively, whenever we find tuples that we haven't seen before. In
		// order to reuse the same underlying memory we need to slice up that
		// slice (note that keyID=0 is reserved for the end of all hash chains,
		// so we make the length 1).
		ht.buildScratch.next = ht.buildScratch.next[:1]
	}
}
