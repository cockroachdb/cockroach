// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexechash

import (
	"context"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execgen"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// HashTableBuildMode represents different modes in which the HashTable can be
// built.
type HashTableBuildMode int

const (
	// HashTableFullBuildMode is the mode where HashTable buffers all input
	// tuples and populates first and next arrays for each hash bucket.
	HashTableFullBuildMode HashTableBuildMode = iota

	// HashTableDistinctBuildMode is the mode where HashTable only buffers
	// distinct tuples and discards the duplicates. In this mode the hash table
	// actually stores only the equality columns, so the columns with positions
	// not present in eqCols will remain zero-capacity vectors in Vals.
	HashTableDistinctBuildMode
)

// HashTableProbeMode represents different modes of probing the HashTable.
type HashTableProbeMode int

const (
	// HashTableDefaultProbeMode is the default probing mode of the HashTable.
	HashTableDefaultProbeMode HashTableProbeMode = iota

	// HashTableDeletingProbeMode is the mode of probing the HashTable in which
	// it "deletes" the tuples from itself once they are matched against
	// probing tuples.
	// For example, if we have a HashTable consisting of tuples {1, 1}, {1, 2},
	// {2, 3}, and the probing tuples are {1, 4}, {1, 5}, {1, 6}, then we get
	// the following when probing on the first column only:
	//   {1, 4} -> {1, 1}   | HashTable = {1, 2}, {2, 3}
	//   {1, 5} -> {1, 2}   | HashTable = {2, 3}
	//   {1, 6} -> no match | HashTable = {2, 3}
	// Note that the output of such probing is not fully deterministic when
	// tuples contain non-equality columns.
	HashTableDeletingProbeMode
)

// hashTableBuildBuffer stores the information related to the build table.
type hashTableBuildBuffer struct {
	// First stores the first keyID of the key that resides in each bucket.
	// This keyID is used to determine the corresponding equality column key as
	// well as output column values.
	First []uint64

	// Next is a densely-packed list that stores the keyID of the next key in the
	// hash table bucket chain, where an id of 0 is reserved to represent end of
	// chain.
	Next []uint64
}

// hashTableProbeBuffer stores the information related to the probe table.
type hashTableProbeBuffer struct {
	// First stores the first keyID of the key that resides in each bucket.
	// This keyID is used to determine the corresponding equality column key as
	// well as output column values.
	First []uint64

	// Next is a densely-packed list that stores the keyID of the next key in the
	// hash table bucket chain, where an id of 0 is reserved to represent end of
	// chain.
	Next []uint64

	// limitedSlicesAreAccountedFor indicates whether we have already
	// accounted for the memory used by the slices below.
	limitedSlicesAreAccountedFor bool

	///////////////////////////////////////////////////////////////
	// Slices below are allocated dynamically but are limited by //
	// coldata.BatchSize() in size.                              //
	///////////////////////////////////////////////////////////////

	// HeadID stores the first build table keyID that matched with the probe batch
	// key at any given index.
	HeadID []uint64

	// differs stores whether the key at any index differs with the build table
	// key.
	differs []bool

	// distinct stores whether the key in the probe batch is distinct in the build
	// table.
	distinct []bool

	// GroupID stores the keyID that maps to the joining rows of the build table.
	// The ith element of GroupID stores the keyID of the build table that
	// corresponds to the ith key in the probe table.
	GroupID []uint64

	// ToCheck stores the indices of the eqCol rows that have yet to be found or
	// rejected.
	ToCheck []uint64

	// HashBuffer stores the hash values of each tuple in the probe table. It will
	// be dynamically updated when the HashTable is build in distinct mode.
	HashBuffer []uint64
}

// HashTable is a structure used by the hash joiner to store the build table
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
type HashTable struct {
	allocator *colmem.Allocator

	// unlimitedSlicesNumUint64AccountedFor stores the number of uint64 from
	// the unlimited slices that we have already accounted for.
	unlimitedSlicesNumUint64AccountedFor int64

	// BuildScratch contains the scratch buffers required for the build table.
	BuildScratch hashTableBuildBuffer

	// ProbeScratch contains the scratch buffers required for the probe table.
	ProbeScratch hashTableProbeBuffer

	// Keys is a scratch space that stores the equality columns (either of
	// probe or build table, depending on the phase of algorithm).
	Keys []coldata.Vec

	// Same and Visited are only used when the HashTable contains non-distinct
	// keys (in HashTableFullBuildMode mode).
	//
	// Same is a densely-packed list that stores the keyID of the next key in the
	// hash table that has the same value as the current key. The HeadID of the key
	// is the first key of that value found in the next linked list. This field
	// will be lazily populated by the prober.
	Same []uint64
	// Visited represents whether each of the corresponding keys have been touched
	// by the prober.
	Visited []bool

	// Vals stores columns of the build source that are specified in
	// colsToStore in the constructor. The ID of a tuple at any index of Vals
	// is index + 1.
	Vals *colexecutils.AppendOnlyBufferedBatch
	// keyCols stores the indices of Vals which are key columns.
	keyCols []uint32

	// numBuckets returns the number of buckets the HashTable employs. This is
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
	cancelChecker  colexecutils.CancelChecker

	BuildMode HashTableBuildMode
	probeMode HashTableProbeMode
}

var _ colexecop.Resetter = &HashTable{}

// NewHashTable returns a new HashTable.
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
func NewHashTable(
	ctx context.Context,
	allocator *colmem.Allocator,
	loadFactor float64,
	initialNumHashBuckets uint64,
	sourceTypes []*types.T,
	eqCols []uint32,
	allowNullEquality bool,
	buildMode HashTableBuildMode,
	probeMode HashTableProbeMode,
) *HashTable {
	if !allowNullEquality && probeMode == HashTableDeletingProbeMode {
		// At the moment, we don't have a use case for such behavior, so let's
		// assert that it is not requested.
		colexecerror.InternalError(errors.AssertionFailedf("HashTableDeletingProbeMode is supported only when null equality is allowed"))
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
	// Vals).
	var colsToStore []int
	switch buildMode {
	case HashTableFullBuildMode:
		colsToStore = make([]int, len(sourceTypes))
		for i := range colsToStore {
			colsToStore[i] = i
		}
	case HashTableDistinctBuildMode:
		colsToStore = make([]int, len(eqCols))
		for i := range colsToStore {
			colsToStore[i] = int(eqCols[i])
		}
	default:
		colexecerror.InternalError(errors.AssertionFailedf("unknown HashTableBuildMode %d", buildMode))
	}
	ht := &HashTable{
		allocator: allocator,
		BuildScratch: hashTableBuildBuffer{
			First: make([]uint64, initialNumHashBuckets),
		},
		Keys:              make([]coldata.Vec, len(eqCols)),
		Vals:              colexecutils.NewAppendOnlyBufferedBatch(allocator, sourceTypes, colsToStore),
		keyCols:           eqCols,
		numBuckets:        initialNumHashBuckets,
		loadFactor:        loadFactor,
		allowNullEquality: allowNullEquality,
		BuildMode:         buildMode,
		probeMode:         probeMode,
	}

	if buildMode == HashTableDistinctBuildMode {
		ht.ProbeScratch.First = make([]uint64, initialNumHashBuckets)
		// ht.BuildScratch.Next will be populated dynamically by appending to
		// it, but we need to make sure that the special keyID=0 (which
		// indicates the end of the hash chain) is always present.
		ht.BuildScratch.Next = []uint64{0}
	}

	ht.cancelChecker.Init(ctx)
	return ht
}

// HashTableInitialToCheck is a slice that contains all consequent integers in
// [0, coldata.MaxBatchSize) range that can be used to initialize ToCheck buffer
// for most of the join types.
var HashTableInitialToCheck []uint64

func init() {
	HashTableInitialToCheck = make([]uint64, coldata.MaxBatchSize)
	for i := range HashTableInitialToCheck {
		HashTableInitialToCheck[i] = uint64(i)
	}
}

// shouldResize returns whether the hash table storing numTuples should be
// resized in order to not exceed the load factor given the current number of
// buckets.
func (ht *HashTable) shouldResize(numTuples int) bool {
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
// in ht.Vals. It'll determine the appropriate number of buckets that satisfy
// the target load factor.
func (ht *HashTable) buildFromBufferedTuples() {
	for ht.shouldResize(ht.Vals.Length()) {
		ht.numBuckets *= 2
	}
	ht.BuildScratch.First = colexecutils.MaybeAllocateUint64Array(ht.BuildScratch.First, int(ht.numBuckets))
	if ht.ProbeScratch.First != nil {
		ht.ProbeScratch.First = colexecutils.MaybeAllocateUint64Array(ht.ProbeScratch.First, int(ht.numBuckets))
	}
	for i, keyCol := range ht.keyCols {
		ht.Keys[i] = ht.Vals.ColVec(int(keyCol))
	}
	// ht.BuildScratch.Next is used to store the computed hash value of each key.
	ht.BuildScratch.Next = colexecutils.MaybeAllocateUint64Array(ht.BuildScratch.Next, ht.Vals.Length()+1)
	ht.ComputeBuckets(ht.BuildScratch.Next[1:], ht.Keys, ht.Vals.Length(), nil /* sel */)
	ht.buildNextChains(ht.BuildScratch.First, ht.BuildScratch.Next, 1 /* offset */, uint64(ht.Vals.Length()))
	// Account for memory used by the internal auxiliary slices that are
	// limited in size.
	ht.ProbeScratch.accountForLimitedSlices(ht.allocator)
	// Note that if ht.ProbeScratch.first is nil, it'll have zero capacity.
	newUint64Count := int64(cap(ht.BuildScratch.First) + cap(ht.ProbeScratch.First) + cap(ht.BuildScratch.Next))
	ht.allocator.AdjustMemoryUsage(sizeOfUint64 * (newUint64Count - ht.unlimitedSlicesNumUint64AccountedFor))
	ht.unlimitedSlicesNumUint64AccountedFor = newUint64Count
}

// FullBuild executes the entirety of the hash table build phase using the input
// as the build source. The input is entirely consumed in the process. Note that
// the hash table is assumed to operate in HashTableFullBuildMode.
func (ht *HashTable) FullBuild(input colexecop.Operator) {
	if ht.BuildMode != HashTableFullBuildMode {
		colexecerror.InternalError(errors.AssertionFailedf(
			"HashTable.FullBuild is called in unexpected build mode %d", ht.BuildMode,
		))
	}
	// We're using the hash table with the full build mode in which we will
	// fully buffer all tuples from the input first and only then we'll build
	// the hash table. Such approach allows us to compute the desired number of
	// hash buckets for the target load factor (this is done in
	// buildFromBufferedTuples()).
	for {
		batch := input.Next()
		if batch.Length() == 0 {
			break
		}
		ht.allocator.PerformAppend(ht.Vals, func() {
			ht.Vals.AppendTuples(batch, 0 /* startIdx */, batch.Length())
		})
	}
	ht.buildFromBufferedTuples()
}

// DistinctBuild appends all distinct tuples from batch to the hash table. Note
// that the hash table is assumed to operate in HashTableDistinctBuildMode.
// batch is updated to include only the distinct tuples.
func (ht *HashTable) DistinctBuild(batch coldata.Batch) {
	if ht.BuildMode != HashTableDistinctBuildMode {
		colexecerror.InternalError(errors.AssertionFailedf(
			"HashTable.DistinctBuild is called in unexpected build mode %d", ht.BuildMode,
		))
	}
	ht.ComputeHashAndBuildChains(batch)
	ht.RemoveDuplicates(batch, ht.Keys, ht.ProbeScratch.First, ht.ProbeScratch.Next, ht.CheckProbeForDistinct)
	// We only check duplicates when there is at least one buffered tuple.
	if ht.Vals.Length() > 0 {
		ht.RemoveDuplicates(batch, ht.Keys, ht.BuildScratch.First, ht.BuildScratch.Next, ht.CheckBuildForDistinct)
	}
	if batch.Length() > 0 {
		ht.AppendAllDistinct(batch)
	}
}

// ComputeHashAndBuildChains computes the hash codes of the tuples in batch and
// then builds 'next' chains between those tuples. The goal is to separate all
// tuples in batch into singly linked lists containing only tuples with the
// same hash code. Those 'next' chains are stored in ht.ProbeScratch.Next.
func (ht *HashTable) ComputeHashAndBuildChains(batch coldata.Batch) {
	srcVecs := batch.ColVecs()
	for i, keyCol := range ht.keyCols {
		ht.Keys[i] = srcVecs[keyCol]
	}

	batchLength := batch.Length()
	if cap(ht.ProbeScratch.Next) < batchLength+1 {
		ht.ProbeScratch.Next = make([]uint64, batchLength+1)
	}
	ht.ComputeBuckets(ht.ProbeScratch.Next[1:batchLength+1], ht.Keys, batchLength, batch.Selection())
	ht.ProbeScratch.HashBuffer = append(ht.ProbeScratch.HashBuffer[:0], ht.ProbeScratch.Next[1:batchLength+1]...)

	// We need to zero out 'first' buffer for all hash codes present in
	// HashBuffer, and there are two possible approaches that we choose from
	// based on a heuristic - we can either iterate over all hash codes and
	// zero out only the relevant elements (beneficial when 'first' buffer is
	// at least batchLength in size) or zero out the whole 'first' buffer
	// (beneficial otherwise).
	if batchLength < len(ht.ProbeScratch.First) {
		for _, hash := range ht.ProbeScratch.HashBuffer[:batchLength] {
			ht.ProbeScratch.First[hash] = 0
		}
	} else {
		for n := 0; n < len(ht.ProbeScratch.First); n += copy(ht.ProbeScratch.First[n:], colexecutils.ZeroUint64Column) {
		}
	}

	ht.buildNextChains(ht.ProbeScratch.First, ht.ProbeScratch.Next, 1 /* offset */, uint64(batchLength))
}

// FindBuckets finds the buckets for all tuples in batch when probing against a
// hash table that is specified by 'first' and 'next' vectors as well as
// 'duplicatesChecker'. `duplicatesChecker` takes a slice of key columns of the
// batch, number of tuples to check, and the selection vector of the batch, and
// it returns number of tuples that needs to be checked for next iteration.
// The "buckets" are specified by equal values in ht.ProbeScratch.HeadID.
// NOTE: *first* and *next* vectors should be properly populated.
// NOTE: batch is assumed to be non-zero length.
func (ht *HashTable) FindBuckets(
	batch coldata.Batch,
	keyCols []coldata.Vec,
	first, next []uint64,
	duplicatesChecker func([]coldata.Vec, uint64, []int) uint64,
) {
	batchLength := batch.Length()
	sel := batch.Selection()

	ht.ProbeScratch.SetupLimitedSlices(batchLength, ht.BuildMode)
	// Early bounds checks.
	groupIDs := ht.ProbeScratch.GroupID
	_ = groupIDs[batchLength-1]
	for i, hash := range ht.ProbeScratch.HashBuffer[:batchLength] {
		f := first[hash]
		//gcassert:bce
		groupIDs[i] = f
	}
	copy(ht.ProbeScratch.ToCheck, HashTableInitialToCheck[:batchLength])

	for nToCheck := uint64(batchLength); nToCheck > 0; {
		// Continue searching for the build table matching keys while the ToCheck
		// array is non-empty.
		nToCheck = duplicatesChecker(keyCols, nToCheck, sel)
		ht.FindNext(next, nToCheck)
	}
}

// RemoveDuplicates updates the selection vector of the batch to only include
// distinct tuples when probing against a hash table specified by 'first' and
// 'next' vectors as well as 'duplicatesChecker'.
// NOTE: *first* and *next* vectors should be properly populated.
func (ht *HashTable) RemoveDuplicates(
	batch coldata.Batch,
	keyCols []coldata.Vec,
	first, next []uint64,
	duplicatesChecker func([]coldata.Vec, uint64, []int) uint64,
) {
	ht.FindBuckets(batch, keyCols, first, next, duplicatesChecker)
	ht.updateSel(batch)
}

// AppendAllDistinct appends all tuples from batch to the hash table. It
// assumes that all tuples are distinct and that ht.ProbeScratch.HashBuffer
// contains the hash codes for all of them.
// NOTE: batch must be of non-zero length.
func (ht *HashTable) AppendAllDistinct(batch coldata.Batch) {
	numBuffered := uint64(ht.Vals.Length())
	ht.allocator.PerformAppend(ht.Vals, func() {
		ht.Vals.AppendTuples(batch, 0 /* startIdx */, batch.Length())
	})
	ht.BuildScratch.Next = append(ht.BuildScratch.Next, ht.ProbeScratch.HashBuffer[:batch.Length()]...)
	ht.buildNextChains(ht.BuildScratch.First, ht.BuildScratch.Next, numBuffered+1, uint64(batch.Length()))
	if ht.shouldResize(ht.Vals.Length()) {
		ht.buildFromBufferedTuples()
	}
}

// MaybeRepairAfterDistinctBuild checks whether the hash table built via
// DistinctBuild is in an inconsistent state and repairs it if so.
func (ht *HashTable) MaybeRepairAfterDistinctBuild() {
	// BuildScratch.Next has an extra 0th element not used by the tuples
	// reserved for the end of the chain.
	if len(ht.BuildScratch.Next) < ht.Vals.Length()+1 {
		// The hash table in such a state that some distinct tuples were
		// appended to ht.Vals, but 'next' and 'first' slices were not updated
		// accordingly.
		numConsistentTuples := len(ht.BuildScratch.Next) - 1
		lastBatchNumDistinctTuples := ht.Vals.Length() - numConsistentTuples
		ht.BuildScratch.Next = append(ht.BuildScratch.Next, ht.ProbeScratch.HashBuffer[:lastBatchNumDistinctTuples]...)
		ht.buildNextChains(ht.BuildScratch.First, ht.BuildScratch.Next, uint64(numConsistentTuples)+1, uint64(lastBatchNumDistinctTuples))
	}
}

// checkCols performs a column by column checkCol on the key columns.
func (ht *HashTable) checkCols(probeVecs []coldata.Vec, nToCheck uint64, probeSel []int) {
	switch ht.probeMode {
	case HashTableDefaultProbeMode:
		for i, keyCol := range ht.keyCols {
			ht.checkCol(probeVecs[i], ht.Vals.ColVec(int(keyCol)), i, nToCheck, probeSel)
		}
	case HashTableDeletingProbeMode:
		for i, keyCol := range ht.keyCols {
			ht.checkColDeleting(probeVecs[i], ht.Vals.ColVec(int(keyCol)), i, nToCheck, probeSel)
		}
	default:
		colexecerror.InternalError(errors.AssertionFailedf("unsupported hash table probe mode: %d", ht.probeMode))
	}
}

// checkColsForDistinctTuples performs a column by column check to find distinct
// tuples in the probe table that are not present in the build table.
func (ht *HashTable) checkColsForDistinctTuples(
	probeVecs []coldata.Vec, nToCheck uint64, probeSel []int,
) {
	buildVecs := ht.Vals.ColVecs()
	for i := range ht.keyCols {
		probeVec := probeVecs[i]
		buildVec := buildVecs[ht.keyCols[i]]

		ht.checkColForDistinctTuples(probeVec, buildVec, nToCheck, probeSel)
	}
}

// ComputeBuckets computes the hash value of each key and stores the result in
// buckets.
func (ht *HashTable) ComputeBuckets(buckets []uint64, keys []coldata.Vec, nKeys int, sel []int) {
	if nKeys == 0 {
		// No work to do - avoid doing the loops below.
		return
	}

	initHash(buckets, nKeys, DefaultInitHashValue)

	// Check if we received more tuples than the current allocation size and
	// increase it if so (limiting it by coldata.BatchSize()).
	if nKeys > ht.datumAlloc.AllocSize && ht.datumAlloc.AllocSize < coldata.BatchSize() {
		ht.datumAlloc.AllocSize = nKeys
		if ht.datumAlloc.AllocSize > coldata.BatchSize() {
			ht.datumAlloc.AllocSize = coldata.BatchSize()
		}
	}

	for i := range ht.keyCols {
		rehash(buckets, keys[i], nKeys, sel, ht.cancelChecker, &ht.overloadHelper, &ht.datumAlloc)
	}

	finalizeHash(buckets, nKeys, ht.numBuckets)
}

// buildNextChains builds the hash map from the computed hash values.
func (ht *HashTable) buildNextChains(first, next []uint64, offset, batchSize uint64) {
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
	ht.cancelChecker.CheckEveryCall()
}

// SetupLimitedSlices ensures that HeadID, differs, distinct, GroupID, and
// ToCheck are of the desired length and are setup for probing.
// Note that if the old GroupID or ToCheck slices have enough capacity, they
// are *not* zeroed out.
func (p *hashTableProbeBuffer) SetupLimitedSlices(length int, buildMode HashTableBuildMode) {
	p.HeadID = colexecutils.MaybeAllocateLimitedUint64Array(p.HeadID, length)
	p.differs = colexecutils.MaybeAllocateLimitedBoolArray(p.differs, length)
	if buildMode == HashTableDistinctBuildMode {
		p.distinct = colexecutils.MaybeAllocateLimitedBoolArray(p.distinct, length)
	}
	// Note that we don't use maybeAllocate* methods below because GroupID and
	// ToCheck don't need to be zeroed out when reused.
	if cap(p.GroupID) < length {
		p.GroupID = make([]uint64, length)
	} else {
		p.GroupID = p.GroupID[:length]
	}
	if cap(p.ToCheck) < length {
		p.ToCheck = make([]uint64, length)
	} else {
		p.ToCheck = p.ToCheck[:length]
	}
}

// FindNext determines the id of the next key inside the GroupID buckets for
// each equality column key in ToCheck.
func (ht *HashTable) FindNext(next []uint64, nToCheck uint64) {
	for _, toCheck := range ht.ProbeScratch.ToCheck[:nToCheck] {
		ht.ProbeScratch.GroupID[toCheck] = next[ht.ProbeScratch.GroupID[toCheck]]
	}
}

// CheckBuildForDistinct finds all tuples in probeVecs that are *not* present in
// buffered tuples stored in ht.Vals. It stores the probeVecs's distinct tuples'
// keyIDs in HeadID buffer.
// NOTE: It assumes that probeVecs does not contain any duplicates itself.
// NOTE: It assumes that probeSel has already been populated and it is not nil.
// NOTE: It assumes that nToCheck is positive.
func (ht *HashTable) CheckBuildForDistinct(
	probeVecs []coldata.Vec, nToCheck uint64, probeSel []int,
) uint64 {
	if probeSel == nil {
		colexecerror.InternalError(errors.AssertionFailedf("invalid selection vector"))
	}
	ht.checkColsForDistinctTuples(probeVecs, nToCheck, probeSel)
	nDiffers := uint64(0)
	toCheckSlice := ht.ProbeScratch.ToCheck
	_ = toCheckSlice[nToCheck-1]
	for toCheckPos := uint64(0); toCheckPos < nToCheck && nDiffers < nToCheck; toCheckPos++ {
		//gcassert:bce
		toCheck := toCheckSlice[toCheckPos]
		if ht.ProbeScratch.distinct[toCheck] {
			ht.ProbeScratch.distinct[toCheck] = false
			// Calculated using the convention: keyID = keys.indexOf(key) + 1.
			ht.ProbeScratch.HeadID[toCheck] = toCheck + 1
		} else if ht.ProbeScratch.differs[toCheck] {
			// Continue probing in this next chain for the probe key.
			ht.ProbeScratch.differs[toCheck] = false
			//gcassert:bce
			toCheckSlice[nDiffers] = toCheck
			nDiffers++
		}
	}
	return nDiffers
}

// CheckBuildForAggregation finds all tuples in probeVecs that *are* present in
// buffered tuples stored in ht.Vals. For each present tuple at position i it
// stores keyID of its duplicate in HeadID[i], for all non-present tuples the
// corresponding HeadID value is left unchanged.
// NOTE: It assumes that probeVecs does not contain any duplicates itself.
// NOTE: It assumes that probeSel has already been populated and it is not nil.
// NOTE: It assumes that nToCheck is positive.
func (ht *HashTable) CheckBuildForAggregation(
	probeVecs []coldata.Vec, nToCheck uint64, probeSel []int,
) uint64 {
	if probeSel == nil {
		colexecerror.InternalError(errors.AssertionFailedf("invalid selection vector"))
	}
	ht.checkColsForDistinctTuples(probeVecs, nToCheck, probeSel)
	nDiffers := uint64(0)
	toCheckSlice := ht.ProbeScratch.ToCheck
	_ = toCheckSlice[nToCheck-1]
	for toCheckPos := uint64(0); toCheckPos < nToCheck && nDiffers < nToCheck; toCheckPos++ {
		//gcassert:bce
		toCheck := toCheckSlice[toCheckPos]
		if !ht.ProbeScratch.distinct[toCheck] {
			// If the tuple is distinct, it doesn't have a duplicate in the
			// hash table already, so we skip it.
			if ht.ProbeScratch.differs[toCheck] {
				// We have a hash collision, so we need to continue probing
				// against the next tuples in the hash chain.
				ht.ProbeScratch.differs[toCheck] = false
				//gcassert:bce
				toCheckSlice[nDiffers] = toCheck
				nDiffers++
			} else {
				// This tuple has a duplicate in the hash table, so we remember
				// keyID of that duplicate.
				ht.ProbeScratch.HeadID[toCheck] = ht.ProbeScratch.GroupID[toCheck]
			}
		}
	}
	return nDiffers
}

// Reset resets the HashTable for reuse.
// NOTE: memory that already has been allocated for ht.Vals is *not* released.
// However, resetting the length of ht.Vals to zero doesn't confuse the
// allocator - it is smart enough to look at the capacities of the allocated
// vectors, and the capacities would stay the same until an actual new
// allocation is needed, and at that time the allocator will update the memory
// account accordingly.
func (ht *HashTable) Reset(_ context.Context) {
	for n := 0; n < len(ht.BuildScratch.First); n += copy(ht.BuildScratch.First[n:], colexecutils.ZeroUint64Column) {
	}
	ht.Vals.ResetInternalBatch()
	// ht.ProbeScratch.Next, ht.Same and ht.Visited are reset separately before
	// they are used (these slices are not used in all of the code paths).
	// ht.ProbeScratch.HeadID, ht.ProbeScratch.differs, and
	// ht.ProbeScratch.distinct are reset before they are used (these slices
	// are limited in size by dynamically allocated).
	// ht.ProbeScratch.GroupID and ht.ProbeScratch.ToCheck don't need to be
	// reset because they are populated manually every time before checking the
	// columns.
	if ht.BuildMode == HashTableDistinctBuildMode {
		// In "distinct" build mode, ht.BuildScratch.Next is populated
		// iteratively, whenever we find tuples that we haven't seen before. In
		// order to reuse the same underlying memory we need to slice up that
		// slice (note that keyID=0 is reserved for the end of all hash chains,
		// so we make the length 1).
		ht.BuildScratch.Next = ht.BuildScratch.Next[:1]
	}
}
