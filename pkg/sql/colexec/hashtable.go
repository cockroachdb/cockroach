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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// HashTableNumBuckets is the default number of buckets in the colexec hashtable.
// TODO(yuzefovich): support rehashing instead of large fixed bucket size.
const HashTableNumBuckets = 1 << 16

// hashTableBuildMode represents different modes in which the hashTable can be
// built.
type hashTableBuildMode int

const (
	// hashTableFullBuildMode is the mode where hashTable buffers all input
	// tuples and populates first and next arrays for each hash bucket.
	hashTableFullBuildMode hashTableBuildMode = iota

	// hashTableDistinctBuildMode is the mode where hashTable only buffers
	// distinct tuples and discards the duplicates.
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

	// headID stores the first build table keyID that matched with the probe batch
	// key at any given index.
	headID []uint64

	// differs stores whether the key at any index differs with the build table
	// key.
	differs []bool

	// distinct stores whether the key in the probe batch is distinct in the build
	// table.
	distinct []bool

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

	// buildScratch contains the scratch buffers required for the build table.
	buildScratch hashTableBuildBuffer

	// probeScratch contains the scratch buffers required for the probe table.
	probeScratch hashTableProbeBuffer

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

	// vals stores the union of the equality and output columns of the build
	// table. A key tuple is defined as the elements in each row of vals that
	// makes up the equality columns. The ID of a key at any index of vals is
	// index + 1.
	vals *appendOnlyBufferedBatch
	// keyCols stores the indices of vals which are key columns.
	keyCols []uint32

	// numBuckets returns the number of buckets the hashTable employs. This is
	// equivalent to the size of first.
	numBuckets uint64

	// allowNullEquality determines if NULL keys should be treated as equal to
	// each other.
	allowNullEquality bool

	overloadHelper overloadHelper
	datumAlloc     sqlbase.DatumAlloc
	cancelChecker  CancelChecker

	buildMode hashTableBuildMode
	probeMode hashTableProbeMode
}

var _ resetter = &hashTable{}

func newHashTable(
	allocator *colmem.Allocator,
	numBuckets uint64,
	sourceTypes []*types.T,
	eqCols []uint32,
	allowNullEquality bool,
	buildMode hashTableBuildMode,
	probeMode hashTableProbeMode,
) *hashTable {
	if !allowNullEquality && probeMode == hashTableDeletingProbeMode {
		// At the moment, we don't have a use case for such behavior, so let's
		// assert that it is not requested.
		colexecerror.InternalError("hashTableDeletingProbeMode is supported only when null equality is allowed")
	}
	ht := &hashTable{
		allocator: allocator,

		buildScratch: hashTableBuildBuffer{
			first: make([]uint64, numBuckets),
		},

		probeScratch: hashTableProbeBuffer{
			keys:    make([]coldata.Vec, len(eqCols)),
			buckets: make([]uint64, coldata.BatchSize()),
			groupID: make([]uint64, coldata.BatchSize()),
			headID:  make([]uint64, coldata.BatchSize()),
			toCheck: make([]uint64, coldata.BatchSize()),
			differs: make([]bool, coldata.BatchSize()),
		},

		vals:              newAppendOnlyBufferedBatch(allocator, sourceTypes, 0 /* initialSize */),
		keyCols:           eqCols,
		numBuckets:        numBuckets,
		allowNullEquality: allowNullEquality,
		buildMode:         buildMode,
		probeMode:         probeMode,
	}

	if buildMode == hashTableDistinctBuildMode {
		ht.probeScratch.first = make([]uint64, numBuckets)
		ht.probeScratch.next = make([]uint64, coldata.BatchSize()+1)
		ht.buildScratch.next = make([]uint64, 1, coldata.BatchSize()+1)
		ht.probeScratch.hashBuffer = make([]uint64, coldata.BatchSize())
		ht.probeScratch.distinct = make([]bool, coldata.BatchSize())
	}

	return ht
}

// build executes the entirety of the hash table build phase using the input
// as the build source. The input is entirely consumed in the process.
func (ht *hashTable) build(ctx context.Context, input colexecbase.Operator) {
	nKeyCols := len(ht.keyCols)

	switch ht.buildMode {
	case hashTableFullBuildMode:
		for {
			batch := input.Next(ctx)
			if batch.Length() == 0 {
				break
			}

			ht.allocator.PerformOperation(ht.vals.ColVecs(), func() {
				ht.vals.append(batch, 0 /* startIdx */, batch.Length())
			})
		}

		keyCols := make([]coldata.Vec, nKeyCols)
		for i := 0; i < nKeyCols; i++ {
			keyCols[i] = ht.vals.ColVec(int(ht.keyCols[i]))
		}

		// ht.next is used to store the computed hash value of each key.
		ht.buildScratch.next = maybeAllocateUint64Array(ht.buildScratch.next, ht.vals.Length()+1)
		ht.computeBuckets(ctx, ht.buildScratch.next[1:], keyCols, ht.vals.Length(), nil)
		ht.buildNextChains(ctx, ht.buildScratch.first, ht.buildScratch.next, 1, ht.vals.Length())
	case hashTableDistinctBuildMode:
		for {
			batch := input.Next(ctx)
			if batch.Length() == 0 {
				break
			}

			srcVecs := batch.ColVecs()

			for i := 0; i < nKeyCols; i++ {
				ht.probeScratch.keys[i] = srcVecs[ht.keyCols[i]]
			}

			ht.computeBuckets(ctx, ht.probeScratch.next[1:], ht.probeScratch.keys, batch.Length(), batch.Selection())
			copy(ht.probeScratch.hashBuffer, ht.probeScratch.next[1:])

			// We should not zero out the entire `first` buffer here since the size of
			// the `first` buffer same as the hash range (2^16) by default. The size
			// of the hashBuffer is same as the batch size which is often a lot
			// smaller than the hash range. Since we are only concerned with tuples
			// inside the hashBuffer, we only need to zero out the corresponding
			// entries in the `first` buffer that occurred in the hashBuffer.
			for _, hash := range ht.probeScratch.hashBuffer[:batch.Length()] {
				ht.probeScratch.first[hash] = 0
			}

			ht.buildNextChains(ctx, ht.probeScratch.first, ht.probeScratch.next, 1, batch.Length())

			ht.removeDuplicates(batch, ht.probeScratch.keys, ht.probeScratch.first, ht.probeScratch.next, ht.checkProbeForDistinct)

			numBuffered := ht.vals.Length()
			// We only check duplicates when there is at least one buffered
			// tuple.
			if numBuffered > 0 {
				ht.removeDuplicates(batch, ht.probeScratch.keys, ht.buildScratch.first, ht.buildScratch.next, ht.checkBuildForDistinct)
			}

			ht.allocator.PerformOperation(ht.vals.ColVecs(), func() {
				ht.vals.append(batch, 0 /* startIdx */, batch.Length())
			})

			ht.buildScratch.next = append(ht.buildScratch.next, ht.probeScratch.hashBuffer[:batch.Length()]...)
			ht.buildNextChains(ctx, ht.buildScratch.first, ht.buildScratch.next, numBuffered+1, batch.Length())
		}
	default:
		colexecerror.InternalError(fmt.Sprintf("hashTable in unhandled state"))
	}
}

// removeDuplicates checks the tuples in the probe table against another table
// and updates the selection vector of the probe table to only include distinct
// tuples. The table removeDuplicates will check against is specified by
// `first`, `next` vectors and `duplicatesChecker`. `duplicatesChecker` takes
// a slice of key columns of the probe table, number of tuple to check and the
// selection vector of the probe table and returns number of tuples that needs
// to be checked for next iteration. It populates the ht.probeScratch.headID to
// point to the keyIDs that need to be included in probe table's selection
// vector.
// NOTE: *first* and *next* vectors should be properly populated.
func (ht *hashTable) removeDuplicates(
	batch coldata.Batch,
	keyCols []coldata.Vec,
	first, next []uint64,
	duplicatesChecker func([]coldata.Vec, uint64, []int) uint64,
) {
	nToCheck := uint64(batch.Length())
	sel := batch.Selection()

	for i := uint64(0); i < nToCheck; i++ {
		ht.probeScratch.groupID[i] = first[ht.probeScratch.hashBuffer[i]]
		ht.probeScratch.toCheck[i] = i
	}

	for nToCheck > 0 {
		// Continue searching for the build table matching keys while the toCheck
		// array is non-empty.
		nToCheck = duplicatesChecker(keyCols, nToCheck, sel)
		ht.findNext(next, nToCheck)
	}

	ht.updateSel(batch)
}

// checkCols performs a column by column checkCol on the key columns.
func (ht *hashTable) checkCols(
	probeVecs, buildVecs []coldata.Vec, buildKeyCols []uint32, nToCheck uint64, probeSel []int,
) {
	switch ht.probeMode {
	case hashTableDefaultProbeMode:
		for i := range ht.keyCols {
			ht.checkCol(probeVecs[i], buildVecs[buildKeyCols[i]], i, nToCheck, probeSel)
		}
	case hashTableDeletingProbeMode:
		for i := range ht.keyCols {
			ht.checkColDeleting(probeVecs[i], buildVecs[buildKeyCols[i]], i, nToCheck, probeSel)
		}
	default:
		colexecerror.InternalError(fmt.Sprintf("unsupported hash table probe mode: %d", ht.probeMode))
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
	initHash(buckets, nKeys, defaultInitHashValue)

	if nKeys == 0 {
		// No work to do - avoid doing the loops below.
		return
	}

	for i := range ht.keyCols {
		rehash(ctx, buckets, keys[i], nKeys, sel, ht.cancelChecker, ht.overloadHelper, &ht.datumAlloc)
	}

	finalizeHash(buckets, nKeys, ht.numBuckets)
}

// buildNextChains builds the hash map from the computed hash values.
func (ht *hashTable) buildNextChains(
	ctx context.Context, first, next []uint64, offset, batchSize int,
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
		ht.cancelChecker.check(ctx)
		// keyID is stored into corresponding hash bucket at the front of the next
		// chain.
		hash := next[id]
		firstKeyID := first[hash]
		// This is to ensure that `first` always points to the tuple with smallest
		// keyID in each equality chain. firstKeyID==0 means it is the first tuple
		// that we have encountered with the given hash value.
		if firstKeyID == 0 || uint64(id) < firstKeyID {
			next[id] = first[hash]
			first[hash] = uint64(id)
		} else {
			next[id] = next[firstKeyID]
			next[firstKeyID] = uint64(id)
		}
	}
}

// maybeAllocate* methods make sure that the passed in array is allocated and
// of the desired length.
func maybeAllocateUint64Array(array []uint64, length int) []uint64 {
	if array == nil || cap(array) < length {
		return make([]uint64, length)
	}
	array = array[:length]
	for n := 0; n < length; n += copy(array[n:], zeroUint64Column) {
	}
	return array
}

func maybeAllocateBoolArray(array []bool, length int) []bool {
	if array == nil || cap(array) < length {
		return make([]bool, length)
	}
	array = array[:length]
	for n := 0; n < length; n += copy(array[n:], zeroBoolColumn) {
	}
	return array
}

func (ht *hashTable) maybeAllocateSameAndVisited() {
	ht.same = maybeAllocateUint64Array(ht.same, ht.vals.Length()+1)
	ht.visited = maybeAllocateBoolArray(ht.visited, ht.vals.Length()+1)
	// Since keyID = 0 is reserved for end of list, it can be marked as visited
	// at the beginning.
	ht.visited[0] = true
}

// lookupInitial finds the corresponding hash table buckets for the equality
// column of the batch and stores the results in groupID. It also initializes
// toCheck with all indices in the range [0, batchSize).
func (ht *hashTable) lookupInitial(ctx context.Context, batchSize int, sel []int) {
	ht.computeBuckets(ctx, ht.probeScratch.buckets, ht.probeScratch.keys, batchSize, sel)
	for i := 0; i < batchSize; i++ {
		ht.probeScratch.groupID[i] = ht.buildScratch.first[ht.probeScratch.buckets[i]]
		ht.probeScratch.toCheck[i] = uint64(i)
	}
}

// findNext determines the id of the next key inside the groupID buckets for
// each equality column key in toCheck.
func (ht *hashTable) findNext(next []uint64, nToCheck uint64) {
	for i := uint64(0); i < nToCheck; i++ {
		ht.probeScratch.groupID[ht.probeScratch.toCheck[i]] =
			next[ht.probeScratch.groupID[ht.probeScratch.toCheck[i]]]
	}
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
	// ht.probeScratch.buckets doesn't need to be reset because buckets are
	// always initialized when computing the hash.
	copy(ht.probeScratch.groupID[:coldata.BatchSize()], zeroUint64Column)
	// ht.probeScratch.toCheck doesn't need to be reset because it is populated
	// manually every time before checking the columns.
	copy(ht.probeScratch.headID[:coldata.BatchSize()], zeroUint64Column)
	copy(ht.probeScratch.differs[:coldata.BatchSize()], zeroBoolColumn)
	copy(ht.probeScratch.distinct, zeroBoolColumn)
	if ht.buildMode == hashTableDistinctBuildMode && cap(ht.buildScratch.next) > 0 {
		// In "distinct" build mode, ht.buildScratch.next is populated
		// iteratively, whenever we find tuples that we haven't seen before. In
		// order to reuse the same underlying memory we need to slice up that
		// slice (note that keyID=0 is reserved for the end of all hash chains,
		// so we make the length 1).
		ht.buildScratch.next = ht.buildScratch.next[:1]
	}
}
