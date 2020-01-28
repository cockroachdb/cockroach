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

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
)

// TODO(yuzefovich): support rehashing instead of large fixed bucket size.
const hashTableBucketSize = 1 << 16

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
	vals *bufferedBatch
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

func newHashTable(
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
	nKeep := uint32(0)

	keepTypes := make([]coltypes.T, 0, nCols)
	keepCols := make([]uint32, 0, nCols)

	for i := 0; i < nCols; i++ {
		if keepCol[i] {
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

		vals:     newBufferedBatch(allocator, keepTypes, 0 /* initialSize */),
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

// build executes the entirety of the hash table build phase using the input
// as the build source. The input is entirely consumed in the process.
func (ht *hashTable) build(ctx context.Context, input Operator) {
	for {
		batch := input.Next(ctx)
		if batch.Length() == 0 {
			break
		}

		ht.loadBatch(batch)
	}

	nKeyCols := len(ht.keyCols)
	keyCols := make([]coldata.Vec, nKeyCols)
	for i := 0; i < nKeyCols; i++ {
		keyCols[i] = ht.vals.colVecs[ht.keyCols[i]]
	}

	// ht.next is used to store the computed hash value of each key.
	ht.next = make([]uint64, ht.vals.length+1)
	ht.computeBuckets(ctx, ht.next[1:], keyCols, ht.vals.length, nil)
	ht.buildNextChains(ctx)
}

// findSameTuples populates the hashTable's same array by probing the
// hashTable with every single input key.
// NOTE: the hashTable *must* have been already built.
func (ht *hashTable) findSameTuples(ctx context.Context) {
	ht.same = make([]uint64, ht.vals.length+1)
	ht.head = make([]bool, ht.vals.length+1)
	ht.allocateVisited()

	nKeyCols := len(ht.keyCols)
	batchStart := uint64(0)
	for batchStart < ht.vals.length {
		batchEnd := batchStart + uint64(coldata.BatchSize())
		if batchEnd > ht.vals.length {
			batchEnd = ht.vals.length
		}

		batchSize := uint16(batchEnd - batchStart)

		for i := 0; i < nKeyCols; i++ {
			ht.keys[i] = ht.vals.colVecs[ht.keyCols[i]].Window(ht.valTypes[ht.keyCols[i]], batchStart, batchEnd)
		}

		ht.lookupInitial(ctx, batchSize, nil)
		nToCheck := batchSize

		for nToCheck > 0 {
			// Continue searching for the build table matching keys while the toCheck
			// array is non-empty.
			nToCheck = ht.check(nToCheck, nil)
			ht.findNext(nToCheck)
		}

		// Reset each element of headID to 0 to indicate that the probe key has not
		// been found in the build table. Also mark the corresponding indices as
		// head of the linked list.
		for i := uint16(0); i < batchSize; i++ {
			ht.head[ht.headID[i]] = true
			ht.headID[i] = 0
		}

		batchStart = batchEnd
	}
}

// loadBatch appends a new batch of keys and outputs to the existing keys and
// output columns.
func (ht *hashTable) loadBatch(batch coldata.Batch) {
	batchSize := batch.Length()
	ht.allocator.PerformOperation(ht.vals.colVecs, func() {
		for i, colIdx := range ht.valCols {
			ht.vals.colVecs[i].Append(
				coldata.SliceArgs{
					ColType:   ht.valTypes[i],
					Src:       batch.ColVec(int(colIdx)),
					Sel:       batch.Selection(),
					DestIdx:   ht.vals.length,
					SrcEndIdx: uint64(batchSize),
				},
			)
		}
		ht.vals.length += uint64(batchSize)
	})
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
	for id := uint64(1); id <= ht.vals.length; id++ {
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
	ht.visited = make([]bool, ht.vals.length+1)

	// Since keyID = 0 is reserved for end of list, it can be marked as visited
	// at the beginning.
	ht.visited[0] = true
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
