// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package exec

import (
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
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
	// phase. Probing is done in batches and the against the stored hash map.
	hjProbing
)

// hashJoinerSpec is the specification for a hash joiner processor. The hash
// joiner performs an inner join on the left and right's equal columns and
// returns combined left and right output columns.
type hashJoinerSpec struct {
	// left and right are the specifications of the two input table sources to
	// the hash joiner.
	left  hashJoinerSourceSpec
	right hashJoinerSourceSpec

	// buildRightSide indicates whether or not the build table is the right side.
	// By default, this flag is false and the build table is the left side.
	buildRightSide bool
}

type hashJoinerSourceSpec struct {
	// eqCols specify the indices of the source tables equality column during the
	// hash join.
	eqCols []int

	// outCols specify the indices of the columns that should be outputted by the
	// hash joiner.
	outCols []int

	// sourceTypes specify the types of the input columns of the source table for
	// the hash joiner.
	sourceTypes []types.T

	// source specifies the input operator to the hash join.
	source Operator
}

// hashJoinEqInnerDistinctOp performs a hash join on the input tables equality
// columns. It requires that the build table's equality columns only contain
// distinct values, otherwise the behavior is undefined. An inner join is
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
// In the vectorized implementation of the probe phrase, the following tasks are
// performed:
// 1. Compute the bucket number for each probe rows key tuple and store the
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

type hashJoinEqInnerDistinctOp struct {
	// spec, if not nil, holds the specification for the current hash joiner
	// process.
	spec hashJoinerSpec

	// ht holds the hashTable that is populated during the build
	// phase and used during the probe phase.
	ht *hashTable

	// prober, if not nil, stores the batch prober used by the hashJoiner in the
	// probe phase.
	prober *hashJoinProber

	// runningState stores the current state hashJoiner.
	runningState hashJoinerState
}

var _ Operator = &hashJoinEqInnerDistinctOp{}

func (hj *hashJoinEqInnerDistinctOp) Init() {
	hj.spec.left.source.Init()
	hj.spec.right.source.Init()

	nOutCols := len(hj.spec.left.outCols) + len(hj.spec.right.outCols)
	if nOutCols == 0 {
		panic("no output columns specified for hash joiner")
	}

	// Prepare the hashTable using the specified side as the build table. Prepare
	// the prober using the other side as the probe table.
	if hj.spec.buildRightSide {
		hj.ht = makeHashTable(hashTableBucketSize, hj.spec.right.sourceTypes, hj.spec.right.eqCols, hj.spec.right.outCols)
		hj.prober = makeHashJoinProber(hj.ht, hj.spec.left, hj.spec.right.sourceTypes, hj.spec.right.outCols, true)
	} else {
		hj.ht = makeHashTable(hashTableBucketSize, hj.spec.left.sourceTypes, hj.spec.left.eqCols, hj.spec.left.outCols)
		hj.prober = makeHashJoinProber(hj.ht, hj.spec.right, hj.spec.left.sourceTypes, hj.spec.left.outCols, false)
	}

	hj.runningState = hjBuilding
}

func (hj *hashJoinEqInnerDistinctOp) Next() ColBatch {
	switch hj.runningState {
	case hjBuilding:
		hj.build()
		fallthrough
	case hjProbing:
		return hj.prober.probe()
	default:
		panic("hash joiner in unhandled state")
	}
}

func (hj *hashJoinEqInnerDistinctOp) build() {
	builder := makeHashJoinBuilder(hj.ht)

	if hj.spec.buildRightSide {
		builder.exec(hj.spec.right.source, hj.spec.right.eqCols, hj.spec.right.outCols)
	} else {
		builder.exec(hj.spec.left.source, hj.spec.left.eqCols, hj.spec.left.outCols)
	}

	hj.runningState = hjProbing
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
	// first stores the keyID of the first key that resides in each bucket. This
	// keyID is used to determine the corresponding equality column key as well
	// as output column values.
	first []uint64

	// next is a densely-packed list that stores the keyID of the next key in the
	// hash table bucket chain, where an id of 0 is reserved to represent end of
	// chain.
	next []uint64

	// vals stores the union of the equality and output columns of the left
	// table. A key tuple is defined as the elements in each row of vals that
	// makes up the equality columns. The ID of a key at any index of vals is
	// index + 1.
	vals []ColVec

	// keyCols stores the indices of vals which are key columns.
	keyCols []int
	// keyTypes stores the corresponding types of the key columns.
	keyTypes []types.T

	// outCols stores the indices of vals which are output columns.
	outCols []int
	// outTypes stores the corresponding type of each output column.
	outTypes []types.T

	// size returns the total number of keyCols the hashTable currently stores.
	size uint64
	// bucketSize returns the number of buckets the hashTable employs. This is
	// equivalent to the size of first.
	bucketSize uint64
}

func makeHashTable(
	bucketSize uint64, sourceTypes []types.T, eqCols []int, outCols []int,
) *hashTable {
	// Compute the union of eqCols and outCols and compress vals to only keep the
	// important columns.
	nCols := len(sourceTypes)
	keepCol := make([]bool, nCols)
	compressed := make([]int, nCols)

	for _, colIdx := range eqCols {
		keepCol[colIdx] = true
	}

	for _, colIdx := range outCols {
		keepCol[colIdx] = true
	}

	// Extract the important columns and discard the rest.
	cols := make([]ColVec, 0)
	nKeep := 0
	for i := 0; i < nCols; i++ {
		if keepCol[i] {
			cols = append(cols, newMemColumn(sourceTypes[i], 0))
			compressed[i] = nKeep
			nKeep++
		}
	}

	// Extract and types and indices of the eqCols and outCols.
	nKeys := len(eqCols)
	keyTypes := make([]types.T, nKeys)
	keys := make([]int, nKeys)
	for i, colIdx := range eqCols {
		keyTypes[i] = sourceTypes[colIdx]
		keys[i] = compressed[colIdx]
	}

	nOutCols := len(outCols)
	outTypes := make([]types.T, nOutCols)
	outs := make([]int, nOutCols)
	for i, colIdx := range outCols {
		outTypes[i] = sourceTypes[colIdx]
		outs[i] = compressed[colIdx]
	}

	return &hashTable{
		first: make([]uint64, bucketSize),

		vals: cols,

		keyCols:  keys,
		keyTypes: keyTypes,

		outCols:  outs,
		outTypes: outTypes,

		bucketSize: bucketSize,
	}
}

// loadBatch appends a new batch of keys and outputs to the existing keys and
// output columns.
func (ht *hashTable) loadBatch(batch ColBatch, eqCols []int, outCols []int) {
	batchSize := batch.Length()
	sel := batch.Selection()

	if sel != nil {
		for i, colIdx := range eqCols {
			ht.vals[ht.keyCols[i]].AppendWithSel(batch.ColVec(colIdx), sel, batchSize, ht.keyTypes[i], ht.size)
		}

		for i, colIdx := range outCols {
			ht.vals[ht.outCols[i]].AppendWithSel(batch.ColVec(colIdx), sel, batchSize, ht.outTypes[i], ht.size)
		}
	} else {
		for i, colIdx := range eqCols {
			ht.vals[ht.keyCols[i]].Append(batch.ColVec(colIdx), ht.keyTypes[i], ht.size, batchSize)
		}

		for i, colIdx := range outCols {
			ht.vals[ht.outCols[i]].Append(batch.ColVec(colIdx), ht.outTypes[i], ht.size, batchSize)
		}
	}

	ht.size += uint64(batchSize)
}

// initHash, rehash, and finalizeHash work together to compute the hash value
// for an individual key tuple which represents a row's equality columns. Since this
// key is a tuple of various types, rehash is used to apply a transformation on
// the resulting hash value based on an element of the key of a specified type.
//
// The current integer tuple hashing heuristic is based off of Java's
// Arrays.hashCode(int[]) and only supports int8, int16, int32, and int64
// elements. float32 and float64 are hashed according to their respective 32-bit
// and 64-bit integer representation. bool keys are hashed as a 1 for true and 0
// for false. bytes are hashed as an array of int8 integers.
//
// initHash initializes the hash value of each key to its initial state for
// rehashing purposes.
func (ht *hashTable) initHash(buckets []uint64, nKeys uint64) {
	for i := uint64(0); i < nKeys; i++ {
		buckets[i] = 1
	}
}

// rehash takes a element of a key (tuple representing a row of equality
// column values) at a given column and computes a new hash by applying a
// transformation to the existing hash. This function is generated by execgen.

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
func (ht *hashTable) computeBuckets(buckets []uint64, keys []ColVec, nKeys uint64, sel []uint16) {
	ht.initHash(buckets, nKeys)
	for i, t := range ht.keyTypes {
		ht.rehash(buckets, i, t, keys[i], nKeys, sel)
	}
	ht.finalizeHash(buckets, nKeys)
}

// insertKeys builds the hash map from the compute hash values.
func (ht *hashTable) insertKeys(buckets []uint64) {
	ht.next = make([]uint64, ht.size+1)

	for id := uint64(1); id <= ht.size; id++ {
		// keyID is stored into corresponding hash bucket at the front of the next
		// chain.
		ht.next[id] = ht.first[buckets[id-1]]
		ht.first[buckets[id-1]] = id
	}
}

// hashJoinBuilder is used by the hashJoiner during the build phase. It
// pre-loads all batches from the build relation before building the hash table.
type hashJoinBuilder struct {
	ht *hashTable
}

func makeHashJoinBuilder(ht *hashTable) *hashJoinBuilder {
	return &hashJoinBuilder{
		ht: ht,
	}
}

// exec executes the entirety of the hash table build phase using the source as
// the build relation. The source operator is entirely consumed in the process.
func (builder *hashJoinBuilder) exec(source Operator, eqCols []int, outCols []int) {
	for {
		batch := source.Next()

		if batch.Length() == 0 {
			break
		}

		builder.ht.loadBatch(batch, eqCols, outCols)
	}

	// buckets is used to store the computed hash value of each key.
	nKeys := len(eqCols)
	keyCols := make([]ColVec, nKeys)
	for i := 0; i < nKeys; i++ {
		keyCols[i] = builder.ht.vals[builder.ht.keyCols[i]]
	}

	buckets := make([]uint64, builder.ht.size)
	builder.ht.computeBuckets(buckets, keyCols, builder.ht.size, nil)
	builder.ht.insertKeys(buckets)
}

// hashJoinProber is used by the hashJoinEqInnerDistinctOp during the probe phase. It
// operates on a single batch of obtained from the probe relation and probes the
// hashTable to construct the resulting output batch.
type hashJoinProber struct {
	ht *hashTable

	// batch stores the resulting output batch that is constructed and returned
	// for every input batch during the probe phase.
	batch ColBatch

	// groupID stores the keyID that maps to the joining rows of the build table.
	// The ith element of groupID stores the keyID of the build table that
	// corresponds to the ith key in the probe table.
	groupID []uint64
	// toCheck stores the indices of the eqCol rows that have yet to be found or
	// rejected.
	toCheck []uint16

	// differs stores whether the key at any index differs with the build table
	// key.
	differs []bool

	buildIdx []uint64
	probeIdx []uint16

	// keyCols stores the equality columns on the probe table for a single batch.
	keys []ColVec
	// buckets is used to store the computed hash value of each key in a single
	// batch.
	buckets []uint64

	// spec holds the specifications for the source operator used in the probe
	// phase.
	spec hashJoinerSourceSpec

	//nBuildCols stores the number of columns in the build table.
	nBuildCols int
	// buildOutCols stores the indices of the output columns for the build table.
	buildOutCols []int

	// probeLeftSide indicates whether the prober is probing on the left source or
	// the right source.
	probeLeftSide bool
}

func makeHashJoinProber(
	ht *hashTable,
	probe hashJoinerSourceSpec,
	buildColTypes []types.T,
	buildOutCols []int,
	probeLeftSide bool,
) *hashJoinProber {
	// Prepare the output batch by allocating with the correct column types.
	var outColTypes []types.T
	if probeLeftSide {
		outColTypes = append(probe.sourceTypes, buildColTypes...)
	} else {
		outColTypes = append(buildColTypes, probe.sourceTypes...)
	}

	return &hashJoinProber{
		ht: ht,

		batch: NewMemBatch(outColTypes),

		groupID: make([]uint64, ColBatchSize),
		toCheck: make([]uint16, ColBatchSize),
		differs: make([]bool, ColBatchSize),

		buildIdx: make([]uint64, ColBatchSize),
		probeIdx: make([]uint16, ColBatchSize),

		keys:    make([]ColVec, len(probe.eqCols)),
		buckets: make([]uint64, ColBatchSize),

		spec: probe,

		nBuildCols:   len(buildColTypes),
		buildOutCols: buildOutCols,

		probeLeftSide: probeLeftSide,
	}
}

// probe returns a ColBatch with N + M columns where N is the number of left
// source columns and M is the number of right source columns. The first N
// columns correspond to the respective left source columns, followed by the
// right source columns as the last M elements. Even though all the columns are
// present in the result, only the specified output columns store relevant
// information. The remaining columns are there as dummy columns and their
// states are undefined.
func (prober *hashJoinProber) probe() ColBatch {
	prober.batch.SetLength(0)

	for {
		batch := prober.spec.source.Next()
		batchSize := batch.Length()

		if batchSize == 0 {
			break
		}

		for i, colIdx := range prober.spec.eqCols {
			prober.keys[i] = batch.ColVec(colIdx)
		}

		sel := batch.Selection()

		// initialize groupID with the initial hash buckets and toCheck with all
		// applicable indices.
		prober.lookupInitial(batchSize, sel)
		nToCheck := batchSize

		// continue searching along the hash table next chains for the corresponding
		// buckets. If the key is found or end of next chain is reached, the key is
		// removed from the toCheck array.
		for nToCheck > 0 {
			nToCheck = prober.check(nToCheck, sel)
			prober.findNext(nToCheck)
		}

		prober.collectResults(batch, batchSize, sel)

		// since is possible for the hash join to return an empty group, we should
		// loop until we have a non-empty output batch, or an empty input batch.
		// Otherwise, the client will assume that the ColBatch is completely
		// consumed when it isn't.
		if prober.batch.Length() > 0 {
			// todo (changangela): add buffering to return batches of equal length on
			// each call to Next()
			break
		}
	}

	return prober.batch
}

// lookupInitial finds the corresponding hash table buckets for the equality
// column of the batch and stores the results in groupID. It also initializes
// toCheck with all indices in the range [0, batchSize).
func (prober *hashJoinProber) lookupInitial(batchSize uint16, sel []uint16) {
	prober.ht.computeBuckets(prober.buckets, prober.keys, uint64(batchSize), sel)
	for i := uint16(0); i < batchSize; i++ {
		prober.groupID[i] = prober.ht.first[prober.buckets[i]]
		prober.toCheck[i] = i
	}
}

// check determines if the current key in the groupID buckets matches the
// equality column key. If there is a match, then the key is removed from
// toCheck. If the bucket has reached the end, the key is rejected. The toCheck
// list is reconstructed to only hold the indices of the eqCol keys that have
// not been found. The new length of toCheck is returned by this function.
func (prober *hashJoinProber) check(nToCheck uint16, sel []uint16) uint16 {
	for i, t := range prober.ht.keyTypes {
		prober.checkCol(t, i, nToCheck, sel)
	}

	// select the indices that differ and put them into toCheck.
	nDiffers := uint16(0)
	for i := uint16(0); i < nToCheck; i++ {
		if prober.differs[prober.toCheck[i]] {
			prober.differs[prober.toCheck[i]] = false
			prober.toCheck[nDiffers] = prober.toCheck[i]
			nDiffers++
		}
	}

	return nDiffers
}

// findNext determines the id of the next key inside the groupID buckets for
// each equality column key in toCheck.
func (prober *hashJoinProber) findNext(nToCheck uint16) {
	for i := uint16(0); i < nToCheck; i++ {
		prober.groupID[prober.toCheck[i]] = prober.ht.next[prober.groupID[prober.toCheck[i]]]
	}
}

// collectResults prepares the batch with the joined output columns where the
// build row index for each probe row is given in the groupID slice.
func (prober *hashJoinProber) collectResults(batch ColBatch, batchSize uint16, sel []uint16) {
	nResults := uint16(0)

	if sel != nil {
		for i := uint16(0); i < batchSize; i++ {
			if prober.groupID[i] != 0 {
				// Index of keys and outputs in the hash table is calculated as ID - 1.
				prober.buildIdx[nResults] = prober.groupID[i] - 1
				prober.probeIdx[nResults] = sel[i]
				nResults++
			}
		}
	} else {
		for i := uint16(0); i < batchSize; i++ {
			if prober.groupID[i] != 0 {
				// Index of keys and outputs in the hash table is calculated as ID - 1.
				prober.buildIdx[nResults] = prober.groupID[i] - 1
				prober.probeIdx[nResults] = i
				nResults++
			}
		}
	}

	// Stitch together the resulting inner join rows and add them to the output
	// batch with the left table columns preceding right table columns.
	var buildColOffset, probeColOffset int
	if prober.probeLeftSide {
		buildColOffset = len(prober.spec.sourceTypes)
		probeColOffset = 0
	} else {
		buildColOffset = 0
		probeColOffset = prober.nBuildCols
	}

	for i, colIdx := range prober.buildOutCols {
		outCol := prober.batch.ColVec(colIdx + buildColOffset)
		valCol := prober.ht.vals[prober.ht.outCols[i]]
		colType := prober.ht.outTypes[i]
		outCol.CopyWithSelInt64(valCol, prober.buildIdx, nResults, colType)
	}

	for _, colIdx := range prober.spec.outCols {
		outCol := prober.batch.ColVec(colIdx + probeColOffset)
		valCol := batch.ColVec(colIdx)
		colType := prober.spec.sourceTypes[colIdx]
		outCol.CopyWithSelInt16(valCol, prober.probeIdx, nResults, colType)
	}

	prober.batch.SetLength(nResults)
}
