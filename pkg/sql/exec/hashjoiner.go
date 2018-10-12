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

import "github.com/cockroachdb/cockroach/pkg/sql/exec/types"

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
	sourceTypes []types.T

	// source specifies the input operator to the hash join.
	source Operator
}

// hashJoinEqInnerOp performs a hash join on the input tables equality columns.
// It requires that the output for every input batch in the probe phase fits
// within ColBatchSize, otherwise the behavior is undefined. An inner join is
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
// Depending on the value of the buildDistinct flag, there are two variations of
// the probe phase. The planner will set buildDistinct to true if and only if
// either the left equality columns or the right equality columns make a
// distinct key. This corresponding table would then be used as the build table.
//
// In the vectorized implementation of the distinct build table probe phase, the
// following tasks are performed by the fastProbe function:
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
// In the vectorized implementation of the non-distinct build table probe phase,
// the following tasks are performed by the probe function:
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
//    to ensure that each batch is at most ColBatchSize.

type hashJoinEqInnerOp struct {
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
}

var _ Operator = &hashJoinEqInnerOp{}

func (hj *hashJoinEqInnerOp) Init() {
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
		hashTableBucketSize,
		build.sourceTypes,
		build.eqCols,
		build.outCols,
	)

	hj.builder = makeHashJoinBuilder(
		hj.ht,
		build.source,
		build.eqCols,
		build.outCols,
	)

	hj.prober = makeHashJoinProber(
		hj.ht, probe,
		build.sourceTypes,
		build.outCols,
		hj.spec.buildRightSide,
	)

	hj.runningState = hjBuilding
}

func (hj *hashJoinEqInnerOp) Next() ColBatch {
	switch hj.runningState {
	case hjBuilding:
		hj.build()
		return hj.Next()
	case hjProbing:
		return hj.prober.probe(hj.spec.buildDistinct)
	default:
		panic("hash joiner in unhandled state")
	}
}

func (hj *hashJoinEqInnerOp) build() {

	hj.builder.exec()

	if !hj.spec.buildDistinct {
		hj.ht.same = make([]uint64, hj.ht.size+1)
		hj.ht.visited = make([]bool, hj.ht.size+1)

		// Since keyID = 0 is reserved for end of list, it can be marked as visited
		// at the beginning.
		hj.ht.visited[0] = true
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

	// same and visited are only used when the hashTable contains non-distinct
	// keys.
	//
	// same is a densely-packed list that stores the keyID of the next key in the
	// hash table that has the same value as the current key. The head of the key
	// is the first key of that value found in the next linked list. This field
	// will be lazily populated by the prober.
	same []uint64
	// visited represents whether each the corresponding key has been touched by
	// the prober.
	visited []bool

	// vals stores the union of the equality and output columns of the left
	// table. A key tuple is defined as the elements in each row of vals that
	// makes up the equality columns. The ID of a key at any index of vals is
	// index + 1.
	vals []ColVec

	// keyCols stores the indices of vals which are key columns.
	keyCols []uint32
	// keyTypes stores the corresponding types of the key columns.
	keyTypes []types.T

	// outCols stores the indices of vals which are output columns.
	outCols []uint32
	// outTypes stores the corresponding type of each output column.
	outTypes []types.T

	// size returns the total number of keyCols the hashTable currently stores.
	size uint64
	// bucketSize returns the number of buckets the hashTable employs. This is
	// equivalent to the size of first.
	bucketSize uint64
}

func makeHashTable(
	bucketSize uint64, sourceTypes []types.T, eqCols []uint32, outCols []uint32,
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
	cols := make([]ColVec, 0)
	nKeep := uint32(0)
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
	keys := make([]uint32, nKeys)
	for i, colIdx := range eqCols {
		keyTypes[i] = sourceTypes[colIdx]
		keys[i] = compressed[colIdx]
	}

	nOutCols := len(outCols)
	outTypes := make([]types.T, nOutCols)
	outs := make([]uint32, nOutCols)
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
func (ht *hashTable) loadBatch(batch ColBatch, eqCols []uint32, outCols []uint32) {
	batchSize := batch.Length()
	sel := batch.Selection()

	if sel != nil {
		for i, colIdx := range eqCols {
			ht.vals[ht.keyCols[i]].AppendWithSel(batch.ColVec(int(colIdx)), sel, batchSize, ht.keyTypes[i], ht.size)
		}

		for i, colIdx := range outCols {
			ht.vals[ht.outCols[i]].AppendWithSel(batch.ColVec(int(colIdx)), sel, batchSize, ht.outTypes[i], ht.size)
		}
	} else {
		for i, colIdx := range eqCols {
			ht.vals[ht.keyCols[i]].Append(batch.ColVec(int(colIdx)), ht.keyTypes[i], ht.size, batchSize)
		}

		for i, colIdx := range outCols {
			ht.vals[ht.outCols[i]].Append(batch.ColVec(int(colIdx)), ht.outTypes[i], ht.size, batchSize)
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

	// source is the input operator used during the build phase of the hash join.
	source Operator

	// eqCols and outCols hold the equality and output column indices of the build
	// table.
	eqCols  []uint32
	outCols []uint32
}

func makeHashJoinBuilder(
	ht *hashTable, source Operator, eqCols []uint32, outCols []uint32,
) *hashJoinBuilder {
	return &hashJoinBuilder{
		ht: ht,

		source:  source,
		eqCols:  eqCols,
		outCols: outCols,
	}
}

// exec executes the entirety of the hash table build phase using the source as
// the build relation. The source operator is entirely consumed in the process.
func (builder *hashJoinBuilder) exec() {
	for {
		batch := builder.source.Next()

		if batch.Length() == 0 {
			break
		}

		builder.ht.loadBatch(batch, builder.eqCols, builder.outCols)
	}

	// buckets is used to store the computed hash value of each key.
	nKeys := len(builder.eqCols)
	keyCols := make([]ColVec, nKeys)
	for i := 0; i < nKeys; i++ {
		keyCols[i] = builder.ht.vals[builder.ht.keyCols[i]]
	}

	buckets := make([]uint64, builder.ht.size)
	builder.ht.computeBuckets(buckets, keyCols, builder.ht.size, nil)
	builder.ht.insertKeys(buckets)
}

// hashJoinProber is used by the hashJoinEqInnerOp during the probe phase. It
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

	// head stores the first build table keyID that matched with the probe batch
	// key at any given index.
	head []uint64

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
	nBuildCols uint32
	// buildOutCols stores the indices of the output columns for the build table.
	buildOutCols []uint32

	// probeLeftSide indicates whether the prober is probing on the left source or
	// the right source.
	probeLeftSide bool

	// prevBatch, if not nil, indicates that the previous probe input batch has
	// not been fully processed.
	prevBatch ColBatch
}

func makeHashJoinProber(
	ht *hashTable,
	probe hashJoinerSourceSpec,
	buildColTypes []types.T,
	buildOutCols []uint32,
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

		head: make([]uint64, ColBatchSize),

		buildIdx: make([]uint64, ColBatchSize),
		probeIdx: make([]uint16, ColBatchSize),

		keys:    make([]ColVec, len(probe.eqCols)),
		buckets: make([]uint64, ColBatchSize),

		spec: probe,

		nBuildCols:   uint32(len(buildColTypes)),
		buildOutCols: buildOutCols,

		probeLeftSide: probeLeftSide,
	}
}

// probe is a general prober that works with non-distinct build table equality
// columns. It returns a ColBatch with N + M columns where N is the number of
// left source columns and M is the number of right source columns. The first N
// columns correspond to the respective left source columns, followed by the
// right source columns as the last M elements. Even though all the columns are
// present in the result, only the specified output columns store relevant
// information. The remaining columns are there as dummy columns and their
// states are undefined.
//
// buildDistinct is true if the build table equality columns are distinct. It
// performs the same operation as the probe() function normally would while
// taking a shortcut to improve speed.
func (prober *hashJoinProber) probe(buildDistinct bool) ColBatch {
	prober.batch.SetLength(0)

	if batch := prober.prevBatch; batch != nil {
		// The previous result was bigger than the maximum batch size, so we didn't
		// finish outputting it in the last call to probe. Continue outputting the
		// result from the previous batch.
		prober.prevBatch = nil
		batchSize := batch.Length()
		sel := batch.Selection()

		nResults := prober.collect(batch, batchSize, sel)
		prober.congregate(nResults, batch, batchSize, sel)
	} else {
		for {
			batch := prober.spec.source.Next()
			batchSize := batch.Length()

			if batchSize == 0 {
				break
			}

			for i, colIdx := range prober.spec.eqCols {
				prober.keys[i] = batch.ColVec(int(colIdx))
			}

			sel := batch.Selection()

			// Initialize groupID with the initial hash buckets and toCheck with all
			// applicable indices.
			prober.lookupInitial(batchSize, sel)
			nToCheck := batchSize

			var nResults uint16

			if buildDistinct {
				// Continue searching along the hash table next chains for the corresponding
				// buckets. If the key is found or end of next chain is reached, the key is
				// removed from the toCheck array.
				for nToCheck > 0 {
					nToCheck = prober.distinctCheck(nToCheck, sel)
					prober.findNext(nToCheck)
				}

				nResults = prober.distinctCollect(batch, batchSize, sel)
			} else {
				for nToCheck > 0 {
					// Continue searching for the build table matching keys while the toCheck
					// array is non-empty.
					nToCheck = prober.check(nToCheck, sel)
					prober.findNext(nToCheck)
				}

				nResults = prober.collect(batch, batchSize, sel)
			}

			prober.congregate(nResults, batch, batchSize, sel)

			if prober.batch.Length() > 0 {
				break
			}
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

// findNext determines the id of the next key inside the groupID buckets for
// each equality column key in toCheck.
func (prober *hashJoinProber) findNext(nToCheck uint16) {
	for i := uint16(0); i < nToCheck; i++ {
		prober.groupID[prober.toCheck[i]] = prober.ht.next[prober.groupID[prober.toCheck[i]]]
	}
}

// check performs a equality check between the current key in the groupID bucket
// and the probe key at that index. If there is a match, the hashTable's same
// array is updated to lazily populate the a linked list of identical build
// table keys. The visited flag for corresponding build table key is also set. A
// key is removed from toCheck if it has already been visited in a previous
// probe, or the bucket has reached the end (key not found in build table). The
// new length of toCheck is returned by this function.
func (prober *hashJoinProber) check(nToCheck uint16, sel []uint16) uint16 {
	for i, t := range prober.ht.keyTypes {
		prober.checkCol(t, i, nToCheck, sel)
	}

	nDiffers := uint16(0)
	for i := uint16(0); i < nToCheck; i++ {
		if !prober.differs[prober.toCheck[i]] {
			// If the current key matches with the probe key, we want to update head
			// with the current key if it has not been set yet.
			keyID := prober.groupID[prober.toCheck[i]]

			if prober.head[prober.toCheck[i]] == 0 {
				prober.head[prober.toCheck[i]] = keyID
			}
			headID := prober.head[prober.toCheck[i]]

			if !prober.ht.visited[keyID] {
				// We can then add this keyID into the same array at the end of the
				// corresponding linked list and mark this ID as visited. Since there
				// can be multiple keys that match this probe key, we want to mark
				// differs at this position to be true. This way, the prober will
				// continue probing for this key until it reaches the end of the next
				// chain.
				prober.differs[prober.toCheck[i]] = true
				prober.ht.visited[keyID] = true

				if headID != keyID {
					prober.ht.same[keyID] = prober.ht.same[headID]
					prober.ht.same[headID] = keyID
				}
			}
		}

		if prober.differs[prober.toCheck[i]] {
			// Continue probing in this next chain for the probe key.
			prober.differs[prober.toCheck[i]] = false
			prober.toCheck[nDiffers] = prober.toCheck[i]
			nDiffers++
		}
	}

	return nDiffers
}

// collect prepares the buildIdx and probeIdx arrays where the buildIdx and
// probeIdx at each index are joined to make an output row. The total number of
// resulting rows is returned.
func (prober *hashJoinProber) collect(batch ColBatch, batchSize uint16, sel []uint16) uint16 {
	nResults := uint16(0)
	if sel != nil {
		for i := uint16(0); i < batchSize; i++ {
			currentID := prober.head[i]
			for currentID != 0 {
				if nResults >= ColBatchSize {
					prober.prevBatch = batch
					return nResults
				}
				prober.buildIdx[nResults] = currentID - 1
				prober.probeIdx[nResults] = sel[i]
				currentID = prober.ht.same[currentID]
				prober.head[i] = currentID
				nResults++
			}
		}
	} else {
		for i := uint16(0); i < batchSize; i++ {
			currentID := prober.head[i]
			for currentID != 0 {
				if nResults >= ColBatchSize {
					prober.prevBatch = batch
					return nResults
				}
				prober.buildIdx[nResults] = currentID - 1
				prober.probeIdx[nResults] = i
				currentID = prober.ht.same[currentID]
				prober.head[i] = currentID
				nResults++
			}
		}
	}

	return nResults
}

// congregate uses the probeIdx and buildidx pairs to stitch together the
// resulting inner join rows and add them to the output batch with the left
// table columns preceding the right table columns.
func (prober *hashJoinProber) congregate(
	nResults uint16, batch ColBatch, batchSize uint16, sel []uint16,
) {
	var buildColOffset, probeColOffset uint32
	if prober.probeLeftSide {
		buildColOffset = uint32(len(prober.spec.sourceTypes))
		probeColOffset = 0
	} else {
		buildColOffset = 0
		probeColOffset = prober.nBuildCols
	}

	for i, colIdx := range prober.buildOutCols {
		outCol := prober.batch.ColVec(int(colIdx + buildColOffset))
		valCol := prober.ht.vals[prober.ht.outCols[i]]
		colType := prober.ht.outTypes[i]
		outCol.CopyWithSelInt64(valCol, prober.buildIdx, nResults, colType)
	}

	for _, colIdx := range prober.spec.outCols {
		outCol := prober.batch.ColVec(int(colIdx + probeColOffset))
		valCol := batch.ColVec(int(colIdx))
		colType := prober.spec.sourceTypes[colIdx]
		outCol.CopyWithSelInt16(valCol, prober.probeIdx, nResults, colType)
	}

	prober.batch.SetLength(nResults)
}

// distinctCheck determines if the current key in the groupID buckets matches the
// equality column key. If there is a match, then the key is removed from
// toCheck. If the bucket has reached the end, the key is rejected. The toCheck
// list is reconstructed to only hold the indices of the eqCol keys that have
// not been found. The new length of toCheck is returned by this function.
func (prober *hashJoinProber) distinctCheck(nToCheck uint16, sel []uint16) uint16 {
	for i, t := range prober.ht.keyTypes {
		prober.checkCol(t, i, nToCheck, sel)
	}

	// Select the indices that differ and put them into toCheck.
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

// distinctCollect prepares the batch with the joined output columns where the build
// row index for each probe row is given in the groupID slice. This function
// requires assumes a 1-n hash join.
func (prober *hashJoinProber) distinctCollect(
	batch ColBatch, batchSize uint16, sel []uint16,
) uint16 {
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

	return nResults
}

// NewEqInnerHashJoiner creates a new inner equality hash join operator
// on the left and right input tables. leftEqCols and rightEqCols specify the
// equality columns while leftOutCols and rightOutCols specifies the output
// columns.
func NewEqInnerHashJoiner(
	leftSource Operator,
	rightSource Operator,
	leftEqCols []uint32,
	rightEqCols []uint32,
	leftOutCols []uint32,
	rightOutCols []uint32,
	leftTypes []types.T,
	rightTypes []types.T,
	buildRightSide bool,
	buildDistinct bool,
) (Operator, error) {
	spec := hashJoinerSpec{
		left: hashJoinerSourceSpec{
			eqCols:      leftEqCols,
			outCols:     leftOutCols,
			sourceTypes: leftTypes,
			source:      leftSource,
		},

		right: hashJoinerSourceSpec{
			eqCols:      rightEqCols,
			outCols:     rightOutCols,
			sourceTypes: rightTypes,
			source:      rightSource,
		},

		buildRightSide: buildRightSide,
		buildDistinct:  buildDistinct,
	}

	return &hashJoinEqInnerOp{
		spec: spec,
	}, nil
}
