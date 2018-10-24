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
	"bytes"

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
	// build and probe are the specifications of the two input table sources to
	// the hash joiner. build represents the source used for the build phase and
	// probe represents the source used for the probe phase.
	build hashJoinerSourceSpec
	probe hashJoinerSourceSpec
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

// hashJoinEqInnerDistinctOp performs a hash join. It requires both sides to have
// exactly 1 equal column, and their types must be types.Int64. It also requires
// that the build table's equality column only contain distinct values,
// otherwise the behavior is undefined. An inner join is performed and there is
// no guarantee on the ordering of the output columns.
type hashJoinEqInnerDistinctOp struct {
	// spec, if not nil, holds the specification for the current hash joiner
	// process.
	spec hashJoinerSpec

	// ht, if not nil, holds the hashTable that is populated during the build
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
	nOutCols := len(hj.spec.build.outCols) + len(hj.spec.probe.outCols)
	if nOutCols == 0 {
		panic("no output columns specified for hash joiner")
	}

	keyTypes := make([]types.T, len(hj.spec.build.eqCols))
	for i, colIdx := range hj.spec.build.eqCols {
		keyTypes[i] = hj.spec.build.sourceTypes[colIdx]
	}

	// prepare the hashTable using the specified side as the build table.
	buildColTypes := make([]types.T, len(hj.spec.build.outCols))
	for i, colIdx := range hj.spec.build.outCols {
		buildColTypes[i] = hj.spec.build.sourceTypes[colIdx]
	}
	hj.ht = makeHashTable(hashTableBucketSize, keyTypes, buildColTypes)

	// prepare the prober.
	probeColTypes := make([]types.T, len(hj.spec.probe.outCols))
	for i, colIdx := range hj.spec.probe.outCols {
		probeColTypes[i] = hj.spec.probe.sourceTypes[colIdx]
	}

	hj.prober = makeHashJoinProber(hj.ht, buildColTypes, probeColTypes, hj.spec.probe.eqCols)

	hj.runningState = hjBuilding
}

func (hj *hashJoinEqInnerDistinctOp) Next() ColBatch {
	switch hj.runningState {
	case hjBuilding:
		hj.build()
		fallthrough
	case hjProbing:
		return hj.prober.probe(hj.spec.probe.source, hj.spec.probe.eqCols, hj.spec.probe.outCols, hj.spec.probe.sourceTypes)
	default:
		panic("hash joiner in unhandled state")
	}
}

func (hj *hashJoinEqInnerDistinctOp) build() {
	builder := makeHashJoinBuilder(hj.ht)

	builder.exec(hj.spec.build.source, hj.spec.build.eqCols, hj.spec.build.outCols)

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

	// keys stores the equality columns. The id of a key at any index any of the
	// columns is index + 1.
	keys []ColVec
	// keyTypes stores the corresponding types of the key columns.
	keyTypes []types.T

	// values stores the output columns where each output column has the same
	// length as the keys.
	values []ColVec
	// valueColTypes stores the corresponding type of each values column.
	valueColTypes []types.T
	// nValCols is the number of build table output columns.
	nValCols int

	// size returns the total number of keys the hashTable currently stores.
	size uint64
	// bucketSize returns the number of buckets the hashTable employs. This is
	// equivalent to the size of first.
	bucketSize uint64
}

func makeHashTable(bucketSize uint64, keyTypes []types.T, outColTypes []types.T) *hashTable {
	nEqCols := len(keyTypes)
	keys := make([]ColVec, nEqCols)
	for i, t := range keyTypes {
		keys[i] = newMemColumn(t, 0)
	}

	nOutCols := len(outColTypes)
	values := make([]ColVec, nOutCols)
	for i, t := range outColTypes {
		values[i] = newMemColumn(t, 0)
	}

	return &hashTable{
		first:         make([]uint64, bucketSize),
		keys:          keys,
		keyTypes:      keyTypes,
		values:        values,
		valueColTypes: outColTypes,
		nValCols:      nOutCols,
		bucketSize:    bucketSize,
	}
}

// loadBatch appends a new batch of keys and values to the existing keys and
// values.
func (ht *hashTable) loadBatch(batch ColBatch, eqCols []int, outCols []int) {
	batchSize := batch.Length()
	sel := batch.Selection()

	if sel != nil {
		for i, colIdx := range eqCols {
			ht.keys[i].AppendSelected(batch.ColVec(colIdx), sel, batchSize, ht.keyTypes[i], ht.size)
		}

		for i, colIdx := range outCols {
			ht.values[i].AppendSelected(batch.ColVec(colIdx), sel, batchSize, ht.valueColTypes[i], ht.size)
		}
	} else {
		for i, colIdx := range eqCols {
			ht.keys[i].Append(batch.ColVec(colIdx), ht.keyTypes[i], ht.size, batchSize)
		}

		for i, colIdx := range outCols {
			ht.values[i].Append(batch.ColVec(colIdx), ht.valueColTypes[i], ht.size, batchSize)
		}
	}

	ht.size += uint64(batchSize)
}

// initHash, rehash, and finalizeHash work together to compute the hash value
// for an individual key which represents a row's equality columns. Since this
// key is a tuple of various types, rehash is used to apply a transformation on
// the resulting hash value based on an element of the key of a specified type.
// The current array hashing heuristic is based off of Java's
// Arrays.hashCode(int[]) and only supports int64 elements.
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
// transformation to the existing hash.
func (ht *hashTable) rehash(
	buckets []uint64, keyIdx int, t types.T, col ColVec, nKeys uint64, sel []uint16,
) {
	// todo(changangela) this function needs to be templated eventually
	switch t {
	case types.Int8:
		keys := col.Int8()
		if sel != nil {
			for i := uint64(0); i < nKeys; i++ {
				buckets[i] = buckets[i]*31 + uint64(keys[sel[i]])
			}
		} else {
			for i := uint64(0); i < nKeys; i++ {
				buckets[i] = buckets[i]*31 + uint64(keys[i])
			}
		}
	case types.Int16:
		keys := col.Int16()
		if sel != nil {
			for i := uint64(0); i < nKeys; i++ {
				buckets[i] = buckets[i]*31 + uint64(keys[sel[i]])
			}
		} else {
			for i := uint64(0); i < nKeys; i++ {
				buckets[i] = buckets[i]*31 + uint64(keys[i])
			}
		}
	case types.Int32:
		keys := col.Int32()
		if sel != nil {
			for i := uint64(0); i < nKeys; i++ {
				buckets[i] = buckets[i]*31 + uint64(keys[sel[i]])
			}
		} else {
			for i := uint64(0); i < nKeys; i++ {
				buckets[i] = buckets[i]*31 + uint64(keys[i])
			}
		}
	case types.Int64:
		keys := col.Int64()
		if sel != nil {
			for i := uint64(0); i < nKeys; i++ {
				buckets[i] = buckets[i]*31 + uint64(keys[sel[i]])
			}
		} else {
			for i := uint64(0); i < nKeys; i++ {
				buckets[i] = buckets[i]*31 + uint64(keys[i])
			}
		}
	case types.Bool:
		keys := col.Bool()
		if sel != nil {
			for i := uint64(0); i < nKeys; i++ {
				if keys[sel[i]] {
					buckets[i] = buckets[i]*31 + 1
				}
			}
		} else {
			for i := uint64(0); i < nKeys; i++ {
				if keys[i] {
					buckets[i] = buckets[i]*31 + 1
				}
			}
		}
	case types.Bytes:
		keys := col.Bytes()
		if sel != nil {
			for i := uint64(0); i < nKeys; i++ {
				hash := 1
				for b := range keys[sel[i]] {
					hash = hash*31 + b
				}
				buckets[i] = buckets[i]*31 + uint64(hash)
			}
		} else {
			for i := uint64(0); i < nKeys; i++ {
				hash := 1
				for b := range keys[i] {
					hash = hash*31 + b
				}
				buckets[i] = buckets[i]*31 + uint64(hash)
			}
		}
	default:
		panic("key type is not currently supported by hash joiner")
	}
}

// finalizeHash takes each key's hash value and applies a final transformation
// onto it so that it fits within the hashTable's bucket size.
func (ht *hashTable) finalizeHash(buckets []uint64, nKeys uint64) {
	// todo(changangela): modulo a power of 2 could be made significantly faster
	// (20% speed improvement in benchmarks) if we use bitwise operators instead.
	for i := uint64(0); i < nKeys; i++ {
		// buckets[i] = buckets[i] % ht.bucketSize

		// theoretically, this could be optimized into the following (because
		// bucketSize is a power of 2):
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
	buckets := make([]uint64, builder.ht.size)
	builder.ht.computeBuckets(buckets, builder.ht.keys, builder.ht.size, nil)
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

	// keys stores the equality columns on the probe table for a single batch.
	keys []ColVec
	// buckets is used to store the computed hash value of each key in a single
	// batch.
	buckets []uint64
}

func makeHashJoinProber(
	ht *hashTable, buildColTypes []types.T, probeColTypes []types.T, probeEqCols []int,
) *hashJoinProber {
	// prepare the output batch by allocating with the correct column types
	outColTypes := append(buildColTypes, probeColTypes...)
	return &hashJoinProber{
		ht: ht,

		batch: NewMemBatch(outColTypes),

		groupID: make([]uint64, ColBatchSize),
		toCheck: make([]uint16, ColBatchSize),
		differs: make([]bool, ColBatchSize),

		buildIdx: make([]uint64, ColBatchSize),
		probeIdx: make([]uint16, ColBatchSize),

		keys:    make([]ColVec, len(probeEqCols)),
		buckets: make([]uint64, ColBatchSize),
	}
}

func (prober *hashJoinProber) probe(
	source Operator, eqCols []int, outCols []int, outColTypes []types.T,
) ColBatch {
	prober.batch.SetLength(0)

	for {
		batch := source.Next()
		batchSize := batch.Length()

		if batchSize == 0 {
			break
		}

		for i, colIdx := range eqCols {
			prober.keys[i] = batch.ColVec(colIdx)
		}

		sel := batch.Selection()

		prober.lookupInitial(batchSize, sel)
		nToCheck := batchSize

		// continue searching along the hash table next chains for the corresponding
		// buckets. If the key is found or end of next chain is reached, the key is
		// removed from the toCheck array.
		for nToCheck > 0 {
			nToCheck = prober.check(nToCheck, sel)
			prober.findNext(nToCheck)
		}

		prober.collectResults(batch, batchSize, outCols, outColTypes, sel)

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
// column of the batch.
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
	// todo(changangela) this function needs to be templated eventually
	for keyColIdx, t := range prober.ht.keyTypes {
		switch t {
		case types.Int64:
			buildKeys := prober.ht.keys[keyColIdx].Int64()
			probeKeys := prober.keys[keyColIdx].Int64()

			if sel != nil {
				for i := uint16(0); i < nToCheck; i++ {
					// keyID of 0 is reserved to represent the end of the next chain.
					if keyID := prober.groupID[prober.toCheck[i]]; keyID != 0 {
						// the build table key (calculated using keys[keyID - 1] = key) is
						// compared to the corresponding probe table to determine if a match is
						// found.
						if buildKeys[keyID-1] != probeKeys[sel[prober.toCheck[i]]] {
							prober.differs[prober.toCheck[i]] = true
						}
					}
				}
			} else {
				for i := uint16(0); i < nToCheck; i++ {
					// keyID of 0 is reserved to represent the end of the next chain.
					if keyID := prober.groupID[prober.toCheck[i]]; keyID != 0 {
						// the build table key (calculated using keys[keyID - 1] = key) is
						// compared to the corresponding probe table to determine if a match is
						// found.
						if buildKeys[keyID-1] != probeKeys[prober.toCheck[i]] {
							prober.differs[prober.toCheck[i]] = true
						}
					}
				}
			}
		case types.Int32:
			buildKeys := prober.ht.keys[keyColIdx].Int32()
			probeKeys := prober.keys[keyColIdx].Int32()

			if sel != nil {
				for i := uint16(0); i < nToCheck; i++ {
					if keyID := prober.groupID[prober.toCheck[i]]; keyID != 0 {
						if buildKeys[keyID-1] != probeKeys[sel[prober.toCheck[i]]] {
							prober.differs[prober.toCheck[i]] = true
						}
					}
				}
			} else {
				for i := uint16(0); i < nToCheck; i++ {
					if keyID := prober.groupID[prober.toCheck[i]]; keyID != 0 {
						if buildKeys[keyID-1] != probeKeys[prober.toCheck[i]] {
							prober.differs[prober.toCheck[i]] = true
						}
					}
				}
			}
		case types.Int16:
			buildKeys := prober.ht.keys[keyColIdx].Int16()
			probeKeys := prober.keys[keyColIdx].Int16()

			if sel != nil {
				for i := uint16(0); i < nToCheck; i++ {
					if keyID := prober.groupID[prober.toCheck[i]]; keyID != 0 {
						if buildKeys[keyID-1] != probeKeys[sel[prober.toCheck[i]]] {
							prober.differs[prober.toCheck[i]] = true
						}
					}
				}
			} else {
				for i := uint16(0); i < nToCheck; i++ {
					if keyID := prober.groupID[prober.toCheck[i]]; keyID != 0 {
						if buildKeys[keyID-1] != probeKeys[prober.toCheck[i]] {
							prober.differs[prober.toCheck[i]] = true
						}
					}
				}
			}
		case types.Int8:
			buildKeys := prober.ht.keys[keyColIdx].Int8()
			probeKeys := prober.keys[keyColIdx].Int8()

			if sel != nil {
				for i := uint16(0); i < nToCheck; i++ {
					if keyID := prober.groupID[prober.toCheck[i]]; keyID != 0 {
						if buildKeys[keyID-1] != probeKeys[sel[prober.toCheck[i]]] {
							prober.differs[prober.toCheck[i]] = true
						}
					}
				}
			} else {
				for i := uint16(0); i < nToCheck; i++ {
					if keyID := prober.groupID[prober.toCheck[i]]; keyID != 0 {
						if buildKeys[keyID-1] != probeKeys[prober.toCheck[i]] {
							prober.differs[prober.toCheck[i]] = true
						}
					}
				}
			}
		case types.Bool:
			buildKeys := prober.ht.keys[keyColIdx].Bool()
			probeKeys := prober.keys[keyColIdx].Bool()

			if sel != nil {
				for i := uint16(0); i < nToCheck; i++ {
					if keyID := prober.groupID[prober.toCheck[i]]; keyID != 0 {
						if buildKeys[keyID-1] != probeKeys[sel[prober.toCheck[i]]] {
							prober.differs[prober.toCheck[i]] = true
						}
					}
				}
			} else {
				for i := uint16(0); i < nToCheck; i++ {
					if keyID := prober.groupID[prober.toCheck[i]]; keyID != 0 {
						if buildKeys[keyID-1] != probeKeys[prober.toCheck[i]] {
							prober.differs[prober.toCheck[i]] = true
						}
					}
				}
			}
		case types.Bytes:
			buildKeys := prober.ht.keys[keyColIdx].Bytes()
			probeKeys := prober.keys[keyColIdx].Bytes()

			if sel != nil {
				for i := uint16(0); i < nToCheck; i++ {
					if keyID := prober.groupID[prober.toCheck[i]]; keyID != 0 {
						if !bytes.Equal(buildKeys[keyID-1], probeKeys[sel[prober.toCheck[i]]]) {
							prober.differs[prober.toCheck[i]] = true
						}
					}
				}
			} else {
				for i := uint16(0); i < nToCheck; i++ {
					if keyID := prober.groupID[prober.toCheck[i]]; keyID != 0 {
						if !bytes.Equal(buildKeys[keyID-1], probeKeys[prober.toCheck[i]]) {
							prober.differs[prober.toCheck[i]] = true
						}
					}
				}
			}
		default:
			panic("key type is not currently supported by hash joiner")
		}

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
func (prober *hashJoinProber) collectResults(
	batch ColBatch, batchSize uint16, outCols []int, outColTypes []types.T, sel []uint16,
) {
	nResults := uint16(0)

	if sel != nil {
		for i := uint16(0); i < batchSize; i++ {
			if prober.groupID[i] != 0 {
				// index of keys and values in the hash table is calculated as id - 1
				prober.buildIdx[nResults] = prober.groupID[i] - 1
				prober.probeIdx[nResults] = sel[i]
				nResults++
			}
		}
	} else {
		for i := uint16(0); i < batchSize; i++ {
			if prober.groupID[i] != 0 {
				// index of keys and values in the hash table is calculated as id - 1
				prober.buildIdx[nResults] = prober.groupID[i] - 1
				prober.probeIdx[nResults] = i
				nResults++
			}
		}
	}

	for colIdx := 0; colIdx < prober.ht.nValCols; colIdx++ {
		outCol := prober.batch.ColVec(colIdx)
		valCol := prober.ht.values[colIdx]
		colType := prober.ht.valueColTypes[colIdx]
		outCol.CopyFrom(valCol, prober.buildIdx, nResults, colType)
	}

	for i, colIdx := range outCols {
		outCol := prober.batch.ColVec(i + prober.ht.nValCols)
		valCol := batch.ColVec(colIdx)
		colType := outColTypes[colIdx]
		outCol.CopyFromBatch(valCol, prober.probeIdx, nResults, colType)
	}

	prober.batch.SetLength(nResults)
}
