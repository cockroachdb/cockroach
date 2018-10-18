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
	// phase. Output columns are stored and a hash map is constructed from the
	// equality columns.
	// todo(changangela): dynamically determine which side to store (which side is
	// smallest) in this state.
	hjBuilding = iota

	// hjProbing represents the state the hashJoiner is in when it is in the probe
	// phase. Probing is done in batches and the against the stored hash map.
	hjProbing
)

// hashJoinerSpec is the specification for a hash joiner processor. The hash
// joiner performs an inner join on the left and right equal columns and returns
// combined left and right output columns.
type hashJoinerSpec struct {

	// leftEqCol and rightEqCol specify the indices of the joint constraint left
	// and right table equality columns.
	// todo(changangela) add support for multiple equality columns
	leftEqCol  int
	rightEqCol int

	// leftOutCols and rightOutCols specify the indices of the columns that should
	// be outputted by the hash joiner.
	leftOutCols  []int
	rightOutCols []int

	// leftSourceTypes and rightSourceTypes specify the types of the input columns
	// of the left and right tables for the hash joiner.
	leftSourceTypes  []types.T
	rightSourceTypes []types.T
}

// hashJoinerInt64Op performs a hash join. It requires both sides to have
// exactly 1 equal column, and their types must be types.Int64. It also requires
// that the left equal column only contain distinct values, otherwise the
// behavior is undefined. An inner join is performed and there is no guarantee
// on the ordering of the output columns.
type hashJoinerInt64Op struct {
	// leftSource and rightSource store the input operators for the two sides of
	// the join.
	leftSource  Operator
	rightSource Operator

	// spec, if not nil, holds the specification for the current hash joiner
	// process.
	spec *hashJoinerSpec
	// ht, if not nil, holds the hashTable that is populated during the build
	// phase and used during the probe phase.
	ht *hashTable
	// prober, if not nil, stores the batch prober used by the hashJoiner in the probe phase.
	prober *hashJoinProber

	// runningState stores the current state hashJoiner.
	runningState hashJoinerState
}

var _ Operator = &hashJoinerInt64Op{}

func (hj *hashJoinerInt64Op) Init() {
	nOutCols := len(hj.spec.leftOutCols) + len(hj.spec.rightOutCols)
	if nOutCols == 0 {
		panic("no output columns specified for hash joiner")
	}

	hj.leftSource.Init()
	hj.rightSource.Init()

	keyType := hj.spec.leftSourceTypes[hj.spec.leftEqCol]
	if hj.spec.rightSourceTypes[hj.spec.rightEqCol] != keyType {
		panic("hash joiner equal columns must be same type")
	}

	// prepare the hashTable using the left side as the build table
	buildColTypes := make([]types.T, len(hj.spec.leftOutCols))
	for i, colID := range hj.spec.leftOutCols {
		buildColTypes[i] = hj.spec.leftSourceTypes[colID]
	}
	hj.ht = makeHashTable(hashTableBucketSize, keyType, buildColTypes)

	// prepare the prober
	probeColTypes := make([]types.T, len(hj.spec.rightOutCols))
	for i, colID := range hj.spec.rightOutCols {
		probeColTypes[i] = hj.spec.rightSourceTypes[colID]
	}
	hj.prober = makeHashJoinProber(hj.ht, buildColTypes, probeColTypes)

	hj.runningState = hjBuilding
}

func (hj *hashJoinerInt64Op) Next() ColBatch {
	switch hj.runningState {
	case hjBuilding:
		hj.build()
		fallthrough
	case hjProbing:
		return hj.prober.probe(hj.rightSource, hj.spec.rightEqCol, hj.spec.rightOutCols, hj.spec.rightSourceTypes)
	default:
		panic("hash joiner in unhandled state")
	}
}

func (hj *hashJoinerInt64Op) build() {
	builder := makeHashJoinBuilder(hj.ht)
	builder.exec(hj.leftSource, hj.spec.leftEqCol, hj.spec.leftOutCols)

	hj.runningState = hjProbing
}

// hashTable is a structure used by the hash joiner to store the build table
// batches. Keys are stored according to the encoding of the equality column,
// which point to the corresponding output row. The table can then be probed in
// column batches to find at most one matching row per column batch row.
type hashTable struct {
	// first stores the id of the first key that resides in each bucket. This id
	// is used to determine the corresponding equality column key as well as
	// output column values.
	first []uint64
	// next is a densely-packed list that stores the id of the next key in the
	// hash table bucket chain, where an id of 0 is reserved to represent end of
	// chain.
	next []uint64

	// keys stores the equality column. The id of a key at any index in the column
	// is index + 1.
	keys ColVec
	// keyType stores the corresponding type of the keys column.
	keyType types.T
	// values stores the output columns where each output column has the same
	// length as the keys.
	values []ColVec
	// valueColTypes stores the corresponding type of each values column.
	valueColTypes []types.T
	nOutCols      int

	// size returns the total number of keys the hashTable currently stores.
	size uint64
	// bucketSize returns the number of buckets the hashTable employs. This is
	// equivalent to the size of first.
	bucketSize uint64
}

func makeHashTable(bucketSize uint64, keyType types.T, outColTypes []types.T) *hashTable {
	nOutCols := len(outColTypes)
	values := make([]ColVec, nOutCols)
	for i, t := range outColTypes {
		values[i] = newMemColumn(t, 0)
	}

	return &hashTable{
		first:         make([]uint64, bucketSize),
		keys:          newMemColumn(keyType, 0),
		keyType:       keyType,
		next:          make([]uint64, 1),
		values:        values,
		valueColTypes: outColTypes,
		nOutCols:      nOutCols,
		bucketSize:    bucketSize,
	}
}

func (ht *hashTable) hashInt64(key int64) uint64 {
	// todo(changangela) hash functions should be improved
	return uint64(key) % ht.bucketSize
}

func (ht *hashTable) insertHash(hash uint64, id uint64) {
	// id is stored into corresponding hash bucket at the front of the next chain.
	ht.next[id] = ht.first[hash]
	ht.first[hash] = id
}

// loadBatch appends a new batch of keys and values to the existing keys and
// values.
func (ht *hashTable) loadBatch(batch ColBatch, eqColID int, outCols []int) {
	// todo(changangela) examine the batch selection vector
	batchSize := batch.Length()
	eqCol := batch.ColVec(eqColID)

	ht.keys.Append(eqCol, ht.keyType, ht.size, batchSize)

	for i, colID := range outCols {
		ht.values[i].Append(batch.ColVec(colID), ht.valueColTypes[i], ht.size, batchSize)
	}

	ht.size += uint64(batchSize)
}

// insertKeys builds the hash map from the currently stored keys.
func (ht *hashTable) insertKeys() {
	ht.next = make([]uint64, ht.size+1)

	switch ht.keyType {
	case types.Int64:
		keys := ht.keys.Int64()
		for i := uint64(0); i < ht.size; i++ {
			ht.insertHash(ht.hashInt64(keys[i]), i+1)
		}
	default:
		panic("key type is not currently supported by hash joiner")
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
func (builder *hashJoinBuilder) exec(source Operator, eqColID int, outCols []int) {
	for {
		batch := source.Next()

		if batch.Length() == 0 {
			break
		}

		builder.ht.loadBatch(batch, eqColID, outCols)
	}

	builder.ht.insertKeys()
}

// hashJoinProber is used by the hashJoinerInt64Op during the probe phase. It
// operates on a single batch of obtained from the probe relation and probes the
// hashTable to construct the resulting output batch.
type hashJoinProber struct {
	ht *hashTable

	// outFlow stores the resulting output batch that is constructed and returned
	// for every input batch during the probe phase.
	outFlow ColBatch
}

func makeHashJoinProber(
	ht *hashTable, buildColTypes []types.T, probeColTypes []types.T,
) *hashJoinProber {
	// prepare the output batch by allocating with the correct column types
	outColTypes := append(buildColTypes, probeColTypes...)
	return &hashJoinProber{
		ht:      ht,
		outFlow: NewMemBatch(outColTypes),
	}
}

func (prober *hashJoinProber) probe(
	source Operator, eqColID int, outCols []int, outColTypes []types.T,
) ColBatch {
	prober.outFlow.SetLength(0)

	for {
		batch := source.Next()
		batchSize := batch.Length()

		if batchSize == 0 {
			break
		}

		eqCol := batch.ColVec(eqColID)

		// groupID stores the id that maps to the joining rows of the build table.
		groupID := make([]uint64, batchSize)
		// toCheck stores the indices of the eqCol rows that have yet to be found or
		// rejected.
		toCheck := make([]uint16, batchSize)

		prober.lookupInitial(eqCol, groupID, toCheck, batchSize)
		nToCheck := batchSize

		// continue searching along the hash table next chains for the corresponding
		// buckets. If the key is found or end of next chain is reached, the key is
		// removed from the toCheck array.
		for nToCheck > 0 {
			nToCheck = prober.check(eqCol, groupID, toCheck, nToCheck)
			prober.findNext(groupID, toCheck, nToCheck)
		}

		prober.collectResults(groupID, batch, batchSize, outCols, outColTypes)

		// since is possible for the hash join to return an empty group, we should
		// loop until we have a non-empty output batch, or an empty input batch.
		// Otherwise, the client will assume that the ColBatch is completely
		// consumed when it isn't.
		if prober.outFlow.Length() > 0 {
			// todo (changangela): add buffering to return batches of equal length on
			// each call to Next()
			break
		}
	}

	return prober.outFlow
}

// lookupInitial finds the corresponding hash table buckets for the equality
// column batch.
func (prober *hashJoinProber) lookupInitial(
	eqCol ColVec, groupID []uint64, toCheck []uint16, batchSize uint16,
) {
	keyType := prober.ht.keyType

	switch keyType {
	case types.Int64:
		col := eqCol.Int64()
		for i := uint16(0); i < batchSize; i++ {
			groupID[i] = prober.ht.first[prober.ht.hashInt64(col[i])]
			toCheck[i] = i
		}
	default:
		panic("key type is not currently supported by hash joiner")
	}
}

// check determines if the current key in the groupID buckets matches the
// equality column key. If there is a match, then the key is removed from
// toCheck. If the bucket has reached the end, the key is rejected. The toCheck
// list is reconstructed to only hold the indices of the eqCol keys that have
// not been found.
func (prober *hashJoinProber) check(
	eqCol ColVec, groupID []uint64, toCheck []uint16, nToCheck uint16,
) uint16 {
	keyType := prober.ht.keyType
	switch keyType {
	case types.Int64:
		keys := prober.ht.keys.Int64()
		col := eqCol.Int64()
		nDiffers := uint16(0)

		for i := uint16(0); i < nToCheck; i++ {
			// id of 0 is reserved to represent the end of the next chain.
			if id := groupID[toCheck[i]]; id != 0 {
				// the index of a key is the id offset by 1
				if keys[id-1] != col[toCheck[i]] {
					toCheck[nDiffers] = toCheck[i]
					nDiffers++
				}
			}
		}

		return nDiffers
	default:
		panic("key type is not currently supported by hash joiner")
	}
}

// findNext determines the id of the next key inside the groupID buckets for
// each equality column key in toCheck.
func (prober *hashJoinProber) findNext(groupID []uint64, toCheck []uint16, nToCheck uint16) {
	for i := uint16(0); i < nToCheck; i++ {
		groupID[toCheck[i]] = prober.ht.next[groupID[toCheck[i]]]
	}
}

// collectResults prepares the outFlow with the joined output columns where the
// build row index for each probe row is given in the groupID slice.
func (prober *hashJoinProber) collectResults(
	groupID []uint64, batch ColBatch, batchSize uint16, outCols []int, outColTypes []types.T,
) {
	buildID := make([]uint64, batchSize)
	probeID := make([]uint64, batchSize)
	nResults := uint16(0)

	for i := uint16(0); i < batchSize; i++ {
		if groupID[i] != 0 {
			// index of keys and values in the hash table is calculated as id - 1
			buildID[nResults] = groupID[i] - 1
			probeID[nResults] = uint64(i)
			nResults++
		}
	}

	for colIDx := 0; colIDx < prober.ht.nOutCols; colIDx++ {
		outCol := prober.outFlow.ColVec(colIDx)
		valCol := prober.ht.values[colIDx]
		colType := prober.ht.valueColTypes[colIDx]
		outCol.CopyFrom(valCol, buildID, nResults, colType)
	}

	for i, colID := range outCols {
		outCol := prober.outFlow.ColVec(i + prober.ht.nOutCols)
		valCol := batch.ColVec(colID)
		colType := outColTypes[colID]
		outCol.CopyFrom(valCol, probeID, nResults, colType)
	}

	prober.outFlow.SetLength(nResults)
}
