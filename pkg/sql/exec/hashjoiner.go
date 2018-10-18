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
	hjStateUnknown hashJoinerState = iota
	// hjBuilding represents the state the hashJoiner when it is in the build
	// phase. Output columns are stored and a hash map is constructed from the
	// equality columns.
	// todo(changangela): dynamically determine which side to store (which side is
	// smallest) in this state.
	hjBuilding

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

// hashJoinerInt64 performs a hash join. It requires both sides to have
// exactly 1 equal column, and their types must be types.Int64. It also requires
// that the left equal column only contain distinct values, otherwise the
// behavior is undefined. An inner join is performed and there is no guarantee
// on the ordering of the output columns.
type hashJoinerInt64 struct {
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

var _ Operator = &hashJoinerInt64{}

func (hj *hashJoinerInt64) Init() {
	nOutCols := len(hj.spec.leftOutCols) + len(hj.spec.rightOutCols)
	if nOutCols == 0 {
		panic("no output columns specified for hash joiner")
	}

	hj.leftSource.Init()
	hj.rightSource.Init()

	keyType := hj.spec.leftSourceTypes[hj.spec.leftEqCol]
	if keyType != types.Int64 {
		panic("key type is not currently supported by hash joiner")
	}

	// prepare the hashTable using the left side as the build table
	outColTypes := make([]types.T, len(hj.spec.leftOutCols))
	for i, colId := range hj.spec.leftOutCols {
		outColTypes[i] = hj.spec.leftSourceTypes[colId]
	}
	hj.ht = makeHashTable(hashTableBucketSize, keyType, outColTypes)

	// prepare the prober
	hj.prober = makeHashJoinProber(hj.ht, outColTypes)

	hj.runningState = hjBuilding
}

func (hj *hashJoinerInt64) Next() ColBatch {
	switch hj.runningState {
	case hjBuilding:
		hj.build()
		fallthrough
	case hjProbing:
		return hj.prober.probe(hj.rightSource, hj.spec.rightEqCol, hj.spec.rightOutCols)
	default:
		panic("hash joiner in unhandled state")
	}
}

func (hj *hashJoinerInt64) build() {
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

	// size returns the total number of keys the hashTable currently stores.
	size uint64
	// bucketSize returns the number of buckets the hashTable employs. This is
	// equivalent to the size of first.
	bucketSize uint64
}

func makeHashTable(bucketSize uint64, keyType types.T, outColTypes []types.T) *hashTable {
	values := make([]ColVec, len(outColTypes))
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
		bucketSize:    bucketSize,
	}
}

// todo(changangela) hash functions should be improved
func (hashTable *hashTable) hashInt8(key int8) uint64 {
	return uint64(key) % hashTable.bucketSize
}

func (hashTable *hashTable) hashInt16(key int16) uint64 {
	return uint64(key) % hashTable.bucketSize
}

func (hashTable *hashTable) hashInt32(key int32) uint64 {
	return uint64(key) % hashTable.bucketSize
}

func (hashTable *hashTable) hashInt64(key int64) uint64 {
	return uint64(key) % hashTable.bucketSize
}

func (ht *hashTable) insertHash(hash uint64, id uint64) {
	// id is stored into corresponding hash bucket at the front of the next chain.
	ht.next[id] = ht.first[hash]
	ht.first[hash] = id
}

// loadBatch appends a new batch of keys and values to the existing keys and
// values.
func (ht *hashTable) loadBatch(batch ColBatch, eqColId int, outCols []int) {
	eqCol := batch.ColVec(eqColId)

	ht.keys = colAppend(ht.keys, eqCol, ht.keyType)

	for i, colId := range outCols {
		ht.values[i] = colAppend(ht.values[i], batch.ColVec(colId), ht.valueColTypes[i])
	}

	ht.size += uint64(batch.Length())
}

// insertKeys builds the hash map from the currently stored keys.
func (ht *hashTable) insertKeys() {
	ht.next = make([]uint64, ht.size+1)

	switch ht.keyType {
	case types.Int8:
		keys := ht.keys.Int8()
		for i := uint64(0); i < ht.size; i++ {
			ht.insertHash(ht.hashInt8(keys[i]), i+1)
		}
	case types.Int16:
		keys := ht.keys.Int16()
		for i := uint64(0); i < ht.size; i++ {
			ht.insertHash(ht.hashInt16(keys[i]), i+1)
		}
	case types.Int32:
		keys := ht.keys.Int32()
		for i := uint64(0); i < ht.size; i++ {
			ht.insertHash(ht.hashInt32(keys[i]), i+1)
		}
	case types.Int64:
		keys := ht.keys.Int64()
		for i := uint64(0); i < ht.size; i++ {
			ht.insertHash(ht.hashInt64(keys[i]), i+1)
		}
	default:
		panic("key type is not currently supported by hash joiner")
	}
}

// hashJoinBuilder is used by the hashJoinerInt64 during the build phase. It
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
func (builder *hashJoinBuilder) exec(source Operator, eqColId int, outCols []int) {
	for {
		batch := source.Next()

		if batch.Length() == 0 {
			break
		}

		builder.ht.loadBatch(batch, eqColId, outCols)
	}

	builder.ht.insertKeys()
}

// hashJoinProber is used by the hashJoinerInt64 during the probe phase. It
// operates on a single batch of obtained from the probe relation and probes the
// hashTable to construct the resulting output batch.
type hashJoinProber struct {
	ht *hashTable

	// outFlow stores the resulting output batch that is constructed and returned
	// for every input batch during the probe phase.
	outFlow ColBatch
}

func makeHashJoinProber(ht *hashTable, outColTypes []types.T) *hashJoinProber {
	// prepare the output batch by allocating with the correct column types
	return &hashJoinProber{
		ht:      ht,
		outFlow: NewMemBatch(outColTypes...),
	}
}

func (prober *hashJoinProber) probe(source Operator, eqColId int, outCols []int) ColBatch {
	prober.outFlow.SetLength(0)

	return prober.outFlow
}
