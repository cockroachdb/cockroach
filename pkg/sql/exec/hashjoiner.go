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
	// values stores the output columns where each output column has the same
	// length as the keys.
	values []ColVec

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
		first:      make([]uint64, bucketSize),
		keys:       newMemColumn(keyType, 0),
		next:       make([]uint64, 1),
		values:     values,
		bucketSize: bucketSize,
	}
}

func (ht *hashTable) loadBatch(batch ColBatch, eqColId int, outCols []int) {
	eqCol := batch.ColVec(eqColId)

	ht.keys = colAppend(ht.keys, eqCol)

	for i, colId := range outCols {
		ht.values[i] = colAppend(ht.values[i], batch.ColVec(colId))
	}

	ht.size += uint64(batch.Length())
}

func (ht *hashTable) insertHash(hash uint64, id uint64) {
	ht.next[id] = ht.first[hash]
	ht.first[hash] = id
}

func (ht *hashTable) insertKeys() {
	ht.next = make([]uint64, ht.size+1)

	switch ht.keys.Type() {
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

func (builder *hashJoinBuilder) start(source Operator, eqColId int, outCols []int) {
	for {
		batch := source.Next()

		if batch.Length() == 0 {
			break
		}

		builder.ht.loadBatch(batch, eqColId, outCols)
	}

	builder.ht.insertKeys()
}

func (hashTable *hashTable) hashInt64(key int64) uint64 {
	// todo(changangela) hash function should be improved
	return uint64(key) % hashTable.bucketSize
}

type hashJoinerSpec struct {
	leftEqCol  int
	rightEqCol int

	leftOutCols  []int
	rightOutCols []int

	// todo(changangela) add a Types() function that return the column types to
	// the Operator interface or cache the first batch to dynamically determine
	// the column types.
	leftSourceTypes  []types.T
	rightSourceTypes []types.T
}

// hashJoinerInt64 performs a hash join. It requires both sides to have
// exactly 1 equal column, and their types must be types.Int64. It also requires
// that the left equal column only contain distinct values, otherwise the
// behavior is undefined. An inner join is performed and there is no guarantee
// on the ordering of the output columns.
type hashJoinerInt64 struct {
	leftSource  Operator
	rightSource Operator
	spec        *hashJoinerSpec

	ht           *hashTable
	runningState hashJoinerState

	outFlow ColBatch
}

var _ Operator = &hashJoinerInt64{}

func (hj *hashJoinerInt64) Init() {
	nOutCols := len(hj.spec.leftOutCols) + len(hj.spec.rightOutCols)
	if nOutCols == 0 {
		panic("no output columns specified for hash joiner")
	}

	hj.leftSource.Init()
	hj.rightSource.Init()

	// prepare the hashTable using the left side as the build table
	keyType := hj.spec.leftSourceTypes[hj.spec.leftEqCol]
	outColTypes := make([]types.T, len(hj.spec.leftOutCols))
	for i, colId := range hj.spec.leftOutCols {
		outColTypes[i] = hj.spec.leftSourceTypes[colId]
	}
	hj.ht = makeHashTable(hashTableBucketSize, keyType, outColTypes)

	// prepare the output batch by allocating with the correct column types
	hj.outFlow = NewMemBatch(outColTypes...)

	hj.runningState = hjBuilding
}

func (hj *hashJoinerInt64) Next() ColBatch {
	hj.outFlow.SetLength(0)

	switch hj.runningState {
	case hjBuilding:
		hj.build()
	case hjProbing:
		//hj.probe()
	default:
		panic("hash joiner in unhandled state")
	}

	return hj.outFlow
}

func (hj *hashJoinerInt64) build() {
	builder := makeHashJoinBuilder(hj.ht)
	builder.start(hj.leftSource, hj.spec.leftEqCol, hj.spec.leftOutCols)
	hj.runningState = hjProbing
}
