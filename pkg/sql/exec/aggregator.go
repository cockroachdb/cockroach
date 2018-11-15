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
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlpb"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
	"github.com/pkg/errors"
)

// aggregateFunc is an aggregate function that performs computation on a batch
// when Compute(batch) is called and writes the output to the ColVec passed in
// in Init. The aggregateFunc performs an aggregation per group and outputs the
// aggregation once the end of the group is reached. If the end of the group is
// not reached before the batch is finished, the aggregateFunc will store a
// carry value that it will use next time Compute is called. Note that this
// carry value is stored at the output index. Therefore if any memory
// modification of the output vector is made, the caller *MUST* copy the value
// at the current index inclusive for a correct aggregation.
type aggregateFunc interface {
	// Init sets the groups for the aggregation and the output vector. Each index
	// in groups corresponds to a column value in the input batch. true represents
	// the first value of a new group.
	Init(groups []bool, vec ColVec)

	// Reset resets the aggregate function for another run. Primarily used for
	// benchmarks.
	Reset()

	// CurrentOutputIndex returns the current index in the output vector that the
	// aggregate function is writing to. All indices < the index returned are
	// finished aggregations for previous groups. A negative index may be returned
	// to signify an aggregate function that has not yet performed any
	// computation.
	CurrentOutputIndex() int
	// SetOutputIndex sets the output index to write to. The value for the current
	// index is carried over. Note that calling SetOutputIndex is a noop if
	// CurrentOutputIndex returns a negative value (i.e. the aggregate function
	// has not yet performed any computation). This method also has the side
	// effect of clearing the output buffer past the given index.
	SetOutputIndex(idx int)

	// Compute computes the aggregation on the input batch. A zero-length input
	// batch tells the aggregate function that it should flush its results.
	Compute(batch ColBatch, inputIdxs []uint32)
}

// orderedAggregator is an aggregator that performs arbitrary aggregations on
// input ordered by a set of grouping columns. Before performing any
// aggregations, the aggregator sets up a chain of distinct operators that will
// produce a vector of booleans (referenced in groupCol) that specifies whether
// or not the corresponding columns in the input batch are part of a new group.
// The memory is modified by the distinct operator flow.
// Every aggregate function will change the shape of the data. i.e. a new column
// value will be output for each input group. Since the number of input groups
// is variable and the number of output values is constant, care must be taken
// not to overflow the output buffer. To avoid having to perform bounds checks
// for the aggregate functions, the aggregator allocates twice the size of the
// input batch for the functions to write to. Before the next batch is
// processed, the aggregator checks what index the functions are outputting to.
// If greater than the expected output batch size by downstream operators, the
// overflow values are copied to the start of the batch. Since the input batch
// size is not necessarily the same as the output batch size, more than one copy
// and return must be performed until the aggregator is in a state where its
// functions are in a state where the output indices would not overflow the
// output batch if a worst case input batch is encountered (one where every
// value is part of a new group).
type orderedAggregator struct {
	input Operator

	done bool

	aggCols [][]uint32
	aggTyps [][]types.T

	outputTyps []types.T

	// scratch is the ColBatch to output and variables related to it. Aggregate
	// function operators write directly to this output batch.
	scratch struct {
		ColBatch
		// resumeIdx is the index at which the aggregation functions should start
		// writing to on the next iteration of Next().
		resumeIdx int
		// outputSize is ColBatchSize by default.
		outputSize int
	}

	// groupCol is the slice that aggregateFuncs use to determine whether a value
	// is part of the current aggregation group. See aggregateFunc.Init for more
	// information.
	groupCol []bool
	// aggregateFuncs are the aggregator's aggregate function operators.
	aggregateFuncs []aggregateFunc
}

var _ Operator = &orderedAggregator{}

// NewOrderedAggregator creates an ordered aggregator on the given grouping
// columns.
// TODO(asubiotto): Take in distsqlrun.AggregatorSpec_Func. This is currently
// impossible due to an import cycle so we hack around it by taking in the raw
// integer specifier.
func NewOrderedAggregator(
	input Operator,
	colTypes []types.T,
	aggFns []distsqlpb.AggregatorSpec_Func,
	groupCols []uint32,
	aggCols [][]uint32,
) (Operator, error) {
	if len(aggFns) != len(aggCols) {
		return nil,
			errors.Errorf(
				"mismatched aggregation lengths: aggFns(%d), aggCols(%d)",
				len(aggFns),
				len(aggCols),
			)
	}

	groupTyps := extractGroupTypes(groupCols, colTypes)
	aggTyps := extractAggTypes(aggCols, colTypes)

	op, groupCol, err := orderedDistinctColsToOperators(input, groupCols, groupTyps)
	if err != nil {
		return nil, err
	}

	a := &orderedAggregator{}
	if len(groupCols) == 0 {
		// If there were no groupCols, we can't rely on the distinct operators to
		// mark the first row as distinct, so we have to do it ourselves. Set up a
		// oneShotOp to set the first row to distinct.
		op = &oneShotOp{
			input: op,
			fn: func(batch ColBatch) {
				if batch.Length() == 0 {
					return
				}
				if sel := batch.Selection(); sel != nil {
					groupCol[sel[0]] = true
				} else {
					groupCol[0] = true
				}
			},
			outputSourceRef: &a.input,
		}
	}

	*a = orderedAggregator{
		input: op,

		aggCols:  aggCols,
		aggTyps:  aggTyps,
		groupCol: groupCol,
	}

	a.aggregateFuncs, a.outputTyps, err = makeAggregateFuncs(aggTyps, aggFns)

	if err != nil {
		return nil, err
	}

	return a, nil
}

func makeAggregateFuncs(
	aggTyps [][]types.T, aggFns []distsqlpb.AggregatorSpec_Func,
) ([]aggregateFunc, []types.T, error) {
	funcs := make([]aggregateFunc, len(aggFns))
	outTyps := make([]types.T, len(aggFns))

	for i := range aggFns {
		var err error
		switch aggFns[i] {
		case distsqlpb.AggregatorSpec_ANY_NOT_NULL:
			funcs[i], err = newAnyNotNullAgg(aggTyps[i][0])
		case distsqlpb.AggregatorSpec_AVG:
			funcs[i], err = newAvgAgg(aggTyps[i][0])
		case distsqlpb.AggregatorSpec_SUM, distsqlpb.AggregatorSpec_SUM_INT:
			funcs[i], err = newSumAgg(aggTyps[i][0])
		case distsqlpb.AggregatorSpec_COUNT_ROWS:
			funcs[i] = newCountAgg()
		default:
			return nil, nil, errors.Errorf("unsupported columnar aggregate function %d", aggFns[i])
		}

		// Set the output type of the aggregate.
		switch aggFns[i] {
		case distsqlpb.AggregatorSpec_COUNT_ROWS:
			// TODO(jordan): this is a somewhat of a hack. The aggregate functions
			// should come with their own output types, somehow.
			outTyps[i] = types.Int64
		default:
			// Output types are the input types for now.
			outTyps[i] = aggTyps[i][0]
		}

		if err != nil {
			return nil, nil, err
		}
	}

	return funcs, outTyps, nil
}

func (a *orderedAggregator) initWithBatchSize(inputSize, outputSize int) {
	a.input.Init()

	// Twice the input batchSize is allocated to avoid having to check for
	// overflow when outputting.
	a.scratch.ColBatch = NewMemBatchWithSize(a.outputTyps, inputSize*2)
	for i := 0; i < len(a.outputTyps); i++ {
		vec := a.scratch.ColVec(i)
		a.aggregateFuncs[i].Init(a.groupCol, vec)
	}
	a.scratch.outputSize = outputSize
}

func (a *orderedAggregator) Init() {
	a.initWithBatchSize(ColBatchSize, ColBatchSize)
}

func (a *orderedAggregator) Next() ColBatch {
	if a.done {
		a.scratch.SetLength(0)
		return a.scratch
	}
	if a.scratch.resumeIdx >= a.scratch.outputSize {
		// Copy the second part of the output batch into the first and resume from
		// there.
		newResumeIdx := a.scratch.resumeIdx - a.scratch.outputSize
		for i := 0; i < len(a.outputTyps); i++ {
			// According to the aggregate function interface contract, the value at
			// the current index must also be copied.
			a.scratch.ColVec(i).Copy(a.scratch.ColVec(i), uint64(a.scratch.outputSize),
				uint64(a.scratch.resumeIdx+1), a.outputTyps[i])
			a.aggregateFuncs[i].SetOutputIndex(newResumeIdx)
		}
		a.scratch.resumeIdx = newResumeIdx
		if a.scratch.resumeIdx >= a.scratch.outputSize {
			// We still have overflow output values.
			a.scratch.SetLength(uint16(a.scratch.outputSize))
			return a.scratch
		}
	}

	for a.scratch.resumeIdx < a.scratch.outputSize {
		batch := a.input.Next()
		for i, fn := range a.aggregateFuncs {
			fn.Compute(batch, a.aggCols[i])
		}
		a.scratch.resumeIdx = a.aggregateFuncs[0].CurrentOutputIndex()
		if batch.Length() == 0 {
			a.done = true
			break
		}
		// zero out a.groupCol. This is necessary because distinct ors the
		// uniqueness of a value with the groupCol, allowing the operators to be
		// linked.
		copy(a.groupCol, zeroBoolVec)
	}

	if a.scratch.resumeIdx > a.scratch.outputSize {
		a.scratch.SetLength(uint16(a.scratch.outputSize))
	} else {
		a.scratch.SetLength(uint16(a.scratch.resumeIdx))
	}

	return a.scratch
}

// Reset resets the orderedAggregator for another run. Primarily used for
// benchmarks.
func (a *orderedAggregator) Reset() {
	a.done = false
	a.scratch.resumeIdx = 0
	for _, fn := range a.aggregateFuncs {
		fn.Reset()
	}
}

// hashAggregator is an operator that performs an aggregation based on
// specified grouping columns. This operator performs a hash table build at the
// first call to `Next()` which consumes the entire build source. After the
// build phase, the state of the hashAggregator is changed to isAggregating and
// the grouped hash table buckets are then passed as input to an
// orderedAggregator. The output row ordering of this aggregator is arbitrary.
type hashAggregator struct {
	spec aggregatorSpec

	// isAggregating represents whether the hashAggregator is in the building or
	// aggregating phase.
	isAggregating bool

	ht      *hashTable
	builder *hashJoinBuilder

	// orderedAgg is the underlying orderedAggregator used by this
	// hashAggregator once the bucket-groups of the input has been determined.
	orderedAgg *orderedAggregator
}

// NewHashAggregator creates a hashed aggregator on the given grouping
// columns. The input specifications to this function are the same as that of
// the NewOrderedAggregator function.
func NewHashAggregator(
	input Operator,
	colTypes []types.T,
	aggFns []distsqlpb.AggregatorSpec_Func,
	groupCols []uint32,
	aggCols [][]uint32,
) (Operator, error) {
	aggTyps := extractAggTypes(aggCols, colTypes)

	// Only keep relevant output columns, those that are used as input to an
	// aggregation.
	nCols := uint32(len(colTypes))
	keepCol := make([]bool, nCols)

	// compressed represents a mapping between each original column and its index
	// in the new compressed columns set. This is required since we are
	// effectively compressing the original list of columns by only keeping the
	// columns used as input to an aggregation function,
	compressed := make([]uint32, nCols)
	for _, cols := range aggCols {
		for _, col := range cols {
			keepCol[col] = true
		}
	}

	// Map the corresponding aggCols to the new output column indices.
	nOutCols := uint32(0)
	outCols := make([]uint32, 0)
	for i := range keepCol {
		if keepCol[i] {
			outCols = append(outCols, uint32(i))
			compressed[i] = nOutCols
			nOutCols++
		}
	}

	mappedAggCols := make([][]uint32, len(aggCols))
	for aggIdx := range aggCols {
		mappedAggCols[aggIdx] = make([]uint32, len(aggCols[aggIdx]))
		for i := range mappedAggCols[aggIdx] {
			mappedAggCols[aggIdx][i] = compressed[aggCols[aggIdx][i]]
		}
	}

	ht := makeHashTable(
		hashTableBucketSize,
		colTypes,
		groupCols,
		outCols,
	)

	builder := makeHashJoinBuilder(
		ht,
		hashJoinerSourceSpec{
			source:      input,
			eqCols:      groupCols,
			outCols:     outCols,
			sourceTypes: colTypes,
		},
	)

	funcs, outTyps, err := makeAggregateFuncs(aggTyps, aggFns)
	if err != nil {
		return nil, err
	}

	distinctCol := make([]bool, ColBatchSize)

	// op is the underlying batching operop.distinct[op.batchStart:batchEnd]ator used as input by the orderedAgg to
	// create the batches of ColBatchSize from built hashTable.
	op := makeHashAggregatorBatchOp(ht, distinctCol)

	orderedAgg := &orderedAggregator{
		input:          op,
		aggCols:        mappedAggCols,
		aggTyps:        aggTyps,
		groupCol:       distinctCol,
		aggregateFuncs: funcs,
		outputTyps:     outTyps,
	}

	return &hashAggregator{
		spec: aggregatorSpec{
			input:     input,
			colTypes:  colTypes,
			aggFns:    aggFns,
			groupCols: groupCols,
			aggCols:   aggCols,
		},

		ht:      ht,
		builder: builder,

		orderedAgg: orderedAgg,
	}, nil
}

func (ag *hashAggregator) Init() {
	ag.spec.input.Init()
	ag.orderedAgg.Init()
}

func (ag *hashAggregator) Next() ColBatch {
	if !ag.isAggregating {
		ag.build()
	}

	return ag.orderedAgg.Next()
}

// Reset resets the hashAggregator for another run. Primarily used for
// benchmarks.
func (ag *hashAggregator) Reset() {
	ag.ht.size = 0
	ag.isAggregating = false
	ag.orderedAgg.Reset()
}

func (ag *hashAggregator) build() {
	ag.builder.exec()
	ag.isAggregating = true
}

var _ Operator = &hashAggregator{}

// aggregatorSpec represents the specifications for the various aggregation
// operators.
type aggregatorSpec struct {
	// input is the source Operator consumed by the aggregator.
	input Operator
	// colTypes represents the column types corresponding to the input columns.
	colTypes []types.T

	// aggFns represents the list of aggregation functions to be performed.
	aggFns []distsqlpb.AggregatorSpec_Func

	// aggCols is a slice where each index represents a new aggregation function.
	// The slice at each index maps to the input column indices used as input to
	// each corresponding aggregation function.
	aggCols [][]uint32
	// groupCols holds the list of group column indices.
	groupCols []uint32
}

// hashAggregatorBatchOp is a helper operator used by the hashAggregator as
// input to its orderedAggregator. Once the building of the hashTable is
// completed, this batch operator exists to return the next batch of input to
// the orderedAggregator based on the results of the pre-built hashTable.
type hashAggregatorBatchOp struct {
	ht *hashTable

	// sel is an ordered list of indices to select representing the input rows.
	// This selection vector is much bigger than ColBatchSize and should be
	// batched with the hashAggregatorBatchOp operator.
	sel []uint64
	// distinct represents whether each corresponding row is part of a new group.
	distinct []bool

	// distinctCol is a reference to the slice that aggregateFuncs use to
	// determine whether a value is part of the current aggregation group. See
	// aggregateFunc.Init for more information.
	distinctCol []bool

	batchStart uint64
	batch      ColBatch
}

func makeHashAggregatorBatchOp(ht *hashTable, distinctCol []bool) Operator {
	return &hashAggregatorBatchOp{
		distinctCol: distinctCol,
		ht:          ht,
		batch:       NewMemBatch(ht.outTypes),
	}
}

func (op *hashAggregatorBatchOp) Init() {}

func (op *hashAggregatorBatchOp) Next() ColBatch {
	// The selection vector needs to be populated before any batching can be
	// done.
	if op.sel == nil {
		// After the entire hashTable is built, we want to construct the selection
		// vector. This vector would be an ordered list of indices indicating the
		// ordering of the bucket-grouped rows of input. The same linked list is
		// traversed from each head to form this ordered list.
		headID := uint64(0)
		curID := uint64(0)

		// Since next is no longer useful and pre-allocated to the appropriate size,
		// we can use it as the selection vector. This way we don't have to
		// reallocate a huge array.
		op.sel = op.ht.next
		// Since visited is no longer used and pre-allocated to the appropriate
		// size, we can use it for the distinct vector.
		op.distinct = op.ht.visited

		for i := uint64(0); i < op.ht.size; i++ {
			op.distinct[i] = false
			for !op.ht.head[headID] || curID == 0 {
				op.distinct[i] = true
				headID++
				curID = headID
			}
			op.sel[i] = curID - 1
			curID = op.ht.same[curID]
		}
	}

	// Create and return the next batch of input to a maximum size of
	// ColBatchSize. The rows in the new batch is specified by the corresponding
	// slice in the selection vector.
	nSelected := uint16(0)

	batchEnd := op.batchStart + uint64(ColBatchSize)
	if batchEnd > op.ht.size {
		batchEnd = op.ht.size
	}
	nSelected = uint16(batchEnd - op.batchStart)

	copy(op.distinctCol, op.distinct[op.batchStart:batchEnd])

	for i, colIdx := range op.ht.outCols {
		toCol := op.batch.ColVec(i)
		fromCol := op.ht.vals[colIdx]
		toCol.CopyWithSelInt64(fromCol, op.sel[op.batchStart:batchEnd], nSelected, op.ht.valTypes[op.ht.outCols[i]])
	}

	op.batchStart = batchEnd

	op.batch.SetLength(nSelected)
	return op.batch
}

var _ Operator = &hashAggregatorBatchOp{}

// extractGroupTypes returns an array representing the type corresponding to
// each group column. This information is extracted from the group column
// indices and their corresponding column types.
func extractGroupTypes(groupCols []uint32, colTypes []types.T) []types.T {
	groupTyps := make([]types.T, len(groupCols))

	for i, colIdx := range groupCols {
		groupTyps[i] = colTypes[colIdx]
	}

	return groupTyps
}

// extractAggTypes returns a nested array representing the input types
// corresponding to each aggregation function.
func extractAggTypes(aggCols [][]uint32, colTypes []types.T) [][]types.T {
	aggTyps := make([][]types.T, len(aggCols))

	for aggIdx := range aggCols {
		aggTyps[aggIdx] = make([]types.T, len(aggCols[aggIdx]))
		for i, colIdx := range aggCols[aggIdx] {
			aggTyps[aggIdx][i] = colTypes[colIdx]
		}
	}

	return aggTyps
}
