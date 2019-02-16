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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/distsqlpb"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
)

// NewSorter returns a new sort operator, which sorts its input on the columns
// given in orderingCols. The inputTypes must correspond 1-1 with the columns in
// the input operator.
func NewSorter(
	input Operator, inputTypes []types.T, orderingCols []distsqlpb.Ordering_Column,
) (Operator, error) {
	sorters := make([]colSorter, len(orderingCols))
	partitioners := make([]partitioner, len(orderingCols)-1)
	isOrderingCol := make([]bool, len(inputTypes))

	var err error
	for i, ord := range orderingCols {
		sorters[i], err = newSingleSorter(inputTypes[ord.ColIdx], ord.Direction)
		if err != nil {
			return nil, err
		}
		if i < len(orderingCols)-1 {
			partitioners[i], err = newPartitioner(inputTypes[ord.ColIdx])
			if err != nil {
				return nil, err
			}
		}
		// All ordering columns will have been sorted properly by the time the spool
		// phase is over - only the columns that weren't sort columns will need to
		// be reordered.
		isOrderingCol[ord.ColIdx] = true
	}

	return &sortOp{
		input:         input,
		inputTypes:    inputTypes,
		sorters:       sorters,
		partitioners:  partitioners,
		orderingCols:  orderingCols,
		isOrderingCol: isOrderingCol,
		state:         sortSpooling,
	}, nil
}

type sortOp struct {
	input Operator

	// inputTypes contains the types of all of the columns from input.
	inputTypes []types.T
	// orderingCols is the ordered list of column orderings that the sorter should
	// sort on.
	orderingCols []distsqlpb.Ordering_Column
	// isOrderingCol is set to true for every column that will have been pre-sorted
	// by the time the spool phase is finished. This will be true for all of the
	// sort columns except for the final one. The rest of the columns will not be
	// sorted yet, and will need to be sorted before outputting by rearrangement
	// to the order specified by the order field.
	isOrderingCol []bool
	// sorters contains one colSorter per sort column.
	sorters []colSorter
	// partitioners contains one partitioner per sort column except for the last,
	// which doesn't need to be partitioned.
	partitioners []partitioner

	// values stores all the values from the input after spooling. Each ColVec in
	// this slice is the entire column from the input, since this operator needs
	// to globally sort its input.
	values []ColVec
	// order maintains the order of tuples in the batch, after sorting. The value
	// at index i in order is the ordinal value of the tuple in the input that
	// belongs at index i. For example, if the input column to sort was
	// [c,b,a,d], the order vector after sorting would be [2,1,0,3].
	order []uint64
	// spooledTuples is the number of tuples spooled.
	spooledTuples uint64
	// emitted is the number of tuples emitted so far.
	emitted uint64
	// state is the current state of the sort.
	state sortState

	workingSpace []uint64
	output       ColBatch
}

// colSorter is a single-column sorter, specialized on a particular type.
type colSorter interface {
	// init prepares this sorter, given a particular ColVec and an order vector,
	// which must be the same size as the input ColVec and will be permuted with
	// the same swaps as the column. workingSpace is a vector of the same size as
	// the column that is needed for temporary space.
	init(col ColVec, order []uint64, workingSpace []uint64)
	// sort globally sorts this sorter's column.
	sort()
	// sortPartitions sorts this sorter's column once for every partition in the
	// partition slice.
	sortPartitions(partitions []uint64)
	// reorder reorders this sorter's column according to its order vector.
	reorder()
}

func (p *sortOp) Init() {
	p.input.Init()
	p.output = NewMemBatch(p.inputTypes)
	p.values = make([]ColVec, len(p.inputTypes))
	for i := 0; i < len(p.inputTypes); i++ {
		p.values[i] = newMemColumn(p.inputTypes[i], 0)
	}
}

// sortState represents the state of the sort operator.
type sortState int

const (
	// sortSpooling is the initial state of the operator, where it must spool all
	// input data and sort it.
	sortSpooling sortState = iota
	// sortEmitting is the second state of the operator, indicating that each call
	// to Next will return another chunk of the sorted data.
	sortEmitting
)

func (p *sortOp) Next() ColBatch {
	switch p.state {
	case sortSpooling:
		p.spoolAndSort()
		p.state = sortEmitting
		fallthrough
	case sortEmitting:
		newEmitted := p.emitted + ColBatchSize
		if newEmitted > p.spooledTuples {
			newEmitted = p.spooledTuples
		}
		p.output.SetLength(uint16(newEmitted - p.emitted))
		if p.output.Length() == 0 {
			return p.output
		}

		for j := 0; j < len(p.values); j++ {
			if p.isOrderingCol[j] {
				// the vec is already sorted, so just fill it directly.
				p.output.ColVec(j).Copy(p.values[j], p.emitted, newEmitted, p.inputTypes[j])
			} else {
				p.output.ColVec(j).CopyWithSelInt64(p.values[j], p.order[p.emitted:], p.output.Length(), p.inputTypes[j])
			}
		}
		p.emitted = newEmitted
		return p.output
	}
	panic(fmt.Sprintf("invalid sort state %v", p.state))
}

func (p *sortOp) spoolAndSort() {
	batch := p.input.Next()
	var nTuples uint64
	// First, copy all vecs into values.
	for ; batch.Length() != 0; batch = p.input.Next() {
		for i := 0; i < len(p.values); i++ {
			if batch.Selection() == nil {
				p.values[i].Append(batch.ColVec(i),
					p.inputTypes[i],
					nTuples,
					batch.Length(),
				)
			} else {
				p.values[i].AppendWithSel(batch.ColVec(i),
					batch.Selection(),
					batch.Length(),
					p.inputTypes[i],
					nTuples,
				)
			}
		}
		nTuples += uint64(batch.Length())
	}
	p.spooledTuples = nTuples

	// Allocate p.order and p.workingSpace if it hasn't been allocated yet or the
	// underlying memory is insufficient.
	if p.order == nil {
		p.order = make([]uint64, nTuples)
		p.workingSpace = make([]uint64, nTuples)
	} else if uint64(cap(p.order)) < nTuples {
		p.order = make([]uint64, nTuples)
		p.workingSpace = make([]uint64, nTuples)
	}
	p.order = p.order[:nTuples]
	p.workingSpace = p.workingSpace[:nTuples]

	// Initialize the order vector to the ordinal positions within the input set.
	for i := uint64(0); i < uint64(len(p.order)); i++ {
		p.order[i] = i
	}

	for i := range p.orderingCols {
		p.sorters[i].init(p.values[p.orderingCols[i].ColIdx], p.order, p.workingSpace)
	}

	// Now, sort each column in turn. The first column doesn't need special
	// treatment - we just globally sort it.

	p.sorters[0].sort()
	if len(p.sorters) == 1 {
		// We're done sorting. Transition to emitting.
		return
	}

	// The rest of the columns need p sorts, one per partition in the previous
	// column. For example, in a two column sort:
	//
	// 1  b
	// 2  b
	// 1  a
	// 2  a
	//
	// We'll first sort the first column:
	//
	// 1  b
	// 1  a
	// 2  b
	// 2  a
	//
	// Then, for each group in the sorted, first column, we sort the second col:
	//
	// 1 a
	// 1 b
	// 2 a
	// 2 b

	outputCol := make([]bool, nTuples)
	partitions := make([]uint64, 0, 16)
	for i, sorter := range p.sorters[1:] {
		// We partition the previous column by running an ordered distinct operation
		// on it, ORing the results together with each subsequent column. This
		// produces a distinct vector (a boolean vector that has true in each
		// position that is different from the last position).
		p.partitioners[i].partition(p.values[p.orderingCols[i].ColIdx], outputCol, nTuples)
		// Convert the distinct vector into a selection vector - a vector of indices
		// that were true in the distinct vector.
		partitions = boolVecToSel64(outputCol, partitions[:0])
		// Reorder the column we're about to sort, based on the swaps we've seen in
		// the sort so far.
		sorter.reorder()
		// For each partition (set of tuples that are identical in all of the sort
		// columns we've seen so far), sort based on the new column.
		sorter.sortPartitions(partitions)
	}
}

func (p *sortOp) reset() {
	p.emitted = 0
	p.spooledTuples = 0
	p.state = sortSpooling
}

// NewSortChunks returns a new sort chunks operator, which sorts its input on
// the columns given in orderingCols. The inputTypes must correspond 1-1 with
// the columns in the input operator. The input tuples must be sorted on first
// matchLen columns.
func NewSortChunks(
	input Operator, inputTypes []types.T, orderingCols []distsqlpb.Ordering_Column, matchLen int,
) (Operator, error) {
	chunker, err := newChunker(input, inputTypes, matchLen)
	if err != nil {
		return nil, err
	}

	var sorter Operator
	if len(orderingCols) > matchLen {
		sorter, err = NewSorter(chunker, inputTypes, orderingCols[matchLen:])
	} else {
		sorter = &noopOperator{input: chunker}
	}
	if err != nil {
		return nil, err
	}
	// TODO(yuzefovich): current implementation of sort chunks will be returning
	// batches of sizes equal to sizes of the corresponding chunks. Should we use
	// a coalescer operator around it?
	return &sortChunksOp{input: chunker, sorter: sorter}, nil
}

type sortChunksOp struct {
	input  *chunker
	sorter Operator
}

func (c *sortChunksOp) Init() {
	c.input.Init()
	c.sorter.Init()
}

func (c *sortChunksOp) Next() ColBatch {
	for {
		batch := c.sorter.Next()
		if batch.Length() == 0 {
			if c.input.inputDone {
				// We're done, so return a zero-length batch.
				return batch
			}
			// We're not yet done - need to process another chunk.
			if resetter, ok := c.sorter.(resetter); ok {
				resetter.reset()
			} else {
				panic(fmt.Sprintf("sorter doesn't implement resetter interface"))
			}
		} else {
			return batch
		}
	}
}

// chunkerState represents the state of the chunker operator.
type chunkerState int

const (
	// chunkerReading is the state of the chunker operator in which it reads a
	// batch from its input and partitions the batch into chunks. Depending on
	// current state of the chunker's buffer and number of chunks in the batch,
	// chunker might stay in chunkerReading state or switch to either of the
	// emitting states.
	chunkerReading chunkerState = iota
	// chunkerEmittingFromBuffer is the state of the chunker operator in which it
	// emits tuples that have been buffered. All the tuples belong to the same
	// chunk. After all tuples are emitted, the buffer is cleared, a zero-length
	// batch is emitted to indicate the end of the chunk, and the chunker
	// transitions into chunkerEmittingFromBatch state.
	chunkerEmittingFromBuffer
	// chunkerEmittingFromBatch is the state of the chunker operator in which it
	// emits all chunks that are fully-contained within the last read batch (i.e.
	// all except the last chunk which might include tuples from the next batch).
	// The last chunk is buffered, and the chunker transitions into
	// chunkerReading state.
	chunkerEmittingFromBatch
)

// chunker is an operator that produces chunks from its input when the tuples
// are already ordered on the first matchLen columns. To indicate the end of a
// chunk, the chunker emits a single zero-length batch. To indicate that the
// input has been fully consumed it emits the second zero-length batch (and
// will continue to do so when Next()'ed).
//
// Note: chunker assumes that its input produces batches with no selection
// vector, so it always puts a deselection operator on top of its input.
type chunker struct {
	input Operator
	// inputTypes contains the types of all of the columns from input.
	inputTypes []types.T
	// inputDone indicates whether input has been fully consumed.
	inputDone bool
	// matchLen indicates the number of first columns on which input is ordered.
	matchLen int

	// batch is the last read batch from input.
	batch ColBatch
	// partitioners contains one partitioner for each of matchLen first already
	// ordered columns.
	partitioners []partitioner
	// partitionCol is a bool slice for partitioners' output to be ORed.
	partitionCol []bool
	// initPartitionCol is a slice of the same size with partitionCol initialized
	// with false to use for copying into partitionCol to reset the latter.
	initPartitionCol []bool
	// differentiators contains one tupleDifferentiator for each of matchLen
	// first columns.
	differentiators []tupleDifferentiator

	// chunks contains the indices of the first tuples within different chunks
	// found in the last read batch. Note: the first chunk might be a part of
	// the chunk that is currently being buffered, and similarly the last chunk
	// might include tuples from the batches to be read.
	chunks []uint64
	// chunksProcessedIdx indicates which chunk within s.chunks should be
	// processed next.
	chunksProcessedIdx int
	// shouldEmitChunkEnd indicates whether a zero-length batch should be emitted
	// to indicate the end of the last emitted chunk.
	shouldEmitChunkEnd bool

	// buffered indicates the number of currently buffered tuples.
	buffered uint64
	// emittedFromBuffer indicates the number of already emitted tuples from
	// buffer.
	emittedFromBuffer uint64
	// bufferedValues is a buffer to store tuples when a chunk is bigger than
	// ColBatchSize or when the chunk is the last in the last read batch (we
	// don't know yet where the end of such chunk is).
	bufferedValues []ColVec

	// initialized indicates whether chunker has already been initialized (used
	// to do a noop on all except for the first call to chunker.Init()).
	initialized bool
	output      ColBatch
	state       chunkerState
}

func newChunker(input Operator, inputTypes []types.T, matchLen int) (*chunker, error) {
	var err error
	partitioners := make([]partitioner, matchLen)
	differentiators := make([]tupleDifferentiator, matchLen)
	for col := 0; col < matchLen; col++ {
		partitioners[col], err = newPartitioner(inputTypes[col])
		if err != nil {
			return nil, err
		}
		differentiators[col], err = newTupleDifferentiator(inputTypes[col])
		if err != nil {
			return nil, err
		}
	}
	return &chunker{
		input:           NewDeselectorOp(input, inputTypes),
		inputTypes:      inputTypes,
		matchLen:        matchLen,
		partitioners:    partitioners,
		differentiators: differentiators,
		state:           chunkerReading,
	}, nil
}

func (s *chunker) Init() {
	if !s.initialized {
		s.input.Init()
		s.output = NewMemBatch(s.inputTypes)
		s.bufferedValues = make([]ColVec, len(s.inputTypes))
		for i := 0; i < len(s.inputTypes); i++ {
			s.bufferedValues[i] = newMemColumn(s.inputTypes[i], 0)
		}
		s.partitionCol = make([]bool, ColBatchSize)
		s.initPartitionCol = make([]bool, ColBatchSize)
		s.chunks = make([]uint64, 0, 16)
		s.initialized = true
	}
}

func (s *chunker) Next() ColBatch {
	for {
		switch s.state {
		case chunkerReading:
			s.batch = s.input.Next()
			if s.batch.Length() == 0 {
				s.inputDone = true
				if s.buffered > 0 {
					s.state = chunkerEmittingFromBuffer
				} else {
					s.state = chunkerEmittingFromBatch
				}
				continue
			}
			if s.batch.Selection() != nil {
				// We assume that the input has been deselected, so the batch should
				// never have a selection vector set.
				panic(fmt.Sprintf("unexpected: batch with non-nil selection vector"))
			}

			// TODO(yuzefovich): is this the best way to reinitialize s.partitionCol
			// to false?
			copy(s.partitionCol, s.initPartitionCol)
			for i, partitioner := range s.partitioners {
				partitioner.partition(s.batch.ColVec(i), s.partitionCol, uint64(s.batch.Length()))
			}
			s.chunks = boolVecToSel64(s.partitionCol, s.chunks[:0])

			if s.buffered == 0 {
				// There are no buffered tuples, so a new chunk starts in the current
				// batch.
				if len(s.chunks) > 1 {
					// There is at least one chunk that is fully contained within
					// s.batch, so we proceed to emitting it.
					s.state = chunkerEmittingFromBatch
					continue
				}
				// All tuples in s.batch belong to the same chunk. Possibly tuples from
				// the next batch will also belong to this chunk, so we buffer the full
				// s.batch.
				s.buffer(0 /* start */, s.batch.Length())
				s.state = chunkerReading
				continue
			} else {
				// There are some buffered tuples, so we need to check whether the
				// first tuple of s.batch belongs to the chunk that is being buffered.
				differ := false
				for i, differentiator := range s.differentiators {
					differentiator.differ(s.bufferedValues[i], 0, s.batch.ColVec(i), 0, &differ)
				}
				if differ {
					// Buffered tuples comprise a full chunk, so we proceed to emitting
					// it.
					s.state = chunkerEmittingFromBuffer
					continue
				}

				// The first tuple of s.batch belongs to the chunks that is being
				// buffered.
				if len(s.chunks) == 1 {
					// All tuples in s.batch belong to the same chunk that is being
					// buffered. Possibly tuples from the next batch will also belong to
					// this chunk, so we buffer the full s.batch.
					s.buffer(0 /* start */, s.batch.Length())
					s.state = chunkerReading
					continue
				}
				// First s.chunks[1] tuples belong to the same chunk that is being
				// buffered, so we buffer them and proceed to emitting all buffered
				// tuples.
				s.buffer(0 /* start */, uint16(s.chunks[1]))
				s.chunksProcessedIdx = 1
				s.state = chunkerEmittingFromBuffer
				continue
			}
		case chunkerEmittingFromBuffer:
			newEmitted := s.emittedFromBuffer + ColBatchSize
			if newEmitted > s.buffered {
				newEmitted = s.buffered
			}
			s.output.SetLength(uint16(newEmitted - s.emittedFromBuffer))
			if s.output.Length() == 0 {
				s.state = chunkerEmittingFromBatch
				s.buffered = 0
				s.emittedFromBuffer = 0
				s.shouldEmitChunkEnd = false
				return s.output
			}
			for i := 0; i < len(s.bufferedValues); i++ {
				s.output.ColVec(i).Copy(s.bufferedValues[i], s.emittedFromBuffer, newEmitted, s.inputTypes[i])
			}
			s.emittedFromBuffer = newEmitted
			return s.output
		case chunkerEmittingFromBatch:
			if s.shouldEmitChunkEnd {
				// Emit a zero-length batch to indicate the end of the last emitted
				// chunk.
				s.output.SetLength(0)
				s.shouldEmitChunkEnd = false
				return s.output
			}
			if s.chunksProcessedIdx+1 < len(s.chunks) {
				// This chunk is fully contained within s.batch, so we emit it right
				// away.
				chunkStart := s.chunks[s.chunksProcessedIdx]
				chunkEnd := s.chunks[s.chunksProcessedIdx+1]
				s.output.SetLength(uint16(chunkEnd - chunkStart))
				for i := 0; i < len(s.inputTypes); i++ {
					s.output.ColVec(i).Copy(s.batch.ColVec(i), chunkStart, chunkEnd, s.inputTypes[i])
				}
				s.chunksProcessedIdx++
				s.shouldEmitChunkEnd = true
				return s.output
			} else if s.chunksProcessedIdx == len(s.chunks)-1 {
				// Other tuples might belong to this chunk, so we buffer it.
				s.buffer(uint16(s.chunks[s.chunksProcessedIdx]), s.batch.Length())
				// All tuples in s.batch have been processed, so we reset s.chunks and
				// the corresponding variables.
				s.chunks = s.chunks[:0]
				s.chunksProcessedIdx = 0
				s.shouldEmitChunkEnd = false
				s.state = chunkerReading
			} else {
				// All tuples in s.batch have been emitted.
				if s.inputDone {
					s.output.SetLength(0)
					return s.output
				}
				panic(fmt.Sprintf("unexpected: chunkerEmittingFromBatch state" +
					"when s.chunks is fully processed and input is not done"))
			}
		default:
			panic(fmt.Sprintf("invalid chunker operator state %v", s.state))
		}
	}
}

// buffer appends all tuples in range [start,end) from s.batch to already
// buffered tuples.
func (s *chunker) buffer(start uint16, end uint16) {
	if start == 0 {
		for i := 0; i < len(s.bufferedValues); i++ {
			s.bufferedValues[i].Append(
				s.batch.ColVec(i),
				s.inputTypes[i],
				s.buffered,
				end,
			)
		}
	} else {
		for i := 0; i < len(s.bufferedValues); i++ {
			s.bufferedValues[i].AppendSlice(
				s.batch.ColVec(i),
				s.inputTypes[i],
				s.buffered,
				start,
				end,
			)
		}
	}
	s.buffered += uint64(end - start)
}
