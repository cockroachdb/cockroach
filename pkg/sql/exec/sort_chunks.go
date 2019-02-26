// Copyright 2019 The Cockroach Authors.
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

// NewSortChunks returns a new sort chunks operator, which sorts its input on
// the columns given in orderingCols. The inputTypes must correspond 1-1 with
// the columns in the input operator. The input tuples must be sorted on first
// matchLen columns.
func NewSortChunks(
	input Operator, inputTypes []types.T, orderingCols []distsqlpb.Ordering_Column, matchLen int,
) (Operator, error) {
	if matchLen == len(orderingCols) {
		// input is already ordered on all orderingCols, so the sorter is a noop.
		return &noopOperator{input: input}, nil
	}
	chunker, err := newChunker(input, inputTypes, matchLen)
	if err != nil {
		return nil, err
	}
	sorter, err := newSorter(chunker, inputTypes, orderingCols[matchLen:])
	if err != nil {
		return nil, err
	}
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
	// "emits" tuples that have been buffered. All the tuples belong to the same
	// chunk ("emits" is in quotes because the chunker does not emit batches as
	// usual - it, instead, implements spooler interface, and the batches should
	// be accessed through those methods). The chunker transitions to
	// chunkerEmittedFromBuffer state and emits a single nil batch.
	chunkerEmittingFromBuffer
	// chunkerEmittedFromBuffer is the state of the chunker operator which
	// indicates that the tuples need to be read from the buffer. The chunker
	// transitions to chunkerEmittingFromBatchState.
	chunkerEmittedFromBuffer
	// chunkerEmittingFromBatch is the state of the chunker operator in which it
	// "emits" all chunks that are fully contained within the last read batch
	// (i.e. all except for the last chunk which might include tuples from the
	// next batch). The chunker transitions to chunkerEmittedFromBatch state.
	chunkerEmittingFromBatch
	// chunkerEmittedFromBatch is the state of the chunker operator which
	// indicates that the tuples need to be read from the last read batch
	// directly. Only tuples that are fully contained within the last read batch
	// are "emitted". The last chunk is buffered, and the chunker transitions to
	// chunkerReading state.
	chunkerEmittedFromBatch
)

// chunker is a spooler that produces chunks from its input when the tuples
// are already ordered on the first matchLen columns. The chunks are not
// emitted in batches as usual when Next()'ed, but, instead, they can be
// accessed via getValues().
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

	// chunks contains the indices of the first tuples within different chunks
	// found in the last read batch. Note: the first chunk might be a part of
	// the chunk that is currently being buffered, and similarly the last chunk
	// might include tuples from the batches to be read.
	chunks []uint64
	// chunksProcessedIdx indicates which chunk within s.chunks should be
	// processed next.
	chunksProcessedIdx int
	// chunksStartIdx indicates the index of the chunk within s.chunks that is
	// the first one to be emitted from s.batch directly.
	chunksStartIdx int

	// buffered indicates the number of currently buffered tuples.
	buffered uint64
	// bufferedValues is a buffer to store tuples when a chunk is bigger than
	// ColBatchSize or when the chunk is the last in the last read batch (we
	// don't know yet where the end of such chunk is).
	bufferedValues []ColVec

	// initialized indicates whether chunker has already been initialized (used
	// to do a noop on all (except for the first) calls to chunker.Init()).
	initialized bool
	output      ColBatch
	state       chunkerState
}

func newChunker(input Operator, inputTypes []types.T, matchLen int) (*chunker, error) {
	var err error
	partitioners := make([]partitioner, matchLen)
	for col := 0; col < matchLen; col++ {
		partitioners[col], err = newPartitioner(inputTypes[col])
		if err != nil {
			return nil, err
		}
	}
	return &chunker{
		input:        NewDeselectorOp(input, inputTypes),
		inputTypes:   inputTypes,
		matchLen:     matchLen,
		partitioners: partitioners,
		state:        chunkerReading,
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
		s.chunks = make([]uint64, 0, 16)
		s.initialized = true
	}
}

// prepareNextChunks prepares the chunks for the sort chunks operator.
// Note: it DOES NOT return the batches directly; instead, the chunker is
// transitioned into either chunkerEmittedFromBuffer or chunkerEmittedFromBatch
// state to indicate where the chunks are actually stored. In order to access
// the tuples, getValues() must be used.
func (s *chunker) prepareNextChunks() {
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

			copy(s.partitionCol, zeroBoolVec)
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
				for i := 0; i < s.matchLen; i++ {
					tuplesDiffer, err := tuplesDiffer(s.inputTypes[i])
					if err != nil {
						panic(err)
					}
					tuplesDiffer(s.bufferedValues[i], 0 /*aTupleIdx */, s.batch.ColVec(i), 0 /* bTupleIdx */, &differ)
				}
				if differ {
					// Buffered tuples comprise a full chunk, so we proceed to emitting
					// it.
					s.state = chunkerEmittingFromBuffer
					continue
				}

				// The first tuple of s.batch belongs to the chunk that is being
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
			s.state = chunkerEmittedFromBuffer
			return
		case chunkerEmittedFromBuffer:
			s.state = chunkerEmittingFromBatch
			fallthrough
		case chunkerEmittingFromBatch:
			s.state = chunkerEmittedFromBatch
			if s.chunksProcessedIdx+1 < len(s.chunks) {
				s.chunksStartIdx = s.chunksProcessedIdx
				s.chunksProcessedIdx = len(s.chunks) - 1
				return
			}
		case chunkerEmittedFromBatch:
			if s.chunksProcessedIdx == len(s.chunks)-1 {
				// Other tuples might belong to this chunk, so we buffer it.
				s.buffer(uint16(s.chunks[s.chunksProcessedIdx]), s.batch.Length())
				// All tuples in s.batch have been processed, so we reset s.chunks and
				// the corresponding variables.
				s.chunks = s.chunks[:0]
				s.chunksProcessedIdx = 0
				s.state = chunkerReading
			} else {
				// All tuples in s.batch have been emitted.
				if s.inputDone {
					return
				}
				panic(fmt.Sprintf("unexpected: chunkerEmittedFromBatch state" +
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

func (s *chunker) spool() {
	s.prepareNextChunks()
}

func (s *chunker) getValues(i int) ColVec {
	switch s.state {
	case chunkerEmittedFromBuffer:
		return s.bufferedValues[i].Slice(s.inputTypes[i], 0 /* start */, s.buffered)
	case chunkerEmittedFromBatch:
		return s.batch.ColVec(i).Slice(s.inputTypes[i], s.chunks[s.chunksStartIdx], s.chunks[len(s.chunks)-1])
	default:
		panic(fmt.Sprintf("unexpected state in getValues: %v", s.state))
	}
}

func (s *chunker) getNumTuples() uint64 {
	switch s.state {
	case chunkerEmittedFromBuffer:
		return s.buffered
	case chunkerEmittedFromBatch:
		return s.chunks[len(s.chunks)-1] - s.chunks[s.chunksStartIdx]
	default:
		panic(fmt.Sprintf("unexpected state in getNumTuples: %v", s.state))
	}
}

func (s *chunker) getPartitionsCol() []bool {
	switch s.state {
	case chunkerEmittedFromBuffer:
		// There is a single chunk in the buffer, so, per spooler's contract, we
		// return nil.
		return nil
	case chunkerEmittedFromBatch:
		if s.chunksStartIdx+1 == len(s.chunks)-1 {
			// There is a single chunk that is fully contained within s.batch, so,
			// per spooler's contract, we return nil.
			return nil
		}
		copy(s.partitionCol, zeroBoolVec)
		for i := s.chunksStartIdx; i < len(s.chunks)-1; i++ {
			// getValues returns a slice starting from s.chunks[s.chunksStartIdx], so
			// we need to account for that by shifting as well.
			s.partitionCol[s.chunks[i]-s.chunks[s.chunksStartIdx]] = true
		}
		return s.partitionCol
	default:
		panic(fmt.Sprintf("unexpected state in getPartitionsCol: %v", s.state))
	}
}

func (s *chunker) reset() {
	s.buffered = 0
}
