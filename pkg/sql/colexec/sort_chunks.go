// Copyright 2019 The Cockroach Authors.
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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execerror"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
)

// NewSortChunks returns a new sort chunks operator, which sorts its input on
// the columns given in orderingCols. The inputTypes must correspond 1-1 with
// the columns in the input operator. The input tuples must be sorted on first
// matchLen columns.
func NewSortChunks(
	allocator *Allocator,
	input Operator,
	inputTypes []coltypes.T,
	orderingCols []execinfrapb.Ordering_Column,
	matchLen int,
) (Operator, error) {
	if matchLen == len(orderingCols) {
		// input is already ordered on all orderingCols, so there is nothing more
		// to sort.
		return input, nil
	}
	if matchLen < 1 {
		execerror.VectorizedInternalPanic(fmt.Sprintf("Sort Chunks should only be used when the input is "+
			"already ordered on at least one column. matchLen = %d was given.",
			matchLen))
	}
	chunker, err := newChunker(allocator, input, inputTypes, orderingCols[:matchLen])
	if err != nil {
		return nil, err
	}
	sorter, err := newSorter(allocator, chunker, inputTypes, orderingCols[matchLen:])
	if err != nil {
		return nil, err
	}
	return &sortChunksOp{input: chunker, sorter: sorter}, nil
}

type sortChunksOp struct {
	input  *chunker
	sorter resettableOperator
}

func (c *sortChunksOp) ChildCount(verbose bool) int {
	return 0
}

func (c *sortChunksOp) Child(nth int, verbose bool) execinfra.OpNode {
	if nth == 0 {
		return c.input
	}
	execerror.VectorizedInternalPanic(fmt.Sprintf("invalid index %d", nth))
	// This code is unreachable, but the compiler cannot infer that.
	return nil
}

var _ Operator = &sortChunksOp{}

func (c *sortChunksOp) Init() {
	c.input.init()
	c.sorter.Init()
}

func (c *sortChunksOp) Next(ctx context.Context) coldata.Batch {
	for {
		batch := c.sorter.Next(ctx)
		if batch.Length() == 0 {
			if c.input.done() {
				// We're done, so return a zero-length batch.
				return batch
			}
			// We're not yet done - need to process another chunk, so we empty the
			// chunker's buffer and reset the sorter. Note that we do not want to do
			// the full reset of the chunker because we're in the middle of
			// processing of the input to sortChunksOp.
			c.input.emptyBuffer()
			c.sorter.reset()
		} else {
			return batch
		}
	}
}

// chunkerState represents the state of the chunker spooler.
type chunkerState int

const (
	// chunkerReading is the state of the chunker spooler in which it reads a
	// batch from its input and partitions the batch into chunks. Depending on
	// current state of the chunker's buffer and number of chunks in the batch,
	// chunker might stay in chunkerReading state or switch to either of the
	// emitting states.
	chunkerReading chunkerState = iota
	// chunkerEmittingFromBuffer is the state of the chunker spooler in which it
	// prepares to "emit" tuples that have been buffered. All the tuples belong
	// to the same chunk ("emit" is in quotes because the chunker does not emit
	// batches as usual - it, instead, implements spooler interface, and the
	// batches should be accessed through those methods). The chunker transitions
	// to chunkerEmittingFromBuffer state and indicates that the tuples need to
	// be read from the buffer.
	chunkerEmittingFromBuffer
	// chunkerEmittingFromBatch is the state of the chunker spooler in which it
	// prepares to "emit" all chunks that are fully contained within the last
	// read batch (i.e. all chunks except for the last chunk which might include
	// tuples from the next batch). The last chunk within the batch is buffered,
	// the chunker transitions to chunkerReading state and indicates that the
	// tuples need to be read from s.batch.
	chunkerEmittingFromBatch
)

// chunkerReadingState indicates where the spooler needs to read tuples from
// for emitting.
type chunkerReadingState int

const (
	// chunkerReadFromBuffer indicates that the tuples need to be read from the
	// buffer.
	chunkerReadFromBuffer = iota
	// chunkerReadFromBatch indicates that the tuples need to be read from the
	// last read batch directly. Only tuples that are fully contained within the
	// last read batch are "emitted".
	chunkerReadFromBatch
	// inputDone indicates that the input has been fully consumed and there are
	// no more tuples to read from the chunker.
	inputDone
)

// chunker is a spooler that produces chunks from its input when the tuples
// are already ordered on the first matchLen columns. The chunks are not
// emitted in batches as usual when Next()'ed, but, instead, they should be
// accessed via getValues().
//
// Note 1: the chunker assumes that its input produces batches with no
// selection vector, so it always puts a deselector on top of its input. It
// does the coalescing itself, so it does not use an extra coalescer.
// Note 2: the chunker intentionally does not implement resetter interface (if
// it did, the sorter would reset it, but we don't want that since we're likely
// in the middle of processing the input). Instead, sortChunksOp will empty the
// buffer when appropriate.
type chunker struct {
	OneInputNode
	NonExplainable

	allocator *Allocator
	// inputTypes contains the types of all of the columns from input.
	inputTypes []coltypes.T
	// inputDone indicates whether input has been fully consumed.
	inputDone bool
	// alreadySortedCols indicates the columns on which the input is already
	// ordered.
	alreadySortedCols []execinfrapb.Ordering_Column

	// batch is the last read batch from input.
	batch coldata.Batch
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
	// the first one to be emitted from s.batch directly in when reading from
	// batch.
	chunksStartIdx int

	// bufferedTuples is a buffer to store tuples when a chunk is bigger than
	// coldata.BatchSize() or when the chunk is the last in the last read batch
	// (we don't know yet where the end of such chunk is).
	bufferedTuples *bufferedBatch

	readFrom chunkerReadingState
	state    chunkerState
}

var _ spooler = &chunker{}

func newChunker(
	allocator *Allocator,
	input Operator,
	inputTypes []coltypes.T,
	alreadySortedCols []execinfrapb.Ordering_Column,
) (*chunker, error) {
	var err error
	partitioners := make([]partitioner, len(alreadySortedCols))
	for i, col := range alreadySortedCols {
		partitioners[i], err = newPartitioner(inputTypes[col.ColIdx])
		if err != nil {
			return nil, err
		}
	}
	deselector := NewDeselectorOp(allocator, input, inputTypes)
	return &chunker{
		OneInputNode:      NewOneInputNode(deselector),
		allocator:         allocator,
		inputTypes:        inputTypes,
		alreadySortedCols: alreadySortedCols,
		partitioners:      partitioners,
		state:             chunkerReading,
	}, nil
}

func (s *chunker) init() {
	s.input.Init()
	s.bufferedTuples = newBufferedBatch(s.allocator, s.inputTypes, 0 /* initialSize */)
	s.partitionCol = make([]bool, coldata.BatchSize())
	s.chunks = make([]uint64, 0, 16)
}

// done indicates whether the chunker has fully consumed its input.
func (s *chunker) done() bool {
	return s.readFrom == inputDone
}

// prepareNextChunks prepares the chunks for the chunker spooler.
//
// Note: it does not return the batches directly; instead, the chunker
// remembers where the next chunks to be emitted are actually stored. In order
// to access the chunks, getValues() must be used.
func (s *chunker) prepareNextChunks(ctx context.Context) chunkerReadingState {
	for {
		switch s.state {
		case chunkerReading:
			s.batch = s.input.Next(ctx)
			if s.batch.Length() == 0 {
				s.inputDone = true
				if s.bufferedTuples.length > 0 {
					s.state = chunkerEmittingFromBuffer
				} else {
					s.state = chunkerEmittingFromBatch
				}
				continue
			}
			if s.batch.Selection() != nil {
				// We assume that the input has been deselected, so the batch should
				// never have a selection vector set.
				execerror.VectorizedInternalPanic(fmt.Sprintf("unexpected: batch with non-nil selection vector"))
			}

			// First, run the partitioners on our pre-sorted columns to determine the
			// boundaries of the chunks (stored in s.chunks) to sort further.
			copy(s.partitionCol, zeroBoolColumn)
			for i, orderedCol := range s.alreadySortedCols {
				s.partitioners[i].partition(s.batch.ColVec(int(orderedCol.ColIdx)), s.partitionCol,
					uint64(s.batch.Length()))
			}
			s.chunks = boolVecToSel64(s.partitionCol, s.chunks[:0])

			if s.bufferedTuples.length == 0 {
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
				for _, col := range s.alreadySortedCols {
					if err := tuplesDiffer(
						s.inputTypes[col.ColIdx],
						s.bufferedTuples.colVecs[col.ColIdx],
						0, /*aTupleIdx */
						s.batch.ColVec(int(col.ColIdx)),
						0, /* bTupleIdx */
						&differ,
					); err != nil {
						execerror.VectorizedInternalPanic(err)
					}
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
			s.state = chunkerEmittingFromBatch
			return chunkerReadFromBuffer
		case chunkerEmittingFromBatch:
			if s.chunksProcessedIdx+1 < len(s.chunks) {
				// There is at least one chunk that is fully contained within s.batch.
				// We don't know yet whether the tuples from the next batch belong to
				// the last chunk of the current batch, so we will buffer those and can
				// only emit "internal" to s.batch chunks. Additionally, if
				// s.chunksProcessedIdx == 1, then the first chunk was already combined
				// with the buffered tuples and emitted.
				s.chunksStartIdx = s.chunksProcessedIdx
				s.chunksProcessedIdx = len(s.chunks) - 1
				return chunkerReadFromBatch
			} else if s.chunksProcessedIdx == len(s.chunks)-1 {
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
					return inputDone
				}
				execerror.VectorizedInternalPanic(fmt.Sprintf("unexpected: chunkerEmittingFromBatch state" +
					"when s.chunks is fully processed and input is not done"))
			}
		default:
			execerror.VectorizedInternalPanic(fmt.Sprintf("invalid chunker spooler state %v", s.state))
		}
	}
}

// buffer appends all tuples in range [start,end) from s.batch to already
// buffered tuples.
func (s *chunker) buffer(start uint16, end uint16) {
	if start == end {
		return
	}
	s.allocator.PerformOperation(s.bufferedTuples.colVecs, func() {
		for i := 0; i < len(s.bufferedTuples.colVecs); i++ {
			s.bufferedTuples.colVecs[i].Append(
				coldata.SliceArgs{
					ColType:     s.inputTypes[i],
					Src:         s.batch.ColVec(i),
					DestIdx:     s.bufferedTuples.length,
					SrcStartIdx: uint64(start),
					SrcEndIdx:   uint64(end),
				},
			)
		}
		s.bufferedTuples.length += uint64(end - start)
	})
}

func (s *chunker) spool(ctx context.Context) {
	s.readFrom = s.prepareNextChunks(ctx)
}

func (s *chunker) getValues(i int) coldata.Vec {
	switch s.readFrom {
	case chunkerReadFromBuffer:
		return s.bufferedTuples.colVecs[i].Window(s.inputTypes[i], 0 /* start */, s.bufferedTuples.length)
	case chunkerReadFromBatch:
		return s.batch.ColVec(i).Window(s.inputTypes[i], s.chunks[s.chunksStartIdx], s.chunks[len(s.chunks)-1])
	default:
		execerror.VectorizedInternalPanic(fmt.Sprintf("unexpected chunkerReadingState in getValues: %v", s.state))
		// This code is unreachable, but the compiler cannot infer that.
		return nil
	}
}

func (s *chunker) getNumTuples() uint64 {
	switch s.readFrom {
	case chunkerReadFromBuffer:
		return s.bufferedTuples.length
	case chunkerReadFromBatch:
		return s.chunks[len(s.chunks)-1] - s.chunks[s.chunksStartIdx]
	case inputDone:
		return 0
	default:
		execerror.VectorizedInternalPanic(fmt.Sprintf("unexpected chunkerReadingState in getNumTuples: %v", s.state))
		// This code is unreachable, but the compiler cannot infer that.
		return 0
	}
}

func (s *chunker) getPartitionsCol() []bool {
	switch s.readFrom {
	case chunkerReadFromBuffer:
		// There is a single chunk in the buffer, so, per spooler's contract, we
		// return nil.
		return nil
	case chunkerReadFromBatch:
		if s.chunksStartIdx+1 == len(s.chunks)-1 {
			// There is a single chunk that is fully contained within s.batch, so,
			// per spooler's contract, we return nil.
			return nil
		}
		copy(s.partitionCol, zeroBoolColumn)
		for i := s.chunksStartIdx; i < len(s.chunks)-1; i++ {
			// getValues returns a slice starting from s.chunks[s.chunksStartIdx], so
			// we need to account for that by shifting as well.
			s.partitionCol[s.chunks[i]-s.chunks[s.chunksStartIdx]] = true
		}
		return s.partitionCol
	default:
		execerror.VectorizedInternalPanic(fmt.Sprintf("unexpected chunkerReadingState in getPartitionsCol: %v", s.state))
		// This code is unreachable, but the compiler cannot infer that.
		return nil
	}
}

func (s *chunker) getWindowedBatch(startIdx, endIdx uint64) coldata.Batch {
	execerror.VectorizedInternalPanic("getWindowedBatch is not implemented on chunker spooler")
	// This code is unreachable, but the compiler cannot infer that.
	return nil
}

func (s *chunker) emptyBuffer() {
	s.bufferedTuples.reset()
}
