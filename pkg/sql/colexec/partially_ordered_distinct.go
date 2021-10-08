// Copyright 2020 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// newPartiallyOrderedDistinct creates a distinct operator on the given
// distinct columns when we have partial ordering on some of the distinct
// columns.
func newPartiallyOrderedDistinct(
	allocator *colmem.Allocator,
	input colexecop.Operator,
	distinctCols []uint32,
	orderedCols []uint32,
	typs []*types.T,
	nullsAreDistinct bool,
	errorOnDup string,
) (colexecop.Operator, error) {
	if len(orderedCols) == 0 || len(orderedCols) == len(distinctCols) {
		return nil, errors.AssertionFailedf(
			"partially ordered distinct wrongfully planned: numDistinctCols=%d "+
				"numOrderedCols=%d", len(distinctCols), len(orderedCols))
	}
	chunker, err := newChunker(allocator, input, typs, orderedCols, nullsAreDistinct)
	if err != nil {
		return nil, err
	}
	chunkerOperator := newChunkerOperator(allocator, chunker, typs)
	// distinctUnorderedCols will contain distinct columns that are not present
	// among orderedCols. The unordered distinct operator will use these columns
	// to find distinct tuples within "chunks" of tuples that are the same on the
	// ordered columns.
	distinctUnorderedCols := make([]uint32, 0, len(distinctCols)-len(orderedCols))
	for _, distinctCol := range distinctCols {
		isOrdered := false
		for _, orderedCol := range orderedCols {
			if orderedCol == distinctCol {
				isOrdered = true
				break
			}
		}
		if !isOrdered {
			distinctUnorderedCols = append(distinctUnorderedCols, distinctCol)
		}
	}
	distinct := NewUnorderedDistinct(allocator, chunkerOperator, distinctUnorderedCols, typs, nullsAreDistinct, errorOnDup)
	return &partiallyOrderedDistinct{
		input:    chunkerOperator,
		distinct: distinct.(colexecop.ResettableOperator),
	}, nil
}

// partiallyOrderedDistinct implements DISTINCT operation using a combination
// of chunkerOperator and unorderedDistinct. It's only job is to check whether
// the input has been fully processed and, if not, to move to the next chunk
// (where "chunk" is all tuples that are equal on the ordered columns).
type partiallyOrderedDistinct struct {
	colexecop.InitHelper

	input    *chunkerOperator
	distinct colexecop.ResettableOperator
}

var _ colexecop.Operator = &partiallyOrderedDistinct{}

func (p *partiallyOrderedDistinct) ChildCount(bool) int {
	return 1
}

func (p *partiallyOrderedDistinct) Child(nth int, _ bool) execinfra.OpNode {
	if nth == 0 {
		return p.input
	}
	colexecerror.InternalError(errors.AssertionFailedf("invalid index %d", nth))
	// This code is unreachable, but the compiler cannot infer that.
	return nil
}

func (p *partiallyOrderedDistinct) Init(ctx context.Context) {
	if !p.InitHelper.Init(ctx) {
		return
	}
	p.distinct.Init(p.Ctx)
}

func (p *partiallyOrderedDistinct) Next() coldata.Batch {
	for {
		batch := p.distinct.Next()
		if batch.Length() == 0 {
			if p.input.done() {
				// We're done, so return a zero-length batch.
				return coldata.ZeroBatch
			}
			// p.distinct will reset p.Input.
			p.distinct.Reset(p.Ctx)
		} else {
			return batch
		}
	}
}

func newChunkerOperator(
	allocator *colmem.Allocator, input *chunker, inputTypes []*types.T,
) *chunkerOperator {
	return &chunkerOperator{
		input:         input,
		inputTypes:    inputTypes,
		windowedBatch: allocator.NewMemBatchNoCols(inputTypes, coldata.BatchSize()),
	}
}

// chunkerOperator is an adapter from chunker to Operator interface. It outputs
// all tuples from a single chunk followed by zero-length batches until it is
// reset.
// It will have returned all tuples from all of the chunks only when it returns
// a zero-length *and* done() method returns true (i.e. a zero-length batch
// indicates the end of a chunk, but when done() returns true, it indicates
// that the input has been fully processed).
type chunkerOperator struct {
	colexecop.InitHelper

	input      *chunker
	inputTypes []*types.T
	// haveChunksToEmit indicates whether we have spooled input and still there
	// are more chunks to emit.
	haveChunksToEmit bool
	// numTuplesInChunks stores the number of tuples that are currently spooled
	// by input.
	numTuplesInChunks int
	// currentChunkFinished indicates whether we have emitted all tuples from the
	// current chunk and should be returning a zero-length batch.
	currentChunkFinished bool
	// newChunksCol, when non-nil, stores the boundaries of chunks. Every true
	// value indicates that a new chunk begins at the corresponding index. If
	// newChunksCol is nil, all spooled tuples belong to the same chunk.
	newChunksCol []bool
	// outputTupleStartIdx indicates the index of the first tuple to be included
	// in the output batch.
	outputTupleStartIdx int
	// windowedBatch is the output batch of chunkerOperator. For performance
	// reasons, the spooled tuples are not copied into it, instead we use a
	// "window" approach.
	windowedBatch coldata.Batch
}

var _ colexecop.ResettableOperator = &chunkerOperator{}

func (c *chunkerOperator) ChildCount(bool) int {
	return 1
}

func (c *chunkerOperator) Child(nth int, _ bool) execinfra.OpNode {
	if nth == 0 {
		return c.input
	}
	colexecerror.InternalError(errors.AssertionFailedf("invalid index %d", nth))
	// This code is unreachable, but the compiler cannot infer that.
	return nil
}

func (c *chunkerOperator) Init(ctx context.Context) {
	if !c.InitHelper.Init(ctx) {
		return
	}
	c.input.init(c.Ctx)
}

func (c *chunkerOperator) Next() coldata.Batch {
	if c.currentChunkFinished {
		return coldata.ZeroBatch
	}
	if !c.haveChunksToEmit {
		// We don't have any chunks to emit, so we need to spool the input.
		c.input.spool()
		c.haveChunksToEmit = true
		c.numTuplesInChunks = c.input.getNumTuples()
		c.newChunksCol = c.input.getPartitionsCol()
	}
	outputTupleEndIdx := c.numTuplesInChunks
	if c.outputTupleStartIdx == outputTupleEndIdx {
		// Current chunk has been fully output.
		c.currentChunkFinished = true
		return coldata.ZeroBatch
	}
	if c.newChunksCol == nil {
		// When newChunksCol is nil, then all tuples that are returned via
		// getValues are equal on the ordered columns, so we simply emit the next
		// "window" of those tuples.
		if outputTupleEndIdx-c.outputTupleStartIdx > coldata.BatchSize() {
			outputTupleEndIdx = c.outputTupleStartIdx + coldata.BatchSize()
		}
	} else {
		// newChunksCol is non-nil, so there are multiple chunks within the
		// current tuples. We will emit a single chunk as a separate batch and
		// then will proceed to emitting zero-length batches until we're reset.
		outputTupleEndIdx = c.outputTupleStartIdx + 1
		for outputTupleEndIdx < c.numTuplesInChunks && !c.newChunksCol[outputTupleEndIdx] {
			outputTupleEndIdx++
		}
		c.currentChunkFinished = true
	}
	for i := range c.inputTypes {
		window := c.input.getValues(i).Window(c.outputTupleStartIdx, outputTupleEndIdx)
		c.windowedBatch.ReplaceCol(window, i)
	}
	c.windowedBatch.SetSelection(false)
	c.windowedBatch.SetLength(outputTupleEndIdx - c.outputTupleStartIdx)
	c.outputTupleStartIdx = outputTupleEndIdx
	return c.windowedBatch
}

func (c *chunkerOperator) done() bool {
	return c.input.done()
}

func (c *chunkerOperator) Reset(_ context.Context) {
	c.currentChunkFinished = false
	if c.newChunksCol != nil {
		if c.outputTupleStartIdx == c.numTuplesInChunks {
			// We have processed all chunks among the current tuples, so we will need
			// to get new chunks.
			c.haveChunksToEmit = false
		}
	} else {
		// We have processed all current tuples (that comprised a single chunk), so
		// we will need to get new chunks.
		c.haveChunksToEmit = false
	}
	if !c.haveChunksToEmit {
		c.input.emptyBuffer()
		c.outputTupleStartIdx = 0
	}
}
