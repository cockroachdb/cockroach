// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// {{/*
// +build execgen_template
//
// This file is the execgen template for crossjoiner.eg.go. It's formatted in a
// special way, so it's both valid Go and a valid text/template input. This
// permits editing this file with editor support.
//
// */}}

package colexecjoin

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/typeconv"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// Workaround for bazel auto-generated code. goimports does not automatically
// pick up the right packages when run within the bazel sandbox.
var (
	_ = typeconv.DatumVecCanonicalTypeFamily
	_ = types.BoolFamily
)

// buildFromLeftBatch is the body of buildFromLeftInput that templates out the
// fact whether the current left batch has a selection vector or not.
// execgen:inline
// execgen:template<hasSel>
func buildFromLeftBatch(b *crossJoinerBase, currentBatch coldata.Batch, sel []int, hasSel bool) {
	leftNumRepeats := b.builderState.setup.leftNumRepeats
	leftSrcEndIdx := b.builderState.setup.leftSrcEndIdx
	outputCapacity := b.output.Capacity()
	var srcStartIdx int
	bs := &b.builderState.left
	// Loop over every column.
	for colIdx := range b.left.types {
		outStartIdx := destStartIdx
		src := currentBatch.ColVec(colIdx)
		srcNulls := src.Nulls()
		out := b.output.ColVec(colIdx)
		outNulls := out.Nulls()
		switch b.left.canonicalTypeFamilies[colIdx] {
		// {{range .}}
		case _CANONICAL_TYPE_FAMILY:
			switch b.left.types[colIdx].Width() {
			// {{range .WidthOverloads}}
			case _TYPE_WIDTH:
				srcCol := src.TemplateType()
				outCol := out.TemplateType()
				if leftNumRepeats == 1 {
					// Loop over every tuple in the current batch.
					for bs.curSrcStartIdx < leftSrcEndIdx {
						// Repeat each tuple one time.
						if hasSel {
							srcStartIdx = sel[bs.curSrcStartIdx]
						} else {
							srcStartIdx = bs.curSrcStartIdx
						}
						if srcNulls.NullAt(srcStartIdx) {
							outNulls.SetNull(outStartIdx)
						} else {
							val := srcCol.Get(srcStartIdx)
							outCol.Set(outStartIdx, val)
						}
						outStartIdx++
						bs.curSrcStartIdx++

						if outStartIdx == outputCapacity {
							// We reached the capacity of the output vector,
							// so we move to the next column.
							if colIdx == len(b.left.types)-1 {
								// This is the last column.
								return
							}
							// We need to start building the next column.
							break
						}
					}
				} else {
					// Loop over every tuple in the current batch.
					for ; bs.curSrcStartIdx < leftSrcEndIdx; bs.curSrcStartIdx++ {
						// Repeat each row leftNumRepeats times.
						if hasSel {
							srcStartIdx = sel[bs.curSrcStartIdx]
						} else {
							srcStartIdx = bs.curSrcStartIdx
						}
						toAppend := leftNumRepeats - bs.numRepeatsIdx
						if outStartIdx+toAppend > outputCapacity {
							toAppend = outputCapacity - outStartIdx
						}
						if srcNulls.NullAt(srcStartIdx) {
							outNulls.SetNullRange(outStartIdx, outStartIdx+toAppend)
							outStartIdx += toAppend
						} else {
							val := srcCol.Get(srcStartIdx)
							for i := 0; i < toAppend; i++ {
								outCol.Set(outStartIdx, val)
								outStartIdx++
							}
						}

						if outStartIdx == outputCapacity {
							// We reached the capacity of the output vector,
							// so we move to the next column.
							if colIdx == len(b.left.types)-1 {
								// This is the last column.
								bs.numRepeatsIdx += toAppend
								if bs.numRepeatsIdx == leftNumRepeats {
									// The current tuple has already been repeated
									// the desired number of times, so we advance
									// the source index.
									bs.curSrcStartIdx++
									bs.numRepeatsIdx = 0
								}
								return
							}
							// We need to start building the next column.
							break
						}
						// We fully processed the current tuple for the current
						// column, and before moving on to the next one, we need
						// to reset numRepeatsIdx (so that the next tuple would
						// be repeated leftNumRepeats times).
						bs.numRepeatsIdx = 0
					}
				}
				// {{end}}
			}
		// {{end}}
		default:
			colexecerror.InternalError(errors.AssertionFailedf("unhandled type %s", b.left.types[colIdx].String()))
		}
		if colIdx < len(b.left.types)-1 {
			// Transition to building the next column with the same
			// initial builder state as the current column.
			*bs = initialBuilderState
		}
	}
}

// buildFromLeftInput builds part of the output of a cross join that comes from
// the vectors of the left input. The new output tuples are put starting at
// index destStartIdx and will not exceed the capacity of the output batch. It
// is assumed that setupLeftBuilder and prepareForNextLeftBatch have been
// called.
//
// The goal of this method is to repeat each tuple from the left input
// leftNumRepeats times. Only the tuples in [curSrcStartIdx; leftSrcEndIdx) are
// used from the current left batch.
func (b *crossJoinerBase) buildFromLeftInput(ctx context.Context, destStartIdx int) {
	initialBuilderState := b.builderState.left
	currentBatch := b.builderState.left.currentBatch
	b.left.unlimitedAllocator.PerformOperation(
		b.output.ColVecs()[:len(b.left.types)],
		func() {
			sel := currentBatch.Selection()
			if sel != nil {
				buildFromLeftBatch(b, currentBatch, sel, true)
			} else {
				buildFromLeftBatch(b, currentBatch, sel, false)
			}
		},
	)
}

// buildFromRightInput builds part of the output of a cross join that comes from
// the vectors of the right input. The new output tuples are put starting at
// index destStartIdx and will not exceed the capacity of the output batch. It
// is assumed that the right input has been fully consumed and is stores in
// b.rightTuples spilling queue.
//
// The goal of this method is to repeat all tuples from the right input
// rightNumRepeats times (i.e. repeating the whole list of tuples at once).
func (b *crossJoinerBase) buildFromRightInput(ctx context.Context, destStartIdx int) {
	var err error
	bs := &b.builderState
	b.right.unlimitedAllocator.PerformOperation(
		b.output.ColVecs()[bs.rightColOffset:],
		func() {
			outStartIdx := destStartIdx
			outputCapacity := b.output.Capacity()
			// Repeat the buffered tuples rightNumRepeats times until we fill
			// the output capacity.
			for ; outStartIdx < outputCapacity && bs.right.numRepeatsIdx < bs.setup.rightNumRepeats; bs.right.numRepeatsIdx++ {
				currentBatch := bs.right.currentBatch
				if currentBatch == nil {
					currentBatch, err = b.rightTuples.Dequeue(ctx)
					if err != nil {
						colexecerror.InternalError(err)
					}
					bs.right.currentBatch = currentBatch
					bs.right.curSrcStartIdx = 0
				}
				batchLength := currentBatch.Length()
				for batchLength > 0 {
					toAppend := batchLength - bs.right.curSrcStartIdx
					if outStartIdx+toAppend > outputCapacity {
						toAppend = outputCapacity - outStartIdx
					}

					// Loop over every column.
					for colIdx := range b.right.types {
						src := currentBatch.ColVec(colIdx)
						srcNulls := src.Nulls()
						out := b.output.ColVec(colIdx + bs.rightColOffset)
						outNulls := out.Nulls()
						switch b.right.canonicalTypeFamilies[colIdx] {
						// {{range .}}
						case _CANONICAL_TYPE_FAMILY:
							switch b.right.types[colIdx].Width() {
							// {{range .WidthOverloads}}
							case _TYPE_WIDTH:
								srcCol := src.TemplateType()
								outCol := out.TemplateType()

								// Optimization in the case that group length is 1, use assign
								// instead of copy.
								if toAppend == 1 {
									if srcNulls.NullAt(bs.right.curSrcStartIdx) {
										outNulls.SetNull(outStartIdx)
									} else {
										v := srcCol.Get(bs.right.curSrcStartIdx)
										outCol.Set(outStartIdx, v)
									}
								} else {
									out.Copy(
										coldata.SliceArgs{
											Src:         src,
											DestIdx:     outStartIdx,
											SrcStartIdx: bs.right.curSrcStartIdx,
											SrcEndIdx:   bs.right.curSrcStartIdx + toAppend,
										},
									)
								}
								// {{end}}
							}
							// {{end}}
						default:
							colexecerror.InternalError(errors.AssertionFailedf("unhandled type %s", b.right.types[colIdx].String()))
						}
					}
					outStartIdx += toAppend

					if toAppend < batchLength-bs.right.curSrcStartIdx {
						// If we haven't materialized all the tuples from the
						// batch, then we are ready to emit the output batch.
						bs.right.curSrcStartIdx += toAppend
						return
					}
					// We have fully processed the current batch, so we need to
					// get the next one.
					currentBatch, err = b.rightTuples.Dequeue(ctx)
					if err != nil {
						colexecerror.InternalError(err)
					}
					bs.right.currentBatch = currentBatch
					batchLength = currentBatch.Length()
					bs.right.curSrcStartIdx = 0
				}
				// We have fully processed all the batches from the right side,
				// so we need to Rewind the queue.
				if err := b.rightTuples.Rewind(); err != nil {
					colexecerror.InternalError(err)
				}
				bs.right.currentBatch = nil
			}
		})
}
