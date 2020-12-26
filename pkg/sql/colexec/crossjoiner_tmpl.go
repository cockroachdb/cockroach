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

package colexec

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execgen"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase/colexecerror"
	"github.com/cockroachdb/errors"
)

// buildFromLeftInput builds part of the output of a cross join that comes from
// the vectors of the left input. The new output tuples are put starting at
// index destStartIdx and will not exceed the capacity of the output batch. It
// is assumed that setupBuilder has been called.
//
// The goal of this method is to repeat each tuple from the left input
// leftNumRepeats times. For set-operation joins only first setOpLeftSrcIdx
// tuples are built from.
func (b *crossJoinerBase) buildFromLeftInput(ctx context.Context, destStartIdx int) {
	var err error
	currentBatch := b.builderState.left.currentBatch
	if currentBatch == nil {
		currentBatch, err = b.left.tuples.dequeue(ctx)
		if err != nil {
			colexecerror.InternalError(err)
		}
		b.builderState.left.currentBatch = currentBatch
		b.builderState.left.curSrcStartIdx = 0
		b.builderState.left.numRepeatsIdx = 0
	}
	initialBuilderState := b.builderState.left
	b.left.unlimitedAllocator.PerformOperation(
		b.output.ColVecs()[:len(b.left.types)],
		func() {
			isSetOp := b.joinType.IsSetOpJoin()
			batchLength := currentBatch.Length()
			for batchLength > 0 {
				// Loop over every column.
			LeftColLoop:
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
							// Loop over every tuple in the current batch.
							for ; b.builderState.left.curSrcStartIdx < batchLength; b.builderState.left.curSrcStartIdx++ {
								// Repeat each row leftNumRepeats times.
								// {{/*
								// TODO(yuzefovich): we can optimize this code for
								// LEFT SEMI, INTERSECT ALL, and EXCEPT ALL joins
								// because in that case numRepeats is always 1.
								// */}}
								srcStartIdx := b.builderState.left.curSrcStartIdx
								repeatsLeft := b.builderState.setup.leftNumRepeats - b.builderState.left.numRepeatsIdx
								toAppend := repeatsLeft
								if outStartIdx+toAppend > b.output.Capacity() {
									toAppend = b.output.Capacity() - outStartIdx
								}

								if isSetOp {
									if b.builderState.left.setOpLeftSrcIdx == b.builderState.setup.leftSrcEndIdx {
										// We have fully materialized first leftSrcEndIdx
										// tuples in the current column, so we need to
										// either transition to the next column or exit.
										// We can accomplish this by setting toAppend
										// to 0.
										toAppend = 0
									}
									b.builderState.left.setOpLeftSrcIdx += toAppend
								}

								// {{/*
								// TODO(yuzefovich): check whether it is beneficial
								// to have 'if toAppend > 0' check here.
								// */}}
								if srcNulls.NullAt(srcStartIdx) {
									outNulls.SetNullRange(outStartIdx, outStartIdx+toAppend)
									outStartIdx += toAppend
								} else {
									val := srcCol.Get(srcStartIdx)
									for i := 0; i < toAppend; i++ {
										execgen.SET(outCol, outStartIdx, val)
										outStartIdx++
									}
								}

								if toAppend < repeatsLeft {
									// We didn't materialize all the tuples in the current batch, so
									// we move to the next column.
									if colIdx == len(b.left.types)-1 {
										// This is the last column, so we update the builder state
										// and exit.
										b.builderState.left.numRepeatsIdx += toAppend
										return
									}
									// We need to start building the next column
									// with the same initial builder state as the
									// current column.
									b.builderState.left = initialBuilderState
									continue LeftColLoop
								}
								// We fully processed the current tuple, and before moving on to the
								// next one, we need to reset numRepeatsIdx (so that the next tuple
								// would be repeated leftNumRepeats times).
								b.builderState.left.numRepeatsIdx = 0
							}
							// {{end}}
						}
					// {{end}}
					default:
						colexecerror.InternalError(errors.AssertionFailedf("unhandled type %s", b.left.types[colIdx].String()))
					}
					if colIdx == len(b.left.types)-1 {
						// We have appended some tuples into the output batch from the current
						// batch (the latter is now fully processed), so we need to adjust
						// destStartIdx accordingly for the next batch.
						destStartIdx = outStartIdx
					} else {
						b.builderState.left = initialBuilderState
					}
				}
				// We have processed all tuples in the current batch from the
				// buffered group, so we need to dequeue the next one.
				b.left.unlimitedAllocator.ReleaseBatch(currentBatch)
				currentBatch, err = b.left.tuples.dequeue(ctx)
				if err != nil {
					colexecerror.InternalError(err)
				}
				b.builderState.left.currentBatch = currentBatch
				batchLength = currentBatch.Length()
				// We have transitioned to building from a new batch, so we
				// need to update the builder state to build from the beginning
				// of the new batch.
				b.builderState.left.curSrcStartIdx = 0
				b.builderState.left.numRepeatsIdx = 0
				// We also need to update 'initialBuilderState' so that the
				// builder state gets reset correctly in-between different
				// columns in the loop above.
				initialBuilderState = b.builderState.left
			}
		},
	)
}

// buildFromRightInput builds part of the output of a cross join that comes from
// the vectors of the right input. The new output tuples are put starting at
// index destStartIdx and will not exceed the capacity of the output batch. It
// is assumed that setupBuilder has been called.
//
// The goal of this method is to repeat all tuples from the right input
// rightNumRepeats times (i.e. repeating the whole list of tuples at once).
func (b *crossJoinerBase) buildFromRightInput(ctx context.Context, destStartIdx int) {
	var err error
	b.right.unlimitedAllocator.PerformOperation(
		b.output.ColVecs()[b.builderState.rightColOffset:],
		func() {
			outStartIdx := destStartIdx
			// Repeat the buffered tuples rightNumRepeats times.
			for ; b.builderState.right.numRepeatsIdx < b.builderState.setup.rightNumRepeats; b.builderState.right.numRepeatsIdx++ {
				currentBatch := b.builderState.right.currentBatch
				if currentBatch == nil {
					currentBatch, err = b.right.tuples.dequeue(ctx)
					if err != nil {
						colexecerror.InternalError(err)
					}
					b.builderState.right.currentBatch = currentBatch
					b.builderState.right.curSrcStartIdx = 0
				}
				batchLength := currentBatch.Length()
				for batchLength > 0 {
					toAppend := batchLength - b.builderState.right.curSrcStartIdx
					if outStartIdx+toAppend > b.output.Capacity() {
						toAppend = b.output.Capacity() - outStartIdx
					}

					// Loop over every column.
					for colIdx := range b.right.types {
						src := currentBatch.ColVec(colIdx)
						srcNulls := src.Nulls()
						out := b.output.ColVec(colIdx + b.builderState.rightColOffset)
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
									if srcNulls.NullAt(b.builderState.right.curSrcStartIdx) {
										outNulls.SetNull(outStartIdx)
									} else {
										v := srcCol.Get(b.builderState.right.curSrcStartIdx)
										execgen.SET(outCol, outStartIdx, v)
									}
								} else {
									out.Copy(
										coldata.CopySliceArgs{
											SliceArgs: coldata.SliceArgs{
												Src:         src,
												DestIdx:     outStartIdx,
												SrcStartIdx: b.builderState.right.curSrcStartIdx,
												SrcEndIdx:   b.builderState.right.curSrcStartIdx + toAppend,
											},
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

					if toAppend < batchLength-b.builderState.right.curSrcStartIdx {
						// If we haven't materialized all the tuples from the
						// batch, then we are ready to emit the output batch.
						b.builderState.right.curSrcStartIdx += toAppend
						return
					}
					// We have fully processed the current batch, so we need to
					// get the next one.
					b.right.unlimitedAllocator.ReleaseBatch(currentBatch)
					currentBatch, err = b.right.tuples.dequeue(ctx)
					if err != nil {
						colexecerror.InternalError(err)
					}
					b.builderState.right.currentBatch = currentBatch
					batchLength = currentBatch.Length()
					b.builderState.right.curSrcStartIdx = 0
				}
				// We have fully processed all the batches from the right side,
				// so we need to rewind the queue.
				if err := b.right.tuples.rewind(); err != nil {
					colexecerror.InternalError(err)
				}
				b.builderState.right.currentBatch = nil
			}
		})
}
