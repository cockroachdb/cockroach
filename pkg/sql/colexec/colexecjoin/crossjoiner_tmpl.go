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
	if currentBatch == nil || b.builderState.left.curSrcStartIdx == currentBatch.Length() {
		// We need to get the next batch to build from if it is the first one or
		// we have fully processed the previous one.
		currentBatch, err = b.left.tuples.Dequeue(ctx)
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
			leftNumRepeats := b.builderState.setup.leftNumRepeats
			isSetOp := b.joinType.IsSetOpJoin()
			outputCapacity := b.output.Capacity()
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
							if leftNumRepeats == 1 {
								// Loop over every tuple in the current batch.
								for b.builderState.left.curSrcStartIdx < batchLength {
									// Repeat each tuple one time.
									if isSetOp {
										if b.builderState.left.setOpLeftSrcIdx == b.builderState.setup.leftSrcEndIdx {
											// We have fully materialized first leftSrcEndIdx
											// tuples in the current column, so we need to
											// either transition to the next column or exit.
											if colIdx == len(b.left.types)-1 {
												// This is the last column.
												return
											}
											// We need to start building the next column
											// with the same initial builder state as the
											// current column.
											b.builderState.left = initialBuilderState
											continue LeftColLoop
										}
										b.builderState.left.setOpLeftSrcIdx++
									}

									srcStartIdx := b.builderState.left.curSrcStartIdx
									if srcNulls.NullAt(srcStartIdx) {
										outNulls.SetNull(outStartIdx)
									} else {
										val := srcCol.Get(srcStartIdx)
										outCol.Set(outStartIdx, val)
									}
									outStartIdx++
									b.builderState.left.curSrcStartIdx++

									if outStartIdx == outputCapacity {
										// We reached the capacity of the output vector,
										// so we move to the next column.
										if colIdx == len(b.left.types)-1 {
											// This is the last column.
											return
										}
										// We need to start building the next column
										// with the same initial builder state as the
										// current column.
										b.builderState.left = initialBuilderState
										continue LeftColLoop
									}
								}
							} else {
								// Loop over every tuple in the current batch.
								for ; b.builderState.left.curSrcStartIdx < batchLength; b.builderState.left.curSrcStartIdx++ {
									// Repeat each row leftNumRepeats times.
									srcStartIdx := b.builderState.left.curSrcStartIdx
									toAppend := leftNumRepeats - b.builderState.left.numRepeatsIdx
									if outStartIdx+toAppend > outputCapacity {
										toAppend = outputCapacity - outStartIdx
									}

									if isSetOp {
										if b.builderState.left.setOpLeftSrcIdx == b.builderState.setup.leftSrcEndIdx {
											// We have fully materialized first leftSrcEndIdx
											// tuples in the current column, so we need to
											// either transition to the next column or exit.
											if colIdx == len(b.left.types)-1 {
												// This is the last column.
												return
											}
											// We need to start building the next column
											// with the same initial builder state as the
											// current column.
											b.builderState.left = initialBuilderState
											continue LeftColLoop
										}
										b.builderState.left.setOpLeftSrcIdx += toAppend
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
											b.builderState.left.numRepeatsIdx += toAppend
											if b.builderState.left.numRepeatsIdx == leftNumRepeats {
												// The current tuple has already been repeated
												// the desired number of times, so we advance
												// the source index.
												b.builderState.left.curSrcStartIdx++
												b.builderState.left.numRepeatsIdx = 0
											}
											return
										}
										// We need to start building the next column
										// with the same initial builder state as the
										// current column.
										b.builderState.left = initialBuilderState
										continue LeftColLoop
									}
									// We fully processed the current tuple for the current
									// column, and before moving on to the next one, we need
									// to reset numRepeatsIdx (so that the next tuple would
									// be repeated leftNumRepeats times).
									b.builderState.left.numRepeatsIdx = 0
								}
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
				// buffered group, so we need to Dequeue the next one.
				currentBatch, err = b.left.tuples.Dequeue(ctx)
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
			outputCapacity := b.output.Capacity()
			// Repeat the buffered tuples rightNumRepeats times.
			for ; b.builderState.right.numRepeatsIdx < b.builderState.setup.rightNumRepeats; b.builderState.right.numRepeatsIdx++ {
				currentBatch := b.builderState.right.currentBatch
				if currentBatch == nil {
					currentBatch, err = b.right.tuples.Dequeue(ctx)
					if err != nil {
						colexecerror.InternalError(err)
					}
					b.builderState.right.currentBatch = currentBatch
					b.builderState.right.curSrcStartIdx = 0
				}
				batchLength := currentBatch.Length()
				for batchLength > 0 {
					toAppend := batchLength - b.builderState.right.curSrcStartIdx
					if outStartIdx+toAppend > outputCapacity {
						toAppend = outputCapacity - outStartIdx
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
										outCol.Set(outStartIdx, v)
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
					currentBatch, err = b.right.tuples.Dequeue(ctx)
					if err != nil {
						colexecerror.InternalError(err)
					}
					b.builderState.right.currentBatch = currentBatch
					batchLength = currentBatch.Length()
					b.builderState.right.curSrcStartIdx = 0

					if outStartIdx == outputCapacity {
						// We reached the capacity of the output batch, so we
						// can emit it.
						return
					}
				}
				// We have fully processed all the batches from the right side,
				// so we need to Rewind the queue.
				if err := b.right.tuples.Rewind(); err != nil {
					colexecerror.InternalError(err)
				}
				b.builderState.right.currentBatch = nil
			}
		})
}
