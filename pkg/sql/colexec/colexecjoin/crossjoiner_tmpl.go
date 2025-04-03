// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// {{/*
//go:build execgen_template

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
	if !hasSel {
		// Remove the unused warning.
		_ = sel
	}
	// We'll be modifying the builder state as we go, but we'll need to be able
	// to restore the state to initial for each column.
	initialBuilderState := b.builderState.left
	bs := &b.builderState.left
	leftNumRepeats := b.builderState.setup.leftNumRepeats
	leftSrcEndIdx := b.builderState.setup.leftSrcEndIdx
	outputCapacity := b.output.Capacity()
	var srcStartIdx int
	// Loop over every column.
	for colIdx := range b.left.types {
		if colIdx > 0 {
			// Restore the builder state so that this new column started off
			// fresh.
			*bs = initialBuilderState
		}
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
					for bs.curSrcStartIdx < leftSrcEndIdx && outStartIdx < outputCapacity {
						// Repeat each tuple one time.
						if hasSel {
							srcStartIdx = sel[bs.curSrcStartIdx]
						} else {
							srcStartIdx = bs.curSrcStartIdx
						}
						if srcNulls.NullAt(srcStartIdx) {
							outNulls.SetNull(outStartIdx)
						} else {
							// {{if .IsBytesLike}}
							outCol.Copy(srcCol, outStartIdx, srcStartIdx)
							// {{else}}
							val := srcCol.Get(srcStartIdx)
							outCol.Set(outStartIdx, val)
							// {{end}}
						}
						outStartIdx++
						bs.curSrcStartIdx++
					}
				} else {
					// Loop over every tuple in the current batch.
					for bs.curSrcStartIdx < leftSrcEndIdx && outStartIdx < outputCapacity {
						// Repeat each row leftNumRepeats times.
						if hasSel {
							srcStartIdx = sel[bs.curSrcStartIdx]
						} else {
							srcStartIdx = bs.curSrcStartIdx
						}
						// {{/* toAppend will always be positive. */}}
						toAppend := leftNumRepeats - bs.numRepeatsIdx
						if outStartIdx+toAppend > outputCapacity {
							// We don't have enough space to repeat the current
							// value the required number of times, so we'll have
							// to continue from here on the next call.
							toAppend = outputCapacity - outStartIdx
							bs.numRepeatsIdx += toAppend
						} else {
							// We fully processed the current tuple for the
							// current column, and before moving on to the next
							// one, we need to reset numRepeatsIdx (so that the
							// next tuple would be repeated leftNumRepeats
							// times).
							bs.curSrcStartIdx++
							bs.numRepeatsIdx = 0
						}
						if srcNulls.NullAt(srcStartIdx) {
							outNulls.SetNullRange(outStartIdx, outStartIdx+toAppend)
						} else {
							// {{if not .IsBytesLike}}
							// {{if .Sliceable}}
							outCol := outCol[outStartIdx:]
							_ = outCol[toAppend-1]
							// {{end}}
							val := srcCol.Get(srcStartIdx)
							// {{end}}
							for i := 0; i < toAppend; i++ {
								// {{if .IsBytesLike}}
								outCol.Copy(srcCol, outStartIdx+i, srcStartIdx)
								// {{else}}
								// {{if .Sliceable}}
								// {{/*
								//     For the sliceable types, we sliced outCol
								//     to start at outStartIdx, so we use index
								//     i directly.
								// */}}
								//gcassert:bce
								outCol.Set(i, val)
								// {{else}}
								// {{/*
								//     For the non-sliceable types, outCol
								//     vector is the original one (i.e. without
								//     an adjustment), so we need to add
								//     outStartIdx to set the element at the
								//     correct index.
								// */}}
								outCol.Set(outStartIdx+i, val)
								// {{end}}
								// {{end}}
							}
						}
						outStartIdx += toAppend
					}
				}
				// {{end}}
			}
		// {{end}}
		default:
			colexecerror.InternalError(errors.AssertionFailedf("unhandled type %s", b.left.types[colIdx].String()))
		}
	}
	// If there are no columns projected from the left input, simply advance the
	// cross-joiner state according to the number of input rows.
	if len(b.left.types) == 0 {
		outStartIdx := destStartIdx
		for bs.curSrcStartIdx < leftSrcEndIdx && outStartIdx < outputCapacity {
			// Repeat each row leftNumRepeats times.
			// {{/* toAppend will always be positive. */}}
			toAppend := leftNumRepeats - bs.numRepeatsIdx
			if outStartIdx+toAppend > outputCapacity {
				// We don't have enough space to repeat the current
				// value the required number of times, so we'll have
				// to continue from here on the next call.
				toAppend = outputCapacity - outStartIdx
				bs.numRepeatsIdx += toAppend
			} else {
				// We fully processed the current tuple for the
				// current column, and before moving on to the next
				// one, we need to reset numRepeatsIdx (so that the
				// next tuple would be repeated leftNumRepeats
				// times).
				bs.curSrcStartIdx++
				bs.numRepeatsIdx = 0
			}
			outStartIdx += toAppend
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
// leftNumRepeats times. Only the tuples in [curSrcStartIdx, leftSrcEndIdx) are
// used from the current left batch.
func (b *crossJoinerBase) buildFromLeftInput(ctx context.Context, destStartIdx int) {
	currentBatch := b.builderState.left.currentBatch
	b.left.unlimitedAllocator.PerformOperation(
		b.output.ColVecs()[:len(b.left.types)],
		func() {
			if sel := currentBatch.Selection(); sel != nil {
				buildFromLeftBatch(b, currentBatch, sel, true /* hasSel */)
			} else {
				buildFromLeftBatch(b, currentBatch, nil, false /* hasSel */)
			}
		},
	)
}

// buildFromRightInput builds part of the output of a cross join that comes from
// the vectors of the right input. The new output tuples are put starting at
// index destStartIdx and will not exceed the capacity of the output batch. It
// is assumed that the right input has been fully consumed and is stored in
// b.rightTuples spilling queue.
//
// The goal of this method is to repeat all tuples from the right input
// rightNumRepeats times (i.e. repeating the whole list of tuples at once).
func (b *crossJoinerBase) buildFromRightInput(ctx context.Context, destStartIdx int) {
	var err error
	bs := &b.builderState.right
	rightNumRepeats := b.builderState.setup.rightNumRepeats
	b.right.unlimitedAllocator.PerformOperation(
		b.output.ColVecs()[b.builderState.rightColOffset:],
		func() {
			outStartIdx := destStartIdx
			outputCapacity := b.output.Capacity()
			// Repeat the buffered tuples rightNumRepeats times until we fill
			// the output capacity.
			for ; outStartIdx < outputCapacity && bs.numRepeatsIdx < rightNumRepeats; bs.numRepeatsIdx++ {
				currentBatch := bs.currentBatch
				if currentBatch == nil {
					currentBatch, err = b.rightTuples.Dequeue(ctx)
					if err != nil {
						colexecerror.InternalError(err)
					}
					bs.currentBatch = currentBatch
					bs.curSrcStartIdx = 0
				}
				batchLength := currentBatch.Length()
				for batchLength > 0 {
					toAppend := batchLength - bs.curSrcStartIdx
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
									if srcNulls.NullAt(bs.curSrcStartIdx) {
										outNulls.SetNull(outStartIdx)
									} else {
										// {{if .IsBytesLike}}
										outCol.Copy(srcCol, outStartIdx, bs.curSrcStartIdx)
										// {{else}}
										v := srcCol.Get(bs.curSrcStartIdx)
										outCol.Set(outStartIdx, v)
										// {{end}}
									}
								} else {
									out.Copy(
										coldata.SliceArgs{
											Src:         src,
											DestIdx:     outStartIdx,
											SrcStartIdx: bs.curSrcStartIdx,
											SrcEndIdx:   bs.curSrcStartIdx + toAppend,
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

					if toAppend < batchLength-bs.curSrcStartIdx {
						// If we haven't materialized all the tuples from the
						// batch, then we are ready to emit the output batch.
						bs.curSrcStartIdx += toAppend
						return
					}
					// We have fully processed the current batch, so we need to
					// get the next one.
					currentBatch, err = b.rightTuples.Dequeue(ctx)
					if err != nil {
						colexecerror.InternalError(err)
					}
					bs.currentBatch = currentBatch
					batchLength = currentBatch.Length()
					bs.curSrcStartIdx = 0
				}
				// We have fully processed all the batches from the right side,
				// so we need to Rewind the queue.
				if err := b.rightTuples.Rewind(ctx); err != nil {
					colexecerror.InternalError(err)
				}
				bs.currentBatch = nil
			}
		})
}
