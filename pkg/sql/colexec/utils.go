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
	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
)

var (
	zeroBoolColumn   = make([]bool, coldata.MaxBatchSize)
	zeroUint16Column = make([]uint16, coldata.MaxBatchSize)
	zeroUint64Column = make([]uint64, coldata.MaxBatchSize)

	zeroDecimalValue  apd.Decimal
	zeroFloat64Value  float64
	zeroInt16Value    int16
	zeroInt32Value    int32
	zeroInt64Value    int64
	zeroIntervalValue duration.Duration
)

// decimalOverloadScratch is a utility struct that helps us avoid allocations
// of temporary decimals on every overloaded operation with them. In order for
// the templates to see it correctly, a local variable named `scratch` of this
// type must be declared before the inlined overloaded code.
type decimalOverloadScratch struct {
	tmpDec1, tmpDec2 apd.Decimal
}

// CopyBatch copies the original batch and returns that copy. However, note that
// the underlying capacity might be different (a new batch is created only with
// capacity original.Length()).
func CopyBatch(allocator *Allocator, original coldata.Batch) coldata.Batch {
	typs := make([]coltypes.T, original.Width())
	for i, vec := range original.ColVecs() {
		typs[i] = vec.Type()
	}
	b := allocator.NewMemBatchWithSize(typs, int(original.Length()))
	b.SetLength(original.Length())
	allocator.PerformOperation(b.ColVecs(), func() {
		for colIdx, col := range original.ColVecs() {
			b.ColVec(colIdx).Copy(coldata.CopySliceArgs{
				SliceArgs: coldata.SliceArgs{
					ColType:   typs[colIdx],
					Src:       col,
					SrcEndIdx: uint64(original.Length()),
				},
			})
		}
	})
	return b
}

// makeWindowIntoBatch updates windowedBatch so that it provides a "window"
// into inputBatch starting at tuple index startIdx. It handles selection
// vectors on inputBatch as well (in which case windowedBatch will also have a
// "windowed" selection vector).
func makeWindowIntoBatch(
	windowedBatch, inputBatch coldata.Batch, startIdx uint16, inputTypes []coltypes.T,
) {
	inputBatchLen := inputBatch.Length()
	windowStart := uint64(startIdx)
	windowEnd := uint64(inputBatchLen)
	if sel := inputBatch.Selection(); sel != nil {
		// We have a selection vector on the input batch, and in order to avoid
		// deselecting (i.e. moving the data over), we will provide an adjusted
		// selection vector to the windowed batch as well.
		windowedBatch.SetSelection(true)
		windowIntoSel := sel[startIdx:inputBatchLen]
		copy(windowedBatch.Selection(), windowIntoSel)
		maxSelIdx := uint16(0)
		for _, selIdx := range windowIntoSel {
			if selIdx > maxSelIdx {
				maxSelIdx = selIdx
			}
		}
		windowStart = 0
		windowEnd = uint64(maxSelIdx + 1)
	} else {
		windowedBatch.SetSelection(false)
	}
	for i, typ := range inputTypes {
		window := inputBatch.ColVec(i).Window(typ, windowStart, windowEnd)
		windowedBatch.ReplaceCol(window, i)
	}
	windowedBatch.SetLength(inputBatchLen - startIdx)
}
