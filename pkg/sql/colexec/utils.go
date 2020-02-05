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
	zeroUint64Column = make([]uint64, coldata.MaxBatchSize)

	zeroDecimalValue  apd.Decimal
	zeroFloat64Value  float64
	zeroInt16Value    int16
	zeroInt32Value    int32
	zeroInt64Value    int64
	zeroIntervalValue duration.Duration
)

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
